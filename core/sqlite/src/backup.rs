//! Complete causal snapshots with mode-specific attachment closure. No protocol migration.
use crate::attachments::{nonce, safe, sync_dir, verify};
use crate::*;
use rusqlite::backup::{Backup, StepResult};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::{Read as IoRead, Write as IoWrite},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackupKind {
    LocalDisaster,
    SharedBootstrap,
}
#[derive(Clone, Copy, Debug)]
pub struct BackupOptions {
    pub pages_per_step: i32,
    pub max_steps: u32,
    pub busy_retries: u32,
}
impl Default for BackupOptions {
    fn default() -> Self {
        Self {
            pages_per_step: 64,
            max_steps: 100_000,
            busy_retries: 10,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileEntry {
    pub path: String,
    pub sha256: String,
    pub size: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub format: u32,
    pub protocol: u32,
    pub internal: Vec<u32>,
    pub kind: BackupKind,
    pub identity: [String; 3],
    pub schema: Schema,
    pub files: Vec<FileEntry>,
}
fn copy_file(from: &Path, to: &Path, c: &Cancellation) -> Result<FileEntry> {
    safe(from, false)?;
    safe(to, true)?;
    let mut src = File::open(from)?;
    let mut dst = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(to)?;
    let mut buf = vec![0; attachments::CHUNK];
    loop {
        c.check().map_err(Error::Network)?;
        let n = src.read(&mut buf)?;
        if n == 0 {
            break;
        }
        dst.write_all(&buf[..n])?;
    }
    dst.sync_all()?;
    let a = verify(&dst, 1 << 40, c)?;
    Ok(FileEntry {
        path: String::new(),
        sha256: a.hash,
        size: a.size.to_string(),
    })
}
fn snapshot(source: &Connection, path: &Path, o: BackupOptions, c: &Cancellation) -> Result<()> {
    if o.pages_per_step <= 0
        || o.pages_per_step > 4096
        || o.max_steps == 0
        || o.max_steps > 1_000_000
        || o.busy_retries > 100
    {
        return Err(Error::Invalid("backup limits"));
    }
    let mut dest = Connection::open(path)?;
    dest.busy_timeout(Duration::ZERO)?;
    let backup = Backup::new(source, &mut dest)?;
    let mut busy = 0;
    for _ in 0..o.max_steps {
        c.check().map_err(Error::Network)?;
        match backup.step(o.pages_per_step)? {
            StepResult::Done => {
                drop(backup);
                drop(dest);
                File::open(path)?.sync_all()?;
                return Ok(());
            }
            StepResult::More => (),
            StepResult::Busy | StepResult::Locked => {
                busy += 1;
                if busy > o.busy_retries {
                    return Err(Error::Busy);
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            _ => return Err(Error::Invalid("backup step")),
        }
    }
    Err(Error::Invalid("backup step budget"))
}
impl Store {
    pub fn backup(
        &mut self,
        destination: &Path,
        kind: BackupKind,
        o: BackupOptions,
        c: &Cancellation,
    ) -> Result<Manifest> {
        let db = destination.join("store.db");
        validate_file_path(&db)?;
        self.check()?;
        c.check().map_err(Error::Network)?;
        safe(destination, true)?;
        let incomplete:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM _lfs_staging) OR EXISTS(SELECT 1 FROM _lfs_files WHERE state='Installed')",[],|r|r.get(0))?;
        if incomplete {
            return Err(Error::Busy);
        }
        let status = self.status()?;
        if kind == BackupKind::SharedBootstrap && (status.pending_outbox != 0 || !status.healthy())
        {
            return Err(Error::Invalid("shared backup unpublished or blocked"));
        }
        std::fs::create_dir(destination)?;
        let result = (|| {
            snapshot(&self.conn, &db, o, c)?;
            let snapshot = Connection::open(&db)?;
            let cancel = c.clone();
            snapshot.progress_handler(1000, Some(move || cancel.check().is_err()))?;
            // In-flight scratch is deliberately excluded. Complete immutable file closure is mandatory.
            snapshot.execute("DELETE FROM _lfs_staging", [])?;
            if kind == BackupKind::SharedBootstrap {
                c.check().map_err(Error::Network)?;
                // Only the copy is scrubbed. VACUUM also removes pre-existing freelist/freeblock
                // remnants; secure_delete alone cannot remove already discarded source bytes.
                let scrub = snapshot.execute_batch(
                    "PRAGMA secure_delete=ON;
                     DELETE FROM _lfs_files WHERE hash NOT IN (SELECT hash FROM _lfs_attachment_refs);
                     VACUUM;",
                );
                c.check().map_err(Error::Network)?;
                scrub?;
            }
            let selection = match kind {
                BackupKind::SharedBootstrap => {
                    "SELECT DISTINCT hash FROM _lfs_attachment_refs ORDER BY hash"
                }
                BackupKind::LocalDisaster => {
                    "SELECT hash FROM _lfs_attachment_refs UNION SELECT hash FROM _lfs_files WHERE state='Available' ORDER BY hash"
                }
            };
            let hashes: Vec<String> = snapshot
                .prepare(selection)?
                .query_map([], |r| r.get(0))?
                .collect::<std::result::Result<_, _>>()?;
            std::fs::create_dir(destination.join("objects"))?;
            let mut files = Vec::new();
            for h in hashes {
                attachments::digest(&h)?;
                let available: bool = snapshot.query_row(
                    "SELECT EXISTS(SELECT 1 FROM _lfs_files WHERE hash=?1 AND state='Available')",
                    [&h],
                    |r| r.get(0),
                )?;
                if !available {
                    return Err(Error::Invalid("attachment unavailable"));
                }
                let mut entry = copy_file(
                    &self.attachment_root.join("objects").join(&h),
                    &destination.join("objects").join(&h),
                    c,
                )?;
                if entry.sha256 != h {
                    return Err(Error::Invalid("backup attachment hash"));
                }
                entry.path = format!("objects/{h}");
                files.push(entry);
            }
            drop(snapshot);
            File::open(&db)?.sync_all()?;
            let a = verify(&File::open(&db)?, 1 << 40, c)?;
            files.push(FileEntry {
                path: "store.db".into(),
                sha256: a.hash,
                size: a.size.to_string(),
            });
            files.sort_by(|a, b| a.path.cmp(&b.path));
            let manifest = Manifest {
                format: 1,
                protocol: 1,
                internal: vec![2, 1, 1, 1],
                kind,
                identity: [
                    self.identity.dataset.clone(),
                    self.identity.device.clone(),
                    self.identity.incarnation.clone(),
                ],
                schema: self.schema.clone(),
                files,
            };
            let bytes = serde_json::to_vec(&manifest).map_err(|_| Error::Invalid("manifest"))?;
            if bytes.len() > 4 * 1024 * 1024 {
                return Err(Error::Invalid("manifest limit"));
            }
            sync_dir(&destination.join("objects"))?;
            sync_dir(destination)?;
            let mut f = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(destination.join("manifest.tmp"))?;
            c.check().map_err(Error::Network)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
            std::fs::rename(
                destination.join("manifest.tmp"),
                destination.join("manifest.json"),
            )?;
            sync_dir(&destination.join("objects"))?;
            sync_dir(destination)?;
            if let Some(p) = destination.parent() {
                sync_dir(p)?;
            }
            Ok(manifest)
        })();
        if result.is_err() {
            let _ = std::fs::remove_dir_all(destination);
        }
        result
    }
    fn validate_snapshot(&mut self) -> Result<()> {
        let integrity: String = self
            .conn
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
        if integrity != "ok" {
            return Err(Error::Invalid("database integrity"));
        }
        let packets: Vec<(Vec<u8>, Vec<u8>)> = self
            .conn
            .prepare("SELECT hash,bytes FROM _lfs_packets")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<std::result::Result<_, _>>()?;
        let mut expected_nodes = BTreeSet::new();
        let mut expected_edges = BTreeSet::new();
        let mut ops = BTreeMap::new();
        let mut streams = BTreeMap::new();
        for (h, b) in &packets {
            if hash(b).as_slice() != h {
                return Err(Error::Invalid("backup packet hash"));
            }
            let p = Packet::decode(b).map_err(Error::Invalid)?;
            self.validate_packet(&p)?;
            streams.insert((p.identity.clone(), p.seq), hash(b));
            for op in p.operations {
                expected_nodes.insert((
                    op.id.encode(),
                    op.table,
                    op.key.clone(),
                    h.clone(),
                    op.id.index,
                ));
                for parent in &op.parents {
                    expected_edges.insert((op.id.encode(), parent.clone()));
                }
                ops.insert(op.id.encode(), op);
            }
        }
        for (_, b) in &packets {
            let p = Packet::decode(b).map_err(Error::Invalid)?;
            if p.seq > 1 && streams.get(&(p.identity, p.seq - 1)) != p.predecessor.as_ref() {
                return Err(Error::Invalid("backup stream gap"));
            }
        }
        let mut remaining = ops.clone();
        let mut done = BTreeSet::new();
        let mut registers: BTreeMap<(u32, String), lfs_protocol::Register> = BTreeMap::new();
        while !remaining.is_empty() {
            let ready: Vec<_> = remaining
                .iter()
                .filter(|(_, o)| o.parents.iter().all(|p| done.contains(p)))
                .map(|(id, _)| id.clone())
                .collect();
            if ready.is_empty() {
                return Err(Error::Invalid("backup DAG cycle or missing parent"));
            }
            for id in ready {
                let op = remaining.remove(&id).unwrap();
                registers
                    .entry((op.table, op.key.clone()))
                    .or_default()
                    .insert(op)
                    .map_err(Error::Invalid)?;
                done.insert(id);
            }
        }
        type NodeRow = (Vec<u8>, u32, String, Vec<u8>, u32);
        let nodes: BTreeSet<NodeRow> = self
            .conn
            .prepare("SELECT id,table_id,record_key,hash,op_index FROM _lfs_nodes")?
            .query_map([], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
            })?
            .collect::<std::result::Result<_, _>>()?;
        let edges: BTreeSet<(Vec<u8>, Vec<u8>)> = self
            .conn
            .prepare("SELECT child,parent FROM _lfs_edges")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<std::result::Result<_, _>>()?;
        if nodes != expected_nodes || edges != expected_edges {
            return Err(Error::Invalid("backup DAG metadata"));
        }
        let mut tips = BTreeSet::new();
        let mut expected_rows: BTreeMap<u32, Vec<Vec<Value>>> = BTreeMap::new();
        for ((t, k), reg) in registers {
            for head in reg.heads() {
                tips.insert((t, k.clone(), head.id.encode()));
            }
            if let Some(row) = reg.projection().and_then(|o| o.row) {
                expected_rows.entry(t).or_default().push(row);
            }
        }
        let actual_tips: BTreeSet<(u32, String, Vec<u8>)> = self
            .conn
            .prepare("SELECT table_id,record_key,id FROM _lfs_tips")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<std::result::Result<_, _>>()?;
        if tips != actual_tips {
            return Err(Error::Invalid("backup heads"));
        }
        for t in &self.schema.tables {
            let rows = query(
                &self.conn,
                &format!(
                    "SELECT * FROM \"{}\" ORDER BY \"{}\"",
                    t.name, t.columns[0].name
                ),
                &[],
            )?;
            if rows != expected_rows.remove(&t.id).unwrap_or_default() {
                return Err(Error::Invalid("backup projection"));
            }
        }
        type WireRow = (Vec<u8>, Vec<u8>, Vec<u8>, bool);
        let wires:Vec<WireRow>=self.conn.prepare("SELECT payload_hash,envelope_hash,bytes,1 FROM _lfs_outgoing_wire UNION ALL SELECT payload_hash,envelope_hash,bytes,0 FROM _lfs_received_wire")?.query_map([],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?.collect::<std::result::Result<_,_>>()?;
        let mut expected_refs = BTreeSet::new();
        for (ph, eh, b, outgoing) in wires {
            let p = Package::decode_envelope(&b)?;
            if p.key.hash.as_slice() != ph || hash(&b).as_slice() != eh {
                return Err(Error::Invalid("backup envelope"));
            }
            attachments::validate_refs(&p.attachment_references)?;
            if outgoing && packets.iter().any(|(h, _)| *h == ph) {
                for h in p.attachment_references {
                    expected_refs.insert((ph.clone(), h));
                }
            }
        }
        let refsets: Vec<(Vec<u8>, String)> = self
            .conn
            .prepare("SELECT packet_hash,refs FROM _lfs_refsets")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<std::result::Result<_, _>>()?;
        for (ph, refs) in refsets {
            let refs: Vec<String> = refs.lines().map(str::to_owned).collect();
            attachments::validate_refs(&refs)?;
            if packets.iter().any(|(h, _)| *h == ph) {
                for h in refs {
                    expected_refs.insert((ph.clone(), h));
                }
            }
        }
        let actual_refs: BTreeSet<(Vec<u8>, String)> = self
            .conn
            .prepare("SELECT packet_hash,hash FROM _lfs_attachment_refs")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<std::result::Result<_, _>>()?;
        if actual_refs != expected_refs {
            return Err(Error::Invalid("backup attachment reference metadata"));
        }
        let broken:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM _lfs_delivery d LEFT JOIN _lfs_packets p ON d.hash=p.hash LEFT JOIN _lfs_outgoing_wire w ON d.hash=w.payload_hash WHERE p.hash IS NULL OR w.payload_hash IS NULL OR d.dataset!=p.dataset OR d.device!=p.device OR d.incarnation!=p.incarnation OR d.seq!=p.seq)",[],|r|r.get(0))?;
        if broken {
            return Err(Error::Invalid("backup delivery"));
        }
        Ok(())
    }
}
impl Sdk {
    pub fn backup(
        &self,
        destination: impl AsRef<Path>,
        kind: BackupKind,
        o: BackupOptions,
        c: &Cancellation,
    ) -> Result<Manifest> {
        self.with(|s| s.backup(destination.as_ref(), kind, o, c))
    }
    /// Destination is a NEW directory. On validation failure, only this newly created directory is removed.
    pub fn restore(
        backup: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        c: &Cancellation,
    ) -> Result<Self> {
        let backup = backup.as_ref();
        let destination = destination.as_ref();
        let db = destination.join("store.db");
        validate_file_path(&db)?;
        safe(backup, false)?;
        safe(destination, true)?;
        c.check().map_err(Error::Network)?;
        let manifest_path = backup.join("manifest.json");
        safe(&manifest_path, false)?;
        if std::fs::metadata(&manifest_path)?.len() > 4 * 1024 * 1024 {
            return Err(Error::Invalid("manifest limit"));
        }
        let m: Manifest = serde_json::from_slice(&std::fs::read(manifest_path)?)
            .map_err(|_| Error::Invalid("manifest"))?;
        if m.format != 1
            || m.protocol != 1
            || m.internal != [2, 1, 1, 1]
            || m.files.is_empty()
            || m.files.windows(2).any(|w| w[0].path >= w[1].path)
        {
            return Err(Error::Invalid("backup version or file order"));
        }
        m.schema.validate()?;
        if m.files.iter().filter(|e| e.path == "store.db").count() != 1 {
            return Err(Error::Invalid("backup database"));
        }
        for e in &m.files {
            if e.path != "store.db" {
                attachments::digest(
                    e.path
                        .strip_prefix("objects/")
                        .ok_or(Error::Invalid("backup path"))?,
                )?;
            }
            attachments::digest(&e.sha256)?;
            let p = backup.join(&e.path);
            safe(&p, false)?;
            let a = verify(&File::open(p)?, 1 << 40, c)?;
            if a.hash != e.sha256 || a.size.to_string() != e.size {
                return Err(Error::Invalid("backup file integrity"));
            }
        }
        std::fs::create_dir(destination)?;
        let result = (|| {
            copy_file(&backup.join("store.db"), &db, c)?;
            let root = attachments::root_for(&db);
            std::fs::create_dir(&root)?;
            std::fs::create_dir(root.join("objects"))?;
            for e in &m.files {
                if let Some(h) = e.path.strip_prefix("objects/") {
                    copy_file(&backup.join(&e.path), &root.join("objects").join(h), c)?;
                }
            }
            let inspect =
                Connection::open_with_flags(&db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            let required:Vec<String>=inspect.prepare("SELECT hash FROM _lfs_attachment_refs UNION SELECT hash FROM _lfs_files WHERE state IN ('Available','Installed')")?.query_map([],|r|r.get(0))?.collect::<std::result::Result<_,_>>()?;
            for h in required {
                if !m
                    .files
                    .iter()
                    .any(|e| e.path == format!("objects/{h}") && e.sha256 == h)
                {
                    return Err(Error::Invalid("backup missing referenced file"));
                }
            }
            let metadata: Vec<(String, i64, String)> = inspect
                .prepare("SELECT hash,size,state FROM _lfs_files")?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                .collect::<std::result::Result<_, _>>()?;
            for (h, size, state) in metadata {
                attachments::digest(&h)?;
                if !matches!(state.as_str(), "Missing" | "Available") {
                    return Err(Error::Invalid("backup incomplete attachment state"));
                }
                if state == "Available"
                    && !m.files.iter().any(|e| {
                        e.path == format!("objects/{h}") && size >= 0 && e.size == size.to_string()
                    })
                {
                    return Err(Error::Invalid("backup attachment size metadata"));
                }
            }
            drop(inspect);
            let id = Identity {
                dataset: m.identity[0].clone(),
                device: m.identity[1].clone(),
                incarnation: m.identity[2].clone(),
            };
            let mut store = Store::open(&db, id, m.schema, Baseline::Reject)?;
            store.validate_snapshot()?;
            let closure:Vec<String>=store.conn.prepare("SELECT hash FROM _lfs_attachment_refs UNION SELECT hash FROM _lfs_files WHERE state='Available'")?.query_map([],|r|r.get(0))?.collect::<std::result::Result<_,_>>()?;
            for h in closure {
                if !m.files.iter().any(|e| e.path == format!("objects/{h}")) {
                    return Err(Error::Invalid("backup missing referenced file"));
                }
                store.open_attachment(&h, c)?;
            }
            if m.kind == BackupKind::SharedBootstrap {
                let s = store.status()?;
                if s.pending_outbox != 0 || !s.healthy() {
                    return Err(Error::Invalid("shared backup unpublished or blocked"));
                }
            }
            c.check().map_err(Error::Network)?;
            let new_id = Identity {
                incarnation: nonce()?,
                ..store.identity.clone()
            };
            store.conn.execute(
                "UPDATE _lfs_state SET incarnation=?1,next_seq=1,predecessor=NULL",
                [&new_id.incarnation],
            )?;
            let schema = store.schema.clone();
            drop(store);
            File::open(&db)?.sync_all()?;
            sync_dir(&root.join("objects"))?;
            sync_dir(&root)?;
            sync_dir(destination)?;
            if let Some(p) = destination.parent() {
                sync_dir(p)?;
            }
            Sdk::open(db, new_id, schema, Baseline::Reject)
        })();
        if result.is_err() {
            let _ = std::fs::remove_dir_all(destination);
        }
        result
    }
}
