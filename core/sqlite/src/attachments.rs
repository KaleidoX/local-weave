//! Owned immutable files. Hosts must exclude external writers and concurrent symlink replacement.
use crate::*;
use rusqlite::OptionalExtension;
use sha2::{Digest, Sha256};
use std::{
    io::{Read as IoRead, Seek, SeekFrom, Write as IoWrite},
    path::{Component, PathBuf},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachmentBarrier {
    StagedDurable,
    InstalledDurable,
    InstalledMetadata,
    AvailableMetadata,
}
pub const CHUNK: usize = 64 * 1024;
pub const DEFAULT_LIMIT: u64 = 256 * 1024 * 1024;
pub(crate) const INTERNAL5: &[(&str, &str)] = &[
    (
        "_lfs_attachment_version",
        "CREATE TABLE _lfs_attachment_version (version INTEGER PRIMARY KEY CHECK(version=1)) STRICT",
    ),
    (
        "_lfs_files",
        "CREATE TABLE _lfs_files (hash TEXT PRIMARY KEY NOT NULL, size INTEGER NOT NULL, state TEXT NOT NULL) STRICT",
    ),
    (
        "_lfs_staging",
        "CREATE TABLE _lfs_staging (token TEXT PRIMARY KEY NOT NULL, hash TEXT, size INTEGER) STRICT",
    ),
    (
        "_lfs_attachment_refs",
        "CREATE TABLE _lfs_attachment_refs (packet_hash BLOB NOT NULL, hash TEXT NOT NULL, PRIMARY KEY(packet_hash,hash)) STRICT",
    ),
    (
        "_lfs_refsets",
        "CREATE TABLE _lfs_refsets (packet_hash BLOB PRIMARY KEY NOT NULL, refs TEXT NOT NULL) STRICT",
    ),
];
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Attachment {
    pub hash: String,
    pub size: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttachmentStatus {
    pub hash: String,
    pub size: Option<u64>,
    pub state: String,
}
pub fn digest(s: &str) -> Result<[u8; 32]> {
    if s.len() != 64
        || !s
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return Err(Error::Invalid("attachment hash"));
    }
    let mut out = [0; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|_| Error::Invalid("attachment hash"))?;
    }
    Ok(out)
}
pub(crate) fn validate_refs(refs: &[String]) -> Result<()> {
    if refs.len() > 256 {
        return Err(Error::Invalid("attachment references"));
    }
    for r in refs {
        digest(r)?;
    }
    if refs.windows(2).any(|w| w[0] >= w[1]) {
        return Err(Error::Invalid("attachment reference order"));
    }
    Ok(())
}
pub(crate) fn root_for(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".attachments");
    PathBuf::from(name)
}
pub(crate) fn nonce() -> Result<String> {
    let mut b = [0; 32];
    getrandom::fill(&mut b).map_err(|_| Error::Invalid("random source"))?;
    Ok(hash_hex(&b))
}
pub(crate) fn safe(path: &Path, missing: bool) -> Result<()> {
    let mut p = PathBuf::new();
    for c in path.components() {
        if matches!(c, Component::ParentDir) {
            return Err(Error::Invalid("unsafe path"));
        }
        p.push(c);
        match std::fs::symlink_metadata(&p) {
            Ok(m) if m.file_type().is_symlink() => return Err(Error::Invalid("symlink")),
            Ok(m) if !m.is_file() && !m.is_dir() => return Err(Error::Invalid("special file")),
            Ok(_) => (),
            Err(e) if missing && e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
pub(crate) fn sync_dir(p: &Path) -> Result<()> {
    File::open(p)?.sync_all()?;
    Ok(())
}
pub(crate) fn verify(mut f: &File, limit: u64, c: &Cancellation) -> Result<Attachment> {
    f.seek(SeekFrom::Start(0))?;
    let mut h = Sha256::new();
    let mut size = 0u64;
    let mut buf = vec![0; CHUNK];
    loop {
        c.check().map_err(Error::Network)?;
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        size += n as u64;
        if size > limit {
            return Err(Error::Invalid("attachment limit"));
        }
        h.update(&buf[..n]);
    }
    f.seek(SeekFrom::Start(0))?;
    Ok(Attachment {
        hash: hash_hex(&h.finalize().into()),
        size,
    })
}
pub(crate) struct Staging {
    pub token: String,
    pub file: File,
}
impl Store {
    pub(crate) fn initialize_attachments(&self) -> Result<()> {
        let exists: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='_lfs_attachment_version')",
            [],
            |r| r.get(0),
        )?;
        if exists {
            let version: i64 =
                self.conn
                    .query_row("SELECT version FROM _lfs_attachment_version", [], |r| {
                        r.get(0)
                    })?;
            if version != 1 {
                return Err(Error::Schema("attachment metadata version"));
            }
            for (name, _) in INTERNAL5 {
                let found: bool = self.conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name=?1)",
                    [name],
                    |r| r.get(0),
                )?;
                if !found {
                    return Err(Error::Schema("incomplete attachment metadata"));
                }
            }
        }
        for (_, sql) in INTERNAL5 {
            self.conn.execute_batch(&sql.replacen(
                "CREATE TABLE",
                "CREATE TABLE IF NOT EXISTS",
                1,
            ))?;
        }
        self.conn.execute(
            "INSERT OR IGNORE INTO _lfs_attachment_version VALUES (1)",
            [],
        )?;
        safe(&self.attachment_root, true)?;
        for d in ["", "staging", "objects", "quarantine"] {
            let p = self.attachment_root.join(d);
            safe(&p, true)?;
            std::fs::create_dir_all(&p)?;
            sync_dir(&p)?;
        }
        if let Some(p) = self
            .attachment_root
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
        {
            sync_dir(p)?;
        }
        Ok(())
    }
    fn object(&self, h: &str) -> Result<PathBuf> {
        digest(h)?;
        let p = self.attachment_root.join("objects").join(h);
        safe(&p, true)?;
        Ok(p)
    }
    pub(crate) fn begin_attachment(&mut self) -> Result<Staging> {
        self.check()?;
        let token = nonce()?;
        let path = self.attachment_root.join("staging").join(&token);
        safe(&path, true)?;
        self.conn
            .execute("INSERT INTO _lfs_staging VALUES (?1,NULL,NULL)", [&token])?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        sync_dir(&self.attachment_root.join("staging"))?;
        Ok(Staging { token, file })
    }
    pub(crate) fn install_attachment(
        &mut self,
        staging: Staging,
        expected: Option<&str>,
        limit: u64,
        c: &Cancellation,
    ) -> Result<Attachment> {
        self.install_attachment_observed(staging, expected, limit, c, &|_| {})
    }
    fn install_attachment_observed(
        &mut self,
        staging: Staging,
        expected: Option<&str>,
        limit: u64,
        c: &Cancellation,
        observer: &impl Fn(AttachmentBarrier),
    ) -> Result<Attachment> {
        self.check()?;
        let a = verify(&staging.file, limit, c)?;
        if expected.is_some_and(|v| v != a.hash) {
            return Err(Error::Invalid("attachment hash mismatch"));
        }
        staging.file.sync_all()?;
        self.conn.execute(
            "UPDATE _lfs_staging SET hash=?2,size=?3 WHERE token=?1",
            params![staging.token, a.hash, a.size as i64],
        )?;
        observer(AttachmentBarrier::StagedDurable);
        let source = self.attachment_root.join("staging").join(&staging.token);
        safe(&source, false)?;
        let final_path = self.object(&a.hash)?;
        match std::fs::hard_link(&source, &final_path) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                if verify(&File::open(&final_path)?, limit, c)? != a {
                    return Err(Error::Invalid("existing content corruption"));
                }
            }
            Err(e) => return Err(e.into()),
        }
        sync_dir(&self.attachment_root.join("objects"))?;
        observer(AttachmentBarrier::InstalledDurable);
        self.conn.execute("INSERT INTO _lfs_files VALUES (?1,?2,'Installed') ON CONFLICT(hash) DO UPDATE SET size=excluded.size,state='Installed'",params![a.hash,a.size as i64])?;
        observer(AttachmentBarrier::InstalledMetadata);
        c.check().map_err(Error::Network)?;
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result: Result<()> = (|| {
            self.conn.execute(
                "UPDATE _lfs_files SET state='Available' WHERE hash=?1",
                [&a.hash],
            )?;
            self.conn
                .execute("DELETE FROM _lfs_staging WHERE token=?1", [&staging.token])?;
            self.conn.execute_batch("COMMIT")?;
            Ok(())
        })();
        if result.is_err() {
            self.rollback();
        }
        result?;
        observer(AttachmentBarrier::AvailableMetadata);
        std::fs::remove_file(source)?;
        sync_dir(&self.attachment_root.join("staging"))?;
        Ok(a)
    }
    pub fn add_attachment(
        &mut self,
        input: impl IoRead,
        limit: u64,
        c: &Cancellation,
    ) -> Result<Attachment> {
        self.add_attachment_observed(input, limit, c, &|_| {})
    }
    /// Observer runs outside SQLite transactions at durable boundaries. Must not reenter the SDK.
    pub fn add_attachment_observed(
        &mut self,
        mut input: impl IoRead,
        limit: u64,
        c: &Cancellation,
        observer: &impl Fn(AttachmentBarrier),
    ) -> Result<Attachment> {
        check_limit(limit)?;
        let mut staging = self.begin_attachment()?;
        let mut buf = vec![0; CHUNK];
        let mut total = 0;
        loop {
            c.check().map_err(Error::Network)?;
            let n = input.read(&mut buf)?;
            if n == 0 {
                break;
            }
            total += n as u64;
            if total > limit {
                return Err(Error::Invalid("attachment limit"));
            }
            staging.file.write_all(&buf[..n])?;
        }
        self.install_attachment_observed(staging, None, limit, c, observer)
    }
    pub fn open_attachment(&mut self, h: &str, c: &Cancellation) -> Result<File> {
        self.check()?;
        let p = self.object(h)?;
        let state: Option<String> = self
            .conn
            .query_row("SELECT state FROM _lfs_files WHERE hash=?1", [h], |r| {
                r.get(0)
            })
            .optional()?;
        if state.as_deref() != Some("Available") {
            return Err(Error::Invalid("attachment unavailable"));
        }
        let result = (|| {
            let f = File::open(p)?;
            if verify(&f, 1 << 40, c)?.hash != h {
                return Err(Error::Invalid("attachment hash mismatch"));
            }
            Ok(f)
        })();
        if result.is_err() {
            self.conn
                .execute("UPDATE _lfs_files SET state='Missing' WHERE hash=?1", [h])?;
        }
        result
    }
    pub fn attachment_status(&mut self, h: &str) -> Result<AttachmentStatus> {
        self.check()?;
        digest(h)?;
        let row: Option<(i64, String)> = self
            .conn
            .query_row(
                "SELECT size,state FROM _lfs_files WHERE hash=?1",
                [h],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let (size, state) = row.unwrap_or((-1, "Missing".into()));
        Ok(AttachmentStatus {
            hash: h.into(),
            size: (size >= 0).then_some(size as u64),
            state,
        })
    }
    pub fn write_transaction_with_attachments<T>(
        &mut self,
        refs: &[String],
        f: impl FnOnce(&Write<'_>) -> Result<T>,
    ) -> Result<(T, Option<Commit>)> {
        validate_refs(refs)?;
        for r in refs {
            self.open_attachment(r, &Cancellation::default())?;
        }
        self.local_refs = refs.to_vec();
        let result = catch_unwind(AssertUnwindSafe(|| self.write_transaction(f)));
        self.local_refs.clear();
        match result {
            Ok(v) => v,
            Err(p) => resume_unwind(p),
        }
    }
    /// References retained by all causal versions of this record, including conflicts and tombstones.
    pub fn record_attachment_references(&mut self, table: u32, key: &str) -> Result<Vec<String>> {
        self.check()?;
        Ok(self.conn.prepare("SELECT DISTINCT r.hash FROM _lfs_attachment_refs r JOIN _lfs_nodes n ON n.hash=r.packet_hash WHERE n.table_id=?1 AND n.record_key=?2 ORDER BY r.hash")?.query_map(params![table,key],|r|r.get(0))?.collect::<std::result::Result<_,_>>()?)
    }
    pub fn resolve_with_attachments(
        &mut self,
        table: u32,
        key: &str,
        observed: Vec<OpId>,
        row: Option<Vec<Value>>,
        refs: &[String],
    ) -> Result<Commit> {
        validate_refs(refs)?;
        for r in refs {
            self.open_attachment(r, &Cancellation::default())?;
        }
        self.local_refs = refs.to_vec();
        let result = catch_unwind(AssertUnwindSafe(|| self.resolve(table, key, observed, row)));
        self.local_refs.clear();
        match result {
            Ok(v) => v,
            Err(p) => resume_unwind(p),
        }
    }
    pub(crate) fn persist_references(&self, h: [u8; 32], refs: &[String]) -> Result<()> {
        for r in refs {
            self.conn.execute(
                "INSERT OR IGNORE INTO _lfs_files VALUES (?1,-1,'Missing')",
                [r],
            )?;
            self.conn.execute(
                "INSERT OR IGNORE INTO _lfs_attachment_refs VALUES (?1,?2)",
                params![h.as_slice(), r],
            )?;
        }
        Ok(())
    }
    pub(crate) fn retain_refset(&self, p: &Package) -> Result<()> {
        let refs = p.attachment_references.join("\n");
        let old: Option<String> = self
            .conn
            .query_row(
                "SELECT refs FROM _lfs_refsets WHERE packet_hash=?1",
                [p.key.hash.as_slice()],
                |r| r.get(0),
            )
            .optional()?;
        if old.is_some_and(|o| o != refs) {
            self.stop(&p.key)?;
            return Err(Error::StreamFork);
        }
        let outgoing: Option<Vec<u8>> = self
            .conn
            .query_row(
                "SELECT bytes FROM _lfs_outgoing_wire WHERE payload_hash=?1",
                [p.key.hash.as_slice()],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(b) = outgoing
            && Package::decode_envelope(&b)?.attachment_references != p.attachment_references
        {
            self.stop(&p.key)?;
            return Err(Error::StreamFork);
        }
        self.conn.execute(
            "INSERT OR IGNORE INTO _lfs_refsets VALUES (?1,?2)",
            params![p.key.hash.as_slice(), refs],
        )?;
        Ok(())
    }
    pub(crate) fn apply_refset(&self, h: [u8; 32]) -> Result<()> {
        let refs: Option<String> = self
            .conn
            .query_row(
                "SELECT refs FROM _lfs_refsets WHERE packet_hash=?1",
                [h.as_slice()],
                |r| r.get(0),
            )
            .optional()?;
        self.persist_references(
            h,
            &refs
                .unwrap_or_default()
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>(),
        )
    }
    fn quarantine(&self, p: &Path) -> Result<()> {
        safe(p, false)?;
        std::fs::rename(p, self.attachment_root.join("quarantine").join(nonce()?))?;
        sync_dir(&self.attachment_root.join("quarantine"))?;
        Ok(())
    }
    pub fn reconcile_attachments(&mut self) -> Result<()> {
        self.check()?;
        let entries: Vec<(String, Option<String>)> = self
            .conn
            .prepare("SELECT token,hash FROM _lfs_staging")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<std::result::Result<_, _>>()?;
        for (token, h) in entries {
            digest(&token)?;
            if let Some(h) = h {
                let p = self.object(&h)?;
                if p.exists() {
                    let a = verify(&File::open(&p)?, 1 << 40, &Cancellation::default())?;
                    if a.hash == h {
                        self.conn.execute("INSERT INTO _lfs_files VALUES (?1,?2,'Available') ON CONFLICT(hash) DO UPDATE SET state='Available',size=excluded.size",params![h,a.size as i64])?;
                    }
                }
            }
            let p = self.attachment_root.join("staging").join(&token);
            if p.exists() {
                self.quarantine(&p)?;
            }
            self.conn
                .execute("DELETE FROM _lfs_staging WHERE token=?1", [token])?;
        }
        for entry in std::fs::read_dir(self.attachment_root.join("staging"))? {
            let p = entry?.path();
            safe(&p, false)?;
            self.quarantine(&p)?;
        }
        for entry in std::fs::read_dir(self.attachment_root.join("objects"))? {
            let p = entry?.path();
            safe(&p, false)?;
            let h = p
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or(Error::Invalid("owned filename"))?;
            let tracked = digest(h).is_ok()
                && self.conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM _lfs_files WHERE hash=?1)",
                    [h],
                    |r| r.get::<_, bool>(0),
                )?;
            if !tracked {
                self.quarantine(&p)?;
                continue;
            }
            let a = verify(&File::open(&p)?, 1 << 40, &Cancellation::default())?;
            if a.hash != h {
                self.quarantine(&p)?;
                self.conn
                    .execute("UPDATE _lfs_files SET state='Missing' WHERE hash=?1", [h])?;
            } else {
                self.conn.execute(
                    "UPDATE _lfs_files SET state='Available',size=?2 WHERE hash=?1",
                    params![h, a.size as i64],
                )?;
            }
        }
        let hashes: Vec<String> = self
            .conn
            .prepare("SELECT hash FROM _lfs_files WHERE state IN ('Available','Installed')")?
            .query_map([], |r| r.get(0))?
            .collect::<std::result::Result<_, _>>()?;
        for h in hashes {
            if !self.object(&h)?.exists() {
                self.conn
                    .execute("UPDATE _lfs_files SET state='Missing' WHERE hash=?1", [h])?;
            }
        }
        sync_dir(&self.attachment_root.join("objects"))?;
        sync_dir(&self.attachment_root.join("staging"))?;
        Ok(())
    }
}
pub fn check_limit(limit: u64) -> Result<()> {
    if limit == 0 || limit > 1 << 40 {
        Err(Error::Invalid("attachment limit"))
    } else {
        Ok(())
    }
}
