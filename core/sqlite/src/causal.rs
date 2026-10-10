use crate::*;
use lfs_protocol::MAX_PACKET;
use rusqlite::{OptionalExtension, params_from_iter};

pub(crate) const INTERNAL2: &[(&str, &str)] = &[
    (
        "_lfs_variants",
        "CREATE TABLE _lfs_variants (dataset TEXT NOT NULL, device TEXT NOT NULL, incarnation TEXT NOT NULL, seq INTEGER NOT NULL, claimed_hash BLOB NOT NULL, content_hash BLOB NOT NULL, chunk INTEGER NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY(dataset,device,incarnation,seq,claimed_hash,content_hash,chunk)) STRICT",
    ),
    (
        "_lfs_cursors",
        "CREATE TABLE _lfs_cursors (singleton INTEGER PRIMARY KEY CHECK(singleton=1), scan INTEGER NOT NULL, revisit INTEGER NOT NULL, degraded INTEGER NOT NULL) STRICT",
    ),
    (
        "_lfs_rejected",
        "CREATE TABLE _lfs_rejected (dataset TEXT NOT NULL, device TEXT NOT NULL, incarnation TEXT NOT NULL, seq INTEGER NOT NULL, hash BLOB NOT NULL, chunk INTEGER NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY(dataset,device,incarnation,seq,hash,chunk)) STRICT",
    ),
    (
        "_lfs_meta",
        "CREATE TABLE _lfs_meta (version INTEGER PRIMARY KEY CHECK(version=2)) STRICT",
    ),
    (
        "_lfs_packets",
        "CREATE TABLE _lfs_packets (hash BLOB PRIMARY KEY NOT NULL, dataset TEXT NOT NULL, device TEXT NOT NULL, incarnation TEXT NOT NULL, seq INTEGER NOT NULL, bytes BLOB NOT NULL, UNIQUE(dataset,device,incarnation,seq)) STRICT",
    ),
    (
        "_lfs_nodes",
        "CREATE TABLE _lfs_nodes (id BLOB PRIMARY KEY NOT NULL, table_id INTEGER NOT NULL, record_key TEXT NOT NULL, hash BLOB NOT NULL, op_index INTEGER NOT NULL) STRICT",
    ),
    (
        "_lfs_edges",
        "CREATE TABLE _lfs_edges (child BLOB NOT NULL, parent BLOB NOT NULL, PRIMARY KEY(child,parent)) STRICT",
    ),
    (
        "_lfs_tips",
        "CREATE TABLE _lfs_tips (table_id INTEGER NOT NULL, record_key TEXT NOT NULL, id BLOB NOT NULL, PRIMARY KEY(table_id,record_key,id)) STRICT",
    ),
    (
        "_lfs_delivery",
        "CREATE TABLE _lfs_delivery (dataset TEXT NOT NULL, device TEXT NOT NULL, incarnation TEXT NOT NULL, seq INTEGER NOT NULL, hash BLOB NOT NULL, acknowledged INTEGER NOT NULL, PRIMARY KEY(dataset,device,incarnation,seq,hash)) STRICT",
    ),
    (
        "_lfs_inbox",
        "CREATE TABLE _lfs_inbox (dataset TEXT NOT NULL, device TEXT NOT NULL, incarnation TEXT NOT NULL, seq INTEGER NOT NULL, hash BLOB NOT NULL, bytes BLOB NOT NULL, fingerprint TEXT NOT NULL, attachments INTEGER NOT NULL, status TEXT NOT NULL, error TEXT, PRIMARY KEY(dataset,device,incarnation,seq,hash)) STRICT",
    ),
    (
        "_lfs_stopped",
        "CREATE TABLE _lfs_stopped (dataset TEXT NOT NULL, device TEXT NOT NULL, incarnation TEXT NOT NULL, reason TEXT NOT NULL, PRIMARY KEY(dataset,device,incarnation)) STRICT",
    ),
];
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PacketKey {
    pub identity: Identity,
    pub seq: u64,
    pub hash: [u8; 32],
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Package {
    pub key: PacketKey,
    pub fingerprint: String,
    pub bytes: Vec<u8>,
    pub attachment_references: Vec<String>,
}
impl Package {
    pub fn from_pending(p: PendingPacket, schema: &Schema) -> Self {
        Self {
            key: PacketKey {
                identity: p.identity,
                seq: p.seq,
                hash: p.hash,
            },
            bytes: p.bytes,
            fingerprint: schema.fingerprint(),
            attachment_references: vec![],
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InboxState {
    Downloaded,
    Validated,
    WaitingDependencies,
    Applied,
    Failed,
    Retryable,
    Blocked,
}
impl InboxState {
    fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "Downloaded" => Self::Downloaded,
            "Validated" => Self::Validated,
            "WaitingDependencies" => Self::WaitingDependencies,
            "Applied" => Self::Applied,
            "Failed" => Self::Failed,
            "Retryable" => Self::Retryable,
            "Blocked" => Self::Blocked,
            _ => return Err(Error::Invalid("inbox state")),
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboxEntry {
    pub key: PacketKey,
    pub state: InboxState,
    pub error: Option<String>,
}
#[derive(Clone, Debug)]
pub struct Status {
    pub pending_outbox: usize,
    pub inbox: Vec<InboxEntry>,
    pub stopped_streams: usize,
    pub blocked_streams: Vec<Identity>,
    pub last_sync_error: Option<String>,
    pub discovery_incomplete: bool,
    pub outstanding_objects: usize,
}
impl Status {
    pub fn healthy(&self) -> bool {
        self.outstanding_objects == 0
            && !self.discovery_incomplete
            && self.last_sync_error.is_none()
            && self.stopped_streams == 0
            && self.inbox.iter().all(|e| e.state == InboxState::Applied)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Conflict {
    pub table: u32,
    pub key: String,
    pub heads: Vec<Operation>,
}
impl Store {
    pub(crate) fn initialize_causal(&mut self, tracked: bool) -> Result<()> {
        let exists: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='_lfs_meta')",
            [],
            |r| r.get(0),
        )?;
        if exists {
            let v: i64 = self
                .conn
                .query_row("SELECT version FROM _lfs_meta", [], |r| r.get(0))?;
            if v != 2 {
                return Err(Error::Schema("metadata version"));
            }
            for (n, _) in INTERNAL2 {
                let yes: bool = self.conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name=?1)",
                    [n],
                    |r| r.get(0),
                )?;
                if !yes {
                    return Err(Error::Schema("incomplete metadata v2"));
                }
            }
            return Ok(());
        }
        for (_, sql) in INTERNAL2 {
            self.conn.execute_batch(sql)?;
        }
        self.conn.execute("INSERT INTO _lfs_meta VALUES (2)", [])?;
        self.conn
            .execute("INSERT INTO _lfs_cursors VALUES (1,0,0,0)", [])?;
        if tracked {
            let old: Vec<(Vec<u8>, Vec<u8>, i64)> = self
                .conn
                .prepare("SELECT bytes,hash,acknowledged FROM _lfs_outbox ORDER BY seq")?
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                .collect::<std::result::Result<_, _>>()?;
            for (b, h, ack) in old {
                let p = Packet::decode(&b).map_err(Error::Invalid)?;
                if hash(&b).as_slice() != h {
                    return Err(Error::Invalid("legacy outbox hash"));
                }
                let digest = hash(&b);
                self.validate_packet(&p)?;
                self.store_packet(&p, &b, digest)?;
                self.conn.execute(
                    "INSERT INTO _lfs_delivery VALUES (?1,?2,?3,?4,?5,?6)",
                    params![
                        p.identity.dataset,
                        p.identity.device,
                        p.identity.incarnation,
                        p.seq as i64,
                        digest.as_slice(),
                        ack
                    ],
                )?;
            }
        }
        Ok(())
    }
    pub(crate) fn validate_packet(&self, p: &Packet) -> Result<()> {
        if p.identity.dataset != self.identity.dataset {
            return Err(Error::IdentityMismatch);
        }
        if p.schema != self.schema.version {
            return Err(Error::Schema("schema version"));
        }
        for op in &p.operations {
            for parent in &op.parents {
                let id = OpId::decode(parent).map_err(Error::Invalid)?;
                if id.schema != self.schema.version {
                    return Err(Error::Schema("parent schema"));
                }
                if id.device == p.identity.device
                    && id.incarnation == p.identity.incarnation
                    && id.seq >= p.seq
                {
                    return Err(Error::Invalid("parent stream order"));
                }
            }
            self.validate_row(op.table, &op.key, op.row.as_deref())?;
        }
        Ok(())
    }
    fn validate_row(&self, table: u32, key: &str, row: Option<&[Value]>) -> Result<()> {
        let t = self
            .schema
            .tables
            .iter()
            .find(|t| t.id == table)
            .ok_or(Error::Schema("table whitelist"))?;
        if let Some(row) = row {
            if row.len() != t.columns.len() || row.first() != Some(&Value::Text(key.to_owned())) {
                return Err(Error::Schema("column count or row key"));
            }
            for (v, c) in row.iter().zip(&t.columns) {
                v.validate().map_err(Error::Invalid)?;
                if !matches!((v,c.kind),(Value::Null,_) if c.nullable)
                    && !matches!(
                        (v, c.kind),
                        (Value::Text(_), ColumnType::Text)
                            | (Value::Integer(_), ColumnType::Integer)
                            | (Value::Real(_), ColumnType::Real)
                            | (Value::Blob(_), ColumnType::Blob)
                    )
                {
                    return Err(Error::Schema("column type/nonnull constraint"));
                }
            }
        }
        Ok(())
    }
    pub(crate) fn record_heads(&self, table: u32, key: &str) -> Result<Vec<Operation>> {
        let rows:Vec<(Vec<u8>,u32)>=self.conn.prepare("SELECT p.bytes,n.op_index FROM _lfs_tips t JOIN _lfs_nodes n ON t.id=n.id JOIN _lfs_packets p ON n.hash=p.hash WHERE t.table_id=?1 AND t.record_key=?2")?.query_map(params![table,key],|r|Ok((r.get(0)?,r.get(1)?)))?.collect::<std::result::Result<_,_>>()?;
        let mut out = Vec::new();
        for (b, i) in rows {
            out.push(
                Packet::decode(&b)
                    .map_err(Error::Invalid)?
                    .operations
                    .get(i as usize)
                    .ok_or(Error::Invalid("node index"))?
                    .clone(),
            );
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }
    pub fn heads(&mut self, table: u32, key: &str) -> Result<Vec<Operation>> {
        self.check()?;
        self.record_heads(table, key)
    }
    pub fn list_conflicts(&mut self) -> Result<Vec<Conflict>> {
        self.check()?;
        let keys:Vec<(u32,String)>=self.conn.prepare("SELECT table_id,record_key FROM _lfs_tips GROUP BY table_id,record_key HAVING count(*)>1 ORDER BY table_id,record_key")?.query_map([],|r|Ok((r.get(0)?,r.get(1)?)))?.collect::<std::result::Result<_,_>>()?;
        keys.into_iter()
            .map(|(table, key)| {
                Ok(Conflict {
                    heads: self.record_heads(table, &key)?,
                    table,
                    key,
                })
            })
            .collect()
    }
    fn parent_scope(&self, op: &Operation) -> Result<()> {
        let mut missing = false;
        for id in &op.parents {
            let scope: Option<(u32, String)> = self
                .conn
                .query_row(
                    "SELECT table_id,record_key FROM _lfs_nodes WHERE id=?1",
                    [id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            match scope {
                None => missing = true,
                Some((t, k)) if t != op.table || k != op.key => {
                    return Err(Error::Invalid("parent record scope"));
                }
                _ => {}
            }
        }
        if missing {
            Err(Error::MissingDependencies)
        } else {
            Ok(())
        }
    }
    pub(crate) fn store_packet(&self, p: &Packet, bytes: &[u8], digest: [u8; 32]) -> Result<()> {
        self.conn.execute(
            "INSERT INTO _lfs_packets VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                digest.as_slice(),
                p.identity.dataset,
                p.identity.device,
                p.identity.incarnation,
                p.seq as i64,
                bytes
            ],
        )?;
        for op in &p.operations {
            self.parent_scope(op)?;
            self.conn.execute(
                "INSERT INTO _lfs_nodes VALUES (?1,?2,?3,?4,?5)",
                params![
                    op.id.encode(),
                    op.table,
                    op.key,
                    digest.as_slice(),
                    op.id.index
                ],
            )?;
            for parent in &op.parents {
                self.conn.execute(
                    "INSERT INTO _lfs_edges VALUES (?1,?2)",
                    params![op.id.encode(), parent],
                )?;
                self.conn
                    .execute("DELETE FROM _lfs_tips WHERE id=?1", [parent])?;
            }
            self.conn.execute(
                "INSERT INTO _lfs_tips VALUES (?1,?2,?3)",
                params![op.table, op.key, op.id.encode()],
            )?;
            let winner =
                lfs_protocol::Register::select_projection(self.record_heads(op.table, &op.key)?)
                    .ok_or(Error::Invalid("missing head"))?;
            self.project(op.table, &op.key, winner.row.as_deref())?;
        }
        Ok(())
    }
    fn project(&self, table: u32, key: &str, row: Option<&[Value]>) -> Result<()> {
        self.validate_row(table, key, row)?;
        let t = self.schema.tables.iter().find(|t| t.id == table).unwrap();
        self.conn.execute(
            &format!(
                "DELETE FROM \"{}\" WHERE \"{}\"=?1",
                t.name, t.columns[0].name
            ),
            [key],
        )?;
        if let Some(row) = row {
            let placeholders = vec!["?"; row.len()].join(",");
            self.conn.execute(
                &format!("INSERT INTO \"{}\" VALUES ({placeholders})", t.name),
                params_from_iter(access::sql_values(row)?),
            )?;
        }
        Ok(())
    }
    pub fn resolve(
        &mut self,
        table: u32,
        key: &str,
        observed: Vec<OpId>,
        row: Option<Vec<Value>>,
    ) -> Result<Commit> {
        self.check()?;
        self.validate_row(table, key, row.as_deref())?;
        let mut parents: Vec<Vec<u8>> = observed.iter().map(OpId::encode).collect();
        parents.sort();
        if parents.is_empty() || parents.windows(2).any(|w| w[0] == w[1]) {
            return Err(Error::Invalid("observed parents"));
        }
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| {
            let (seq, prev): (i64, Option<Vec<u8>>) =
                self.conn
                    .query_row("SELECT next_seq,predecessor FROM _lfs_state", [], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })?;
            let op = Operation {
                id: OpId {
                    dataset: self.identity.dataset.clone(),
                    device: self.identity.device.clone(),
                    incarnation: self.identity.incarnation.clone(),
                    seq: seq as u64,
                    index: 0,
                    schema: self.schema.version,
                },
                table,
                key: key.to_owned(),
                parents,
                row,
            };
            self.parent_scope(&op)?;
            self.commit_local(Packet {
                identity: self.identity.clone(),
                seq: seq as u64,
                schema: self.schema.version,
                predecessor: prev
                    .map(|p| p.try_into().map_err(|_| Error::Invalid("predecessor")))
                    .transpose()?,
                operations: vec![op],
            })
        })();
        match result {
            Ok(c) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    self.rollback();
                    return Err(e.into());
                }
                Ok(c)
            }
            Err(e) => {
                self.rollback();
                Err(e)
            }
        }
    }
    fn parents_known(&self, p: &Packet) -> Result<bool> {
        for op in &p.operations {
            match self.parent_scope(op) {
                Ok(()) => {}
                Err(Error::MissingDependencies) => return Ok(false),
                Err(e) => return Err(e),
            }
        }
        Ok(true)
    }
    fn has_fork(&self, k: &PacketKey) -> Result<bool> {
        let applied:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM _lfs_packets WHERE dataset=?1 AND device=?2 AND incarnation=?3 AND seq=?4 AND hash!=?5)",params![k.identity.dataset,k.identity.device,k.identity.incarnation,k.seq as i64,k.hash.as_slice()],|r|r.get(0))?;
        if applied {
            return Ok(true);
        }
        let candidates:Vec<Vec<u8>>=self.conn.prepare("SELECT bytes FROM _lfs_inbox WHERE dataset=?1 AND device=?2 AND incarnation=?3 AND seq=?4 AND hash!=?5 AND status IN ('Validated','WaitingDependencies','Applied')")?.query_map(params![k.identity.dataset,k.identity.device,k.identity.incarnation,k.seq as i64,k.hash.as_slice()],|r|r.get(0))?.collect::<std::result::Result<_,_>>()?;
        for bytes in candidates {
            let p = Packet::decode(&bytes).map_err(Error::Invalid)?;
            match self.parents_known(&p) {
                Ok(true) => return Ok(true),
                Ok(false) | Err(Error::Invalid("parent record scope")) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(false)
    }
    pub(crate) fn stopped(&self, k: &PacketKey) -> Result<bool> {
        Ok(self.conn.query_row("SELECT EXISTS(SELECT 1 FROM _lfs_stopped WHERE dataset=?1 AND device=?2 AND incarnation=?3)",params![k.identity.dataset,k.identity.device,k.identity.incarnation],|r|r.get(0))?)
    }
    fn mark(&self, k: &PacketKey, state: &str, error: Option<&str>) -> Result<()> {
        self.conn.execute("UPDATE _lfs_inbox SET status=?6,error=?7 WHERE dataset=?1 AND device=?2 AND incarnation=?3 AND seq=?4 AND hash=?5",params![k.identity.dataset,k.identity.device,k.identity.incarnation,k.seq as i64,k.hash.as_slice(),state,error])?;
        Ok(())
    }
    pub(crate) fn stop(&self, k: &PacketKey) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO _lfs_stopped VALUES (?1,?2,?3,'stream fork')",
            params![
                k.identity.dataset,
                k.identity.device,
                k.identity.incarnation
            ],
        )?;
        self.conn.execute("UPDATE _lfs_inbox SET status='Blocked',error='stream fork' WHERE dataset=?1 AND device=?2 AND incarnation=?3 AND status!='Applied'",params![k.identity.dataset,k.identity.device,k.identity.incarnation])?;
        Ok(())
    }
    /// Accepted inputs are bounded to 16 MiB, including a quarantine allowance above the 8 MiB protocol limit.
    /// Larger host inputs are refused before custody; no decoder or SQL receives them.
    pub fn receive(&mut self, package: Package) -> Result<InboxState> {
        self.receive_cancellable(package, || Ok(()))
    }
    pub(crate) fn receive_cancellable(
        &mut self,
        package: Package,
        check: impl Fn() -> Result<()>,
    ) -> Result<InboxState> {
        check()?;
        self.check()?;
        let k = &package.key;
        k.identity.validate().map_err(Error::Invalid)?;
        if package.bytes.len() > 2 * MAX_PACKET
            || package.fingerprint.len() > MAX_FINGERPRINT
            || package.attachment_references.len() > 256
            || package.attachment_references.iter().any(|v| v.len() > 512)
            || k.seq == 0
            || k.seq >= i64::MAX as u64
        {
            return Err(Error::Invalid("package limit"));
        }
        let validation = self.validate_package(&package);
        if let Err(e) = validation {
            let retryable = matches!(e,Error::Invalid(reason) if !matches!(reason,"attachments unsupported"|"parent record scope"|"parent stream order"|"packet size"));
            self.retain_attempt(
                &ObjectKey {
                    packet: k.clone(),
                    envelope_hash: None,
                },
                &package.evidence_envelope(),
                false,
                retryable,
                &e.to_string(),
            )?;
            // A failed attempt cannot overwrite a previously accepted receipt.
            self.conn.execute(
                "INSERT OR IGNORE INTO _lfs_inbox VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?10,?9)",
                params![
                    k.identity.dataset,
                    k.identity.device,
                    k.identity.incarnation,
                    k.seq as i64,
                    k.hash.as_slice(),
                    if package.bytes.len() <= MAX_PACKET {
                        package.bytes.as_slice()
                    } else {
                        &[]
                    },
                    package.fingerprint,
                    package.attachment_references.len() as i64,
                    e.to_string(),
                    if retryable { "Retryable" } else { "Failed" }
                ],
            )?;
            if package.bytes.len() > MAX_PACKET {
                for (i, chunk) in package.bytes.chunks(MAX_PACKET).enumerate() {
                    self.conn.execute(
                        "INSERT OR IGNORE INTO _lfs_rejected VALUES (?1,?2,?3,?4,?5,?6,?7)",
                        params![
                            k.identity.dataset,
                            k.identity.device,
                            k.identity.incarnation,
                            k.seq as i64,
                            k.hash.as_slice(),
                            i as i64,
                            chunk
                        ],
                    )?;
                }
            }
            return Err(e);
        }
        if self.stopped(k)?
            || (self.parents_known(&Packet::decode(&package.bytes).map_err(Error::Invalid)?)?
                && self.has_fork(k)?)
        {
            self.retain_attempt(
                &ObjectKey {
                    packet: k.clone(),
                    envelope_hash: None,
                },
                &package.evidence_envelope(),
                false,
                false,
                "stream fork",
            )?;
            self.conn.execute("INSERT OR IGNORE INTO _lfs_inbox VALUES (?1,?2,?3,?4,?5,?6,?7,0,'Blocked','stream fork')",params![k.identity.dataset,k.identity.device,k.identity.incarnation,k.seq as i64,k.hash.as_slice(),package.bytes,package.fingerprint])?;
            self.stop(k)?;
            return Err(Error::StreamFork);
        }
        self.retain_refset(&package)?;
        self.conn.execute(
            "INSERT OR IGNORE INTO _lfs_inbox VALUES (?1,?2,?3,?4,?5,?6,?7,0,'Validated',NULL)",
            params![
                k.identity.dataset,
                k.identity.device,
                k.identity.incarnation,
                k.seq as i64,
                k.hash.as_slice(),
                package.bytes,
                package.fingerprint
            ],
        )?;
        self.conn.execute("UPDATE _lfs_inbox SET bytes=?6,fingerprint=?7,attachments=0,status='Validated',error=NULL WHERE dataset=?1 AND device=?2 AND incarnation=?3 AND seq=?4 AND hash=?5 AND (status='Retryable' OR (status='Failed' AND (bytes!=?6 OR fingerprint!=?7 OR attachments!=0)))",params![k.identity.dataset,k.identity.device,k.identity.incarnation,k.seq as i64,k.hash.as_slice(),package.bytes,package.fingerprint])?;
        self.revisit_cancellable(check)?;
        self.status()?
            .inbox
            .into_iter()
            .find(|e| e.key == *k)
            .ok_or(Error::Invalid("missing inbox"))
            .and_then(|e| match e.state {
                InboxState::Failed => Err(Error::Invalid("blocked receipt")),
                InboxState::Blocked => Err(Error::StreamFork),
                state => Ok(state),
            })
    }
    pub(crate) fn validate_package(&self, package: &Package) -> Result<()> {
        let k = &package.key;
        if package.bytes.len() > MAX_PACKET {
            return Err(Error::Invalid("packet size"));
        }
        if hash(&package.bytes) != k.hash {
            return Err(Error::Invalid("packet hash"));
        }
        let p = Packet::decode(&package.bytes).map_err(Error::Invalid)?;
        if p.identity != k.identity || p.seq != k.seq {
            return Err(Error::Invalid("object identity"));
        }
        if package.fingerprint != self.schema.fingerprint() {
            return Err(Error::Schema("schema fingerprint"));
        }
        crate::attachments::validate_refs(&package.attachment_references)?;
        self.validate_packet(&p)?;
        for op in &p.operations {
            match self.parent_scope(op) {
                Ok(()) | Err(Error::MissingDependencies) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
    pub fn revisit(&mut self) -> Result<()> {
        self.revisit_cancellable(|| Ok(()))
    }
    pub(crate) fn revisit_cancellable(&mut self, check: impl Fn() -> Result<()>) -> Result<()> {
        self.check()?;
        let mut budget = 1000usize;
        loop {
            let cursor: i64 = self
                .conn
                .query_row("SELECT revisit FROM _lfs_cursors", [], |r| r.get(0))?;
            let ids:Vec<(i64,PacketKey)>=self.conn.prepare("SELECT rowid,dataset,device,incarnation,seq,hash FROM _lfs_inbox WHERE rowid>?1 AND status IN ('Downloaded','Validated','WaitingDependencies') ORDER BY rowid LIMIT ?2")?.query_map(params![cursor,budget as i64],|r|{let h:Vec<u8>=r.get(5)?;Ok((r.get(0)?,PacketKey{identity:Identity{dataset:r.get(1)?,device:r.get(2)?,incarnation:r.get(3)?},seq:r.get::<_,i64>(4)? as u64,hash:h.try_into().map_err(|_|rusqlite::Error::InvalidQuery)?}))})?.collect::<std::result::Result<_,_>>()?;
            if ids.is_empty() {
                self.conn.execute("UPDATE _lfs_cursors SET revisit=0", [])?;
                if cursor > 0 { continue } else { break }
            }

            let mut progress = false;
            for (rowid, key) in ids {
                check()?;
                if budget == 0 {
                    return Ok(());
                }
                budget -= 1;
                self.conn
                    .execute("UPDATE _lfs_cursors SET revisit=?1", [rowid])?;
                if self.stopped(&key)? {
                    continue;
                }
                let k = &key;
                let (bytes,fingerprint,attachments):(Vec<u8>,String,i64)=self.conn.query_row("SELECT bytes,fingerprint,attachments FROM _lfs_inbox WHERE dataset=?1 AND device=?2 AND incarnation=?3 AND seq=?4 AND hash=?5",params![k.identity.dataset,k.identity.device,k.identity.incarnation,k.seq as i64,k.hash.as_slice()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
                let result = (|| {
                    if hash(&bytes) != k.hash
                        || fingerprint != self.schema.fingerprint()
                        || attachments != 0
                    {
                        return Err(Error::Invalid("inbox integrity/schema/attachments"));
                    }
                    let p = Packet::decode(&bytes).map_err(Error::Invalid)?;
                    self.validate_packet(&p)?;
                    if p.identity != k.identity || p.seq != k.seq {
                        return Err(Error::Invalid("inbox identity"));
                    }
                    for op in &p.operations {
                        self.parent_scope(op)?;
                    }
                    if self.has_fork(k)? {
                        return Err(Error::StreamFork);
                    }
                    let applied:Option<Vec<u8>>=self.conn.query_row("SELECT hash FROM _lfs_packets WHERE dataset=?1 AND device=?2 AND incarnation=?3 AND seq=?4",params![k.identity.dataset,k.identity.device,k.identity.incarnation,k.seq as i64],|r|r.get(0)).optional()?;
                    if let Some(h) = applied {
                        if h != k.hash {
                            return Err(Error::StreamFork);
                        }
                        self.mark(k, "Applied", None)?;
                        return Ok(());
                    }
                    if p.seq > 1 {
                        let prev:Option<Vec<u8>>=self.conn.query_row("SELECT hash FROM _lfs_packets WHERE dataset=?1 AND device=?2 AND incarnation=?3 AND seq=?4",params![k.identity.dataset,k.identity.device,k.identity.incarnation,k.seq as i64-1],|r|r.get(0)).optional()?;
                        match prev {
                            None => return Err(Error::MissingDependencies),
                            Some(h)
                                if Some(h.as_slice())
                                    != p.predecessor.as_ref().map(|v| v.as_slice()) =>
                            {
                                return Err(Error::StreamFork);
                            }
                            _ => {}
                        }
                    }
                    for op in &p.operations {
                        self.parent_scope(op)?;
                    }
                    self.conn.execute_batch("BEGIN IMMEDIATE")?;
                    let applied = (|| {
                        self.store_packet(&p, &bytes, k.hash)?;
                        self.apply_refset(k.hash)?;
                        #[cfg(test)]
                        if self.fail_after_remote {
                            self.conn.execute("INSERT INTO _lfs_meta VALUES (2)", [])?;
                        }
                        self.mark(k, "Applied", None)?;
                        self.conn.execute_batch("COMMIT")?;
                        Ok(())
                    })();
                    if applied.is_err() {
                        self.rollback()
                    }
                    applied
                })();
                match result {
                    Ok(()) => progress = true,
                    Err(Error::MissingDependencies) => self.mark(
                        k,
                        "WaitingDependencies",
                        Some("missing predecessor or record parent"),
                    )?,
                    Err(Error::StreamFork) => {
                        self.stop(k)?;
                    }
                    Err(e) => {
                        self.mark(k, "Failed", Some(&e.to_string()))?;
                    }
                }
            }
            if !progress || budget == 0 {
                break;
            }
        }
        Ok(())
    }
    pub(crate) fn scan_degraded(&mut self) -> Result<bool> {
        self.check()?;
        Ok(self
            .conn
            .query_row("SELECT degraded FROM _lfs_cursors", [], |r| r.get(0))?)
    }
    pub(crate) fn save_scan_degraded(&mut self, v: bool) -> Result<()> {
        self.check()?;
        self.conn
            .execute("UPDATE _lfs_cursors SET degraded=?1", [v])?;
        Ok(())
    }
    pub(crate) fn scan_cursor(&mut self) -> Result<usize> {
        self.check()?;
        Ok(self
            .conn
            .query_row("SELECT scan FROM _lfs_cursors", [], |r| r.get::<_, i64>(0))?
            as usize)
    }
    pub(crate) fn save_scan_cursor(&mut self, c: usize) -> Result<()> {
        self.check()?;
        let c = i64::try_from(c).map_err(|_| Error::Invalid("scan cursor"))?;
        self.conn.execute("UPDATE _lfs_cursors SET scan=?1", [c])?;
        Ok(())
    }
    pub fn status(&mut self) -> Result<Status> {
        self.check()?;
        let mut stmt=self.conn.prepare("SELECT dataset,device,incarnation,seq,hash,status,error FROM _lfs_inbox ORDER BY dataset,device,incarnation,seq,hash")?;
        let mut rows = stmt.query([])?;
        let mut inbox = Vec::new();
        while let Some(r) = rows.next()? {
            let h: Vec<u8> = r.get(4)?;
            inbox.push(InboxEntry {
                key: PacketKey {
                    identity: Identity {
                        dataset: r.get(0)?,
                        device: r.get(1)?,
                        incarnation: r.get(2)?,
                    },
                    seq: r.get::<_, i64>(3)? as u64,
                    hash: h.try_into().map_err(|_| Error::Invalid("inbox hash"))?,
                },
                state: InboxState::parse(&r.get::<_, String>(5)?)?,
                error: r.get(6)?,
            });
        }
        Ok(Status {
            pending_outbox: self.conn.query_row(
                "SELECT count(*) FROM _lfs_delivery WHERE acknowledged=0",
                [],
                |r| r.get::<_, i64>(0),
            )? as usize,
            stopped_streams: self
                .conn
                .query_row("SELECT count(*) FROM _lfs_stopped", [], |r| {
                    r.get::<_, i64>(0)
                })? as usize,
            blocked_streams:self.conn.prepare("SELECT dataset,device,incarnation FROM _lfs_stopped ORDER BY dataset,device,incarnation")?.query_map([],|r|Ok(Identity {dataset:r.get(0)?,device:r.get(1)?,incarnation:r.get(2)?}))?.collect::<std::result::Result<_,_>>()?,
            inbox,
            outstanding_objects:self.conn.query_row("SELECT count(*) FROM _lfs_download_debt",[],|r|r.get::<_,i64>(0))? as usize,
            last_sync_error: None,
            discovery_incomplete: self.conn.query_row(
                "SELECT scan!=0 OR degraded!=0 FROM _lfs_cursors",
                [],
                |r| r.get(0),
            )?,
        })
    }
    /// Additional bounded payloads observed under an already retained claimed PacketKey.
    pub fn retained_variants(&mut self, k: &PacketKey) -> Result<Vec<Vec<u8>>> {
        self.check()?;
        let mut stmt=self.conn.prepare("SELECT content_hash,bytes FROM _lfs_variants WHERE dataset=?1 AND device=?2 AND incarnation=?3 AND seq=?4 AND claimed_hash=?5 ORDER BY content_hash,chunk")?;
        let mut rows = stmt.query(params![
            k.identity.dataset,
            k.identity.device,
            k.identity.incarnation,
            k.seq as i64,
            k.hash.as_slice()
        ])?;
        let mut result = Vec::new();
        let mut previous: Option<Vec<u8>> = None;
        while let Some(r) = rows.next()? {
            let h: Vec<u8> = r.get(0)?;
            let bytes: Vec<u8> = r.get(1)?;
            if previous.as_ref() != Some(&h) {
                result.push(Vec::new());
                previous = Some(h);
            }
            result.last_mut().unwrap().extend(bytes);
        }
        Ok(result)
    }
    pub fn retained_bytes(&mut self, k: &PacketKey) -> Result<Vec<u8>> {
        self.check()?;
        let mut chunks=self.conn.prepare("SELECT bytes FROM _lfs_rejected WHERE dataset=?1 AND device=?2 AND incarnation=?3 AND seq=?4 AND hash=?5 ORDER BY chunk")?;
        let parts: Vec<Vec<u8>> = chunks
            .query_map(
                params![
                    k.identity.dataset,
                    k.identity.device,
                    k.identity.incarnation,
                    k.seq as i64,
                    k.hash.as_slice()
                ],
                |r| r.get(0),
            )?
            .collect::<std::result::Result<_, _>>()?;
        if !parts.is_empty() {
            return Ok(parts.concat());
        }
        Ok(self.conn.query_row("SELECT bytes FROM _lfs_inbox WHERE dataset=?1 AND device=?2 AND incarnation=?3 AND seq=?4 AND hash=?5",params![k.identity.dataset,k.identity.device,k.identity.incarnation,k.seq as i64,k.hash.as_slice()],|r|r.get(0))?)
    }
    pub fn applied_packets(&mut self) -> Result<Vec<PacketKey>> {
        self.check()?;
        let mut out = Vec::new();
        let mut s=self.conn.prepare("SELECT dataset,device,incarnation,seq,hash FROM _lfs_packets ORDER BY dataset,device,incarnation,seq")?;
        let mut rows = s.query([])?;
        while let Some(r) = rows.next()? {
            let h: Vec<u8> = r.get(4)?;
            out.push(PacketKey {
                identity: Identity {
                    dataset: r.get(0)?,
                    device: r.get(1)?,
                    incarnation: r.get(2)?,
                },
                seq: r.get::<_, i64>(3)? as u64,
                hash: h.try_into().map_err(|_| Error::Invalid("hash"))?,
            });
        }
        Ok(out)
    }
}
