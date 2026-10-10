//! Serial, SDK-owned SQLite Session store. All callbacks are synchronous and must not do network I/O.
mod access;
mod compatibility;
pub use compatibility::{SqliteCompatibility, check_sqlite_compatibility};
pub mod attachments;
mod attempts;
pub mod backup;
pub use attempts::FailedAttempt;
mod causal;
mod envelope;
pub use envelope::{MAX_ENVELOPE, ObjectKey, OutgoingEnvelope};
mod schema;
pub mod sync;
pub use access::{Read, Write};
use access::{auth, clear_auth, owned, query};
pub use causal::{Conflict, InboxEntry, InboxState, Package, PacketKey, Status};
use fallible_streaming_iterator::FallibleStreamingIterator;
pub use lfs_protocol::{Identity, Packet, Value};
use lfs_protocol::{OpId, Operation, hash, hash_hex};
use rusqlite::{Connection, params, session::Session};
pub use schema::{Column, ColumnType, MAX_FINGERPRINT, Schema, Table};
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    path::Path,
    time::Duration,
};
pub use sync::{
    Cancellation, Fault, MemoryTransport, NetworkError, Sdk, SyncLimits, SyncReport, Transport,
};

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug)]
pub enum Error {
    Sqlite(rusqlite::Error),
    Io(std::io::Error),
    DatabaseInUse,
    Schema(&'static str),
    Invalid(&'static str),
    BaselineRequired,
    IdentityMismatch,
    ExternalChange,
    Caller(String),
    Poisoned,
    ConflictRequiresResolution { table: u32, key: String },
    MissingDependencies,
    StreamFork,
    Busy,
    SyncInProgress,
    Closed,
    Network(sync::NetworkError),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Self::Sqlite(e)
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Baseline {
    Reject,
    Import,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingPacket {
    pub seq: u64,
    pub identity: Identity,
    pub hash: [u8; 32],
    pub object_id: String,
    pub bytes: Vec<u8>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct Commit {
    pub packet: Packet,
    pub pending: PendingPacket,
}
#[derive(Clone, Debug)]
pub struct RuntimeInfo {
    pub sqlite_version: String,
    pub session: bool,
    pub preupdate_hook: bool,
}

pub struct Store {
    conn: Connection,
    _ownership: File,
    schema: Schema,
    identity: Identity,
    data_version: i64,
    schema_version: i64,
    poisoned: bool,
    attachment_root: std::path::PathBuf,
    local_refs: Vec<String>,
    #[cfg(test)]
    fail_after_outbox: bool,
    #[cfg(test)]
    fail_after_remote: bool,
}
fn pragma(conn: &Connection, name: &str) -> Result<i64> {
    Ok(conn.query_row(&format!("PRAGMA {name}"), [], |r| r.get(0))?)
}
const INTERNAL: &[(&str, &str)] = &[
    (
        "_lfs_state",
        "CREATE TABLE _lfs_state (singleton INTEGER PRIMARY KEY CHECK(singleton=1), dataset TEXT NOT NULL, device TEXT NOT NULL, incarnation TEXT NOT NULL, schema_spec TEXT NOT NULL, next_seq INTEGER NOT NULL, predecessor BLOB) STRICT",
    ),
    (
        "_lfs_outbox",
        "CREATE TABLE _lfs_outbox (seq INTEGER PRIMARY KEY, hash BLOB NOT NULL, bytes BLOB NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0) STRICT",
    ),
    (
        "_lfs_versions",
        "CREATE TABLE _lfs_versions (op_id BLOB PRIMARY KEY NOT NULL, table_id INTEGER NOT NULL, record_key TEXT NOT NULL, parent BLOB, packet_seq INTEGER NOT NULL, op_index INTEGER NOT NULL) STRICT",
    ),
    (
        "_lfs_heads",
        "CREATE TABLE _lfs_heads (table_id INTEGER NOT NULL, record_key TEXT NOT NULL, op_id BLOB NOT NULL, PRIMARY KEY(table_id, record_key)) STRICT",
    ),
];
// Inspect the original path: SQLite recognizes a raw, case-sensitive `file:` prefix.
// Encoded OS bytes preserve ASCII prefixes without discarding non-UTF8 filenames.
fn validate_file_path(path: &Path) -> Result<()> {
    let bytes = path.as_os_str().as_encoded_bytes();
    if bytes.is_empty() || path == Path::new(":memory:") || bytes.starts_with(b"file:") {
        return Err(Error::Invalid("file database required"));
    }
    Ok(())
}
impl Store {
    /// Existing untracked rows require an explicit Import; noncanonical schemas are refused.
    pub fn open(
        path: impl AsRef<Path>,
        identity: Identity,
        schema: Schema,
        baseline: Baseline,
    ) -> Result<Self> {
        identity.validate().map_err(Error::Invalid)?;
        schema.validate()?;
        let path = path.as_ref();
        validate_file_path(path)?;
        check_sqlite_compatibility()?;
        attachments::safe(path, true)?;
        let ownership = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        ownership.try_lock().map_err(|_| Error::DatabaseInUse)?;
        let conn = Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(Duration::from_millis(100))?;
        conn.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA locking_mode=EXCLUSIVE; PRAGMA synchronous=FULL; PRAGMA trusted_schema=OFF; BEGIN EXCLUSIVE;")?;
        conn.set_limit(
            rusqlite::limits::Limit::SQLITE_LIMIT_LENGTH,
            (MAX_ENVELOPE + 65536) as i32,
        )?;
        conn.set_limit(rusqlite::limits::Limit::SQLITE_LIMIT_SQL_LENGTH, 65536)?;
        let mut store = Self {
            conn,
            _ownership: ownership,
            schema,
            identity,
            data_version: 0,
            schema_version: 0,
            poisoned: false,
            attachment_root: attachments::root_for(path),
            local_refs: Vec::new(),
            #[cfg(test)]
            fail_after_outbox: false,
            #[cfg(test)]
            fail_after_remote: false,
        };
        let init = store.initialize(baseline);
        match init {
            Ok(()) => store.conn.execute_batch("COMMIT")?,
            Err(e) => {
                let _ = store.conn.execute_batch("ROLLBACK");
                return Err(e);
            }
        }
        store.data_version = pragma(&store.conn, "data_version")?;
        store.schema_version = pragma(&store.conn, "schema_version")?;
        store.reconcile_attachments()?;
        let _ = store.revisit();
        Ok(store)
    }
    fn initialize(&mut self, baseline: Baseline) -> Result<()> {
        let objects: Vec<(String, String, String, Option<String>)> = self
            .conn
            .prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY name")?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?
            .collect::<std::result::Result<_, _>>()?;
        let tracked = objects.iter().any(|(_, n, _, _)| n == "_lfs_state");
        let empty = objects.is_empty();
        for (kind, name, table, sql) in &objects {
            if kind == "index"
                && sql.is_none()
                && (self.schema.tables.iter().any(|t| t.name == *table)
                    || (tracked
                        && INTERNAL
                            .iter()
                            .chain(causal::INTERNAL2)
                            .chain(envelope::INTERNAL3)
                            .chain(attempts::INTERNAL4)
                            .chain(attachments::INTERNAL5)
                            .any(|(n, _)| n == table)))
            {
                continue;
            }
            let expected = self
                .schema
                .tables
                .iter()
                .find(|t| t.name == *name)
                .map(Table::create_sql)
                .or_else(|| {
                    if tracked {
                        INTERNAL
                            .iter()
                            .chain(causal::INTERNAL2)
                            .chain(envelope::INTERNAL3)
                            .chain(attempts::INTERNAL4)
                            .chain(attachments::INTERNAL5)
                            .find(|(n, _)| n == name)
                            .map(|(_, s)| s.to_string())
                    } else {
                        None
                    }
                });
            if kind != "table" || expected.as_ref() != sql.as_ref() {
                return Err(Error::Schema("unknown or modified schema object"));
            }
        }
        for t in &self.schema.tables {
            if empty {
                self.conn.execute_batch(&t.create_sql())?;
            } else if !objects.iter().any(|(_, n, _, _)| n == &t.name) {
                return Err(Error::Schema("missing registered table"));
            }
            // Inspect actual SQLite column metadata as well as canonical DDL.
            let columns: Vec<(String, String, i64, i64, i64)> = self
                .conn
                .prepare(&format!("PRAGMA table_xinfo(\"{}\")", t.name))?
                .query_map([], |r| {
                    Ok((r.get(1)?, r.get(2)?, r.get(3)?, r.get(5)?, r.get(6)?))
                })?
                .collect::<std::result::Result<_, _>>()?;
            if columns.len() != t.columns.len()
                || columns.iter().enumerate().any(|(i, c)| {
                    c.0 != t.columns[i].name
                        || c.2 != i64::from(!t.columns[i].nullable)
                        || c.3 != i64::from(i == 0)
                        || c.4 != 0
                })
            {
                return Err(Error::Schema("column inspection mismatch"));
            }
        }
        if tracked {
            for (name, _) in INTERNAL {
                if !objects.iter().any(|(_, n, _, _)| n == name) {
                    return Err(Error::Schema("incomplete metadata"));
                }
            }
            let state: (String, String, String, String) = self.conn.query_row(
                "SELECT dataset,device,incarnation,schema_spec FROM _lfs_state WHERE singleton=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?;
            if state
                != (
                    self.identity.dataset.clone(),
                    self.identity.device.clone(),
                    self.identity.incarnation.clone(),
                    self.schema.fingerprint(),
                )
            {
                return Err(Error::IdentityMismatch);
            }
        } else {
            let mut keys = BTreeSet::new();
            for t in &self.schema.tables {
                let mut stmt = self.conn.prepare(&format!(
                    "SELECT \"{}\" FROM \"{}\"",
                    t.columns[0].name, t.name
                ))?;
                let mut rows = stmt.query([])?;
                while let Some(row) = rows.next()? {
                    if baseline == Baseline::Reject {
                        return Err(Error::BaselineRequired);
                    }
                    if keys.len() >= lfs_protocol::MAX_OPS {
                        return Err(Error::Invalid("baseline operation limit"));
                    }
                    let Value::Text(key) = owned(row.get_ref(0)?)? else {
                        return Err(Error::Schema("baseline key"));
                    };
                    keys.insert((t.id, key));
                }
            }
            for (_, sql) in INTERNAL {
                self.conn.execute_batch(sql)?;
            }
            self.conn.execute(
                "INSERT INTO _lfs_state VALUES (1,?1,?2,?3,?4,1,NULL)",
                params![
                    self.identity.dataset,
                    self.identity.device,
                    self.identity.incarnation,
                    self.schema.fingerprint()
                ],
            )?;
            self.initialize_causal(false)?;
            self.initialize_wire()?;
            self.persist(keys)?;
        }
        if tracked {
            self.initialize_causal(true)?;
            self.initialize_wire()?;
        }
        self.initialize_attempts()?;
        self.initialize_attachments()?;
        Ok(())
    }
    fn check(&mut self) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if pragma(&self.conn, "data_version")? != self.data_version
            || pragma(&self.conn, "schema_version")? != self.schema_version
        {
            self.poisoned = true;
            return Err(Error::ExternalChange);
        }
        Ok(())
    }
    pub fn registered_schema(&self) -> &Schema {
        &self.schema
    }
    pub fn runtime_info(&self) -> Result<RuntimeInfo> {
        compatibility::runtime_info(&self.conn)
    }
    pub fn read<T>(&mut self, callback: impl FnOnce(&Read<'_>) -> Result<T>) -> Result<T> {
        self.check()?;
        auth(&self.conn, &self.schema, false, Default::default())?;
        let result = catch_unwind(AssertUnwindSafe(|| callback(&Read { conn: &self.conn })));
        if let Err(e) = clear_auth(&self.conn) {
            self.poisoned = true;
            return Err(e);
        }
        match result {
            Ok(v) => v,
            Err(p) => resume_unwind(p),
        }
    }
    pub fn write_transaction<T>(
        &mut self,
        callback: impl FnOnce(&Write<'_>) -> Result<T>,
    ) -> Result<(T, Option<Commit>)> {
        self.check()?;
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut session = Session::new(&self.conn)?;
            for t in &self.schema.tables {
                session.attach(Some(t.name.as_str()))?;
            }
            let stepping = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            auth(&self.conn, &self.schema, true, stepping.clone())?;
            let tainted = std::cell::Cell::new(false);
            let called = catch_unwind(AssertUnwindSafe(|| {
                callback(&Write {
                    tainted: &tainted,
                    conn: &self.conn,
                    stepping,
                })
            }));
            clear_auth(&self.conn)?;
            let output = match called {
                Ok(v) => v?,
                Err(p) => resume_unwind(p),
            };
            if tainted.get() {
                return Err(Error::Invalid("tainted write transaction"));
            }
            if self.conn.is_autocommit() {
                return Err(Error::Invalid("SQLite ended the write transaction"));
            }
            let changes = session.changeset()?;
            let mut iter = changes.iter()?;
            let mut keys = BTreeSet::new();
            while let Some(item) = iter.next()? {
                let op = item.op()?;
                let t = self
                    .schema
                    .tables
                    .iter()
                    .find(|t| t.name == op.table_name())
                    .ok_or(Error::Schema("Session table"))?;
                let key = if op.code() == rusqlite::hooks::Action::SQLITE_INSERT {
                    item.new_value(0)?
                } else {
                    item.old_value(0)?
                };
                let Value::Text(key) = owned(key)? else {
                    return Err(Error::Invalid("Session primary key"));
                };
                keys.insert((t.id, key));
                if keys.len() > lfs_protocol::MAX_OPS {
                    return Err(Error::Invalid("operation limit"));
                }
            }
            drop(iter);
            drop(changes);
            drop(session);
            if keys.is_empty() && !self.local_refs.is_empty() {
                return Err(Error::Invalid(
                    "attachment references require record changes",
                ));
            }
            Ok((output, self.persist(keys)?))
        }));
        if let Err(e) = clear_auth(&self.conn) {
            self.poisoned = true;
            return Err(e);
        }
        match result {
            Ok(Ok(v)) => match self.conn.execute_batch("COMMIT") {
                Ok(()) => Ok(v),
                Err(e) => {
                    self.rollback();
                    Err(e.into())
                }
            },
            Ok(Err(e)) => {
                self.rollback();
                Err(e)
            }
            Err(p) => {
                self.rollback();
                resume_unwind(p)
            }
        }
    }
    fn rollback(&mut self) {
        if !self.conn.is_autocommit() && self.conn.execute_batch("ROLLBACK").is_err() {
            self.poisoned = true;
        }
    }
    fn persist(&self, keys: BTreeSet<(u32, String)>) -> Result<Option<Commit>> {
        if keys.is_empty() {
            return Ok(None);
        }
        let (seq, predecessor): (i64, Option<Vec<u8>>) = self.conn.query_row(
            "SELECT next_seq,predecessor FROM _lfs_state WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if seq <= 0 || seq == i64::MAX {
            return Err(Error::Invalid("sequence exhausted/corrupt"));
        }
        let mut operations = Vec::new();
        for (table, key) in keys {
            let t = self
                .schema
                .tables
                .iter()
                .find(|t| t.id == table)
                .ok_or(Error::Schema("table id"))?;
            let row = query(
                &self.conn,
                &format!(
                    "SELECT * FROM \"{}\" WHERE \"{}\"=?1",
                    t.name, t.columns[0].name
                ),
                &[Value::Text(key.clone())],
            )?
            .into_iter()
            .next();
            let parents = self.record_heads(table, &key)?;
            if parents.len() > 1 {
                return Err(Error::ConflictRequiresResolution { table, key });
            }
            let id = OpId {
                dataset: self.identity.dataset.clone(),
                device: self.identity.device.clone(),
                incarnation: self.identity.incarnation.clone(),
                seq: seq as u64,
                index: operations.len() as u32,
                schema: self.schema.version,
            };
            operations.push(Operation {
                id,
                table,
                key,
                parents: parents.into_iter().map(|o| o.id.encode()).collect(),
                row,
            });
        }
        let packet = Packet {
            identity: self.identity.clone(),
            schema: self.schema.version,
            seq: seq as u64,
            predecessor: predecessor
                .map(|v| v.try_into().map_err(|_| Error::Invalid("predecessor hash")))
                .transpose()?,
            operations,
        };
        self.commit_local(packet).map(Some)
    }
    fn commit_local(&self, packet: Packet) -> Result<Commit> {
        if self.conn.query_row("SELECT EXISTS(SELECT 1 FROM _lfs_stopped WHERE dataset=?1 AND device=?2 AND incarnation=?3)",params![self.identity.dataset,self.identity.device,self.identity.incarnation],|r|r.get::<_,bool>(0))? {return Err(Error::StreamFork)}
        let seq = packet.seq;
        let bytes = packet.encode().map_err(Error::Invalid)?;
        let digest = hash(&bytes);
        self.store_packet(&packet, &bytes, digest)?;
        self.freeze_wire(&packet, &bytes)?;
        self.persist_references(digest, &self.local_refs)?;
        self.conn.execute(
            "UPDATE _lfs_state SET next_seq=?1,predecessor=?2 WHERE singleton=1",
            params![seq as i64 + 1, digest.as_slice()],
        )?;
        let k = PacketKey {
            identity: packet.identity.clone(),
            seq,
            hash: digest,
        };
        self.conn.execute(
            "INSERT INTO _lfs_delivery VALUES (?1,?2,?3,?4,?5,0)",
            params![
                k.identity.dataset,
                k.identity.device,
                k.identity.incarnation,
                seq as i64,
                digest.as_slice()
            ],
        )?;
        #[cfg(test)]
        if self.fail_after_outbox {
            self.conn.execute(
                "INSERT INTO _lfs_delivery SELECT * FROM _lfs_delivery WHERE seq=?1",
                [seq as i64],
            )?;
        }
        Ok(Commit {
            pending: self.pending(seq, digest, bytes),
            packet,
        })
    }
    fn pending(&self, seq: u64, hash: [u8; 32], bytes: Vec<u8>) -> PendingPacket {
        PendingPacket {
            seq,
            identity: self.identity.clone(),
            object_id: format!("{seq:020}-{}", hash_hex(&hash)),
            hash,
            bytes,
        }
    }
    /// Owned bytes; release the mutable Store borrow before doing network I/O.
    pub fn pending_packets(&mut self, limit: usize) -> Result<Vec<PendingPacket>> {
        self.check()?;
        if limit == 0 || limit > 1000 {
            return Err(Error::Invalid("pending limit"));
        }
        let mut stmt = self.conn.prepare(
            "SELECT d.seq,d.hash,p.bytes FROM _lfs_delivery d JOIN _lfs_packets p ON d.hash=p.hash WHERE d.acknowledged=0 ORDER BY d.seq LIMIT ?1",
        )?;
        let mut rows = stmt.query([limit as i64])?;
        let mut out = Vec::new();
        let mut total = 0;
        while let Some(row) = rows.next()? {
            let seq: i64 = row.get(0)?;
            let digest: Vec<u8> = row.get(1)?;
            let bytes: Vec<u8> = row.get(2)?;
            total += bytes.len();
            if total > 32 * 1024 * 1024 {
                break;
            }
            let digest: [u8; 32] = digest
                .try_into()
                .map_err(|_| Error::Invalid("outbox hash"))?;
            if seq <= 0 || hash(&bytes) != digest {
                return Err(Error::Invalid("outbox corruption"));
            }
            let identity = Packet::decode(&bytes).map_err(Error::Invalid)?.identity;
            let mut pending = self.pending(seq as u64, digest, bytes);
            pending.identity = identity;
            out.push(pending);
        }
        Ok(out)
    }
    /// Host explicitly confirms this exact packet. Keeps history for ancestry; never deletes remote data.
    pub fn acknowledge(&mut self, seq: u64, hash: [u8; 32]) -> Result<()> {
        self.check()?;
        let bytes: Vec<u8> = self.conn.query_row(
            "SELECT bytes FROM _lfs_packets WHERE hash=?1",
            [hash.as_slice()],
            |r| r.get(0),
        )?;
        let identity = Packet::decode(&bytes).map_err(Error::Invalid)?.identity;
        let seq = i64::try_from(seq).map_err(|_| Error::Invalid("sequence"))?;
        if self.conn.execute(
            "UPDATE _lfs_delivery SET acknowledged=1 WHERE seq=?1 AND hash=?2 AND dataset=?3 AND device=?4 AND incarnation=?5",
            params![seq, hash.as_slice(),self.identity.dataset, identity.device, identity.incarnation],
        )? != 1
        {
            return Err(Error::Invalid("unknown packet/hash"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
