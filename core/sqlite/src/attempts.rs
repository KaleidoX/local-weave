//! Append-only bounded failure evidence; never used as a competing valid packet.
use crate::*;

pub(crate) const INTERNAL4: &[(&str, &str)] = &[
    (
        "_lfs_download_debt",
        "CREATE TABLE _lfs_download_debt (id INTEGER PRIMARY KEY, dataset TEXT NOT NULL, device TEXT NOT NULL, incarnation TEXT NOT NULL, seq INTEGER NOT NULL, hash BLOB NOT NULL, envelope_hash BLOB NOT NULL, turn INTEGER NOT NULL, blocked INTEGER NOT NULL DEFAULT 0, error TEXT, UNIQUE(dataset,device,incarnation,seq,hash,envelope_hash)) STRICT",
    ),
    (
        "_lfs_attempts",
        "CREATE TABLE _lfs_attempts (id INTEGER PRIMARY KEY, dataset TEXT NOT NULL, device TEXT NOT NULL, incarnation TEXT NOT NULL, seq INTEGER NOT NULL, hash BLOB NOT NULL, envelope_hash BLOB, wire INTEGER NOT NULL, retryable INTEGER NOT NULL, error TEXT NOT NULL) STRICT",
    ),
    (
        "_lfs_attempt_bytes",
        "CREATE TABLE _lfs_attempt_bytes (attempt INTEGER NOT NULL, chunk INTEGER NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY(attempt,chunk)) STRICT",
    ),
];
#[derive(Clone, Debug)]
pub struct FailedAttempt {
    pub key: ObjectKey,
    /// Actual wire bytes when wire=true; otherwise LFSE fields from the supplied Package.
    pub bytes: Vec<u8>,
    pub wire: bool,
    pub retryable: bool,
    pub error: String,
}
impl Store {
    pub(crate) fn initialize_attempts(&self) -> Result<()> {
        for (name, ddl) in INTERNAL4 {
            let exists: bool = self.conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name=?1)",
                [name],
                |r| r.get(0),
            )?;
            if !exists {
                self.conn.execute_batch(ddl)?;
            }
        }
        Ok(())
    }
    pub(crate) fn retain_attempt(
        &mut self,
        key: &ObjectKey,
        bytes: &[u8],
        wire: bool,
        retryable: bool,
        error: &str,
    ) -> Result<()> {
        if bytes.len() > 2 * lfs_protocol::MAX_PACKET + MAX_ENVELOPE - lfs_protocol::MAX_PACKET {
            return Err(Error::Invalid("evidence limit"));
        }
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| {
            let k = &key.packet;
            self.conn.execute(
                "INSERT INTO _lfs_attempts VALUES (NULL,?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                params![
                    k.identity.dataset,
                    k.identity.device,
                    k.identity.incarnation,
                    k.seq as i64,
                    k.hash.as_slice(),
                    key.envelope_hash.as_ref().map(|h| h.as_slice()),
                    wire,
                    retryable,
                    error
                ],
            )?;
            let id = self.conn.last_insert_rowid();
            for (i, b) in bytes.chunks(lfs_protocol::MAX_PACKET).enumerate() {
                self.conn.execute(
                    "INSERT INTO _lfs_attempt_bytes VALUES (?1,?2,?3)",
                    params![id, i as i64, b],
                )?;
            }
            self.conn.execute_batch("COMMIT")?;
            Ok(())
        })();
        if result.is_err() {
            self.rollback();
        }
        result
    }
    pub fn failed_attempts(&mut self) -> Result<Vec<FailedAttempt>> {
        self.check()?;
        let mut stmt=self.conn.prepare("SELECT id,dataset,device,incarnation,seq,hash,envelope_hash,wire,retryable,error FROM _lfs_attempts ORDER BY id")?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(r) = rows.next()? {
            let id: i64 = r.get(0)?;
            let h: Vec<u8> = r.get(5)?;
            let e: Option<Vec<u8>> = r.get(6)?;
            let bytes: Vec<Vec<u8>> = self
                .conn
                .prepare("SELECT bytes FROM _lfs_attempt_bytes WHERE attempt=?1 ORDER BY chunk")?
                .query_map([id], |r| r.get(0))?
                .collect::<std::result::Result<_, _>>()?;
            out.push(FailedAttempt {
                key: ObjectKey {
                    packet: PacketKey {
                        identity: Identity {
                            dataset: r.get(1)?,
                            device: r.get(2)?,
                            incarnation: r.get(3)?,
                        },
                        seq: r.get::<_, i64>(4)? as u64,
                        hash: h.try_into().map_err(|_| Error::Invalid("attempt hash"))?,
                    },
                    envelope_hash: e
                        .map(|v| v.try_into().map_err(|_| Error::Invalid("attempt hash")))
                        .transpose()?,
                },
                bytes: bytes.concat(),
                wire: r.get(7)?,
                retryable: r.get(8)?,
                error: r.get(9)?,
            });
        }
        Ok(out)
    }
}

impl Store {
    pub(crate) fn enqueue(&self, k: &ObjectKey) -> Result<()> {
        k.packet.identity.validate().map_err(Error::Invalid)?;
        if k.packet.seq == 0 || k.packet.seq >= i64::MAX as u64 {
            return Err(Error::Invalid("object seq"));
        }
        let n: i64 = self
            .conn
            .query_row("SELECT count(*) FROM _lfs_download_debt", [], |r| r.get(0))?;
        if n >= 10000 {
            return Err(Error::Invalid("download debt limit"));
        }
        let p = &k.packet;
        self.conn.execute("INSERT OR IGNORE INTO _lfs_download_debt(dataset,device,incarnation,seq,hash,envelope_hash,turn) VALUES (?1,?2,?3,?4,?5,?6,0)",params![p.identity.dataset,p.identity.device,p.identity.incarnation,p.seq as i64,p.hash.as_slice(),k.envelope_hash.as_ref().map(|h|h.as_slice()).unwrap_or(&[])])?;
        Ok(())
    }
    pub(crate) fn download_debt(&self, limit: usize) -> Result<Vec<(i64, ObjectKey)>> {
        Ok(self.conn.prepare("SELECT id,dataset,device,incarnation,seq,hash,envelope_hash FROM _lfs_download_debt WHERE blocked=0 ORDER BY turn,id LIMIT ?1")?.query_map([limit as i64],|r| {
            let h:Vec<u8>=r.get(5)?;let e:Vec<u8>=r.get(6)?;
            Ok((r.get(0)?,ObjectKey {packet:PacketKey {identity:Identity {dataset:r.get(1)?,device:r.get(2)?,incarnation:r.get(3)?},seq:r.get::<_,i64>(4)? as u64,hash:h.try_into().map_err(|_|rusqlite::Error::InvalidQuery)?},envelope_hash:if e.is_empty(){None}else{Some(e.try_into().map_err(|_|rusqlite::Error::InvalidQuery)?)} }))
        })?.collect::<std::result::Result<_,_>>()?)
    }
    pub(crate) fn finish_debt(&self, id: i64) -> Result<()> {
        self.conn
            .execute("DELETE FROM _lfs_download_debt WHERE id=?1", [id])?;
        Ok(())
    }
    pub(crate) fn fail_debt(&self, id: i64, blocked: bool, error: &str) -> Result<()> {
        self.conn.execute("UPDATE _lfs_download_debt SET turn=(SELECT coalesce(max(turn),0)+1 FROM _lfs_download_debt),blocked=?2,error=?3 WHERE id=?1",params![id,blocked,error])?;
        Ok(())
    }
}
