//! Canonical network envelope v1. Inner LFS1 bytes and predecessor hashes are unchanged.
use crate::*;

pub const MAX_ENVELOPE: usize = lfs_protocol::MAX_PACKET + 256 * 516 + MAX_FINGERPRINT + 64;
pub(crate) const INTERNAL3: &[(&str, &str)] = &[
    (
        "_lfs_wire_version",
        "CREATE TABLE _lfs_wire_version (version INTEGER PRIMARY KEY CHECK(version=1)) STRICT",
    ),
    (
        "_lfs_outgoing_wire",
        "CREATE TABLE _lfs_outgoing_wire (payload_hash BLOB PRIMARY KEY NOT NULL, envelope_hash BLOB NOT NULL, bytes BLOB NOT NULL) STRICT",
    ),
    (
        "_lfs_received_wire",
        "CREATE TABLE _lfs_received_wire (dataset TEXT NOT NULL, device TEXT NOT NULL, incarnation TEXT NOT NULL, seq INTEGER NOT NULL, payload_hash BLOB NOT NULL, envelope_hash BLOB NOT NULL, bytes BLOB NOT NULL, PRIMARY KEY(dataset,device,incarnation,seq,envelope_hash)) STRICT",
    ),
];
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectKey {
    pub packet: PacketKey,
    /// None is reserved for Stage2 MODEL transports; production always supplies the full object hash.
    pub envelope_hash: Option<[u8; 32]>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutgoingEnvelope {
    pub key: ObjectKey,
    pub bytes: Vec<u8>,
}
impl OutgoingEnvelope {
    /// For transport fixtures/import tools. SDK sends only the transactionally persisted instance.
    pub fn from_package(package: &Package) -> Result<Self> {
        Ok(Self {
            key: package.object_key()?,
            bytes: package.encode_envelope()?,
        })
    }
    pub fn validate(&self) -> Result<Package> {
        if self.bytes.len() > MAX_ENVELOPE {
            return Err(Error::Invalid("envelope limit"));
        }
        if self.key.envelope_hash != Some(hash(&self.bytes)) {
            return Err(Error::Invalid("full envelope hash"));
        }
        let p = Package::decode_envelope(&self.bytes)?;
        if p.key != self.key.packet {
            return Err(Error::Invalid("envelope identity"));
        }
        Ok(p)
    }
}
impl Package {
    pub fn encode_envelope(&self) -> Result<Vec<u8>> {
        if self.bytes.len() > lfs_protocol::MAX_PACKET
            || self.fingerprint.len() > MAX_FINGERPRINT
            || self.attachment_references.len() > 256
            || self.attachment_references.iter().any(|s| s.len() > 512)
        {
            return Err(Error::Invalid("envelope limit"));
        }
        Ok(self.evidence_envelope())
    }
    pub(crate) fn evidence_envelope(&self) -> Vec<u8> {
        let mut out = b"LFSE\0\0\0\x01".to_vec();
        fn field(out: &mut Vec<u8>, b: &[u8]) {
            out.extend_from_slice(&(b.len() as u32).to_be_bytes());
            out.extend_from_slice(b);
        }
        field(&mut out, self.fingerprint.as_bytes());
        out.extend_from_slice(&(self.attachment_references.len() as u32).to_be_bytes());
        for r in &self.attachment_references {
            field(&mut out, r.as_bytes());
        }
        out.extend_from_slice(&self.key.hash);
        field(&mut out, &self.bytes);
        out
    }
    pub fn decode_envelope(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_ENVELOPE {
            return Err(Error::Invalid("envelope limit"));
        }
        struct Decoder<'a>(&'a [u8]);
        impl<'a> Decoder<'a> {
            fn take(&mut self, n: usize) -> Result<&'a [u8]> {
                if self.0.len() < n {
                    return Err(Error::Invalid("truncated envelope"));
                }
                let (v, tail) = self.0.split_at(n);
                self.0 = tail;
                Ok(v)
            }
            fn n(&mut self) -> Result<usize> {
                Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()) as usize)
            }
            fn field(&mut self, cap: usize) -> Result<&'a [u8]> {
                let n = self.n()?;
                if n > cap {
                    return Err(Error::Invalid("envelope field limit"));
                }
                self.take(n)
            }
            fn text(&mut self, cap: usize) -> Result<String> {
                String::from_utf8(self.field(cap)?.to_vec())
                    .map_err(|_| Error::Invalid("envelope utf8"))
            }
        }
        let mut d = Decoder(bytes);
        if d.take(8)? != b"LFSE\0\0\0\x01" {
            return Err(Error::Invalid("envelope version"));
        }
        let fingerprint = d.text(MAX_FINGERPRINT)?;
        let n = d.n()?;
        if n > 256 {
            return Err(Error::Invalid("envelope references limit"));
        }
        let mut attachment_references = Vec::new();
        for _ in 0..n {
            attachment_references.push(d.text(512)?);
        }
        let digest: [u8; 32] = d.take(32)?.try_into().unwrap();
        let payload = d.field(lfs_protocol::MAX_PACKET)?;
        if !d.0.is_empty() || hash(payload) != digest {
            return Err(Error::Invalid("envelope payload hash or suffix"));
        }
        let p = Packet::decode(payload).map_err(Error::Invalid)?;
        Ok(Self {
            key: PacketKey {
                identity: p.identity,
                seq: p.seq,
                hash: digest,
            },
            fingerprint,
            bytes: payload.to_vec(),
            attachment_references,
        })
    }
    pub fn object_key(&self) -> Result<ObjectKey> {
        let bytes = self.encode_envelope()?;
        // Do not serialize an identity which disagrees with the actual payload header.
        if Self::decode_envelope(&bytes)?.key != self.key {
            return Err(Error::Invalid("envelope identity"));
        }
        Ok(ObjectKey {
            packet: self.key.clone(),
            envelope_hash: Some(hash(&bytes)),
        })
    }
}
impl Store {
    pub(crate) fn initialize_wire(&self) -> Result<()> {
        let exists: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='_lfs_wire_version')",
            [],
            |r| r.get(0),
        )?;
        if exists {
            let version: i64 =
                self.conn
                    .query_row("SELECT version FROM _lfs_wire_version", [], |r| r.get(0))?;
            if version != 1 {
                return Err(Error::Schema("wire metadata version"));
            }
            for (name, _) in INTERNAL3 {
                let found: bool = self.conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name=?1)",
                    [name],
                    |r| r.get(0),
                )?;
                if !found {
                    return Err(Error::Schema("incomplete wire metadata"));
                }
            }
            return Ok(());
        }
        for (_, ddl) in INTERNAL3 {
            self.conn.execute_batch(ddl)?;
        }
        self.conn
            .execute("INSERT INTO _lfs_wire_version VALUES (1)", [])?;
        // One-time pre-release transition in the open transaction, never at upload time.
        let rows: Vec<Vec<u8>> = self.conn.prepare("SELECT p.bytes FROM _lfs_delivery d JOIN _lfs_packets p ON p.hash=d.hash ORDER BY d.seq")?.query_map([], |r|r.get(0))?.collect::<std::result::Result<_,_>>()?;
        for bytes in rows {
            let p = Packet::decode(&bytes).map_err(Error::Invalid)?;
            self.freeze_wire(&p, &bytes)?;
        }
        Ok(())
    }
    pub(crate) fn freeze_wire(&self, p: &Packet, bytes: &[u8]) -> Result<()> {
        let package = Package {
            key: PacketKey {
                identity: p.identity.clone(),
                seq: p.seq,
                hash: hash(bytes),
            },
            bytes: bytes.to_vec(),
            fingerprint: self.schema.fingerprint(),
            attachment_references: self.local_refs.clone(),
        };
        let wire = package.encode_envelope()?;
        self.conn.execute(
            "INSERT INTO _lfs_outgoing_wire VALUES (?1,?2,?3)",
            params![package.key.hash.as_slice(), hash(&wire).as_slice(), wire],
        )?;
        Ok(())
    }
    pub fn pending_envelopes(&mut self, limit: usize) -> Result<Vec<OutgoingEnvelope>> {
        let packets = self.pending_packets(limit)?;
        let mut out = Vec::new();
        let mut total = 0usize;
        for p in packets {
            let (digest, bytes): (Vec<u8>, Vec<u8>) = self.conn.query_row(
                "SELECT envelope_hash,bytes FROM _lfs_outgoing_wire WHERE payload_hash=?1",
                [p.hash.as_slice()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            if total.saturating_add(bytes.len()) > 32 * 1024 * 1024 {
                break;
            }
            total += bytes.len();
            let package = Package::decode_envelope(&bytes)?;
            if digest != hash(&bytes)
                || package.key.identity != p.identity
                || package.key.seq != p.seq
                || package.bytes != p.bytes
            {
                return Err(Error::Invalid("outgoing envelope corruption"));
            }
            out.push(OutgoingEnvelope {
                key: ObjectKey {
                    packet: package.key,
                    envelope_hash: Some(hash(&bytes)),
                },
                bytes,
            });
        }
        Ok(out)
    }
    pub(crate) fn known_objects(&mut self) -> Result<BTreeSet<ObjectKey>> {
        let mut known: BTreeSet<_> = self
            .applied_packets()?
            .into_iter()
            .chain(
                self.status()?
                    .inbox
                    .into_iter()
                    .filter(|i| i.state != InboxState::Retryable)
                    .map(|i| i.key),
            )
            .map(|packet| ObjectKey {
                packet,
                envelope_hash: None,
            })
            .collect();
        let sql = "SELECT w.dataset,w.device,w.incarnation,w.seq,w.payload_hash,w.envelope_hash FROM _lfs_received_wire w WHERE EXISTS(SELECT 1 FROM _lfs_inbox i WHERE i.dataset=w.dataset AND i.device=w.device AND i.incarnation=w.incarnation AND i.seq=w.seq AND i.hash=w.payload_hash AND i.status!='Retryable') UNION ALL SELECT p.dataset,p.device,p.incarnation,p.seq,p.hash,w.envelope_hash FROM _lfs_outgoing_wire w JOIN _lfs_packets p ON p.hash=w.payload_hash";
        let mut stmt = self.conn.prepare(sql)?;
        let keys = stmt.query_map([], |r| {
            let h: Vec<u8> = r.get(4)?;
            let e: Vec<u8> = r.get(5)?;
            Ok(ObjectKey {
                packet: PacketKey {
                    identity: Identity {
                        dataset: r.get(0)?,
                        device: r.get(1)?,
                        incarnation: r.get(2)?,
                    },
                    seq: r.get::<_, i64>(3)? as u64,
                    hash: h.try_into().map_err(|_| rusqlite::Error::InvalidQuery)?,
                },
                envelope_hash: Some(e.try_into().map_err(|_| rusqlite::Error::InvalidQuery)?),
            })
        })?;
        for key in keys {
            known.insert(key?);
        }
        Ok(known)
    }
    pub(crate) fn receive_download(
        &mut self,
        key: &ObjectKey,
        data: sync::Download,
        check: impl Fn() -> Result<()>,
    ) -> Result<InboxState> {
        match data {
            sync::Download::Package(p) => self.receive_object(key, p, check),
            sync::Download::Wire(bytes) => {
                let envelope = OutgoingEnvelope {
                    key: key.clone(),
                    bytes,
                };
                match envelope.validate() {
                    Ok(p) => self.receive_object(key, p, check),
                    Err(e) => {
                        self.retain_attempt(key, &envelope.bytes, true, true, &e.to_string())?;
                        Err(e)
                    }
                }
            }
        }
    }
    pub(crate) fn receive_object(
        &mut self,
        key: &ObjectKey,
        p: Package,
        check: impl Fn() -> Result<()>,
    ) -> Result<InboxState> {
        if p.key != key.packet {
            self.retain_attempt(
                key,
                &p.evidence_envelope(),
                false,
                true,
                "download object identity",
            )?;
            return Err(Error::Network(NetworkError::Corrupt));
        }
        if let Some(expected) = key.envelope_hash {
            let bytes = p.encode_envelope()?;
            let integrity = (|| {
                if hash(&bytes) != expected
                    || Package::decode_envelope(&bytes)? != p
                    || p.key != key.packet
                {
                    return Err(Error::Network(NetworkError::Corrupt));
                }
                Ok(())
            })();
            if let Err(e) = integrity {
                self.retain_attempt(key, &bytes, true, true, &e.to_string())?;
                return Err(e);
            }
            if let Err(e) = self.validate_package(&p) {
                self.retain_attempt(key, &bytes, true, false, &e.to_string())?;
                return self.receive_cancellable(p, check);
            }
            let k = &p.key;
            self.conn.execute(
                "INSERT OR IGNORE INTO _lfs_received_wire VALUES (?1,?2,?3,?4,?5,?6,?7)",
                params![
                    k.identity.dataset,
                    k.identity.device,
                    k.identity.incarnation,
                    k.seq as i64,
                    k.hash.as_slice(),
                    expected.as_slice(),
                    bytes
                ],
            )?;
        }
        self.receive_cancellable(p, check)
    }
    /// Full verified transmitted receipts, including envelope variants, for diagnostics.
    pub fn received_envelopes(&mut self) -> Result<Vec<Vec<u8>>> {
        self.check()?;
        Ok(self
            .conn
            .prepare("SELECT bytes FROM _lfs_received_wire ORDER BY rowid")?
            .query_map([], |r| r.get(0))?
            .collect::<std::result::Result<_, _>>()?)
    }
}
