//! Canonical bounded packet encoding/decoding and pure causal full-row register.
mod decode;
mod register;
pub use register::Register;
use sha2::{Digest, Sha256};

pub const MAX_PACKET: usize = 8 * 1024 * 1024;
pub const MAX_CELL: usize = 512 * 1024;
pub const MAX_OPS: usize = 1000;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}
impl Value {
    pub fn validate(&self) -> Result<(), &'static str> {
        match self {
            Self::Real(v) if !v.is_finite() => Err("nonfinite real"),
            Self::Text(v) if v.len() > MAX_CELL => Err("cell limit"),
            Self::Blob(v) if v.len() > MAX_CELL => Err("cell limit"),
            _ => Ok(()),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Identity {
    pub dataset: String,
    pub device: String,
    pub incarnation: String,
}
impl Identity {
    pub fn validate(&self) -> Result<(), &'static str> {
        for s in [&self.dataset, &self.device, &self.incarnation] {
            if s.is_empty() || s.len() > 128 {
                return Err("identity length");
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct OpId {
    pub dataset: String,
    pub device: String,
    pub incarnation: String,
    pub seq: u64,
    pub index: u32,
    pub schema: u32,
}
impl OpId {
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        for s in [&self.dataset, &self.device, &self.incarnation] {
            bytes(&mut b, s.as_bytes());
        }
        b.extend(self.seq.to_be_bytes());
        b.extend(self.index.to_be_bytes());
        b.extend(self.schema.to_be_bytes());
        b
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct Operation {
    pub id: OpId,
    pub table: u32,
    pub key: String,
    /// Canonical encoded OpIds, sorted and unique. Stage 1 has at most one parent.
    pub parents: Vec<Vec<u8>>,
    pub row: Option<Vec<Value>>,
}
#[derive(Clone, Debug, PartialEq)]
pub struct Packet {
    pub identity: Identity,
    pub schema: u32,
    pub seq: u64,
    pub predecessor: Option<[u8; 32]>,
    pub operations: Vec<Operation>,
}
fn bytes(b: &mut Vec<u8>, v: &[u8]) {
    b.extend((v.len() as u32).to_be_bytes());
    b.extend(v);
}
fn value(b: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => b.push(0),
        Value::Integer(v) => {
            b.push(1);
            b.extend(v.to_be_bytes());
        }
        Value::Real(v) => {
            b.push(2);
            b.extend(v.to_bits().to_be_bytes());
        }
        Value::Text(v) => {
            b.push(3);
            bytes(b, v.as_bytes());
        }
        Value::Blob(v) => {
            b.push(4);
            bytes(b, v);
        }
    }
}
impl Packet {
    pub fn encode(&self) -> Result<Vec<u8>, &'static str> {
        self.identity.validate()?;
        if self.schema == 0
            || self.seq == 0
            || self.seq >= i64::MAX as u64
            || (self.seq == 1) != self.predecessor.is_none()
            || self.operations.is_empty()
            || self.operations.len() > MAX_OPS
        {
            return Err("packet bounds");
        }
        let mut b = b"LFS1\0\0\0\x01".to_vec();
        for s in [
            &self.identity.dataset,
            &self.identity.device,
            &self.identity.incarnation,
        ] {
            bytes(&mut b, s.as_bytes());
        }
        b.extend(self.schema.to_be_bytes());
        b.extend(self.seq.to_be_bytes());
        b.push(u8::from(self.predecessor.is_some()));
        if let Some(p) = self.predecessor {
            b.extend(p);
        }
        b.extend((self.operations.len() as u32).to_be_bytes());
        let mut previous = None;
        for (i, op) in self.operations.iter().enumerate() {
            if op.id
                != (OpId {
                    dataset: self.identity.dataset.clone(),
                    device: self.identity.device.clone(),
                    incarnation: self.identity.incarnation.clone(),
                    seq: self.seq,
                    index: i as u32,
                    schema: self.schema,
                })
            {
                return Err("operation identity");
            }
            let key = (op.table, op.key.as_bytes());
            if op.table == 0 || op.key.len() > MAX_CELL || previous.is_some_and(|p| p >= key) {
                return Err("operation order/key");
            }
            previous = Some(key);
            if op.parents.len() > 256
                || op.parents.windows(2).any(|w| w[0] >= w[1])
                || op.parents.iter().any(|p| p.len() > 512)
            {
                return Err("parents");
            }
            for parent in &op.parents {
                let id = OpId::decode(parent)?;
                if id.dataset != self.identity.dataset || id.identity_key() == op.id.identity_key()
                {
                    return Err("parent identity");
                }
            }
            b.extend(op.table.to_be_bytes());
            value(&mut b, &Value::Text(op.key.clone()));
            b.extend((op.parents.len() as u32).to_be_bytes());
            for p in &op.parents {
                bytes(&mut b, p);
            }
            match &op.row {
                None => b.push(0),
                Some(row) => {
                    if row.is_empty()
                        || row.len() > 64
                        || row.first() != Some(&Value::Text(op.key.clone()))
                    {
                        return Err("row shape/key");
                    }
                    b.push(1);
                    b.extend((row.len() as u32).to_be_bytes());
                    for v in row {
                        v.validate()?;
                        value(&mut b, v);
                    }
                }
            }
            if b.len() > MAX_PACKET {
                return Err("packet size");
            }
        }
        Ok(b)
    }
}
pub fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
pub fn hash_hex(hash: &[u8; 32]) -> String {
    hash.iter().map(|v| format!("{v:02x}")).collect()
}
