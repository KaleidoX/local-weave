use lfs_sqlite::{Error, Result, Value};
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "lowercase",
    deny_unknown_fields
)]
pub(crate) enum Typed {
    Null,
    Integer(String),
    Real(String),
    Text(String),
    Blob(String),
}
pub(crate) fn unhex(s: &str) -> Result<Vec<u8>> {
    if !s.len().is_multiple_of(2)
        || !s
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::Invalid("hex"));
    }
    s.as_bytes()
        .chunks(2)
        .map(|v| {
            u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16)
                .map_err(|_| Error::Invalid("hex"))
        })
        .collect()
}
pub(crate) fn hex(v: &[u8]) -> String {
    v.iter().map(|b| format!("{b:02x}")).collect()
}
impl Typed {
    pub(crate) fn value(self) -> Result<Value> {
        let v = match self {
            Self::Null => Value::Null,
            Self::Integer(s) => {
                let n = s.parse::<i64>().map_err(|_| Error::Invalid("integer"))?;
                if n.to_string() != s {
                    return Err(Error::Invalid("integer"));
                }
                Value::Integer(n)
            }
            Self::Real(s) => {
                if s.len() != 16 {
                    return Err(Error::Invalid("f64 bits"));
                }
                let b = unhex(&s)?;
                Value::Real(f64::from_bits(u64::from_be_bytes(b.try_into().unwrap())))
            }
            Self::Text(s) => Value::Text(s),
            Self::Blob(s) => Value::Blob(unhex(&s)?),
        };
        v.validate().map_err(Error::Invalid)?;
        Ok(v)
    }
    pub(crate) fn from(v: Value) -> Self {
        match v {
            Value::Null => Self::Null,
            Value::Integer(n) => Self::Integer(n.to_string()),
            Value::Real(n) => Self::Real(format!("{:016x}", n.to_bits())),
            Value::Text(s) => Self::Text(s),
            Value::Blob(b) => Self::Blob(hex(&b)),
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Statement {
    pub(crate) sql: String,
    #[serde(default)]
    pub(crate) params: Vec<Typed>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Batch {
    pub(crate) statements: Vec<Statement>,
    #[serde(default)]
    pub(crate) attachments: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Resolution {
    pub(crate) table: u32,
    pub(crate) key: String,
    pub(crate) observed: Vec<String>,
    pub(crate) row: Option<Vec<Typed>>,
    #[serde(default)]
    pub(crate) attachments: Vec<String>,
}
