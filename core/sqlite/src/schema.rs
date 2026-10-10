use crate::{Error, Result};
use std::collections::BTreeSet;
/// Full canonical DDL fingerprint, format unchanged. Maximum legal shape is 350,922 bytes.
pub const MAX_FINGERPRINT: usize = 384 * 1024;

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnType {
    Text,
    Integer,
    Real,
    Blob,
}
impl ColumnType {
    fn sql(self) -> &'static str {
        match self {
            Self::Text => "TEXT",
            Self::Integer => "INTEGER",
            Self::Real => "REAL",
            Self::Blob => "BLOB",
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub kind: ColumnType,
    pub nullable: bool,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Table {
    pub id: u32,
    pub name: String,
    pub columns: Vec<Column>,
}
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    pub version: u32,
    pub tables: Vec<Table>,
}
fn identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_')
        && s.as_bytes()[0].is_ascii_lowercase()
        && !s.starts_with("sqlite_")
}
impl Schema {
    pub fn validate(&self) -> Result<()> {
        if self.version == 0 || self.tables.is_empty() || self.tables.len() > 64 {
            return Err(Error::Schema("schema bounds"));
        }
        let mut names = BTreeSet::new();
        let mut ids = BTreeSet::new();
        for t in &self.tables {
            if !identifier(&t.name)
                || !names.insert(&t.name)
                || t.id == 0
                || !ids.insert(t.id)
                || t.columns.is_empty()
                || t.columns.len() > 64
            {
                return Err(Error::Schema("table definition"));
            }
            let mut cols = BTreeSet::new();
            for c in &t.columns {
                if !identifier(&c.name) || !cols.insert(&c.name) {
                    return Err(Error::Schema("column name"));
                }
            }
            if t.columns[0].kind != ColumnType::Text || t.columns[0].nullable {
                return Err(Error::Schema(
                    "first column must be nonnull TEXT primary key",
                ));
            }
        }
        if self.fingerprint().len() > MAX_FINGERPRINT {
            return Err(Error::Schema("fingerprint limit"));
        }
        Ok(())
    }
    pub fn fingerprint(&self) -> String {
        let mut tables = self.tables.clone();
        tables.sort_by_key(|t| t.id);
        format!(
            "{}:{}",
            self.version,
            tables
                .iter()
                .map(|t| format!("{}:{}", t.id, t.create_sql()))
                .collect::<Vec<_>>()
                .join(";")
        )
    }
}
impl Table {
    /// Exact supported DDL, also usable by explicit baseline producers after Schema::validate.
    pub fn create_sql(&self) -> String {
        format!(
            "CREATE TABLE \"{}\" ({}) STRICT",
            self.name,
            self.columns
                .iter()
                .enumerate()
                .map(|(i, c)| format!(
                    "\"{}\" {}{}{}",
                    c.name,
                    c.kind.sql(),
                    if c.nullable { "" } else { " NOT NULL" },
                    if i == 0 { " PRIMARY KEY" } else { "" }
                ))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}
