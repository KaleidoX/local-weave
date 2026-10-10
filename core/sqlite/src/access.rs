use crate::{Error, Result, Schema, Value};
use rusqlite::{
    Connection,
    hooks::{AuthAction, AuthContext, Authorization},
    params_from_iter,
    types::ValueRef,
};
use std::{collections::BTreeSet, time::Duration};

pub struct Read<'a> {
    pub(crate) conn: &'a Connection,
}
pub struct Write<'a> {
    pub(crate) tainted: &'a std::cell::Cell<bool>,
    pub(crate) conn: &'a Connection,
    pub(crate) stepping: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
struct StepGuard<'a>(&'a std::sync::atomic::AtomicBool);
impl Drop for StepGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Relaxed);
    }
}
pub(crate) fn sql_values(values: &[Value]) -> Result<Vec<rusqlite::types::Value>> {
    values
        .iter()
        .map(|v| {
            v.validate().map_err(Error::Invalid)?;
            Ok(match v {
                Value::Null => rusqlite::types::Value::Null,
                Value::Integer(v) => rusqlite::types::Value::Integer(*v),
                Value::Real(v) => rusqlite::types::Value::Real(*v),
                Value::Text(v) => rusqlite::types::Value::Text(v.clone()),
                Value::Blob(v) => rusqlite::types::Value::Blob(v.clone()),
            })
        })
        .collect()
}
pub(crate) fn owned(v: ValueRef<'_>) -> Result<Value> {
    let v = match v {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(v) => Value::Integer(v),
        ValueRef::Real(v) => Value::Real(v),
        ValueRef::Text(v) if v.len() > lfs_protocol::MAX_CELL => {
            return Err(Error::Invalid("cell limit"));
        }
        ValueRef::Blob(v) if v.len() > lfs_protocol::MAX_CELL => {
            return Err(Error::Invalid("cell limit"));
        }
        ValueRef::Text(v) => Value::Text(
            std::str::from_utf8(v)
                .map_err(|_| Error::Invalid("invalid UTF-8"))?
                .to_owned(),
        ),
        ValueRef::Blob(v) => Value::Blob(v.to_vec()),
    };
    v.validate().map_err(Error::Invalid)?;
    Ok(v)
}
pub(crate) fn query(conn: &Connection, sql: &str, values: &[Value]) -> Result<Vec<Vec<Value>>> {
    if sql.len() > 65536 {
        return Err(Error::Invalid("SQL length"));
    }
    let values = sql_values(values)?;
    let mut stmt = conn.prepare(sql)?;
    if !stmt.readonly() {
        return Err(Error::Invalid("read requires readonly statement"));
    }
    let n = stmt.column_count();
    if n > 64 {
        return Err(Error::Invalid("column limit"));
    }
    let mut rows = stmt.query(params_from_iter(values))?;
    let mut result = Vec::new();
    let mut size = 0;
    while let Some(row) = rows.next()? {
        if result.len() >= 10000 {
            return Err(Error::Invalid("query row limit"));
        }
        let mut out = Vec::new();
        for i in 0..n {
            let v = owned(row.get_ref(i)?)?;
            size += match &v {
                Value::Text(v) => v.len(),
                Value::Blob(v) => v.len(),
                _ => 8,
            };
            out.push(v);
        }
        if size > lfs_protocol::MAX_PACKET {
            return Err(Error::Invalid("query byte limit"));
        }
        result.push(out);
    }
    Ok(result)
}
impl Read<'_> {
    pub fn query(&self, sql: &str, values: &[Value]) -> Result<Vec<Vec<Value>>> {
        query(self.conn, sql, values)
    }
}
impl Write<'_> {
    fn require_transaction(&self) -> Result<()> {
        if self.tainted.get() {
            return Err(Error::Invalid("tainted write transaction"));
        }
        if self.conn.is_autocommit() {
            return Err(Error::Invalid("SQLite ended the write transaction"));
        }
        Ok(())
    }
    fn latch<T>(&self, f: impl FnOnce() -> Result<T>) -> Result<T> {
        let result = f();
        if result.is_err() {
            self.tainted.set(true);
        }
        result
    }
    pub fn execute(&self, sql: &str, values: &[Value]) -> Result<usize> {
        self.latch(|| self.execute_inner(sql, values))
    }
    fn execute_inner(&self, sql: &str, values: &[Value]) -> Result<usize> {
        self.require_transaction()?;
        if sql.len() > 65536 {
            return Err(Error::Invalid("SQL length"));
        }
        let values = sql_values(values)?;
        let mut statement = self.conn.prepare(sql)?;
        self.stepping
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let _guard = StepGuard(&self.stepping);
        let result = statement.execute(params_from_iter(values));
        self.require_transaction()?;
        Ok(result?)
    }
    pub fn query(&self, sql: &str, values: &[Value]) -> Result<Vec<Vec<Value>>> {
        self.latch(|| {
            self.require_transaction()?;
            query(self.conn, sql, values)
        })
    }
}
pub(crate) fn auth(
    conn: &Connection,
    schema: &Schema,
    write: bool,
    stepping: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Result<()> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    conn.progress_handler(1000, Some(move || std::time::Instant::now() >= deadline))?;
    let tables: BTreeSet<String> = schema.tables.iter().map(|t| t.name.clone()).collect();
    conn.authorizer(Some(move |ctx: AuthContext<'_>| {
        if ctx.accessor.is_some() || ctx.database_name.is_some_and(|d| d != "main") {
            return Authorization::Deny;
        }
        let allow = match ctx.action {
            // Session lazily inspects columns during the first sqlite3_step, after caller SQL was authorized.
            AuthAction::Pragma {
                pragma_name: "table_info" | "table_xinfo",
                pragma_value: Some(table),
            } => {
                stepping.load(std::sync::atomic::Ordering::Relaxed)
                    && write
                    && tables.contains(table)
            }
            AuthAction::Select => true,
            AuthAction::Read { table_name, .. } => tables.contains(table_name),
            AuthAction::Insert { table_name }
            | AuthAction::Delete { table_name }
            | AuthAction::Update { table_name, .. } => write && tables.contains(table_name),
            AuthAction::Function { function_name } => matches!(
                function_name,
                "length" | "coalesce" | "abs" | "count" | "min" | "max" | "typeof"
            ),
            _ => false,
        };
        if allow {
            Authorization::Allow
        } else {
            Authorization::Deny
        }
    }))?;
    Ok(())
}
pub(crate) fn clear_auth(conn: &Connection) -> Result<()> {
    conn.progress_handler(0, None::<fn() -> bool>)?;
    conn.authorizer(None::<fn(AuthContext<'_>) -> Authorization>)?;
    Ok(())
}
