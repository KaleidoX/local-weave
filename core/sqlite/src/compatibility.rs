use crate::access::{auth, clear_auth, owned, query};
use crate::{Column, ColumnType, Error, Result, RuntimeInfo, Schema, Table, Value, Write};
use fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::{Connection, hooks::Action, session::Session};
use std::{
    cell::Cell,
    sync::{Arc, atomic::AtomicBool},
};

/// Evidence from the linked engine, checked on a private in-memory connection.
#[derive(Clone, Debug)]
pub struct SqliteCompatibility {
    pub runtime: RuntimeInfo,
    pub controlled_capture: bool,
    pub rollback: bool,
}

pub(crate) fn runtime_info(conn: &Connection) -> Result<RuntimeInfo> {
    Ok(RuntimeInfo {
        sqlite_version: rusqlite::version().to_owned(),
        session: conn.query_row(
            "SELECT sqlite_compileoption_used('ENABLE_SESSION')",
            [],
            |r| r.get(0),
        )?,
        preupdate_hook: conn.query_row(
            "SELECT sqlite_compileoption_used('ENABLE_PREUPDATE_HOOK')",
            [],
            |r| r.get(0),
        )?,
    })
}

fn controlled(conn: &Connection, schema: &Schema, sql: &str) -> Result<()> {
    let stepping = Arc::new(AtomicBool::new(false));
    auth(conn, schema, true, stepping.clone())?;
    let tainted = Cell::new(false);
    let result = Write {
        conn,
        tainted: &tainted,
        stepping,
    }
    .execute(sql, &[]);
    clear_auth(conn)?;
    result?;
    Ok(())
}

fn capture(
    conn: &Connection,
    schema: &Schema,
    sql: &str,
    action: Action,
    old: Option<&str>,
    new: Option<&str>,
) -> Result<()> {
    conn.execute_batch("BEGIN IMMEDIATE")?;
    let mut session = Session::new(conn)?;
    session.attach(Some("records"))?;
    controlled(conn, schema, sql)?;
    let changes = session.changeset()?;
    let mut iter = changes.iter()?;
    let item = iter
        .next()?
        .ok_or(Error::Invalid("SQLite Session did not capture row"))?;
    let op = item.op()?;
    if op.table_name() != "records"
        || op.code() != action
        || op.number_of_columns() != 2
        || item.pk()? != [1, 0]
    {
        return Err(Error::Invalid("SQLite Session operation mismatch"));
    }
    let key = if action == Action::SQLITE_INSERT {
        item.new_value(0)?
    } else {
        item.old_value(0)?
    };
    if owned(key)? != Value::Text("key".into()) {
        return Err(Error::Invalid("SQLite Session key mismatch"));
    }
    if let Some(expected) = old
        && owned(item.old_value(1)?)? != Value::Text(expected.into())
    {
        return Err(Error::Invalid("SQLite Session old row mismatch"));
    }
    if let Some(expected) = new
        && owned(item.new_value(1)?)? != Value::Text(expected.into())
    {
        return Err(Error::Invalid("SQLite Session new row mismatch"));
    }
    if iter.next()?.is_some() {
        return Err(Error::Invalid("SQLite Session unexpected operation"));
    }
    conn.execute_batch("COMMIT")?;
    Ok(())
}

/// Checks real Session insert/update/delete capture and rollback using production
/// write authorization and stepping. No user paths or persistent files are used.
pub fn check_sqlite_compatibility() -> Result<SqliteCompatibility> {
    let conn = Connection::open_in_memory()?;
    let runtime = runtime_info(&conn)?;
    if !runtime.session || !runtime.preupdate_hook {
        return Err(Error::Invalid("SQLite Session runtime unavailable"));
    }
    let schema = Schema {
        version: 1,
        tables: vec![Table {
            id: 1,
            name: "records".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    kind: ColumnType::Text,
                    nullable: false,
                },
                Column {
                    name: "value".into(),
                    kind: ColumnType::Text,
                    nullable: false,
                },
            ],
        }],
    };
    schema.validate()?;
    conn.execute_batch(&schema.tables[0].create_sql())?;
    capture(
        &conn,
        &schema,
        "INSERT INTO records VALUES ('key','initial')",
        Action::SQLITE_INSERT,
        None,
        Some("initial"),
    )?;
    capture(
        &conn,
        &schema,
        "UPDATE records SET value='updated'",
        Action::SQLITE_UPDATE,
        Some("initial"),
        Some("updated"),
    )?;
    capture(
        &conn,
        &schema,
        "DELETE FROM records",
        Action::SQLITE_DELETE,
        Some("updated"),
        None,
    )?;
    if !query(&conn, "SELECT * FROM records", &[])?.is_empty() {
        return Err(Error::Invalid("SQLite Session delete mismatch"));
    }
    conn.execute_batch("BEGIN IMMEDIATE")?;
    let mut session = Session::new(&conn)?;
    session.attach(Some("records"))?;
    controlled(
        &conn,
        &schema,
        "INSERT INTO records VALUES ('rollback','discard')",
    )?;
    if session.changeset()?.iter()?.next()?.is_none() {
        return Err(Error::Invalid("SQLite Session rollback capture missing"));
    }
    conn.execute_batch("ROLLBACK")?;
    if !conn.is_autocommit()
        || !query(&conn, "SELECT * FROM records", &[])?.is_empty()
        || session.changeset()?.iter()?.next()?.is_some()
    {
        return Err(Error::Invalid("SQLite Session rollback mismatch"));
    }
    Ok(SqliteCompatibility {
        runtime,
        controlled_capture: true,
        rollback: true,
    })
}
