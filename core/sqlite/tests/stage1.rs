use lfs_sqlite::*;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static SERIAL: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "lfs-s1-{}-{}.db",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        )))
    }
    fn open(&self) -> Store {
        Store::open(&self.0, identity(), schema(), Baseline::Reject).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
        let _ = std::fs::remove_file(format!("{}-journal", self.0.display()));
    }
}
fn identity() -> Identity {
    Identity {
        dataset: "dataset".into(),
        device: "device".into(),
        incarnation: "incarnation".into(),
    }
}
fn schema() -> Schema {
    Schema {
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
                    name: "title".into(),
                    kind: ColumnType::Text,
                    nullable: false,
                },
                Column {
                    name: "counter".into(),
                    kind: ColumnType::Integer,
                    nullable: false,
                },
                Column {
                    name: "data".into(),
                    kind: ColumnType::Blob,
                    nullable: true,
                },
                Column {
                    name: "ratio".into(),
                    kind: ColumnType::Real,
                    nullable: true,
                },
            ],
        }],
    }
}
fn insert(w: &Write<'_>, key: &str) -> Result<usize> {
    w.execute(
        "INSERT INTO records VALUES (?1,'initial',9223372036854775807,x'00ff',1.25)",
        &[Value::Text(key.into())],
    )
}
fn count(s: &mut Store) -> i64 {
    let rows = s
        .read(|r| r.query("SELECT count(*) FROM records", &[]))
        .unwrap();
    let Value::Integer(v) = rows[0][0] else {
        panic!()
    };
    v
}
#[test]
fn runtime_capture_full_rows_pk_move_and_delete() {
    let f = Fixture::new();
    let mut s = f.open();
    let runtime = s.runtime_info().unwrap();
    println!(
        "SQLite {} ENABLE_SESSION={} ENABLE_PREUPDATE_HOOK={}",
        runtime.sqlite_version, runtime.session, runtime.preupdate_hook
    );
    assert!(runtime.session && runtime.preupdate_hook);
    let (_, first) = s.write_transaction(|w| insert(w, "one")).unwrap();
    let first = first.unwrap();
    assert_eq!(first.packet.seq, 1);
    assert!(first.packet.operations[0].parents.is_empty());
    assert_eq!(
        first.packet.operations[0].row.as_ref().unwrap(),
        &vec![
            Value::Text("one".into()),
            Value::Text("initial".into()),
            Value::Integer(i64::MAX),
            Value::Blob(vec![0, 255]),
            Value::Real(1.25)
        ]
    );
    let (_, updated) = s
        .write_transaction(|w| w.execute("UPDATE records SET title='updated' WHERE id='one'", &[]))
        .unwrap();
    let updated = updated.unwrap();
    let row = updated.packet.operations[0].row.as_ref().unwrap();
    assert_eq!(row[2], Value::Integer(i64::MAX));
    assert_eq!(row[3], Value::Blob(vec![0, 255]));
    assert_eq!(
        updated.packet.operations[0].parents,
        vec![first.packet.operations[0].id.encode()]
    );
    assert_eq!(updated.packet.predecessor, Some(first.pending.hash));
    let (_, moved) = s
        .write_transaction(|w| w.execute("UPDATE records SET id='two' WHERE id='one'", &[]))
        .unwrap();
    let moved = moved.unwrap();
    assert_eq!(moved.packet.operations.len(), 2);
    assert_eq!(moved.packet.operations[0].key, "one");
    assert!(moved.packet.operations[0].row.is_none());
    assert_eq!(moved.packet.operations[1].key, "two");
    assert_eq!(
        moved.packet.operations[1].row.as_ref().unwrap()[1],
        Value::Text("updated".into())
    );
    let (_, deleted) = s
        .write_transaction(|w| w.execute("DELETE FROM records", &[]))
        .unwrap();
    assert!(deleted.unwrap().packet.operations[0].row.is_none());
    assert_eq!(count(&mut s), 0);
}
#[test]
fn rollback_panic_no_net_and_fresh_sessions() {
    let f = Fixture::new();
    let mut s = f.open();
    assert!(
        s.write_transaction(|w| {
            insert(w, "bad")?;
            Err::<(), _>(Error::Caller("abort".into()))
        })
        .is_err()
    );
    assert_eq!(count(&mut s), 0);
    assert!(s.pending_packets(10).unwrap().is_empty());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = s.write_transaction::<()>(|w| {
            insert(w, "panic")?;
            panic!("caller panic")
        });
    }));
    assert!(panic.is_err());
    assert_eq!(count(&mut s), 0);
    let read_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = s.read::<()>(|_| panic!("read panic"));
    }));
    assert!(read_panic.is_err());
    let (_, net) = s
        .write_transaction(|w| {
            insert(w, "zero")?;
            w.execute("DELETE FROM records", &[])
        })
        .unwrap();
    assert!(net.is_none());
    let (_, first) = s.write_transaction(|w| insert(w, "kept")).unwrap();
    assert_eq!(first.unwrap().packet.seq, 1);
    let (_, net) = s
        .write_transaction(|w| {
            w.execute("UPDATE records SET title='temp'", &[])?;
            w.execute("UPDATE records SET title='initial'", &[])
        })
        .unwrap();
    assert!(net.is_none());
    let (_, second) = s
        .write_transaction(|w| w.execute("UPDATE records SET title='next'", &[]))
        .unwrap();
    assert_eq!(second.unwrap().packet.seq, 2);
    assert_eq!(s.pending_packets(10).unwrap().len(), 2);
}
#[test]
fn reopen_exact_retry_acknowledgement_and_sequence() {
    let f = Fixture::new();
    let mut s = f.open();
    let (_, commit) = s.write_transaction(|w| insert(w, "one")).unwrap();
    let first = commit.unwrap();
    let original = s.pending_packets(10).unwrap();
    drop(s);
    let mut s = f.open();
    assert_eq!(s.pending_packets(10).unwrap(), original);
    assert!(s.acknowledge(1, [0; 32]).is_err());
    assert_eq!(s.pending_packets(10).unwrap(), original);
    s.acknowledge(1, first.pending.hash).unwrap();
    s.acknowledge(1, first.pending.hash).unwrap();
    assert!(s.pending_packets(10).unwrap().is_empty());
    drop(s);
    let mut s = f.open();
    assert!(s.pending_packets(10).unwrap().is_empty());
    let (_, next) = s
        .write_transaction(|w| w.execute("DELETE FROM records", &[]))
        .unwrap();
    let next = next.unwrap();
    assert_eq!(next.packet.seq, 2);
    assert_eq!(
        next.packet.operations[0].parents,
        vec![first.packet.operations[0].id.encode()]
    );
}
#[test]
fn caller_authorizer_denies_and_read_is_readonly() {
    let f = Fixture::new();
    let mut s = f.open();
    s.write_transaction(|w| insert(w, "one")).unwrap();
    for sql in [
        "DELETE FROM _lfs_outbox",
        "SELECT * FROM _lfs_state",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT x",
        "ATTACH ':memory:' AS other",
        "DETACH main",
        "PRAGMA user_version=2",
        "PRAGMA table_xinfo(records)",
        "UPDATE records SET title='x'; COMMIT",
        "CREATE TABLE rogue (x)",
        "DROP TABLE records",
        "ALTER TABLE records ADD COLUMN x",
        "CREATE TEMP TABLE rogue(x)",
        "INSERT INTO unregistered VALUES (1)",
        "SELECT load_extension('nope')",
        "SELECT writefile('/tmp/lfs-unauthorized','x')",
        "SELECT * FROM pragma_table_info('records')",
        "DELETE FROM temp.records",
    ] {
        let r = s.write_transaction(|w| {
            w.execute("UPDATE records SET title='should_rollback'", &[])?;
            w.execute(sql, &[])
        });
        assert!(r.is_err(), "accepted: {sql}");
        assert_eq!(
            s.read(|r| r.query("SELECT title FROM records", &[]))
                .unwrap(),
            vec![vec![Value::Text("initial".into())]]
        );
    }
    assert!(s.read(|r| r.query("DELETE FROM records", &[])).is_err());
    assert!(
        s.read(|r| r.query("SELECT * FROM _lfs_heads", &[]))
            .is_err()
    );
    assert_eq!(count(&mut s), 1);
    assert_eq!(s.pending_packets(10).unwrap().len(), 1);
}
#[test]
fn exclusive_ownership_and_external_connection() {
    let f = Fixture::new();
    let mut s = f.open();
    assert!(matches!(
        Store::open(&f.0, identity(), schema(), Baseline::Reject),
        Err(Error::DatabaseInUse)
    ));
    let external = rusqlite::Connection::open(&f.0).unwrap();
    external.busy_timeout(std::time::Duration::ZERO).unwrap();
    assert!(
        external
            .execute(
                "INSERT INTO records(id,title,counter) VALUES ('rogue','x',0)",
                []
            )
            .is_err()
    );
    drop(external);
    assert_eq!(count(&mut s), 0);
}
#[test]
fn explicit_baseline_and_unknown_schema_refusal() {
    let f = Fixture::new();
    let c = rusqlite::Connection::open(&f.0).unwrap();
    c.execute_batch(&schema().tables[0].create_sql()).unwrap();
    c.execute(
        "INSERT INTO records VALUES ('old','before SDK',9,NULL,NULL)",
        [],
    )
    .unwrap();
    drop(c);
    assert!(matches!(
        Store::open(&f.0, identity(), schema(), Baseline::Reject),
        Err(Error::BaselineRequired)
    ));
    let mut s = Store::open(&f.0, identity(), schema(), Baseline::Import).unwrap();
    assert_eq!(count(&mut s), 1);
    let baseline = s.pending_packets(10).unwrap();
    assert_eq!(baseline[0].seq, 1);
    let (_, update) = s
        .write_transaction(|w| w.execute("UPDATE records SET title='after'", &[]))
        .unwrap();
    assert_eq!(update.unwrap().packet.operations[0].parents.len(), 1);
    drop(s);
    let mut s = f.open();
    assert_eq!(s.pending_packets(10).unwrap()[0], baseline[0]);
    drop(s);
    let c = rusqlite::Connection::open(&f.0).unwrap();
    c.execute_batch("CREATE TABLE rogue (x)").unwrap();
    drop(c);
    assert!(matches!(
        Store::open(&f.0, identity(), schema(), Baseline::Reject),
        Err(Error::Schema(_))
    ));
}
#[test]
fn unsupported_data_and_schema_rollback() {
    let f = Fixture::new();
    let mut s = f.open();
    for sql in [
        "INSERT INTO records(title,counter) VALUES ('x',1)",
        "INSERT INTO records VALUES ('x','x',1,NULL,1e999)",
        "INSERT INTO records VALUES ('x',CAST(x'ff' AS TEXT),1,NULL,NULL)",
    ] {
        assert!(s.write_transaction(|w| w.execute(sql, &[])).is_err());
        assert_eq!(count(&mut s), 0);
    }
    assert!(
        s.write_transaction(|w| w.execute(
            "INSERT INTO records VALUES ('x','x',1,NULL,?1)",
            &[Value::Real(f64::NAN)]
        ))
        .is_err()
    );
    assert!(s.pending_packets(10).unwrap().is_empty());
    drop(s);
    let mut wrong = identity();
    wrong.incarnation = "clone".into();
    assert!(matches!(
        Store::open(&f.0, wrong, schema(), Baseline::Reject),
        Err(Error::IdentityMismatch)
    ));
}
#[test]
fn canonical_schema_only_and_sql_cannot_bypass_prepare() {
    for extra in [
        "CREATE TRIGGER bad AFTER INSERT ON records BEGIN DELETE FROM records; END",
        "CREATE UNIQUE INDEX bad ON records(title)",
        "CREATE VIEW bad AS SELECT * FROM records",
    ] {
        let f = Fixture::new();
        let c = rusqlite::Connection::open(&f.0).unwrap();
        c.execute_batch(&schema().tables[0].create_sql()).unwrap();
        c.execute_batch(extra).unwrap();
        drop(c);
        assert!(matches!(
            Store::open(&f.0, identity(), schema(), Baseline::Reject),
            Err(Error::Schema(_))
        ));
        let c = rusqlite::Connection::open(&f.0).unwrap();
        let n: i64 = c
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE name='bad'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
    }
    let f = Fixture::new();
    let mut s = f.open();
    // R5: denied caller PRAGMA taints the callback, including its earlier insert.
    assert!(
        s.write_transaction(|w| {
            insert(w, "one")?;
            assert!(w.query("PRAGMA table_xinfo(records)", &[]).is_err());
            assert!(w.execute("PRAGMA table_xinfo(records)", &[]).is_err());
            assert!(w.execute("UPDATE records SET title='safe'", &[]).is_err());
            Ok(())
        })
        .is_err()
    );
    assert_eq!(count(&mut s), 0);
    assert!(s.pending_packets(10).unwrap().is_empty());
}
#[test]
fn separate_process_cannot_write_live_database() {
    let f = Fixture::new();
    let _store = f.open();
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "process_lock_probe", "--nocapture"])
        .env("LFS_STAGE1_LOCK_PROBE", &f.0)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}
#[test]
fn process_lock_probe() {
    let Some(path) = std::env::var_os("LFS_STAGE1_LOCK_PROBE") else {
        return;
    };
    assert!(matches!(
        Store::open(&path, identity(), schema(), Baseline::Reject),
        Err(Error::DatabaseInUse)
    ));
    let c = rusqlite::Connection::open(path).unwrap();
    c.busy_timeout(std::time::Duration::ZERO).unwrap();
    assert!(
        c.execute(
            "INSERT INTO records VALUES ('external','x',0,NULL,NULL)",
            []
        )
        .is_err()
    );
}
#[test]
fn sqlite_implicit_rollback_cannot_escape_atomic_boundary() {
    let f = Fixture::new();
    let mut s = f.open();
    let result = s.write_transaction(|w| {
        insert(w, "one")?;
        assert!(
            w.execute(
                "INSERT OR ROLLBACK INTO records VALUES ('one','duplicate',1,NULL,NULL)",
                &[]
            )
            .is_err()
        );
        assert!(insert(w, "escaped").is_err());
        assert!(w.query("SELECT * FROM records", &[]).is_err());
        Ok(()) // A caller swallowing the SQL error still cannot commit or write outside the transaction.
    });
    assert!(result.is_err());
    assert_eq!(count(&mut s), 0);
    assert!(s.pending_packets(10).unwrap().is_empty());
    let (_, next) = s.write_transaction(|w| insert(w, "next")).unwrap();
    assert_eq!(next.unwrap().packet.seq, 1);
}
