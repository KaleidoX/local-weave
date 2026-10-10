use lfs_sqlite::*;

#[test]
fn caller_metadata_is_denied_and_atomic() {
    let root = std::env::temp_dir().join(format!("lfs-host-sqlite-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
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
    let mut store = Store::open(
        root.join("db"),
        Identity {
            dataset: "d".into(),
            device: "a".into(),
            incarnation: "i".into(),
        },
        schema,
        Baseline::Reject,
    )
    .unwrap();
    let first = store
        .write_transaction(|w| w.execute("INSERT INTO records VALUES ('k','initial')", &[]))
        .unwrap()
        .1
        .unwrap();
    let pending = store.pending_packets(100).unwrap();
    for sql in [
        "PRAGMA table_info(records)",
        "PRAGMA table_xinfo(records)",
        "PRAGMA main.table_info(records)",
        "PRAGMA main.table_xinfo(records)",
        "PRAGMA temp.table_info(records)",
        "PRAGMA table_info(_lfs_state)",
        "SELECT * FROM pragma_table_info('records')",
        "SELECT * FROM pragma_table_xinfo('records')",
        "INSERT INTO records SELECT name, type FROM pragma_table_info('records')",
        "INSERT INTO records SELECT name, type FROM pragma_table_xinfo('records')",
        "SELECT * FROM _lfs_state",
        "ATTACH ':memory:' AS other",
        "COMMIT",
        "CREATE TABLE rogue(x)",
        "SELECT load_extension('nope')",
    ] {
        assert!(
            store.read(|r| r.query(sql, &[])).is_err(),
            "read accepted {sql}"
        );
        for query in [false, true] {
            assert!(
                store
                    .write_transaction(|w| {
                        w.execute("UPDATE records SET value='rollback'", &[])?;
                        if query {
                            w.query(sql, &[]).map(|_| ())
                        } else {
                            w.execute(sql, &[]).map(|_| ())
                        }
                    })
                    .is_err(),
                "write accepted {sql}, query={query}"
            );
            assert_eq!(store.pending_packets(100).unwrap(), pending);
            assert_eq!(
                store
                    .read(|r| r.query("SELECT value FROM records", &[]))
                    .unwrap(),
                vec![vec![Value::Text("initial".into())]]
            );
        }
    }
    assert!(
        store
            .write_transaction::<()>(|w| {
                w.execute("UPDATE records SET value='callback failure'", &[])?;
                Err(Error::Caller("abort".into()))
            })
            .is_err()
    );
    assert_eq!(store.pending_packets(100).unwrap(), pending);
    assert_eq!(
        store
            .read(|r| r.query("SELECT value FROM records", &[]))
            .unwrap(),
        vec![vec![Value::Text("initial".into())]]
    );
    let updated = store
        .write_transaction(|w| w.execute("UPDATE records SET value='updated'", &[]))
        .unwrap()
        .1
        .unwrap();
    assert_eq!(updated.packet.seq, 2);
    assert_eq!(
        updated.packet.operations[0].parents,
        vec![first.packet.operations[0].id.encode()]
    );
    assert_eq!(
        updated.packet.operations[0].row,
        Some(vec![Value::Text("k".into()), Value::Text("updated".into())])
    );
    let deleted = store
        .write_transaction(|w| w.execute("DELETE FROM records", &[]))
        .unwrap()
        .1
        .unwrap();
    assert_eq!(deleted.packet.seq, 3);
    assert!(deleted.packet.operations[0].row.is_none());
}

#[test]
fn public_compatibility_checks_real_engine() {
    for _ in 0..2 {
        let report: SqliteCompatibility = check_sqlite_compatibility().unwrap();
        assert!(report.controlled_capture && report.rollback);
        assert!(report.runtime.session && report.runtime.preupdate_hook);
        assert_eq!(report.runtime.sqlite_version, rusqlite::version());
        println!("{report:?}");
    }
}
