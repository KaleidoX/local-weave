// Mandated gray-box persistence fault: actual SQLite constraint failure after outbox insertion.
use super::*;
#[test]
fn persistence_failure_rolls_back_business_sequence_versions_and_outbox() {
    let path = std::env::temp_dir().join(format!("lfs-fault-{}.db", std::process::id()));
    let schema = Schema {
        version: 1,
        tables: vec![Table {
            id: 1,
            name: "items".into(),
            columns: vec![Column {
                name: "id".into(),
                kind: ColumnType::Text,
                nullable: false,
            }],
        }],
    };
    let identity = Identity {
        dataset: "d".into(),
        device: "a".into(),
        incarnation: "i".into(),
    };
    let mut s = Store::open(&path, identity, schema, Baseline::Reject).unwrap();
    s.fail_after_outbox = true;
    assert!(
        s.write_transaction(|w| w.execute("INSERT INTO items VALUES ('a')", &[]))
            .is_err()
    );
    assert!(
        s.read(|r| r.query("SELECT * FROM items", &[]))
            .unwrap()
            .is_empty()
    );
    assert!(s.pending_packets(10).unwrap().is_empty());
    for table in [
        "_lfs_versions",
        "_lfs_heads",
        "_lfs_outbox",
        "_lfs_outgoing_wire",
        "_lfs_delivery",
        "_lfs_packets",
    ] {
        let n: i64 = s
            .conn
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }
    s.fail_after_outbox = false;
    let (_, commit) = s
        .write_transaction(|w| w.execute("INSERT INTO items VALUES ('a')", &[]))
        .unwrap();
    let commit = commit.unwrap();
    assert_eq!(commit.packet.seq, 1);
    assert!(commit.packet.operations[0].parents.is_empty());
    drop(s);
    std::fs::remove_file(path).unwrap();
}

#[test]
fn remote_failure_after_projection_rolls_back_dag_inbox_progress_and_no_echo() {
    let path = std::env::temp_dir().join(format!("lfs-remote-fault-{}.db", std::process::id()));
    let schema = Schema {
        version: 1,
        tables: vec![Table {
            id: 1,
            name: "items".into(),
            columns: vec![Column {
                name: "id".into(),
                kind: ColumnType::Text,
                nullable: false,
            }],
        }],
    };
    let id = Identity {
        dataset: "d".into(),
        device: "a".into(),
        incarnation: "i".into(),
    };
    let mut s = Store::open(&path, id.clone(), schema.clone(), Baseline::Reject).unwrap();
    let mut remote = id;
    remote.device = "remote".into();
    let packet = Packet {
        identity: remote.clone(),
        seq: 1,
        schema: 1,
        predecessor: None,
        operations: vec![Operation {
            id: OpId {
                dataset: remote.dataset.clone(),
                device: remote.device.clone(),
                incarnation: remote.incarnation.clone(),
                seq: 1,
                index: 0,
                schema: 1,
            },
            table: 1,
            key: "k".into(),
            parents: vec![],
            row: Some(vec![Value::Text("k".into())]),
        }],
    };
    let bytes = packet.encode().unwrap();
    let package = Package {
        key: PacketKey {
            identity: remote,
            seq: 1,
            hash: hash(&bytes),
        },
        bytes: bytes.clone(),
        fingerprint: schema.fingerprint(),
        attachment_references: vec![],
    };
    let key = package.key.clone();
    s.fail_after_remote = true;
    assert!(s.receive(package).is_err());
    assert!(
        s.read(|r| r.query("SELECT * FROM items", &[]))
            .unwrap()
            .is_empty()
    );
    assert!(s.heads(1, "k").unwrap().is_empty());
    assert!(s.applied_packets().unwrap().is_empty());
    assert!(s.pending_packets(10).unwrap().is_empty());
    assert_eq!(s.retained_bytes(&key).unwrap(), bytes);
    assert_eq!(s.status().unwrap().inbox[0].state, InboxState::Failed);
    for table in ["_lfs_nodes", "_lfs_edges", "_lfs_tips", "_lfs_packets"] {
        let n: i64 = s
            .conn
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0);
    }
    drop(s);
    std::fs::remove_file(path).unwrap();
}
#[test]
fn stage1_metadata_migration_preserves_import_identity_bytes_and_ack() {
    let path = std::env::temp_dir().join(format!("lfs-migrate-{}.db", std::process::id()));
    let schema = Schema {
        version: 1,
        tables: vec![Table {
            id: 1,
            name: "items".into(),
            columns: vec![Column {
                name: "id".into(),
                kind: ColumnType::Text,
                nullable: false,
            }],
        }],
    };
    let identity = Identity {
        dataset: "d".into(),
        device: "a".into(),
        incarnation: "i".into(),
    };
    let p = Packet {
        identity: identity.clone(),
        seq: 1,
        schema: 1,
        predecessor: None,
        operations: vec![Operation {
            id: OpId {
                dataset: "d".into(),
                device: "a".into(),
                incarnation: "i".into(),
                seq: 1,
                index: 0,
                schema: 1,
            },
            table: 1,
            key: "baseline".into(),
            parents: vec![],
            row: Some(vec![Value::Text("baseline".into())]),
        }],
    };
    let bytes = p.encode().unwrap();
    let h = hash(&bytes);
    {
        let c = Connection::open(&path).unwrap();
        c.execute_batch(&schema.tables[0].create_sql()).unwrap();
        for (_, ddl) in INTERNAL {
            c.execute_batch(ddl).unwrap();
        }
        c.execute("INSERT INTO items VALUES ('baseline')", [])
            .unwrap();
        c.execute(
            "INSERT INTO _lfs_state VALUES (1,'d','a','i',?1,2,?2)",
            params![schema.fingerprint(), h.as_slice()],
        )
        .unwrap();
        c.execute(
            "INSERT INTO _lfs_outbox VALUES (1,?1,?2,0)",
            params![h.as_slice(), bytes],
        )
        .unwrap();
        c.execute(
            "INSERT INTO _lfs_versions VALUES (?1,1,'baseline',NULL,1,0)",
            [p.operations[0].id.encode()],
        )
        .unwrap();
        c.execute(
            "INSERT INTO _lfs_heads VALUES (1,'baseline',?1)",
            [p.operations[0].id.encode()],
        )
        .unwrap();
    }
    let mut s = Store::open(&path, identity.clone(), schema.clone(), Baseline::Reject).unwrap();
    assert_eq!(s.pending_packets(10).unwrap()[0].bytes, bytes);
    assert_eq!(s.heads(1, "baseline").unwrap(), p.operations);
    s.acknowledge(1, h).unwrap();
    drop(s);
    let mut s = Store::open(&path, identity, schema, Baseline::Reject).unwrap();
    assert!(s.pending_packets(10).unwrap().is_empty());
    let c = s
        .write_transaction(|w| w.execute("DELETE FROM items WHERE id='baseline'", &[]))
        .unwrap()
        .1
        .unwrap();
    assert_eq!(c.packet.seq, 2);
    assert_eq!(c.packet.predecessor, Some(h));
    assert_eq!(
        c.packet.operations[0].parents,
        vec![p.operations[0].id.encode()]
    );
    drop(s);
    std::fs::remove_file(path).unwrap();
}

// Mandated pre-release v2 transition and canceled wire custody recovery.
#[test]
fn stage2_wire_transition_freezes_once_and_cancelled_receipt_can_retry() {
    let path = std::env::temp_dir().join(format!("lfs-wire-transition-{}.db", std::process::id()));
    let schema = Schema {
        version: 1,
        tables: vec![Table {
            id: 1,
            name: "items".into(),
            columns: vec![Column {
                name: "id".into(),
                kind: ColumnType::Text,
                nullable: false,
            }],
        }],
    };
    let identity = Identity {
        dataset: "d".into(),
        device: "a".into(),
        incarnation: "i".into(),
    };
    let mut s = Store::open(&path, identity.clone(), schema.clone(), Baseline::Reject).unwrap();
    let commit = s
        .write_transaction(|w| w.execute("INSERT INTO items VALUES ('k')", &[]))
        .unwrap()
        .1
        .unwrap();
    let expected = s.pending_envelopes(1).unwrap();
    for (name, _) in envelope::INTERNAL3 {
        s.conn.execute_batch(&format!("DROP TABLE {name}")).unwrap();
    }
    drop(s);
    let mut s = Store::open(&path, identity, schema.clone(), Baseline::Reject).unwrap();
    assert_eq!(s.pending_envelopes(1).unwrap(), expected);
    assert_eq!(s.pending_packets(1).unwrap()[0], commit.pending);
    let mut remote = commit.packet.clone();
    remote.identity.device = "remote".into();
    remote.operations[0].id.device = "remote".into();
    let bytes = remote.encode().unwrap();
    let package = Package {
        key: PacketKey {
            identity: remote.identity,
            seq: 1,
            hash: hash(&bytes),
        },
        fingerprint: schema.fingerprint(),
        bytes,
        attachment_references: vec![],
    };
    let key = package.object_key().unwrap();
    assert!(
        s.receive_object(&key, package.clone(), || Err(Error::Network(
            NetworkError::Cancelled
        )))
        .is_err()
    );
    assert!(!s.known_objects().unwrap().contains(&key));
    assert_eq!(
        s.receive_object(&key, package, || Ok(())).unwrap(),
        InboxState::Applied
    );
    assert!(s.known_objects().unwrap().contains(&key));
    drop(s);
    std::fs::remove_file(path).unwrap();
}
