use lfs_sqlite::*;
fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or(Error::Invalid("usage: local_loop NEW_DATABASE_PATH"))?;
    let schema = Schema {
        version: 1,
        tables: vec![Table {
            id: 1,
            name: "notes".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    kind: ColumnType::Text,
                    nullable: false,
                },
                Column {
                    name: "body".into(),
                    kind: ColumnType::Text,
                    nullable: false,
                },
            ],
        }],
    };
    let identity = Identity {
        dataset: "example".into(),
        device: "desktop".into(),
        incarnation: "original".into(),
    };
    let mut store = Store::open(&path, identity.clone(), schema.clone(), Baseline::Reject)?;
    let (_, commit) = store.write_transaction(|tx| {
        tx.execute(
            "INSERT INTO notes VALUES (?1,?2)",
            &[
                Value::Text("globally-unique-note".into()),
                Value::Text("offline value".into()),
            ],
        )
    })?;
    println!(
        "Committed: {:?}",
        commit
            .as_ref()
            .map(|c| (&c.pending.object_id, c.pending.bytes.len()))
    );
    println!(
        "Rows: {:?}",
        store.read(|db| db.query("SELECT * FROM notes", &[]))?
    );
    let before = store.pending_packets(10)?;
    drop(store);
    let mut store = Store::open(&path, identity, schema, Baseline::Reject)?;
    assert_eq!(before, store.pending_packets(10)?);
    println!(
        "Reopen preserved exact bytes; {} pending packets. No remote transfer performed.",
        before.len()
    );
    Ok(())
}
