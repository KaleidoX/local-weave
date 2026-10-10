#![allow(dead_code)]
use lfs_sqlite::*;
use lfs_storage::*;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
static SERIAL: AtomicU64 = AtomicU64::new(0);
pub struct Fixture(pub PathBuf);
impl Fixture {
    pub fn new() -> Self {
        let base = std::env::var_os("TMPDIR").expect("TMPDIR required; never fall back to /tmp");
        let path = PathBuf::from(base).join(format!(
            "lfs-stage3-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        std::fs::create_dir(path.join("objects")).unwrap();
        Self(path)
    }
    pub fn sdk(&self, d: &str) -> Sdk {
        Sdk::open(
            self.0.join(format!("{d}.db")),
            identity(d),
            schema(),
            Baseline::Reject,
        )
        .unwrap()
    }
    pub fn transport(&self) -> OpenDalTransport {
        OpenDalTransport::fs(self.0.join("objects"), Options::default()).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
pub fn identity(d: &str) -> Identity {
    Identity {
        dataset: "data/../集?#%".into(),
        device: d.into(),
        incarnation: "inc/../?#%".into(),
    }
}
pub fn schema() -> Schema {
    Schema {
        version: 1,
        tables: vec![Table {
            id: 1,
            name: "items".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    kind: ColumnType::Text,
                    nullable: false,
                },
                Column {
                    name: "text".into(),
                    kind: ColumnType::Text,
                    nullable: false,
                },
                Column {
                    name: "n".into(),
                    kind: ColumnType::Integer,
                    nullable: false,
                },
                Column {
                    name: "r".into(),
                    kind: ColumnType::Real,
                    nullable: false,
                },
                Column {
                    name: "b".into(),
                    kind: ColumnType::Blob,
                    nullable: false,
                },
                Column {
                    name: "nil".into(),
                    kind: ColumnType::Text,
                    nullable: true,
                },
            ],
        }],
    }
}
pub fn insert(s: &Sdk, key: &str) {
    s.write_transaction(|w| {
        w.execute(
            "INSERT INTO items VALUES (?1,'base',?2,1.5,x'00ff',NULL)",
            &[Value::Text(key.into()), Value::Integer(i64::MAX)],
        )
    })
    .unwrap();
}
pub fn rows(s: &Sdk) -> Vec<Vec<Value>> {
    s.read(|r| r.query("SELECT * FROM items ORDER BY id", &[]))
        .unwrap()
}
pub async fn sync(s: &Sdk, t: &impl Transport) {
    let r = s
        .sync_once(
            t,
            &Cancellation::default(),
            SyncLimits {
                page_size: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(r.healthy(), "{r:?}");
}
pub async fn journey(f: &Fixture, t: &impl Transport) {
    let a = f.sdk("a");
    let b = f.sdk("b");
    let c = f.sdk("c");
    insert(&a, "k");
    sync(&a, t).await;
    sync(&b, t).await;
    sync(&c, t).await;
    assert_eq!(
        rows(&b),
        vec![vec![
            Value::Text("k".into()),
            Value::Text("base".into()),
            Value::Integer(i64::MAX),
            Value::Real(1.5),
            Value::Blob(vec![0, 255]),
            Value::Null
        ]]
    );
    a.write_transaction(|w| w.execute("UPDATE items SET text='a' WHERE id='k'", &[]))
        .unwrap();
    b.write_transaction(|w| w.execute("UPDATE items SET text='b' WHERE id='k'", &[]))
        .unwrap();
    c.write_transaction(|w| w.execute("DELETE FROM items WHERE id='k'", &[]))
        .unwrap();
    sync(&a, t).await;
    sync(&b, t).await;
    sync(&c, t).await;
    sync(&a, t).await;
    sync(&b, t).await;
    let conflicts = a.list_conflicts().unwrap();
    assert_eq!(conflicts[0].heads.len(), 3);
    assert_eq!(conflicts, b.list_conflicts().unwrap());
    assert_eq!(conflicts, c.list_conflicts().unwrap());
    assert_eq!(rows(&a), rows(&c));
    let observed = conflicts[0].heads.iter().map(|h| h.id.clone()).collect();
    a.resolve(
        1,
        "k",
        observed,
        Some(vec![
            Value::Text("k".into()),
            Value::Text("resolved".into()),
            Value::Integer(-7),
            Value::Real(2.5),
            Value::Blob(vec![128]),
            Value::Null,
        ]),
    )
    .unwrap();
    sync(&a, t).await;
    sync(&b, t).await;
    sync(&c, t).await;
    assert_eq!(rows(&a), rows(&b));
    assert_eq!(rows(&a), rows(&c));
    assert!(b.list_conflicts().unwrap().is_empty());
    assert_eq!(rows(&b)[0][1], Value::Text("resolved".into()));
    assert!(c.pending_envelopes(10).unwrap().is_empty());
    assert!(b.received_envelopes().unwrap().len() >= 4);
    println!(
        "SDK readback PASS: 3 devices, typed row, update/update/delete conflict, explicit resolve, no echo"
    );
}
