use lfs_sqlite::*;
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
static N: AtomicU64 = AtomicU64::new(0);
fn schema() -> Schema {
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
                    name: "body".into(),
                    kind: ColumnType::Text,
                    nullable: false,
                },
            ],
        }],
    }
}
struct Fixture(PathBuf);
impl std::ops::Deref for Fixture {
    type Target = PathBuf;
    fn deref(&self) -> &PathBuf {
        &self.0
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn fixture() -> Fixture {
    let p = std::env::temp_dir().join(format!(
        "lfs-review-{}-{}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&p).unwrap();
    Fixture(p)
}
fn store(p: &Path, name: &str, schema: Schema) -> Store {
    Store::open(
        p.join(format!("{name}.db")),
        Identity {
            dataset: "probe".into(),
            device: name.into(),
            incarnation: "i1".into(),
        },
        schema,
        Baseline::Reject,
    )
    .unwrap()
}
#[test]
fn r1_invalid_is_not_fork() {
    let p = fixture();
    let s = schema();
    let mut a = store(&p, "a", s.clone());
    let c = a
        .write_transaction(|w| w.execute("INSERT INTO items VALUES ('k','v')", &[]))
        .unwrap()
        .1
        .unwrap();
    let good = Package::from_pending(c.pending, &s);
    for first in [false, true] {
        let mut b = store(&p, if first { "first" } else { "after" }, s.clone());
        if !first {
            b.receive(good.clone()).unwrap();
        }
        let mut bad = good.clone();
        bad.key.hash = [99; 32];
        assert!(b.receive(bad).is_err());
        assert_eq!(b.status().unwrap().stopped_streams, 0);
        assert_eq!(b.receive(good.clone()).unwrap(), InboxState::Applied);
        for variant in 0..3 {
            let mut bad = good.clone();
            match variant {
                0 => bad.fingerprint.push('x'),
                1 => bad.attachment_references = vec!["exact-ref".into()],
                _ => {
                    bad.bytes.pop();
                }
            }
            assert!(b.receive(bad).is_err());
            assert_eq!(b.status().unwrap().stopped_streams, 0);
            assert_eq!(
                b.read(|r| r.query("SELECT * FROM items", &[]))
                    .unwrap()
                    .len(),
                1
            );
        }
        let evidence = b.failed_attempts().unwrap();
        assert_eq!(evidence.len(), 4);
        assert!(
            evidence
                .iter()
                .any(|a| a.bytes.windows(9).any(|w| w == b"exact-ref"))
        );
        let mut packet = c.packet.clone();
        packet.operations[0].row.as_mut().unwrap()[1] = Value::Text("fork".into());
        let bytes = packet.encode().unwrap();
        let mut fork = good.clone();
        fork.key.hash = lfs_protocol::hash(&bytes);
        fork.bytes = bytes;
        assert!(matches!(b.receive(fork), Err(Error::StreamFork)));
        assert_eq!(b.status().unwrap().stopped_streams, 1);
    }
}

fn run<F: std::future::Future>(f: F) -> F::Output {
    let w = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(w);
    let mut f = std::pin::pin!(f);
    match f.as_mut().poll(&mut cx) {
        std::task::Poll::Ready(v) => v,
        _ => panic!("memory transport should be ready"),
    }
}
fn sdk(p: &Path, name: &str) -> Sdk {
    Sdk::open(
        p.join(format!("{name}.db")),
        Identity {
            dataset: "probe".into(),
            device: name.into(),
            incarnation: "i1".into(),
        },
        schema(),
        Baseline::Reject,
    )
    .unwrap()
}
#[test]
fn r3_memory_corruption_recovers_after_restart() {
    let p = fixture();
    let mut a = store(&p, "a", schema());
    let c = a
        .write_transaction(|w| w.execute("INSERT INTO items VALUES ('k','v')", &[]))
        .unwrap()
        .1
        .unwrap();
    let good = Package::from_pending(c.pending, &schema());
    let net = MemoryTransport::default();
    net.put(good.clone()).unwrap();
    let b = sdk(&p, "b");
    net.inject(Fault::CorruptDownload).unwrap();
    let _ = run(b.sync_once(&net, &Cancellation::default(), SyncLimits::default()));
    assert!(!b.status().unwrap().healthy());
    b.close().unwrap();
    drop(b);
    let b = sdk(&p, "b");
    let report = run(b.sync_once(&net, &Cancellation::default(), SyncLimits::default())).unwrap();
    assert!(report.healthy(), "{report:?}");
    assert_eq!(
        b.read(|r| r.query("SELECT * FROM items", &[]))
            .unwrap()
            .len(),
        1
    );
    b.close().unwrap();
    drop(b);
    let mut b = store(&p, "b", schema());
    let attempts = b.failed_attempts().unwrap();
    assert_eq!(attempts.len(), 1);
    assert!(attempts[0].retryable);
    assert_ne!(attempts[0].bytes, good.encode_envelope().unwrap());
}

#[test]
fn r2_fork_isolated_healthy_streams_progress_across_restart() {
    let p = fixture();
    let mut a = store(&p, "a", schema());
    let mut b = store(&p, "b", schema());
    let mut c = store(&p, "c", schema());
    let first = a
        .write_transaction(|w| w.execute("INSERT INTO items VALUES ('a','first')", &[]))
        .unwrap()
        .1
        .unwrap();
    let good = Package::from_pending(first.pending, &schema());
    b.receive(good.clone()).unwrap();
    let mut fork = first.packet;
    fork.operations[0].row.as_mut().unwrap()[1] = Value::Text("fork".into());
    let bytes = fork.encode().unwrap();
    let mut bad = good.clone();
    bad.key.hash = lfs_protocol::hash(&bytes);
    bad.bytes = bytes;
    assert!(matches!(b.receive(bad), Err(Error::StreamFork)));
    let a2 = a
        .write_transaction(|w| w.execute("UPDATE items SET body='forbidden'", &[]))
        .unwrap()
        .1
        .unwrap();
    b.write_transaction(|w| w.execute("INSERT INTO items VALUES ('b','pending')", &[]))
        .unwrap();
    let c1 = c
        .write_transaction(|w| w.execute("INSERT INTO items VALUES ('c','one')", &[]))
        .unwrap()
        .1
        .unwrap();
    let c2 = c
        .write_transaction(|w| w.execute("UPDATE items SET body='two'", &[]))
        .unwrap()
        .1
        .unwrap();
    assert_eq!(
        b.receive(Package::from_pending(c2.pending, &schema()))
            .unwrap(),
        InboxState::WaitingDependencies
    );
    drop(b);
    let b = sdk(&p, "b");
    let net = MemoryTransport::default();
    net.put(Package::from_pending(c1.pending, &schema()))
        .unwrap();
    net.put(Package::from_pending(a2.pending, &schema()))
        .unwrap();
    let report = run(b.sync_once(&net, &Cancellation::default(), SyncLimits::default())).unwrap();
    assert!(!report.healthy());
    assert_eq!(report.uploaded, 1);
    assert_eq!(report.status.stopped_streams, 1);
    assert_eq!(
        b.read(|r| r.query("SELECT body FROM items WHERE id='a'", &[]))
            .unwrap(),
        vec![vec![Value::Text("first".into())]]
    );
    assert_eq!(
        b.read(|r| r.query("SELECT body FROM items WHERE id='c'", &[]))
            .unwrap(),
        vec![vec![Value::Text("two".into())]]
    );
    b.close().unwrap();
    drop(b);
    let b = sdk(&p, "b");
    assert!(!b.status().unwrap().healthy());
    assert_eq!(b.status().unwrap().stopped_streams, 1);
}

struct Listed {
    keys: Vec<ObjectKey>,
    net: MemoryTransport,
    downloads: std::sync::atomic::AtomicUsize,
}
impl Transport for Listed {
    fn upload<'a>(
        &'a self,
        p: OutgoingEnvelope,
        c: &'a Cancellation,
    ) -> lfs_sqlite::sync::NetworkFuture<'a, ()> {
        self.net.upload(p, c)
    }
    fn list<'a>(
        &'a self,
        _: &'a str,
        cursor: usize,
        n: usize,
        _: &'a Cancellation,
    ) -> lfs_sqlite::sync::NetworkFuture<'a, lfs_sqlite::sync::Page> {
        Box::pin(async move {
            let end = (cursor + n).min(self.keys.len());
            Ok(lfs_sqlite::sync::Page {
                keys: self.keys[cursor..end].to_vec(),
                next: (end < self.keys.len()).then_some(end),
                complete: true,
            })
        })
    }
    fn download<'a>(
        &'a self,
        k: &'a ObjectKey,
        c: &'a Cancellation,
    ) -> lfs_sqlite::sync::NetworkFuture<'a, Package> {
        self.downloads.fetch_add(1, Ordering::Relaxed);
        self.net.download(k, c)
    }
}
#[test]
fn r4_missing_first_never_starves_later_object() {
    for page_size in [1, 100] {
        let p = fixture();
        let mut a = store(&p, "a", schema());
        let mut b = store(&p, "b", schema());
        let mut packages = Vec::new();
        for (s, k) in [(&mut a, "a"), (&mut b, "b")] {
            let c = s
                .write_transaction(|w| {
                    w.execute(
                        "INSERT INTO items VALUES (?1,'ok')",
                        &[Value::Text(k.into())],
                    )
                })
                .unwrap()
                .1
                .unwrap();
            packages.push(Package::from_pending(c.pending, &schema()));
        }
        let net = Listed {
            keys: packages
                .iter()
                .map(|p| ObjectKey {
                    packet: p.key.clone(),
                    envelope_hash: None,
                })
                .collect(),
            net: MemoryTransport::default(),
            downloads: 0.into(),
        };
        net.net.put(packages[1].clone()).unwrap();
        let r = sdk(&p, "receiver");
        let limits = SyncLimits {
            pages: 1,
            page_size,
            downloads: 1,
            retries: 0,
            ..SyncLimits::default()
        };
        for _ in 0..5 {
            let before = net.downloads.load(Ordering::Relaxed);
            let _ = run(r.sync_once(&net, &Cancellation::default(), limits.clone()));
            assert!(net.downloads.load(Ordering::Relaxed) - before <= 1);
        }
        assert_eq!(
            r.read(|r| r.query("SELECT id FROM items", &[])).unwrap(),
            vec![vec![Value::Text("b".into())]]
        );
        assert!(!r.status().unwrap().healthy());
        r.close().unwrap();
        drop(r);
        let r = sdk(&p, "receiver");
        assert!(!r.status().unwrap().healthy());
        net.net.put(packages[0].clone()).unwrap();
        for _ in 0..5 {
            run(r.sync_once(&net, &Cancellation::default(), limits.clone())).unwrap();
        }
        assert_eq!(
            r.read(|r| r.query("SELECT id FROM items", &[]))
                .unwrap()
                .len(),
            2
        );
        assert!(r.status().unwrap().healthy());
    }
}

#[test]
fn r5_swallowed_errors_rollback_entire_callback_and_next_transaction_is_fresh() {
    for case in 0..5 {
        let p = fixture();
        let mut s = store(&p, "a", schema());
        let result = s.write_transaction(|w| {
            w.execute("INSERT INTO items VALUES ('before','v')", &[])?;
            let err = match case {
                0 => w
                    .execute(
                        "INSERT OR FAIL INTO items VALUES ('partial','v'),('partial','duplicate')",
                        &[],
                    )
                    .map(|_| ()),
                1 => w.execute("COMMIT", &[]).map(|_| ()),
                2 => w
                    .execute("INSERT INTO items VALUES (?1,'v')", &[])
                    .map(|_| ()),
                3 => w.query("SELECT missing FROM items", &[]).map(|_| ()),
                _ => w
                    .execute(
                        "INSERT INTO items VALUES (?1,'v')",
                        &[Value::Real(f64::NAN)],
                    )
                    .map(|_| ()),
            };
            assert!(err.is_err());
            assert!(
                w.execute("INSERT INTO items VALUES ('after','v')", &[])
                    .is_err(),
                "tainted write accepted case {case}"
            );
            Ok(())
        });
        assert!(result.is_err());
        assert!(
            s.read(|r| r.query("SELECT * FROM items", &[]))
                .unwrap()
                .is_empty()
        );
        assert!(s.pending_packets(10).unwrap().is_empty());
        assert!(s.applied_packets().unwrap().is_empty());
        let c = s
            .write_transaction(|w| w.execute("INSERT INTO items VALUES ('fresh','v')", &[]))
            .unwrap()
            .1
            .unwrap();
        assert_eq!(c.packet.seq, 1);
        assert!(c.packet.operations[0].parents.is_empty());
        assert!(s.write_transaction(|w| w.execute("COMMIT", &[])).is_err());
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _: Result<((), Option<Commit>)> = s.write_transaction(|w| {
                w.execute("DELETE FROM items", &[])?;
                panic!("callback panic")
            });
        }));
        assert!(panic.is_err());
        assert_eq!(
            s.read(|r| r.query("SELECT * FROM items", &[]))
                .unwrap()
                .len(),
            1
        );
        assert!(s.read(|r| r.query("DELETE FROM items", &[])).is_err());
        s.write_transaction(|w| w.execute("UPDATE items SET body='next'", &[]))
            .unwrap();
    }
}

#[test]
fn r1_invalid_metadata_first_does_not_claim_valid_identity() {
    for metadata in 0..2 {
        let p = fixture();
        let mut a = store(&p, "a", schema());
        let mut b = store(&p, "b", schema());
        let c = a
            .write_transaction(|w| w.execute("INSERT INTO items VALUES ('k','v')", &[]))
            .unwrap()
            .1
            .unwrap();
        let good = Package::from_pending(c.pending, &schema());
        let mut bad = good.clone();
        if metadata == 0 {
            bad.fingerprint.push('x');
        } else {
            bad.attachment_references.push("bad-ref".into());
        }
        assert!(b.receive(bad).is_err());
        assert_eq!(b.receive(good).unwrap(), InboxState::Applied);
        assert_eq!(b.status().unwrap().stopped_streams, 0);
        assert_eq!(b.failed_attempts().unwrap().len(), 1);
    }
}

#[test]
fn r4_auth_and_cancellation_stop_without_discarding_debt() {
    let p = fixture();
    let mut a = store(&p, "a", schema());
    let mut keys = Vec::new();
    let net = MemoryTransport::default();
    for k in ["a", "b"] {
        let c = a
            .write_transaction(|w| {
                w.execute(
                    "INSERT INTO items VALUES (?1,'v')",
                    &[Value::Text(k.into())],
                )
            })
            .unwrap()
            .1
            .unwrap();
        let p = Package::from_pending(c.pending, &schema());
        keys.push(ObjectKey {
            packet: p.key.clone(),
            envelope_hash: None,
        });
        net.put(p).unwrap();
    }
    let t = Listed {
        keys,
        net,
        downloads: 0.into(),
    };
    let b = sdk(&p, "b");
    t.net.inject(Fault::Download(NetworkError::Auth)).unwrap();
    assert!(matches!(
        run(b.sync_once(&t, &Cancellation::default(), Default::default())),
        Err(Error::Network(NetworkError::Auth))
    ));
    assert_eq!(t.downloads.load(Ordering::Relaxed), 1);
    assert_eq!(b.status().unwrap().outstanding_objects, 2);
    let token = Cancellation::default();
    token.cancel();
    assert!(matches!(
        run(b.sync_once(&t, &token, Default::default())),
        Err(Error::Network(NetworkError::Cancelled))
    ));
    assert_eq!(t.downloads.load(Ordering::Relaxed), 1);
    assert!(
        run(b.sync_once(&t, &Cancellation::default(), Default::default()))
            .unwrap()
            .healthy()
    );
}
#[test]
fn r4_permanent_schema_block_does_not_starve_healthy_object_or_claim_health() {
    let p = fixture();
    let mut a = store(&p, "a", schema());
    let mut c = store(&p, "c", schema());
    let net = MemoryTransport::default();
    for (s, k, bad) in [(&mut a, "a", true), (&mut c, "c", false)] {
        let commit = s
            .write_transaction(|w| {
                w.execute(
                    "INSERT INTO items VALUES (?1,'v')",
                    &[Value::Text(k.into())],
                )
            })
            .unwrap()
            .1
            .unwrap();
        let mut p = Package::from_pending(commit.pending, &schema());
        if bad {
            p.fingerprint.push('x');
        }
        net.put(p).unwrap();
    }
    let b = sdk(&p, "b");
    let r = run(b.sync_once(&net, &Cancellation::default(), Default::default())).unwrap();
    assert!(!r.healthy());
    assert_eq!(
        b.read(|r| r.query("SELECT id FROM items", &[])).unwrap(),
        vec![vec![Value::Text("c".into())]]
    );
    b.close().unwrap();
    drop(b);
    let b = sdk(&p, "b");
    let r = run(b.sync_once(&net, &Cancellation::default(), Default::default())).unwrap();
    assert!(!r.healthy());
    assert_eq!(r.status.outstanding_objects, 1);
    assert_eq!(r.status.stopped_streams, 0);
}

#[test]
fn r1_unknown_parent_scope_cannot_establish_valid_fork() {
    let p = fixture();
    let mut a = store(&p, "a", schema());
    let mut c = store(&p, "c", schema());
    let mut b = store(&p, "b", schema());
    let good = a
        .write_transaction(|w| w.execute("INSERT INTO items VALUES ('k','v')", &[]))
        .unwrap()
        .1
        .unwrap();
    let parent = c
        .write_transaction(|w| w.execute("INSERT INTO items VALUES ('other','v')", &[]))
        .unwrap()
        .1
        .unwrap();
    let mut bad = good.packet.clone();
    bad.operations[0].parents = vec![parent.packet.operations[0].id.encode()];
    let bytes = bad.encode().unwrap();
    let mut badpkg = Package::from_pending(good.pending.clone(), &schema());
    badpkg.key.hash = lfs_protocol::hash(&bytes);
    badpkg.bytes = bytes;
    assert_eq!(b.receive(badpkg).unwrap(), InboxState::WaitingDependencies);
    assert_eq!(
        b.receive(Package::from_pending(good.pending, &schema()))
            .unwrap(),
        InboxState::Applied
    );
    b.receive(Package::from_pending(parent.pending, &schema()))
        .unwrap();
    assert_eq!(b.status().unwrap().stopped_streams, 0);
    assert_eq!(b.applied_packets().unwrap().len(), 2);
}
