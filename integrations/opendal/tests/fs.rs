//! Mandated Stage3 real local I/O. Fault decorator failures are explicitly SIMULATION.
mod common;
use common::*;
use lfs_protocol::hash;
use lfs_sqlite::sync::{NetworkFuture, Page};
use lfs_sqlite::*;
use lfs_storage::*;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};
struct FaultTransport {
    inner: Arc<OpenDalTransport>,
    faults: Mutex<VecDeque<Fault>>,
}
impl FaultTransport {
    fn new(inner: Arc<OpenDalTransport>, faults: Vec<Fault>) -> Self {
        Self {
            inner,
            faults: Mutex::new(faults.into()),
        }
    }
}
impl Transport for FaultTransport {
    fn upload<'a>(&'a self, p: OutgoingEnvelope, c: &'a Cancellation) -> NetworkFuture<'a, ()> {
        Box::pin(async move {
            let f = {
                let mut q = self.faults.lock().unwrap();
                if matches!(q.front(), Some(Fault::LostAck | Fault::Upload(_))) {
                    q.pop_front()
                } else {
                    None
                }
            };
            if let Some(Fault::Upload(e)) = f {
                return Err(e);
            }
            self.inner.upload(p, c).await?;
            if matches!(f, Some(Fault::LostAck)) {
                Err(NetworkError::LostAck)
            } else {
                Ok(())
            }
        })
    }
    fn list<'a>(
        &'a self,
        d: &'a str,
        cursor: usize,
        n: usize,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, Page> {
        Box::pin(async move {
            let mut p = self.inner.list(d, cursor, n, c).await?;
            let f = self.faults.lock().unwrap().pop_front();
            match f {
                Some(Fault::MissingPage | Fault::LatePage) => {
                    p.keys.clear();
                    p.complete = false
                }
                Some(Fault::DuplicatePage) => {
                    if let Some(k) = p.keys.first().cloned() {
                        p.keys = vec![k; p.keys.len()];
                    }
                    p.complete = false
                }
                Some(Fault::List(e)) => return Err(e),
                None => {}
                _ => panic!("wrong seam"),
            };
            Ok(p)
        })
    }
    fn download<'a>(&'a self, k: &'a ObjectKey, c: &'a Cancellation) -> NetworkFuture<'a, Package> {
        self.inner.download(k, c)
    }
}
#[tokio::test]
async fn real_three_device_typed_conflict_roundtrip() {
    let f = Fixture::new();
    journey(&f, &f.transport()).await;
}
#[tokio::test]
async fn envelope_and_outbox_are_fixed_across_reopen() {
    let f = Fixture::new();
    let s = f.sdk("a");
    insert(&s, "k");
    let old = s.pending_envelopes(10).unwrap();
    let p = Package::decode_envelope(&old[0].bytes).unwrap();
    assert_eq!(hash(&p.bytes), p.key.hash);
    assert_ne!(p.key.hash, old[0].key.envelope_hash.unwrap());
    assert_eq!(old[0].bytes, p.encode_envelope().unwrap());
    s.close().unwrap();
    drop(s);
    let s = f.sdk("a");
    assert_eq!(old, s.pending_envelopes(10).unwrap());
    sync(&s, &f.transport()).await;
}
#[tokio::test]
async fn full_envelope_variant_is_not_hidden_by_payload_key_dedup() {
    let f = Fixture::new();
    let a = f.sdk("a");
    insert(&a, "k");
    let original = a.pending_envelopes(10).unwrap().remove(0);
    let t = f.transport();
    sync(&a, &t).await;
    let b = f.sdk("b");
    sync(&b, &t).await;
    let mut p = Package::decode_envelope(&original.bytes).unwrap();
    p.fingerprint.push(' ');
    assert_eq!(p.key, original.key.packet);
    t.upload(
        OutgoingEnvelope::from_package(&p).unwrap(),
        &Cancellation::default(),
    )
    .await
    .unwrap();
    // R1: an incompatible fingerprint is failure evidence, not a second valid content.
    assert!(
        !b.sync_once(&t, &Cancellation::default(), Default::default())
            .await
            .unwrap()
            .healthy()
    );
    assert_eq!(b.status().unwrap().stopped_streams, 0);
    assert_eq!(b.received_envelopes().unwrap().len(), 1);
    assert!(
        b.failed_attempts()
            .unwrap()
            .iter()
            .any(|a| a.wire && a.bytes == p.encode_envelope().unwrap())
    );
    assert_eq!(rows(&b)[0][1], Value::Text("base".into()));
}
#[tokio::test]
async fn truncated_upload_is_refetched_after_sender_exact_retry() {
    let f = Fixture::new();
    let a = f.sdk("a");
    insert(&a, "k");
    let e = a.pending_envelopes(10).unwrap().remove(0);
    let path = f.0.join("objects").join(object_path(&e.key).unwrap());
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, &e.bytes[..13]).unwrap();
    let b = f.sdk("b");
    let t = f.transport();
    // R3/R4: retained retryable debt makes the report unhealthy without global abort.
    assert!(
        !b.sync_once(&t, &Cancellation::default(), Default::default())
            .await
            .unwrap()
            .healthy()
    );
    assert!(rows(&b).is_empty());
    assert!(b.received_envelopes().unwrap().is_empty());
    sync(&a, &t).await;
    assert_eq!(std::fs::read(path).unwrap(), e.bytes);
    sync(&b, &t).await;
    assert_eq!(rows(&a), rows(&b));
}
#[tokio::test]
async fn valid_mismatched_identity_is_never_overwritten() {
    let f = Fixture::new();
    let a = f.sdk("a");
    let b = f.sdk("b");
    insert(&a, "a");
    insert(&b, "b");
    let ea = a.pending_envelopes(1).unwrap().remove(0);
    let eb = b.pending_envelopes(1).unwrap().remove(0);
    let path = f.0.join("objects").join(object_path(&ea.key).unwrap());
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, &eb.bytes).unwrap();
    assert!(
        a.sync_once(&f.transport(), &Cancellation::default(), Default::default())
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(path).unwrap(), eb.bytes);
    assert_eq!(a.status().unwrap().pending_outbox, 1);
}
#[tokio::test]
async fn corrupt_full_hash_and_oversized_objects_fail_closed() {
    let f = Fixture::new();
    let a = f.sdk("a");
    insert(&a, "k");
    let e = a.pending_envelopes(1).unwrap().remove(0);
    let t = f.transport();
    sync(&a, &t).await;
    let path = f.0.join("objects").join(object_path(&e.key).unwrap());
    let mut bad = e.bytes.clone();
    bad[12] ^= 1;
    std::fs::write(&path, &bad).unwrap();
    assert!(matches!(
        t.download(&e.key, &Cancellation::default()).await,
        Err(NetworkError::Corrupt)
    ));
    std::fs::write(&path, vec![0; MAX_ENVELOPE + 1]).unwrap();
    assert_eq!(
        t.download(&e.key, &Cancellation::default()).await,
        Err(NetworkError::Limit)
    );
}
#[tokio::test]
async fn gaps_and_cross_stream_dependencies_persist_across_restart() {
    let f = Fixture::new();
    let a = f.sdk("a");
    insert(&a, "base");
    let first = a.pending_envelopes(10).unwrap()[0].clone();
    insert(&a, "unrelated");
    let second = a.pending_envelopes(10).unwrap()[1].clone();
    let t = f.transport();
    t.upload(second.clone(), &Cancellation::default())
        .await
        .unwrap();
    let b = f.sdk("b");
    let r = b
        .sync_once(&t, &Cancellation::default(), Default::default())
        .await
        .unwrap();
    assert!(!r.healthy());
    assert!(rows(&b).is_empty());
    assert_eq!(
        b.status().unwrap().inbox[0].state,
        InboxState::WaitingDependencies
    );
    b.close().unwrap();
    drop(b);
    t.upload(first.clone(), &Cancellation::default())
        .await
        .unwrap();
    let b = f.sdk("b");
    sync(&b, &t).await;
    assert_eq!(rows(&a), rows(&b));
    b.write_transaction(|w| w.execute("UPDATE items SET text='from b' WHERE id='base'", &[]))
        .unwrap();
    let dependent = b.pending_envelopes(1).unwrap().remove(0);
    let f2 = Fixture::new();
    let t2 = f2.transport();
    t2.upload(dependent.clone(), &Cancellation::default())
        .await
        .unwrap();
    let c = f2.sdk("c");
    assert!(
        !c.sync_once(&t2, &Cancellation::default(), Default::default())
            .await
            .unwrap()
            .healthy()
    );
    c.close().unwrap();
    drop(c);
    t2.upload(first.clone(), &Cancellation::default())
        .await
        .unwrap();
    let c = f2.sdk("c");
    sync(&c, &t2).await;
    assert_eq!(rows(&c)[0][1], Value::Text("from b".into()));
}
#[tokio::test]
async fn simulated_missing_late_duplicate_native_pages_recover() {
    for fault in [Fault::MissingPage, Fault::LatePage, Fault::DuplicatePage] {
        let f = Fixture::new();
        let a = f.sdk("a");
        for k in ["1", "2", "3"] {
            insert(&a, k);
        }
        let t = Arc::new(f.transport());
        sync(&a, t.as_ref()).await;
        let b = f.sdk("b");
        let faulty = FaultTransport::new(t.clone(), vec![fault]);
        let report = b
            .sync_once(
                &faulty,
                &Cancellation::default(),
                SyncLimits {
                    page_size: 2,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(!report.healthy());
        b.close().unwrap();
        drop(b);
        let b = f.sdk("b");
        sync(&b, t.as_ref()).await;
        assert_eq!(rows(&a), rows(&b));
    }
}
#[tokio::test]
async fn simulated_retry_classification_is_bounded_and_auth_cancel_fail_closed() {
    for e in [
        NetworkError::Transient,
        NetworkError::Timeout,
        NetworkError::RateLimit,
        NetworkError::Auth,
        NetworkError::Cancelled,
    ] {
        let f = Fixture::new();
        let a = f.sdk("a");
        insert(&a, "k");
        let t = Arc::new(f.transport());
        let ft = FaultTransport::new(t, vec![Fault::Upload(e.clone()); 4]);
        assert!(
            a.sync_once(
                &ft,
                &Cancellation::default(),
                SyncLimits {
                    retries: 2,
                    ..Default::default()
                }
            )
            .await
            .is_err()
        );
        let left = ft.faults.lock().unwrap().len();
        assert_eq!(
            left,
            if matches!(e, NetworkError::Auth | NetworkError::Cancelled) {
                3
            } else {
                1
            }
        );
        assert_eq!(a.status().unwrap().pending_outbox, 1);
    }
}
#[tokio::test]
async fn cancelled_native_operation_never_acknowledges() {
    let f = Fixture::new();
    let a = f.sdk("a");
    insert(&a, "k");
    let cancel = Cancellation::default();
    cancel.cancel();
    assert!(matches!(
        a.sync_once(&f.transport(), &cancel, Default::default())
            .await,
        Err(Error::Network(NetworkError::Cancelled))
    ));
    assert_eq!(a.status().unwrap().pending_outbox, 1);
}
#[tokio::test]
async fn generated_paths_are_bounded_and_symlinks_and_foreign_files_rejected() {
    let f = Fixture::new();
    let a = f.sdk("a");
    insert(&a, "k");
    let e = a.pending_envelopes(1).unwrap().remove(0);
    let path = object_path(&e.key).unwrap();
    assert!(!path.contains(".."));
    assert!(path.split('/').all(|s| s.len() < 255));
    let t = f.transport();
    sync(&a, &t).await;
    let outside = f.0.join("outside");
    std::fs::create_dir(&outside).unwrap();
    let root =
        f.0.join("objects")
            .join(dataset_root(&identity("a").dataset).unwrap());
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&outside, root.join("evil")).unwrap();
        assert!(
            t.list(&identity("a").dataset, 0, 100, &Cancellation::default())
                .await
                .is_err()
        );
        std::fs::remove_file(root.join("evil")).unwrap();
        let original = f.0.join("objects").join(&path);
        std::fs::remove_file(&original).unwrap();
        std::os::unix::fs::symlink(outside.join("escape"), &original).unwrap();
        assert!(t.upload(e.clone(), &Cancellation::default()).await.is_err());
        assert!(!outside.join("escape").exists());
        std::fs::remove_file(original).unwrap();
    }
    std::fs::write(root.join("unexpected"), b"x").unwrap();
    assert!(
        t.list(&identity("a").dataset, 0, 100, &Cancellation::default())
            .await
            .is_err()
    );
}
#[test]
fn constructors_require_explicit_safe_endpoints_and_credentials() {
    let creds = || S3Credentials {
        access_key_id: "test".into(),
        secret_access_key: "test-only".into(),
        session_token: None,
    };
    for url in [
        "http://example.com",
        "http://127.0.0.1:9",
        "https://user:secret@example.com",
        "file:///data",
    ] {
        assert!(OpenDalTransport::s3(url, "test", "bucket", creds(), Options::default()).is_err());
    }
    assert!(
        OpenDalTransport::s3(
            "http://127.0.0.1:9",
            "test",
            "bucket",
            creds(),
            Options {
                allow_loopback_http: true,
                ..Default::default()
            }
        )
        .is_ok()
    );
    assert!(
        OpenDalTransport::webdav(
            "http://example.com",
            WebdavCredentials::Bearer("test".into()),
            Options {
                allow_loopback_http: true,
                ..Default::default()
            }
        )
        .is_err()
    );
}
#[tokio::test]
async fn process_probe() {
    let Ok(root) = std::env::var("LFS_STAGE3_CHILD") else {
        return;
    };
    let s = Sdk::open(
        std::path::Path::new(&root).join("a.db"),
        identity("a"),
        schema(),
        Baseline::Reject,
    )
    .unwrap();
    let t = Arc::new(
        OpenDalTransport::fs(
            std::path::Path::new(&root).join("objects"),
            Options::default(),
        )
        .unwrap(),
    );
    if std::env::var("LFS_STAGE3_MODE").unwrap() == "crash" {
        insert(&s, "k");
        let e = s.pending_envelopes(1).unwrap().remove(0);
        std::fs::write(std::path::Path::new(&root).join("expected"), e.bytes).unwrap();
        let faulty = FaultTransport::new(t, vec![Fault::LostAck]);
        assert!(
            s.sync_once(&faulty, &Cancellation::default(), Default::default())
                .await
                .is_err()
        );
        std::process::exit(23);
    } else {
        sync(&s, t.as_ref()).await;
    }
}
#[test]
fn actual_process_restart_resends_identical_full_envelope_after_lost_ack() {
    let f = Fixture::new();
    let run = |mode: &str| {
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "process_probe", "--nocapture"])
            .env("LFS_STAGE3_CHILD", &f.0)
            .env("LFS_STAGE3_MODE", mode)
            .status()
            .unwrap()
    };
    assert_eq!(run("crash").code(), Some(23));
    let s = f.sdk("a");
    let before = s.pending_envelopes(1).unwrap().remove(0);
    assert_eq!(before.bytes, std::fs::read(f.0.join("expected")).unwrap());
    let remote = f.0.join("objects").join(object_path(&before.key).unwrap());
    assert_eq!(before.bytes, std::fs::read(&remote).unwrap());
    s.close().unwrap();
    drop(s);
    assert!(run("resend").success());
    let s = f.sdk("a");
    assert!(s.pending_envelopes(1).unwrap().is_empty());
    assert_eq!(std::fs::read(remote).unwrap(), before.bytes);
}
#[tokio::test]
async fn actual_http_timeout_and_cancellation() {
    use std::io::Read;
    for timeout in [true, false] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (send, recv) = std::sync::mpsc::channel();
        let (entered, ready) = tokio::sync::oneshot::channel();
        let worker = std::thread::spawn(move || {
            listener.set_nonblocking(true).unwrap();
            loop {
                if recv.try_recv().is_ok() {
                    break;
                }
                match listener.accept() {
                    Ok((mut s, _)) => {
                        s.set_read_timeout(Some(Duration::from_millis(50))).unwrap();
                        let mut b = [0; 1024];
                        let _ = s.read(&mut b);
                        let _ = entered.send(());
                        let _ = recv.recv_timeout(Duration::from_secs(2));
                        break;
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(5)),
                }
            }
        });
        let t = OpenDalTransport::webdav(
            &format!("http://{addr}"),
            WebdavCredentials::Bearer("test-only".into()),
            Options {
                timeout: Duration::from_millis(if timeout { 50 } else { 1000 }),
                allow_loopback_http: true,
            },
        )
        .unwrap();
        let cancel = Cancellation::default();
        let result = if timeout {
            t.list("data", 0, 1, &cancel).await
        } else {
            let (result, ()) = tokio::join!(t.list("data", 0, 1, &cancel), async {
                ready.await.unwrap();
                cancel.cancel();
            });
            result
        };
        let _ = send.send(());
        worker.join().unwrap();
        assert_eq!(
            result.err(),
            Some(if timeout {
                NetworkError::Timeout
            } else {
                NetworkError::Cancelled
            })
        );
    }
}

#[tokio::test]
async fn simulated_http_statuses_keep_auth_rate_and_transient_classifications() {
    use std::io::{Read, Write};
    for (status, expected) in [
        (401, NetworkError::Auth),
        (403, NetworkError::Auth),
        (429, NetworkError::RateLimit),
        (500, NetworkError::Transient),
    ] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let worker = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut req = [0; 4096];
            let _ = stream.read(&mut req);
            write!(
                stream,
                "HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
        });
        let t = OpenDalTransport::webdav(
            &format!("http://{addr}"),
            WebdavCredentials::Bearer("test-only".into()),
            Options {
                allow_loopback_http: true,
                ..Default::default()
            },
        )
        .unwrap();
        let error = t.list("data", 0, 1, &Cancellation::default()).await.err();
        worker.join().unwrap();
        assert_eq!(error, Some(expected));
    }
}

#[tokio::test]
async fn canonical_envelope_limits_headers_and_exact_outgoing_validation() {
    let f = Fixture::new();
    let a = f.sdk("a");
    insert(&a, "k");
    let e = a.pending_envelopes(1).unwrap().remove(0);
    assert_eq!(&e.bytes[..8], b"LFSE\0\0\0\x01");
    for offset in [0, 7, 8, 9, 10, 11, e.bytes.len() - 1] {
        let mut bad = e.bytes.clone();
        bad[offset] ^= 255;
        assert!(Package::decode_envelope(&bad).is_err());
    }
    for n in [0, 7, 11, 35, e.bytes.len() - 1] {
        assert!(Package::decode_envelope(&e.bytes[..n]).is_err());
    }
    let mut extra = e.bytes.clone();
    extra.push(0);
    assert!(Package::decode_envelope(&extra).is_err());
    assert!(Package::decode_envelope(&vec![0; MAX_ENVELOPE + 1]).is_err());
    let t = f.transport();
    let mut wrong = e.clone();
    wrong.key.packet.identity.device = "other".into();
    assert_eq!(
        t.upload(wrong, &Cancellation::default()).await,
        Err(NetworkError::Corrupt)
    );
    let mut wrong = e.clone();
    wrong.bytes[12] ^= 1;
    assert_eq!(
        t.upload(wrong, &Cancellation::default()).await,
        Err(NetworkError::Corrupt)
    );
    assert_eq!(a.pending_envelopes(1).unwrap(), vec![e]);
    // Listing is scoped to this dataset only, not the operator root or another dataset.
    std::fs::create_dir_all(f.0.join("objects/lfs1/foreign")).unwrap();
    std::fs::write(f.0.join("objects/lfs1/foreign/invalid"), b"foreign").unwrap();
    sync(&a, &t).await;
}

#[tokio::test]
async fn r3_corrupt_object_repaired_exactly_after_receiver_restart() {
    let f = Fixture::new();
    let net = f.transport();
    let cancel = Cancellation::default();
    let a = f.sdk("a");
    a.write_transaction(|w| w.execute("INSERT INTO items VALUES ('k','base',0,0.0,x'',NULL)", &[]))
        .unwrap();
    // Capture the transactionally fixed sender bytes through public Store after closing SDK.
    a.close().unwrap();
    drop(a);
    let mut a = Store::open(f.0.join("a.db"), identity("a"), schema(), Baseline::Reject).unwrap();
    let wire = a.pending_envelopes(1).unwrap().remove(0);
    drop(a);
    net.upload(wire.clone(), &cancel).await.unwrap();
    let path = f.0.join("objects").join(object_path(&wire.key).unwrap());
    let bad = wire.bytes[..wire.bytes.len() / 2].to_vec();
    std::fs::write(&path, &bad).unwrap();
    let b = f.sdk("b");
    let _ = b.sync_once(&net, &cancel, SyncLimits::default()).await;
    assert!(!b.status().unwrap().healthy());
    b.close().unwrap();
    drop(b);
    net.upload(wire.clone(), &cancel).await.unwrap();
    let b = f.sdk("b");
    assert!(
        b.sync_once(&net, &cancel, SyncLimits::default())
            .await
            .unwrap()
            .healthy()
    );
    b.close().unwrap();
    drop(b);
    let mut b = Store::open(f.0.join("b.db"), identity("b"), schema(), Baseline::Reject).unwrap();
    let attempts = b.failed_attempts().unwrap();
    assert!(
        attempts
            .iter()
            .any(|a| a.wire && a.bytes == bad && a.key == wire.key)
    );
    assert_eq!(std::fs::read(path).unwrap(), wire.bytes);
}

#[tokio::test]
async fn r6_max_schema_names_and_batch_roundtrip_fs() {
    let f = Fixture::new();
    let net = f.transport();
    let schema = Schema {
        version: u32::MAX,
        tables: (0..64)
            .map(|i| Table {
                id: u32::MAX - i,
                name: format!("t{i:02}{}", "x".repeat(60)),
                columns: (0..64)
                    .map(|j| Column {
                        name: format!("c{j:02}{}", "x".repeat(60)),
                        kind: if j == 0 {
                            ColumnType::Text
                        } else {
                            ColumnType::Integer
                        },
                        nullable: false,
                    })
                    .collect(),
            })
            .collect(),
    };
    schema.validate().unwrap();
    println!("max legal fingerprint bytes={}", schema.fingerprint().len());
    let mut a = Store::open(
        f.0.join("maxa.db"),
        identity("maxa"),
        schema.clone(),
        Baseline::Reject,
    )
    .unwrap();
    let mut b = Store::open(
        f.0.join("maxb.db"),
        identity("maxb"),
        schema.clone(),
        Baseline::Reject,
    )
    .unwrap();
    let sql = format!(
        "INSERT INTO {} VALUES ({})",
        schema.tables[0].name,
        (1..=64)
            .map(|i| format!("?{i}"))
            .collect::<Vec<_>>()
            .join(",")
    );
    let c = a
        .write_transaction(|w| {
            for i in 0..lfs_protocol::MAX_OPS {
                let mut v = vec![Value::Integer(1); 64];
                v[0] = Value::Text(format!("k{i}"));
                w.execute(&sql, &v)?;
            }
            Ok(())
        })
        .unwrap()
        .1
        .unwrap();
    let package = Package::from_pending(c.pending, &schema);
    assert_eq!(b.receive(package.clone()).unwrap(), InboxState::Applied);
    let wire = a.pending_envelopes(1).unwrap().remove(0);
    drop(a);
    drop(b);
    let a = Sdk::open(
        f.0.join("maxa.db"),
        identity("maxa"),
        schema.clone(),
        Baseline::Reject,
    )
    .unwrap();
    let b = Sdk::open(
        f.0.join("maxc.db"),
        identity("maxc"),
        schema.clone(),
        Baseline::Reject,
    )
    .unwrap();
    let cancel = Cancellation::default();
    assert!(
        a.sync_once(&net, &cancel, SyncLimits::default())
            .await
            .unwrap()
            .healthy()
    );
    assert!(
        b.sync_once(&net, &cancel, SyncLimits::default())
            .await
            .unwrap()
            .healthy()
    );
    assert_eq!(
        b.read(|r| r.query(
            &format!("SELECT count(*) FROM {}", schema.tables[0].name),
            &[]
        ))
        .unwrap(),
        vec![vec![Value::Integer(lfs_protocol::MAX_OPS as i64)]]
    );
    assert_eq!(
        std::fs::read(f.0.join("objects").join(object_path(&wire.key).unwrap())).unwrap(),
        wire.bytes
    );
    let mut bad = package;
    bad.fingerprint = "x".repeat(MAX_FINGERPRINT + 1);
    assert!(bad.encode_envelope().is_err());
    let mut store = Store::open(
        f.0.join("maxb.db"),
        identity("maxb"),
        schema.clone(),
        Baseline::Reject,
    )
    .unwrap();
    assert!(store.receive(bad).is_err());
    let mut invalid = wire.bytes.clone();
    invalid[8..12].copy_from_slice(&((MAX_FINGERPRINT + 1) as u32).to_be_bytes());
    assert!(Package::decode_envelope(&invalid).is_err());
}
