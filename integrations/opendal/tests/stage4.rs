//! Mandated desktop API acceptance; fault transports are not backend evidence.
use lfs_sqlite::backup::{BackupKind, BackupOptions};
use lfs_sqlite::sync::{NetworkFuture, Page};
use lfs_sqlite::*;
use lfs_storage::{OpenDalTransport, Options};
use std::{
    fs::File,
    io::{Read as IoRead, Write as IoWrite},
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let p = PathBuf::from(std::env::var_os("TMPDIR").expect("explicit TMPDIR")).join(format!(
            "lfs-stage4-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&p).unwrap();
        std::fs::create_dir(p.join("remote")).unwrap();
        Self(p)
    }
    fn sdk(&self, n: &str) -> Sdk {
        Sdk::open(
            self.0.join(format!("{n}.db")),
            identity(n),
            schema(),
            Baseline::Reject,
        )
        .unwrap()
    }
    fn transport(&self) -> OpenDalTransport {
        OpenDalTransport::fs(self.0.join("remote"), Options::default()).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn identity(n: &str) -> Identity {
    Identity {
        dataset: "stage4".into(),
        device: n.into(),
        incarnation: "first".into(),
    }
}
fn schema() -> Schema {
    Schema {
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
    }
}
fn insert(s: &Sdk, key: &str, value: &str, refs: &[String]) -> Result<()> {
    s.write_transaction_with_attachments(refs, |w| {
        w.execute(
            "INSERT OR REPLACE INTO notes VALUES (?1,?2)",
            &[Value::Text(key.into()), Value::Text(value.into())],
        )
    })?;
    Ok(())
}
fn rows(s: &Sdk) -> Vec<Vec<Value>> {
    s.read(|r| r.query("SELECT * FROM notes ORDER BY id", &[]))
        .unwrap()
}
async fn sync(s: &Sdk, t: &impl Transport) {
    s.sync_once(t, &Cancellation::default(), SyncLimits::default())
        .await
        .unwrap();
}
struct LimitedReader {
    left: usize,
    calls: usize,
    cancel: Option<Cancellation>,
    fail: bool,
}
impl IoRead for LimitedReader {
    fn read(&mut self, b: &mut [u8]) -> std::io::Result<usize> {
        assert!(b.len() <= attachments::CHUNK);
        self.calls += 1;
        if self.calls == 3 {
            if let Some(c) = &self.cancel {
                c.cancel();
            }
            if self.fail {
                return Err(std::io::ErrorKind::Interrupted.into());
            }
        }
        let n = b.len().min(self.left);
        b[..n].fill(21);
        self.left -= n;
        Ok(n)
    }
}
#[test]
fn local_stream_limit_cancel_interruption_reopen_and_atomic_refs() {
    let f = Fixture::new();
    let s = f.sdk("a");
    let c = Cancellation::default();
    let a = s
        .add_attachment(
            LimitedReader {
                left: attachments::CHUNK * 4 + 1,
                calls: 0,
                cancel: None,
                fail: false,
            },
            1_000_000,
            &c,
        )
        .unwrap();
    assert_eq!(a.size, attachments::CHUNK as u64 * 4 + 1);
    assert!(s.add_attachment(&b"over"[..], 2, &c).is_err());
    assert!(
        s.add_attachment(
            LimitedReader {
                left: 1_000_000,
                calls: 0,
                cancel: None,
                fail: true
            },
            1_000_000,
            &c
        )
        .is_err()
    );
    let cancel = Cancellation::default();
    assert!(matches!(
        s.add_attachment(
            LimitedReader {
                left: 1_000_000,
                calls: 0,
                cancel: Some(cancel.clone()),
                fail: false
            },
            1_000_000,
            &cancel
        ),
        Err(Error::Network(NetworkError::Cancelled))
    ));
    assert!(insert(&s, "no", "no", &["../escape".into()]).is_err());
    assert!(rows(&s).is_empty());
    assert!(
        s.write_transaction_with_attachments(std::slice::from_ref(&a.hash), |w| {
            w.execute("INSERT INTO notes VALUES ('rolled','x')", &[])?;
            Err::<(), _>(Error::Caller("abort".into()))
        })
        .is_err()
    );
    assert!(rows(&s).is_empty());
    assert!(s.pending_envelopes(10).unwrap().is_empty());
    assert!(
        s.write_transaction_with_attachments(std::slice::from_ref(&a.hash), |_| Ok(()))
            .is_err()
    );
    insert(&s, "yes", "retained", std::slice::from_ref(&a.hash)).unwrap();
    let wire = s.pending_envelopes(10).unwrap();
    assert_eq!(
        Package::decode_envelope(&wire[0].bytes)
            .unwrap()
            .attachment_references,
        std::slice::from_ref(&a.hash)
    );
    drop(s);
    let s = f.sdk("a");
    assert_eq!(s.pending_envelopes(10).unwrap(), wire);
    assert_eq!(s.attachment_status(&a.hash).unwrap().state, "Available");
    assert!(
        std::fs::read_dir(f.0.join("a.db.attachments/staging"))
            .unwrap()
            .next()
            .is_none()
    );
    assert!(
        std::fs::read_dir(f.0.join("a.db.attachments/quarantine"))
            .unwrap()
            .count()
            >= 3
    );
}
#[derive(Clone, Copy)]
enum Mode {
    UploadFailure,
    DownloadCorrupt,
    DownloadCancel,
}
struct FaultTransport {
    inner: OpenDalTransport,
    mode: Mode,
}
impl Transport for FaultTransport {
    fn upload<'a>(&'a self, p: OutgoingEnvelope, c: &'a Cancellation) -> NetworkFuture<'a, ()> {
        self.inner.upload(p, c)
    }
    fn list<'a>(
        &'a self,
        d: &'a str,
        x: usize,
        n: usize,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, Page> {
        self.inner.list(d, x, n, c)
    }
    fn download<'a>(&'a self, k: &'a ObjectKey, c: &'a Cancellation) -> NetworkFuture<'a, Package> {
        self.inner.download(k, c)
    }
    fn upload_attachment<'a>(
        &'a self,
        d: &'a str,
        h: &'a str,
        f: File,
        n: u64,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, ()> {
        if matches!(self.mode, Mode::UploadFailure) {
            Box::pin(async { Err(NetworkError::Transient) })
        } else {
            self.inner.upload_attachment(d, h, f, n, c)
        }
    }
    fn download_attachment<'a>(
        &'a self,
        _d: &'a str,
        _h: &'a str,
        mut f: File,
        _n: u64,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, ()> {
        Box::pin(async move {
            f.write_all(&vec![55; attachments::CHUNK + 4]).unwrap();
            match self.mode {
                Mode::DownloadCancel => {
                    c.cancel();
                    Err(NetworkError::Cancelled)
                }
                _ => Ok(()),
            }
        })
    }
}
#[tokio::test]
async fn publish_order_full_hash_and_failed_download_never_available() {
    let f = Fixture::new();
    let a = f.sdk("a");
    let b = f.sdk("b");
    let c = Cancellation::default();
    let content = vec![7; attachments::CHUNK * 7 + 5];
    let attachment = a.add_attachment(content.as_slice(), 1_000_000, &c).unwrap();
    insert(&a, "x", "value", std::slice::from_ref(&attachment.hash)).unwrap();
    let exact = a.pending_envelopes(10).unwrap();
    let fault = FaultTransport {
        inner: f.transport(),
        mode: Mode::UploadFailure,
    };
    assert!(
        a.sync_once(&fault, &c, SyncLimits::default())
            .await
            .is_err()
    );
    assert_eq!(a.pending_envelopes(10).unwrap(), exact);
    assert!(
        fault
            .list("stage4", 0, 100, &c)
            .await
            .unwrap()
            .keys
            .is_empty()
    );
    let t = f.transport();
    sync(&a, &t).await;
    sync(&b, &t).await;
    assert_eq!(rows(&a), rows(&b));
    assert_eq!(
        b.attachment_status(&attachment.hash).unwrap().state,
        "Missing"
    );
    for mode in [Mode::DownloadCorrupt, Mode::DownloadCancel] {
        let fault = FaultTransport {
            inner: f.transport(),
            mode,
        };
        let cc = Cancellation::default();
        assert!(
            b.download_attachment(&fault, &attachment.hash, 1_000_000, &cc)
                .await
                .is_err()
        );
        assert_eq!(
            b.attachment_status(&attachment.hash).unwrap().state,
            "Missing"
        );
    }
    assert!(
        b.download_attachment(&t, &attachment.hash, 3, &c)
            .await
            .is_err()
    );
    assert_eq!(
        b.attachment_status(&attachment.hash).unwrap().state,
        "Missing"
    );
    b.download_attachment(&t, &attachment.hash, 1_000_000, &c)
        .await
        .unwrap();
    let mut result = Vec::new();
    b.open_attachment(&attachment.hash, &c)
        .unwrap()
        .read_to_end(&mut result)
        .unwrap();
    assert_eq!(result, content);
    // Damage remote content; SHA verification fails and a new device remains Missing.
    let remote = find_file(&f.0.join("remote"), &attachment.hash).unwrap();
    std::fs::write(&remote, b"corrupt").unwrap();
    let fresh = f.sdk("fresh");
    assert!(
        fresh
            .download_attachment(&t, &attachment.hash, 1_000_000, &c)
            .await
            .is_err()
    );
    assert_eq!(
        fresh.attachment_status(&attachment.hash).unwrap().state,
        "Missing"
    );
    // Sender retry repairs corrupted immutable content before the next referencing envelope is published.
    insert(
        &a,
        "retry",
        "same file",
        std::slice::from_ref(&attachment.hash),
    )
    .unwrap();
    sync(&a, &t).await;
    fresh
        .download_attachment(&t, &attachment.hash, 1_000_000, &c)
        .await
        .unwrap();
}
fn find_file(root: &std::path::Path, name: &str) -> Option<PathBuf> {
    for e in std::fs::read_dir(root).ok()? {
        let p = e.ok()?.path();
        if p.is_dir() {
            if let Some(v) = find_file(&p, name) {
                return Some(v);
            }
        } else if p.file_name()?.to_str()? == name {
            return Some(p);
        }
    }
    None
}
#[test]
fn owned_paths_orphans_symlinks_and_corrupt_content() {
    let f = Fixture::new();
    let s = f.sdk("a");
    let c = Cancellation::default();
    let a = s.add_attachment(&b"durable"[..], 100, &c).unwrap();
    assert!(s.open_attachment("../../escape", &c).is_err());
    drop(s);
    std::fs::write(
        f.0.join("a.db.attachments/objects").join("orphan"),
        b"orphan",
    )
    .unwrap();
    let s = f.sdk("a");
    assert!(!f.0.join("a.db.attachments/objects/orphan").exists());
    drop(s);
    std::fs::write(
        f.0.join("a.db.attachments/objects").join(&a.hash),
        b"broken",
    )
    .unwrap();
    let s = f.sdk("a");
    assert_eq!(s.attachment_status(&a.hash).unwrap().state, "Missing");
    assert!(s.open_attachment(&a.hash, &c).is_err());
    drop(s);
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(
            f.0.join("remote"),
            f.0.join("a.db.attachments/objects").join(&a.hash),
        )
        .unwrap();
        assert!(Sdk::open(f.0.join("a.db"), identity("a"), schema(), Baseline::Reject).is_err());
    }
}
#[tokio::test]
async fn local_backup_preserves_pending_bytes_rotates_and_clones_converge() {
    let f = Fixture::new();
    let a = f.sdk("a");
    let c = Cancellation::default();
    let file = a
        .add_attachment(&vec![3; attachments::CHUNK + 1][..], 1_000_000, &c)
        .unwrap();
    insert(&a, "x", "pending", std::slice::from_ref(&file.hash)).unwrap();
    let exact = a.pending_envelopes(10).unwrap();
    let backup = f.0.join("backup");
    a.backup(
        &backup,
        BackupKind::LocalDisaster,
        BackupOptions::default(),
        &c,
    )
    .unwrap();
    let b = Sdk::restore(&backup, f.0.join("clone-b"), &c).unwrap();
    let d = Sdk::restore(&backup, f.0.join("clone-d"), &c).unwrap();
    assert_eq!(b.pending_envelopes(10).unwrap(), exact);
    assert_eq!(d.pending_envelopes(10).unwrap(), exact);
    assert_ne!(b.identity().unwrap(), d.identity().unwrap());
    assert_ne!(a.identity().unwrap(), b.identity().unwrap());
    assert!(matches!(
        Sdk::open(
            f.0.join("clone-b/store.db"),
            b.identity().unwrap(),
            schema(),
            Baseline::Reject
        ),
        Err(Error::DatabaseInUse)
    ));
    insert(&b, "x", "clone b", &[]).unwrap();
    insert(&d, "x", "clone d", &[]).unwrap();
    for s in [&b, &d] {
        let p = s.pending_envelopes(10).unwrap();
        let new = p
            .iter()
            .map(|e| Package::decode_envelope(&e.bytes).unwrap())
            .find(|p| p.key.identity == s.identity().unwrap())
            .unwrap();
        assert_eq!(new.key.seq, 1);
        assert!(p.iter().any(|p| p == &exact[0]));
    }
    let t = f.transport();
    sync(&b, &t).await;
    sync(&d, &t).await;
    sync(&b, &t).await;
    sync(&a, &t).await;
    assert_eq!(b.status().unwrap().pending_outbox, 0);
    assert_eq!(d.status().unwrap().pending_outbox, 0);
    assert_eq!(rows(&a), rows(&b));
    assert_eq!(rows(&b), rows(&d));
    assert_eq!(b.list_conflicts().unwrap()[0].heads.len(), 2);
    let conflict_backup = f.0.join("conflict-backup");
    b.backup(
        &conflict_backup,
        BackupKind::SharedBootstrap,
        BackupOptions::default(),
        &c,
    )
    .unwrap();
    let restored = Sdk::restore(&conflict_backup, f.0.join("conflict-restored"), &c).unwrap();
    assert_eq!(
        b.list_conflicts().unwrap(),
        restored.list_conflicts().unwrap()
    );
}
#[tokio::test]
async fn shared_refuses_unpublished_overwrite_and_restores_tombstones() {
    let f = Fixture::new();
    let a = f.sdk("a");
    let t = f.transport();
    let c = Cancellation::default();
    insert(&a, "x", "published p", &[]).unwrap();
    sync(&a, &t).await;
    insert(&a, "x", "unpublished q", &[]).unwrap();
    let backup = f.0.join("shared");
    assert!(
        a.backup(
            &backup,
            BackupKind::SharedBootstrap,
            BackupOptions::default(),
            &c
        )
        .is_err()
    );
    assert!(!backup.exists());
    sync(&a, &t).await;
    a.write_transaction(|w| w.execute("DELETE FROM notes WHERE id='x'", &[]))
        .unwrap();
    sync(&a, &t).await;
    a.backup(
        &backup,
        BackupKind::SharedBootstrap,
        BackupOptions::default(),
        &c,
    )
    .unwrap();
    let b = Sdk::restore(&backup, f.0.join("restored"), &c).unwrap();
    assert!(rows(&b).is_empty());
    assert_eq!(b.pending_envelopes(10).unwrap().len(), 0);
    let late = f.sdk("late");
    sync(&late, &t).await;
    assert!(rows(&late).is_empty());
    assert!(Sdk::restore(&backup, f.0.join("restored"), &c).is_err());
    assert!(rows(&b).is_empty());
}
#[test]
fn backup_cancel_budget_corrupt_missing_manifest_and_original_intact() {
    let f = Fixture::new();
    let a = f.sdk("a");
    let c = Cancellation::default();
    let file = a
        .add_attachment(&b"precious generated fixture"[..], 100, &c)
        .unwrap();
    insert(&a, "x", "pending", std::slice::from_ref(&file.hash)).unwrap();
    let exact = a.pending_envelopes(10).unwrap();
    let before = rows(&a);
    let cancel = Cancellation::default();
    cancel.cancel();
    assert!(
        a.backup(
            f.0.join("cancel"),
            BackupKind::LocalDisaster,
            BackupOptions::default(),
            &cancel
        )
        .is_err()
    );
    assert!(!f.0.join("cancel").exists());
    assert!(
        a.backup(
            f.0.join("budget"),
            BackupKind::LocalDisaster,
            BackupOptions {
                pages_per_step: 1,
                max_steps: 1,
                busy_retries: 0
            },
            &c
        )
        .is_err()
    );
    assert!(!f.0.join("budget").exists());
    let backup = f.0.join("backup");
    a.backup(
        &backup,
        BackupKind::LocalDisaster,
        BackupOptions::default(),
        &c,
    )
    .unwrap();
    let original = std::fs::read(backup.join("store.db")).unwrap();
    std::fs::write(backup.join("store.db"), b"bad").unwrap();
    assert!(Sdk::restore(&backup, f.0.join("bad-db"), &c).is_err());
    assert!(!f.0.join("bad-db").exists());
    std::fs::write(backup.join("store.db"), original).unwrap();
    std::fs::remove_file(backup.join("objects").join(&file.hash)).unwrap();
    assert!(Sdk::restore(&backup, f.0.join("missing-file"), &c).is_err());
    assert!(!f.0.join("missing-file").exists());
    let manifest = backup.join("manifest.json");
    let mut v: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    v["files"][0]["path"] = "../outside".into();
    std::fs::write(manifest, serde_json::to_vec(&v).unwrap()).unwrap();
    assert!(Sdk::restore(&backup, f.0.join("traversal"), &c).is_err());
    assert_eq!(rows(&a), before);
    assert_eq!(a.pending_envelopes(10).unwrap(), exact);
}

#[test]
fn crash_probe_child() {
    let Ok(dir) = std::env::var("LFS_STAGE4_CRASH_DIR") else {
        return;
    };
    let barrier = std::env::var("LFS_STAGE4_CRASH_BARRIER").unwrap();
    let sdk = Sdk::open(
        PathBuf::from(dir).join("crash.db"),
        identity("crash"),
        schema(),
        Baseline::Reject,
    )
    .unwrap();
    sdk.add_attachment_observed(
        &vec![0x44; attachments::CHUNK * 3 + 17][..],
        1_000_000,
        &Cancellation::default(),
        &|phase| {
            if format!("{phase:?}") == barrier {
                std::fs::write(
                    PathBuf::from(std::env::var("LFS_STAGE4_CRASH_DIR").unwrap())
                        .join("barrier.ready"),
                    barrier.as_bytes(),
                )
                .unwrap();
                loop {
                    std::thread::park();
                }
            }
        },
    )
    .unwrap();
    panic!("crash barrier missed");
}
#[test]
fn process_death_at_real_durability_barriers_reconciles() {
    for barrier in [
        "StagedDurable",
        "InstalledDurable",
        "InstalledMetadata",
        "AvailableMetadata",
    ] {
        let f = Fixture::new();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_probe_child", "--nocapture"])
            .env("LFS_STAGE4_CRASH_DIR", &f.0)
            .env("LFS_STAGE4_CRASH_BARRIER", barrier)
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !f.0.join("barrier.ready").exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let reached = f.0.join("barrier.ready").exists();
        child.kill().unwrap();
        let status = child.wait().unwrap();
        assert!(reached, "barrier not reached");
        assert!(!status.success());
        let s = Sdk::open(
            f.0.join("crash.db"),
            identity("crash"),
            schema(),
            Baseline::Reject,
        )
        .unwrap();
        let hash = lfs_protocol::hash_hex(&lfs_protocol::hash(&vec![
            0x44;
            attachments::CHUNK * 3 + 17
        ]));
        let available = s.attachment_status(&hash).unwrap().state == "Available";
        assert_eq!(available, barrier != "StagedDurable");
        if available {
            s.open_attachment(&hash, &Cancellation::default()).unwrap();
        }
        assert!(s.pending_envelopes(10).unwrap().is_empty());
        assert!(
            std::fs::read_dir(f.0.join("crash.db.attachments/staging"))
                .unwrap()
                .next()
                .is_none()
        );
    }
}

#[test]
fn rehashed_manifest_does_not_hide_dag_or_file_closure_corruption() {
    let f = Fixture::new();
    let a = f.sdk("a");
    let c = Cancellation::default();
    let file = a.add_attachment(&b"closure"[..], 100, &c).unwrap();
    insert(&a, "x", "pending", std::slice::from_ref(&file.hash)).unwrap();
    let backup = f.0.join("backup");
    a.backup(
        &backup,
        BackupKind::LocalDisaster,
        BackupOptions::default(),
        &c,
    )
    .unwrap();
    let conn = rusqlite::Connection::open(backup.join("store.db")).unwrap();
    conn.execute("DELETE FROM _lfs_tips", []).unwrap();
    drop(conn);
    let manifest_path = backup.join("manifest.json");
    let mut m: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    let bytes = std::fs::read(backup.join("store.db")).unwrap();
    let entry = m["files"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|f| f["path"] == "store.db")
        .unwrap();
    entry["sha256"] = lfs_protocol::hash_hex(&lfs_protocol::hash(&bytes)).into();
    entry["size"] = bytes.len().to_string().into();
    std::fs::write(manifest_path, serde_json::to_vec(&m).unwrap()).unwrap();
    assert!(Sdk::restore(&backup, f.0.join("bad-dag"), &c).is_err());
    assert!(!f.0.join("bad-dag").exists());
    assert_eq!(rows(&a).len(), 1);
}
#[tokio::test]
async fn blocked_causal_state_refuses_shared_export() {
    let f = Fixture::new();
    let a = f.sdk("a");
    let b = f.sdk("b");
    insert(&a, "x", "p", &[]).unwrap();
    insert(&a, "x", "q", &[]).unwrap();
    let envelopes = a.pending_envelopes(10).unwrap();
    let t = MemoryTransport::default();
    t.put(Package::decode_envelope(&envelopes[1].bytes).unwrap())
        .unwrap();
    sync(&b, &t).await;
    assert!(!b.status().unwrap().healthy());
    assert!(
        b.backup(
            f.0.join("blocked"),
            BackupKind::SharedBootstrap,
            BackupOptions::default(),
            &Cancellation::default()
        )
        .is_err()
    );
    assert!(!f.0.join("blocked").exists());
}

#[tokio::test]
async fn explicit_resolution_references_preserve_history_and_atomicity() {
    let f = Fixture::new();
    let a = f.sdk("a");
    let b = f.sdk("b");
    let t = f.transport();
    let c = Cancellation::default();
    let old = a
        .add_attachment(&b"old historical content"[..], 100, &c)
        .unwrap();
    insert(&a, "x", "base", std::slice::from_ref(&old.hash)).unwrap();
    sync(&a, &t).await;
    sync(&b, &t).await;
    insert(&a, "x", "a", &[]).unwrap();
    insert(&b, "x", "b", &[]).unwrap();
    sync(&a, &t).await;
    sync(&b, &t).await;
    sync(&a, &t).await;
    let heads = a.list_conflicts().unwrap()[0]
        .heads
        .iter()
        .map(|o| o.id.clone())
        .collect::<Vec<_>>();
    let new = a
        .add_attachment(&b"resolution content"[..], 100, &c)
        .unwrap();
    let refs = vec![new.hash.clone()];
    assert!(
        a.resolve_with_attachments(
            1,
            "x",
            heads.clone(),
            Some(vec![
                Value::Text("wrong-key".into()),
                Value::Text("bad".into())
            ]),
            &refs
        )
        .is_err()
    );
    assert!(a.pending_envelopes(10).unwrap().is_empty());
    a.resolve_with_attachments(1, "x", heads, None, &refs)
        .unwrap();
    let wire = a.pending_envelopes(10).unwrap();
    assert_eq!(
        Package::decode_envelope(&wire[0].bytes)
            .unwrap()
            .attachment_references,
        refs
    );
    sync(&a, &t).await;
    sync(&b, &t).await;
    let mut all = vec![old.hash, new.hash];
    all.sort();
    assert_eq!(b.record_attachment_references(1, "x").unwrap(), all);
    assert!(rows(&b).is_empty());
    assert!(b.list_conflicts().unwrap().is_empty());
}
#[test]
fn malformed_remote_reference_has_no_file_effect_and_valid_variant_stops_stream() {
    let f = Fixture::new();
    let a = f.sdk("a");
    insert(&a, "x", "p", &[]).unwrap();
    let mut p = Package::decode_envelope(&a.pending_envelopes(10).unwrap()[0].bytes).unwrap();
    let mut b = Store::open(f.0.join("b.db"), identity("b"), schema(), Baseline::Reject).unwrap();
    p.attachment_references = vec!["../../escape".into()];
    assert!(b.receive(p.clone()).is_err());
    assert!(
        b.read(|r| r.query("SELECT * FROM notes", &[]))
            .unwrap()
            .is_empty()
    );
    assert!(!f.0.join("escape").exists());
    p.attachment_references.clear();
    assert_eq!(b.receive(p.clone()).unwrap(), InboxState::Applied);
    p.attachment_references = vec!["0".repeat(64)];
    assert!(matches!(b.receive(p), Err(Error::StreamFork)));
    assert_eq!(b.status().unwrap().stopped_streams, 1);
}

struct InterruptedUpload {
    inner: OpenDalTransport,
    partial: PathBuf,
    cancel: bool,
}
impl Transport for InterruptedUpload {
    fn upload<'a>(&'a self, p: OutgoingEnvelope, c: &'a Cancellation) -> NetworkFuture<'a, ()> {
        self.inner.upload(p, c)
    }
    fn list<'a>(
        &'a self,
        d: &'a str,
        x: usize,
        n: usize,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, Page> {
        self.inner.list(d, x, n, c)
    }
    fn download<'a>(&'a self, k: &'a ObjectKey, c: &'a Cancellation) -> NetworkFuture<'a, Package> {
        self.inner.download(k, c)
    }
    fn upload_attachment<'a>(
        &'a self,
        _d: &'a str,
        _h: &'a str,
        mut file: File,
        _limit: u64,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, ()> {
        Box::pin(async move {
            let mut chunk = vec![0; attachments::CHUNK];
            let n = file.read(&mut chunk).unwrap();
            assert_eq!(n, attachments::CHUNK);
            let mut partial = File::create(&self.partial).unwrap();
            partial.write_all(&chunk).unwrap();
            partial.sync_all().unwrap();
            if self.cancel {
                c.cancel();
                Err(NetworkError::Cancelled)
            } else {
                Err(NetworkError::Transient)
            }
        })
    }
}
#[tokio::test]
async fn partial_upload_and_mid_transfer_cancel_keep_exact_pending_after_restart() {
    let f = Fixture::new();
    let a = f.sdk("a");
    let c = Cancellation::default();
    let bytes = vec![0x77; attachments::CHUNK * 4 + 1];
    let file = a.add_attachment(bytes.as_slice(), 1_000_000, &c).unwrap();
    insert(&a, "x", "pending", std::slice::from_ref(&file.hash)).unwrap();
    let exact = a.pending_envelopes(10).unwrap();
    for cancel in [false, true] {
        let fault = InterruptedUpload {
            inner: f.transport(),
            partial: f.0.join("fault-partial-upload"),
            cancel,
        };
        let result = a
            .sync_once(&fault, &Cancellation::default(), SyncLimits::default())
            .await;
        assert!(if cancel {
            matches!(result, Err(Error::Network(NetworkError::Cancelled)))
        } else {
            matches!(result, Err(Error::Network(NetworkError::Transient)))
        });
        assert_eq!(
            std::fs::metadata(&fault.partial).unwrap().len(),
            attachments::CHUNK as u64
        );
        assert_eq!(a.pending_envelopes(10).unwrap(), exact);
        assert!(
            fault
                .list("stage4", 0, 100, &c)
                .await
                .unwrap()
                .keys
                .is_empty()
        );
    }
    drop(a);
    let a = f.sdk("a");
    assert_eq!(a.pending_envelopes(10).unwrap(), exact);
    let t = f.transport();
    sync(&a, &t).await;
    assert!(a.pending_envelopes(10).unwrap().is_empty());
    let b = f.sdk("b");
    sync(&b, &t).await;
    b.download_attachment(&t, &file.hash, 1_000_000, &c)
        .await
        .unwrap();
    assert_eq!(rows(&a), rows(&b));
}
#[test]
fn backup_busy_during_active_write_does_not_create_destination() {
    let f = Fixture::new();
    let a = f.sdk("a");
    let enter = std::sync::Barrier::new(2);
    let leave = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            a.write_transaction(|_| {
                enter.wait();
                leave.wait();
                Ok(())
            })
        });
        enter.wait();
        let result = a.backup(
            f.0.join("busy"),
            BackupKind::LocalDisaster,
            BackupOptions::default(),
            &Cancellation::default(),
        );
        leave.wait();
        writer.join().unwrap().unwrap();
        assert!(matches!(result, Err(Error::Busy)));
        assert!(!f.0.join("busy").exists());
    });
}

// Mandated additive v2 P6; v1 P1-P5 and its optional UNKNOWN observation are unchanged.
#[test]
fn p6_shared_excludes_unreferenced_available_cache() {
    let f = Fixture::new();
    let a = f.sdk("a");
    let c = Cancellation::default();
    let private = a
        .add_attachment(&b"P6 private never referenced cache fixture"[..], 100, &c)
        .unwrap();
    let shared = f.0.join("shared");
    let m = a
        .backup(
            &shared,
            BackupKind::SharedBootstrap,
            BackupOptions::default(),
            &c,
        )
        .unwrap();
    assert!(
        !m.files
            .iter()
            .any(|e| e.path == format!("objects/{}", private.hash)),
        "P6: unrelated Available local cache included in SharedBootstrap: {}",
        private.hash
    );
}

#[tokio::test]
async fn p6_history_restore_local_pending_and_source_unchanged() {
    let f = Fixture::new();
    let a = f.sdk("a");
    let t = f.transport();
    let c = Cancellation::default();
    let private_bytes = b"P6 never published private file exact byte canary";
    let private = a.add_attachment(&private_bytes[..], 100, &c).unwrap();
    let historical_bytes = b"P6 public ancestral attachment";
    let historical = a.add_attachment(&historical_bytes[..], 100, &c).unwrap();
    insert(&a, "x", "ancestor", std::slice::from_ref(&historical.hash)).unwrap();
    sync(&a, &t).await;
    insert(&a, "x", "superseded without attachment", &[]).unwrap();
    a.write_transaction(|w| w.execute("DELETE FROM notes WHERE id='x'", &[]))
        .unwrap();
    sync(&a, &t).await;
    assert!(a.status().unwrap().healthy());
    let before_db = std::fs::read(f.0.join("a.db")).unwrap();
    let before_rows = rows(&a);
    let before_status = a.status().unwrap();
    let shared = f.0.join("shared");
    let m = a
        .backup(
            &shared,
            BackupKind::SharedBootstrap,
            BackupOptions::default(),
            &c,
        )
        .unwrap();
    assert_eq!(m.format, 1);
    assert_eq!(m.protocol, 1);
    assert_eq!(m.internal, [2, 1, 1, 1]);
    assert_eq!(m.files.len(), 2);
    assert!(!shared.join("objects").join(&private.hash).exists());
    assert_eq!(
        std::fs::read(shared.join("objects").join(&historical.hash)).unwrap(),
        historical_bytes
    );
    for path in ["manifest.json", "store.db"] {
        let bytes = std::fs::read(shared.join(path)).unwrap();
        for excluded in [private.hash.as_bytes(), &private_bytes[..]] {
            assert!(
                !bytes.windows(excluded.len()).any(|w| w == excluded),
                "private bytes in {path}"
            );
        }
    }
    let inspect = rusqlite::Connection::open_with_flags(
        shared.join("store.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let files: Vec<String> = inspect
        .prepare("SELECT hash FROM _lfs_files")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    assert_eq!(files, vec![historical.hash.clone()]);
    assert_eq!(
        inspect
            .query_row("PRAGMA freelist_count", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    drop(inspect);
    assert_eq!(std::fs::read(f.0.join("a.db")).unwrap(), before_db);
    assert_eq!(rows(&a), before_rows);
    assert_eq!(
        format!("{:?}", a.status().unwrap()),
        format!("{before_status:?}")
    );
    assert_eq!(
        a.attachment_status(&private.hash).unwrap().state,
        "Available"
    );
    let b = Sdk::restore(&shared, f.0.join("restored"), &c).unwrap();
    assert_eq!(rows(&b), before_rows);
    assert_eq!(
        b.record_attachment_references(1, "x").unwrap(),
        vec![historical.hash.clone()]
    );
    let mut bytes = Vec::new();
    b.open_attachment(&historical.hash, &c)
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    assert_eq!(bytes, historical_bytes);
    assert_ne!(b.identity().unwrap(), a.identity().unwrap());
    insert(&b, "restored", "new operation", &[]).unwrap();
    sync(&b, &t).await;
    assert!(b.status().unwrap().healthy());

    insert(&a, "pending", "local only", &[]).unwrap();
    let exact = a.pending_envelopes(10).unwrap();
    assert_eq!(exact.len(), 1);
    let before_db = std::fs::read(f.0.join("a.db")).unwrap();
    let refused = f.0.join("refused");
    assert!(
        a.backup(
            &refused,
            BackupKind::SharedBootstrap,
            BackupOptions::default(),
            &c
        )
        .is_err()
    );
    assert!(!refused.exists());
    let local = f.0.join("local");
    let m = a
        .backup(
            &local,
            BackupKind::LocalDisaster,
            BackupOptions::default(),
            &c,
        )
        .unwrap();
    assert!(
        m.files
            .iter()
            .any(|e| e.path == format!("objects/{}", private.hash))
    );
    assert_eq!(
        std::fs::read(local.join("objects").join(&private.hash)).unwrap(),
        private_bytes
    );
    let restored = Sdk::restore(&local, f.0.join("local-restored"), &c).unwrap();
    assert_eq!(restored.pending_envelopes(10).unwrap(), exact);
    let mut bytes = Vec::new();
    restored
        .open_attachment(&private.hash, &c)
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    assert_eq!(bytes, private_bytes);
    assert_eq!(std::fs::read(f.0.join("a.db")).unwrap(), before_db);
    assert_eq!(a.pending_envelopes(10).unwrap(), exact);
    assert_eq!(
        std::fs::read(f.0.join("a.db.attachments/objects").join(&private.hash)).unwrap(),
        private_bytes
    );
}

#[test]
fn p6_failed_backup_does_not_mutate_source_metadata() {
    let f = Fixture::new();
    let a = f.sdk("a");
    let c = Cancellation::default();
    let file = a
        .add_attachment(&b"generated corruption fixture"[..], 100, &c)
        .unwrap();
    std::fs::write(
        f.0.join("a.db.attachments/objects").join(&file.hash),
        b"corrupt fixture",
    )
    .unwrap();
    let before = std::fs::read(f.0.join("a.db")).unwrap();
    let dest = f.0.join("failed");
    assert!(
        a.backup(
            &dest,
            BackupKind::LocalDisaster,
            BackupOptions::default(),
            &c
        )
        .is_err()
    );
    assert!(!dest.exists());
    assert_eq!(std::fs::read(f.0.join("a.db")).unwrap(), before);
    assert_eq!(a.attachment_status(&file.hash).unwrap().state, "Available");
    let cancelled = Cancellation::default();
    cancelled.cancel();
    assert!(
        a.backup(
            &dest,
            BackupKind::SharedBootstrap,
            BackupOptions::default(),
            &cancelled
        )
        .is_err()
    );
    assert!(!dest.exists());
    assert_eq!(std::fs::read(f.0.join("a.db")).unwrap(), before);
}
