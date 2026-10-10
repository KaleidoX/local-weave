//! Mandated S2 public API tests. Memory is MODEL only; filesystem tests perform real I/O.
use lfs_protocol::hash;
use lfs_sqlite::*;
use lfs_storage::{OpenDalTransport, Options, object_path};
use opendal::{Capability, OperationContext, Operator, raw::*, services};
use std::{
    io::Read as _,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Debug, Default)]
struct Counts {
    applied: AtomicUsize,
    contexts: AtomicUsize,
    read: AtomicUsize,
    write: AtomicUsize,
    stat: AtomicUsize,
    list: AtomicUsize,
    scheduled: AtomicUsize,
    completed: AtomicUsize,
}
fn count(a: &AtomicUsize) -> usize {
    a.load(Ordering::SeqCst)
}
#[derive(Debug)]
struct HostLayer(Arc<Counts>, bool);
impl Layer for HostLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        self.0.applied.fetch_add(1, Ordering::SeqCst);
        Arc::new(Observed {
            inner,
            counts: self.0.clone(),
            deny_list: self.1,
            reported_root: None,
        })
    }
    fn apply_context(&self, _: Servicer, ctx: OperationContext) -> OperationContext {
        self.0.contexts.fetch_add(1, Ordering::SeqCst);
        ctx
    }
}
#[derive(Debug)]
struct ReportedRoot(String);
impl Layer for ReportedRoot {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(Observed {
            inner,
            counts: Arc::new(Counts::default()),
            deny_list: false,
            reported_root: Some(self.0.clone()),
        })
    }
}
struct HostExecutor(Arc<Counts>);
impl opendal::Execute for HostExecutor {
    fn execute(&self, f: BoxedStaticFuture<()>) {
        self.0.scheduled.fetch_add(1, Ordering::SeqCst);
        let counts = self.0.clone();
        tokio::spawn(async move {
            f.await;
            counts.completed.fetch_add(1, Ordering::SeqCst);
        });
    }
}
#[derive(Debug)]
struct Observed {
    inner: Servicer,
    counts: Arc<Counts>,
    deny_list: bool,
    reported_root: Option<String>,
}
impl Service for Observed {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;
    fn info(&self) -> ServiceInfo {
        let info = self.inner.info();
        match &self.reported_root {
            Some(root) => info.with_root(root),
            None => info,
        }
    }
    fn capability(&self) -> Capability {
        let mut c = self.inner.capability();
        if self.deny_list {
            c.list = false;
        }
        c
    }
    async fn stat(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpStat,
    ) -> opendal::Result<RpStat> {
        self.counts.stat.fetch_add(1, Ordering::SeqCst);
        // Host instrumentation uses the actual context supplied to a real service call.
        let (tx, rx) = tokio::sync::oneshot::channel();
        ctx.executor()
            .clone()
            .into_inner()
            .execute(Box::pin(async move {
                tx.send(()).unwrap();
            }));
        rx.await.unwrap();
        self.inner.stat(ctx, path, args).await
    }
    fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> opendal::Result<Self::Reader> {
        self.counts.read.fetch_add(1, Ordering::SeqCst);
        self.inner.read(ctx, path, args)
    }
    fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> opendal::Result<Self::Writer> {
        self.counts.write.fetch_add(1, Ordering::SeqCst);
        self.inner.write(ctx, path, args)
    }
    fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> opendal::Result<Self::Lister> {
        self.counts.list.fetch_add(1, Ordering::SeqCst);
        self.inner.list(ctx, path, args)
    }
    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> opendal::Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }
    fn delete(&self, ctx: &OperationContext) -> opendal::Result<Self::Deleter> {
        self.inner.delete(ctx)
    }
    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> opendal::Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args)
    }
    fn compose(
        &self,
        ctx: &OperationContext,
        to: &str,
        args: OpCompose,
    ) -> opendal::Result<Self::Composer> {
        self.inner.compose(ctx, to, args)
    }
    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> opendal::Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }
    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> opendal::Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}
fn observed(op: Operator, counts: &Arc<Counts>) -> Operator {
    op.with_context(
        OperationContext::new()
            .with_executor(opendal::Executor::with(HostExecutor(counts.clone()))),
    )
    .layer(HostLayer(counts.clone(), false))
}
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let p = PathBuf::from(std::env::var_os("TMPDIR").expect("explicit TMPDIR")).join(format!(
            "s2-native-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&p).unwrap();
        std::fs::create_dir(p.join("remote")).unwrap();
        Self(p)
    }
    fn root(&self) -> PathBuf {
        self.0.join("remote")
    }
    fn op(&self) -> Operator {
        fs_op(&self.root())
    }
    fn sdk(&self, device: &str) -> Sdk {
        Sdk::open(
            self.0.join(format!("{device}.db")),
            Identity {
                dataset: "native-S2".into(),
                device: device.into(),
                incarnation: "one".into(),
            },
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
            },
            Baseline::Reject,
        )
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn fs_op(root: &Path) -> Operator {
    Operator::new(services::Fs::default().root(root.to_str().unwrap())).unwrap()
}
fn insert(s: &Sdk) {
    s.write_transaction(|w| w.execute("INSERT INTO notes VALUES ('key','native')", &[]))
        .unwrap();
}
async fn sync(s: &Sdk, t: &OpenDalTransport) {
    assert!(
        s.sync_once(t, &Cancellation::default(), SyncLimits::default())
            .await
            .unwrap()
            .healthy()
    );
}
fn rows(s: &Sdk) -> Vec<Vec<Value>> {
    s.read(|r| r.query("SELECT * FROM notes ORDER BY id", &[]))
        .unwrap()
}
fn no_io(c: &Counts) {
    assert_eq!(
        [
            count(&c.read),
            count(&c.write),
            count(&c.stat),
            count(&c.list),
            count(&c.scheduled)
        ],
        [0; 5]
    );
}
fn assert_observed(c: &Counts) {
    assert_eq!(count(&c.applied), 1);
    assert_eq!(count(&c.contexts), 1);
    for a in [
        &c.read,
        &c.write,
        &c.stat,
        &c.list,
        &c.scheduled,
        &c.completed,
    ] {
        assert!(count(a) > 0);
    }
    println!(
        "host layer apply={} context={} read={} write={} stat={} list={} executor_scheduled={} completed={}",
        count(&c.applied),
        count(&c.contexts),
        count(&c.read),
        count(&c.write),
        count(&c.stat),
        count(&c.list),
        count(&c.scheduled),
        count(&c.completed)
    );
}
#[tokio::test]
async fn native_fs_two_devices_exact_retry_layer_context_and_attachment() {
    let f = Fixture::new();
    let counts = Arc::new(Counts::default());
    let t =
        OpenDalTransport::from_fs_operator(f.root(), observed(f.op(), &counts), Options::default())
            .unwrap();
    no_io(&counts);
    assert_eq!(count(&counts.applied), 1);
    assert_eq!(count(&counts.contexts), 1);
    assert_eq!(std::fs::read_dir(f.root()).unwrap().count(), 0);
    let a = f.sdk("a");
    let b = f.sdk("b");
    let c = Cancellation::default();
    let content = vec![0x57; attachments::CHUNK * 3 + 17];
    let attachment = a
        .add_attachment(&content[..], content.len() as u64, &c)
        .unwrap();
    a.write_transaction_with_attachments(std::slice::from_ref(&attachment.hash), |w| {
        w.execute("INSERT INTO notes VALUES ('key','native')", &[])
    })
    .unwrap();
    let exact = a.pending_envelopes(1).unwrap().remove(0);
    drop(a);
    let a = f.sdk("a");
    assert_eq!(a.pending_envelopes(1).unwrap(), vec![exact.clone()]);
    sync(&a, &t).await;
    let path = f.root().join(object_path(&exact.key).unwrap());
    assert_eq!(std::fs::read(&path).unwrap(), exact.bytes);
    assert_eq!(
        hash(&std::fs::read(&path).unwrap()),
        exact.key.envelope_hash.unwrap()
    );
    t.upload(exact.clone(), &c).await.unwrap();
    assert_eq!(
        t.download(&exact.key, &c)
            .await
            .unwrap()
            .encode_envelope()
            .unwrap(),
        exact.bytes
    );
    sync(&b, &t).await;
    assert_eq!(rows(&a), rows(&b));
    assert!(a.pending_envelopes(10).unwrap().is_empty());
    assert!(b.pending_envelopes(10).unwrap().is_empty());
    assert_eq!(
        b.attachment_status(&attachment.hash).unwrap().state,
        "Missing"
    );
    b.download_attachment(&t, &attachment.hash, content.len() as u64, &c)
        .await
        .unwrap();
    assert_eq!(
        b.attachment_status(&attachment.hash).unwrap().state,
        "Available"
    );
    let mut back = Vec::new();
    b.open_attachment(&attachment.hash, &c)
        .unwrap()
        .read_to_end(&mut back)
        .unwrap();
    assert_eq!(back, content);
    assert_observed(&counts);
}
#[tokio::test]
async fn generic_memory_model_preserves_layer_and_executor() {
    let f = Fixture::new();
    let a = f.sdk("a");
    let b = f.sdk("b");
    insert(&a);
    let counts = Arc::new(Counts::default());
    let t = OpenDalTransport::from_operator(
        observed(Operator::new(services::Memory::default()).unwrap(), &counts),
        Options::default(),
    )
    .unwrap();
    no_io(&counts);
    assert_eq!(count(&counts.applied), 1);
    assert_eq!(count(&counts.contexts), 1);
    sync(&a, &t).await;
    sync(&b, &t).await;
    assert_eq!(rows(&a), rows(&b));
    assert!(b.pending_envelopes(1).unwrap().is_empty());
    assert!(!t.attachment_streaming_supported());
    assert_observed(&counts);
}
#[test]
fn constructors_reject_scheme_root_aliases_missing_capability_and_timeout() {
    let f = Fixture::new();
    assert!(OpenDalTransport::from_operator(f.op(), Options::default()).is_err());
    assert!(
        OpenDalTransport::from_fs_operator(
            f.root(),
            Operator::new(services::Memory::default()).unwrap(),
            Options::default()
        )
        .is_err()
    );
    let other = f.0.join("other");
    std::fs::create_dir(&other).unwrap();
    for root in [
        other,
        PathBuf::from("relative"),
        f.0.join("missing"),
        f.root().join(".."),
        PathBuf::from(format!("{}/./", f.root().display())),
    ] {
        assert!(OpenDalTransport::from_fs_operator(root, f.op(), Options::default()).is_err());
    }
    let file = f.0.join("file");
    std::fs::write(&file, b"x").unwrap();
    assert!(OpenDalTransport::from_fs_operator(&file, f.op(), Options::default()).is_err());
    for seconds in [0, 121] {
        let opts = Options {
            timeout: std::time::Duration::from_secs(seconds),
            ..Default::default()
        };
        assert!(matches!(
            OpenDalTransport::from_fs_operator(f.root(), f.op(), opts),
            Err(NetworkError::Limit)
        ));
        assert!(matches!(
            OpenDalTransport::from_operator(
                Operator::new(services::Memory::default()).unwrap(),
                opts
            ),
            Err(NetworkError::Limit)
        ));
    }
    let counts = Arc::new(Counts::default());
    let op = f.op().layer(HostLayer(counts.clone(), true));
    assert!(matches!(
        OpenDalTransport::from_fs_operator(f.root(), op, Options::default()),
        Err(NetworkError::Unsupported)
    ));
    let op = Operator::new(services::Memory::default())
        .unwrap()
        .layer(HostLayer(counts.clone(), true));
    assert!(matches!(
        OpenDalTransport::from_operator(op, Options::default()),
        Err(NetworkError::Unsupported)
    ));
    no_io(&counts);
    assert_eq!(std::fs::read_dir(f.root()).unwrap().count(), 0);
    assert!(!f.0.join("missing").exists());
}
#[cfg(unix)]
#[tokio::test]
async fn native_fs_rejects_ancestor_and_publish_symlinks_without_ack() {
    let f = Fixture::new();
    let alias = f.0.join("alias");
    std::os::unix::fs::symlink(f.root(), &alias).unwrap();
    assert!(OpenDalTransport::from_fs_operator(&alias, f.op(), Options::default()).is_err());
    // Fs builder already canonicalizes its input; original spelling is unavailable.
    let canonical_op = fs_op(&alias);
    assert_eq!(Path::new(&canonical_op.info().root()), f.root());
    assert!(OpenDalTransport::from_fs_operator(f.root(), canonical_op, Options::default()).is_ok());
    // A host layer reporting an actual symlink root must still be rejected.
    let reported_alias = f.op().layer(ReportedRoot(alias.to_str().unwrap().into()));
    assert!(
        OpenDalTransport::from_fs_operator(f.root(), reported_alias, Options::default()).is_err()
    );
    std::fs::create_dir(f.root().join("child")).unwrap();
    assert!(
        OpenDalTransport::from_fs_operator(
            alias.join("child"),
            fs_op(&f.root().join("child")),
            Options::default()
        )
        .is_err()
    );
    let t = OpenDalTransport::from_fs_operator(f.root(), f.op(), Options::default()).unwrap();
    let a = f.sdk("a");
    insert(&a);
    let exact = a.pending_envelopes(1).unwrap().remove(0);
    let outside = f.0.join("outside");
    std::fs::create_dir(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, f.root().join("lfs1")).unwrap();
    assert!(
        a.sync_once(&t, &Cancellation::default(), SyncLimits::default())
            .await
            .is_err()
    );
    assert_eq!(a.pending_envelopes(1).unwrap(), vec![exact]);
    assert_eq!(std::fs::read_dir(outside).unwrap().count(), 0);
    assert!(
        t.list("native-S2", 0, 100, &Cancellation::default())
            .await
            .is_err()
    );
}
