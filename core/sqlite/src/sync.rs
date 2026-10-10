//! Bounded synchronization and MODEL/FAULT transport. The host supplies runtime and storage.
use crate::*;
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
pub type NetworkFuture<'a, T> =
    Pin<Box<dyn Future<Output = std::result::Result<T, NetworkError>> + Send + 'a>>;
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NetworkError {
    Transient,
    Auth,
    RateLimit,
    Timeout,
    Cancelled,
    Missing,
    LostAck,
    Corrupt,
    Limit,
    Unsupported,
}
#[derive(Default)]
struct CancelState {
    cancelled: AtomicBool,
    next: AtomicU64,
    wakers: Mutex<BTreeMap<u64, std::task::Waker>>,
}
#[derive(Clone, Default)]
pub struct Cancellation(Arc<CancelState>);
impl Cancellation {
    pub fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::Release);
        let wakers = std::mem::take(&mut *self.0.wakers.lock().unwrap());
        for (_, w) in wakers {
            w.wake();
        }
    }
    pub fn check(&self) -> std::result::Result<(), NetworkError> {
        if self.0.cancelled.load(Ordering::Acquire) {
            Err(NetworkError::Cancelled)
        } else {
            Ok(())
        }
    }
    pub async fn during<T>(
        &self,
        f: impl Future<Output = std::result::Result<T, NetworkError>>,
    ) -> std::result::Result<T, NetworkError> {
        let id = self.0.next.fetch_add(1, Ordering::Relaxed);
        let _guard = Registration { token: self, id };
        let mut f = std::pin::pin!(f);
        std::future::poll_fn(|cx| {
            if let Err(e) = self.check() {
                return std::task::Poll::Ready(Err(e));
            }
            {
                let mut w = self.0.wakers.lock().unwrap();
                if !w.contains_key(&id) && w.len() >= 64 {
                    return std::task::Poll::Ready(Err(NetworkError::Limit));
                }
                w.insert(id, cx.waker().clone());
            }
            if let Err(e) = self.check() {
                return std::task::Poll::Ready(Err(e));
            }
            f.as_mut().poll(cx)
        })
        .await
    }
}
struct Registration<'a> {
    token: &'a Cancellation,
    id: u64,
}
impl Drop for Registration<'_> {
    fn drop(&mut self) {
        self.token.0.wakers.lock().unwrap().remove(&self.id);
    }
}
#[derive(Clone, Debug)]
pub struct Page {
    pub keys: Vec<ObjectKey>,
    pub next: Option<usize>,
    /// False when enumeration reports missing/deferred entries; caller must rescan.
    pub complete: bool,
}
pub enum Download {
    Package(Package),
    Wire(Vec<u8>),
}
pub trait Transport: Send + Sync {
    /// Must stream and fully read back SHA256 before success; defaults fail closed.
    fn upload_attachment<'a>(
        &'a self,
        _dataset: &'a str,
        _hash: &'a str,
        _file: File,
        _limit: u64,
        _c: &'a Cancellation,
    ) -> NetworkFuture<'a, ()> {
        Box::pin(async { Err(NetworkError::Unsupported) })
    }
    /// Writes bounded chunks into a staging file, never marks local availability.
    fn download_attachment<'a>(
        &'a self,
        _dataset: &'a str,
        _hash: &'a str,
        _file: File,
        _limit: u64,
        _c: &'a Cancellation,
    ) -> NetworkFuture<'a, ()> {
        Box::pin(async { Err(NetworkError::Unsupported) })
    }

    /// Preserve bounded raw wire evidence for the core validator when supported.
    fn fetch<'a>(&'a self, key: &'a ObjectKey, c: &'a Cancellation) -> NetworkFuture<'a, Download> {
        Box::pin(async move { self.download(key, c).await.map(Download::Package) })
    }

    fn upload<'a>(&'a self, p: OutgoingEnvelope, c: &'a Cancellation) -> NetworkFuture<'a, ()>;
    fn list<'a>(
        &'a self,
        dataset: &'a str,
        cursor: usize,
        limit: usize,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, Page>;
    fn download<'a>(
        &'a self,
        key: &'a ObjectKey,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, Package>;
}
#[derive(Clone, Debug)]
pub enum Fault {
    Upload(NetworkError),
    List(NetworkError),
    Download(NetworkError),
    LostAck,
    MissingPage,
    DuplicatePage,
    LatePage,
    CorruptDownload,
}
#[derive(Default)]
struct Memory {
    objects: BTreeMap<PacketKey, Package>,
    faults: VecDeque<Fault>,
}
#[derive(Clone, Default)]
pub struct MemoryTransport(Arc<Mutex<Memory>>);
impl MemoryTransport {
    /// Queue is deliberately bounded; each matching seam is consumed once.
    pub fn inject(&self, f: Fault) -> std::result::Result<(), NetworkError> {
        let mut m = self.0.lock().unwrap();
        if m.faults.len() >= 64 {
            return Err(NetworkError::Limit);
        }
        m.faults.push_back(f);
        Ok(())
    }
    pub fn objects(&self) -> Vec<Package> {
        self.0.lock().unwrap().objects.values().cloned().collect()
    }
    pub fn put(&self, p: Package) -> std::result::Result<(), NetworkError> {
        let mut m = self.0.lock().unwrap();
        if p.bytes.len() > lfs_protocol::MAX_PACKET || m.objects.len() >= 10000 {
            return Err(NetworkError::Limit);
        }
        if m.objects.get(&p.key).is_some_and(|old| old != &p) {
            return Err(NetworkError::Corrupt);
        }
        m.objects.insert(p.key.clone(), p);
        Ok(())
    }
}
impl Transport for MemoryTransport {
    fn upload<'a>(&'a self, p: OutgoingEnvelope, c: &'a Cancellation) -> NetworkFuture<'a, ()> {
        Box::pin(async move {
            c.check()?;
            let fault = {
                let mut m = self.0.lock().unwrap();
                if matches!(m.faults.front(), Some(Fault::Upload(_) | Fault::LostAck)) {
                    m.faults.pop_front()
                } else {
                    None
                }
            };
            if let Some(Fault::Upload(e)) = fault {
                return Err(e);
            }
            self.put(p.validate().map_err(|_| NetworkError::Corrupt)?)?;
            c.check()?;
            if matches!(fault, Some(Fault::LostAck)) {
                Err(NetworkError::LostAck)
            } else {
                Ok(())
            }
        })
    }
    fn list<'a>(
        &'a self,
        dataset: &'a str,
        cursor: usize,
        limit: usize,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, Page> {
        Box::pin(async move {
            c.check()?;
            if limit == 0 || limit > 1000 {
                return Err(NetworkError::Limit);
            }
            let mut m = self.0.lock().unwrap();
            let fault = if matches!(
                m.faults.front(),
                Some(Fault::List(_) | Fault::MissingPage | Fault::DuplicatePage | Fault::LatePage)
            ) {
                m.faults.pop_front()
            } else {
                None
            };
            if let Some(Fault::List(e)) = fault {
                return Err(e);
            }
            let all: Vec<_> = m
                .objects
                .keys()
                .filter(|k| k.identity.dataset == dataset)
                .cloned()
                .map(|packet| ObjectKey {
                    packet,
                    envelope_hash: None,
                })
                .collect();
            if cursor > all.len() {
                return Err(NetworkError::Limit);
            }
            let end = (cursor + limit).min(all.len());
            let mut keys = all[cursor..end].to_vec();
            let next = if end < all.len() { Some(end) } else { None };
            let complete = !matches!(
                fault,
                Some(Fault::MissingPage | Fault::LatePage | Fault::DuplicatePage)
            );
            match fault {
                Some(Fault::MissingPage | Fault::LatePage) => keys.clear(),
                Some(Fault::DuplicatePage) => {
                    if let Some(k) = keys.first().cloned() {
                        keys = vec![k; keys.len()]
                    }
                }
                _ => {}
            }
            Ok(Page {
                keys,
                next,
                complete,
            })
        })
    }
    fn download<'a>(
        &'a self,
        key: &'a ObjectKey,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, Package> {
        Box::pin(async move {
            c.check()?;
            let mut m = self.0.lock().unwrap();
            let fault = if matches!(
                m.faults.front(),
                Some(Fault::Download(_) | Fault::CorruptDownload)
            ) {
                m.faults.pop_front()
            } else {
                None
            };
            if let Some(Fault::Download(e)) = fault {
                return Err(e);
            }
            let mut p = m
                .objects
                .get(&key.packet)
                .cloned()
                .ok_or(NetworkError::Missing)?;
            if matches!(fault, Some(Fault::CorruptDownload))
                && let Some(b) = p.bytes.last_mut()
            {
                *b ^= 1;
            }
            Ok(p)
        })
    }
}
#[derive(Clone, Debug)]
pub struct SyncLimits {
    pub uploads: usize,
    pub pages: usize,
    pub page_size: usize,
    pub downloads: usize,
    pub retries: usize,
}
impl Default for SyncLimits {
    fn default() -> Self {
        Self {
            uploads: 100,
            pages: 100,
            page_size: 100,
            downloads: 1000,
            retries: 2,
        }
    }
}
#[derive(Clone, Debug)]
pub struct SyncReport {
    pub uploaded: usize,
    pub downloaded: usize,
    pub complete_scan: bool,
    pub status: Status,
}
impl SyncReport {
    pub fn healthy(&self) -> bool {
        self.complete_scan && self.status.healthy() && self.status.pending_outbox == 0
    }
}
#[derive(Clone, Debug)]
pub struct Event {
    pub success: bool,
    pub detail: String,
}
pub struct Sdk {
    store: Mutex<Option<Store>>,
    syncing: AtomicBool,
    events: Mutex<VecDeque<Event>>,
    last_error: Mutex<Option<String>>,
}
struct SyncGuard<'a>(&'a AtomicBool);
impl Drop for SyncGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release)
    }
}
impl Sdk {
    pub fn open(
        path: impl AsRef<Path>,
        identity: Identity,
        schema: Schema,
        baseline: Baseline,
    ) -> Result<Self> {
        Ok(Self {
            store: Mutex::new(Some(Store::open(path, identity, schema, baseline)?)),
            syncing: AtomicBool::new(false),
            events: Mutex::new(VecDeque::new()),
            last_error: Mutex::new(None),
        })
    }
    pub(crate) fn with<T>(&self, f: impl FnOnce(&mut Store) -> Result<T>) -> Result<T> {
        let mut s = self.store.try_lock().map_err(|e| match e {
            std::sync::TryLockError::WouldBlock => Error::Busy,
            _ => Error::Poisoned,
        })?;
        f(s.as_mut().ok_or(Error::Closed)?)
    }
    pub fn read<T>(&self, f: impl FnOnce(&Read<'_>) -> Result<T>) -> Result<T> {
        self.with(|s| s.read(f))
    }
    pub fn write_transaction<T>(
        &self,
        f: impl FnOnce(&Write<'_>) -> Result<T>,
    ) -> Result<(T, Option<Commit>)> {
        self.with(|s| s.write_transaction(f))
    }
    pub fn pending_envelopes(&self, limit: usize) -> Result<Vec<OutgoingEnvelope>> {
        self.with(|s| s.pending_envelopes(limit))
    }
    pub fn identity(&self) -> Result<Identity> {
        self.with(|s| Ok(s.identity.clone()))
    }
    pub fn schema(&self) -> Result<Schema> {
        self.with(|s| Ok(s.schema.clone()))
    }
    pub fn add_attachment(
        &self,
        input: impl std::io::Read,
        limit: u64,
        c: &Cancellation,
    ) -> Result<attachments::Attachment> {
        self.with(|s| s.add_attachment(input, limit, c))
    }
    pub fn add_attachment_observed(
        &self,
        input: impl std::io::Read,
        limit: u64,
        c: &Cancellation,
        observer: &impl Fn(attachments::AttachmentBarrier),
    ) -> Result<attachments::Attachment> {
        self.with(|s| s.add_attachment_observed(input, limit, c, observer))
    }
    pub fn open_attachment(&self, h: &str, c: &Cancellation) -> Result<File> {
        self.with(|s| s.open_attachment(h, c))
    }
    pub fn attachment_status(&self, h: &str) -> Result<attachments::AttachmentStatus> {
        self.with(|s| s.attachment_status(h))
    }
    pub fn write_transaction_with_attachments<T>(
        &self,
        refs: &[String],
        f: impl FnOnce(&Write<'_>) -> Result<T>,
    ) -> Result<(T, Option<Commit>)> {
        self.with(|s| s.write_transaction_with_attachments(refs, f))
    }
    pub async fn download_attachment(
        &self,
        t: &impl Transport,
        h: &str,
        limit: u64,
        c: &Cancellation,
    ) -> Result<attachments::Attachment> {
        attachments::digest(h)?;
        attachments::check_limit(limit)?;
        c.check().map_err(Error::Network)?;
        let (staging, dataset) =
            self.with(|s| Ok((s.begin_attachment()?, s.identity.dataset.clone())))?;
        t.download_attachment(&dataset, h, staging.file.try_clone()?, limit, c)
            .await
            .map_err(Error::Network)?;
        self.with(|s| s.install_attachment(staging, Some(h), limit, c))
    }
    pub fn failed_attempts(&self) -> Result<Vec<FailedAttempt>> {
        self.with(Store::failed_attempts)
    }
    pub fn received_envelopes(&self) -> Result<Vec<Vec<u8>>> {
        self.with(Store::received_envelopes)
    }
    pub fn status(&self) -> Result<Status> {
        let mut status = self.with(Store::status)?;
        status.last_sync_error = self.last_error.lock().unwrap().clone();
        Ok(status)
    }
    pub fn list_conflicts(&self) -> Result<Vec<Conflict>> {
        self.with(Store::list_conflicts)
    }
    pub fn resolve(
        &self,
        t: u32,
        k: &str,
        parents: Vec<OpId>,
        row: Option<Vec<Value>>,
    ) -> Result<Commit> {
        self.with(|s| s.resolve(t, k, parents, row))
    }
    pub fn resolve_with_attachments(
        &self,
        t: u32,
        k: &str,
        parents: Vec<OpId>,
        row: Option<Vec<Value>>,
        refs: &[String],
    ) -> Result<Commit> {
        self.with(|s| s.resolve_with_attachments(t, k, parents, row, refs))
    }
    pub fn record_attachment_references(&self, t: u32, k: &str) -> Result<Vec<String>> {
        self.with(|s| s.record_attachment_references(t, k))
    }
    pub fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().iter().cloned().collect()
    }
    /// Closes without acknowledging or deleting durable pending bytes; sync must finish/cancel first.
    pub fn close(&self) -> Result<Status> {
        if self
            .syncing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::SyncInProgress);
        }
        let _guard = SyncGuard(&self.syncing);
        let mut s = self.store.try_lock().map_err(|_| Error::Busy)?;
        let status = s.as_mut().ok_or(Error::Closed)?.status()?;
        s.take();
        Ok(status)
    }
    pub async fn sync_once(
        &self,
        t: &impl Transport,
        c: &Cancellation,
        limits: SyncLimits,
    ) -> Result<SyncReport> {
        self.sync_once_with_attachment_limit(t, c, limits, attachments::DEFAULT_LIMIT)
            .await
    }
    pub async fn sync_once_with_attachment_limit(
        &self,
        t: &impl Transport,
        c: &Cancellation,
        limits: SyncLimits,
        attachment_limit: u64,
    ) -> Result<SyncReport> {
        attachments::check_limit(attachment_limit)?;
        if self
            .syncing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Error::SyncInProgress);
        }
        let _guard = SyncGuard(&self.syncing);
        let result = self.sync_inner(t, c, limits, attachment_limit).await;
        *self.last_error.lock().unwrap() = match &result {
            Err(e) => Some(e.to_string()),
            Ok(r) if !r.healthy() => Some("incomplete discovery or pending/blocked work".into()),
            _ => None,
        };
        let mut events = self.events.lock().unwrap();
        if events.len() == 64 {
            events.pop_front();
        }
        events.push_back(Event {
            success: result.as_ref().is_ok_and(SyncReport::healthy),
            detail: match &result {
                Ok(r) => format!(
                    "upload={} download={} healthy={}",
                    r.uploaded,
                    r.downloaded,
                    r.healthy()
                ),
                Err(e) => e.to_string(),
            },
        });
        result
    }
    async fn sync_inner(
        &self,
        t: &impl Transport,
        c: &Cancellation,
        l: SyncLimits,
        attachment_limit: u64,
    ) -> Result<SyncReport> {
        if l.uploads == 0
            || l.uploads > 1000
            || l.pages == 0
            || l.pages > 1000
            || l.page_size == 0
            || l.page_size > 1000
            || l.downloads == 0
            || l.downloads > 10000
            || l.retries > 8
        {
            return Err(Error::Invalid("sync limits"));
        }
        c.check().map_err(Error::Network)?;
        let (dataset, packets) = self.with(|s| {
            s.revisit_cancellable(|| c.check().map_err(Error::Network))?;
            Ok((s.identity.dataset.clone(), s.pending_envelopes(l.uploads)?))
        })?;
        let mut budget = l.retries;
        let mut uploaded = 0;
        for p in packets {
            if self.with(|s| s.stopped(&p.key.packet))? {
                continue;
            }
            c.check().map_err(Error::Network)?;
            let package = Package::decode_envelope(&p.bytes)?;
            for h in &package.attachment_references {
                let file = self.with(|s| s.open_attachment(h, c))?;
                t.upload_attachment(&dataset, h, file, attachment_limit, c)
                    .await
                    .map_err(Error::Network)?;
                c.check().map_err(Error::Network)?;
            }
            retry(c, &mut budget, || t.upload(p.clone(), c)).await?;
            c.check().map_err(Error::Network)?;
            // Readback verifies complete bytes and metadata, not an upload acknowledgement alone.
            let back = retry(c, &mut budget, || t.download(&p.key, c)).await?;
            if back != package || back.encode_envelope()? != p.bytes {
                return Err(Error::Network(NetworkError::Corrupt));
            }
            c.check().map_err(Error::Network)?;
            self.with(|s| s.acknowledge(p.key.packet.seq, p.key.packet.hash))?;
            uploaded += 1;
        }
        let mut cursor = self.with(Store::scan_cursor)?;
        let known = self.with(Store::known_objects)?;
        let mut downloaded = 0;
        let mut complete_scan = false;
        let mut complete_pages = !self.with(Store::scan_degraded)?;
        for _ in 0..l.pages {
            c.check().map_err(Error::Network)?;
            let slots =
                self.with(|s| Ok(10000usize.saturating_sub(s.status()?.outstanding_objects)))?;
            if slots == 0 {
                break;
            }
            let page = retry(c, &mut budget, || {
                t.list(&dataset, cursor, l.page_size.min(slots), c)
            })
            .await?;
            complete_pages &= page.complete;
            if !page.complete {
                self.with(|s| s.save_scan_degraded(true))?;
            }
            if page.keys.len() > l.page_size {
                return Err(Error::Network(NetworkError::Limit));
            }
            for k in page.keys {
                if !known.contains(&k) && !self.with(|s| s.stopped(&k.packet))? {
                    self.with(|s| s.enqueue(&k))?;
                }
            }
            match page.next {
                None => {
                    self.with(|s| {
                        s.save_scan_cursor(0)?;
                        s.save_scan_degraded(false)
                    })?;
                    complete_scan = complete_pages;
                    break;
                }
                Some(n) if n > cursor => {
                    cursor = n;
                    self.with(|s| s.save_scan_cursor(cursor))?;
                }
                _ => return Err(Error::Network(NetworkError::Corrupt)),
            }
        }
        // Debt is durable and fair independently of page boundaries and failed downloads.
        for (id, k) in self.with(|s| s.download_debt(l.downloads))? {
            c.check().map_err(Error::Network)?;
            if self.with(|s| s.stopped(&k.packet))? {
                self.with(|s| s.fail_debt(id, true, "stream fork"))?;
                continue;
            }
            let result = match retry(c, &mut budget, || t.fetch(&k, c)).await {
                Ok(p) => {
                    self.with(|s| s.receive_download(&k, p, || c.check().map_err(Error::Network)))
                }
                Err(e) => {
                    self.with(|s| s.retain_attempt(&k, &[], false, true, &e.to_string()))?;
                    Err(e)
                }
            };
            match result {
                Ok(state) => {
                    if matches!(state, InboxState::Failed | InboxState::Blocked) {
                        self.with(|s| s.fail_debt(id, true, "permanent blocked receipt"))?;
                    } else {
                        self.with(|s| s.finish_debt(id))?;
                    }
                    downloaded += 1;
                }
                Err(e) => {
                    let blocked = matches!(e, Error::StreamFork | Error::Schema(_))
                        || self.with(|s| {
                            Ok(s.status()?.inbox.iter().any(|i| {
                                i.key == k.packet
                                    && matches!(i.state, InboxState::Failed | InboxState::Blocked)
                            }))
                        })?;
                    self.with(|s| s.fail_debt(id, blocked, &e.to_string()))?;
                    if matches!(
                        e,
                        Error::Network(NetworkError::Auth | NetworkError::Cancelled)
                            | Error::Io(_)
                            | Error::Sqlite(_)
                            | Error::Poisoned
                    ) {
                        return Err(e);
                    }
                }
            }
        }
        c.check().map_err(Error::Network)?;
        let status = self.with(Store::status)?;
        Ok(SyncReport {
            uploaded,
            downloaded,
            complete_scan,
            status,
        })
    }
}
async fn retry<T, F, Fut>(c: &Cancellation, budget: &mut usize, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, NetworkError>>,
{
    loop {
        c.check().map_err(Error::Network)?;
        match c.during(f()).await {
            Ok(v) => {
                c.check().map_err(Error::Network)?;
                return Ok(v);
            }
            Err(e)
                if matches!(
                    e,
                    NetworkError::Transient | NetworkError::Timeout | NetworkError::RateLimit
                ) && *budget > 0 =>
            {
                *budget -= 1
            }
            Err(e) => return Err(Error::Network(e)),
        }
    }
}
