#[cfg(feature = "drive-experimental")]
mod drive;
#[cfg(feature = "drive-experimental")]
pub use drive::{DriveToken, GoogleDriveTransport};
// Immutable, full-envelope objects over OpenDAL 0.59.4. No remote GC or shared manifest.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::StreamExt;
use lfs_protocol::{hash, hash_hex};
use lfs_sqlite::sync::{NetworkFuture, Page};
use lfs_sqlite::{
    Cancellation, Identity, MAX_ENVELOPE, NetworkError, ObjectKey, OutgoingEnvelope, Package,
    PacketKey, Transport,
};
#[cfg(any(feature = "fs", feature = "webdav", feature = "s3"))]
use opendal::services;
use opendal::{ErrorKind, Operator};
use std::{
    collections::{BTreeSet, VecDeque},
    path::{Component, Path, PathBuf},
    time::Duration,
};
use tokio::sync::Mutex;
type Result<T> = std::result::Result<T, NetworkError>;

#[derive(Clone, Copy, Debug)]
pub struct Options {
    pub timeout: Duration,
    /// Explicit test-only escape hatch; only http://127.0.0.1 or http://[::1] allowed.
    pub allow_loopback_http: bool,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(15),
            allow_loopback_http: false,
        }
    }
}
/// No Debug implementation: host-supplied credentials must not appear in diagnostics.
#[cfg(feature = "webdav")]
pub enum WebdavCredentials {
    Basic { username: String, password: String },
    Bearer(String),
}
#[cfg(feature = "s3")]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}
#[derive(Clone, Copy, Debug)]
pub struct Capabilities {
    pub read: bool,
    pub write: bool,
    pub stat: bool,
    pub list: bool,
    pub native_recursive_list: bool,
}
struct Scan {
    dataset: String,
    position: usize,
    current: Option<opendal::Lister>,
    dirs: VecDeque<String>,
    visited: BTreeSet<String>,
}
pub struct OpenDalTransport {
    op: Operator,
    options: Options,
    fs_root: Option<PathBuf>,
    capabilities: Capabilities,
    scan: Mutex<Option<Scan>>,
    attachment_streaming: bool,
    webdav_bootstrap: Option<(Operator, String)>,
}
fn classify(e: opendal::Error) -> NetworkError {
    if e.message() == "lfs response limit" {
        return NetworkError::Limit;
    }
    match e.kind() {
        ErrorKind::NotFound => NetworkError::Missing,
        ErrorKind::PermissionDenied => NetworkError::Auth,
        ErrorKind::RateLimited => NetworkError::RateLimit,
        ErrorKind::Unsupported => NetworkError::Unsupported,
        _ if e.is_temporary() => NetworkError::Transient,
        _ => NetworkError::Corrupt,
    }
}
#[cfg(any(feature = "webdav", feature = "s3"))]
fn endpoint(value: &str, options: Options) -> Result<()> {
    let u = url::Url::parse(value).map_err(|_| NetworkError::Unsupported)?;
    if !u.username().is_empty()
        || u.password().is_some()
        || u.query().is_some()
        || u.fragment().is_some()
    {
        return Err(NetworkError::Unsupported);
    }
    if u.scheme() == "https" && u.host_str().is_some() {
        return Ok(());
    }
    if options.allow_loopback_http
        && u.scheme() == "http"
        && matches!(u.host_str(), Some("127.0.0.1" | "[::1]"))
    {
        return Ok(());
    }
    Err(NetworkError::Unsupported)
}
fn segment(s: &str) -> String {
    URL_SAFE_NO_PAD.encode(s.as_bytes())
}
fn unsegment(s: &str) -> Result<String> {
    if s.is_empty() || s.len() > 171 {
        return Err(NetworkError::Corrupt);
    }
    let text = String::from_utf8(
        URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|_| NetworkError::Corrupt)?,
    )
    .map_err(|_| NetworkError::Corrupt)?;
    if segment(&text) != s {
        return Err(NetworkError::Corrupt);
    }
    Ok(text)
}
fn digest(s: &str) -> Result<[u8; 32]> {
    if s.len() != 64
        || !s
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return Err(NetworkError::Corrupt);
    }
    let mut out = [0; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).map_err(|_| NetworkError::Corrupt)?;
    }
    Ok(out)
}
pub fn dataset_root(dataset: &str) -> Result<String> {
    if dataset.is_empty() || dataset.len() > 128 {
        return Err(NetworkError::Corrupt);
    }
    Ok(format!("lfs1/{}/", segment(dataset)))
}
pub fn object_path(k: &ObjectKey) -> Result<String> {
    k.packet
        .identity
        .validate()
        .map_err(|_| NetworkError::Corrupt)?;
    if k.packet.seq == 0 || k.packet.seq >= i64::MAX as u64 {
        return Err(NetworkError::Corrupt);
    }
    Ok(format!(
        "{}{}/{}/{:020}-{}-{}.lfs",
        dataset_root(&k.packet.identity.dataset)?,
        segment(&k.packet.identity.device),
        segment(&k.packet.identity.incarnation),
        k.packet.seq,
        hash_hex(&k.packet.hash),
        hash_hex(&k.envelope_hash.ok_or(NetworkError::Corrupt)?)
    ))
}
fn parse_path(path: &str, dataset: &str) -> Result<ObjectKey> {
    let root = dataset_root(dataset)?;
    let tail = path.strip_prefix(&root).ok_or(NetworkError::Corrupt)?;
    let parts: Vec<_> = tail.split('/').collect();
    if parts.len() != 3 {
        return Err(NetworkError::Corrupt);
    }
    let name = parts[2].strip_suffix(".lfs").ok_or(NetworkError::Corrupt)?;
    let fields: Vec<_> = name.split('-').collect();
    if fields.len() != 3 || fields[0].len() != 20 || !fields[0].bytes().all(|c| c.is_ascii_digit())
    {
        return Err(NetworkError::Corrupt);
    }
    let k = ObjectKey {
        packet: PacketKey {
            identity: Identity {
                dataset: dataset.to_owned(),
                device: unsegment(parts[0])?,
                incarnation: unsegment(parts[1])?,
            },
            seq: fields[0].parse().map_err(|_| NetworkError::Corrupt)?,
            hash: digest(fields[1])?,
        },
        envelope_hash: Some(digest(fields[2])?),
    };
    if object_path(&k)? != path {
        return Err(NetworkError::Corrupt);
    }
    Ok(k)
}
impl OpenDalTransport {
    fn build(op: Operator, options: Options, fs_root: Option<PathBuf>) -> Result<Self> {
        if options.timeout.is_zero() || options.timeout > Duration::from_secs(120) {
            return Err(NetworkError::Limit);
        }
        let c = op.info().capability();
        let capabilities = Capabilities {
            read: c.read,
            write: c.write,
            stat: c.stat,
            list: c.list,
            native_recursive_list: c.list_with_recursive,
        };
        if !(c.read && c.write && c.stat && c.list) {
            return Err(NetworkError::Unsupported);
        }
        let attachment_streaming = c.write_can_multi && matches!(op.info().scheme(), "fs" | "s3");
        Ok(Self {
            attachment_streaming,
            webdav_bootstrap: None,
            op,
            options,
            fs_root,
            capabilities,
            scan: Mutex::new(None),
        })
    }
    /// Consume a host operator without rebuilding its layers or operation context.
    /// Fs requires explicit root binding via `from_fs_operator`.
    pub fn from_operator(op: Operator, options: Options) -> Result<Self> {
        if op.info().scheme() == "fs" {
            return Err(NetworkError::Unsupported);
        }
        Self::build(op, options, None)
    }
    /// Bind an existing Fs operator to its actual, existing, symlink-free root.
    /// Host layers and context are trusted and retained unchanged.
    pub fn from_fs_operator(
        root: impl AsRef<Path>,
        op: Operator,
        options: Options,
    ) -> Result<Self> {
        if op.info().scheme() != "fs" {
            return Err(NetworkError::Unsupported);
        }
        let root = native_fs_root(root.as_ref())?;
        let info = op.info();
        let operator_root = native_fs_root(Path::new(&info.root()))?;
        if root != operator_root {
            return Err(NetworkError::Unsupported);
        }
        Self::build(op, options, Some(root))
    }
    #[cfg(any(feature = "webdav", feature = "s3"))]
    fn strict_http(op: Operator, options: Options) -> Result<Self> {
        if options.timeout.is_zero() || options.timeout > Duration::from_secs(120) {
            return Err(NetworkError::Limit);
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| NetworkError::Unsupported)?;
        let transport = opendal::HttpTransporter::new(BoundedHttp {
            inner: opendal_http_transport_reqwest::ReqwestTransport::new(client),
            options,
        });
        Self::build(
            op.with_context(opendal::OperationContext::new().with_http_transport(transport)),
            options,
            None,
        )
    }
    /// Root must already exist and be exclusively managed by the host. Reject symlinks in all ancestors.
    #[cfg(feature = "fs")]
    pub fn fs(root: impl AsRef<Path>, options: Options) -> Result<Self> {
        let root = root.as_ref();
        if !root.is_absolute() {
            return Err(NetworkError::Unsupported);
        }
        check_components(root, false)?;
        if !root.is_dir() {
            return Err(NetworkError::Unsupported);
        }
        let op = Operator::new(
            services::Fs::default().root(root.to_str().ok_or(NetworkError::Unsupported)?),
        )
        .map_err(classify)?;
        Self::build(op, options, Some(root.to_owned()))
    }
    #[cfg(feature = "webdav")]
    pub fn webdav(
        endpoint_url: &str,
        credentials: WebdavCredentials,
        options: Options,
    ) -> Result<Self> {
        Self::webdav_with_directory(endpoint_url, "", credentials, options)
    }
    /// Scope WebDAV objects to a relative directory, creating missing parents on sync.
    #[cfg(feature = "webdav")]
    pub fn webdav_with_directory(
        endpoint_url: &str,
        directory: &str,
        credentials: WebdavCredentials,
        options: Options,
    ) -> Result<Self> {
        endpoint(endpoint_url, options)?;
        let directory = directory.trim_matches('/');
        if directory.len() > 1024
            || directory.contains(['\\', '\0'])
            || (!directory.is_empty()
                && directory
                    .split('/')
                    .any(|p| p.is_empty() || p == "." || p == ".."))
        {
            return Err(NetworkError::Unsupported);
        }
        let make = |root: &str| {
            let mut b = services::Webdav::default()
                .endpoint(endpoint_url.trim_end_matches('/'))
                .root(root);
            b = match &credentials {
                WebdavCredentials::Basic { username, password } => {
                    if username.is_empty() || password.is_empty() {
                        return Err(NetworkError::Auth);
                    }
                    b.username(username).password(password)
                }
                WebdavCredentials::Bearer(token) => {
                    if token.is_empty() {
                        return Err(NetworkError::Auth);
                    }
                    b.token(token)
                }
            };
            Self::strict_http(Operator::new(b).map_err(classify)?, options)
        };
        let mut transport = make(directory)?;
        if !directory.is_empty() {
            transport.webdav_bootstrap = Some((make("/")?.op, directory.to_owned()));
        }
        Ok(transport)
    }
    #[cfg(feature = "s3")]
    pub fn s3(
        endpoint_url: &str,
        region: &str,
        bucket: &str,
        credentials: S3Credentials,
        options: Options,
    ) -> Result<Self> {
        endpoint(endpoint_url, options)?;
        if region.is_empty()
            || bucket.is_empty()
            || credentials.access_key_id.is_empty()
            || credentials.secret_access_key.is_empty()
        {
            return Err(NetworkError::Auth);
        }
        let mut b = services::S3::default()
            .endpoint(endpoint_url)
            .region(region)
            .bucket(bucket)
            .access_key_id(&credentials.access_key_id)
            .secret_access_key(&credentials.secret_access_key)
            .disable_config_load()
            .disable_ec2_metadata();
        if let Some(token) = credentials.session_token {
            if token.is_empty() {
                return Err(NetworkError::Auth);
            }
            b = b.session_token(&token);
        }
        Self::strict_http(Operator::new(b).map_err(classify)?, options)
    }
    pub fn capabilities(&self) -> Capabilities {
        self.capabilities
    }
    fn safe_path(&self, path: &str) -> Result<()> {
        if path.starts_with('/')
            || path.contains('\\')
            || path.split('/').any(|s| s == "." || s == "..")
        {
            return Err(NetworkError::Corrupt);
        }
        if let Some(root) = &self.fs_root {
            check_components(&root.join(path), true)?;
        }
        Ok(())
    }
    async fn bounded_read(&self, path: &str) -> Result<Vec<u8>> {
        self.safe_path(path)?;
        let length = self.op.stat(path).await.map_err(classify)?.content_length();
        if length > MAX_ENVELOPE as u64 {
            return Err(NetworkError::Limit);
        }
        let reader = self.op.reader(path).await.map_err(classify)?;
        let mut stream = reader.into_stream(0..length).await.map_err(classify)?;
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(classify)?;
            if bytes.len().saturating_add(chunk.len()) > MAX_ENVELOPE {
                return Err(NetworkError::Limit);
            }
            bytes.extend_from_slice(&chunk.to_bytes());
        }
        if self.op.stat(path).await.map_err(classify)?.content_length() != length
            || bytes.len() as u64 != length
        {
            return Err(NetworkError::Corrupt);
        }
        Ok(bytes)
    }
    fn durable_fs(&self, path: &str) -> Result<()> {
        if let Some(root) = &self.fs_root {
            self.safe_path(path)?;
            let mut current = root.join(path);
            loop {
                std::fs::File::open(&current)
                    .and_then(|f| f.sync_all())
                    .map_err(|_| NetworkError::Transient)?;
                if current == *root {
                    break;
                }
                if !current.pop() || !current.starts_with(root) {
                    return Err(NetworkError::Corrupt);
                }
            }
        }
        Ok(())
    }
    async fn prepare_directory(&self, directory: &str) -> Result<()> {
        if self.op.info().scheme() != "webdav" {
            return Ok(());
        }
        let (op, directory) = match &self.webdav_bootstrap {
            Some((op, base)) => (op, format!("{base}/{directory}")),
            None => (&self.op, directory.to_owned()),
        };
        let mut prefix = String::new();
        for part in directory.trim_end_matches('/').split('/') {
            prefix.push_str(part);
            prefix.push('/');
            op.create_dir(&prefix).await.map_err(classify)?;
        }
        Ok(())
    }
    async fn publish(&self, p: OutgoingEnvelope) -> Result<()> {
        p.validate().map_err(|_| NetworkError::Corrupt)?;
        let key = p.key;
        let path = object_path(&key)?;
        let bytes = p.bytes;
        self.prepare_directory(path.rsplit_once('/').ok_or(NetworkError::Corrupt)?.0)
            .await?;
        match self.bounded_read(&path).await {
            Ok(old) if old == bytes => return self.durable_fs(&path),
            Ok(old) => {
                // A valid but mismatched envelope is evidence, never overwrite it.
                if Package::decode_envelope(&old).is_ok() {
                    return Err(NetworkError::Corrupt);
                }
            }
            Err(NetworkError::Missing) => {}
            Err(e) => return Err(e),
        }
        self.safe_path(&path)?;
        // No rename/conditional-create assumption; only this exact immutable hash path is written.
        let result = self.op.write(&path, bytes.clone()).await.map_err(classify);
        let back = self.bounded_read(&path).await;
        match back {
            Ok(back)
                if back.len() == bytes.len()
                    && hash(&back) == key.envelope_hash.unwrap()
                    && back == bytes =>
            {
                self.durable_fs(&path)
            }
            _ => match result {
                Err(e) => Err(e),
                Ok(_) => Err(NetworkError::Corrupt),
            },
        }
    }
    async fn page(&self, dataset: &str, cursor: usize, limit: usize) -> Result<Page> {
        if limit == 0 || limit > 1000 || cursor > 1_000_000 {
            return Err(NetworkError::Limit);
        }
        let root = dataset_root(dataset)?;
        self.safe_path(&root)?;
        if cursor == 0 {
            self.prepare_directory(&root).await?;
        }
        let mut guard = self.scan.lock().await;
        if guard
            .as_ref()
            .is_none_or(|s| s.dataset != dataset || s.position != cursor)
            || cursor == 0
        {
            *guard = Some(Scan {
                dataset: dataset.to_owned(),
                position: 0,
                current: None,
                dirs: VecDeque::from([root.clone()]),
                visited: BTreeSet::from([root.clone()]),
            });
        }
        let scan = guard.as_mut().unwrap();
        let mut keys = Vec::new();
        loop {
            if keys.len() == limit {
                return Ok(Page {
                    keys,
                    next: Some(scan.position),
                    complete: true,
                });
            }
            if scan.current.is_none() {
                let Some(dir) = scan.dirs.front().cloned() else {
                    *guard = None;
                    return Ok(Page {
                        keys,
                        next: None,
                        complete: true,
                    });
                };
                self.safe_path(&dir)?;
                if let Some(root) = &self.fs_root {
                    match std::fs::read_dir(root.join(&dir)) {
                        Ok(entries) => {
                            for (i, entry) in entries.enumerate() {
                                if i >= 1_000_000 {
                                    return Err(NetworkError::Limit);
                                }
                                let kind = entry
                                    .map_err(|_| NetworkError::Corrupt)?
                                    .file_type()
                                    .map_err(|_| NetworkError::Corrupt)?;
                                if !kind.is_file() && !kind.is_dir() {
                                    return Err(NetworkError::Corrupt);
                                }
                            }
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                        Err(_) => return Err(NetworkError::Corrupt),
                    }
                }
                match self
                    .op
                    .lister_with(&dir)
                    .recursive(self.capabilities.native_recursive_list)
                    .await
                {
                    Ok(lister) => {
                        scan.dirs.pop_front();
                        scan.current = Some(lister);
                    }
                    Err(e) if e.kind() == ErrorKind::NotFound => {
                        scan.dirs.pop_front();
                        continue;
                    }
                    Err(e) => return Err(classify(e)),
                }
            }
            let Some(entry) = scan.current.as_mut().unwrap().next().await else {
                scan.current = None;
                continue;
            };
            let entry = entry.map_err(classify)?;
            let path = entry.path();
            self.safe_path(path)?;
            if !path.starts_with(&root) {
                return Err(NetworkError::Corrupt);
            }
            if entry.metadata().is_dir() {
                let rel = path.strip_prefix(&root).unwrap().trim_end_matches('/');
                if rel.is_empty() {
                    continue;
                }
                let parts: Vec<_> = rel.split('/').collect();
                if parts.len() > 2 {
                    return Err(NetworkError::Corrupt);
                }
                for part in parts {
                    unsegment(part)?;
                }
                if !self.capabilities.native_recursive_list && scan.visited.insert(path.to_owned())
                {
                    if scan.visited.len() > 4096 {
                        return Err(NetworkError::Limit);
                    }
                    scan.dirs.push_back(path.to_owned());
                }
            } else if entry.metadata().is_file() {
                let key = parse_path(path, dataset)?;
                scan.position += 1;
                if scan.position > 1_000_000 {
                    return Err(NetworkError::Limit);
                }
                if scan.position > cursor {
                    keys.push(key);
                }
            } else {
                return Err(NetworkError::Corrupt);
            }
        }
    }
}
fn native_fs_root(root: &Path) -> Result<PathBuf> {
    if !root.is_absolute() {
        return Err(NetworkError::Unsupported);
    }
    // Path::components normalizes interior dots; reject them before normalization.
    if root
        .as_os_str()
        .as_encoded_bytes()
        .split(|b| *b == b'/')
        .any(|s| s == b"." || s == b"..")
    {
        return Err(NetworkError::Corrupt);
    }
    check_components(root, false)?;
    if !root.is_dir() {
        return Err(NetworkError::Unsupported);
    }
    std::fs::canonicalize(root).map_err(|_| NetworkError::Corrupt)
}
fn check_components(path: &Path, allow_missing: bool) -> Result<()> {
    let mut p = PathBuf::new();
    for c in path.components() {
        if matches!(c, Component::ParentDir | Component::CurDir) {
            return Err(NetworkError::Corrupt);
        }
        p.push(c);
        match std::fs::symlink_metadata(&p) {
            Ok(m) if m.file_type().is_symlink() => return Err(NetworkError::Corrupt),
            Ok(_) => {}
            Err(e) if allow_missing && e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(NetworkError::Corrupt),
        }
    }
    Ok(())
}
impl Transport for OpenDalTransport {
    fn upload_attachment<'a>(
        &'a self,
        dataset: &'a str,
        h: &'a str,
        mut file: std::fs::File,
        limit: u64,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, ()> {
        Box::pin(async move {
            // Only incrementally writing services are admitted; WebDAV OneShotWriter is excluded.
            if !self.attachment_streaming {
                return Err(NetworkError::Unsupported);
            }
            let path = attachment_path(dataset, h)?;
            self.safe_path(&path)?;
            c.during(async {
                tokio::time::timeout(self.options.timeout, async {
                    use std::io::Read;
                    let mut writer = self
                        .op
                        .writer_with(&path)
                        .chunk(5 * 1024 * 1024)
                        .concurrent(1)
                        .await
                        .map_err(classify)?;
                    let result = async {
                        let mut total = 0u64;
                        let mut buffer = vec![0; lfs_sqlite::attachments::CHUNK];
                        loop {
                            c.check()?;
                            let n = file.read(&mut buffer).map_err(|_| NetworkError::Corrupt)?;
                            if n == 0 {
                                break;
                            }
                            total += n as u64;
                            if total > limit {
                                return Err(NetworkError::Limit);
                            }
                            writer.write(buffer[..n].to_vec()).await.map_err(classify)?;
                        }
                        writer.close().await.map_err(classify)?;
                        self.verify_attachment(&path, h, None, limit, c).await?;
                        self.durable_fs(&path)
                    }
                    .await;
                    if result.is_err() {
                        let _ = writer.abort().await;
                    }
                    result
                })
                .await
                .map_err(|_| NetworkError::Timeout)?
            })
            .await
        })
    }
    fn download_attachment<'a>(
        &'a self,
        dataset: &'a str,
        h: &'a str,
        file: std::fs::File,
        limit: u64,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, ()> {
        Box::pin(async move {
            if !self.attachment_streaming {
                return Err(NetworkError::Unsupported);
            }
            let path = attachment_path(dataset, h)?;
            c.during(async {
                tokio::time::timeout(
                    self.options.timeout,
                    self.verify_attachment(&path, h, Some(file), limit, c),
                )
                .await
                .map_err(|_| NetworkError::Timeout)?
            })
            .await
        })
    }

    fn fetch<'a>(
        &'a self,
        k: &'a ObjectKey,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, lfs_sqlite::sync::Download> {
        Box::pin(async move {
            c.during(async {
                tokio::time::timeout(self.options.timeout, async {
                    self.bounded_read(&object_path(k)?)
                        .await
                        .map(lfs_sqlite::sync::Download::Wire)
                })
                .await
                .map_err(|_| NetworkError::Timeout)?
            })
            .await
        })
    }

    fn upload<'a>(&'a self, p: OutgoingEnvelope, c: &'a Cancellation) -> NetworkFuture<'a, ()> {
        Box::pin(async move {
            c.during(async {
                tokio::time::timeout(self.options.timeout, self.publish(p))
                    .await
                    .map_err(|_| NetworkError::Timeout)?
            })
            .await
        })
    }
    fn list<'a>(
        &'a self,
        d: &'a str,
        cursor: usize,
        limit: usize,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, Page> {
        Box::pin(async move {
            c.during(async {
                tokio::time::timeout(self.options.timeout, self.page(d, cursor, limit))
                    .await
                    .map_err(|_| NetworkError::Timeout)?
            })
            .await
        })
    }
    fn download<'a>(&'a self, k: &'a ObjectKey, c: &'a Cancellation) -> NetworkFuture<'a, Package> {
        Box::pin(async move {
            c.during(async {
                tokio::time::timeout(self.options.timeout, async {
                    let bytes = self.bounded_read(&object_path(k)?).await?;
                    if Some(hash(&bytes)) != k.envelope_hash {
                        return Err(NetworkError::Corrupt);
                    }
                    let p = Package::decode_envelope(&bytes).map_err(|_| NetworkError::Corrupt)?;
                    if p.key != k.packet {
                        return Err(NetworkError::Corrupt);
                    }
                    Ok(p)
                })
                .await
                .map_err(|_| NetworkError::Timeout)?
            })
            .await
        })
    }
}

// Per-operator policy: bounded HTTP bodies, no redirects/proxies, explicit status classification.
#[cfg(any(feature = "webdav", feature = "s3"))]
struct BoundedHttp {
    inner: opendal_http_transport_reqwest::ReqwestTransport,
    options: Options,
}
#[cfg(any(feature = "webdav", feature = "s3"))]
impl opendal::HttpTransport for BoundedHttp {
    async fn fetch(
        &self,
        req: http::Request<opendal::Buffer>,
    ) -> opendal::Result<http::Response<opendal::HttpBody>> {
        let mut request_url = url::Url::parse(&req.uri().to_string())
            .map_err(|_| opendal::Error::new(ErrorKind::Unsupported, "unsafe endpoint"))?;
        request_url.set_query(None);
        endpoint(request_url.as_str(), self.options)
            .map_err(|_| opendal::Error::new(ErrorKind::Unsupported, "unsafe endpoint"))?;
        let head = req.method() == http::Method::HEAD;
        let cap = if req.method().as_str() == "PROPFIND" {
            4 * 1024 * 1024
        } else {
            MAX_ENVELOPE
        };
        let response = self.inner.fetch(req).await?;
        match response.status().as_u16() {
            401 | 403 => {
                return Err(opendal::Error::new(
                    ErrorKind::PermissionDenied,
                    "authentication denied",
                ));
            }
            429 => {
                return Err(
                    opendal::Error::new(ErrorKind::RateLimited, "rate limited").set_temporary()
                );
            }
            300..=399 => {
                return Err(opendal::Error::new(
                    ErrorKind::Unsupported,
                    "redirect refused",
                ));
            }
            _ => {}
        }
        if !head
            && response
                .headers()
                .get(http::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .is_some_and(|n| n > cap as u64)
        {
            return Err(opendal::Error::new(
                ErrorKind::Unexpected,
                "lfs response limit",
            ));
        }
        Ok(response.map(|body| {
            body.map_inner(|stream| {
                let mut total = 0usize;
                Box::new(stream.map(move |chunk| {
                    let chunk = chunk?;
                    total = total.saturating_add(chunk.len());
                    if total > cap {
                        Err(opendal::Error::new(
                            ErrorKind::Unexpected,
                            "lfs response limit",
                        ))
                    } else {
                        Ok(chunk)
                    }
                }))
            })
        }))
    }
}

fn attachment_path(dataset: &str, h: &str) -> Result<String> {
    lfs_sqlite::attachments::digest(h).map_err(|_| NetworkError::Corrupt)?;
    // Separate namespace: record enumeration never interprets file objects as envelopes.
    Ok(format!("lfs-attachments/{}/{}", segment(dataset), h))
}
impl OpenDalTransport {
    pub fn attachment_streaming_supported(&self) -> bool {
        self.attachment_streaming
    }
    async fn verify_attachment(
        &self,
        path: &str,
        h: &str,
        mut output: Option<std::fs::File>,
        limit: u64,
        c: &Cancellation,
    ) -> Result<()> {
        use sha2::{Digest, Sha256};
        use std::io::Write;
        self.safe_path(path)?;
        let length = self.op.stat(path).await.map_err(classify)?.content_length();
        if length > limit {
            return Err(NetworkError::Limit);
        }
        let reader = self
            .op
            .reader_with(path)
            .chunk(lfs_sqlite::attachments::CHUNK)
            .await
            .map_err(classify)?;
        let mut stream = reader.into_stream(0..length).await.map_err(classify)?;
        let mut digest = Sha256::new();
        let mut total = 0u64;
        while let Some(chunk) = stream.next().await {
            c.check()?;
            let chunk = chunk.map_err(classify)?.to_bytes();
            total += chunk.len() as u64;
            if total > limit {
                return Err(NetworkError::Limit);
            }
            digest.update(&chunk);
            if let Some(f) = &mut output {
                f.write_all(&chunk).map_err(|_| NetworkError::Transient)?;
            }
        }
        if total != length
            || hash_hex(&digest.finalize().into()) != h
            || self.op.stat(path).await.map_err(classify)?.content_length() != length
        {
            return Err(NetworkError::Corrupt);
        }
        c.check()?;
        Ok(())
    }
}
