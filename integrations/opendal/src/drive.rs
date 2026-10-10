//! Limited ID-aware Drive record REST supplement over OpenDAL's public HTTP transport.
use crate::{Options, Result, object_path, parse_path, segment, unsegment};
use lfs_protocol::hash;
use lfs_sqlite::sync::{Download, NetworkFuture, Page};
use lfs_sqlite::{
    Cancellation, MAX_ENVELOPE, NetworkError, ObjectKey, OutgoingEnvelope, Package, Transport,
};
use opendal::raw::oio::ReadStream;
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    time::Duration,
};
use tokio::sync::Mutex;
const FOLDER: &str = "application/vnd.google-apps.folder";
const FILE: &str = "application/octet-stream";
const FIELDS: &str = "id,name,parents,mimeType,trashed,size,driveId";
const JSON_CAP: usize = 1024 * 1024;
const MAX_IDS: usize = 10000;
const MAX_DIRS: usize = 1024;
const MAX_PAGES: usize = 1024;
const MAX_CANDIDATES: usize = 128;

/// Host-owned token snapshot. No Debug, serialization, implicit discovery or refresh.
pub struct DriveToken(http::HeaderValue);
impl DriveToken {
    pub fn new(token: String) -> Result<Self> {
        if token.is_empty()
            || token.len() > 8192
            || !token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-._~+/=".contains(&b))
        {
            return Err(NetworkError::Auth);
        }
        let mut value = http::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| NetworkError::Auth)?;
        value.set_sensitive(true);
        Ok(Self(value))
    }
}
#[derive(Clone, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct Meta {
    id: String,
    name: String,
    parents: Option<Vec<String>>,
    mime_type: String,
    trashed: bool,
    size: Option<String>,
    drive_id: Option<String>,
}
impl Meta {
    fn valid(&self) -> Result<()> {
        id(&self.id)?;
        if self.drive_id.is_some() {
            return Err(NetworkError::Unsupported);
        }
        if self.name.is_empty() || self.name.len() > 1024 || self.mime_type.len() > 128 {
            return Err(NetworkError::Corrupt);
        }
        if let Some(parents) = &self.parents {
            if parents.len() > 1 {
                return Err(NetworkError::Corrupt);
            }
            for p in parents {
                id(p)?;
            }
        }
        Ok(())
    }
    fn child(&self, parent: &str) -> bool {
        !self.trashed && self.parents.as_deref() == Some(&[parent.to_owned()])
    }
}
fn id(s: &str) -> Result<()> {
    if s.is_empty()
        || s.len() > 256
        || !s
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(NetworkError::Corrupt);
    }
    Ok(())
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Listing {
    files: Vec<Meta>,
    next_page_token: Option<String>,
    incomplete_search: bool,
}
#[derive(Default)]
struct Budget {
    pages: usize,
    entries: usize,
    dirs: usize,
}
struct Scan {
    dataset: String,
    expected: usize,
    at: usize,
    keys: Vec<ObjectKey>,
}
/// Records only. Construction is not a root authority/capability check.
/// Host must replace this transport after refreshing its token. No internal retry loop.
pub struct GoogleDriveTransport {
    root: String,
    token: DriveToken,
    origin: String,
    http: opendal::HttpTransporter,
    options: Options,
    scan: Mutex<Option<Scan>>,
    candidates: Mutex<BTreeMap<ObjectKey, (Vec<Meta>, usize)>>,
}
impl GoogleDriveTransport {
    /// Validate non-secret configuration without network or token discovery.
    pub fn validate_configuration(
        root_folder_id: &str,
        owned_loopback_origin: Option<&str>,
    ) -> Result<()> {
        id(root_folder_id)?;
        if root_folder_id == "root" {
            return Err(NetworkError::Unsupported);
        }
        if let Some(origin) = owned_loopback_origin {
            let u = url::Url::parse(origin).map_err(|_| NetworkError::Unsupported)?;
            if u.scheme() != "http"
                || u.host_str() != Some("127.0.0.1")
                || u.port().is_none()
                || u.path() != "/"
                || !u.username().is_empty()
                || u.password().is_some()
                || u.query().is_some()
                || u.fragment().is_some()
            {
                return Err(NetworkError::Unsupported);
            }
        }
        Ok(())
    }
    pub fn new(root_folder_id: &str, token: DriveToken, options: Options) -> Result<Self> {
        Self::build(root_folder_id, token, "https://www.googleapis.com", options)
    }
    /// Explicit owned MODEL_HTTP only; accepts an origin on 127.0.0.1 with a port.
    pub fn owned_loopback(
        root_folder_id: &str,
        token: DriveToken,
        origin: &str,
        options: Options,
    ) -> Result<Self> {
        Self::validate_configuration(root_folder_id, Some(origin))?;
        Self::build(root_folder_id, token, origin.trim_end_matches('/'), options)
    }
    fn build(root: &str, token: DriveToken, origin: &str, options: Options) -> Result<Self> {
        Self::validate_configuration(root, None)?;
        if options.timeout.is_zero() || options.timeout > Duration::from_secs(120) {
            return Err(NetworkError::Limit);
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| NetworkError::Unsupported)?;
        Ok(Self {
            root: root.into(),
            token,
            origin: origin.into(),
            http: opendal::HttpTransporter::new(
                opendal_http_transport_reqwest::ReqwestTransport::new(client),
            ),
            options,
            scan: Mutex::new(None),
            candidates: Mutex::new(BTreeMap::new()),
        })
    }
    async fn bounded<T>(&self, c: &Cancellation, f: impl Future<Output = Result<T>>) -> Result<T> {
        c.during(async {
            tokio::time::timeout(self.options.timeout, f)
                .await
                .map_err(|_| NetworkError::Timeout)?
        })
        .await
    }
    async fn request(
        &self,
        method: &str,
        route: &str,
        query: &[(&str, &str)],
        content_type: &str,
        bytes: Vec<u8>,
        cap: usize,
    ) -> Result<Vec<u8>> {
        let mut url = url::Url::parse(&format!("{}{route}", self.origin))
            .map_err(|_| NetworkError::Unsupported)?;
        url.query_pairs_mut().extend_pairs(query.iter().copied());
        let req = http::Request::builder()
            .method(method)
            .uri(url.as_str())
            .header(http::header::AUTHORIZATION, self.token.0.clone())
            .header(http::header::CONTENT_TYPE, content_type)
            .body(opendal::Buffer::from(bytes))
            .map_err(|_| NetworkError::Corrupt)?;
        tokio::time::timeout(self.options.timeout.min(Duration::from_secs(2)), async {
            let response = self
                .http
                .fetch(req)
                .await
                .map_err(|_| NetworkError::Transient)?;
            let status = response.status().as_u16();
            match status {
                401 => return Err(NetworkError::Auth),
                404 => return Err(NetworkError::Missing),
                429 => return Err(NetworkError::RateLimit),
                500..=599 => return Err(NetworkError::Transient),
                300..=399 => return Err(NetworkError::Unsupported),
                _ => (),
            }
            let bound = if status == 403 { 16384 } else { cap };
            if response
                .headers()
                .get(http::header::CONTENT_LENGTH)
                .and_then(|h| h.to_str().ok())
                .and_then(|v| v.parse::<usize>().ok())
                .is_some_and(|v| v > bound)
            {
                return Err(NetworkError::Limit);
            }
            let mut body = response.into_body();
            let mut out = Vec::new();
            loop {
                let chunk = body.read().await.map_err(|_| NetworkError::Transient)?;
                if chunk.is_empty() {
                    break;
                }
                if out.len().saturating_add(chunk.len()) > bound {
                    return Err(NetworkError::Limit);
                }
                out.extend_from_slice(&chunk.to_bytes());
            }
            if status == 403 {
                let v: serde_json::Value =
                    serde_json::from_slice(&out).unwrap_or(serde_json::Value::Null);
                let rate = v
                    .pointer("/error/errors")
                    .and_then(|v| v.as_array())
                    .is_some_and(|errors| {
                        errors.iter().take(64).any(|e| {
                            matches!(
                                e.get("reason").and_then(|v| v.as_str()),
                                Some("rateLimitExceeded" | "userRateLimitExceeded")
                            )
                        })
                    });
                return Err(if rate {
                    NetworkError::RateLimit
                } else {
                    NetworkError::Auth
                });
            }
            if !(200..=299).contains(&status) {
                return Err(NetworkError::Corrupt);
            }
            Ok(out)
        })
        .await
        .map_err(|_| NetworkError::Timeout)?
    }
    async fn meta(&self, file_id: &str) -> Result<Meta> {
        id(file_id)?;
        let b = self
            .request(
                "GET",
                &format!("/drive/v3/files/{file_id}"),
                &[("fields", FIELDS)],
                "application/json",
                vec![],
                JSON_CAP,
            )
            .await?;
        let m: Meta = serde_json::from_slice(&b).map_err(|_| NetworkError::Corrupt)?;
        m.valid()?;
        if m.id != file_id {
            return Err(NetworkError::Corrupt);
        }
        Ok(m)
    }
    async fn root(&self) -> Result<()> {
        let m = self.meta(&self.root).await?;
        if m.trashed || m.mime_type != FOLDER {
            return Err(NetworkError::Corrupt);
        }
        Ok(())
    }
    async fn children(&self, parent: &str, budget: &mut Budget) -> Result<Vec<Meta>> {
        budget.dirs += 1;
        if budget.dirs > MAX_DIRS {
            return Err(NetworkError::Limit);
        }
        let q = format!("'{parent}' in parents and trashed = false");
        let fields = format!("nextPageToken,incompleteSearch,files({FIELDS})");
        let mut token = String::new();
        let mut tokens = BTreeSet::new();
        let mut all = BTreeMap::new();
        loop {
            budget.pages += 1;
            if budget.pages > MAX_PAGES {
                return Err(NetworkError::Limit);
            }
            let mut query = vec![
                ("q", q.as_str()),
                ("fields", fields.as_str()),
                ("pageSize", "1000"),
                ("spaces", "drive"),
            ];
            if !token.is_empty() {
                query.push(("pageToken", &token));
            }
            let b = self
                .request(
                    "GET",
                    "/drive/v3/files",
                    &query,
                    "application/json",
                    vec![],
                    JSON_CAP,
                )
                .await?;
            let page: Listing = serde_json::from_slice(&b).map_err(|_| NetworkError::Corrupt)?;
            if page.incomplete_search {
                return Err(NetworkError::Transient);
            }
            budget.entries += page.files.len();
            if budget.entries > MAX_IDS {
                return Err(NetworkError::Limit);
            }
            for m in page.files {
                m.valid()?;
                if !m.child(parent) || (m.mime_type != FOLDER && m.mime_type != FILE) {
                    continue;
                }
                if let Some(old) = all.insert(m.id.clone(), m.clone())
                    && old != m
                {
                    return Err(NetworkError::Corrupt);
                }
            }
            match page.next_page_token {
                None => break,
                Some(next) => {
                    if next.is_empty()
                        || next.len() > 2048
                        || !next.bytes().all(|b| b.is_ascii_graphic())
                        || !tokens.insert(next.clone())
                    {
                        return Err(NetworkError::Corrupt);
                    }
                    token = next;
                }
            }
        }
        Ok(all.into_values().collect())
    }
    async fn folders(
        &self,
        parents: Vec<String>,
        name: &str,
        create: bool,
        budget: &mut Budget,
    ) -> Result<Vec<String>> {
        let mut out = BTreeSet::new();
        for parent in &parents {
            for m in self.children(parent, budget).await? {
                if m.name == name && m.mime_type == FOLDER {
                    out.insert(m.id);
                }
            }
        }
        if out.is_empty() && create {
            let parent = parents.first().ok_or(NetworkError::Missing)?;
            let bytes =
                serde_json::to_vec(&json!({"name":name,"parents":[parent],"mimeType":FOLDER}))
                    .map_err(|_| NetworkError::Corrupt)?;
            let b = self
                .request(
                    "POST",
                    "/drive/v3/files",
                    &[("fields", FIELDS)],
                    "application/json",
                    bytes,
                    JSON_CAP,
                )
                .await?;
            let m: Meta = serde_json::from_slice(&b).map_err(|_| NetworkError::Corrupt)?;
            m.valid()?;
            if !m.child(parent) || m.name != name || m.mime_type != FOLDER {
                return Err(NetworkError::Corrupt);
            }
            let back = self.meta(&m.id).await?;
            if !back.child(parent) || back.name != name || back.mime_type != FOLDER {
                return Err(NetworkError::Corrupt);
            }
            out.insert(back.id);
        }
        if out.len() > MAX_CANDIDATES {
            return Err(NetworkError::Limit);
        }
        Ok(out.into_iter().collect())
    }
    async fn locate(
        &self,
        key: &ObjectKey,
        create: bool,
    ) -> Result<(Vec<String>, Vec<Meta>, String)> {
        let path = object_path(key)?;
        self.root().await?;
        let parts: Vec<_> = path.split('/').collect();
        let mut dirs = vec![self.root.clone()];
        let mut budget = Budget::default();
        for part in &parts[..4] {
            dirs = self.folders(dirs, part, create, &mut budget).await?;
        }
        let name = parts[4].to_owned();
        let mut found = BTreeMap::new();
        for parent in &dirs {
            for m in self.children(parent, &mut budget).await? {
                if m.name == name && m.mime_type == FILE {
                    found.insert(m.id.clone(), m);
                }
            }
        }
        if found.len() > MAX_CANDIDATES {
            return Err(NetworkError::Limit);
        }
        Ok((dirs, found.into_values().collect(), name))
    }
    async fn read_candidate(&self, located: &Meta) -> Result<Vec<u8>> {
        let m = self.meta(&located.id).await?;
        if m != *located || m.trashed || m.mime_type != FILE {
            return Err(NetworkError::Corrupt);
        }
        let size = m
            .size
            .as_ref()
            .ok_or(NetworkError::Corrupt)?
            .parse::<usize>()
            .map_err(|_| NetworkError::Corrupt)?;
        if size > MAX_ENVELOPE {
            return Err(NetworkError::Limit);
        }
        let bytes = self
            .request(
                "GET",
                &format!("/drive/v3/files/{}", m.id),
                &[("alt", "media")],
                "application/octet-stream",
                vec![],
                MAX_ENVELOPE,
            )
            .await?;
        if bytes.len() != size {
            return Err(NetworkError::Corrupt);
        }
        Ok(bytes)
    }
    async fn fetch_bytes(&self, key: &ObjectKey) -> Result<Vec<u8>> {
        let (_, mut candidates, _) = self.locate(key, false).await?;
        {
            let mut state = self.candidates.lock().await;
            if state.len() >= MAX_IDS && !state.contains_key(key) {
                return Err(NetworkError::Limit);
            }
            let previous = state
                .get(key)
                .and_then(|(ids, at)| ids.get(*at))
                .map(|m| m.id.clone());
            if let Some(at) = previous.and_then(|id| candidates.iter().position(|m| m.id == id)) {
                candidates.rotate_left(at);
            }
            state.insert(key.clone(), (candidates.clone(), 0));
        }
        let mut bad = None;
        let mut error = NetworkError::Missing;
        for (at, m) in candidates.iter().enumerate() {
            if let Some((ids, next)) = self.candidates.lock().await.get_mut(key) {
                *next = (at + 1) % ids.len();
            }
            let read = tokio::time::timeout(self.options.timeout / 4, self.read_candidate(m))
                .await
                .unwrap_or(Err(NetworkError::Timeout));
            match read {
                Ok(bytes) => {
                    if (OutgoingEnvelope {
                        key: key.clone(),
                        bytes: bytes.clone(),
                    })
                    .validate()
                    .is_ok()
                    {
                        return Ok(bytes);
                    }
                    bad = Some(bytes);
                }
                Err(NetworkError::Auth) => return Err(NetworkError::Auth),
                Err(e) => error = e,
            }
        }
        bad.ok_or(error)
    }
    async fn publish(&self, p: OutgoingEnvelope) -> Result<()> {
        p.validate().map_err(|_| NetworkError::Corrupt)?;
        if p.bytes.len() > 4_999_000 {
            return Err(NetworkError::Limit);
        }
        let (dirs, candidates, name) = self.locate(&p.key, true).await?;
        let mut found = false;
        for m in candidates {
            match self.read_candidate(&m).await {
                Ok(b) => {
                    if b == p.bytes && hash(&b) == p.key.envelope_hash.unwrap() {
                        found = true;
                    }
                }
                Err(NetworkError::Missing | NetworkError::Corrupt | NetworkError::Limit) => (),
                Err(e) => return Err(e),
            }
        }
        if found {
            return Ok(());
        }
        let parent = dirs.first().ok_or(NetworkError::Missing)?;
        let metadata = json!({"name":name,"parents":[parent],"mimeType":FILE}).to_string();
        let boundary = format!("lfs_{}", lfs_protocol::hash_hex(&hash(&p.bytes)));
        if p.bytes
            .windows(boundary.len())
            .any(|w| w == boundary.as_bytes())
        {
            return Err(NetworkError::Corrupt);
        }
        let mut body = format!("--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{metadata}\r\n--{boundary}\r\nContent-Type: application/octet-stream\r\n\r\n").into_bytes();
        body.extend_from_slice(&p.bytes);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        if body.len() > 5_000_000 {
            return Err(NetworkError::Limit);
        }
        let b = self
            .request(
                "POST",
                "/upload/drive/v3/files",
                &[("uploadType", "multipart"), ("fields", FIELDS)],
                &format!("multipart/related; boundary={boundary}"),
                body,
                JSON_CAP,
            )
            .await?;
        let m: Meta = serde_json::from_slice(&b).map_err(|_| NetworkError::Corrupt)?;
        m.valid()?;
        if !m.child(parent) || m.name != name || m.mime_type != FILE {
            return Err(NetworkError::Corrupt);
        }
        let back = self.read_candidate(&m).await?;
        if back != p.bytes || Some(hash(&back)) != p.key.envelope_hash {
            return Err(NetworkError::Corrupt);
        }
        Ok(())
    }
    async fn discover(&self, dataset: &str) -> Result<Vec<ObjectKey>> {
        crate::dataset_root(dataset)?;
        self.root().await?;
        let mut budget = Budget::default();
        let dirs = self
            .folders(vec![self.root.clone()], "lfs1", false, &mut budget)
            .await?;
        let dirs = self
            .folders(dirs, &segment(dataset), false, &mut budget)
            .await?;
        let mut keys = BTreeSet::new();
        let mut locations: BTreeMap<ObjectKey, (Vec<Meta>, usize)> = BTreeMap::new();
        for dir in dirs {
            for device in self.children(&dir, &mut budget).await? {
                if device.mime_type != FOLDER {
                    continue;
                }
                if unsegment(&device.name).is_err() {
                    continue;
                }
                for inc in self.children(&device.id, &mut budget).await? {
                    if inc.mime_type != FOLDER || unsegment(&inc.name).is_err() {
                        continue;
                    }
                    for file in self.children(&inc.id, &mut budget).await? {
                        if file.mime_type != FILE {
                            continue;
                        }
                        let path = format!(
                            "lfs1/{}/{}/{}/{}",
                            segment(dataset),
                            device.name,
                            inc.name,
                            file.name
                        );
                        if let Ok(key) = parse_path(&path, dataset) {
                            keys.insert(key.clone());
                            let entry = locations.entry(key).or_default();
                            if !entry.0.iter().any(|m| m.id == file.id) {
                                entry.0.push(file);
                            }
                            if entry.0.len() > MAX_CANDIDATES {
                                return Err(NetworkError::Limit);
                            }
                        }
                    }
                }
            }
        }
        let mut prior = self.candidates.lock().await;
        for (key, (ids, at)) in &mut locations {
            if let Some((old, next)) = prior.get(key)
                && let Some(previous) = old.get(*next)
            {
                *at = ids.iter().position(|m| m.id == previous.id).unwrap_or(0);
            }
        }
        *prior = locations;
        Ok(keys.into_iter().collect())
    }
    async fn page(&self, dataset: &str, cursor: usize, limit: usize) -> Result<Page> {
        if limit == 0 || limit > 1000 || cursor > 1_000_000 {
            return Err(NetworkError::Limit);
        }
        crate::dataset_root(dataset)?;
        let mut guard = self.scan.lock().await;
        if cursor == 0
            || guard
                .as_ref()
                .is_none_or(|s| s.dataset != dataset || s.expected != cursor)
        {
            *guard = Some(Scan {
                dataset: dataset.into(),
                expected: cursor,
                at: 0,
                keys: self.discover(dataset).await?,
            });
        } else {
            self.root().await?;
        }
        let s = guard.as_mut().unwrap();
        let end = (s.at + limit).min(s.keys.len());
        let keys = s.keys[s.at..end].to_vec();
        s.at = end;
        s.expected = cursor.checked_add(keys.len()).ok_or(NetworkError::Limit)?;
        let next = if end == s.keys.len() {
            None
        } else {
            Some(s.expected)
        };
        if next.is_none() {
            *guard = None;
        }
        Ok(Page {
            keys,
            next,
            complete: true,
        })
    }
}
impl Transport for GoogleDriveTransport {
    fn upload<'a>(&'a self, p: OutgoingEnvelope, c: &'a Cancellation) -> NetworkFuture<'a, ()> {
        Box::pin(async move { self.bounded(c, self.publish(p)).await })
    }
    fn list<'a>(
        &'a self,
        d: &'a str,
        cursor: usize,
        limit: usize,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, Page> {
        Box::pin(async move { self.bounded(c, self.page(d, cursor, limit)).await })
    }
    fn fetch<'a>(&'a self, k: &'a ObjectKey, c: &'a Cancellation) -> NetworkFuture<'a, Download> {
        Box::pin(async move {
            self.bounded(c, self.fetch_bytes(k))
                .await
                .map(Download::Wire)
        })
    }
    fn download<'a>(&'a self, k: &'a ObjectKey, c: &'a Cancellation) -> NetworkFuture<'a, Package> {
        Box::pin(async move {
            let bytes = self.bounded(c, self.fetch_bytes(k)).await?;
            OutgoingEnvelope {
                key: k.clone(),
                bytes,
            }
            .validate()
            .map_err(|_| NetworkError::Corrupt)
        })
    }
}
