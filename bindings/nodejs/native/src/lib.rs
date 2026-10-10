//! Each native handle owns one serial worker, SDK and current-thread runtime.
mod backend;
mod values;
use backend::Backend;
use lfs_sqlite::{Baseline, Cancellation, Error as CoreError, Identity, Schema, Sdk};
use napi::{Env, Task, bindgen_prelude::*};
use napi_derive::napi;
use serde::Deserialize;
use serde_json::{Value as Json, json};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, mpsc};
use values::{Batch, Resolution, Statement, Typed, hex, unhex};

const MAX_INPUT: usize = 8 * 1024 * 1024;
const MAX_PENDING: usize = 16;
static HANDLES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
struct HandlePermit;
impl HandlePermit {
    fn acquire() -> napi::Result<Self> {
        HANDLES
            .fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |n| if n < 16 { Some(n + 1) } else { None },
            )
            .map_err(|_| fail("Busy"))?;
        Ok(Self)
    }
}
impl Drop for HandlePermit {
    fn drop(&mut self) {
        HANDLES.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}
fn fail(code: &str) -> napi::Error {
    napi::Error::new(napi::Status::GenericFailure, code)
}
fn category(e: &CoreError) -> &'static str {
    use lfs_sqlite::NetworkError as N;
    match e {
        CoreError::ConflictRequiresResolution { .. } => "ConflictRequiresResolution",
        CoreError::Busy | CoreError::SyncInProgress | CoreError::DatabaseInUse => "Busy",
        CoreError::Closed => "Closed",
        CoreError::Schema(_) | CoreError::IdentityMismatch | CoreError::BaselineRequired => {
            "Schema"
        }
        CoreError::Network(N::Cancelled) => "Cancelled",
        CoreError::Network(N::Unsupported) => "Unsupported",
        CoreError::Network(N::Auth) => "Auth",
        CoreError::Network(N::Transient | N::Timeout | N::RateLimit | N::LostAck) => "Transient",
        CoreError::Network(N::Missing) => "Missing",
        CoreError::Network(N::Corrupt) => "Corrupt",
        CoreError::Network(N::Limit) => "Limit",
        CoreError::MissingDependencies
        | CoreError::StreamFork
        | CoreError::Invalid("shared backup unpublished or blocked") => "Blocked",
        CoreError::Io(_) => "Io",
        CoreError::Poisoned | CoreError::ExternalChange => "Corrupt",
        CoreError::Sqlite(rusqlite::Error::SqliteFailure(c, _)) => match c.code {
            rusqlite::ErrorCode::ConstraintViolation => "Constraint",
            rusqlite::ErrorCode::AuthorizationForStatementDenied => "Forbidden",
            rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked => "Busy",
            _ => "Invalid",
        },
        _ => "Invalid",
    }
}
fn parse<T: serde::de::DeserializeOwned>(s: &str) -> lfs_sqlite::Result<T> {
    if s.len() > MAX_INPUT {
        return Err(CoreError::Invalid("limit"));
    }
    serde_json::from_str(s).map_err(|_| CoreError::Invalid("input"))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    path: String,
    identity: [String; 3],
    #[serde(deserialize_with = "strict_schema")]
    schema: Schema,
    backend: Backend,
    #[serde(default)]
    baseline_import: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreConfig {
    backup: String,
    destination: String,
    backend: Backend,
}
enum OpenConfig {
    New(Config),
    Restore(RestoreConfig),
}
impl OpenConfig {
    fn into_sdk(self, c: &Cancellation) -> lfs_sqlite::Result<(Sdk, Backend)> {
        match self {
            Self::New(config) => {
                config.backend.validate()?;
                let [dataset, device, incarnation] = config.identity;
                let sdk = Sdk::open(
                    config.path,
                    Identity {
                        dataset,
                        device,
                        incarnation,
                    },
                    config.schema,
                    if config.baseline_import {
                        Baseline::Import
                    } else {
                        Baseline::Reject
                    },
                )?;
                Ok((sdk, config.backend))
            }
            Self::Restore(config) => {
                config.backend.validate()?;
                Ok((
                    Sdk::restore(config.backup, config.destination, c)?,
                    config.backend,
                ))
            }
        }
    }
}
#[derive(Default)]
struct Gate {
    pending: usize,
    closing: bool,
    syncing: bool,
}
struct Job {
    op: String,
    input: String,
    cancel: Cancellation,
    reply: mpsc::Sender<napi::Result<String>>,
}
struct Control {
    sender: mpsc::SyncSender<Job>,
    gate: Arc<Mutex<Gate>>,
}
#[napi]
pub struct NativeStore {
    control: Arc<Control>,
}
#[napi]
pub struct CancelToken {
    token: Cancellation,
}
#[napi]
impl CancelToken {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            token: Cancellation::default(),
        }
    }
    #[napi]
    pub fn cancel(&self) {
        self.token.cancel();
    }
}
impl Default for CancelToken {
    fn default() -> Self {
        Self::new()
    }
}
pub struct Reply {
    receiver: mpsc::Receiver<napi::Result<String>>,
    control: Arc<Control>,
    syncing: bool,
}
impl Drop for Reply {
    fn drop(&mut self) {
        if let Ok(mut gate) = self.control.gate.lock() {
            gate.pending -= 1;
            if self.syncing {
                gate.syncing = false;
            }
        }
    }
}
impl Task for Reply {
    type Output = String;
    type JsValue = String;
    fn compute(&mut self) -> napi::Result<String> {
        self.receiver.recv().map_err(|_| fail("Closed"))?
    }
    fn resolve(&mut self, _: Env, value: String) -> napi::Result<String> {
        Ok(value)
    }
}
#[napi]
impl NativeStore {
    #[napi]
    pub fn request(
        &self,
        op: String,
        input: String,
        token: Option<&CancelToken>,
    ) -> napi::Result<AsyncTask<Reply>> {
        if input.len() > MAX_INPUT || op.len() > 32 {
            return Err(fail("Limit"));
        }
        let mut gate = self.control.gate.lock().map_err(|_| fail("Closed"))?;
        if gate.closing {
            return Err(fail("Closed"));
        }
        if (op != "close" && gate.pending >= MAX_PENDING) || (op == "sync" && gate.syncing) {
            return Err(fail("Busy"));
        }
        let (tx, rx) = mpsc::channel();
        self.control
            .sender
            .try_send(Job {
                op: op.clone(),
                input,
                cancel: token.map(|t| t.token.clone()).unwrap_or_default(),
                reply: tx,
            })
            .map_err(|e| match e {
                mpsc::TrySendError::Full(_) => fail("Busy"),
                mpsc::TrySendError::Disconnected(_) => {
                    gate.closing = true;
                    fail("Closed")
                }
            })?;
        gate.pending += 1;
        if op == "sync" {
            gate.syncing = true;
        }
        if op == "close" {
            gate.closing = true;
        }
        Ok(AsyncTask::new(Reply {
            receiver: rx,
            control: self.control.clone(),
            syncing: op == "sync",
        }))
    }
}
pub struct Open {
    input: String,
    restore: bool,
    cancel: Cancellation,
    permit: Option<HandlePermit>,
}
impl Task for Open {
    type Output = NativeStore;
    type JsValue = NativeStore;
    fn compute(&mut self) -> napi::Result<NativeStore> {
        let config = if self.restore {
            parse(&self.input).map(OpenConfig::Restore)
        } else {
            parse(&self.input).map(OpenConfig::New)
        }
        .map_err(|e| fail(category(&e)))?;
        let cancel = self.cancel.clone();
        let (tx, rx) = mpsc::sync_channel::<Job>(MAX_PENDING + 1);
        let (ready_tx, ready_rx) = mpsc::channel();
        let gate = Arc::new(Mutex::new(Gate::default()));
        let permit = self.permit.take().ok_or_else(|| fail("Closed"))?;
        std::thread::Builder::new().name("lfs-node".into()).spawn(move || {
            let _permit=permit;
            let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().max_blocking_threads(2).build() {Ok(r)=>r,Err(_)=>{let _=ready_tx.send(Err(fail("Io")));return;}};
            let (sdk,backend) = match config.into_sdk(&cancel) {Ok(s) => s, Err(e) => {let _ = ready_tx.send(Err(fail(category(&e)))); return;}};
            if ready_tx.send(Ok(())).is_err() { return; }
            let mut events=VecDeque::new();
            for job in rx {
                let result=if job.op=="events" {Ok(json!(events))} else {execute(&sdk,&backend,&runtime,&job.op,&job.input,&job.cancel)};
                if job.op!="events" {
                    if events.len()==64 {events.pop_front();}
                    let operation=match job.op.as_str() {"read"|"write"|"sync"|"resolve"|"conflicts"|"close"|"status"=>job.op.as_str(),_=>"operation"};
                    events.push_back(json!({"operation":operation,"success":result.is_ok(),"error":result.as_ref().err().map(category)}));
                }
                let result=result.map(|v|v.to_string()).map_err(|e|fail(category(&e)));
                let _ = job.reply.send(result);
                if job.op == "close" { break; }
            }
        }).map_err(|_|fail("Io"))?;
        ready_rx.recv().map_err(|_| fail("Closed"))??;
        Ok(NativeStore {
            control: Arc::new(Control { sender: tx, gate }),
        })
    }
    fn resolve(&mut self, _: Env, value: NativeStore) -> napi::Result<NativeStore> {
        Ok(value)
    }
}
#[napi]
pub fn open(input: String) -> napi::Result<AsyncTask<Open>> {
    if input.len() > MAX_INPUT {
        return Err(fail("Limit"));
    }
    Ok(AsyncTask::new(Open {
        input,
        restore: false,
        cancel: Cancellation::default(),
        permit: Some(HandlePermit::acquire()?),
    }))
}
#[napi]
pub fn restore(input: String, token: Option<&CancelToken>) -> napi::Result<AsyncTask<Open>> {
    if input.len() > MAX_INPUT {
        return Err(fail("Limit"));
    }
    Ok(AsyncTask::new(Open {
        input,
        restore: true,
        cancel: token.map(|t| t.token.clone()).unwrap_or_default(),
        permit: Some(HandlePermit::acquire()?),
    }))
}
fn status(s: &Sdk) -> lfs_sqlite::Result<Json> {
    let st = s.status()?;
    Ok(
        json!({"pendingOutbox":st.pending_outbox.to_string(),"healthy":st.healthy(),"blockedStreams":st.stopped_streams.to_string(),"outstandingObjects":st.outstanding_objects.to_string(),"conflicts":s.list_conflicts()?.len().to_string()}),
    )
}
fn execute(
    s: &Sdk,
    backend: &Backend,
    runtime: &tokio::runtime::Runtime,
    op: &str,
    input: &str,
    c: &Cancellation,
) -> lfs_sqlite::Result<Json> {
    if op != "close" {
        c.check().map_err(CoreError::Network)?;
    }
    match op {
        "read" => { let q:Statement=parse(input)?; validate_sql(&q.sql)?; let p=q.params.into_iter().map(Typed::value).collect::<lfs_sqlite::Result<Vec<_>>>()?;
            let rows=s.read(|r|r.query(&q.sql,&p))?;
            Ok(json!(rows.into_iter().map(|r|r.into_iter().map(Typed::from).collect::<Vec<_>>()).collect::<Vec<_>>())) }
        "write" => { let b:Batch=parse(input)?; if b.statements.is_empty() || b.statements.len()>1000 {return Err(CoreError::Invalid("batch"));}
            let statements=b.statements.into_iter().map(|q|{validate_sql(&q.sql)?; Ok((q.sql,q.params.into_iter().map(Typed::value).collect::<lfs_sqlite::Result<Vec<_>>>()?))}).collect::<lfs_sqlite::Result<Vec<_>>>()?;
            let (_,commit)=s.write_transaction_with_attachments(&b.attachments,|w|{for (sql,p) in &statements {c.check().map_err(CoreError::Network)?; w.execute(sql,p)?;} c.check().map_err(CoreError::Network)?; Ok(())})?;
            Ok(json!({"committed":commit.is_some(),"status":status(s)?})) }
        "sync" => {
            let t=backend.transport()?;
            let r=runtime.block_on(s.sync_once(&t,c,lfs_sqlite::SyncLimits::default()))?;
            Ok(json!({"uploaded":r.uploaded.to_string(),"downloaded":r.downloaded.to_string(),"completeScan":r.complete_scan,"healthy":r.healthy(),"status":status(s)?}))
        },
        "conflicts" => Ok(json!(s.list_conflicts()?.into_iter().map(|c|json!({"table":c.table,"key":c.key,"heads":c.heads.into_iter().map(|o|json!({"id":hex(&o.id.encode()),"row":o.row.map(|r|r.into_iter().map(Typed::from).collect::<Vec<_>>())})).collect::<Vec<_>>()})).collect::<Vec<_>>())),
        "resolve" => {
            let r:Resolution=parse(input)?;
            let ids=r.observed.iter().map(|v| lfs_protocol::OpId::decode(&unhex(v)?).map_err(CoreError::Invalid)).collect::<lfs_sqlite::Result<Vec<_>>>()?;
            let row=r.row.map(|r|r.into_iter().map(Typed::value).collect::<lfs_sqlite::Result<Vec<_>>>()).transpose()?;
            s.resolve_with_attachments(r.table,&r.key,ids,row,&r.attachments)?;
            status(s)
        },
        "attachmentAdd" => {
            let path:String=parse(input)?; safe_path(&path)?;
            if !std::fs::symlink_metadata(&path)?.is_file() {return Err(CoreError::Invalid("file"));}
            let a=s.add_attachment(std::fs::File::open(path)?,lfs_sqlite::attachments::DEFAULT_LIMIT,c)?;
            Ok(json!({"hash":a.hash,"size":a.size.to_string(),"state":"Available"}))
        },
        "attachmentDownload" => {
            let hash:String=parse(input)?;
            let t=backend.transport()?;
            if !t.attachment_streaming_supported() {return Err(CoreError::Network(lfs_sqlite::NetworkError::Unsupported));}
            let a=runtime.block_on(s.download_attachment(&t,&hash,lfs_sqlite::attachments::DEFAULT_LIMIT,c))?;
            Ok(json!({"hash":a.hash,"size":a.size.to_string(),"state":"Available"}))
        },
        "attachmentStatus" => {
            let h:String=parse(input)?;let a=s.attachment_status(&h)?;
            Ok(json!({"hash":a.hash,"size":a.size.map(|n|n.to_string()),"state":a.state}))
        },
        "attachmentExport" => {
            use std::io::{Read,Write};
            let (hash,path):(String,String)=parse(input)?;safe_path(&path)?;
            let mut src=s.open_attachment(&hash,c)?;
            let mut dst=std::fs::OpenOptions::new().create_new(true).write(true).open(&path)?;
            let result=(|| -> lfs_sqlite::Result<()> {let mut buf=vec![0;65536];loop {c.check().map_err(CoreError::Network)?;let n=src.read(&mut buf)?;if n==0 {break;}dst.write_all(&buf[..n])?;}dst.sync_all()?;Ok(())})();
            drop(dst); if result.is_err() {let _=std::fs::remove_file(&path);}result?;
            Ok(json!({"exported":true}))
        },
        "backup" => {
            let (destination,kind):(String,lfs_sqlite::backup::BackupKind)=parse(input)?;
            let m=s.backup(destination,kind,lfs_sqlite::backup::BackupOptions::default(),c)?;
            Ok(json!(m))
        },
        "info" => {let id=s.identity()?; Ok(json!({"identity":[id.dataset,id.device,id.incarnation],"schema":s.schema()?}))},
        "status" => status(s),
        "close" => {s.close()?;Ok(json!({"closed":true}))},
        _ => Err(CoreError::Network(lfs_sqlite::NetworkError::Unsupported)),
    }
}

fn validate_sql(sql: &str) -> lfs_sqlite::Result<()> {
    if sql.len() > 65536 || sql.contains(';') {
        Err(CoreError::Invalid("single statement"))
    } else {
        Ok(())
    }
}

pub(crate) fn safe_path(path: &str) -> lfs_sqlite::Result<()> {
    use std::path::{Component, Path, PathBuf};
    if path.is_empty() || path.len() > 4096 {
        return Err(CoreError::Invalid("path"));
    }
    let mut current = PathBuf::new();
    for part in Path::new(path).components() {
        if matches!(part, Component::ParentDir) {
            return Err(CoreError::Invalid("path"));
        }
        current.push(part);
        match std::fs::symlink_metadata(&current) {
            Ok(m) if m.file_type().is_symlink() => return Err(CoreError::Invalid("symlink")),
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

fn strict_schema<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Schema, D::Error> {
    fn keys(v: &Json, allowed: &[&str]) -> bool {
        v.as_object()
            .is_some_and(|m| m.keys().all(|k| allowed.contains(&k.as_str())))
    }
    let v = Json::deserialize(deserializer)?;
    let valid = keys(&v, &["version", "tables"])
        && v["tables"].as_array().is_some_and(|ts| {
            ts.iter().all(|t| {
                keys(t, &["id", "name", "columns"])
                    && t["columns"]
                        .as_array()
                        .is_some_and(|cs| cs.iter().all(|c| keys(c, &["name", "kind", "nullable"])))
            })
        });
    if !valid {
        return Err(serde::de::Error::custom("schema"));
    }
    serde_json::from_value(v).map_err(|_| serde::de::Error::custom("schema"))
}
