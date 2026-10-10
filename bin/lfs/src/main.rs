use lfs_sqlite::backup::{BackupKind, BackupOptions};
use lfs_sqlite::sync::{Download, NetworkFuture, Page};
use lfs_sqlite::*;
use lfs_storage::{
    DriveToken, GoogleDriveTransport, OpenDalTransport, Options, S3Credentials, WebdavCredentials,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use std::{
    fs::{File, OpenOptions},
    io::{Read as IoRead, Write as IoWrite},
    path::{Path, PathBuf},
};
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    identity: [String; 3],
    schema: Schema,
    backend: Backend,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Backend {
    GoogleDrive {
        root_folder_id: String,
        access_token_env: String,
        #[serde(default)]
        owned_loopback_origin: Option<String>,
    },
    Fs {
        root: PathBuf,
    },
    WebDAV {
        endpoint: String,
        username: String,
        password_env: String,
        #[serde(default)]
        allow_loopback_http: bool,
    },
    S3 {
        endpoint: String,
        region: String,
        bucket: String,
        access_key_id_env: String,
        secret_access_key_env: String,
        session_token_env: Option<String>,
        #[serde(default)]
        allow_loopback_http: bool,
    },
}
fn credential_source(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_')
    {
        return Err(Error::Invalid("credential source name"));
    }
    Ok(())
}
impl Backend {
    fn validate(&self) -> Result<()> {
        if let Self::GoogleDrive {
            root_folder_id,
            access_token_env,
            owned_loopback_origin,
        } = self
        {
            credential_source(access_token_env)?;
            GoogleDriveTransport::validate_configuration(
                root_folder_id,
                owned_loopback_origin.as_deref(),
            )
            .map_err(Error::Network)?;
        }
        Ok(())
    }
    fn kind(&self) -> &str {
        match self {
            Self::GoogleDrive { .. } => "GoogleDrive",
            Self::Fs { .. } => "Fs",
            Self::WebDAV { .. } => "WebDAV",
            Self::S3 { .. } => "S3",
        }
    }
    fn transport(self) -> Result<CliTransport> {
        fn explicit(name: &str) -> Result<String> {
            credential_source(name)?;
            std::env::var(name).map_err(|_| Error::Network(NetworkError::Auth))
        }
        let result = match self {
            Self::GoogleDrive {
                root_folder_id,
                access_token_env,
                owned_loopback_origin,
            } => {
                let token =
                    DriveToken::new(explicit(&access_token_env)?).map_err(Error::Network)?;
                let t = match owned_loopback_origin {
                    Some(origin) => GoogleDriveTransport::owned_loopback(
                        &root_folder_id,
                        token,
                        &origin,
                        Options::default(),
                    ),
                    None => GoogleDriveTransport::new(&root_folder_id, token, Options::default()),
                };
                return t.map(CliTransport::Drive).map_err(Error::Network);
            }
            Self::Fs { root } => OpenDalTransport::fs(root, Options::default()),
            Self::WebDAV {
                endpoint,
                username,
                password_env,
                allow_loopback_http,
            } => OpenDalTransport::webdav(
                &endpoint,
                WebdavCredentials::Basic {
                    username,
                    password: explicit(&password_env)?,
                },
                Options {
                    allow_loopback_http,
                    ..Default::default()
                },
            ),
            Self::S3 {
                endpoint,
                region,
                bucket,
                access_key_id_env,
                secret_access_key_env,
                session_token_env,
                allow_loopback_http,
            } => OpenDalTransport::s3(
                &endpoint,
                &region,
                &bucket,
                S3Credentials {
                    access_key_id: explicit(&access_key_id_env)?,
                    secret_access_key: explicit(&secret_access_key_env)?,
                    session_token: session_token_env.as_deref().map(explicit).transpose()?,
                },
                Options {
                    allow_loopback_http,
                    ..Default::default()
                },
            ),
        };
        result.map(CliTransport::OpenDal).map_err(Error::Network)
    }
}
enum CliTransport {
    OpenDal(OpenDalTransport),
    Drive(GoogleDriveTransport),
}
impl Transport for CliTransport {
    fn upload<'a>(&'a self, p: OutgoingEnvelope, c: &'a Cancellation) -> NetworkFuture<'a, ()> {
        match self {
            Self::OpenDal(t) => t.upload(p, c),
            Self::Drive(t) => t.upload(p, c),
        }
    }
    fn list<'a>(
        &'a self,
        d: &'a str,
        at: usize,
        n: usize,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, Page> {
        match self {
            Self::OpenDal(t) => t.list(d, at, n, c),
            Self::Drive(t) => t.list(d, at, n, c),
        }
    }
    fn download<'a>(&'a self, k: &'a ObjectKey, c: &'a Cancellation) -> NetworkFuture<'a, Package> {
        match self {
            Self::OpenDal(t) => t.download(k, c),
            Self::Drive(t) => t.download(k, c),
        }
    }
    fn fetch<'a>(&'a self, k: &'a ObjectKey, c: &'a Cancellation) -> NetworkFuture<'a, Download> {
        match self {
            Self::OpenDal(t) => t.fetch(k, c),
            Self::Drive(t) => t.fetch(k, c),
        }
    }
    fn upload_attachment<'a>(
        &'a self,
        d: &'a str,
        h: &'a str,
        f: File,
        n: u64,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, ()> {
        match self {
            Self::OpenDal(t) => t.upload_attachment(d, h, f, n, c),
            Self::Drive(t) => t.upload_attachment(d, h, f, n, c),
        }
    }
    fn download_attachment<'a>(
        &'a self,
        d: &'a str,
        h: &'a str,
        f: File,
        n: u64,
        c: &'a Cancellation,
    ) -> NetworkFuture<'a, ()> {
        match self {
            Self::OpenDal(t) => t.download_attachment(d, h, f, n, c),
            Self::Drive(t) => t.download_attachment(d, h, f, n, c),
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "lowercase")]
enum Typed {
    Null,
    Integer(String),
    Real(String),
    Text(String),
    Blob(String),
}
fn unhex(s: &str) -> Result<Vec<u8>> {
    if !s.len().is_multiple_of(2)
        || !s
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::Invalid("hex"));
    }
    s.as_bytes()
        .chunks(2)
        .map(|v| {
            u8::from_str_radix(std::str::from_utf8(v).unwrap(), 16)
                .map_err(|_| Error::Invalid("hex"))
        })
        .collect()
}
fn hex(v: &[u8]) -> String {
    v.iter().map(|b| format!("{b:02x}")).collect()
}
impl Typed {
    fn value(self) -> Result<Value> {
        let v = match self {
            Self::Null => Value::Null,
            Self::Integer(s) => {
                let n = s.parse::<i64>().map_err(|_| Error::Invalid("integer"))?;
                if n.to_string() != s {
                    return Err(Error::Invalid("integer"));
                }
                Value::Integer(n)
            }
            Self::Real(s) => {
                if s.len() != 16 {
                    return Err(Error::Invalid("f64 bits"));
                }
                let b = unhex(&s)?;
                Value::Real(f64::from_bits(u64::from_be_bytes(b.try_into().unwrap())))
            }
            Self::Text(s) => Value::Text(s),
            Self::Blob(s) => Value::Blob(unhex(&s)?),
        };
        v.validate().map_err(Error::Invalid)?;
        Ok(v)
    }
    fn from(v: Value) -> Self {
        match v {
            Value::Null => Self::Null,
            Value::Integer(n) => Self::Integer(n.to_string()),
            Value::Real(n) => Self::Real(format!("{:016x}", n.to_bits())),
            Value::Text(s) => Self::Text(s),
            Value::Blob(b) => Self::Blob(hex(&b)),
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Statement {
    sql: String,
    #[serde(default)]
    params: Vec<Typed>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Batch {
    statements: Vec<Statement>,
    #[serde(default)]
    attachments: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Resolution {
    table: u32,
    key: String,
    observed: Vec<String>,
    row: Option<Vec<Typed>>,
    #[serde(default)]
    attachments: Vec<String>,
}
fn input<T: serde::de::DeserializeOwned>() -> Result<T> {
    let mut b = Vec::new();
    std::io::stdin()
        .take((MAX_ENVELOPE + 1) as u64)
        .read_to_end(&mut b)?;
    if b.len() > MAX_ENVELOPE {
        return Err(Error::Invalid("input limit"));
    }
    serde_json::from_slice(&b).map_err(|_| Error::Invalid("JSON input"))
}
fn safe(p: &Path) -> Result<()> {
    let mut path = PathBuf::new();
    for part in p.components() {
        if matches!(part, std::path::Component::ParentDir) {
            return Err(Error::Invalid("path"));
        }
        path.push(part);
        match std::fs::symlink_metadata(&path) {
            Ok(m) if m.file_type().is_symlink() => return Err(Error::Invalid("symlink")),
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}
fn save(root: &Path, c: &Config) -> Result<()> {
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join("config.json"))?;
    f.write_all(&serde_json::to_vec(c).map_err(|_| Error::Invalid("config"))?)?;
    f.sync_all()?;
    File::open(root)?.sync_all()?;
    Ok(())
}
fn status(s: &Sdk) -> Result<Json> {
    let st = s.status()?;
    Ok(
        json!({"pending_outbox":st.pending_outbox.to_string(),"healthy":st.healthy(),"blocked_streams":st.stopped_streams.to_string(),"outstanding_objects":st.outstanding_objects.to_string(),"conflicts":s.list_conflicts()?.len().to_string()}),
    )
}
async fn run() -> Result<Json> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let cancel = Cancellation::default();
    if let Some(i) = args.iter().position(|s| s == "--cancel") {
        args.remove(i);
        cancel.cancel();
    }
    if let Some(i) = args.iter().position(|s| s.starts_with("--timeout-ms=")) {
        let value = args.remove(i);
        let millis = value
            .trim_start_matches("--timeout-ms=")
            .parse::<u64>()
            .map_err(|_| Error::Invalid("timeout"))?;
        if millis == 0 || millis > 120000 {
            return Err(Error::Invalid("timeout"));
        }
        let token = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
            token.cancel();
        });
    }
    cancel.check().map_err(Error::Network)?;
    let command = args.first().ok_or(Error::Invalid("command"))?;
    if command == "doctor" {
        let report = check_sqlite_compatibility().inspect_err(|e| {
            eprintln!("SQLite compatibility check failed: {e}");
        })?;
        return Ok(json!({
            "sqlite_version": report.runtime.sqlite_version,
            "session": report.runtime.session,
            "preupdate_hook": report.runtime.preupdate_hook,
            "controlled_capture": report.controlled_capture,
            "rollback": report.rollback,
        }));
    }
    if command == "help" {
        return Ok(
            json!({"usage":"lfs help|diagnostics|doctor; lfs COMMAND DEVICE_DIR [ARG]; lfs restore BACKUP_DIR NEW_DIR", "commands":["init (stdin config)","config","status","read (stdin statement)","write (stdin batch)","sync-once","conflicts","resolve (stdin resolution)","attachment-add FILE","attachment-download HASH","backup DEST [local|shared]","restore BACKUP NEW_DIR (stdin backend)","diagnostics","doctor"],"typed":"integer decimal string / real 16 hex f64 bits / blob lowercase hex"}),
        );
    }
    if command == "diagnostics" {
        return Ok(
            json!({"sdk":"0.1.0","record_backends":{"Fs":"LOCAL_TESTED","WebDAV":"LOCAL_SERVICE_TESTED","S3":"LOCAL_SERVICE_TESTED_BASIC_ONLY","Drive":"DEFERRED;PROTOTYPE_NOT_ACCEPTED;REAL_DRIVE_UNVERIFIED"},"attachment_backends":{"Fs":"LOCAL_TESTED","WebDAV":"UNSUPPORTED","S3":"LOCAL_SERVICE_TESTED_MULTIPART_ONLY","Drive":"NOT_IMPLEMENTED"},"cli_backends":["Fs","WebDAV","S3"],"deferred_cli_backends":["GoogleDrive"],"diagnostic_scope":"STATIC_METADATA_NOT_RUNTIME_VERIFICATION;USE_DOCTOR_FOR_SQLITE", "sqlite_engine":"HOST_PROVIDED_DEFAULT;BUNDLED_OPT_IN", "storage_adapter":"OPTIONAL_DEFAULT_EMPTY;CLI_SELECTS_FACTORIES", "node_host_engine":"NOT_ACCEPTED;EXISTING_ARTIFACT_NOT_REBUILT","node":"NATIVE_LINUX_NODE22_FS_LOCAL_TESTED","android":"PLANNED","ios":"PLANNED","browser":"EXCLUDED"}),
        );
    }
    if ![
        "init",
        "config",
        "status",
        "read",
        "write",
        "sync-once",
        "conflicts",
        "resolve",
        "attachment-add",
        "attachment-download",
        "backup",
        "restore",
    ]
    .contains(&command.as_str())
    {
        return Err(Error::Network(NetworkError::Unsupported));
    }
    let root = Path::new(args.get(1).ok_or(Error::Invalid("directory"))?);
    safe(root)?;
    if command == "restore" {
        let dest = Path::new(args.get(2).ok_or(Error::Invalid("destination"))?);
        safe(dest)?;
        let backend: Backend = input()?;
        backend.validate()?;
        let s = Sdk::restore(root, dest, &cancel)?;
        let id = s.identity()?;
        let cfg = Config {
            identity: [id.dataset, id.device, id.incarnation],
            schema: s.schema()?,
            backend,
        };
        save(dest, &cfg)?;
        return Ok(json!({"restored":true,"incarnation":cfg.identity[2],"status":status(&s)?}));
    }
    if command == "init" {
        let cfg: Config = input()?;
        cfg.schema.validate()?;
        cfg.backend.validate()?;
        let id = Identity {
            dataset: cfg.identity[0].clone(),
            device: cfg.identity[1].clone(),
            incarnation: cfg.identity[2].clone(),
        };
        id.validate().map_err(Error::Invalid)?;
        std::fs::create_dir(root)?;
        let result = (|| {
            let s = Sdk::open(
                root.join("store.db"),
                id,
                cfg.schema.clone(),
                Baseline::Reject,
            )?;
            save(root, &cfg)?;
            status(&s)
        })();
        if result.is_err() {
            let _ = std::fs::remove_dir_all(root);
        }
        return result;
    }
    let cp = root.join("config.json");
    safe(&cp)?;
    if std::fs::metadata(&cp)?.len() > 1024 * 1024 {
        return Err(Error::Invalid("config limit"));
    }
    let cfg: Config =
        serde_json::from_slice(&std::fs::read(cp)?).map_err(|_| Error::Invalid("config"))?;
    if command == "config" {
        return Ok(
            json!({"identity":cfg.identity,"schema":cfg.schema,"backend":cfg.backend.kind()}),
        );
    }
    let id = Identity {
        dataset: cfg.identity[0].clone(),
        device: cfg.identity[1].clone(),
        incarnation: cfg.identity[2].clone(),
    };
    let s = Sdk::open(root.join("store.db"), id, cfg.schema, Baseline::Reject)?;
    match command.as_str() {
        "status" => status(&s),
        "read" => {
            let q: Statement = input()?;
            let params = q
                .params
                .into_iter()
                .map(Typed::value)
                .collect::<Result<Vec<_>>>()?;
            let rows = s.read(|r| r.query(&q.sql, &params))?;
            Ok(
                json!({"rows":rows.into_iter().map(|r|r.into_iter().map(Typed::from).collect::<Vec<_>>()).collect::<Vec<_>>()}),
            )
        }
        "write" => {
            let b: Batch = input()?;
            if b.statements.len() > 1000 {
                return Err(Error::Invalid("batch limit"));
            }
            let statements = b
                .statements
                .into_iter()
                .map(|q| {
                    Ok((
                        q.sql,
                        q.params
                            .into_iter()
                            .map(Typed::value)
                            .collect::<Result<Vec<_>>>()?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            let (_, commit) = s.write_transaction_with_attachments(&b.attachments, |w| {
                for (sql, params) in &statements {
                    cancel.check().map_err(Error::Network)?;
                    w.execute(sql, params)?;
                }
                Ok(())
            })?;
            Ok(json!({"committed":commit.is_some(),"status":status(&s)?}))
        }
        "sync-once" => {
            let t = cfg.backend.transport()?;
            s.sync_once(&t, &cancel, SyncLimits::default()).await?;
            let st = s.status()?;
            if !st.healthy() || st.pending_outbox != 0 {
                return Err(Error::MissingDependencies);
            }
            status(&s)
        }
        "conflicts" => {
            let list=s.list_conflicts()?.into_iter().map(|c|json!({"table":c.table,"key":c.key,"heads":c.heads.into_iter().map(|o|json!({"id":hex(&o.id.encode()),"row":o.row.map(|r|r.into_iter().map(Typed::from).collect::<Vec<_>>())})).collect::<Vec<_>>()})).collect::<Vec<_>>();
            Ok(json!({"conflicts":list}))
        }
        "resolve" => {
            let r: Resolution = input()?;
            let ids = r
                .observed
                .iter()
                .map(|s| lfs_protocol::OpId::decode(&unhex(s)?).map_err(Error::Invalid))
                .collect::<Result<Vec<_>>>()?;
            let row = r
                .row
                .map(|r| r.into_iter().map(Typed::value).collect::<Result<Vec<_>>>())
                .transpose()?;
            s.resolve_with_attachments(r.table, &r.key, ids, row, &r.attachments)?;
            status(&s)
        }
        "attachment-add" => {
            let p = Path::new(args.get(2).ok_or(Error::Invalid("file"))?);
            safe(p)?;
            let a = s.add_attachment(File::open(p)?, attachments::DEFAULT_LIMIT, &cancel)?;
            Ok(json!({"hash":a.hash,"size":a.size.to_string(),"state":"Available"}))
        }
        "attachment-download" => {
            let h = args.get(2).ok_or(Error::Invalid("hash"))?;
            let t = cfg.backend.transport()?;
            let a = s
                .download_attachment(&t, h, attachments::DEFAULT_LIMIT, &cancel)
                .await?;
            Ok(json!({"hash":a.hash,"size":a.size.to_string(),"state":"Available"}))
        }
        "backup" => {
            let dest = Path::new(args.get(2).ok_or(Error::Invalid("destination"))?);
            let kind = match args.get(3).map(String::as_str).unwrap_or("local") {
                "local" => BackupKind::LocalDisaster,
                "shared" => BackupKind::SharedBootstrap,
                _ => return Err(Error::Invalid("backup kind")),
            };
            let m = s.backup(dest, kind, BackupOptions::default(), &cancel)?;
            Ok(json!({"manifest":m}))
        }
        _ => Err(Error::Network(NetworkError::Unsupported)),
    }
}
fn classify(e: &Error) -> (i32, &'static str) {
    match e {
        Error::ConflictRequiresResolution { .. } => (3, "conflict"),
        Error::Busy | Error::DatabaseInUse | Error::SyncInProgress => (4, "busy"),
        Error::Network(NetworkError::Auth) => (5, "auth"),
        Error::Network(
            NetworkError::Transient
            | NetworkError::Timeout
            | NetworkError::RateLimit
            | NetworkError::LostAck,
        ) => (6, "transient"),
        Error::MissingDependencies | Error::StreamFork => (7, "blocked"),
        Error::Network(NetworkError::Cancelled) => (8, "cancelled"),
        Error::Network(NetworkError::Unsupported) => (9, "unsupported"),
        Error::Invalid("shared backup unpublished or blocked") => (7, "blocked"),
        Error::Sqlite(rusqlite::Error::SqliteFailure(code, _))
            if matches!(
                code.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            ) =>
        {
            (4, "busy")
        }
        _ => (2, "invalid"),
    }
}
#[tokio::main]
async fn main() {
    match run().await {
        Ok(v) => println!("{}", json!({"ok":true,"result":v})),
        Err(e) => {
            let (code, kind) = classify(&e);
            println!("{}", json!({"ok":false,"error":{"kind":kind,"code":code}}));
            std::process::exit(code)
        }
    }
}
