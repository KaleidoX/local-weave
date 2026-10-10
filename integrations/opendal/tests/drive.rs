mod common;
use common::*;

use lfs_storage::{DriveToken, GoogleDriveTransport, Options};

#[tokio::test]
async fn drive_public_record_journey() {
    let model = Model::new();
    let endpoint = model.origin.clone();
    let t = GoogleDriveTransport::owned_loopback(
        "synthetic-root",
        DriveToken::new("synthetic-drive-test-token".into()).unwrap(),
        &endpoint,
        Options::default(),
    )
    .unwrap();
    let f = Fixture::new();
    journey(&f, &t).await;
}

struct Model {
    process: std::process::Child,
    root: std::path::PathBuf,
    origin: String,
}
impl Model {
    fn new() -> Self {
        let root = std::path::PathBuf::from(std::env::var_os("TMPDIR").unwrap()).join(format!(
            "drive-model-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let process = std::process::Command::new("python3")
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/drive_model.py"
            ))
            .arg("--root")
            .arg(&root)
            .spawn()
            .unwrap();
        let mut model = Self {
            process,
            root,
            origin: String::new(),
        };
        for _ in 0..100 {
            if let Ok(bytes) = std::fs::read(model.root.join("ready.json"))
                && let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes)
            {
                model.origin = v["origin"].as_str().unwrap().into();
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(!model.origin.is_empty(), "MODEL_HTTP readiness");
        use std::io::{Read, Write};
        let mut socket =
            std::net::TcpStream::connect(model.origin.trim_start_matches("http://")).unwrap();
        socket
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        socket
            .write_all(b"GET /ready HTTP/1.0\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let mut response = String::new();
        socket.read_to_string(&mut response).unwrap();
        assert!(response.contains("200 OK") && response.contains("MODEL_HTTP"));
        println!(
            "MODEL_HTTP owned PID={} origin={} ready=true",
            model.process.id(),
            model.origin
        );
        model
    }
}
impl Drop for Model {
    fn drop(&mut self) {
        self.process.kill().unwrap();
        let status = self.process.wait().unwrap();
        println!("MODEL_HTTP owned PID={} reaped={status}", self.process.id());
        if let Some(dest) = std::env::var_os("LFS_DRIVE_EVIDENCE") {
            let dest = std::path::PathBuf::from(dest).join(self.root.file_name().unwrap());
            std::fs::create_dir_all(&dest).unwrap();
            for item in std::fs::read_dir(&self.root).unwrap() {
                let item = item.unwrap();
                std::fs::copy(item.path(), dest.join(item.file_name())).unwrap();
            }
        }
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

impl Model {
    fn transport(&self, options: Options) -> GoogleDriveTransport {
        GoogleDriveTransport::owned_loopback(
            "synthetic-root",
            DriveToken::new("synthetic-drive-test-token".into()).unwrap(),
            &self.origin,
            options,
        )
        .unwrap()
    }
    async fn control(&self, v: serde_json::Value) {
        let response = reqwest::Client::new()
            .post(format!("{}/control", self.origin))
            .body(v.to_string())
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
    }
    async fn fixture(&self, v: serde_json::Value) -> serde_json::Value {
        let b = reqwest::Client::new()
            .post(format!("{}/fixture", self.origin))
            .body(v.to_string())
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        serde_json::from_slice(&b).unwrap()
    }
    fn manifest(&self) -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(self.root.join("manifest.json")).unwrap()).unwrap()
    }
}
use lfs_sqlite::*;
use serde_json::json;

#[tokio::test]
async fn drive_slow_bad_candidate_does_not_starve_valid_duplicate() {
    let model = Model::new();
    let f = Fixture::new();
    let a = f.sdk("a");
    insert(&a, "k");
    let p = a.pending_envelopes(1).unwrap().remove(0);
    let c = Cancellation::default();
    let t = model.transport(Options::default());
    t.upload(p.clone(), &c).await.unwrap();
    let files = model.manifest();
    let original = files
        .as_object()
        .unwrap()
        .values()
        .find(|m| m.get("size").is_some())
        .unwrap();
    model.fixture(json!({"clone":original["id"]})).await;
    model.control(json!({"slow_id":original["id"]})).await;
    let t = model.transport(Options {
        timeout: std::time::Duration::from_millis(500),
        ..Default::default()
    });
    assert_eq!(
        t.download(&p.key, &c)
            .await
            .unwrap()
            .encode_envelope()
            .unwrap(),
        p.bytes
    );
}

#[tokio::test]
async fn drive_duplicates_paging_parents_reopen_and_immutable_lost_ack() {
    let model = Model::new();
    let f = Fixture::new();
    let a = f.sdk("a");
    insert(&a, "k");
    let p = a.pending_envelopes(1).unwrap().remove(0);
    let c = Cancellation::default();
    model.control(json!({"lost_ack":true,"page_size":1,"empty_page":true,"repeat_id":true,"foreign":true})).await;
    let t = model.transport(Options::default());
    assert_eq!(t.upload(p.clone(), &c).await, Err(NetworkError::Transient));
    a.close().unwrap();
    drop(a);
    drop(t);
    let a = f.sdk("a");
    assert_eq!(a.pending_envelopes(1).unwrap()[0], p);
    let t = model.transport(Options::default());
    t.upload(p.clone(), &c).await.unwrap();
    let manifest = model.manifest();
    let file = manifest
        .as_object()
        .unwrap()
        .values()
        .find(|m| m.get("size").is_some())
        .unwrap()
        .clone();
    assert_eq!(
        manifest
            .as_object()
            .unwrap()
            .values()
            .filter(|m| m.get("size").is_some())
            .count(),
        1
    );
    model
        .fixture(json!({"clone":file["id"],"corrupt":true}))
        .await;
    model.fixture(json!({"clone":file["id"]})).await;
    let inc = file["parents"][0].as_str().unwrap();
    let duplicate_dir = model.fixture(json!({"clone":inc})).await;
    model
        .fixture(json!({"clone":file["id"],"overrides":{"parents":[duplicate_dir["id"]]}}))
        .await;
    model.fixture(json!({"clone":file["id"],"overrides":{"mimeType":"application/vnd.google-apps.shortcut"}})).await;
    let trashed = model.fixture(json!({"clone":file["id"]})).await;
    model
        .fixture(json!({"edit":trashed["id"],"overrides":{"trashed":true}}))
        .await;
    let before = model.manifest().as_object().unwrap().len();
    t.upload(p.clone(), &c).await.unwrap();
    assert_eq!(before, model.manifest().as_object().unwrap().len());
    assert_eq!(
        t.download(&p.key, &c)
            .await
            .unwrap()
            .encode_envelope()
            .unwrap(),
        p.bytes
    );
    let list = t.list(&identity("a").dataset, 0, 1, &c).await.unwrap();
    assert_eq!(list.keys, vec![p.key.clone()]);
    sync(&a, &t).await;
    let b = f.sdk("b");
    sync(&b, &t).await;
    assert_eq!(rows(&a), rows(&b));
    assert!(b.pending_envelopes(10).unwrap().is_empty());
    // All adapter state lost: a persisted nonzero cursor must replay from the beginning.
    let fresh = model.transport(Options::default());
    assert_eq!(
        fresh
            .list(&identity("a").dataset, 900, 1, &c)
            .await
            .unwrap()
            .keys,
        vec![p.key.clone()]
    );
    let requests = std::fs::read_to_string(model.root.join("requests.jsonl")).unwrap();
    assert!(!requests.contains("PATCH") && !requests.contains("DELETE"));
    assert!(
        !requests.contains("/files/foreign")
            && !requests.contains(&format!("/files/{}", trashed["id"].as_str().unwrap()))
    );
    println!(
        "MODEL_HTTP raw ID manifest / exact bytes / empty and short token pages / duplicate directories / lost ACK + reopen PASS"
    );
}

#[tokio::test]
async fn drive_root_status_limits_cancellation_and_attachment_fail_closed() {
    let model = Model::new();
    let t = model.transport(Options::default());
    let c = Cancellation::default();
    for (status, reason, error) in [
        (401, "", NetworkError::Auth),
        (403, "forbidden", NetworkError::Auth),
        (403, "rateLimitExceeded", NetworkError::RateLimit),
        (403, "userRateLimitExceeded", NetworkError::RateLimit),
        (429, "", NetworkError::RateLimit),
        (503, "", NetworkError::Transient),
        (404, "", NetworkError::Missing),
        (302, "", NetworkError::Unsupported),
    ] {
        model
            .control(json!({"status":status,"reason":reason}))
            .await;
        let e = t.list("data", 0, 1, &c).await.unwrap_err();
        assert_eq!(e, error);
        assert!(!format!("{e:?}").contains("synthetic"));
    }
    model.control(json!({"status":0,"incomplete":true})).await;
    assert_eq!(
        t.list("data", 0, 1, &c).await.unwrap_err(),
        NetworkError::Transient
    );
    model
        .control(json!({"incomplete":false,"cycle":true}))
        .await;
    assert_eq!(
        t.list("data", 0, 1, &c).await.unwrap_err(),
        NetworkError::Corrupt
    );
    model
        .control(json!({"cycle":false,"token":"a".repeat(2049)}))
        .await;
    assert_eq!(
        t.list("data", 0, 1, &c).await.unwrap_err(),
        NetworkError::Corrupt
    );
    model.control(json!({"token":null})).await;
    model
        .fixture(json!({"edit":"synthetic-root","overrides":{"trashed":true}}))
        .await;
    assert_eq!(
        t.list("data", 0, 1, &c).await.unwrap_err(),
        NetworkError::Corrupt
    );
    model.fixture(json!({"edit":"synthetic-root","overrides":{"trashed":false,"mimeType":"application/octet-stream"}})).await;
    assert_eq!(
        t.list("data", 0, 1, &c).await.unwrap_err(),
        NetworkError::Corrupt
    );
    model
        .fixture(
            json!({"edit":"synthetic-root","overrides":{"mimeType":"application/vnd.google-apps.folder"}}),
        )
        .await;
    model.control(json!({"delay":0.2})).await;
    let cancel = Cancellation::default();
    let signal = cancel.clone();
    let call = t.list("data", 0, 1, &cancel);
    let trigger = async move {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        signal.cancel();
    };
    let (result, ()) = tokio::join!(call, trigger);
    assert_eq!(result.unwrap_err(), NetworkError::Cancelled);
    model.control(json!({"delay":0})).await;
    let f = Fixture::new();
    let a = f.sdk("attach");
    let attachment = a
        .add_attachment(&b"synthetic attachment"[..], 100, &c)
        .unwrap();
    a.write_transaction_with_attachments(&[attachment.hash], |w| {
        w.execute("INSERT INTO items VALUES ('k','body',1,1.0,x'',NULL)", &[])
    })
    .unwrap();
    let original = a.pending_envelopes(1).unwrap();
    assert!(matches!(
        a.sync_once(&t, &c, SyncLimits::default()).await,
        Err(Error::Network(NetworkError::Unsupported))
    ));
    assert_eq!(a.pending_envelopes(1).unwrap(), original);
    assert!(
        model
            .manifest()
            .as_object()
            .unwrap()
            .values()
            .all(|m| m.get("size").is_none())
    );
    for token in ["", "x\r\nAuthorization: injected", "a b", "é"] {
        assert!(DriveToken::new(token.into()).is_err());
    }
    assert!(
        GoogleDriveTransport::owned_loopback(
            "synthetic-root",
            DriveToken::new("synthetic".into()).unwrap(),
            "http://example.com",
            Options::default()
        )
        .is_err()
    );
    assert!(
        GoogleDriveTransport::new(
            "../root",
            DriveToken::new("synthetic".into()).unwrap(),
            Options::default()
        )
        .is_err()
    );
    assert_eq!(
        t.list("", 0, 1, &c).await.unwrap_err(),
        NetworkError::Corrupt
    );
    assert_eq!(
        t.list(&"x".repeat(129), 0, 1, &c).await.unwrap_err(),
        NetworkError::Corrupt
    );
}

#[tokio::test]
async fn drive_valid_fork_and_bad_candidate_allow_healthy_stream() {
    let model = Model::new();
    let f = Fixture::new();
    let other = Fixture::new();
    let c = Cancellation::default();
    let a = f.sdk("fork");
    let clone = other.sdk("fork");
    let healthy = f.sdk("healthy");
    let b = f.sdk("reader");
    insert(&a, "left");
    insert(&clone, "right");
    insert(&healthy, "healthy");
    let t = model.transport(Options::default());
    for s in [&a, &clone, &healthy] {
        t.upload(s.pending_envelopes(1).unwrap().remove(0), &c)
            .await
            .unwrap();
    }
    let report = b.sync_once(&t, &c, SyncLimits::default()).await.unwrap();
    assert!(!report.healthy());
    assert_eq!(report.status.stopped_streams, 1);
    assert!(
        rows(&b)
            .iter()
            .any(|r| r[0] == Value::Text("healthy".into()))
    );
    assert!(b.pending_envelopes(10).unwrap().is_empty());
    // A corrupt-only filename is evidence, not a valid fork; another stream still applies.
    let clean = Fixture::new();
    let broken = clean.sdk("bad");
    insert(&broken, "bad");
    let bad = broken.pending_envelopes(1).unwrap().remove(0);
    t.upload(bad.clone(), &c).await.unwrap();
    let m = model.manifest();
    let name = lfs_storage::object_path(&bad.key).unwrap();
    let name = name.rsplit('/').next().unwrap();
    let entry = m
        .as_object()
        .unwrap()
        .values()
        .find(|m| m["name"] == name)
        .unwrap();
    std::fs::write(model.root.join(entry["id"].as_str().unwrap()), b"bad").unwrap();
    model
        .fixture(json!({"edit":entry["id"],"overrides":{"size":"3"}}))
        .await;
    insert(&healthy, "healthy2");
    let healthy_report = healthy
        .sync_once(&t, &c, SyncLimits::default())
        .await
        .unwrap();
    assert_eq!(healthy_report.uploaded, 2);
    assert_eq!(healthy_report.status.stopped_streams, 1);
    let result = b.sync_once(&t, &c, SyncLimits::default()).await.unwrap();
    assert_eq!(result.status.stopped_streams, 1);
    assert!(!b.failed_attempts().unwrap().is_empty());
    assert!(
        rows(&b)
            .iter()
            .any(|r| r[0] == Value::Text("healthy2".into()))
    );
    assert!(!rows(&b).iter().any(|r| r[0] == Value::Text("bad".into())));
}

#[tokio::test]
async fn drive_untrusted_create_and_chunked_bounds_keep_exact_pending() {
    let model = Model::new();
    let f = Fixture::new();
    let a = f.sdk("a");
    insert(&a, "k");
    let c = Cancellation::default();
    let t = model.transport(Options::default());
    let pending = a.pending_envelopes(1).unwrap();
    model.control(json!({"wrong_create_parent":true})).await;
    assert_eq!(
        t.upload(pending[0].clone(), &c).await,
        Err(NetworkError::Corrupt)
    );
    assert_eq!(a.pending_envelopes(1).unwrap(), pending);
    model.control(json!({"wrong_create_parent":false})).await;
    sync(&a, &t).await;
    insert(&a, "second");
    let pending = a.pending_envelopes(1).unwrap();
    model.control(json!({"corrupt_create":true})).await;
    assert_eq!(
        t.upload(pending[0].clone(), &c).await,
        Err(NetworkError::Corrupt)
    );
    assert_eq!(a.pending_envelopes(1).unwrap(), pending);
    model.control(json!({"corrupt_create":false})).await;
    sync(&a, &t).await;
    for omit in [false, true] {
        model
            .control(json!({"huge_json":true,"omit_length":omit}))
            .await;
        assert_eq!(
            t.list("data", 0, 1, &c).await.unwrap_err(),
            NetworkError::Limit
        );
    }
    model
        .control(json!({"huge_json":false,"omit_length":false}))
        .await;
    let m = model.manifest();
    let file = m
        .as_object()
        .unwrap()
        .values()
        .find(|m| m.get("size").is_some())
        .unwrap();
    model.control(json!({"missing_id":file["id"]})).await;
    let b = f.sdk("b");
    let report = b.sync_once(&t, &c, SyncLimits::default()).await.unwrap();
    assert!(!report.healthy());
    model.control(json!({"missing_id":null})).await;
    sync(&b, &t).await;
    assert_eq!(rows(&a), rows(&b));
    let before = model.manifest().as_object().unwrap().len();
    let mut invalid = pending[0].clone();
    invalid.bytes[0] ^= 1;
    assert_eq!(t.upload(invalid, &c).await, Err(NetworkError::Corrupt));
    assert_eq!(before, model.manifest().as_object().unwrap().len());
}
