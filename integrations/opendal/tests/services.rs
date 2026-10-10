//! Real local HTTP tests run explicitly by integrations/opendal/tests/http_services.py, never cloud verification.
mod common;
use common::*;
use lfs_sqlite::{Cancellation, NetworkError, Transport};
use lfs_storage::*;
#[tokio::test]
#[ignore = "requires task-owned rclone server; run integrations/opendal/tests/http_services.py separately for webdav and s3"]
async fn real_local_http_sdk_journey() {
    let endpoint = std::env::var("LFS_TEST_ENDPOINT").expect("test endpoint required");
    let backend = std::env::var("LFS_TEST_BACKEND").unwrap();
    let options = Options {
        allow_loopback_http: true,
        ..Default::default()
    };
    let t = match backend.as_str() {
        "webdav" => OpenDalTransport::webdav(
            &endpoint,
            WebdavCredentials::Basic {
                username: "stage3-test".into(),
                password: "stage3-test-only".into(),
            },
            options,
        ),
        "s3" => OpenDalTransport::s3(
            &endpoint,
            "us-east-1",
            "stage3-test",
            S3Credentials {
                access_key_id: "stage3-test".into(),
                secret_access_key: "stage3-test-only".into(),
                session_token: None,
            },
            options,
        ),
        _ => panic!("unknown test backend"),
    }
    .unwrap();
    let bad = match backend.as_str() {
        "webdav" => OpenDalTransport::webdav(
            &endpoint,
            WebdavCredentials::Basic {
                username: "stage3-test".into(),
                password: "wrong-test-only".into(),
            },
            options,
        ),
        "s3" => OpenDalTransport::s3(
            &endpoint,
            "us-east-1",
            "stage3-test",
            S3Credentials {
                access_key_id: "stage3-test".into(),
                secret_access_key: "wrong-test-only".into(),
                session_token: None,
            },
            options,
        ),
        _ => unreachable!(),
    }
    .unwrap();
    assert!(matches!(
        bad.list("data", 0, 1, &Cancellation::default()).await,
        Err(NetworkError::Auth)
    ));
    println!("real local HTTP bad test credentials: Auth, no retry");
    println!("{backend} declared capabilities: {:?}", t.capabilities());
    let f = Fixture::new();
    journey(&f, &t).await;
    let a = f.sdk("attachment-source");
    let b = f.sdk("attachment-target");
    let cancel = Cancellation::default();
    let bytes = vec![0x6c; 11 * 1024 * 1024 + 39];
    let attachment = a
        .add_attachment(bytes.as_slice(), 16 * 1024 * 1024, &cancel)
        .unwrap();
    if backend == "s3" {
        assert!(t.attachment_streaming_supported());
        a.write_transaction_with_attachments(std::slice::from_ref(&attachment.hash), |w| {
            w.execute(
                "INSERT INTO items VALUES ('attachment','real multipart',7,1.5,x'00ff',NULL)",
                &[],
            )
        })
        .unwrap();
        sync(&a, &t).await;
        sync(&b, &t).await;
        assert_eq!(
            b.attachment_status(&attachment.hash).unwrap().state,
            "Missing"
        );
        b.download_attachment(&t, &attachment.hash, 16 * 1024 * 1024, &cancel)
            .await
            .unwrap();
        let mut content = Vec::new();
        std::io::Read::read_to_end(
            &mut b.open_attachment(&attachment.hash, &cancel).unwrap(),
            &mut content,
        )
        .unwrap();
        assert_eq!(content, bytes);
        println!(
            "real local S3 multipart attachment: 11 MiB + 39, full SHA256 upload readback + durable on-demand download"
        );
    } else {
        assert!(!t.attachment_streaming_supported());
        assert_eq!(
            t.upload_attachment(
                &a.identity().unwrap().dataset,
                &attachment.hash,
                a.open_attachment(&attachment.hash, &cancel).unwrap(),
                16 * 1024 * 1024,
                &cancel
            )
            .await
            .unwrap_err(),
            NetworkError::Unsupported
        );
        println!("WebDAV attachment streaming fails closed; record journey still executed");
    }
}
