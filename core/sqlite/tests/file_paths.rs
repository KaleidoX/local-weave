//! Mandated URI/R0 public API regressions, with real SQLite and isolated child cwd.
use lfs_sqlite::*;
use std::{collections::BTreeMap, ffi::OsString, path::Path, process::Command};

fn identity() -> Identity {
    Identity {
        dataset: "paths".into(),
        device: "device".into(),
        incarnation: "first".into(),
    }
}
fn schema() -> Schema {
    Schema {
        version: 1,
        tables: vec![Table {
            id: 1,
            name: "items".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    kind: ColumnType::Text,
                    nullable: false,
                },
                Column {
                    name: "value".into(),
                    kind: ColumnType::Text,
                    nullable: false,
                },
            ],
        }],
    }
}
fn tree(root: &Path) -> BTreeMap<std::path::PathBuf, Option<Vec<u8>>> {
    fn visit(root: &Path, dir: &Path, out: &mut BTreeMap<std::path::PathBuf, Option<Vec<u8>>>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            let value = if path.is_dir() {
                None
            } else {
                Some(std::fs::read(&path).unwrap())
            };
            out.insert(path.strip_prefix(root).unwrap().to_owned(), value);
            if path.is_dir() {
                visit(root, &path, out);
            }
        }
    }
    let mut out = BTreeMap::new();
    visit(root, root, &mut out);
    out
}
fn isolated(name: &str, body: impl FnOnce()) {
    if std::env::var("LFS_FILE_PATH_CHILD").as_deref() == Ok(name) {
        body();
        return;
    }
    let root = std::env::temp_dir().join(format!("lfs-paths-{}-{name}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let result = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env("LFS_FILE_PATH_CHILD", name)
        .current_dir(&root)
        .status()
        .unwrap();
    std::fs::remove_dir_all(root).unwrap();
    assert!(result.success(), "isolated {name}: {result}");
}
fn rejected<T>(result: Result<T>, before: &BTreeMap<std::path::PathBuf, Option<Vec<u8>>>) {
    let after = tree(Path::new("."));
    let invalid = matches!(result, Err(Error::Invalid(_)));
    println!(
        "Error::Invalid={invalid}; filesystem_unchanged={}",
        &after == before
    );
    if let Err(error) = &result {
        println!("error={error:?}");
    }
    assert!(invalid, "persistent file contract requires Error::Invalid");
    assert_eq!(
        &after, before,
        "rejected input changed filesystem entries or bytes"
    );
}
#[test]
fn open_memory_uri_rejected_without_side_effects() {
    isolated("open_memory_uri_rejected_without_side_effects", || {
        std::fs::write("sentinel", b"preserve me").unwrap();
        let before = tree(Path::new("."));
        rejected(
            Store::open(
                "file:memory.db?mode=memory&cache=shared",
                identity(),
                schema(),
                Baseline::Reject,
            ),
            &before,
        );
        rejected(
            Sdk::open(
                "file:memory.db?mode=memory&cache=shared",
                identity(),
                schema(),
                Baseline::Reject,
            ),
            &before,
        );
    });
}

#[test]
fn backup_derived_uri_rejected_without_side_effects() {
    isolated("backup_derived_uri_rejected_without_side_effects", || {
        let mut store = Store::open("source.db", identity(), schema(), Baseline::Reject).unwrap();
        std::fs::write("sentinel", b"preserve me").unwrap();
        let before = tree(Path::new("."));
        rejected(
            store.backup(
                Path::new("file:backup"),
                backup::BackupKind::LocalDisaster,
                backup::BackupOptions::default(),
                &Cancellation::default(),
            ),
            &before,
        );
    });
}

#[test]
fn restore_derived_uri_rejected_without_side_effects() {
    isolated("restore_derived_uri_rejected_without_side_effects", || {
        let sdk = Sdk::open("source.db", identity(), schema(), Baseline::Reject).unwrap();
        sdk.backup(
            "./backup",
            backup::BackupKind::LocalDisaster,
            backup::BackupOptions::default(),
            &Cancellation::default(),
        )
        .unwrap();
        // Existing destination makes an early path rejection observable independently of cleanup.
        std::fs::create_dir("file:restore").unwrap();
        std::fs::write("file:restore/sentinel", b"preserve me").unwrap();
        let before = tree(Path::new("."));
        rejected(
            Sdk::restore("backup", "file:restore", &Cancellation::default()),
            &before,
        );
    });
}

fn uri_paths() -> Vec<OsString> {
    let paths = [
        "file:",
        "file:plain.db",
        "file::memory:",
        "file:memory.db?mode=memory&cache=shared",
        "file:plain.db?mode=rwc",
        "file:plain.db?vfs=missing",
        "file:plain.db#fragment",
        "file:/absolute.db",
        "file:///absolute.db",
        "file://localhost/absolute.db",
    ];
    #[allow(unused_mut)]
    let mut paths: Vec<OsString> = paths.into_iter().map(Into::into).collect();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        paths.push(OsString::from_vec(b"file:nonutf8-\xff.db".to_vec()));
    }
    paths
}
#[test]
fn open_path_boundaries() {
    isolated("open_path_boundaries", || {
        // The literal URI and its SQLite target both already contain unrelated user bytes.
        std::fs::write("file:plain.db", b"literal sentinel").unwrap();
        std::fs::write("plain.db", b"target sentinel").unwrap();
        let before = tree(Path::new("."));
        for path in uri_paths()
            .into_iter()
            .chain([OsString::from(""), OsString::from(":memory:")])
        {
            println!("open rejected path={path:?}");
            rejected(
                Store::open(Path::new(&path), identity(), schema(), Baseline::Reject),
                &before,
            );
            rejected(
                Sdk::open(Path::new(&path), identity(), schema(), Baseline::Reject),
                &before,
            );
        }
    });
}
#[test]
fn derived_path_boundaries() {
    isolated("derived_path_boundaries", || {
        let sdk = Sdk::open("source.db", identity(), schema(), Baseline::Reject).unwrap();
        sdk.backup(
            "./backup",
            backup::BackupKind::LocalDisaster,
            backup::BackupOptions::default(),
            &Cancellation::default(),
        )
        .unwrap();
        std::fs::create_dir("file:existing").unwrap();
        std::fs::write("file:existing/sentinel", b"preserve me").unwrap();
        let before = tree(Path::new("."));
        for path in uri_paths()
            .into_iter()
            .chain([OsString::from("file:existing")])
        {
            println!("derived rejected path={path:?}");
            rejected(
                sdk.backup(
                    Path::new(&path),
                    backup::BackupKind::LocalDisaster,
                    backup::BackupOptions::default(),
                    &Cancellation::default(),
                ),
                &before,
            );
            rejected(
                Sdk::restore("./backup", Path::new(&path), &Cancellation::default()),
                &before,
            );
        }
    });
}
fn persistent(path: &Path) {
    let sdk = Sdk::open(path, identity(), schema(), Baseline::Reject).unwrap();
    sdk.write_transaction(|w| w.execute("INSERT INTO items VALUES ('key','persistent')", &[]))
        .unwrap();
    let before = sdk.pending_envelopes(10).unwrap();
    assert_eq!(before.len(), 1);
    drop(sdk);
    assert!(std::fs::metadata(path).unwrap().len() > 0);
    let sdk = Sdk::open(path, identity(), schema(), Baseline::Reject).unwrap();
    assert_eq!(
        sdk.read(|r| r.query("SELECT * FROM items", &[])).unwrap(),
        vec![vec![
            Value::Text("key".into()),
            Value::Text("persistent".into())
        ]]
    );
    assert_eq!(sdk.pending_envelopes(10).unwrap(), before);
}
#[test]
fn ordinary_paths_preserve_rows_and_exact_outbox() {
    isolated("ordinary_paths_preserve_rows_and_exact_outbox", || {
        for path in [
            "relative.db",
            "punctuation?#.db",
            "./file:literal.db?mode=memory&cache=shared",
            "FILE:uppercase.db",
            "./:memory:",
        ] {
            persistent(Path::new(path));
        }
        persistent(
            &std::env::current_dir()
                .unwrap()
                .join("file:absolute.db?mode=memory#literal"),
        );
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            persistent(Path::new(&OsString::from_vec(b"ordinary-\xff.db".to_vec())));
        }
    });
}
#[test]
fn literal_backup_and_source_directory_restore() {
    isolated("literal_backup_and_source_directory_restore", || {
        let sdk = Sdk::open("source.db", identity(), schema(), Baseline::Reject).unwrap();
        sdk.write_transaction(|w| w.execute("INSERT INTO items VALUES ('key','persistent')", &[]))
            .unwrap();
        let pending = sdk.pending_envelopes(10).unwrap();
        assert_eq!(pending.len(), 1);
        let original = sdk.identity().unwrap();
        for (i, destination) in [
            std::path::PathBuf::from("./file:backup?literal#name"),
            std::env::current_dir()
                .unwrap()
                .join("file:absolute-backup"),
        ]
        .into_iter()
        .enumerate()
        {
            sdk.backup(
                &destination,
                backup::BackupKind::LocalDisaster,
                backup::BackupOptions::default(),
                &Cancellation::default(),
            )
            .unwrap();
            // Source is only read through filesystem APIs: its raw file: directory is valid.
            let source = if i == 0 {
                Path::new("file:backup?literal#name")
            } else {
                &destination
            };
            let restored_dir = format!("./file:restored-{i}");
            let restored = Sdk::restore(source, &restored_dir, &Cancellation::default()).unwrap();
            assert_eq!(
                restored
                    .read(|r| r.query("SELECT * FROM items", &[]))
                    .unwrap(),
                vec![vec![
                    Value::Text("key".into()),
                    Value::Text("persistent".into())
                ]]
            );
            assert_eq!(restored.pending_envelopes(10).unwrap(), pending);
            let new_id = restored.identity().unwrap();
            assert_eq!(new_id.dataset, original.dataset);
            assert_eq!(new_id.device, original.device);
            assert_ne!(new_id.incarnation, original.incarnation);
            drop(restored);
            let reopened = Sdk::open(
                Path::new(&restored_dir).join("store.db"),
                new_id,
                schema(),
                Baseline::Reject,
            )
            .unwrap();
            assert_eq!(reopened.pending_envelopes(10).unwrap(), pending);
        }
    });
}
