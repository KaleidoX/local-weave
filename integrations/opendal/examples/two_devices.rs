//! Public facade + real OpenDAL Fs; all data generated under an explicit TMPDIR.
use lfs_sqlite::backup::{BackupKind, BackupOptions};
use lfs_sqlite::*;
use lfs_storage::{OpenDalTransport, Options};
use std::{io::Read as IoRead, path::PathBuf};
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
fn device(root: &std::path::Path, name: &str) -> Result<Sdk> {
    Sdk::open(
        root.join(format!("{name}.db")),
        Identity {
            dataset: "two-devices".into(),
            device: name.into(),
            incarnation: "initial".into(),
        },
        schema(),
        Baseline::Reject,
    )
}
async fn sync(s: &Sdk, t: &OpenDalTransport, c: &Cancellation) -> Result<()> {
    s.sync_once(t, c, SyncLimits::default()).await?;
    Ok(())
}
#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let base = PathBuf::from(std::env::var_os("TMPDIR").ok_or("explicit TMPDIR required")?);
    let root = base.join(format!("lfs-two-devices-{}", std::process::id()));
    std::fs::create_dir(&root)?;
    let result:Result<()>=async {
        let remote=root.join("remote");std::fs::create_dir(&remote)?;let t=OpenDalTransport::fs(&remote,Options::default()).map_err(Error::Network)?;let c=Cancellation::default();let a=device(&root,"a")?;let b=device(&root,"b")?;
        let body=vec![0x5a;attachments::CHUNK*5+31];let file=a.add_attachment(body.as_slice(),1024*1024,&c)?;
        a.write_transaction_with_attachments(std::slice::from_ref(&file.hash),|w|{w.execute("INSERT INTO notes VALUES (?1,?2)",&[Value::Text("shared".into()),Value::Text("offline a".into())])?;w.execute("INSERT INTO notes VALUES ('deleted','temporary')",&[])?;Ok(())})?;
        b.write_transaction(|w|w.execute("INSERT INTO notes VALUES ('b-only','offline b')",&[]))?;
        sync(&a,&t,&c).await?;sync(&b,&t,&c).await?;sync(&a,&t,&c).await?;
        assert_eq!(b.attachment_status(&file.hash)?.state,"Missing");b.download_attachment(&t,&file.hash,1024*1024,&c).await?;let mut back=Vec::new();b.open_attachment(&file.hash,&c)?.read_to_end(&mut back)?;assert_eq!(back,body);
        a.write_transaction(|w|{w.execute("UPDATE notes SET body='edit a' WHERE id='shared'",&[])?;w.execute("DELETE FROM notes WHERE id='deleted'",&[])})?;
        b.write_transaction(|w|w.execute("UPDATE notes SET body='edit b' WHERE id='shared'",&[]))?;
        sync(&a,&t,&c).await?;sync(&b,&t,&c).await?;sync(&a,&t,&c).await?;
        let conflicts=a.list_conflicts()?;assert_eq!(conflicts.len(),1);assert_eq!(conflicts[0].heads.len(),2);
        a.resolve(1,"shared",conflicts[0].heads.iter().map(|o|o.id.clone()).collect(),Some(vec![Value::Text("shared".into()),Value::Text("explicit full row".into())]))?;
        sync(&a,&t,&c).await?;sync(&b,&t,&c).await?;assert!(b.list_conflicts()?.is_empty());
        let read=|s:&Sdk|s.read(|r|r.query("SELECT * FROM notes ORDER BY id",&[]));assert_eq!(read(&a)?,read(&b)?);assert_eq!(read(&a)?.len(),2);
        let backup=root.join("backup");a.backup(&backup,BackupKind::SharedBootstrap,BackupOptions::default(),&c)?;let restored=Sdk::restore(&backup,root.join("restored"),&c)?;assert_eq!(read(&a)?,read(&restored)?);assert_ne!(a.identity()?.incarnation,restored.identity()?.incarnation);
        println!("PASS public SDK + OpenDAL Fs: offline insert/edit/delete, streamed attachment, on-demand verified content, full-row conflict/observed resolution, equal readback, shared backup/restore");Ok(())
    }.await;
    std::fs::remove_dir_all(root)?;
    result?;
    Ok(())
}
