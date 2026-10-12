#![cfg(feature = "webdav")]
mod common;
use common::*;
use lfs_sqlite::{Cancellation, Transport};
use lfs_storage::{OpenDalTransport, Options, WebdavCredentials};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

struct Server {
    endpoint: String,
    stop: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl Server {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}/dav/", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let end = stop.clone();
        let worker = std::thread::spawn(move || {
            let mut entries = BTreeMap::from([("/dav/".to_owned(), Vec::new())]);
            while !end.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => serve(stream, &mut entries),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2))
                    }
                    Err(e) => panic!("{e}"),
                }
            }
        });
        Self {
            endpoint,
            stop,
            worker: Some(worker),
        }
    }
    fn transport(&self) -> OpenDalTransport {
        OpenDalTransport::webdav_with_directory(
            &self.endpoint,
            "new/book",
            WebdavCredentials::Bearer("fixture".into()),
            Options {
                allow_loopback_http: true,
                ..Default::default()
            },
        )
        .unwrap()
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.worker.take().unwrap().join().unwrap();
    }
}
fn serve(mut stream: TcpStream, entries: &mut BTreeMap<String, Vec<u8>>) {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut input = Vec::new();
    let end = loop {
        let mut byte = [0];
        stream.read_exact(&mut byte).unwrap();
        input.push(byte[0]);
        if input.ends_with(b"\r\n\r\n") {
            break input.len();
        }
    };
    let headers = String::from_utf8(input.clone()).unwrap();
    let mut first = headers.lines().next().unwrap().split_whitespace();
    let method = first.next().unwrap();
    let path = first.next().unwrap();
    let length = headers
        .lines()
        .find_map(|line| {
            line.to_lowercase()
                .strip_prefix("content-length: ")
                .map(|s| s.parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    input.resize(end + length, 0);
    stream.read_exact(&mut input[end..]).unwrap();
    let parent = path
        .trim_end_matches('/')
        .rsplit_once('/')
        .unwrap()
        .0
        .to_owned()
        + "/";
    let (status, body) = if !entries.contains_key(&parent) && !entries.contains_key(path) {
        (409, Vec::new())
    } else {
        match method {
            "MKCOL" => {
                let old = entries.insert(path.into(), Vec::new());
                (if old.is_some() { 405 } else { 201 }, Vec::new())
            }
            "PUT" => {
                entries.insert(path.into(), input[end..].to_vec());
                (201, Vec::new())
            }
            "GET" => match entries.get(path) {
                Some(bytes) => (200, bytes.clone()),
                None => (404, Vec::new()),
            },
            "PROPFIND" if entries.contains_key(path) => {
                let list = headers.lines().any(|l| l.eq_ignore_ascii_case("depth: 1"));
                let mut xml =
                    String::from("<?xml version=\"1.0\"?><d:multistatus xmlns:d=\"DAV:\">");
                for (name, bytes) in entries.iter() {
                    let child = name
                        .strip_prefix(path)
                        .is_some_and(|p| !p.is_empty() && !p.trim_end_matches('/').contains('/'));
                    if name == path || (list && child) {
                        xml += &format!(
                            "<d:response><d:href>{name}</d:href><d:propstat><d:prop><d:getlastmodified>Mon, 12 Oct 2026 00:00:00 GMT</d:getlastmodified><d:resourcetype>{}</d:resourcetype><d:getcontentlength>{}</d:getcontentlength></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>",
                            if name.ends_with('/') {
                                "<d:collection/>"
                            } else {
                                ""
                            },
                            bytes.len()
                        );
                    }
                }
                xml += "</d:multistatus>";
                (207, xml.into_bytes())
            }
            "PROPFIND" => (404, Vec::new()),
            _ => panic!("unexpected fixture method {method}"),
        }
    };
    write!(
        stream,
        "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .unwrap();
    stream.write_all(&body).unwrap();
}

#[tokio::test]
async fn empty_webdav_and_first_upload_create_missing_parents() {
    let server = Server::new();
    let page = server
        .transport()
        .list("empty", 0, 20, &Cancellation::default())
        .await
        .unwrap();
    assert!(page.complete && page.keys.is_empty() && page.next.is_none());
    let fixture = Fixture::new();
    let a = fixture.sdk("a");
    let b = fixture.sdk("b");
    insert(&a, "first");
    sync(&a, &server.transport()).await;
    sync(&b, &server.transport()).await;
    assert_eq!(rows(&a), rows(&b));
    assert_eq!(rows(&b).len(), 1);
}
