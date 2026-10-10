#!/usr/bin/env python3
"""Synthetic Drive-shaped HTTP fixture, never a Google compatibility test."""

import argparse
import json
import pathlib
import re
import threading
import time
import urllib.parse
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from email.parser import BytesParser
from email.policy import default

p = argparse.ArgumentParser()
p.add_argument("--root", required=True)
a = p.parse_args()
root = pathlib.Path(a.root)
root.mkdir(exist_ok=True)
FOLDER = "application/vnd.google-apps.folder"
objects = {
    "synthetic-root": dict(id="synthetic-root", name="dedicated", mimeType=FOLDER, trashed=False)
}
serial = 0
lock = threading.RLock()
controls = {}


def save():
    (root / "manifest.json").write_text(json.dumps(objects, indent=2, sort_keys=True))


def create(meta, body=None):
    global serial
    serial += 1
    ident = f"id{serial:06d}"
    m = dict(meta, id=ident, trashed=False)
    if body is not None:
        m["size"] = str(len(body))
        (root / ident).write_bytes(body)
    objects[ident] = m
    save()
    return m


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def reply(self, status, data, binary=False):
        data = data if binary else json.dumps(data).encode()
        self.send_response(status)
        if not controls.get("omit_length"):
            self.send_header("Content-Length", str(len(data)))
        if status == 302:
            self.send_header("Location", "https://invalid.example/never-follow")
        self.end_headers()
        try:
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        self.handle_request()

    def do_POST(self):
        self.handle_request()

    def do_PATCH(self):
        self.reply(405, {})

    def do_DELETE(self):
        self.reply(405, {})

    def handle_request(self):
        u = urllib.parse.urlsplit(self.path)
        q = urllib.parse.parse_qs(u.query)
        if u.path == "/ready":
            return self.reply(200, {"model": "MODEL_HTTP"})
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        with lock:
            with (root / "requests.jsonl").open("a") as f:
                f.write(
                    json.dumps(
                        dict(
                            method=self.command,
                            path=u.path,
                            query=q,
                            body_bytes=len(body),
                            auth_synthetic=self.headers.get("Authorization")
                            == "Bearer synthetic-drive-test-token",
                        )
                    )
                    + "\n"
                )
            if u.path == "/control":
                changes = json.loads(body)
                controls.update(changes)
                return self.reply(200, {})
            if u.path == "/inspect":
                return self.reply(200, objects)
            if u.path == "/fixture":
                f = json.loads(body)
                if "clone" in f:
                    m = objects[f["clone"]].copy()
                    m.pop("id")
                    m.update(f.get("overrides", {}))
                    data = (root / f["clone"]).read_bytes() if "size" in m else None
                    if f.get("corrupt"):
                        data = b"corrupt synthetic bytes"
                    return self.reply(200, create(m, data))
                if "edit" in f:
                    objects[f["edit"]].update(f["overrides"])
                    save()
                    return self.reply(200, {})
                return self.reply(
                    200, create(f["meta"], bytes.fromhex(f["hex"]) if "hex" in f else None)
                )
            if self.headers.get("Authorization") != "Bearer synthetic-drive-test-token":
                return self.reply(401, {"error": {"message": "DO_NOT_LEAK"}})
            if controls.get("delay"):
                time.sleep(controls["delay"])
            if controls.get("huge_json"):
                return self.reply(200, {"padding": "x" * (1024 * 1024 + 1)})
            status = controls.get("status", 0)
            if status:
                return self.reply(
                    status,
                    {
                        "error": {
                            "message": "DO_NOT_LEAK synthetic-drive-test-token",
                            "errors": [{"reason": controls.get("reason", "forbidden")}],
                        }
                    },
                )
            if self.command == "GET" and u.path == "/drive/v3/files":
                assert all(
                    s in q["fields"][0]
                    for s in [
                        "nextPageToken",
                        "incompleteSearch",
                        "id",
                        "parents",
                        "mimeType",
                        "trashed",
                        "size",
                    ]
                )
                parent = re.fullmatch(
                    r"'([A-Za-z0-9_-]+)' in parents and trashed = false", q["q"][0]
                ).group(1)
                items = [
                    v
                    for v in objects.values()
                    if parent in v.get("parents", []) and not v["trashed"]
                ]
                items.sort(key=lambda m: m["id"], reverse=True)
                page = int(q.get("pageToken", ["0"])[0])
                size = controls.get("page_size", 1000)
                if controls.get("empty_page") and page == 0:
                    selected = []
                    nxt = "1"
                else:
                    start = page - (1 if controls.get("empty_page") else 0)
                    selected = items[start : start + size]
                    nxt = str(page + size) if start + size < len(items) else None
                out = {"files": selected, "incompleteSearch": bool(controls.get("incomplete"))}
                if controls.get("cycle"):
                    nxt = "1"
                if controls.get("token"):
                    nxt = controls["token"]
                if nxt is not None:
                    out["nextPageToken"] = nxt
                if controls.get("repeat_id") and selected:
                    out["files"] = selected + [selected[0]]
                if controls.get("foreign") and selected:
                    out["files"] = selected + [
                        dict(selected[0], id="foreign", parents=["elsewhere"])
                    ]
                return self.reply(200, out)
            if self.command == "GET" and u.path.startswith("/drive/v3/files/"):
                ident = u.path.rsplit("/", 1)[1]
                if controls.get("slow_id") == ident:
                    lock.release()
                    time.sleep(1)
                    lock.acquire()
                if ident not in objects or controls.get("missing_id") == ident:
                    return self.reply(404, {})
                if q.get("alt") == ["media"]:
                    assert objects[ident]["mimeType"] == "application/octet-stream"
                    return self.reply(200, (root / ident).read_bytes(), True)
                return self.reply(200, objects[ident])
            if self.command == "POST" and u.path == "/drive/v3/files":
                meta = json.loads(body)
                assert meta["mimeType"] == FOLDER and len(meta["parents"]) == 1
                return self.reply(200, create(meta))
            if self.command == "POST" and u.path == "/upload/drive/v3/files":
                assert q["uploadType"] == ["multipart"]
                msg = BytesParser(policy=default).parsebytes(
                    (
                        "Content-Type: "
                        + self.headers["Content-Type"]
                        + "\r\nMIME-Version: 1.0\r\n\r\n"
                    ).encode()
                    + body
                )
                parts = list(msg.iter_parts())
                assert len(parts) == 2
                meta = json.loads(parts[0].get_payload(decode=True))
                data = parts[1].get_payload(decode=True)
                assert meta["mimeType"] == "application/octet-stream" and len(meta["parents"]) == 1
                m = create(meta, data)
                if controls.pop("lost_ack", False):
                    return self.reply(503, {"error": {"message": "created but ack lost"}})
                if controls.get("corrupt_create"):
                    (root / m["id"]).write_bytes(b"x" * len(data))
                if controls.get("wrong_create_parent"):
                    m = dict(m, parents=["elsewhere"])
                return self.reply(200, m)
            self.reply(404, {})


save()
server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
(root / "ready.json").write_text(json.dumps({"origin": f"http://127.0.0.1:{server.server_port}"}))
server.serve_forever(poll_interval=0.05)
