#!/usr/bin/env python3
"""Public executable only. Explicit TMPDIR, generated fixtures, no credential access."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import fcntl
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

repo = Path(__file__).resolve().parents[3]
binary = Path(os.environ.get("CARGO_TARGET_DIR", repo / "target")) / "debug/lfs"
base = Path(os.environ["TMPDIR"]).resolve()
root = Path(tempfile.mkdtemp(prefix="lfs-cli-smoke-", dir=base))
secret_marker = "GENERATED_REDACTION_CANARY_not_a_credential"


def run(command, device=None, *args, data=None, code=0):
    argv = [str(binary), command]
    if device is not None:
        argv.append(str(device))
    argv.extend(map(str, args))
    result = subprocess.run(
        argv,
        input=json.dumps(data) if data is not None else "",
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert result.returncode == code, (command, result.returncode, result.stdout, result.stderr)
    assert secret_marker not in result.stdout + result.stderr
    assert not result.stderr, (command, result.stderr)
    body = json.loads(result.stdout)
    assert body["ok"] == (code == 0), body
    return body.get("result", body)


def text(value):
    return {"type": "text", "value": value}


def write(device, sql, params=(), refs=(), code=0):
    return run(
        "write",
        device,
        data={"statements": [{"sql": sql, "params": list(params)}], "attachments": list(refs)},
        code=code,
    )


def read(device):
    return run("read", device, data={"sql": "SELECT * FROM notes ORDER BY id"})["rows"]


try:
    remote = root / "remote"
    remote.mkdir()
    a, b = root / "a", root / "b"
    schema = {
        "version": 1,
        "tables": [
            {
                "id": 1,
                "name": "notes",
                "columns": [
                    {"name": "id", "kind": "Text", "nullable": False},
                    {"name": "body", "kind": "Text", "nullable": False},
                    {"name": "n", "kind": "Integer", "nullable": False},
                    {"name": "r", "kind": "Real", "nullable": False},
                    {"name": "b", "kind": "Blob", "nullable": False},
                ],
            }
        ],
    }
    backend = {"kind": "Fs", "root": str(remote)}
    for name, dest in [("a", a), ("b", b)]:
        run(
            "init",
            dest,
            data={"identity": ["cli-smoke", name, "first"], "schema": schema, "backend": backend},
        )
    payload = bytes(range(256)) * 2049
    source = root / "generated.bin"
    source.write_bytes(payload)
    attachment = run("attachment-add", a, source)
    assert attachment["hash"] == hashlib.sha256(payload).hexdigest()
    values = [
        text("x"),
        text("offline a"),
        {"type": "integer", "value": "9223372036854775807"},
        {"type": "real", "value": "3ff4000000000000"},
        {"type": "blob", "value": "00ff"},
    ]
    write(a, "INSERT INTO notes VALUES (?1,?2,?3,?4,?5)", values, [attachment["hash"]])
    write(b, "INSERT INTO notes VALUES ('b','offline b',-1,1.25,x'00ff')")
    write(a, "INSERT INTO notes VALUES ('deleted','temporary',0,0.0,x'')")
    for dev in [a, b, a]:
        run("sync-once", dev)
    downloaded = run("attachment-download", b, attachment["hash"])
    assert downloaded["state"] == "Available"
    installed = b / "store.db.attachments/objects" / attachment["hash"]
    assert installed.read_bytes() == payload
    assert read(a) == read(b)
    row = [r for r in read(a) if r[0] == text("x")][0]
    assert row[2:] == values[2:], row
    write(a, "UPDATE notes SET body='edit a' WHERE id='x'")
    write(a, "DELETE FROM notes WHERE id='deleted'")
    write(b, "UPDATE notes SET body='edit b' WHERE id='x'")
    for dev in [a, b, a]:
        run("sync-once", dev)
    conflicts = run("conflicts", a)["conflicts"]
    assert len(conflicts) == 1 and len(conflicts[0]["heads"]) == 2
    write(a, "UPDATE notes SET body='invalid conflict write' WHERE id='x'", code=3)
    values[1] = text("explicit resolution")
    run(
        "resolve",
        a,
        data={
            "table": 1,
            "key": "x",
            "observed": [h["id"] for h in conflicts[0]["heads"]],
            "row": values,
        },
    )
    run("backup", a, root / "refused-shared", "shared", code=7)
    local = root / "local"
    run("backup", a, local, "local")
    restored = root / "restored"
    run("restore", local, restored, data=backend)
    assert run("status", restored)["pending_outbox"] == "1"
    assert read(restored) == read(a)
    for dev in [restored, a, b, restored]:
        run("sync-once", dev)
    assert read(a) == read(b) == read(restored)
    assert len(read(a)) == 2
    assert not run("conflicts", b)["conflicts"]
    run("backup", a, root / "shared", "shared")
    write(a, "DROP TABLE notes", code=2)
    write(a, "UPDATE _lfs_state SET next_seq=0", code=2)
    write(a, "ATTACH DATABASE ?1 AS unsafe", [text(secret_marker)], code=2)
    run("attachment-download", a, "../" + secret_marker, code=2)
    run("daemon", a, secret_marker, code=9)
    run("diagnostics")
    run("status", a, "--cancel", code=8)
    with (a / "store.db").open("rb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        run("status", a, code=4)

    class FaultHTTP(BaseHTTPRequestHandler):
        status = 401

        def do_PROPFIND(self):
            self.send_response(self.status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), FaultHTTP)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        generated_env = "LFS_CLI_SMOKE_GENERATED_PASSWORD"
        os.environ[generated_env] = secret_marker
        http_device = root / "http-fault"
        http_backend = {
            "kind": "WebDAV",
            "endpoint": f"http://127.0.0.1:{server.server_port}",
            "username": "generated",
            "password_env": generated_env,
            "allow_loopback_http": True,
        }
        run(
            "init",
            http_device,
            data={
                "identity": ["fault", "device", "first"],
                "schema": schema,
                "backend": http_backend,
            },
        )
        run("sync-once", http_device, code=5)
        FaultHTTP.status = 429
        run("sync-once", http_device, code=6)
    finally:
        os.environ.pop("LFS_CLI_SMOKE_GENERATED_PASSWORD", None)
        server.shutdown()
        server.server_close()
        thread.join()

    run(
        "init",
        root / "bad-config",
        data={
            "identity": ["x", "y", "z"],
            "schema": schema,
            "backend": {"kind": "GoogleDrive", "token": secret_marker},
        },
        code=2,
    )
    assert run("status", a)["pending_outbox"] == "0"
    print(
        "PASS lfs executable JSON: offline insert/edit/delete, 524544-byte attachment, sync/conflicts/observed resolve, i64/f64/blob exact, backup/restore pending, authorizer, exit codes and redaction; no internal helpers"
    )
finally:
    shutil.rmtree(root)
