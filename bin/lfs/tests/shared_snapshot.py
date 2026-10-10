#!/usr/bin/env python3
"""Check CLI shared snapshots, unpublished outbox boundaries, and restore identity."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tempfile

binary = (
    Path(os.environ.get("CARGO_TARGET_DIR", Path(__file__).resolve().parents[3] / "target"))
    / "debug/lfs"
)
root = Path(tempfile.mkdtemp(prefix="shared-p6-", dir=os.environ["TMPDIR"]))


def run(*args, data=None):
    p = subprocess.run(
        [str(binary), *map(str, args)],
        input=json.dumps(data) if data is not None else "",
        text=True,
        capture_output=True,
        timeout=30,
    )
    assert p.returncode == 0, (args, p.stdout, p.stderr)
    result = json.loads(p.stdout)
    assert result["ok"], result
    return result["result"]


def tree(path):
    return {
        str(p.relative_to(path)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in path.rglob("*")
        if p.is_file()
    }


try:
    remote = root / "remote"
    remote.mkdir()
    backend = {"kind": "Fs", "root": str(remote)}
    source = root / "source"
    schema = {
        "version": 1,
        "tables": [
            {
                "id": 1,
                "name": "notes",
                "columns": [
                    {"name": "id", "kind": "Text", "nullable": False},
                    {"name": "body", "kind": "Text", "nullable": False},
                ],
            }
        ],
    }
    run(
        "init",
        source,
        data={"identity": ["p6", "source", "first"], "schema": schema, "backend": backend},
    )
    hashes = []
    contents = [b"P6 public historical object", b"P6 private never referenced object"]
    for i, content in enumerate(contents):
        path = root / f"fixture-{i}"
        path.write_bytes(content)
        hashes.append(run("attachment-add", source, path)["hash"])
    public, private = hashes
    run(
        "write",
        source,
        data={
            "statements": [{"sql": "INSERT INTO notes VALUES ('x','ancestor')"}],
            "attachments": [public],
        },
    )
    run("sync-once", source)
    run("write", source, data={"statements": [{"sql": "DELETE FROM notes WHERE id='x'"}]})
    run("sync-once", source)
    before = tree(source)
    shared = root / "shared"
    run("backup", source, shared, "shared")
    assert tree(source) == before, "source changed during shared backup"
    manifest = json.loads((shared / "manifest.json").read_bytes())
    assert {e["path"] for e in manifest["files"]} == {"store.db", f"objects/{public}"}
    assert not (shared / "objects" / private).exists()
    assert (shared / "objects" / public).read_bytes() == contents[0]
    for path in shared.rglob("*"):
        if path.is_file():
            raw = path.read_bytes()
            assert private.encode() not in raw and contents[1] not in raw, path
    with sqlite3.connect(f"file:{shared / 'store.db'}?mode=ro", uri=True) as db:
        assert db.execute("SELECT hash FROM _lfs_files").fetchall() == [(public,)]
        assert db.execute("PRAGMA freelist_count").fetchone() == (0,)
    restored = root / "restored"
    run("restore", shared, restored, data=backend)
    assert run("read", restored, data={"sql": "SELECT * FROM notes"})["rows"] == []
    run(
        "write",
        restored,
        data={
            "statements": [{"sql": "INSERT INTO notes VALUES ('restored','functional')"}],
            "attachments": [public],
        },
    )
    run("sync-once", restored)
    run(
        "write",
        source,
        data={"statements": [{"sql": "INSERT INTO notes VALUES ('pending','local only')"}]},
    )
    before = tree(source)
    local = root / "local"
    run("backup", source, local, "local")
    assert tree(source) == before, "source changed during local backup"
    assert (local / "objects" / private).read_bytes() == contents[1]
    clone = root / "local-restored"
    run("restore", local, clone, data=backend)
    assert run("status", clone)["pending_outbox"] == "1"
    print(
        "PASS v2 P6: public CLI shared file/manifest/metadata/raw-byte exclusion; "
        "historical object and tombstone preserved; functional restore; "
        "local private bytes and pending retained; source tree unchanged. "
        "v1 P1-P5 and optional UNKNOWN unchanged."
    )
finally:
    shutil.rmtree(root)
