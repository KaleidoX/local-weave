#!/usr/bin/env python3
"""Reproducible independent rclone local HTTP fixture; never cloud verification."""

import argparse
import base64
import json
import hashlib
import hmac
import datetime
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request
import urllib.error

p = argparse.ArgumentParser()
p.add_argument("backend", choices=["webdav", "s3"])
p.add_argument("--evidence", type=Path, required=True)
p.add_argument("--tool", type=Path, required=True, help="Host-provided rclone 1.75.1 binary")
a = p.parse_args()
base = Path(os.environ["TMPDIR"]).resolve()
repo = Path(__file__).resolve().parents[3]
evidence = a.evidence.resolve()
if not (evidence.is_relative_to(base) or evidence.is_relative_to(repo)):
    raise SystemExit("Evidence must be under TMPDIR or repo")
evidence.mkdir(parents=True, exist_ok=True)
tool = a.tool.resolve()
subprocess.run([str(tool), "version", "--config", "/dev/null"], check=True)
resources = Path(tempfile.mkdtemp(prefix="lfs-stage3-http-", dir=base))
data = resources / "data"
data.mkdir()
if a.backend == "s3":
    (data / "stage3-test").mkdir()
with socket.socket() as probe:
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
endpoint = f"http://127.0.0.1:{port}"
cmd = [
    str(tool),
    "serve",
    a.backend,
    str(data),
    "--addr",
    f"127.0.0.1:{port}",
    "--config",
    "/dev/null",
]
cmd += (
    ["--user", "stage3-test", "--pass", "stage3-test-only"]
    if a.backend == "webdav"
    else ["--auth-key", "stage3-test,stage3-test-only"]
)
receipt = {
    "backend": a.backend,
    "level": "REAL_LOCAL_HTTP",
    "endpoint": endpoint,
    "resources": str(resources),
    "command": cmd,
    "server_pid": None,
    "ready": False,
    "test_exit": None,
    "cleanup": False,
}
server = None
try:
    with (evidence / f"stage3-{a.backend}-server.log").open("w") as log:
        server = subprocess.Popen(
            cmd,
            stdout=log,
            stderr=subprocess.STDOUT,
            env={"PATH": os.environ["PATH"], "TMPDIR": str(base), "LANG": "C.UTF-8"},
        )
        receipt["server_pid"] = server.pid
        print(f"Task-owned server PID {server.pid}, endpoint {endpoint}", flush=True)
        (evidence / f"stage3-{a.backend}-handle.json").write_text(json.dumps(receipt, indent=2))
        deadline = time.monotonic() + 20
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        while time.monotonic() < deadline:
            if server.poll() is not None:
                raise RuntimeError("server exited before readiness; see server log")
            req = urllib.request.Request(
                endpoint + "/", method="PROPFIND" if a.backend == "webdav" else "GET"
            )
            if a.backend == "webdav":
                req.add_header("Depth", "0")
                req.add_header(
                    "Authorization",
                    "Basic " + base64.b64encode(b"stage3-test:stage3-test-only").decode(),
                )
            else:
                now = datetime.datetime.now(datetime.timezone.utc)
                stamp = now.strftime("%Y%m%dT%H%M%SZ")
                day = now.strftime("%Y%m%d")
                payload = hashlib.sha256(b"").hexdigest()
                headers = (
                    f"host:127.0.0.1:{port}\nx-amz-content-sha256:{payload}\nx-amz-date:{stamp}\n"
                )
                signed = "host;x-amz-content-sha256;x-amz-date"
                canonical = f"GET\n/\n\n{headers}\n{signed}\n{payload}"
                scope = f"{day}/us-east-1/s3/aws4_request"
                signing = (
                    "AWS4-HMAC-SHA256\n"
                    + stamp
                    + "\n"
                    + scope
                    + "\n"
                    + hashlib.sha256(canonical.encode()).hexdigest()
                )
                key = b"AWS4stage3-test-only"
                for part in [day, "us-east-1", "s3", "aws4_request"]:
                    key = hmac.new(key, part.encode(), hashlib.sha256).digest()
                signature = hmac.new(key, signing.encode(), hashlib.sha256).hexdigest()
                req.add_header("x-amz-date", stamp)
                req.add_header("x-amz-content-sha256", payload)
                req.add_header(
                    "Authorization",
                    f"AWS4-HMAC-SHA256 Credential=stage3-test/{scope}, SignedHeaders={signed}, Signature={signature}",
                )
            try:
                with opener.open(req, timeout=0.5) as response:
                    status = response.status
                    response.read(1024)
            except urllib.error.HTTPError as error:
                status = error.code
                error.close()
            except (OSError, urllib.error.URLError):
                time.sleep(0.05)
                continue
            if status == (207 if a.backend == "webdav" else 200):
                receipt["ready"] = True
                receipt["readiness_status"] = status
                break
            receipt["last_readiness_status"] = status
            time.sleep(0.05)
        if not receipt["ready"]:
            raise RuntimeError("readiness failed")
        print(f"Readiness HTTP status {status}; running actual OpenDAL SDK journey", flush=True)
        env = os.environ.copy()
        env.update(LFS_TEST_ENDPOINT=endpoint, LFS_TEST_BACKEND=a.backend)
        test_cmd = [
            "cargo",
            "test",
            "-p",
            "lfs-storage",
            "--test",
            "services",
            "--features",
            "fs,webdav,s3",
            "--offline",
            "--locked",
            "--",
            "real_local_http_sdk_journey",
            "--exact",
            "--ignored",
            "--nocapture",
        ]
        receipt["test_command"] = test_cmd
        with (evidence / f"stage3-{a.backend}-sdk.log").open("w") as test_log:
            result = subprocess.run(
                test_cmd, cwd=repo, env=env, stdout=test_log, stderr=subprocess.STDOUT, timeout=120
            )
        receipt["test_exit"] = result.returncode
        print((evidence / f"stage3-{a.backend}-sdk.log").read_text(), flush=True)
        output = (evidence / f"stage3-{a.backend}-sdk.log").read_text()
        receipt["successful_test"] = (
            "real_local_http_sdk_journey"
            if (
                "test real_local_http_sdk_journey ... ok" in output
                and "test result: ok. 1 passed; 0 failed; 0 ignored;" in output
            )
            else None
        )
        if result.returncode or not receipt["successful_test"]:
            raise RuntimeError("SDK integration failed or required journey did not execute")
finally:
    if server is not None:
        if server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)
        receipt["server_exit"] = server.returncode
    shutil.rmtree(resources)
    receipt["cleanup"] = not resources.exists() and (server is None or server.poll() is not None)
    (evidence / f"stage3-{a.backend}-handle.json").write_text(json.dumps(receipt, indent=2))
    print(f"Cleanup task-owned server/resources: {receipt['cleanup']}", flush=True)
