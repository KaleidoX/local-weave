#!/usr/bin/env python3
"""Owned real rclone S3 + transparent observation / MODEL_HTTP truncation seam."""

import argparse
import base64
import ctypes
import datetime
import hashlib
import hmac
import http.client
import http.server
import json
import os
import re
from pathlib import Path
import socket
import signal
import subprocess
import tempfile
import threading
import time
import urllib.request

p = argparse.ArgumentParser()
p.add_argument("--evidence", type=Path, required=True)
p.add_argument("--tool", type=Path, required=True, help="Host-provided rclone 1.75.1 binary")
p.add_argument("--pattern", default="^S[1-5] ")
a = p.parse_args()
repo = Path(__file__).resolve().parents[3]
base = Path(os.environ["TMPDIR"]).resolve()
evidence = a.evidence.resolve()
if not (evidence.is_relative_to(repo) or evidence.is_relative_to(base)):
    raise SystemExit("evidence must be inside repo or TMPDIR")
evidence.mkdir(parents=True, exist_ok=False)
fixture = Path(tempfile.mkdtemp(prefix="lfs-node-s3-", dir=base))
data = fixture / "remote"
bucket = "nh2-owned-bucket"
(data / bucket).mkdir(parents=True)
access, secret, token = (
    "NH2_SYNTHETIC_ACCESS",
    "nh2-Synthetic-Secret-Only-7812",
    "nh2-Synthetic-Session-Token-8913",
)
tool = a.tool.resolve()
version = subprocess.run(
    [str(tool), "version", "--config", "/dev/null"],
    capture_output=True,
    text=True,
    check=True,
    timeout=10,
).stdout
assert "v1.75.1" in version
with socket.socket() as probe:
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
argv = [
    str(tool),
    "serve",
    "s3",
    str(data),
    "--addr",
    f"127.0.0.1:{port}",
    "--config",
    "/dev/null",
    "--auth-key",
    access + "," + secret,
    "--log-level",
    "ERROR",
]
receipt = dict(
    category="REAL_LOCAL_S3",
    fixture=str(fixture),
    version=version,
    runner_pid=os.getpid(),
    bucket_prepared_before_serve=True,
    ready=False,
    node_exit=None,
)
server = proxy = node = thread = None
# Linux-only fixture: adopt node --test descendants so timeout cleanup can wait them too.
if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
    raise OSError(ctypes.get_errno(), "fixture subreaper setup failed")


def stop_node_group():
    if node is None:
        return
    # start_new_session makes this group exclusively owned by this runner.
    try:
        os.killpg(node.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    node.wait(timeout=5)
    adopted = []
    deadline = time.monotonic() + 5
    while True:
        try:
            pid, status = os.waitpid(-node.pid, os.WNOHANG)
        except ChildProcessError:
            break
        if pid:
            adopted.append(dict(pid=pid, wait_status=status))
        elif time.monotonic() >= deadline:
            raise RuntimeError("owned Node descendant reap deadline exceeded")
        else:
            time.sleep(0.01)
    receipt.setdefault("adopted_node_descendants_reaped", []).extend(adopted)


trace_lock = threading.Lock()


def trace(event):
    with trace_lock, (evidence / "http.jsonl").open("a") as out:
        out.write(json.dumps(event) + "\n")


class Proxy(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def handle_method(self):
        headers = {
            k: v
            for k, v in self.headers.items()
            if k.lower() not in ("connection", "transfer-encoding", "content-length")
        }
        body = bytearray()
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            while True:
                n = int(self.rfile.readline().split(b";")[0], 16)
                if n == 0:
                    self.rfile.readline()
                    break
                if len(body) + n > 16 * 1024 * 1024:
                    raise ValueError("fixture request bound")
                body.extend(self.rfile.read(n))
                self.rfile.read(2)
        else:
            n = int(self.headers.get("Content-Length", "0"))
            if n > 16 * 1024 * 1024:
                raise ValueError("fixture request bound")
            body.extend(self.rfile.read(n))
        event = dict(
            method=self.command,
            path=self.path,
            phase="received",
            bytes=len(body),
            sha256=hashlib.sha256(body).hexdigest(),
            range=self.headers.get("Range"),
            token_present="x-amz-security-token" in self.headers,
            token_matches_synthetic=self.headers.get("x-amz-security-token") == token,
        )
        trace(event)
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
        try:
            conn.request(self.command, self.path, body=bytes(body), headers=headers)
            response = conn.getresponse()
            payload = response.read(16 * 1024 * 1024 + 1)
            if len(payload) > 16 * 1024 * 1024:
                raise ValueError("fixture response bound")
            fault = (
                self.command == "GET"
                and "/lfs-attachments/" in self.path
                and (fixture / "truncate").exists()
                and response.status in (200, 206)
            )
            response_event = dict(
                method=self.command,
                path=self.path,
                phase="response",
                status=response.status,
                bytes=len(payload),
                sha256=hashlib.sha256(payload).hexdigest(),
                range=self.headers.get("Range"),
                content_range=response.getheader("Content-Range"),
                fault="MODEL_HTTP_TRUNCATE" if fault else None,
            )
            if (
                self.path.split("?")[0].endswith(".lfs")
                and self.command == "GET"
                and response.status == 200
            ):
                response_event["wire_base64"] = base64.b64encode(payload).decode()
            trace(response_event)
            self.send_response(response.status)
            for k, v in response.getheaders():
                if k.lower() not in ("connection", "transfer-encoding", "server", "date"):
                    self.send_header(k, v)
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(payload[: max(1, len(payload) // 2)] if fault else payload)
            self.close_connection = True
        except (BrokenPipeError, ConnectionResetError):
            trace(dict(method=self.command, path=self.path, phase="client-disconnected"))
        finally:
            conn.close()

    do_GET = do_HEAD = do_PUT = do_POST = do_DELETE = handle_method


def signed_get(endpoint, path):
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    day = stamp[:8]
    host = endpoint.split("://")[1]
    empty = hashlib.sha256(b"").hexdigest()
    signed = "host;x-amz-content-sha256;x-amz-date"
    headers = f"host:{host}\nx-amz-content-sha256:{empty}\nx-amz-date:{stamp}\n"
    canonical = f"GET\n{path}\n\n{headers}\n{signed}\n{empty}"
    scope = f"{day}/us-east-1/s3/aws4_request"
    message = (
        f"AWS4-HMAC-SHA256\n{stamp}\n{scope}\n" + hashlib.sha256(canonical.encode()).hexdigest()
    )
    key = ("AWS4" + secret).encode()
    for part in [day, "us-east-1", "s3", "aws4_request"]:
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    sig = hmac.new(key, message.encode(), hashlib.sha256).hexdigest()
    req = urllib.request.Request(
        endpoint + path,
        headers={
            "x-amz-date": stamp,
            "x-amz-content-sha256": empty,
            "Authorization": f"AWS4-HMAC-SHA256 Credential={access}/{scope}, SignedHeaders={signed}, Signature={sig}",
        },
    )
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(
        req, timeout=2
    ) as response:
        return response.status, response.read()


try:
    with (evidence / "server.log").open("w") as log:
        server = subprocess.Popen(
            argv,
            stdout=log,
            stderr=subprocess.STDOUT,
            env={"PATH": os.environ["PATH"], "TMPDIR": str(base), "LANG": "C.UTF-8"},
        )
        receipt["server_pid"] = server.pid
        proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
        proxy.daemon_threads = False
        thread = threading.Thread(target=proxy.serve_forever)
        thread.start()
        endpoint = f"http://127.0.0.1:{proxy.server_port}"
        receipt.update(endpoint=endpoint, upstream=f"http://127.0.0.1:{port}", proxy_in_runner=True)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if server.poll() is not None:
                raise RuntimeError("rclone exited before readiness")
            try:
                root_status, root_body = signed_get(endpoint, "/")
                bucket_status, bucket_body = signed_get(endpoint, "/" + bucket)
                assert root_status == bucket_status == 200
                assert bucket.encode() in root_body and b"ListBucketResult" in bucket_body
                receipt.update(ready=True, root_http=root_status, bucket_http=bucket_status)
                (evidence / "readiness-root.xml").write_bytes(root_body)
                (evidence / "readiness-bucket.xml").write_bytes(bucket_body)
                break
            except (OSError, AssertionError):
                time.sleep(0.05)
        if not receipt["ready"]:
            raise RuntimeError("signed exact bucket ListBucket readiness failed")
        env = dict(
            os.environ,
            NH2_ENDPOINT=endpoint,
            NH2_ACCESS=access,
            NH2_SECRET=secret,
            NH2_TOKEN=token,
            NH2_BUCKET=bucket,
            NH2_FIXTURE=str(fixture),
            NH2_EVIDENCE=str(evidence),
        )
        command = [
            "node",
            "--test",
            "--test-name-pattern",
            a.pattern,
            "bindings/nodejs/test/s3.test.mjs",
        ]
        receipt["node_command"] = command
        with (evidence / "node.log").open("w") as out:
            node = subprocess.Popen(
                command,
                cwd=repo,
                env=env,
                stdout=out,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            receipt["node_pid"] = node.pid
            try:
                receipt["node_exit"] = node.wait(timeout=240)
            except subprocess.TimeoutExpired:
                receipt["deadline_exceeded"] = True
                stop_node_group()
                receipt["node_exit"] = node.returncode
finally:
    try:
        stop_node_group()
    finally:
        try:
            if proxy is not None:
                proxy.shutdown()
                proxy.server_close()
                thread.join(timeout=25)
                receipt["proxy_stopped"] = not thread.is_alive()
        finally:
            if server is not None:
                if server.poll() is None:
                    server.terminate()
                    try:
                        server.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        server.kill()
                        server.wait(timeout=5)
                receipt.update(
                    server_exit=server.returncode, server_reaped=server.poll() is not None
                )
    if node is not None:
        receipt["node_reaped"] = node.poll() is not None
        native = evidence / "node-native.json"
        if native.exists():
            proof = json.loads(native.read_text())
            receipt["native_test_pid"] = proof["pid"]
            receipt["native_test_reaped"] = not Path("/proc", str(proof["pid"])).exists()
            assert receipt["native_test_reaped"]
        output = (evidence / "node.log").read_text()
        names = re.findall(
            r"^test\(\s*'([^']+)'", (repo / "bindings/nodejs/test/s3.test.mjs").read_text(), re.M
        )
        receipt["expected_tests"] = sum(bool(re.search(a.pattern, n)) for n in names)
        receipt["tap_counts"] = {
            k: int(v)
            for k, v in re.findall(r"^# (tests|pass|fail|cancelled|skipped) (\d+)$", output, re.M)
        }
        if receipt["node_exit"] == 0:
            assert receipt["expected_tests"] > 0
            assert (
                receipt["tap_counts"]["tests"]
                == receipt["tap_counts"]["pass"]
                == receipt["expected_tests"]
            )
            assert (
                receipt["tap_counts"]["fail"]
                == receipt["tap_counts"]["cancelled"]
                == receipt["tap_counts"]["skipped"]
                == 0
            )
    # rclone ERROR logs are audited; SDK outputs are never rewritten.
    logpath = evidence / "server.log"
    if logpath.exists():
        raw = logpath.read_text()
        count = sum(raw.count(s) for s in (access, secret, token))
        for s in (access, secret, token):
            raw = raw.replace(s, "[SYNTHETIC_REDACTED]")
        logpath.write_text(raw)
        receipt["server_logger_redactions"] = count
    (evidence / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt))
raise SystemExit(receipt["node_exit"] if receipt["node_exit"] is not None else 1)
