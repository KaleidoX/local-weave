#!/usr/bin/env python3
"""Owned loopback WebDAV qualification of the public Node addon, never cloud proof."""

import argparse
import http.client
import http.server
import threading
import base64
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.request

p = argparse.ArgumentParser()
p.add_argument("--evidence", type=Path, required=True)
p.add_argument("--tool", type=Path, required=True, help="Host-provided rclone 1.75.1 binary")
p.add_argument("--pattern", default="H1 ")
p.add_argument("--bearer", action="store_true")
a = p.parse_args()
repo = Path(__file__).resolve().parents[3]
base = Path(os.environ["TMPDIR"]).resolve()
evidence = a.evidence.resolve()
evidence.mkdir(parents=True, exist_ok=False)
fixture = Path(tempfile.mkdtemp(prefix="lfs-node-webdav-", dir=base))
data = fixture / "remote"
data.mkdir()
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
endpoint = f"http://127.0.0.1:{port}"
username, password = "nh1-synthetic-user", "nh1-synthetic:password-密碼"
argv = [
    str(tool),
    "serve",
    "webdav",
    str(data),
    "--addr",
    f"127.0.0.1:{port}",
    "--config",
    "/dev/null",
    "--user",
    username,
    "--pass",
    password,
]
receipt = dict(
    category="LOCAL_SERVICE",
    version=version,
    fixture=str(fixture),
    argv=argv,
    ready=False,
    server_pid=None,
    server_exit=None,
    reaped=False,
    node_exit=None,
)
server = None
proxy = None
proxy_thread = None
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
auth = "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()
try:
    with (evidence / "server.log").open("w") as log:
        server = subprocess.Popen(
            argv,
            stdout=log,
            stderr=subprocess.STDOUT,
            env={"PATH": os.environ["PATH"], "TMPDIR": str(base), "LANG": "C.UTF-8"},
        )
        receipt["server_pid"] = server.pid
        (evidence / "receipt.json").write_text(json.dumps(receipt, indent=2))
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if server.poll() is not None:
                raise RuntimeError("owned rclone exited before readiness")
            try:
                with opener.open(
                    urllib.request.Request(
                        endpoint + "/",
                        method="PROPFIND",
                        headers={"Authorization": auth, "Depth": "0"},
                    ),
                    timeout=1,
                ) as response:
                    receipt["readiness_http_status"] = response.status
                    if response.status == 207:
                        receipt["ready"] = True
                        break
            except OSError:
                time.sleep(0.1)
        if not receipt["ready"]:
            raise RuntimeError("no PROPFIND 207 readiness")
        env = dict(
            os.environ,
            NH1_ENDPOINT=endpoint,
            NH1_USER=username,
            NH1_PASSWORD=password,
            NH1_FIXTURE=str(fixture),
            NH1_EVIDENCE=str(evidence),
        )
        if a.bearer:
            token = "nh1-Synthetic.bearer_~+/=="
            trace_lock = threading.Lock()

            def trace(event):
                with trace_lock, (evidence / "model-http.jsonl").open("a") as out:
                    out.write(json.dumps(event) + "\n")

            class BearerProxy(http.server.BaseHTTPRequestHandler):
                def log_message(self, *args):
                    pass

                def handle_method(self):
                    authorized = self.headers.get("Authorization") == "Bearer " + token
                    trace(
                        dict(
                            method=self.command,
                            path=self.path,
                            authorized=authorized,
                            phase="received",
                        )
                    )
                    if not authorized:
                        self.send_response(401)
                        self.send_header("WWW-Authenticate", "Bearer")
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    if (fixture / "delay-http").exists():
                        time.sleep(2)
                    body = b""
                    if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
                        while True:
                            size = int(self.rfile.readline().split(b";")[0], 16)
                            if not size:
                                self.rfile.readline()
                                break
                            if len(body) + size > 8 * 1024 * 1024:
                                raise ValueError("fixture request limit")
                            body += self.rfile.read(size)
                            self.rfile.read(2)
                    else:
                        size = int(self.headers.get("Content-Length", "0"))
                        if size > 8 * 1024 * 1024:
                            raise ValueError("fixture request limit")
                        body = self.rfile.read(size)
                    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
                    headers = {
                        k: v
                        for k, v in self.headers.items()
                        if k.lower()
                        not in (
                            "authorization",
                            "host",
                            "connection",
                            "transfer-encoding",
                            "content-length",
                        )
                    }
                    headers["Authorization"] = auth
                    try:
                        connection.request(self.command, self.path, body=body, headers=headers)
                        response = connection.getresponse()
                        payload = response.read(8 * 1024 * 1024 + 1)
                        if len(payload) > 8 * 1024 * 1024:
                            raise ValueError("fixture response limit")
                        self.send_response(response.status)
                        for k, v in response.getheaders():
                            if k.lower() not in (
                                "connection",
                                "transfer-encoding",
                                "content-length",
                                "server",
                                "date",
                            ):
                                self.send_header(k, v)
                        self.send_header("Content-Length", str(len(payload)))
                        self.end_headers()
                        if self.command != "HEAD":
                            self.wfile.write(payload)
                        trace(
                            dict(
                                method=self.command,
                                path=self.path,
                                status=response.status,
                                phase="response",
                            )
                        )
                    except (BrokenPipeError, ConnectionResetError):
                        trace(
                            dict(method=self.command, path=self.path, phase="client-disconnected")
                        )
                    finally:
                        connection.close()

                do_GET = do_HEAD = do_PUT = do_MKCOL = do_PROPFIND = do_DELETE = handle_method

            proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), BearerProxy)
            proxy.daemon_threads = False
            proxy_thread = threading.Thread(target=proxy.serve_forever)
            proxy_thread.start()
            model_endpoint = f"http://127.0.0.1:{proxy.server_port}"
            with opener.open(
                urllib.request.Request(
                    model_endpoint + "/",
                    method="PROPFIND",
                    headers={"Authorization": "Bearer " + token, "Depth": "0"},
                ),
                timeout=5,
            ) as response:
                assert response.status == 207
                receipt["model_readiness_http_status"] = response.status
            receipt["model"] = dict(
                category="MODEL_HTTP",
                pid=os.getpid(),
                endpoint=model_endpoint,
                boundary="Synthetic Bearer-to-Basic proxy; rclone itself only verifies Basic",
            )
            env.update(NH1_BEARER_ENDPOINT=model_endpoint, NH1_BEARER=token)

        command = [
            "node",
            "--test",
            "--test-name-pattern",
            a.pattern,
            "bindings/nodejs/test/webdav.test.mjs",
        ]
        receipt["node_argv"] = command
        with (evidence / "node.log").open("w") as out:
            node = subprocess.Popen(
                command, cwd=repo, env=env, stdout=out, stderr=subprocess.STDOUT
            )
            receipt["node_pid"] = node.pid
            try:
                receipt["node_exit"] = node.wait(timeout=120)
            except subprocess.TimeoutExpired:
                node.kill()
                receipt["node_exit"] = node.wait(timeout=5)
                raise
            finally:
                receipt["node_reaped"] = node.poll() is not None
finally:
    if proxy:
        proxy.shutdown()
        proxy.server_close()
        proxy_thread.join(timeout=10)
        receipt["model_stopped"] = not proxy_thread.is_alive()
    if server:
        server.terminate()
        try:
            receipt["server_exit"] = server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            receipt["server_exit"] = server.wait(timeout=5)
        receipt["reaped"] = server.poll() is not None
    (evidence / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, ensure_ascii=True))
raise SystemExit(receipt["node_exit"])
