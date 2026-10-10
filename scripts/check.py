#!/usr/bin/env python3
"""Check public sources without fixing them; `format` explicitly writes formatting."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile

from project_files import ROOT, public_files, selected

NODE = ROOT / "bindings/nodejs"
ACTIONS = ("format-check", "lint", "type", "test")
SCOPES = ("rust", "node", "python", "docs")
SUPPORT = {
    "format-check": SCOPES,
    "format": SCOPES,
    "lint": SCOPES,
    "type": ("rust", "node"),
    "test": ("rust", "node"),
}


def run(command, *, cwd=ROOT, env=None):
    command = list(map(str, command))
    print("$ " + shlex.join(command), flush=True)
    result = subprocess.run(command, cwd=cwd, env=env)
    print(f"Exit: {result.returncode}", flush=True)
    if result.returncode:
        raise SystemExit(result.returncode)


def node_tool(package, executable):
    manifest = NODE / "node_modules" / package / "package.json"
    expected = json.loads((NODE / "package.json").read_text())["devDependencies"][package]
    if not manifest.is_file() or json.loads(manifest.read_text())["version"] != expected:
        raise ValueError(f"Missing or mismatched {package}; run npm ci --prefix bindings/nodejs")
    path = manifest.parent / executable
    if not path.is_file():
        raise ValueError(f"Missing tool: {path}")
    return ["node", str(path)]


def native_test(common, env):
    # Read Cargo's actual artifact path, never the source-directory addon.
    command = [
        "cargo",
        "build",
        "-p",
        "lfs-node",
        *common,
        "--features",
        "bundled",
        "--message-format=json-render-diagnostics",
    ]
    print("$ " + shlex.join(command), flush=True)
    result = subprocess.run(command, cwd=ROOT, env=env, stdout=subprocess.PIPE, text=True)
    print(f"Exit: {result.returncode}", flush=True)
    if result.returncode:
        print(result.stdout, flush=True)
        raise SystemExit(result.returncode)
    artifacts = []
    for line in result.stdout.splitlines():
        message = json.loads(line)
        if message.get("reason") == "compiler-artifact" and message["target"]["name"] == "lfs_node":
            artifacts.extend(Path(p) for p in message["filenames"] if p.endswith(".so"))
    if len(artifacts) != 1 or not artifacts[0].is_file():
        raise ValueError("Expected exactly one Linux lfs_node shared-library artifact")
    with tempfile.TemporaryDirectory(prefix="localweave-node-") as directory:
        copy = Path(directory)
        (copy / "test").mkdir()
        for name in ("package.json", "index.js", "index.d.ts", "test/native.test.mjs"):
            shutil.copy2(NODE / name, copy / name)
        addon = copy / "lfs_node.node"
        shutil.copy2(artifacts[0], addon)
        print(
            "Loaded candidate addon SHA256: " + hashlib.sha256(addon.read_bytes()).hexdigest(),
            flush=True,
        )
        run(["node", "--test", "test/native.test.mjs"], cwd=copy, env=env)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "actions",
        nargs="*",
        metavar="ACTION",
        help="format-check, lint, type, test; default: all four. format writes files",
    )
    parser.add_argument("--scope", choices=("all", *SCOPES), default="all")
    parser.add_argument("--offline", action="store_true", help="Use cached Cargo dependencies only")
    parser.add_argument(
        "--ruff",
        type=Path,
        default=ROOT / "scripts/.venv/bin/ruff",
        help="Exact-version project-private Ruff executable",
    )
    args = parser.parse_args()
    actions = args.actions or list(ACTIONS)
    if any(action not in SUPPORT for action in actions):
        parser.error("Unknown action; choose format-check, lint, type, test or format")
    if "format" in actions and actions != ["format"]:
        parser.error("format must be used alone; it writes source files")
    scopes = SCOPES if args.scope == "all" else (args.scope,)
    for action in actions:
        if not any(scope in SUPPORT[action] for scope in scopes):
            parser.error(f"{action} has no checks for scope {args.scope}")
    common = ["--locked", *(["--offline"] if args.offline else [])]
    workspace = ["--workspace", *common, "--features", "lfs-sqlite/bundled"]
    with tempfile.TemporaryDirectory(prefix="localweave-check-") as temporary:
        env = dict(os.environ, TMPDIR=temporary, PYTHONDONTWRITEBYTECODE="1")
        for action in actions:
            for scope in scopes:
                if scope not in SUPPORT[action]:
                    continue
                files = selected(scope)
                print(f"[{action}/{scope}] {len(files)} selected files", flush=True)
                if scope == "rust":
                    if action in ("format", "format-check"):
                        run(
                            [
                                "cargo",
                                "fmt",
                                "--all",
                                *(["--", "--check"] if action == "format-check" else []),
                            ],
                            env=env,
                        )
                    elif action == "lint":
                        run(
                            [
                                "cargo",
                                "clippy",
                                *workspace,
                                "--all-targets",
                                "--",
                                "-D",
                                "warnings",
                            ],
                            env=env,
                        )
                    elif action == "type":
                        run(["cargo", "check", *workspace, "--all-targets"], env=env)
                    else:
                        run(["cargo", "test", *workspace], env=env)
                elif action in ("format", "format-check") and scope in ("node", "docs"):
                    run(
                        [
                            *node_tool("prettier", "bin/prettier.cjs"),
                            "--config",
                            ".prettierrc.json",
                            "--ignore-path",
                            ".prettierignore",
                            "--write" if action == "format" else "--check",
                            *files,
                        ],
                        env=env,
                    )
                elif scope == "python":
                    expected = (
                        (ROOT / "scripts/requirements-dev.txt").read_text().strip().split("==")[1]
                    )
                    version = subprocess.run(
                        [str(args.ruff), "--version"], capture_output=True, text=True, check=True
                    )
                    if version.stdout.strip() != f"ruff {expected}":
                        raise ValueError(f"Ruff version must be {expected}")
                    command = [
                        str(args.ruff),
                        "check" if action == "lint" else "format",
                        "--no-cache",
                        "--config",
                        "scripts/ruff.toml",
                    ]
                    if action == "format-check":
                        command.append("--check")
                    run([*command, *files], env=env)
                elif scope == "node":
                    if action == "lint":
                        run(
                            [
                                *node_tool("eslint", "bin/eslint.js"),
                                "--max-warnings",
                                "0",
                                *[str(ROOT / f) for f in files],
                            ],
                            cwd=NODE,
                            env=env,
                        )
                    elif action == "type":
                        types = [str(ROOT / f) for f in files if f.endswith(".ts")]
                        run(
                            [
                                *node_tool("typescript", "bin/tsc"),
                                "--noEmit",
                                "--types",
                                "node",
                                "--strict",
                                "--target",
                                "ES2022",
                                "--module",
                                "NodeNext",
                                "--moduleResolution",
                                "NodeNext",
                                *types,
                            ],
                            cwd=NODE,
                            env=env,
                        )
                    else:
                        native_test(common, env)
                elif scope == "docs":
                    node_tool("yaml", "bin.mjs")
                    run([sys.executable, "-B", "scripts/check_docs.py"], env=env)
                    configs = [
                        str(p.relative_to(ROOT))
                        for p in public_files()
                        if p.suffix in {".yaml", ".yml"}
                    ]
                    if not configs:
                        raise ValueError("Zero YAML config files")
                    run(["node", "bindings/nodejs/check-yaml.mjs", *configs], env=env)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"Check failed: {error}", file=sys.stderr)
        raise SystemExit(1)
