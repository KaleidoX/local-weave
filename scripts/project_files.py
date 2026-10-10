"""Explicit public source sets; never use Git's index or traverse private trees."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ROOT_FILES = (
    ".editorconfig",
    ".gitignore",
    ".prettierignore",
    ".prettierrc.json",
    "AGENTS.md",
    "CHANGELOG.md",
    "CONTRIBUTING.md",
    "Cargo.toml",
    "Cargo.lock",
    "LICENSE",
    "README.md",
    "rust-toolchain.toml",
    "rustfmt.toml",
)
PATTERNS = (
    "core/.gitignore",
    ".github/*.md",
    ".github/ISSUE_TEMPLATE/*.md",
    ".github/workflows/*.yml",
    ".github/workflows/*.yaml",
    "docs/*.md",
    "docs/adr/*.md",
    "docs/testing/*.md",
    "docs/*DEPENDENCIES.tsv",
    "scripts/*.py",
    "scripts/*.toml",
    "scripts/requirements-dev.txt",
    "scripts/.gitignore",
    "bin/lfs/tests/*.py",
    "integrations/opendal/tests/*.py",
    "integrations/opendal/tests/fixtures/*.py",
    "bindings/nodejs/*.js",
    "bindings/nodejs/*.mjs",
    "bindings/nodejs/*.ts",
    "bindings/nodejs/*.md",
    "bindings/nodejs/package.json",
    "bindings/nodejs/package-lock.json",
    "bindings/nodejs/.gitignore",
    "bindings/nodejs/test/*.mjs",
    "bindings/nodejs/test/*.ts",
    "bindings/nodejs/test/*.py",
)
RUST_MODULES = (
    "core/protocol",
    "core/sqlite",
    "integrations/opendal",
    "bin/lfs",
    "bindings/nodejs/native",
)


def public_files(root=ROOT):
    paths = {root / name for name in ROOT_FILES if (root / name).is_file()}
    for pattern in PATTERNS:
        paths.update(root.glob(pattern))
    for module in RUST_MODULES:
        for name in ("Cargo.toml", "build.rs", ".gitignore"):
            path = root / module / name
            if path.is_file():
                paths.add(path)
        for folder in ("src", "tests", "examples"):
            paths.update((root / module / folder).glob("*.rs"))
            paths.update((root / module / folder).glob("*/mod.rs"))
    paths = {
        path
        for path in paths
        if not any(
            part.startswith((".env", "credentials")) for part in path.relative_to(root).parts
        )
    }
    for path in paths:
        if (
            path.is_symlink()
            or not path.is_file()
            or not path.resolve().is_relative_to(root.resolve())
        ):
            raise ValueError(f"Not a regular public source file: {path}")
    return sorted(paths)


def selected(scope, root=ROOT):
    files = public_files(root)
    groups = {
        "rust": [p for p in files if p.suffix == ".rs"],
        "node": [p for p in files if p.suffix in {".js", ".mjs", ".ts"}],
        "python": [p for p in files if p.suffix == ".py"],
        "docs": [
            p
            for p in files
            if p.suffix in {".md", ".json", ".yml", ".yaml"} and p.name != "package-lock.json"
        ],
    }
    result = groups[scope]
    if not result:
        raise ValueError(f"Zero matching files for scope {scope}")
    return [str(p.relative_to(root)) for p in result]
