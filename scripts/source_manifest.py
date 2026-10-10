#!/usr/bin/env python3
"""Generate/check a delivery snapshot from explicit public sources, without Git."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from project_files import ROOT, public_files


def inventory():
    return [
        {"path": str(p.relative_to(ROOT)), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
        for p in public_files()
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("generate", "check"))
    parser.add_argument("--output", type=Path, default=ROOT / "SOURCE_MANIFEST.json")
    args = parser.parse_args()
    files = inventory()
    if not files:
        raise ValueError("Zero source files")
    package = json.loads((ROOT / "bindings/nodejs/package.json").read_text())
    data = {
        "format": 2,
        "kind": "explicit-source-inventory-not-binary-release",
        "project": "local-weave",
        "display_name": "LocalWeave",
        "version": package["version"],
        "license": package["license"],
        "repository": package["repository"]["url"].removesuffix(".git"),
        "file_count": len(files),
        "files": files,
    }
    if args.action == "generate":
        if args.output.resolve() in {p.resolve() for p in public_files()}:
            raise ValueError("Output must not overwrite a public source file")
        data["generated_at"] = datetime.now(timezone.utc).isoformat()
        args.output.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
        print(f"Generated {args.output}: {len(files)} files")
    else:
        actual = json.loads(args.output.read_text())
        actual.pop("generated_at", None)
        if actual != data:
            raise ValueError("Source snapshot differs; regenerate for the next delivery")
        print(f"Checked {args.output}: {len(files)} files")


if __name__ == "__main__":
    main()
