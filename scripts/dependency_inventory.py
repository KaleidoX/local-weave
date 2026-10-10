#!/usr/bin/env python3
"""Generate dependency inventory from cargo metadata JSON; run at repository root."""

import json
import sys
from pathlib import Path

metadata = json.loads(Path(sys.argv[1]).read_text())
packages = sorted(metadata["packages"], key=lambda p: (p["name"], p["version"]))
output = ["name\tversion\tlicense\trust-version\tsource"]
for p in packages:
    output.append(
        "\t".join(
            [
                p["name"],
                p["version"],
                p.get("license") or "LICENSE_FILE:" + str(p.get("license_file")),
                p.get("rust_version") or "",
                p.get("source") or "workspace",
            ]
        )
    )
Path("docs/DEPENDENCIES.tsv").write_text("\n".join(output) + "\n")
print(
    f"{len(packages)} packages; {sum(p.get('source') is not None for p in packages)} external; all targets, not all target builds"
)
missing = [p["name"] for p in packages if not p.get("license") and not p.get("license_file")]
if missing:
    raise SystemExit(f"Missing license metadata: {missing}")
