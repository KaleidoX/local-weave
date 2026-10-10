#!/usr/bin/env python3
"""Validate public config syntax, local Markdown links and repository references."""

import json
import re
import tomllib
from urllib.parse import unquote, urlsplit

from project_files import ROOT, public_files


def main():
    files = public_files()
    documents = [p for p in files if p.suffix == ".md"]
    if not documents:
        raise ValueError("Zero public Markdown files")
    count = 0
    for path in files:
        if path.suffix == ".json":
            json.loads(path.read_text())
        elif path.suffix == ".toml":
            tomllib.loads(path.read_text())
        if path.suffix not in {".md", ".json", ".yaml", ".yml", ".toml"}:
            continue
        text = path.read_text()
        if re.search(r"/(?:home|Users)/[^\s/]+/", text):
            raise ValueError(f"Personal machine path in public documentation/config: {path}")
        for url in re.findall(r"https://github\.com/[^\s)\"'<>`]+", text):
            parts = urlsplit(url).path.lstrip("/").split("/")
            if len(parts) < 2:
                continue
            owner, repository = (unquote(part).casefold() for part in parts[:2])
            repository = repository.removesuffix(".git")
            if (
                repository == "localweave"
                or "sqlite-local-first" in repository
                or "local-weave" in repository
            ) and (owner, repository) != ("kaleidox", "local-weave"):
                raise ValueError(f"Unexpected repository URL: {path}: {url}")
        if path.suffix != ".md":
            continue
        # Code samples are not prose links. Check inline and reference-style targets.
        text = re.sub(r"^\s*(`{3,}|~{3,}).*?^\s*\1\s*$", "", text, flags=re.M | re.S)
        targets = re.findall(r'\]\(([^\s)]+)(?:\s+"[^"\n]*")?\)', text)
        targets += re.findall(r"^\s*\[[^\]]+\]:\s*(\S+)", text, flags=re.M)
        for target in targets:
            link = urlsplit(target.strip("<>"))
            if link.scheme or link.netloc or not link.path:
                continue
            relative = unquote(link.path)
            resolved = (path.parent / relative).resolve()
            if not resolved.is_relative_to(ROOT) or not resolved.exists():
                raise ValueError(
                    f"Broken or external local link: {path.relative_to(ROOT)}: {target}"
                )
            if ".agents" in resolved.relative_to(ROOT).parts:
                raise ValueError(f"Public link depends on internal task: {path}: {target}")
            count += 1
    print(f"Public configs parsed; {len(documents)} Markdown files, {count} local links checked")


if __name__ == "__main__":
    main()
