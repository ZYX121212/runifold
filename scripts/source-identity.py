#!/usr/bin/env python3
"""Identify relevant tracked and nonignored source without emitting file contents."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
PREFIXES = ("crates/", "scripts/", "docs/", ".github/", "benchmarks/", "ai/", "ai-evals/")
ROOT_FILES = {"Cargo.toml", "Cargo.lock", "runifold.ai.json", "runifold.manifest.json", "AGENTS.md", "README.md", "CHANGELOG.md"}


def capture(root=ROOT):
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, stderr=subprocess.DEVNULL).decode().strip()
    if len(revision) not in (40, 64) or any(c not in "0123456789abcdef" for c in revision):
        raise ValueError("invalid revision")
    names = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=root, stderr=subprocess.DEVNULL).decode().split("\0")
    tracked = set(subprocess.check_output(["git", "ls-files", "--cached", "-z"], cwd=root, stderr=subprocess.DEVNULL).decode().split("\0"))
    selected = sorted({name for name in names if name and (name in tracked or name in ROOT_FILES or name.startswith(PREFIXES))
                       and "__pycache__" not in Path(name).parts
                       and not name.endswith((".pyc", ".pyo"))
                       and Path(name).name not in (".env", "credentials.toml")
                       and not Path(name).name.startswith(".env.")})
    if len(selected) > 100_000:
        raise ValueError("source bound exceeded")
    digest = hashlib.sha256()
    total_bytes = 0
    for name in selected:
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("unsafe source selection")
        path = root / relative
        if path.is_symlink():
            raise ValueError("source symlinks are not supported")
        digest.update(len(name.encode()).to_bytes(8, "little") + name.encode())
        if not path.exists():
            digest.update(b"deleted\0")
            continue
        if not path.resolve().is_relative_to(root.resolve()):
            raise ValueError("source resolves outside its root")
        before = path.stat()
        if not path.is_file():
            raise ValueError("source is not a regular file")
        total_bytes += before.st_size
        if total_bytes > 1024**3:
            raise ValueError("source bound exceeded")
        content = hashlib.sha256()
        with path.open("rb") as stream:
            while block := stream.read(64 * 1024):
                content.update(block)
        after = path.stat()
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError("source changed while hashing")
        digest.update(before.st_size.to_bytes(8, "little") + content.digest())
    return {"schema_version": 1, "revision": revision,
            "working_tree_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=root, stderr=subprocess.DEVNULL)),
            "source_sha256": digest.hexdigest(), "source_files": len(selected),
            "scope": "tracked repository inputs and nonignored framework source additions; private files and caches excluded"}


if __name__ == "__main__":
    try:
        print(json.dumps(capture(), sort_keys=True))
    except Exception:
        print(json.dumps({"result": "failed", "boundary": "source_identity"}))
        sys.exit(1)
