#!/usr/bin/env python3
"""Verify durable upgrades and actual historical checkpoint-reader rejection."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import time


ROOT = Path(__file__).resolve().parent.parent
PROBE = Path("crates/runifold-workflow/examples/checkpoint_schema_probe.rs")


def source_fingerprint() -> str:
    paths = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=ROOT
    ).decode().split("\0")
    digest = hashlib.sha256()
    for name in sorted(set(paths)):
        if not (name.startswith("crates/") or name in (
            "Cargo.toml", "Cargo.lock", "scripts/verify-workflow-upgrade.py"
        )):
            continue
        path = ROOT / name
        if path.is_file():
            digest.update(name.encode() + b"\0")
            digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def require(condition: bool, boundary: str) -> None:
    if not condition:
        raise RuntimeError(boundary)


def metadata(root: Path) -> dict:
    return json.loads(subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--format-version", "1", "--locked"], cwd=root
    ))


def workspace_version(data: dict) -> str:
    return next(package["version"] for package in data["packages"] if package["name"] == "runifold-workflow")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy-ref", default="v0.7.2")
    parser.add_argument("--output", type=Path, default=ROOT / "target/reliability-evidence/workflow-upgrade.json")
    args = parser.parse_args()
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.unlink(missing_ok=True)
    logs = output.parent / "workflow-upgrade-logs"
    logs.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    boundary = "source-identity"
    report = {
        "schema_version": 1, "suite": "runifold.workflow-upgrade", "result": "failed",
        "boundaries": [],
        "credential_material": "excluded", "application_payloads": "excluded",
    }

    def run(name: str, command: list[str], cwd: Path = ROOT) -> None:
        nonlocal boundary
        boundary = name
        with (logs / f"{name}.log").open("wb") as log:
            subprocess.run(command, cwd=cwd, stdout=log, stderr=subprocess.STDOUT, check=True)
        report["boundaries"].append(name)

    def inspect(executable: Path, checkpoint: Path) -> dict:
        return json.loads(subprocess.check_output([str(executable), "inspect", str(checkpoint)]))

    try:
        report.update({
            "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT).decode().strip(),
            "working_tree_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT)),
            "source_sha256": source_fingerprint(),
        })
        current = metadata(ROOT)
        target = Path(current["target_directory"])
        report["candidate_version"] = workspace_version(current)
        report["rustc"] = subprocess.check_output(["rustc", "--version"]).decode().strip()
        boundary = "resolve-historical-source"
        revision = subprocess.check_output([
            "git", "rev-parse", "--verify", "--end-of-options", f"{args.legacy_ref}^{{commit}}"
        ], cwd=ROOT).decode().strip()
        report["legacy_revision"] = revision
        run("checkpoint-schema-contract", ["cargo", "test", "-p", "runifold-workflow", "--lib", "checkpoint::tests", "--locked"])
        run("sqlite-upgrade", ["cargo", "test", "-p", "runifold-store-sqlite", "--features", "bundled", "--test", "workflow_upgrade", "--locked"])
        run("sqlite-session-schema", ["cargo", "test", "-p", "runifold-store-sqlite", "--features", "bundled", "--test", "durable_conversation", "unsupported_session_schemas", "--locked"])
        run("postgres-upgrade", ["cargo", "test", "-p", "runifold-store-postgres", "--test", "workflow_upgrade", "--locked"])
        run("postgres-session-schema", ["cargo", "test", "-p", "runifold-store-postgres", "--test", "live_durable_store", "session_schema_rejection", "--locked"])
        run("current-probe-build", ["cargo", "build", "-p", "runifold-workflow", "--example", "checkpoint_schema_probe", "--locked"])
        suffix = ".exe" if os.name == "nt" else ""
        with tempfile.TemporaryDirectory(prefix="runifold-upgrade-") as temporary:
            directory = Path(temporary)
            current_probe = directory / f"current-probe{suffix}"
            shutil.copy2(target / f"debug/examples/checkpoint_schema_probe{suffix}", current_probe)
            historical = directory / "historical"
            historical.mkdir()
            archive = directory / "source.tar"
            with archive.open("wb") as stream:
                subprocess.run(["git", "archive", "--format=tar", revision], cwd=ROOT, stdout=stream, check=True)
            boundary = "extract-historical-source"
            with tarfile.open(archive) as source:
                # Only ordinary source files and directories from the pinned
                # Git object enter the disposable checkout.
                for member in source.getmembers():
                    require(not Path(member.name).is_absolute() and ".." not in Path(member.name).parts, boundary)
                    require(member.isfile() or member.isdir(), boundary)
                source.extractall(historical, filter="data")
            destination = historical / PROBE
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / PROBE, destination)
            legacy_target = target / "upgrade-legacy"
            run("legacy-probe-build", ["cargo", "build", "-p", "runifold-workflow", "--example", "checkpoint_schema_probe", "--locked", "--target-dir", str(legacy_target)], historical)
            report["legacy_version"] = workspace_version(metadata(historical))
            legacy_probe = legacy_target / f"debug/examples/checkpoint_schema_probe{suffix}"
            cases = []
            for schema in (3, 4):
                boundary = f"reader-matrix-v{schema}"
                generated = json.loads(subprocess.check_output([str(current_probe), "produce", str(schema), str(directory)]))
                before = directory / f"v{schema}-before.json"
                after = directory / f"v{schema}-after.json"
                before_hash = hashlib.sha256(before.read_bytes()).hexdigest()
                after_hash = hashlib.sha256(after.read_bytes()).hexdigest()
                require(generated["source_schema"] == schema and generated["advanced_schema"] == 5, boundary)
                require(inspect(legacy_probe, before) == {"readable": True, "schema_version": schema}, boundary)
                require(inspect(current_probe, before) == {"readable": True, "schema_version": schema}, boundary)
                require(inspect(current_probe, after) == {"readable": True, "schema_version": 5}, boundary)
                require(inspect(legacy_probe, after) == {"readable": False, "schema_version": 5, "error_kind": "invalid_payload"}, boundary)
                require(hashlib.sha256(before.read_bytes()).hexdigest() == before_hash, boundary)
                require(hashlib.sha256(after.read_bytes()).hexdigest() == after_hash, boundary)
                cases.append({"source_schema": schema, "advanced_schema": 5, "legacy_before": "accepted", "legacy_after": "rejected", "current_before_and_after": "accepted", "readers_mutate_input": False})
            report["reader_matrix"] = cases
            report["boundaries"].append("historical-reader-rollback-rejection")
        boundary = "source-stability"
        require(source_fingerprint() == report["source_sha256"], boundary)
        report["source_unchanged"] = True
        report["result"] = "passed"
    except Exception:
        report["failed_boundary"] = boundary
        raise SystemExit(f"Workflow upgrade verification failed at {boundary}; inspect {logs}") from None
    finally:
        report["duration_seconds"] = round(time.monotonic() - started, 3)
        with tempfile.NamedTemporaryFile(mode="w", dir=output.parent, prefix="workflow-upgrade-", suffix=".tmp", delete=False) as stream:
            json.dump(report, stream, indent=2)
            stream.write("\n")
            temporary_report = Path(stream.name)
        try:
            os.replace(temporary_report, output)
        finally:
            temporary_report.unlink(missing_ok=True)
    print(f"Workflow upgrade verification passed; evidence: {output}")


if __name__ == "__main__":
    main()
