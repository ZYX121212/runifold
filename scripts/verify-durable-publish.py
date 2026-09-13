#!/usr/bin/env python3
"""Exercise the actual reference executable across a forced process kill."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time


def main():
    binary = str(Path(sys.argv[1]).resolve())
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="runifold-publish-") as root:
        root = Path(root)

        def run(action, directory, succeeds=True):
            result = subprocess.run([binary, action, str(directory)], capture_output=True, text=True, timeout=30)
            if (result.returncode == 0) != succeeds:
                raise AssertionError(f"{action} returned {result.returncode}: {result.stderr}")
            return result.stdout

        normal = root / "normal"
        run("init", normal)
        assert "replayed=false; model_calls=1" in run("run", normal)
        assert "replayed=true; model_calls=0" in run("run", normal)

        denied = root / "denied"
        run("init", denied)
        run("revoked", denied, succeeds=False)
        assert not (denied / "published.json").exists()

        recovery = root / "recovery"
        run("init", recovery)
        child = subprocess.Popen([binary, "pause-after-publish", str(recovery)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 30
            while not (recovery / "published.ready").exists():
                if child.poll() is not None or time.monotonic() >= deadline:
                    raise AssertionError("publisher did not reach receipt-before-ack boundary")
                time.sleep(0.01)
            child.kill()
            assert child.wait(timeout=10) != 0
        finally:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)
        receipt = recovery / "published.json"
        original = receipt.read_bytes()
        run("revoked", recovery, succeeds=False)
        assert receipt.read_bytes() == original
        receipt.write_text('{"report":"different content"}')
        run("run", recovery, succeeds=False)
        receipt.write_bytes(original)
        assert "replayed=true; model_calls=0" in run("run", recovery)
        assert "replayed=true; model_calls=0" in run("run", recovery)
        assert receipt.read_bytes() == original

    evidence = {
        "schema_version": 1,
        "suite": "runifold.durable-publish-reference",
        "result": "passed",
        "revision": os.environ.get("RUNIFOLD_EVIDENCE_REVISION", "working-tree"),
        "duration_seconds": round(time.monotonic() - started, 3),
        "assertions": ["normal_publication", "committed_session_replay", "denied_dispatch_has_no_write",
                       "forced_process_kill_after_external_commit", "revocation_blocks_recovery",
                       "mismatched_receipt_remains_ambiguous", "reconciliation_without_republication"],
        "scope": "offline scripted model, local independent publisher, real SQLite and processes",
    }
    output = Path("target/reliability-evidence/durable-publish.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(evidence, indent=2) + "\n")
    print("Durable publication acceptance passed.")


if __name__ == "__main__":
    main()
