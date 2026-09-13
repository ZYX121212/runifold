#!/usr/bin/env python3
"""Writes credential-free reliability evidence for a bounded soak run."""

import argparse
import json
import re
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--started", required=True, type=int)
    parser.add_argument("--finished", required=True, type=int)
    parser.add_argument("--iterations", required=True, type=int)
    parser.add_argument("--result", choices=["passed", "failed"], default="passed")
    parser.add_argument("--exit-code", type=int, default=0)
    parser.add_argument("--suite", default="complete")
    parser.add_argument("--log")
    arguments = parser.parse_args()
    if arguments.finished < arguments.started:
        raise ValueError("soak finish time precedes its start")
    if arguments.iterations < 0 or (arguments.result == "passed" and arguments.iterations < 1):
        raise ValueError("soak must complete at least one full iteration")
    if (arguments.exit_code == 0) != (arguments.result == "passed"):
        raise ValueError("result and process exit code disagree")
    # Only test identifiers and numeric summaries enter artifacts. Never copy
    # panic payloads, configured connection URLs, model text, or raw logs.
    log = Path(arguments.log).read_text(errors="replace") if arguments.log and Path(arguments.log).exists() else ""
    report = {
        "schema_version": 2,
        "suite": "runifold.multi-hour-soak",
        "result": arguments.result,
        "exit_code": arguments.exit_code,
        "last_suite": arguments.suite,
        "failed_tests": re.findall(r"^test ([A-Za-z0-9_:]+) \.\.\. FAILED$", log, re.MULTILINE),
        "test_summaries": re.findall(r"^test result: (?:ok|FAILED)\. \d+ passed; \d+ failed; \d+ ignored;", log, re.MULTILINE),
        "revision": arguments.revision,
        "duration_seconds": arguments.finished - arguments.started,
        "iterations": arguments.iterations,
        "boundaries": [
            "postgres_conversation_checkpoint_effect_workflow",
            "postgres_restart_and_lease_recovery",
            "effect_ambiguity_and_reconciliation",
            "sqlite_session_admission_summary_and_process_recovery",
            "provider_http_fragmentation_timeout_and_control_plane",
        ],
        "credential_material": "excluded",
        "model_content": "excluded",
    }
    output = Path(arguments.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
