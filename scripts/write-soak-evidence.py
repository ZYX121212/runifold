#!/usr/bin/env python3
"""Writes credential-free reliability evidence for a bounded soak run."""

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--started", required=True, type=int)
    parser.add_argument("--finished", required=True, type=int)
    parser.add_argument("--iterations", required=True, type=int)
    arguments = parser.parse_args()
    if arguments.finished < arguments.started:
        raise ValueError("soak finish time precedes its start")
    if arguments.iterations < 1:
        raise ValueError("soak must complete at least one full iteration")
    report = {
        "schema_version": 1,
        "suite": "runifold.multi-hour-soak",
        "result": "passed",
        "revision": arguments.revision,
        "duration_seconds": arguments.finished - arguments.started,
        "iterations": arguments.iterations,
        "boundaries": [
            "postgres_conversation_checkpoint_effect_workflow",
            "postgres_restart_and_lease_recovery",
            "effect_ambiguity_and_reconciliation",
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
