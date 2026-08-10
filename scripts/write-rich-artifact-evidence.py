#!/usr/bin/env python3
"""Write credential-free evidence for the rich Tool and artifact CI gate."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


REVISION_PATTERN = re.compile(r"(?:unknown|[0-9a-fA-F]{7,64})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--started", required=True, type=int)
    parser.add_argument("--finished", required=True, type=int)
    parser.add_argument("--rustc", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if REVISION_PATTERN.fullmatch(args.revision) is None:
        raise SystemExit("revision must be `unknown` or 7..64 hexadecimal characters")
    if args.finished < args.started:
        raise SystemExit("gate finish time precedes its start")

    evidence = {
        "schema_version": 1,
        "suite": "runifold.rich-tool-artifact-reliability",
        "result": "passed",
        "revision": args.revision,
        "duration_seconds": args.finished - args.started,
        "rustc": args.rustc,
        "boundaries": [
            "artifact-schema-size-and-integrity",
            "agent-rich-tool-result",
            "mcp-lossless-rich-result-and-sampling-history",
            "provider-native-and-canonical-projection",
            "sqlite-scoped-artifact-lifecycle",
            "postgres-scoped-concurrent-idempotency-expiry-and-delete",
        ],
        "credential_material": "excluded",
        "model_content": "excluded",
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
