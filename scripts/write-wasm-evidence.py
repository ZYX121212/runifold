#!/usr/bin/env python3
"""Write non-sensitive evidence after the WASM build and runtime gates pass."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


REVISION_PATTERN = re.compile(r"[0-9a-fA-F]{7,64}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--revision", default="")
    parser.add_argument("--rustc", required=True)
    parser.add_argument("--node", required=True)
    parser.add_argument("--wasm-bindgen", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.revision and REVISION_PATTERN.fullmatch(args.revision) is None:
        raise SystemExit("revision must be 7..64 hexadecimal characters")

    evidence = {
        "schema_version": 1,
        "suite": "runifold.wasm-edge-reliability",
        "result": "passed",
        "revision": args.revision or None,
        "target": "wasm32-unknown-unknown",
        "rustc": args.rustc,
        "node": args.node,
        "wasm_bindgen_test_runner": args.wasm_bindgen,
        "facade_package": "runifold",
        "facade_features": "no-default-features",
        "runtime_package": "runifold-core",
        "runtime_test": "wasm_edge",
        "runtime_assertions": [
            "uuid-v7-identity",
            "child-authority-attenuation",
            "hierarchical-cancellation",
            "atomic-budget-rejection",
        ],
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
