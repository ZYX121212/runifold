#!/usr/bin/env python3
"""Write non-sensitive evidence after the headless browser gate passes."""

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
    parser.add_argument("--browser", required=True)
    parser.add_argument("--driver", required=True)
    parser.add_argument("--wasm-bindgen", required=True)
    parser.add_argument("--turn-image", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.revision and REVISION_PATTERN.fullmatch(args.revision) is None:
        raise SystemExit("revision must be 7..64 hexadecimal characters")

    evidence = {
        "schema_version": 2,
        "suite": "runifold.wasm-browser-provider-reliability",
        "result": "passed",
        "revision": args.revision or None,
        "target": "wasm32-unknown-unknown",
        "rustc": args.rustc,
        "browser": args.browser,
        "webdriver": args.driver,
        "wasm_bindgen_test_runner": args.wasm_bindgen,
        "turn_relay": {
            "image": args.turn_image,
            "transport": "udp",
            "policy": "relay-only",
            "fault": "container-stop-restart",
        },
        "package": "runifold",
        "features": ["anthropic", "gemini", "ollama", "openai"],
        "providers": ["anthropic", "gemini", "ollama", "openai-compatible"],
        "wire_protocols": [
            "anthropic-messages-sse",
            "gemini-generate-content-sse",
            "ollama-chat-ndjson",
            "openai-responses-sse",
            "provider-native-embeddings-json",
            "openai-model-file-batch-http",
            "openai-realtime-ga-websocket",
            "openai-realtime-audio-websocket",
            "openai-realtime-client-secret-http",
            "openai-realtime-ga-webrtc",
        ],
        "credential_policy": "application-gateway-no-upstream-provider-credentials",
        "runtime_assertions": [
            "multi-provider-agent-streaming-fetch",
            "multi-provider-embedding-fetch",
            "ordered-embedding-results",
            "model-discovery",
            "multipart-file-upload",
            "batch-create-inspect-cancel",
            "realtime-session-update-text-response",
            "realtime-pcm-input-output-transcript",
            "realtime-client-secret-redaction",
            "bounded-realtime-browser-receive-queue",
            "realtime-webrtc-sdp-offer-answer",
            "realtime-webrtc-oai-events-state-machine",
            "realtime-webrtc-microphone-playback",
            "bounded-realtime-webrtc-data-channel",
            "realtime-local-stun-srflx-candidate",
            "realtime-turn-credential-redaction",
            "realtime-peer-ice-state",
            "realtime-reconnect-disposition",
            "realtime-automatic-reconnect-controller",
            "realtime-fresh-credential-per-attempt",
            "realtime-gateway-status-aware-reconnect",
            "realtime-pending-peer-failure-cleanup",
            "realtime-real-coturn-relay-only",
            "realtime-coturn-stop-ice-partition",
            "realtime-coturn-restart-peer-rebuild",
            "realtime-old-session-event-isolation",
            "cors-preflight",
            "no-authorization-header",
            "no-provider-api-key-header",
            "in-flight-cancellation",
            "deadline-classification",
            "multi-provider-deadline-classification",
            "rate-limit-retry-safety",
        ],
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
