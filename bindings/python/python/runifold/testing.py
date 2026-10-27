"""Explicit offline fixtures backed by the Rust ScriptedModel testkit."""
from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from . import _native
from ._client import Client
from ._types import encode


def text(value: str, *, chunks: Sequence[str] | None = None, model: str = "test",
         input_tokens: int = 1, output_tokens: int = 1, metadata: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """One canonical streamed response; it performs no network or provider work."""
    pieces = list(chunks) if chunks is not None else [value]
    if "".join(pieces) != value:
        raise ValueError("Text chunks must assemble to the declared final text")
    return [{"type": "response_started", "id": None, "model": {"provider": "scripted", "name": model}},
            {"type": "content_block_started", "index": 0, "kind": {"type": "text"}},
            *[{"type": "text_delta", "index": 0, "text": piece} for piece in pieces],
            {"type": "content_block_completed", "index": 0},
            _usage(input_tokens, output_tokens),
            {"type": "response_completed", "finish_reason": "stop", "provider_metadata": metadata or {}}]


def tool_call(name: str, arguments: dict[str, Any], *, id: str = "call_1", model: str = "test") -> list[dict[str, Any]]:
    """One native model turn requesting a registered Tool."""
    return [{"type": "response_started", "id": None, "model": {"provider": "scripted", "name": model}},
            {"type": "content_part_completed", "index": 0, "part": {
                "type": "tool_call", "id": id, "name": name, "arguments": arguments,
                "raw_arguments": None, "metadata": {}}},
            _usage(1, 1),
            {"type": "response_completed", "finish_reason": "tool_calls", "provider_metadata": {}}]


def _usage(input_tokens: int, output_tokens: int) -> dict[str, Any]:
    return {"type": "usage_updated", "usage": {"input_tokens": input_tokens, "output_tokens": output_tokens,
            "reasoning_tokens": 0, "cached_input_tokens": 0, "cache_write_tokens": 0, "cost_microusd": 0}}


def scripted_client(*responses: Sequence[dict[str, Any]], delay_ms: int = 0, model: str = "test",
                    model_concurrency: int = 4, request_queue: int = 128,
                    run_concurrency: int = 64, tool_concurrency: int = 16) -> Client:
    """Construct an offline Client explicitly, using the real native execution path."""
    client = Client.__new__(Client)
    client._provider = "scripted"
    config = {"provider": "scripted", "provider_name": None, "model": model, "api_key": None,
              "base_url": None, "protocol": "native", "model_concurrency": model_concurrency,
              "request_queue": request_queue, "run_concurrency": run_concurrency,
              "tool_concurrency": tool_concurrency, "circuit_breaker": False}
    client._native = _native.Runtime.scripted(encode(config), encode(responses), delay_ms)
    return client


def recorded_requests(client: Client) -> list[dict[str, Any]]:
    """Inspect full requests only for an explicitly scripted testing Client."""
    import json
    return json.loads(client._native.recorded_requests())  # type: ignore[no-any-return]
