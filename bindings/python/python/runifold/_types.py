"""Typed application values; execution and accounting remain native."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from enum import Enum
import json
from typing import Any, Generic, Mapping, TypeVar

from pydantic import TypeAdapter, ValidationError

T = TypeVar("T")


def encode(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


class RetrySafety(str, Enum):
    SAFE = "Safe"
    UNSAFE = "Unsafe"
    UNKNOWN = "Unknown"


class RunifoldError(Exception):
    """A native failure with safe diagnostics and explicit retry safety."""

    def __init__(self, error: Mapping[str, Any]) -> None:
        self.code = str(error.get("code", "runifold.invocation"))
        self.kind = str(error.get("kind", "Internal"))
        self.retry_safety = RetrySafety(error.get("retry_safety", "Unknown"))
        self.run_id = error.get("run_id")
        self.usage = dict(error.get("usage", {}))
        self.diagnostics = dict(error.get("diagnostics", {}))
        super().__init__(f"{self.code}: {error.get('message', 'Execution failed')}")


@dataclass(frozen=True)
class Budget:
    """Limits for one run tree; reusing this value does not share its ledger."""

    tokens: int | None = None
    cost_microusd: int | None = None
    turns: int | None = 8
    tool_calls: int | None = 32
    delegations: int | None = None

    def __post_init__(self) -> None:
        for value in self.to_native().values():
            if value is not None and (type(value) is not int or not 0 <= value <= 2**64 - 1):
                raise ValueError("Budget limits must be nonnegative 64-bit integers or None")

    def to_native(self) -> dict[str, int | None]:
        return {"tokens": self.tokens, "cost_microusd": self.cost_microusd,
                "turns": self.turns, "tool_calls": self.tool_calls, "delegations": self.delegations}


@dataclass(frozen=True)
class Outcome(Generic[T]):
    """A complete native outcome, retaining transcript and provider data."""

    run_id: str
    text: str = field(repr=False)
    usage: Mapping[str, int]
    turns: int
    tool_calls: int
    checkpoint_id: str | None = None
    parsed: T | None = field(default=None, repr=False)
    _canonical: dict[str, Any] = field(default_factory=dict, repr=False)

    @property
    def canonical(self) -> dict[str, Any]:
        return deepcopy(self._canonical)

    @property
    def response(self) -> dict[str, Any]:
        return deepcopy(self._canonical["response"])

    @property
    def transcript(self) -> list[dict[str, Any]]:
        return deepcopy(self._canonical["transcript"])

    @classmethod
    def from_native(cls, message: dict[str, Any], output_type: Any) -> Outcome[Any]:
        native = message["outcome"]
        parsed = None
        if output_type is not None:
            try:
                parsed = TypeAdapter(output_type).validate_json(encode(message["parsed"]), strict=True)
            except ValidationError as error:
                # Pydantic's normal exception includes model output. Do not put
                # that payload into exception text or the chained traceback.
                raise RunifoldError({"code": "runifold.structured_output", "kind": "InvalidOutput",
                    "retry_safety": "Unsafe", "run_id": message["run_id"], "usage": native["usage"],
                    "message": "Structured response failed local type validation",
                    "diagnostics": {"validation.error_count": error.error_count()}}) from None
        return cls(run_id=message["run_id"], text=message["text"], usage=native["usage"],
                   turns=native["turns"], tool_calls=native["tool_calls"],
                   checkpoint_id=message.get("checkpoint_id"), parsed=parsed, _canonical=native)


@dataclass(frozen=True)
class Event(Generic[T]):
    """One canonical Agent event; model events retain their native type field."""

    type: str
    data: Mapping[str, Any] = field(repr=False)
    outcome: Outcome[T] | None = field(default=None, repr=False)

    @property
    def text_delta(self) -> str:
        model = self.data.get("event", {}) if self.type == "model" else {}
        return str(model.get("text", "")) if model.get("type") == "text_delta" else ""
