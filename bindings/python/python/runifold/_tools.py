"""Typed callback adapters; native ToolRegistry owns all dispatch and policy."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import uuid
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Literal, get_type_hints

from pydantic import ConfigDict, TypeAdapter, ValidationError, create_model

from ._native import ToolContext
from ._types import RetrySafety, encode

Effect = Literal[
    "pure",
    "read_only",
    "idempotent_write",
    "non_idempotent_write",
    "destructive",
    "unknown",
]
Risk = Literal["low", "medium", "high", "critical"]
_ABSENT = object()


class ToolFailure(Exception):
    """An explicitly public application Tool failure; arbitrary exceptions are hidden."""

    def __init__(self, message: str, *, retry_safety: RetrySafety = RetrySafety.UNKNOWN) -> None:
        super().__init__(message)
        self.retry_safety = retry_safety


@dataclass(frozen=True)
class ToolOutput:
    """Rich canonical content, optionally accompanied by a structured JSON value."""

    content: list[dict[str, Any]] = field(repr=False)
    structured_content: Any = field(default=_ABSENT, repr=False)
    metadata: dict[str, Any] = field(default_factory=dict, repr=False)
    is_error: bool = False
    model_visible: bool = True

    @classmethod
    def json(cls, value: Any) -> ToolOutput:
        return cls(
            content=[
                {
                    "type": "text",
                    "text": value if isinstance(value, str) else encode(value),
                }
            ],
            structured_content=value,
        )

    def to_native(self) -> dict[str, Any]:
        value = {
            "content": self.content,
            "metadata": self.metadata,
            "is_error": self.is_error,
            "model_visible": self.model_visible,
        }
        if self.structured_content is not _ABSENT:
            value["structured_content"] = self.structured_content
        return value


class Tool:
    """A schema-bearing registration, invoked only through the native registry."""

    def __init__(
        self,
        function: Callable[..., Any],
        *,
        effect: Effect,
        risk: Risk,
        name: str | None,
        description: str | None,
        version: str,
        output_type: Any,
    ) -> None:
        selected_name = function.__name__ if name is None else name
        if not isinstance(selected_name, str) or not selected_name.strip():
            raise ValueError("Tool name must not be empty")
        if not isinstance(version, str) or not version.strip():
            raise ValueError("Tool version must not be empty")
        signature = inspect.signature(function)
        hints = get_type_hints(function, include_extras=True)
        fields: dict[str, Any] = {}
        context_name = None
        for key, parameter in signature.parameters.items():
            annotation = hints.get(key)
            if parameter.kind not in (
                parameter.POSITIONAL_OR_KEYWORD,
                parameter.KEYWORD_ONLY,
            ):
                raise TypeError("Tool inputs require named, annotated parameters")
            if annotation is ToolContext:
                if context_name is not None:
                    raise TypeError("A Tool may accept at most one ToolContext")
                context_name = key
                continue
            if annotation is None or key.startswith("_"):
                raise TypeError("Tool inputs require named, annotated parameters")
            fields[key] = (
                annotation,
                ... if parameter.default is parameter.empty else parameter.default,
            )
        if "return" not in hints and output_type is None:
            raise TypeError("Tool outputs require a return annotation or output_type")
        model = create_model(
            f"{function.__name__}Input", __config__=ConfigDict(extra="forbid"), **fields
        )
        returned = hints.get("return", Any)
        validator = (
            None
            if returned is ToolOutput
            else TypeAdapter(output_type if output_type is not None else returned)
        )
        schema = (
            TypeAdapter(output_type).json_schema()
            if output_type is not None
            else ({} if validator is None else validator.json_schema())
        )
        effects = {
            "pure": "Pure",
            "read_only": "ReadOnly",
            "idempotent_write": "IdempotentWrite",
            "non_idempotent_write": "NonIdempotentWrite",
            "destructive": "Destructive",
            "unknown": "Unknown",
        }
        risks = {
            "low": "Low",
            "medium": "Medium",
            "high": "High",
            "critical": "Critical",
        }
        if effect not in effects or risk not in risks:
            raise ValueError("Unsupported effect or risk classification")
        descriptor = {
            "name": selected_name,
            "version": version,
            "description": description
            if description is not None
            else inspect.getdoc(function) or function.__name__,
            "input_schema": model.model_json_schema(),
            "output_schema": schema,
            "effect": effects[effect],
            "risk": risks[risk],
            "metadata": {"python.binding": "runifold"},
        }
        identity = hashlib.sha256(
            json.dumps(descriptor, sort_keys=True, allow_nan=False).encode()
        ).hexdigest()
        descriptor["id"] = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"https://github.com/ZYX121212/runifold/python/tools/{identity}",
            )
        )
        self._descriptor = descriptor
        self._function = function
        self._inputs = model
        self._output = validator
        self._context_name = context_name

    @property
    def name(self) -> str:
        return str(self._descriptor["name"])

    @property
    def descriptor(self) -> dict[str, Any]:
        return deepcopy(self._descriptor)

    def __repr__(self) -> str:
        return f"Tool(name={self.name!r})"

    async def _invoke(self, serialized: str, context: ToolContext, lease: Any) -> str:
        transferred = False
        try:
            try:
                validated = self._inputs.model_validate_json(serialized, strict=True)
            except ValidationError:
                return _error(
                    "InvalidInput",
                    "Tool arguments failed type validation",
                    RetrySafety.SAFE,
                )
            arguments = {name: getattr(validated, name) for name in type(validated).model_fields}
            if self._context_name is not None:
                arguments[self._context_name] = context
            if inspect.iscoroutinefunction(self._function):
                result = await self._function(**arguments)
            else:

                def invoke_sync() -> Any:
                    try:
                        try:
                            return self._serialize_output(self._function(**arguments))
                        except ToolFailure as error:
                            return _error("Execution", str(error), error.retry_safety)
                        except Exception:
                            return _error(
                                "Execution",
                                "Python Tool callback failed; inspect application handling",
                                RetrySafety.UNKNOWN,
                            )
                    finally:
                        lease.release()

                # The worker owns the lease through actual completion, even if
                # cancellation stops this await. No new worker bypasses capacity.
                transferred = True
                return str(await asyncio.to_thread(invoke_sync))
            return self._serialize_output(result)
        except ToolFailure as error:
            return _error("Execution", str(error), error.retry_safety)
        except Exception:
            return _error(
                "Execution",
                "Python Tool callback failed; inspect application handling",
                RetrySafety.UNKNOWN,
            )
        finally:
            if not transferred:
                lease.release()

    def _serialize_output(self, result: Any) -> str:
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            return _error(
                "InvalidOutput",
                "Tool returned an awaitable; await it inside an async Tool",
                RetrySafety.UNKNOWN,
            )
        if isinstance(result, ToolOutput):
            output = result
        else:
            if self._output is None:
                return _error(
                    "InvalidOutput",
                    "Rich Tools must return ToolOutput",
                    RetrySafety.UNKNOWN,
                )
            try:
                value = self._output.validate_python(result, strict=True)
                output = ToolOutput.json(self._output.dump_python(value, mode="json"))
            except ValidationError:
                return _error(
                    "InvalidOutput",
                    "Tool result failed type validation",
                    RetrySafety.UNKNOWN,
                )
        try:
            return encode({"type": "output", "output": output.to_native()})
        except (TypeError, ValueError):
            return _error(
                "InvalidOutput",
                "Tool result must be canonical content and finite JSON",
                RetrySafety.UNKNOWN,
            )


def _error(kind: str, message: str, safety: RetrySafety) -> str:
    # The legacy SDK-only Unsafe value has no native wire variant. Translate
    # it conservatively; never weaken it to Safe or lose the public failure.
    native_safety = RetrySafety.UNSAFE_AFTER_SIDE_EFFECT if safety is RetrySafety.UNSAFE else safety
    return encode(
        {
            "type": "error",
            "error": {
                "kind": kind,
                "message": message,
                "retry_safety": native_safety.value,
                "metadata": {},
            },
        }
    )


async def _await_concurrent(future: Any) -> Any:
    return await asyncio.wrap_future(future)


def tool(
    *,
    effect: Effect,
    risk: Risk = "low",
    name: str | None = None,
    description: str | None = None,
    version: str = "1",
    output_type: Any = None,
) -> Callable[[Callable[..., Any]], Tool]:
    """Declare a typed Tool and its effect explicitly; execution stays native."""

    def decorate(function: Callable[..., Any]) -> Tool:
        return Tool(
            function,
            effect=effect,
            risk=risk,
            name=name,
            description=description,
            version=version,
            output_type=output_type,
        )

    return decorate
