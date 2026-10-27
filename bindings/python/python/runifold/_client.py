"""Application entry points over a shared native ProviderRuntime."""
from __future__ import annotations

import asyncio
import hashlib
from importlib.metadata import version as installed_version
from pathlib import Path
import platform
import sys
from collections.abc import AsyncIterator, Sequence
from copy import deepcopy
import json
import math
import os
import re
from typing import Any, Generic, Literal, TypeVar, overload

from pydantic import TypeAdapter

from . import _native
from ._tools import Tool
from ._types import Budget, Event, Outcome, RunifoldError, encode

T = TypeVar("T")
Provider = Literal["openai", "compatible", "anthropic", "gemini", "ollama"]
Protocol = Literal["responses", "chat_completions"]


class Client:
    """Long-lived model client; its Agents share native route/capacity state."""

    def __init__(self, *, model: str, provider: Provider = "openai", api_key: str | None = None,
                 base_url: str | None = None, protocol: Protocol | None = None, provider_name: str | None = None,
                 model_concurrency: int = 4, request_queue: int = 128,
                 run_concurrency: int = 64, tool_concurrency: int = 16,
                 circuit_breaker: bool = True) -> None:
        if provider not in ("openai", "compatible", "anthropic", "gemini", "ollama"):
            raise ValueError("Unsupported provider")
        if api_key is None:
            variable = {"openai": "OPENAI_API_KEY", "compatible": "OPENAI_API_KEY", "anthropic": "ANTHROPIC_API_KEY", "gemini": "GEMINI_API_KEY", "ollama": "OLLAMA_API_KEY"}[provider]
            api_key = os.environ.get(variable)
        selected_protocol = protocol if protocol is not None else ("responses" if provider == "openai" else "chat_completions" if provider == "compatible" else "native")
        if provider_name is not None and provider != "compatible":
            raise ValueError("provider_name only applies to compatible endpoints")
        self._provider: str = provider
        self._native = _native.Runtime(encode({"provider": provider, "model": model,
            "api_key": api_key, "base_url": base_url, "protocol": selected_protocol, "provider_name": provider_name,
            "model_concurrency": model_concurrency, "request_queue": request_queue,
            "run_concurrency": run_concurrency, "tool_concurrency": tool_concurrency,
            "circuit_breaker": circuit_breaker}))

    @classmethod
    def from_env(cls, *, model: str | None = None, **options: Any) -> Client:
        selected = model if model is not None else os.environ.get("RUNIFOLD_MODEL")
        if selected is None:
            raise ValueError("Set RUNIFOLD_MODEL or pass model explicitly")
        options.setdefault("provider", os.environ.get("RUNIFOLD_PROVIDER", "openai"))
        options.setdefault("base_url", os.environ.get("RUNIFOLD_BASE_URL"))
        return cls(model=selected, **options)

    @overload
    def agent(self, name: str, *, output_type: type[T], system: str = "", tools: Sequence[Tool] = (), max_output_tokens: int | None = None) -> Agent[T]: ...
    @overload
    def agent(self, name: str, *, output_type: None = None, system: str = "", tools: Sequence[Tool] = (), max_output_tokens: int | None = None) -> Agent[None]: ...
    def agent(self, name: str, *, output_type: Any = None, system: str = "", tools: Sequence[Tool] = (), max_output_tokens: int | None = None) -> Agent[Any]:
        return Agent(self, name=name, system=system, tools=tools, output_type=output_type, max_output_tokens=max_output_tokens)

    def snapshot(self) -> dict[str, Any]:
        value: dict[str, Any] = json.loads(self._native.snapshot())
        for pool in value["model"]["pools"]:
            pool["reserved_tokens"] = int(pool["reserved_tokens"])
            pool["settled_tokens"] = int(pool["settled_tokens"])
        return value

    def close(self) -> None:
        """Signal all local Runs and prevent new work; effects may still complete."""
        self._native.close()

    def environment(self) -> dict[str, str]:
        """Return local version/artifact fingerprints without paths or credentials."""
        try:
            artifact = hashlib.sha256(Path(_native.__file__).read_bytes()).hexdigest()
        except OSError:
            raise RuntimeError("Could not fingerprint the installed native artifact") from None
        return {"sdk_version": installed_version("runifold-sdk"), "native_version": _native.__version__,
                "runifold_requirement": "=" + _native.__version__, "native_lock_sha256": _native.__lock_sha256__,
                "native_artifact_sha256": artifact, "python": ".".join(map(str, sys.version_info[:3])),
                "platform": sys.platform, "machine": platform.machine(), "pydantic": installed_version("pydantic")}

    async def aclose(self, *, timeout: float = 5.0, wait_for_callbacks: bool = True) -> None:
        """Close local execution, optionally waiting for actual callback completion."""
        self.close()
        await self._native.wait_closed(timeout, wait_for_callbacks)

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()

    def __repr__(self) -> str:
        return f"Client(provider={self._provider!r}, closed={self.snapshot()['closed']})"


class Agent(Generic[T]):
    """An immutable application definition; every invocation uses native Agent."""

    def __init__(self, client: Client, *, name: str, system: str, tools: Sequence[Tool], output_type: Any, max_output_tokens: int | None) -> None:
        selected = {tool.name: tool for tool in tools}
        if len(selected) != len(tools):
            raise ValueError("Tool names must be unique within an Agent")
        if not name.strip():
            raise ValueError("Agent name must not be empty")
        self._client = client
        self._tools = selected
        self._output_type = output_type
        self._config = encode({"name": name, "system": system, "tools": [tool.descriptor for tool in tools],
            "output_schema": TypeAdapter(output_type).json_schema() if output_type is not None else None,
            "output_name": getattr(output_type, "__name__", "output") if output_type is not None else None,
            "max_output_tokens": max_output_tokens})

    async def _dispatch(self, name: str, arguments: str, context: _native.ToolContext, lease: Any) -> str:
        return await self._tools[name]._invoke(arguments, context, lease)

    def start(self, input: str, *, budget: Budget | None = None, timeout: float | None = None,
              allowed_tools: Sequence[str] | None = None, _stream: bool = False,
              _checkpoint_path: str | None = None, _checkpoint_id: str | None = None,
              _resume: bool = False, _retry_interrupted: bool = False) -> Run[T]:
        loop = asyncio.get_running_loop()
        if timeout is not None and (not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("Timeout must be finite and positive")
        options = encode({"budget": (budget if budget is not None else Budget()).to_native(),
            "timeout": timeout, "stream": _stream, "allowed_tools": list(allowed_tools) if allowed_tools is not None else None,
            "checkpoint_path": _checkpoint_path, "checkpoint_id": _checkpoint_id,
            "resume": _resume, "retry_interrupted": _retry_interrupted})
        try:
            native = self._client._native.start(self._config, input, options, self._dispatch)
        except _native.NativeError as error:
            raise RunifoldError(json.loads(str(error))["error"]) from None
        return Run(native, self._output_type, loop)

    async def run(self, input: str, *, budget: Budget | None = None, timeout: float | None = None,
                  allowed_tools: Sequence[str] | None = None) -> Outcome[T]:
        return await self.start(input, budget=budget, timeout=timeout, allowed_tools=allowed_tools).result()

    def stream(self, input: str, *, budget: Budget | None = None, timeout: float | None = None,
               allowed_tools: Sequence[str] | None = None) -> Stream[T]:
        return Stream(self, input, budget, timeout, allowed_tools)

    async def run_checkpointed(self, input: str, *, path: str | os.PathLike[str], checkpoint_id: str | None = None,
                               budget: Budget | None = None, timeout: float | None = None) -> Outcome[T]:
        return await self.start(input, budget=budget, timeout=timeout,
            _checkpoint_path=os.fspath(path), _checkpoint_id=checkpoint_id).result()

    async def resume(self, *, path: str | os.PathLike[str], checkpoint_id: str,
                     budget: Budget | None = None, timeout: float | None = None,
                     policy: Literal["reject_ambiguous", "retry_interrupted_turn"] = "reject_ambiguous") -> Outcome[T]:
        if policy not in ("reject_ambiguous", "retry_interrupted_turn"):
            raise ValueError("Unsupported resume policy")
        return await self.start("", budget=budget, timeout=timeout, _checkpoint_path=os.fspath(path),
            _checkpoint_id=checkpoint_id, _resume=True, _retry_interrupted=policy == "retry_interrupted_turn").result()


class Run(Generic[T]):
    """One native run with explicit identity, cancellation and completion."""

    def __init__(self, native: _native.Run, output_type: Any, loop: asyncio.AbstractEventLoop) -> None:
        self._native = native
        self._output_type = output_type
        self._loop = loop
        self._outcome: Outcome[T] | None = None
        self._failure: RunifoldError | None = None

    @property
    def run_id(self) -> str:
        return self._native.run_id

    @property
    def checkpoint_id(self) -> str | None:
        return self._native.checkpoint_id

    def cancel(self) -> bool:
        return self._native.cancel()

    @property
    def usage(self) -> dict[str, int]:
        return json.loads(self._native.usage())  # type: ignore[no-any-return]

    async def _next(self) -> dict[str, Any] | None:
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("A Run must be consumed on its starting asyncio loop; reuse the Client for another loop")
        raw = await self._native.next()
        return json.loads(raw) if raw is not None else None

    def _terminal(self, message: dict[str, Any]) -> Outcome[T]:
        try:
            if message["type"] == "error":
                raise RunifoldError(message["error"])
            self._outcome = Outcome.from_native(message, self._output_type)
            return self._outcome
        except RunifoldError as error:
            self._failure = error
            raise
        finally:
            self._native.acknowledge_terminal()

    async def result(self) -> Outcome[T]:
        if self._outcome is not None:
            return self._outcome
        if self._failure is not None:
            raise self._failure
        try:
            while True:
                message = await self._next()
                if message is None:
                    raise RunifoldError({"message": "Run ended without a terminal result"})
                if message["type"] != "event":
                    return self._terminal(message)
        finally:
            if not self._native.finished:
                await self.aclose()

    async def aclose(self) -> None:
        self.cancel()
        await asyncio.shield(self._native.wait_closed())

    async def __aenter__(self) -> Run[T]:
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()


class Stream(AsyncIterator[Event[T]], Generic[T]):
    """A bounded native event stream; use async with for deterministic closure."""

    def __init__(self, agent: Agent[T], input: str, budget: Budget | None, timeout: float | None, allowed: Sequence[str] | None) -> None:
        self._agent = agent
        self._input = input
        self._budget = budget
        self._timeout = timeout
        self._allowed = tuple(allowed) if allowed is not None else None
        self._run: Run[T] | None = None
        self._done = False
        self.outcome: Outcome[T] | None = None

    def _start(self) -> Run[T]:
        if self._run is None:
            self._run = self._agent.start(self._input, budget=self._budget, timeout=self._timeout,
                allowed_tools=self._allowed, _stream=True)
        return self._run

    def __aiter__(self) -> Stream[T]:
        return self

    async def __anext__(self) -> Event[T]:
        if self._done:
            raise StopAsyncIteration
        run = self._start()
        try:
            message = await run._next()
            if message is None:
                self._done = True
                raise StopAsyncIteration
            if message["type"] == "event":
                canonical = message["event"]
                name = next(iter(canonical))
                kind = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
                return Event(type=kind, data=deepcopy(canonical[name]))
            self.outcome = run._terminal(message)
            self._done = True
            return Event(type="completed", data={"outcome": self.outcome.canonical}, outcome=self.outcome)
        except BaseException:
            await self.aclose()
            raise

    async def aclose(self) -> None:
        self._done = True
        if self._run is not None:
            await self._run.aclose()

    async def __aenter__(self) -> Stream[T]:
        self._start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()
