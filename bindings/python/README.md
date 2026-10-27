# Runifold Python SDK

An in-process Python adapter over Runifold's Rust runtime. Agent execution, Tool
authorization, budgets, routing, retry and SQLite recovery use the existing native
contracts. There is no required gateway service or Python Agent loop.

Prebuilt wheels and source archives are delivered through the repository GitHub Releases. It has not been published to PyPI.
Distribution name: `runifold-sdk`; import name: `runifold`.

## Build and install

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install 'maturin>=1.12,<2'
maturin build --release --locked --manifest-path bindings/python/Cargo.toml
python -m pip install bindings/python/target/wheels/runifold_sdk-*.whl
```

The build requires the Rust version declared in the native manifest. Installing
a matching built wheel requires no Rust toolchain. Platform compatibility is
established by the wheel CI matrix, not by this source checkout alone.

## First offline application

```python
import asyncio
from runifold import Budget, tool
from runifold.testing import scripted_client, text, tool_call

@tool(effect="pure")
async def greet(name: str) -> str:
    return f"Hello, {name}!"

async def main():
    async with scripted_client(tool_call("greet", {"name": "Ada"}), text("Hello, Ada!")) as client:
        agent = client.agent("assistant", tools=[greet])
        async with agent.stream("Greet Ada", budget=Budget(turns=2, tool_calls=1), timeout=10) as events:
            async for event in events:
                print(event.text_delta, end="", flush=True)
        assert events.outcome is not None
        print(events.outcome.usage)

asyncio.run(main())
```

`testing` uses the Rust ScriptedModel deliberately. Production clients never
silently replace a provider with test output.

## Provider-backed application

```python
from runifold import Client

client = Client.from_env()  # RUNIFOLD_MODEL; provider-specific API key
agent = client.agent("assistant", system="Answer precisely.")
result = await agent.run("Explain structured cancellation.", timeout=30)
print(result.text)
await client.aclose()
```

Supported native HTTP adapters are OpenAI, OpenAI-compatible endpoints,
Anthropic, Gemini and Ollama. Configure `provider`, `model`, optional `base_url`
and credentials explicitly. OpenAI defaults to Responses; compatible endpoints
default to Chat Completions. `protocol` applies only to that protocol family.
API prefixes accept a trailing slash or no trailing slash; the full prefix is retained.
Clients and their Agents share request capacity and route/circuit state. Set
`circuit_breaker=False` when independent tasks must not share a circuit breaker.

## Lifetime and safety

- Reuse a Client; construct it outside or inside an asyncio loop. Each Run belongs
  to the loop on which it starts. Reusing a Client across separate loops creates
  fresh per-call callback context.
- `Budget` is a per-run limit value, not a shared ledger across separate calls.
  Costs use integer micro-US-dollars. Client request limits cover physical model
  attempts across its Agents. Run and callback concurrency are bounded separately.
- A Run or Stream has one active consumer. A second consumer or access from a
  different event loop raises an error before touching the owner's cancellation.
  A closed Stream cannot be restarted. Close arguments are validated before the
  Client changes state.
- Use `async with agent.stream(...)` or `await stream.aclose()` when leaving a
  stream early. Cancelling an await signals its native run. Local cancellation
  does not prove that a remote request stopped or incurred no charge.
- Async Tool callbacks receive cancellation on their owning loop. Synchronous
  callbacks run in a worker thread; that thread may finish after local cancellation.
  Its native callback lease remains occupied until it actually returns. Client
  shutdown can time out rather than claim a still-running callback stopped.
  Blocking SQLite startup also retains native admission capacity until it returns;
  `snapshot()["active_checkpoint_operations"]` reports that work.
- Tool names/versions must be nonempty. Inputs must be named and annotated;
  ordinary underscore-prefixed inputs are rejected, while `_context: ToolContext`
  is a supported context injection. Await asynchronous work inside an async Tool;
  returning a coroutine as a result does not execute it.
- Declare Tool effects explicitly. Ordinary Python exceptions are hidden from
  model/operator messages. `ToolFailure` is an explicit public error contract.
- `Outcome.canonical`, `.response` and `.transcript` retain rich content and
  namespaced provider data; `.text` is only a convenience view.
- `Client.snapshot()` reports payload-free native accounting and effective policy.
  `Client.environment()` fingerprints the installed extension and native lockfile
  without exposing paths or credentials.

## Recovery

`agent.run_checkpointed(input, path="state.sqlite")` returns a checkpoint ID.
`agent.resume(path="state.sqlite", checkpoint_id=...)` reopens it through the
canonical native Agent API and restores persisted usage. The default policy is
`reject_ambiguous`; `retry_interrupted_turn` requires an explicit choice.
Checkpointed streaming is not exposed by the native checkpoint API and is
rejected instead of silently losing persistence. Remote workflows, child-Agent
delegation, hosted services and TypeScript are outside this first binding.

See [development, upgrades, errors and the support matrix](DEVELOPMENT.md).
Run `python bindings/python/examples/offline.py` and `python bindings/python/examples/recovery.py` after installation.

The retry-safety enum preserves the native `RequiresIdempotency`,
`UnsafeAfterVisibleOutput` and `UnsafeAfterSideEffect` distinctions. The original
`UNSAFE` member remains for SDK-local validation failures; explicit Tool failures
using that legacy value cross the native boundary conservatively as
`UnsafeAfterSideEffect`. None of these values authorize an automatic replay.
