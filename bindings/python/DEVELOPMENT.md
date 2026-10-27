# SDK development and compatibility

The private binding has its own Cargo workspace and lockfile. The public Rust
workspace still contains 22 publishable crates. Run version checks from the root:

```sh
python3 scripts/python-sdk-version.py
cargo fmt --all -- --check
cargo fmt --manifest-path bindings/python/Cargo.toml -- --check
cargo clippy --manifest-path bindings/python/Cargo.toml --all-targets --all-features --locked -- -D warnings
```

Create a virtual environment, install maturin and the `[test]` extra, then build a
wheel. Install that wheel before running `python -m pytest bindings/python/tests`.
Run `ruff check bindings/python/python bindings/python/tests bindings/python/examples`
and `ruff format --check` on the same paths. Run `python -m mypy --config-file bindings/python/pyproject.toml bindings/python/python/runifold`.
The tests exercise actual native Agents and all five HTTP protocol selections on
loopback servers. No vendor credentials are needed.

Also run `maturin sdist --manifest-path bindings/python/Cargo.toml`, install the
archive in another environment with a fresh Cargo target directory, and execute
both examples from outside this checkout. A working editable install alone does
not establish that wheel or source packaging works.

When the Rust workspace version changes, run
`python3 scripts/python-sdk-version.py --write`, update the private Cargo lockfile,
and rebuild all artifacts. Exact path constraints prevent a wheel's declared
version from silently drifting away from its native runtime. Check
`client.environment()` and `client.snapshot()` when reproducing a report; version
alone does not identify an extension binary or its dependency/configuration state.
Never replace an already published version with a different artifact.

Python requires 3.10 or later and Pydantic 2.12 or later. Source builds require
Rust 1.88 or later, a C compiler and native platform tooling; SQLite is bundled.
The extension uses CPython's stable ABI from 3.10. Linux, macOS and Windows wheels
are separate builds; a local macOS validation does not qualify other platforms.
Free-threaded Python and alternative Python implementations are not qualified.

## Error handling

Catch `RunifoldError` and inspect `code`, `kind`, `retry_safety`, `run_id`, `usage`
and `diagnostics`. `Safe` means the native boundary regards replay as safe;
`Unknown` means do not automatically repeat a non-idempotent action. A cancelled
local request may retain unknown remote execution and charge state. The binding
preserves all five native retry classifications, including conservative classification of
queued cancellation. Do not infer replay safety from a Python exception class.
Configuration/type errors before native execution use `ValueError`/`TypeError`.
Client admission errors are typed and safe; shutdown of a still-running sync
callback or checkpoint startup operation can raise `TimeoutError`. Admission
capacity stays occupied until the blocking work actually returns.
`active_checkpoint_operations` reports queued/running checkpoint startup work.

The SDK selects native streaming delivery for its Client runtime, including
`agent.run()` which collects the canonical result. This allows `agent.stream()`
to expose actual incremental events. It does not synthesize deltas from a final
response. A custom base URL is the complete API prefix; a missing trailing slash is
normalized without removing its last path segment (e.g. `/v1` for OpenAI/Anthropic
and `/v1beta` for Gemini). Query and fragment syntax is retained for native
validation. HTTP failures include the safe native model category and corrective
actions without copying upstream messages.

## Supported surface

| Capability | First binding |
| --- | --- |
| OpenAI Responses / compatible Chat Completions | Native HTTP |
| Anthropic / Gemini / Ollama | Native HTTP |
| Asyncio Agent calls and incremental streams | Supported |
| Typed async/sync Python Tools, explicit effects | Supported |
| Pydantic structured results and rich canonical content | Supported |
| Per-run budgets/deadlines, bounded shared physical capacity | Supported |
| SQLite checkpoint, reopen, conservative recovery | Supported |
| Checkpointed streaming | Rejected |
| Shared budget ledger across unrelated calls | Not exposed |
| MCP, hosted provider tools, voice/realtime, child Agents | Not exposed |
| Synchronous Client, hosted service, TypeScript | Outside this delivery |

The explicit `runifold.testing` API uses the Rust scripted test provider. It is
not an automatic production fallback.

Linux CI builds x86_64 wheels in a manylinux_2_28 container and asks maturin to
reject wheels incompatible with PyPI platform policy. macOS and Windows use
native builds. The independent native lockfile is audited separately from the
Rust publication workspace. These checks do not publish packages externally.
