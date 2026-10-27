# RFC 0079: Application SDKs over the canonical runtime

- Status: Accepted — independent Python boundary approved on 2026-10-27
- Date: 2026-10-27

## Current architecture

The 22-crate Rust workspace already exposes ProviderRuntime, AgentBuilder,
Agent, typed Tools, structured outputs, streams, budgets, cancellation and durable
execution through the runifold facade. Application examples and AI recipes exist.
There is no Python package, native Python binding or Python distribution gate.
The product charter excludes a mandatory proxy service and a second Agent loop.

## Proposed directory boundary

Keep all existing Rust crate/module ownership and public execution paths.
Improve application examples, diagnostics guidance and compiled recipes in their
existing locations. Add one isolated `bindings/python/` distribution:

```text
bindings/python/
  Cargo.toml                 # private native extension, independent workspace
  Cargo.lock
  pyproject.toml             # maturin distribution, version from native manifest
  src/                       # Python adaptation over existing Rust contracts
  python/runifold/            # typed Python application API and py.typed
  tests/                     # installed-wheel, asyncio and loopback contracts
  examples/                  # runnable application and offline acceptance
```

The binding depends on the existing facade/provider/testkit crates. It is not
published as a twenty-third crates.io crate and does not change the Rust workspace
publication graph. A source-controlled sync/check command derives its version and
internal dependency constraints from the root Cargo.toml; mismatches fail CI.
Wheel and source distributions must both build and install in clean environments.

No existing crate, module or source file is moved. The new boundary owns only
language adaptation, Python task integration, packaging and Python typing.
It cannot implement routing, Tool dispatch, budgets, retry or an Agent loop.

## Python execution contract

- Construct one long-lived native ProviderRuntime per client and reuse it.
- Python Agent calls delegate to the existing Agent run/stream/checkpoint paths.
  Per-call Python event-loop context is captured at invocation, rather than bound
  permanently to whichever loop constructed the client.
- Typed Python Tools export schemas and register normal Rust Tool implementations.
  Registration explicitly grants only their matching capability descriptors.
  Sync and async callbacks retain structured JSON/rich results; unsupported values
  are rejected rather than converted to arbitrary strings.
- Structured output uses the canonical output contract and Python-side validation.
- Preserve canonical content and namespaced provider information in full results.
  Text convenience access does not replace access to the full result.
- Budgets, deadlines, request scheduling and settlement use the native contracts.
  A cancelled Python await/closed stream cancels its native run and releases local
  resources. It never establishes remote termination or a known final charge.
- Error categories, codes, retry safety and safe lifecycle diagnostics cross the
  boundary as typed fields. Credential-bearing configuration and arbitrary upstream
  messages are excluded from ordinary exception/configuration formatting.
- SQLite-backed checkpoint/recovery uses the existing persisted identity and
  schema checks. Reopening a process must recover through canonical APIs.
- Explicit offline testing uses the existing scripted Model/testkit contracts.
  Production configuration never silently substitutes an offline model.

## First delivery and exclusions

Deliver Rust application examples/recipes and one Python package supporting model
calls, typed Tools, structured results, streams, explicit budgets/deadlines,
cancellation, safe diagnosis, shared native runtimes and local durable recovery.
Publish a support matrix for the native HTTP providers and optional advanced
capabilities. Unsupported combinations must fail clearly.

TypeScript, browser bindings, a remote service, hosted execution and a new
orchestration engine are outside this delivery. No PyPI publication is claimed by
merely building a local wheel; external publication is a separate verified action.

## Acceptance

1. A fresh application installs the built package and completes the documented
   Tool + stream + budget + cancellation example without reading internals.
2. Installed-wheel tests execute real Rust Agent code against local HTTP fixtures;
   Python mocks alone cannot qualify the binding.
3. Concurrent calls, separate asyncio loops, callback failures, task cancellation,
   stream closure and timeout races preserve run identity and native resource
   ownership; no late callback may silently repeat a non-idempotent write.
4. Native and Python results agree on canonical content, usage, error categories,
   retry safety and lifecycle/unknown-outcome facts.
5. Durable execution is reopened and resumed, with incompatible schemas rejected.
6. Distribution version, included native runtime version, type markers, source
   distribution contents and isolated installation are verified automatically.
7. Existing Rust crate tests, feature boundaries, strict Clippy/rustdoc, AI recipe
   checks and publication invariants continue to pass. Native extension checks use
   their own Cargo output directory and lockfile.

## Serialization regression found during implementation

Installed-extension tests found that serde mapped a present structured JSON null
to an absent Tool output during Effect deserialization. ToolOutput and ToolResult
now preserve this distinction without changing the wire shape or public fields;
local and installed-native tests cover both absent and explicit null channels.

## Implementation basis

Use PyO3 for native adaptation, maturin for mixed Python/Rust packaging and the
supported Rust/Python async bridge. Their documented cancellation and task-local
behavior is an integration input, not proof of Runifold cancellation semantics;
the installed-wheel contracts above must establish the latter.

- https://pyo3.rs/main/ecosystem/async-await.html
- https://github.com/PyO3/pyo3-async-runtimes
- https://www.maturin.rs/project_layout.html
