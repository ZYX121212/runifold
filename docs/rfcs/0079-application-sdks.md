# RFC 0079: Application SDKs over the canonical runtime

- Status: Accepted — independent Python boundary approved on 2026-10-27
- Date: 2026-10-27

## Current architecture

The 22-crate Rust workspace already exposes ProviderRuntime, AgentBuilder,
Agent, typed Tools, structured outputs, streams, budgets, cancellation and durable
execution through the runifold facade. Application examples and AI recipes exist.
At proposal time there was no Python package, native Python binding or Python distribution gate.
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

## Host boundary hardening

A Run/Stream has one active consumer on its starting asyncio loop. Rejecting a
second consumer or a different loop must not cancel the original execution.
Closing an unstarted Stream is terminal. Invalid shutdown arguments must fail
before changing Client state; a closed Client takes precedence over admission
pressure. Tool identity and Python-callable signature errors fail at registration.
Returned coroutine objects are closed without executing hidden work.
Mutable usage projections cannot modify canonical execution records.
Output serialization failures retain the native InvalidOutput category in durable
Effect receipts while preserving the canonical Tool-error policy.

The Python retry enum carries all native states, including idempotency-required,
unsafe-after-output and unsafe-after-effect. Retain the first binding's local
`Unsafe` value for compatibility and translate it conservatively for Tool error
serialization. Preserve safe model error categories and native remediation hints.

Normalize a missing slash on API prefixes without discarding native endpoint
validation. Installed HTTP tests cover both prefix spellings and partial-output
failure without replay. Linux wheel qualification uses manylinux_2_28 and a
PyPI compatibility audit; private native dependencies have their own audit gate.

## Implementation basis

Use PyO3 for native adaptation, maturin for mixed Python/Rust packaging and the
supported Rust/Python async bridge. Their documented cancellation and task-local
behavior is an integration input, not proof of Runifold cancellation semantics;
the installed-wheel contracts above must establish the latter.

- https://pyo3.rs/main/ecosystem/async-await.html
- https://github.com/PyO3/pyo3-async-runtimes
- https://www.maturin.rs/project_layout.html

## Native application capability contracts

The following additions address large evidence review, bounded completion, and
safe window continuation in the existing Rust runtime. They do not add an
application retry engine or change business acceptance requirements.

### Lossless review bounds

`ReviewLimits::new(request_bytes, feedback_bytes)` specifies inclusive byte bounds
on complete serialized request JSON and canonical repair-feedback JSON. Defaults
remain 1 MiB and 64 KiB. There is no truncation, summarization, or automatic
approval when a bound is exceeded. Zero denies nonempty payloads. Applications
must choose finite bounds appropriate to their memory and model context limits;
raising these bounds does not raise provider limits or compact history.

Configure both the Agent's `TerminalReviewPolicy::with_limits` (or
`TurnReviewPolicy::with_limits`) and each built-in review adapter's `with_limits`.
Custom reviewers construct large verdicts with
`TerminalReviewVerdict::repair_with_limits(original_feedback, limits)` and can
validate requests using `validate_with_limits`. The Agent independently validates
both inputs and returned verdicts against its configured policy, including custom
reviewers. Compositions also validate their complete merged feedback. Checkpoint
review policies include the limits; changing them rejects recovery against the
old policy. Existing serialized policies deserialize with the original defaults.

Feedback values, transcript messages, and candidates retain their original
content. XML escaping used in the model-facing review instruction is reversible;
it does not grant reviewer data system-instruction authority. A later request
includes the accumulated transcript and feedback, so its complete request must
also fit the configured bound.

### Length completion continuation

`CompletionRequirement::new().max_repairs(n).retry_truncated_response(true)` opts
into native regeneration after a known terminal response with finish reason
`Length`. Default behavior remains unchanged. The original partial assistant
message is retained before the runtime repair instruction. The next model turn
must produce a complete replacement satisfying the original request, including a
complete structured value when structured output is configured. The runtime does
not concatenate fragments and assert that the result is complete.

Length, empty-response, and structured-output repairs consume the same existing
`terminal_repairs` counter. There is no additional allowance. Normal turn, token,
cost, deadline, and cancellation checks apply to every replacement. Partial usage
is accounted before regeneration. Truncated Tool calls still fail before Tool
execution or reviewer approval; they are never replayed as actionable calls.
Refusals are not repairable. Unknown request outcomes are model errors and never
enter this completion-repair path. Exhausted Length repairs return
`AgentError::TruncatedTerminalResponse` and persist a terminal failure checkpoint;
resume cannot reset the allowance. Completion policy is part of checkpoint
execution identity.

### Physical route settlement receipts

The native router emits schema-1 `ModelRouteReceipt` under
`runifold.router.receipt` in terminal response metadata and routing-error
metadata. The Agent persists that same receipt as `route_receipt` on journal
`model.completed` and `model.failed` events. These existing event names are the
supported journal equivalent; a separate `route.completed` event is not required.
Direct model injection without the native router has no route receipt and must
not be interpreted as proof of settlement.

Each receipt lists physical attempts in order: route, physical target, physical
invocation identity (when dispatched or admitted to an attempt), attempt number,
normalized error kind, retry safety, safe diagnostics, and reported usage when
available. No prompt, research content, provider error body, or credentials are
included. Earlier failures remain in the successful fallback receipt. Failed
attempt usage is retained even when a later attempt succeeds, and known failed
usage is charged to the run budget separately from the selected response.

`ModelRequestOutcome` distinguishes:

- `Completed`: the runtime observed a terminal response, including `Length`.
  Physical completion alone does not establish business acceptance.
- `NotStarted`: the router rejected an expired scope, a circuit prevented entry,
  or scheduler admission diagnostics establish that the provider was not invoked.
- `Unknown`: dispatch or remote completion cannot be ruled out. A real HTTP 400
  after provider invocation remains unknown under the conservative contract.

`ModelRouteReceipt::permits_continuation()` fails closed for empty receipts,
unknown schema versions, and any unknown attempt, including earlier failures
followed by successful fallback. `RetrySafety::Safe` alone is not settlement
proof. Absent usage means unknown cost, not zero. Applications must additionally
require the complete expected invocation set, enforce business acceptance, and
preserve unknown fees. Cancellation/drop can prevent receipt delivery; a missing
receipt must block a receipt-dependent continuation. Receipts describe local
runtime observations, not independently verified remote billing or termination.

Regression evidence is executable in
`crates/runifold/tests/application_capability_blockers.rs`, the Agent's local
completion/review tests, and the Model router tests. The facade regression uses a
real native Agent and local HTTP cassette to verify successful receipt production,
no dispatch from an expired context, a subsequent HTTP 400 blocking continuation,
and complete Length regeneration with original evidence and cumulative usage.

`ModelCallContext::route_receipt()` also exposes the native invocation snapshot.
Cloned contexts share observations; child attempts receive independent snapshots.
The router records a pending Unknown attempt before invoking a physical provider
and updates observed usage and final disposition in the same canonical stream.
The Agent and default Model collector attach this snapshot when their outer
cancellation wins, preserving earlier failures and known usage even when the
router cannot deliver a terminal event. Dropping a stream leaves dispatched
attempts Unknown. This does not grant retry authority or certify remote shutdown.

### Inspecting HTTP rejection reasons

OpenAI-compatible and Anthropic protocols return `error.message`, Gemini returns
`error.message`, and Ollama returns an `error` string. Model HTTP adapters retain
these fields in private `provider.error.message` metadata, exposed by
`ModelError::provider_message()`. This is untrusted upstream text and may echo
private request data. It is intentionally excluded from Display, safe diagnostics
and route receipts. `AgentError::to_run_error` removes it before durable error
normalization. Complete ModelError Debug/serialization includes private metadata.
No configuration is needed to retain or explicitly read the reason:

```rust,ignore
if let Some(reason) = error.provider_message() {
    // Inspect locally or show in an access-controlled diagnostic view.
    // Do not forward to shared logs, telemetry or model history.
}
```

Messages are limited to 16 KiB at a UTF-8 boundary. The metadata flag
`provider.error.message_truncated` explicitly identifies shortening. Empty,
non-string, missing and non-JSON error bodies produce `None`; the SDK never
invents a server reason. HTTP status and retry authority remain independent.

Protocol references: [OpenAI errors](https://developers.openai.com/api/docs/guides/error-codes),
[Claude error shapes](https://platform.claude.com/docs/en/api/errors),
[Gemini errors](https://ai.google.dev/gemini-api/docs/api-errors),
[Ollama errors](https://docs.ollama.com/api/errors).
