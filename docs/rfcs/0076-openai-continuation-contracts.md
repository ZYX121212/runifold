# RFC 0076: OpenAI continuation and explicit remote lifecycle

- Status: implemented with the boundaries below; live vendor acceptance remains opt-in
- Scope: `runifold-model`, the canonical Agent history path and `runifold-providers::openai`
- Official contract review: 2026-10-14

## Problem and decision

Calling a new model name is insufficient to support its continuation semantics.
Responses assistant phases, namespace-qualified tools, custom tool input and
remote job identity must survive normalization, persistence and replay. Request
options alone do not implement remote cancellation, cursor recovery or host job
ownership. The adapter must not turn an actionable client tool into a successful
opaque terminal response.

Extend the existing encoder, decoder, Model contract and Agent loop. Keep native
wire values in the provider crate or namespaced metadata. No new Agent engine,
implicit capability grant, detached tool worker or retry loop is introduced.

## Assistant history and callable identity

`ModelResponse::assistant_message` preserves namespaced response metadata on the
canonical message. All Agent history paths, including completion repair, provider
pause, terminal completion and checkpoint replay, use this conversion.

The OpenAI decoder records original text/refusal message boundaries, IDs and
phases in `<provider>.message_blocks`. The encoder reconstructs distinct assistant
messages, validating the metadata against the number of canonical text/refusal
parts. This preserves commentary/final-answer identity without putting OpenAI
wire types into the neutral content enum. Content mutations must update or remove
obsolete boundary metadata rather than silently replaying stale values.

A namespaced function or custom call uses `namespace.name` as its canonical Tool
registry key. `<provider>.namespace` retains the original namespace. Define local
Tools with the same qualified name and namespace metadata; the adapter groups
them into native namespace definitions and reverses qualification on replay.
The documented forced-function choice has no namespace selector. Named choices
for namespaced Tools fail locally rather than inventing wire fields; applications
can explicitly narrow the advertised Tool set and use Required. Flat custom Tools
use the documented custom choice type.

Capability checks still apply to the qualified registered Tool. Discovery never
registers a Tool or grants authority.

For an advertised flat Tool, the official tool-search self-namespace form
(`namespace` equal to `name`) resolves to that Tool's existing flat registry key
in HTTP and WebSocket Model adapters. Replay retains the original namespace/name.
An explicitly namespaced Tool still uses its qualified registry key. Declaring
both forms with the same wire identity fails before transport. Standalone raw
decoders and explicit lifecycle/socket owners retain qualified names because they
do not own the application's advertised Tool registry.

Local Tool metadata supports `namespace`, `namespace_description`, `defer_loading`, `allowed_callers`,
`async` and `tool_type` under the selected provider namespace (or the explicit
`openai-compatible` namespace). Custom Tools use `tool_type: "custom"` and a
canonical string input schema; optional native grammar belongs in `format`.
Client tool search uses `tool_type: "tool_search"` on the registered `tool_search`
Tool, with a structured tools-array result. Calls and outputs retain their native
wire type through the canonical Tool/Effect path. Chat rejects these Responses
semantics rather than silently discarding metadata.
This check applies to replayed Tool calls/results and assistant message metadata,
including the single-text fast path, as well as to Tool definitions.
Function/custom-only options on the client search descriptor fail locally;
such options belong on the tools returned by search. Search-loaded tools with a
provider namespace still require the corresponding qualified local registry key.

## Async jobs

`OpenAiAsyncToolState` is bounded, serializable correlation state for explicitly
application-owned asynchronous jobs. Register a complete async call before host
dispatch; identical replay is recognized and changed input under the same call ID
is rejected. Resolve only against the bound application session scope. Original
call IDs, qualified names, caller correlation and output kinds survive restart;
identical completion is replayable and conflicting completion fails.
Result routing metadata (`caller` and `tool_type`) is derived exclusively from
the original call, including absence. Result-supplied routing overrides are
discarded; restored snapshots must preserve the same correlation.

This state does not execute jobs or prove an Effect completed. Hosts authenticate
scope, use the normal Tool/Effect path, persist job records and reconcile receipts.
The default Agent continues to await results at its existing turn boundary.
Early tool launch while model output is still arriving requires explicit host
ownership; this change does not silently detach work or bypass TurnReviewer.

## Background responses and compaction

`OpenAiControlPlane` exposes create/get/cancel/delete for provider background
responses. The host persists the response ID and accounts for known usage; no
ambiguous POST is automatically repeated. Queued/in-progress/cancelled status is
not a successful ModelResponse and never acknowledges an Agent conversation commit.
Ordinary Model invocation rejects `background: true` before transport and directs
the caller to the explicit lifecycle API.

`create_background_stream` enables provider event retention with `stream=true`,
returns the acknowledged response ID and preserves the caller's `store` policy.
Only responses originally created with `stream=true` support stream reattachment.
`store=false` background work can be polled during the provider's temporary
retention window; it is not a durable application record.

`stream_response(id, starting_after, context)` reattaches with a monotonic cursor,
validates returned identity when supplied, bounds frames, and observes cancellation
and deadlines. It exposes provider events without fabricating the missing prefix.
Retrieve the final stored response to reconstruct canonical complete content. A
transport drop leaves remote state unknown; reattachment performs GET, not POST.

`compact_response` returns every item from `/responses/compact` in provider order.
Use the returned `input_message` as the replacement model input window. The
compaction ID is not a `previous_response_id`. Compaction does not rewrite the
immutable conversation transcript or merge semantic memory with execution history.
Server-side `context_management` remains available through explicit provider options;
native compaction items use the existing opaque input-item round trip.

## Responses WebSocket

The native `openai-realtime` transport feature also exposes a separate Responses
connection; Realtime and Responses remain different protocols. Named lanes are
bounded to 32, pending submissions to 64, and each lane has its own decoder.
Request-scoped errors and `response.failed` release only their affected request;
other lanes remain consumable. Connection-limit errors invalidate the connection
without replaying accepted work. Raw failures remain visible to the explicit
connection owner; the Model adapter returns them as model errors.
Provider failures after submission retain unknown retry safety, including failures
received before any content delta. They do not authorize automatic replay.
Lane identity and response lineage are separate. Unknown and steering events stay
visible. A steering acknowledgement means queued input, not an applied update;
keep reading through automatic continuation or required Tool input. Pending
steering is connection-local and must be reconciled after disconnect.

`responses_websocket_model` implements the existing Model/ProviderModel boundary,
so ProviderRuntime and Agent retain canonical authority, accounting and recovery.
It reuses a socket for sequential calls. Atomic Complete mode buffers bounded
canonical events until terminal validation; Streaming exposes deltas. A dropped
or failed stream discards its connection. No in-flight request is automatically
replayed. Explicit connections expose multiplexing and steering; the Model adapter
serializes calls to avoid introducing another scheduler.
Atomic size and lifecycle validation must finish before returning the connection
to its reuse pool, including checks on terminal provider metadata. Decoder errors
after dispatch retain unknown retry safety even if no content was exposed.

The new Responses WebSocket surface is native-only. Browser Responses WebSocket
credential/gateway negotiation is not claimed. Existing browser Realtime contracts
remain independent.

## Model and media compatibility

Public OpenAI GPT-6 family requests validate documented reasoning/tool/sampling
combinations locally. Exact families and dated snapshots are recognized; arbitrary
compatible providers are not assigned public OpenAI model assumptions. Options are
rejected with a correction rather than silently removed. Prompt-cache write usage
and cache diagnostics are preserved when supplied.

GPT Image 2 and 2.5, including the documented 2.5 `2026-09-08` snapshots,
receive explicit built-in profiles. `edit_image` uses the GPT
Image JSON endpoint with bounded reference images, mask, quality and output settings.
Inline media and provider-file ownership use the existing validation path. URL
references are not downloaded by the adapter. Transparent JPEG output is rejected.

## Evidence and exclusions

### Consolidated boundary audit (2026-10-14)

This audit covers the implemented OpenAI Responses/Chat, continuation, media and
Agent-history paths. It is not a claim of feature parity with every OpenAI product.
The scope and regressions are recorded together so individual successful tests
cannot be mistaken for complete integration acceptance.

| Boundary | Checked behavior |
| --- | --- |
| Request encoding | GPT-6 parameter restrictions, strict schemas, tool choice, namespace definitions, custom/client-search descriptors, conflicting provider options and rejection of Responses-specific history in Chat |
| Stream consistency | Function argument deltas, argument-done and item-done agree; cross-type/late events, duplicate call IDs, reused output indices and repeated tool-item completion fail |
| Message completion | Final message text/refusal agrees with closed blocks; incomplete messages preserve partial output with an incomplete terminal reason and cannot become successful completion |
| Response identity | Per-event response IDs and resumed background events match their response/subscription, independently of cursor monotonicity |
| Tool result replay | Unknown output kinds fail; async caller/output-kind correlation includes compatible metadata aliases and survives validated snapshot restoration; self-namespaced discovery calls resolve against advertised Tools without granting authority |
| Remote lifecycle | Explicit background creation, retrieval, cancellation/deletion, storage policy, cursor resume and no automatic repeat submission |
| Compaction | The entire output window is retained; documented instructions, lineage, cache and tier options are forwarded; unsupported/malformed/conflicting provider options fail locally |
| WebSocket ownership | Independent lanes, failure isolation, steering successor, conservative retry state and validation before socket reuse |
| Images and privacy | Returned output-format metadata cannot contradict the requested format; decoder Debug exposes state counts rather than private text/arguments |
| Persistence and authority | Canonical Agent history, SQLite/PostgreSQL recovery and Tool/Effect boundaries remain on their existing execution paths |

`openai_stream_consistency` adds negative-event and complete/partial-output
regressions for these cross-event contracts. Existing HTTP, native WebSocket,
Agent and storage suites cover transport and durable replay. The full workspace
gate uses disposable PostgreSQL containers; vendor canaries stay opt-in.

Compaction consumes only its documented provider options: `instructions`,
`previous_response_id`, `prompt_cache_key`, `prompt_cache_options`,
`prompt_cache_retention` and `service_tier`. Ordinary generation fields are not
compaction parameters. Do not reuse an arbitrary Responses provider-options map
without selecting the compaction subset.

Live vendor acceptance, model/account permissions, gateway-specific behavior,
browser Responses sockets and automatic host async scheduling retain the
exclusions below. They are explicit integration prerequisites or feature limits,
not assertions established by loopback tests.

Completion evidence for this audit:

- `cargo test --workspace --all-features --locked`: 868 passed, 4 explicitly
  ignored (MinIO service acceptance, Ark live canary, OpenAI Realtime live canary
  and one macro rustdoc example). PostgreSQL container/restart tests executed.
- Workspace Clippy with all targets/features and denied warnings, and workspace
  rustdoc with denied warnings: passed.
- Native provider feature isolation (no features, `openai`, `openai-realtime`)
  and `wasm32-unknown-unknown` provider compilation: passed.
- Formatting, diff whitespace, generated knowledge, AI system tests and
  architecture checks: passed.

No live model inference or release/deployment acceptance is inferred from these
results. No code defect confirmed by this audit remains deferred.

- `openai_continuation_http`: complete/streamed phase replay after serialization,
  qualified calls, discovery definitions, custom/client-search outputs, background
  lifecycle/cursor, model preflight, image editing and async correlation restart.
- `openai_responses_ws`: authenticated native socket, interleaved lanes, steering
  acknowledgement and automatic successor, isolated lane failure/reuse, socket
  reuse, terminal atomic-buffer overflow, and conservative disconnect behavior.
- Agent and SQLite durable-conversation tests: assistant metadata survives committed
  checkpoint/transcript storage and completed replay without another model call.

These are offline loopback and storage tests. They do not establish live model
quality, account access, all OpenAI endpoints, browser Responses sockets, automatic
hosted async scheduling, or server-side exactly-once effects. Decisions, GPT-Live
and the managed Agents API are separate product/protocol integrations.

## Official sources

- [Reasoning and phase replay](https://developers.openai.com/api/docs/guides/reasoning)
- [GPT-6 parameter compatibility](https://developers.openai.com/api/docs/guides/latest-model)
- [Tool search](https://developers.openai.com/api/docs/guides/tools-tool-search)
- [Function and custom tools](https://developers.openai.com/api/docs/guides/function-calling)
- [Async tool calling](https://developers.openai.com/api/docs/guides/async-tool-calling)
- [Background mode](https://developers.openai.com/api/docs/guides/background)
- [Compaction](https://developers.openai.com/api/docs/guides/compaction)
- [Responses WebSocket](https://developers.openai.com/api/docs/guides/websocket-mode)
- [Mid-turn steering](https://developers.openai.com/api/docs/guides/steering)
- [Image generation and edits](https://developers.openai.com/api/docs/guides/image-generation)
