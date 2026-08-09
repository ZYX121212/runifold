# RFC 0034: MCP Sampling

- Status: implemented
- Scope: `runifold-mcp`
- Protocol baseline: MCP `2025-11-25`

## Decision

Runifold implements MCP Sampling as an explicitly client-owned model
authority. An MCP server may request `sampling/createMessage`, but it cannot
select a provider credential, force a model, or bypass host review.

The exact client capability is derived from the installed boundaries.
`ModelSamplingProvider` advertises Tool support and maps Tool definitions,
choice, calls, and results to the canonical model API. Context support is
advertised only when a host-owned `SamplingContextProvider` is installed.

## Authority and approval

`SamplingService` applies this sequence:

1. validate the untrusted wire request and acquire a concurrency permit;
2. resolve requested ambient context through the host-owned resolver and
   revalidate the expanded conversation;
3. ask `SamplingApprover::review_request`, which may reject or return an
   edited request;
4. reserve conservative request and requested-token budgets;
5. invoke the host-owned `SamplingProvider`;
6. validate the provider output;
7. ask `SamplingApprover::review_response`, which may reject or return an
   edited response;
8. validate the edited response before disclosing it to the server.

The provider never runs when request approval is denied. The server never
receives model output when response approval is denied. The host may implement
either boundary with interactive user consent, deterministic policy, or both.

`modelPreferences.hints` are advisory. `SamplingModelSelector` makes the actual
client-side choice, and `ModelSamplingProvider` adapts that choice to
Runifold's canonical `Model` boundary. Server-provided metadata is not treated
as trusted routing authority. The selector also receives explicit
`SamplingModelRequirements` through the source-compatible
`select_with_requirements` hook for Tools, image, audio, and document/resource
input; a selected model whose capabilities explicitly reject a requirement
fails before model execution. Existing selectors that implement only `select`
keep working.

## Limits and failure semantics

`SamplingPolicy` bounds:

- message and content-block counts;
- serialized request and response sizes;
- decoded image and audio bytes;
- per-request and lifetime requested-token budgets;
- lifetime accepted-request count;
- concurrent requests and execution time.

Lifetime accounting reserves the requested maximum token count before provider
execution. Reservations are intentionally not refunded after downstream
failure because a provider may already have consumed capacity.

Request-count and token reservations form one logical operation: failure of
the token reservation rolls back the request counter. Nested Tool-result
content contributes to the same `max_content_blocks` limit as top-level
content, so nesting cannot bypass the service-wide shape budget.

Tool-enabled requests require a Tool-capable provider, unique declared Tool
names, object input schemas, valid Tool choice, and an immediately balanced
assistant `tool_use` / user `tool_result` sequence. Model Tool calls must target
a declared Tool and use unique call identifiers. `required` and `none` Tool
choice are rechecked against the Provider response instead of trusting the
Provider to enforce them. Official Tool-result `structuredContent`, `isError`,
and `_meta` fields are preserved; the older namespaced Runifold metadata shape
remains readable for compatibility. Tool-result resource links and embedded
text/blob resources map to canonical resource/document content. Context
inclusion is rejected unless a resolver was negotiated. Unknown non-empty MCP
input blocks use a versioned model-visible envelope, and output media that
cannot use MCP's inline form uses a lossless extension block. Malformed known
blocks still fail closed.
Rejection uses MCP code `-1`; malformed input uses `-32602`; limits,
cancellation, deadline, execution, and invalid output remain distinct typed
failures before their JSON-RPC mapping.

## Task-augmented Sampling

Runifold implements the MCP `2025-11-25` task augmentation for
`sampling/createMessage`. A client advertises
`tasks.requests.sampling.createMessage` and `tasks.cancel` only when both a
`SamplingService` and an `McpSamplingTaskBackend` are installed.
`CreateMessageParams::task` carries
the requested millisecond retention, bounded by `SamplingPolicy::max_task_ttl`.
If the receiver has not advertised this capability, task metadata is ignored
and the request retains its ordinary synchronous meaning.

`McpSamplingClient::create_message_outcome` returns either the complete result
or a durable `McpTask`. The requestor can inspect, wait for, fetch the exact
result of, or cancel that Task through `tasks/get`, `tasks/result`, and
`tasks/cancel`. Polling respects the receiver's interval, local deadline, and
Task retention without cancelling durable work merely because the local wait
ends. Returned Sampling results carry
`_meta["io.modelcontextprotocol/related-task"]`.

The official 2025 wire format is preserved exactly: create returns `{ task }`,
`tasks/get` and `tasks/cancel` return a bare Task, and `tasks/result` waits
through `working` and `input_required` before returning the original success
result or JSON-RPC error. Its `ttl` and `pollInterval` fields remain separate
from the newer Runifold Tasks extension's `ttlMs` and `pollIntervalMs` fields.
Use `CreateMessageParams::with_task_idempotency_key` with a retained UUIDv4/v7
and configure a deployment-stable private
`SamplingTaskIdempotencyNamespace` on the workflow adapter. The adapter derives
a separate server-owned Task ID and verifies the complete approved request on
retry; a key reused with different content is rejected.

The backend owns durable request scheduling, execution state, authorization
partitioning, and exact request/result reconstruction. A newly accepted Task
must already be inspectable and in `working` state before its handle is
returned. Before enqueue, `SamplingService` resolves host context, performs
request approval, revalidates edits, and reserves the request/token admission
budget. On `tasks/result`, it validates the recovered result against the
persisted approved request, performs response approval, and validates any
response edits before disclosure. Process recovery therefore cannot bypass
either approval stage or Tool, role, media, output-size, and Tool-choice
policy.

Successful response approvals are written back as bounded, tenant-scoped,
idempotent durable workflow control records. Response review is guarded by a
store-clock lease with takeover and stale-owner fencing, and both claim and
completion records are protected from ordinary signal compaction. A recreated
client/adapter loads that approved disclosure before running response review
again. This provides one active reviewer, not magical exactly-once semantics
for an external approval service; external reviewers should use the Task ID as
an idempotency key. Sampling workflows
that need an exact protocol failure return `WorkflowSamplingTaskResult::Error`;
its complete JSON-RPC `code`, `message`, and `data` survive persistence.
Unexpected workflow runtime failure remains a normalized internal error.

With the `workflow-tasks` feature, `WorkflowTaskAdapter` implements this
backend directly. `WorkflowSamplingTaskRoute` binds the single Sampling method
to an exact workflow/version and tenant. The approved request is the workflow
input, the exact `CreateMessageResult` is its terminal output, and both are
reconstructed from immutable Checkpoint history. Any `WorkflowStore`
implementation can back the route; the SQLite file-reopen test proves recovery
across Store and client recreation, while PostgreSQL uses the same adapter.

Every failure also carries an optional stable `SamplingStage`. The client
returns that stage as structured JSON-RPC error data without exposing reviewed
content. `McpSamplingClient::create_message_scoped` records redacted lifecycle
events in its `RunContext`; unscoped calls remain usable without a Journal.

## Bidirectional transports

`McpSamplingClient` is bound to one initialized session. It sends a server
request to the client and correlates the response by request id. Cancellation
and deadline expiry send `notifications/cancelled`.

All supported transports carry the reverse request path:

- in-process calls the installed client peer directly;
- stdio multiplexes server requests, client responses, notifications, and
  ordinary client requests over the same framed stream;
- Streamable HTTP publishes server requests over the session SSE stream, then
  accepts the correlated JSON-RPC response in a new client POST returning HTTP
  202.

HTTP request identifiers and session identifiers remain opaque. The HTTP
client consumes reverse requests in a background task only after Sampling is
configured.

Sampling access is explicit: in-process and stdio servers call
`McpSession::sampling_client`; HTTP servers resolve a requester from an active
opaque session id. Runifold does not inject an ambient Sampling handle into
`RunContext`.

## Verification

Tests cover:

- capability negotiation and local preflight rejection;
- both approval stages and non-disclosure after rejection;
- lifetime request and token limits;
- cancellation and server-side timeout propagation;
- redacted scoped lifecycle events and request/response review stages;
- context capability negotiation and pre-review expansion;
- Tool declaration/choice mapping, balanced Tool history, and undeclared or
  malformed Tool-use rejection;
- Tool/media-aware model selection, nested content limits, and atomic lifetime
  budget reservation;
- rich Tool-result resource, structured-content, error, metadata, and
  forward-compatible extension preservation;
- decoded-media limits for embedded resources and Runifold extension blocks,
  so alternate wire shapes cannot bypass the media budget;
- output enforcement for `required` and `none` Tool choice;
- canonical model adaptation with client-owned model selection;
- reverse request/response correlation over in-process, stdio, and real
  loopback Streamable HTTP;
- task capability negotiation, TTL policy, durable-handle creation, polling,
  exact result correlation, recovered-result validation, and cancellation.
