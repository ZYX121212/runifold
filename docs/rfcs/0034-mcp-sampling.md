# RFC 0034: MCP Sampling

- Status: implemented
- Scope: `runifold-mcp`
- Protocol baseline: MCP `2025-11-25`

## Decision

Runifold implements basic MCP Sampling as an explicitly client-owned model
authority. An MCP server may request `sampling/createMessage`, but it cannot
select a provider credential, force a model, bypass host review, or invoke a
Tool through this layer.

The client advertises only `sampling: {}`. Sampling with Tools, ambient MCP
context, and task augmentation are intentionally not advertised. They require
additional balance validation, iteration limits, authority attenuation, and
approval semantics.

## Authority and approval

`SamplingService` applies this sequence:

1. validate the untrusted wire request and acquire a concurrency permit;
2. ask `SamplingApprover::review_request`, which may reject or return an
   edited request;
3. reserve conservative request and requested-token budgets;
4. invoke the host-owned `SamplingProvider`;
5. validate the provider output;
6. ask `SamplingApprover::review_response`, which may reject or return an
   edited response;
7. validate the edited response before disclosing it to the server.

The provider never runs when request approval is denied. The server never
receives model output when response approval is denied. The host may implement
either boundary with interactive user consent, deterministic policy, or both.

`modelPreferences.hints` are advisory. `SamplingModelSelector` makes the actual
client-side choice, and `ModelSamplingProvider` adapts that choice to
Runifold's canonical `Model` boundary. Server-provided metadata is not treated
as trusted routing authority.

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

Basic Sampling rejects Tool definitions, Tool choice, Tool content, non-`none`
context inclusion, and `toolUse` completion results. Rejection uses MCP code
`-1`; malformed input uses `-32602`; limits, cancellation, deadline, execution,
and invalid output remain distinct typed failures before their JSON-RPC
mapping.

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
- malformed Tool-use output rejection;
- canonical model adaptation with client-owned model selection;
- reverse request/response correlation over in-process, stdio, and real
  loopback Streamable HTTP.
