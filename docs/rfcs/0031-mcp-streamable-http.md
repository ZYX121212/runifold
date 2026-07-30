# RFC 0031: MCP Streamable HTTP edge

- Status: implemented
- Scope: `runifold-mcp`, `runifold-observability-otel`
- Protocol baseline: MCP `2025-11-25` plus the `2026-07-28` stateless core

Runifold also implements the `2026-07-28` stateless core request data plane.
Discovery and ordinary Tools, Resources, Prompts, and Completion requests use
per-request metadata plus `MCP-Protocol-Version`, `Mcp-Method`, applicable
`Mcp-Name`, and schema-driven `Mcp-Param-*` headers. Header values are validated
against the body. Stateless pagination uses a server-stable cursor namespace
and no HTTP session.

`McpClient::connect` selects this modern mode when advertised and falls back to
the initialization flow for legacy-only peers.

## Schema-driven Tool parameter headers

The HTTP client compiles `x-mcp-header` annotations while processing
`tools/list`. An annotation is accepted only on a `string`, safe `integer`, or
`boolean` property reachable from the schema root exclusively through
`properties`. Header suffixes must be non-empty HTTP tokens and are
case-insensitively unique. A malformed declaration invalidates only that Tool,
which is excluded from discovery; valid siblings remain usable.

Calls extract the exact nested property path and emit `Mcp-Param-{Name}` only
for a present, non-null value. Values outside safe visible ASCII, values with
leading or trailing whitespace, and strings matching the sentinel syntax use
the specified UTF-8 Base64 envelope. Compiled rules are immutable and shared
through a synchronized per-transport cache.

The server independently compiles its authoritative schema before Tool
execution. Missing, malformed, unexpected, or body-mismatched recognized
headers fail with HTTP 400 and JSON-RPC `-32020`; integers are compared
numerically. Unknown parameter headers are ignored by the endpoint. Tool
parameters that contain secrets must not be annotated because reverse proxies
and other infrastructure may log headers.

## Multi Round-Trip Requests

The client consumes `input_required` results only for Tool calls, Prompt gets,
and Resource reads. The logical operation has one total deadline and a bounded
round count; neither transport round trips nor input handlers reset that
deadline. Every retry uses a fresh JSON-RPC ID, starts from the original
request parameters, includes only the current keyed `inputResponses`, and
echoes the latest `requestState` byte-for-byte without interpreting it.

Input methods are restricted to Sampling, Elicitation, and Roots.
`MrtrInputHandler` is the host-policy boundary and declares the corresponding
per-request capabilities. `SamplingService` implements this boundary directly,
retaining its approval stages, resource limits, and cancellation. Missing
capabilities fail before the server asks for input.

Servers can attach an `MrtrToolGate` to a canonical Tool. The gate receives the
original arguments, untrusted input responses, opaque state, and the same
capability-attenuated `RunContext` that governs execution. It must validate and
cryptographically bind user-specific state when appropriate. Incomplete rounds
emit a redacted `tool.input_required` domain event; the Tool itself is invoked
only after the gate returns `Proceed`.

## Modern notification subscriptions

`subscriptions/listen` is separate from legacy session GET/SSE. HTTP clients
open it with POST and hold the request-scoped SSE response; stdio clients
demultiplex it by JSON-RPC request ID. The first message is
`notifications/subscriptions/acknowledged`, containing the subset the server
supports and authorizes. Every subsequent notification contains
`io.modelcontextprotocol/subscriptionId`.

Tool, Prompt, and Resource list changes are explicit opt-ins. Exact Resource
updates are intersected with the authorized Resource registry. Multiple
subscriptions can coexist. Dropping HTTP closes its response stream; dropping
stdio sends `notifications/cancelled`. Modern HTTP listen streams have no
session ID, replay buffer, or reconnection illusion: callers reopen a
subscription after unexpected transport loss.

## Decision

Runifold supports the finalized MCP Streamable HTTP transport at one endpoint
using POST, GET, and DELETE. HTTP remains an edge concern: the runtime kernel,
Tool registry, Agent loop, and Workflow executor do not depend on Axum,
Reqwest, SSE, authentication, or MCP session types.

`StreamableHttpTransport` implements the existing object-safe `McpTransport`
request and notification boundary. Its concrete API additionally exposes
server-notification subscription, session inspection, and explicit session
deletion. `McpHttpServer` adapts an existing capability-safe `McpServer` into
an Axum router.

## Framing and lifecycle

POST bodies contain exactly one JSON-RPC request or notification. Batching and
client-sent responses are not accepted by this Tools slice.

Clients advertise both `application/json` and `text/event-stream`. A server
policy selects ordinary JSON or a finite SSE stream for request responses.
Notifications return HTTP 202 without a JSON-RPC body.

Successful initialization may create an opaque `MCP-Session-Id`. Every
subsequent POST, GET, or DELETE includes that identifier and
`MCP-Protocol-Version: 2025-11-25`. A missing required session is HTTP 400; an
unknown or deleted session is HTTP 404.

The client converts a session 404 into `McpError::SessionExpired` and clears its
local session identifier. It does not reinitialize or replay the request.
Session recovery is an explicit host decision because a failed response does
not prove that a Tool side effect did not occur.

## Server notifications and resumption

GET opens a long-lived SSE channel for server-originated JSON-RPC
notifications. Each event ID is scoped to its opaque session. The bundled
client retains the latest event ID and supplies `Last-Event-ID` when it
reconnects.

Each session has bounded live and replay buffers. The server replays only
events after an ID found in the same session stream. Foreign, stale, or
unrecognized event IDs do not cause cross-stream replay.

Closing an SSE connection is transport loss, not MCP cancellation. Request
cancellation remains an explicit `notifications/cancelled` message correlated
by JSON-RPC request identity.

## Security

The HTTP server applies security policy before parsing the body:

- requests carrying `Origin` are rejected unless the exact origin is
  allowlisted;
- an optional `HttpAuthorizer` can require a bearer token on every method;
- `StaticBearerAuth` stores credentials as `SecretString`, redacts `Debug`,
  and can serve as both client provider and server authorizer;
- request bodies and per-session buffers are bounded;
- session identifiers are opaque, globally unique values and are never used as
  authorization by themselves.

Requests without `Origin` remain valid for native MCP clients. Local servers
should bind loopback. Public endpoints must use TLS directly or through a
trusted reverse proxy and should use a production authorization provider.

## Failure and retry semantics

HTTP authentication, session expiry, status failure, transport failure,
protocol failure, timeout, and cancellation have distinct typed categories.
Response bodies are not copied into status errors, avoiding accidental secret
or untrusted-content exposure.

The client performs no hidden retry. Its request deadline covers connection,
upload, server execution, and response decoding. On timeout it drops the
request future and sends explicit MCP cancellation. The server also cancels
the matching child Tool run when an in-flight request future is dropped.

## Observability

MCP Tool execution records `runifold.mcp` domain events:

- `tool.started`;
- `tool.completed`;
- `tool.failed`.

Payloads contain the Tool name, JSON-RPC call identity, and negotiated protocol
revision. Tool arguments, results, bearer tokens, session IDs, and response
bodies are not recorded.

The optional OpenTelemetry journal projects these durable events into GenAI
`execute_tool` spans using `gen_ai.operation.name`, `gen_ai.tool.name`, and
`gen_ai.tool.call.id`. The MCP crate does not depend on OpenTelemetry.

## Verification

Tests use real TCP loopback servers rather than in-memory handlers. They cover:

- JSON and SSE response framing;
- bearer authentication and secure Origin defaults;
- 64 concurrent requests on one session;
- resumable notification replay after SSE disconnect;
- explicit session loss without hidden retry;
- request timeout and matching Tool cancellation;
- nested `x-mcp-header` extraction, Unicode and sentinel encoding, malformed
  schema filtering, header tampering, and concurrent rule-cache reads;
- HTTP and stdio `subscriptions/listen` acknowledgment, filtering,
  correlation, concurrent demultiplexing, cancellation, and zero-session
  operation;
- real HTTP MRTR with Elicitation and Roots input, opaque state echo, missing
  capability rejection, bounded state-only retries, and exactly-once Tool
  invocation after preflight.

The in-process and stdio conformance suites remain active, ensuring transport
additions do not change canonical MCP Tool semantics.
