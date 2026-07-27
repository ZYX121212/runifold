# RFC 0030: Capability-safe MCP Tools edge

- Status: implemented
- Scope: `runifold-mcp`
- Protocol baseline: MCP `2025-11-25`

## Decision

MCP is a replaceable edge crate. It depends on `runifold-core`,
`runifold-model`, and `runifold-tool`; no kernel, Agent, or Workflow crate
depends on MCP wire types.

The first vertical slice implements:

- JSON-RPC 2.0 requests, responses, notifications, and typed failures;
- the MCP initialize/initialized lifecycle and exact version negotiation;
- `ping`, `tools/list`, `tools/call`, and `notifications/cancelled`;
- an in-process pluggable transport;
- newline-delimited stdio serving and a multiplexed subprocess client;
- adaptation from remote MCP Tools into canonical Runifold Tools.

Sampling, elicitation, and tasks are not claimed by this version. Streamable
HTTP is specified by [RFC 0031](0031-mcp-streamable-http.md), while Resources
and Prompts are specified by
[RFC 0032](0032-mcp-resources-prompts.md).

## Authority

An `McpServer` receives an explicit `RunContext`. `tools/list` filters the
registry to capabilities granted to that context. `tools/call` repeats the
authorization check and creates a child run containing exactly the selected
Tool capability.

Unknown and unauthorized names produce the same not-found response. This avoids
using Tool discovery as an authority side channel.

Remote Tool annotations are untrusted hints. `McpRemoteTool` requires a
`RemoteToolPolicy` containing host-selected effect and risk classifications.
Annotations are retained as host-only metadata but never grant capabilities,
lower risk, or change retry safety.

## Output and error boundaries

Canonical Tool output crosses MCP only when `ToolOutput::model_visible` is
true. Host-only output is replaced by a safe application-error result.

Tool application failures use `CallToolResult.isError`, allowing a model to
self-correct. Missing Tools, authorization failures, malformed parameters, and
lifecycle violations use JSON-RPC errors.

Remote protocol and transport failures become typed `McpError` values and are
normalized into canonical `ToolError` categories at the Tool adapter boundary.
Raw remote error content is retained only in host metadata.

## Cancellation and time

Every client request has a bounded timeout. A remote Tool invocation clamps that
timeout to its `ToolContext` deadline and observes hierarchical cancellation.
On timeout or cancellation the client sends `notifications/cancelled` and drops
the outstanding transport future.

The server tracks in-flight requests by JSON-RPC identity. Cancellation targets
only the matching child run. Dropped request futures also cancel their child,
so transport loss cannot leave detached Tool work.

## stdio

stdio messages are compact UTF-8 JSON values delimited by a single newline.
The server writes no non-protocol data to stdout. Requests may execute
concurrently and responses are correlated by identity rather than arrival
order.

The client multiplexes concurrent requests over one process. Dropped or timed
out futures remove their pending response slot. EOF and malformed server output
fail every outstanding request.

Shutdown first closes child stdin. A subprocess that does not exit within the
configured grace period is terminated and awaited.
