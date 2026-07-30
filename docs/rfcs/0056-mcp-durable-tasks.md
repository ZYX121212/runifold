# RFC 0056: MCP durable Tasks

- Status: implemented
- Scope: `runifold-mcp`, `runifold-workflow`, `runifold-store-postgres`
- Protocol baseline: MCP `2026-07-28` Tasks extension draft
- Extension: `io.modelcontextprotocol/tasks`

## Decision

Runifold implements the current stateless Tasks extension rather than the
incompatible `2025-11-25` experimental design. The supported control methods
are `tasks/get`, `tasks/update`, and `tasks/cancel`. There is deliberately no
new `tasks/list` or `tasks/result` API.

Tasks are an edge projection, not another orchestration runtime.
`McpTaskBackend` is the protocol-neutral durable boundary. The optional
`workflow-tasks` feature supplies `WorkflowTaskAdapter`, which derives every
Task response from a tenant-scoped `WorkflowStore` snapshot and immutable
checkpoint history. It does not retain a parallel in-memory Task state.

## Capability and creation

Clients opt in explicitly with `McpClientConfig::with_tasks`. The extension is
included in every stateless request's client capabilities. Servers advertise
the extension only when an `McpTaskBackend` is installed.

A backend declares which Tool names require task execution. The server still
verifies Tool existence and capability authority and runs synchronous MRTR
preflight before asking the backend to create a Task. If the current request
did not declare Tasks, the server fails with `-32003`; it never returns a Task
handle unexpectedly.

The creation response is sent only after durable enqueue and a successful
read-back. Therefore `tasks/get` can resolve the returned identifier
immediately.

## Client lifecycle

`call_tool_outcome` preserves the polymorphic result as either a synchronous
`CallToolResult` or an `McpTask`. Existing `call_tool` remains compatible: when
Tasks are enabled and negotiated it transparently polls to the final Tool
result under the original cancellation and total deadline.

Clients can explicitly:

- load current state with `get_task`;
- submit keyed input with `update_task`;
- cooperatively cancel with `cancel_task`;
- resolve a handle with `wait_task`;
- observe complete typed snapshots with `listen_tasks`.

Polling honors `pollIntervalMs`, applies a client maximum, and never resets the
operation deadline. Task execution errors become JSON-RPC errors. A completed
Tool result with `isError: true` remains a completed Task.

### Retention and polling governance

`ttlMs` is interpreted exactly as a retention deadline measured from
`createdAt`; it is not converted into workflow cancellation or an execution
deadline. Clients validate both timestamps as non-negative RFC 3339 values,
reject `lastUpdatedAt < createdAt`, and detect retention arithmetic overflow.

For a non-terminal Task whose retention has elapsed, `wait_task` returns the
typed `McpError::TaskExpired` instead of continuing to poll an unusable handle.
An already observed terminal result remains consumable after its advertised
retention because no further server state is required.

Server `pollIntervalMs` is constrained by both client-configured minimum and
maximum intervals. This prevents an untrusted peer from creating either a
high-frequency polling loop or an unbounded delay. The remaining operation
deadline and remaining retention are still stricter bounds.

## Status notifications

Modern `subscriptions/listen` filters support exact `taskIds`. Requesting them
requires the per-request Tasks capability; otherwise the server returns
`-32003`. The acknowledgment is a subscription agreement, not evidence that an
opaque Task ID exists. Authorization and tenant isolation are re-evaluated by
the durable backend whenever state is derived.

Each accepted Task produces an immediate `notifications/tasks` snapshot. The
server then refreshes the durable current state at a configurable cadence,
emits only changed complete snapshots, and detaches after the first terminal
snapshot. A reconnect therefore receives current durable state rather than
depending on replay of a lossy process-local event. Transient backend read
failures do not invent state transitions or terminate the subscription.

Servers bound Task IDs per subscription and skip missed timer ticks to prevent
client-controlled fan-out and catch-up bursts. `McpTaskSubscription` validates
and exposes typed `McpTask` values while the general `McpSubscription` remains
available for mixed notification filters.

## Workflow mapping

One `WorkflowTaskAdapter` serves exactly one explicit workflow tenant. This is
the authorization partition: routes for a second tenant are rejected. A route
binds one Tool name to an exact workflow name and version. Route tuples must be
unique so a recovered task ID maps back to one Tool contract without storing
mutable side metadata.

Workflow states map as follows:

| Workflow state | MCP Task status |
| --- | --- |
| queued or leased | `working` |
| timer or signal wait | `working` |
| durable interrupt | `input_required` |
| completed checkpoint | `completed` |
| terminal failure | `failed` |
| cancelled | `cancelled` |

Successful results are reconstructed from immutable terminal checkpoint
history. The default mapper preserves an already encoded `CallToolResult`;
otherwise it exposes the canonical workflow output as text and structured
content. Applications can install a typed `WorkflowTaskResultMapper`.

Durable interrupts become keyed `elicitation/create` input requests.
`tasks/update` maps approve, edit, and reject responses into idempotent
`WorkflowInterruptCommand` values. The interrupt identity is reused as the
decision identity, so duplicate delivery cannot create a second decision.

## Persistence and timestamps

`WorkflowTaskSnapshot` now includes workflow identity, version,
store-authoritative creation/update milliseconds, and safe failure detail.
The in-memory store updates these fields on queue, claim, heartbeat, wait,
wake, finish, cancellation, and fork transitions. PostgreSQL reads its existing
`created_at`, `updated_at`, and `failure_reason` columns.

The adapter emits RFC 3339 timestamps and returns a safe internal JSON-RPC
error if terminal history cannot reconstruct the original result.

## Transport and security

Streamable HTTP sends and validates `Mcp-Name: <taskId>` for `tasks/get`,
`tasks/update`, and `tasks/cancel`, in addition to `Mcp-Method`.

Task IDs are UUIDv7 values generated by the durable workflow runtime. They are
not authorization by themselves. Every lookup is scoped to the adapter's
single configured tenant, and tenant mismatch is normalized to “Task does not
exist” to avoid cross-tenant existence disclosure.

Task input requests retain the same trust level as ordinary Elicitation,
Sampling, or Roots requests. A Task is never a higher-trust channel.

## Verification

Conformance tests cover per-request capability rejection, creation read-back,
polling, update, cancellation, exact HTTP routing names, workflow completion,
process-local client reconnection from task ID alone, durable interrupt
mapping, wake-up through `tasks/update`, notification capability enforcement,
filter normalization, changed-state suppression, terminal detachment, and
notification reconnect snapshots. Timestamp ordering, retention overflow,
expired non-terminal handles, retained terminal results, and hostile
one-millisecond polling hints are also covered.

A disposable PostgreSQL fault-injection test durably creates a Task, stops the
database, requires the stale client to surface a bounded storage failure,
restarts the same writable layer, and reconstructs the store, adapter, server,
and client. Recovery uses only the original `taskId`. Eight concurrent
subscriptions then converge on the recovered state and the same idempotently
cancelled terminal snapshot.
