# RFC 0007: Single-agent model-tool loop

- Status: Accepted for initial implementation
- Scope: `runifold-agent`

## Summary

The first agent runtime is a bounded state machine over the existing model,
tool, run, cancellation, and budget primitives:

1. construct a canonical transcript;
2. invoke the configured model;
3. account for the turn and model usage;
4. append the canonical assistant response;
5. execute emitted tool calls through `ToolRegistry`;
6. append canonical tool results;
7. repeat until a terminal response or a hard bound stops execution.

No provider-specific response type enters this loop.

## Configuration

An `Agent` owns:

- a stable local name;
- an object-safe `Model`;
- a provider-qualified `ModelRef`;
- zero or more system messages;
- a `ToolRegistry`;
- local turn, feature-degradation, and tool-error policy;
- an optional execution-local minimum for successful local Tool calls.

Execution occurs inside a caller-provided `RunContext`. This makes authority,
budget, deadline, cancellation, and lineage explicit rather than hidden in a
convenience client.

## Tool errors

Safe operational failures may be returned to the model as failed
`ToolResult`s, allowing it to recover. Authority and lifecycle failures are
never downgraded:

- `CapabilityDenied`;
- `Cancelled`;
- `DeadlineExceeded`.

These terminate the agent even when the configured tool-error policy normally
returns failures to the model. Host-only tool output also terminates rather
than being leaked into the transcript.

## Bounds

The agent enforces both:

- local `max_turns`, which limits this specific loop;
- shared run-tree budgets for turns, model tokens, cost, and tool calls.

Tool-call budget is consumed before tool execution, so a rejected call performs
no external effect. Model usage is accounted immediately after each completed
model response.

## Tool completion contracts

`min_successful_tool_calls` is a completion invariant rather than a resource
budget. While the current execution has fewer successful local Tool results
than required, the model request uses `ToolChoice::Required`. Once the minimum
is satisfied, later requests return to `Auto` so the model can terminate.

Only non-error results from the Agent's local `ToolRegistry` count. Attempts,
application-error results, child-Agent delegations, provider-hosted Tools, and
Tool results loaded from earlier conversation turns do not satisfy the
contract. Results carry the private execution identity in canonical message
metadata, so a stable checkpoint can recover the count without extending the
public checkpoint schema or counting historical transcript entries.

If the remaining shared Tool-call budget is lower than the unsatisfied
minimum, execution fails before another model request. If a Provider ignores
`Required` and returns terminal output, the Agent fails closed with
`ToolRequirementUnsatisfied`.

## Initial invariants

1. Every model and tool call receives a descendant cancellation token.
2. Every model turn and tool attempt is budgeted.
3. Tool calls are executed only through the capability-gated registry.
4. Model-visible tool results preserve the original call ID.
5. A `tool_calls` finish reason without any tool call is a protocol error.
6. Empty model output is a protocol error.
7. The successful outcome contains the complete canonical transcript.
8. Provider selection remains outside the loop.
9. Configured successful Tool-call minima are satisfied before terminal output.

## Deferred decisions

- parallel tool execution with deterministic transcript ordering;
- event journal emission for every state transition;
- checkpoints and resumability;
- model retry and fallback policy;
- context compaction;
- gateway middleware and approval beyond the initial delegation boundary;
- ergonomic root-run builder.
