# RFC 0006: Tool runtime boundary

- Status: Accepted for initial implementation
- Scope: `runifold-tool`

## Summary

Tools are explicit runtime capabilities, not arbitrary callbacks attached to a
prompt. Every tool has a stable identity, versioned schemas, effect class, risk
level, model-facing description, and object-safe asynchronous invocation
boundary.

## Invocation

`ToolRegistry` resolves tools by deterministic model-facing names. Registration
never silently replaces an existing tool. Before invocation, the registry
checks that the owning `RunContext` was explicitly granted the tool's stable
capability ID.

Each call receives a `ToolContext` containing:

- invocation ID;
- owning run ID;
- inherited deadline;
- descendant cancellation token.

The registry races execution against cancellation. Cancelling a tool call does
not cancel its owning run or sibling calls.

## Separation of views

`ToolDescriptor` produces two projections:

1. `CapabilityDescriptor` for policy and runtime authorization;
2. `ToolSpec` for model-visible function selection.

Host metadata, risk, and effect semantics are not automatically exposed to the
model.

## Initial invariants

1. Tool names are non-empty and unique within a registry.
2. Registry lookup is deterministic.
3. An ungranted tool cannot execute even if it is registered.
4. Capability identity, not a mutable display name, authorizes execution.
5. Cancellation and deadlines are part of every invocation context.
6. Errors expose normalized categories and retry safety.
7. Output explicitly records whether it is safe to return to a model.

## Deferred decisions

- JSON Schema validator selection and compilation cache;
- policy and human-approval middleware;
- sandboxed process and WASM tools;
- durable side-effect receipts;
- idempotency enforcement;
- output redaction and size budgets;
- procedural macros for typed Rust functions.
