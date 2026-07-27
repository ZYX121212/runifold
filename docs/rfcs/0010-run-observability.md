# RFC 0010: Run-tree observability

- Status: Accepted for initial implementation
- Scope: `runifold-core`, `runifold-agent`

## Summary

Structured observability belongs to a `RunContext`, not to an Agent, Tool, or
provider adapter. A root Run may attach a `Journal`; every child then uses the
same storage while retaining an independent per-Run event sequence.

The initial integration records enough information to reconstruct Agent
lifecycles, model turns, tool calls, delegation relationships, policy
decisions, budget changes, and terminal failures.

## Recorder ownership

`RunRecorder` binds:

- an object-safe shared `Journal`;
- an `EventFactory` scoped to one Run.

`RunContext::with_journal` enables recording for a root or existing context.
`RunContext::child` automatically creates a child recorder with:

- the same Journal;
- the child's Run ID;
- the parent Run ID;
- a fresh sequence counter.

Runs without a recorder perform no journal allocation or writes.

## Event model

The initial Agent integration emits:

- `Lifecycle::Started`;
- `Lifecycle::Completed`, `Failed`, or `Cancelled`;
- `Budget::Updated`;
- `Child::Started`, `Completed`, `Failed`, or `Cancelled`;
- `runifold.agent/turn.started`;
- `runifold.agent/model.started|completed|failed`;
- `runifold.agent/tool.started|completed|failed`;
- `runifold.agent/delegation.started|completed|failed`;
- `runifold.gateway/policy.allowed|denied|failed`.

Lifecycle completion includes local counters and the shared usage snapshot.
Failures are normalized into `RunError` categories.

## Cross-run causality

Before child execution, the parent records `Child::Started`. Its Event ID
becomes the initial cause of the child's `Lifecycle::Started` event:

```text
parent Child::Started
          │ caused_by
          ▼
child Lifecycle::Started
          │
          ▼
child Lifecycle::Completed

parent Child::Completed ──caused_by──► parent Child::Started
```

Together with `parent_run_id` and `root_run_id`, this makes the execution tree
and the event that created each child reconstructable.

## Privacy defaults

Events record operational metadata, not application content. The default
payloads omit:

- user prompts and system instructions;
- tool arguments;
- model text and reasoning;
- tool and Agent result bodies;
- credentials and provider headers.

Applications may implement explicit capture middleware later, but sensitive
content collection must be opt-in.

## Failure policy

When a Journal is configured, recording is fail-closed:

- failure to record `Lifecycle::Started` prevents model execution;
- failure to record a later event stops further orchestration;
- journal failure is returned as a structured observability error.

This avoids claiming that an execution was audited when its event sink rejected
the record. It does not roll back work already performed. In particular,
budget consumption or an external effect may precede a later journal failure.
The initial synchronous Journal interface also means a slow implementation can
delay execution.

## Initial invariants

1. Event sequence is monotonic within each Run.
2. Parent and child Runs share storage but not sequence counters.
3. Every recorded event contains Run and parent identity.
4. Child lifecycle start is causally linked to parent child creation.
5. Terminal Agent lifecycle is causally linked to Agent lifecycle start.
6. Journal failure is visible and never silently discarded.
7. Default events contain no prompt, arguments, or generated content.
8. Provider-specific response types never enter the Journal contract.

## Deferred decisions

- buffered asynchronous Journal adapters with backpressure;
- transactional or write-ahead effect recording;
- event schema version negotiation;
- event-linked checkpoints and replay beyond the initial Agent snapshot protocol;
- configurable content capture and redaction;
- OpenTelemetry span and metric projection;
- event sampling;
- globally ordered distributed journals;
- event subscribers and live streaming.
