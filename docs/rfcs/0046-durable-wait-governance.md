# RFC 0046: Durable Wait Governance

## Status

Implemented for workflow execution, the in-memory reference store, the
PostgreSQL adapter, and the distributed Worker Runtime.

## Problem

A durable signal wait needs more than reliable delivery. Production control
planes must bound waiting time, cancel work from outside the worker, explain
what happened to every accepted signal identity, and reclaim audit records
without deleting future delivery.

These decisions must remain correct across concurrent publication, timeout,
claim, cancellation, lease takeover, and process failure.

## Signal or timeout

`WorkflowBuilder::wait_for_signal_or_timeout` persists one
`WorkflowWait::SignalOrTimeout` containing a validated name and positive
relative timeout. The store converts that timeout to an authoritative absolute
deadline when the worker releases its lease.

The winner is selected at the store boundary:

- a matching signal accepted strictly before the deadline wins;
- at the deadline or later, timeout wins;
- a signal already buffered before wait installation wins immediately.

The worker commits a typed `WorkflowWaitOutcome`. Signal success includes the
signal identity, name, and payload; timeout contains no invented payload. A
crash before checkpoint commit leaves the durable wake available to the next
fenced owner.

## External cancellation

`WorkflowStore::cancel` is idempotent. Queued, waiting, or leased work becomes
`Cancelled`; a repeated call against any terminal task returns
`AlreadyTerminal`. Cancelling leased work invalidates its heartbeat, finish,
and checkpoint CAS authority through the existing lease fence.

Cancellation also marks every pending signal for that checkpoint as
dead-lettered. PostgreSQL performs task cancellation and signal transition in
one statement.

## Signal lifecycle

Every accepted signal identity has exactly one lifecycle state:

- `Pending`: eligible for a matching future wait;
- `Consumed`: selected by a matching wait;
- `DeadLettered`: no longer deliverable because its target was terminal,
  cancelled, or its matching deadline had elapsed.

An identical publication identity remains `Duplicate` regardless of lifecycle;
binding the same identity to different content remains a conflict.
`inspect_signal` returns identity, checkpoint, name, state, and authoritative
acceptance time. It deliberately excludes payload data from the control-plane
snapshot.

## Retention

`compact_signals` accepts a positive `WorkflowSignalRetention`. It may delete
only consumed or dead-letter signals at or before the store-authoritative
cutoff. Pending signals are never removed, regardless of age.

Compaction is explicit. Runtime operations do not hide migrations or automatic
data loss inside publication and claim paths.

## Invariants

1. Signal and timeout cannot both win one wait.
2. The store clock, not the worker clock, decides the deadline.
3. Deadline equality belongs to timeout.
4. External cancellation fences every prior worker lease.
5. A terminal or late publication is retained as a dead letter.
6. Signal inspection never exposes payloads.
7. Retention never removes pending delivery.
8. Duplicate identity semantics survive consumption and compaction windows.

## Deferred

- operator-authorized dead-letter replay;
- per-tenant encryption and retention policy;
- bulk control-plane pagination.

Tenant partitioning, quotas, fair claims, and tenant-scoped retention are
specified in [RFC 0047](0047-multi-tenant-workflow-admission.md).
