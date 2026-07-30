# RFC 0045: Durable Timers and External Signals

## Status

Implemented for workflow nodes, the in-memory reference store, the PostgreSQL
adapter, and the distributed Worker Runtime.

## Problem

Long-running workflows must often wait for time, webhooks, human approval, or
another system. Holding a worker future and renewing a lease for hours is
wasteful and makes process restarts part of the wait's correctness.

A durable wait must:

- persist intent before releasing ownership;
- use store-authoritative time;
- survive process and worker replacement;
- accept signals before or after the wait is installed;
- deduplicate external delivery;
- never let a stale worker consume or overwrite a wake.

## Workflow definition

`WorkflowBuilder::timer` adds a relative durable timer.
`WorkflowBuilder::wait_for_signal` adds a named signal wait. Signals target a
specific workflow `CheckpointId`, so unrelated workflows or tenants cannot
consume the event accidentally.

Timer nodes pass their current canonical value through. Signal nodes emit the
canonical signal payload as their output.

## Suspend protocol

Before returning suspension, execution persists
`WorkflowCheckpointPhase::Waiting` with the exact node and `WorkflowWait`.
The worker then calls `WorkflowStore::finish` with
`WorkflowDisposition::Suspend` under the current worker identity, fencing
token, and unexpired lease.

The store atomically removes ownership and records either:

- an absolute wake timestamp computed from store time; or
- a named signal subscription.

No worker heartbeat remains active while the task is waiting.

## Signal protocol

`WorkflowSignal` contains:

- a globally stable `WorkflowSignalId` used as an idempotency key;
- the target workflow checkpoint;
- a validated portable name;
- a bounded canonical JSON payload.

Publication is idempotent. Reusing an identity with identical content returns
`Duplicate`; reusing it with different content is a conflict.

Signals arriving before their wait are buffered. Installing a matching wait
consumes the oldest unconsumed signal and immediately returns the workflow to
the queue. Signals arriving after the wait atomically wake it. PostgreSQL claim
also promotes any waiting task with a buffered signal, closing publication /
wait-installation races.

## Wake and recovery

Claims retain a durable `WorkflowWake`. A worker validates it against the
checkpoint wait before committing the waiting node. Timer wakes pass through
the prior value; signal wakes commit their payload.

The wake remains attached across lease expiration. If a worker crashes during
wake processing, its replacement receives the same wake. Checkpoint CAS makes
committing the wait node idempotent, and a stale worker remains fenced.

## Invariants

1. Wait intent is checkpointed before the lease is released.
2. Timers use the store clock, not a worker clock.
3. Waiting tasks hold no worker lease.
4. Signal publication is idempotent and payload-bound.
5. Signal-before-wait is not lost.
6. Wake identity and wait identity must match.
7. Wake delivery survives worker takeover.
8. Only checkpoint CAS commits progression beyond a wait.

## Deferred

- operator-driven signal replay.

Signal-or-timeout races, external workflow cancellation, signal dead letters,
and safe retention are specified in
[RFC 0046](0046-durable-wait-governance.md).
Tenant-scoped signal admission is specified in
[RFC 0047](0047-multi-tenant-workflow-admission.md).
