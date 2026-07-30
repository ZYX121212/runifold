# RFC 0043: Distributed Workflow Claims and Fenced Leases

## Status

Implemented. Worker execution integration is specified in
[RFC 0044](0044-workflow-worker-runtime.md).

## Problem

A durable checkpoint makes process recovery possible, but does not decide
which process owns an interrupted workflow. Multiple workers may observe the
same pending work, a paused worker may resume after another worker takes over,
and worker clocks may disagree.

Distributed execution therefore needs an ownership protocol separate from the
workflow's canonical execution state.

## Boundary

`runifold-workflow` owns the provider-neutral asynchronous `WorkflowStore`
contract and an in-memory reference implementation. `runifold-store-postgres`
owns PostgreSQL SQL, schema setup, database errors, and transport dependencies.

The existing synchronous `CheckpointStore` remains the embedded execution
boundary. Distributed workers instead use the asynchronous checkpoint methods
on `WorkflowStore`; no asynchronous PostgreSQL operation is hidden behind a
synchronous interface.

## Task identity

Every queued `WorkflowTask` contains:

- one stable `CheckpointId`, reused across retries and worker takeovers;
- workflow name and caller-managed version;
- canonical JSON input;
- integer priority.

Enqueue is create-only. Reusing a checkpoint identity returns a conflict.

## Claim protocol

PostgreSQL claims one eligible task with a single atomic statement:

1. select the highest-priority queued or expired task;
2. lock it with `FOR UPDATE SKIP LOCKED`;
3. set the new owner;
4. increment the attempt and fencing token;
5. calculate expiration from `clock_timestamp()`;
6. return the task and lease.

Workers never supply absolute timestamps. Database time is authoritative.

## Fencing

Every ownership-sensitive mutation compares:

- checkpoint identity;
- worker identity;
- fencing token;
- an unexpired lease.

Every successful claim increments the token. A paused worker holding an older
token cannot heartbeat, complete, fail, cancel, or requeue work after a newer
worker takes ownership.

Fencing protects Runifold's durable control state. External resources must
also compare the token, use idempotency keys, or pass through the write-ahead
Effect boundary when stale external writes must be rejected.

## Disposition

The current owner may:

- mark the task completed;
- return it to the queue after a store-relative delay;
- mark it permanently failed with a bounded safe reason;
- mark it cancelled.

Retry delay is computed by the store clock. Terminal tasks are never claimed.

## Initial invariants

1. At most one unexpired lease is current for a task.
2. Claim attempts and fencing tokens increase monotonically.
3. Expired tasks may be reclaimed without coordination with the old worker.
4. Old workers cannot mutate control state after takeover.
5. Duplicate enqueue never replaces an existing task.
6. Claim ordering is deterministic by priority, eligibility time, and ID.
7. Runtime operations never perform hidden schema migrations.
8. PostgreSQL is an edge adapter, not a workflow-domain dependency.

## Deferred

- timer and signal delivery;
- automatic recovery scanning;
- multi-tenant queue partitioning and admission control.
