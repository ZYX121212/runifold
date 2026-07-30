# RFC 0044: Workflow Worker Runtime

## Status

Implemented for one-task execution, continuous bounded supervision, graceful
shutdown, and crash recovery.

## Problem

Claims and leases prevent duplicate ownership, but do not execute a workflow.
A safe worker must resolve the exact definition, create a Run with explicit
authority and budget, persist progress under the lease fence, renew ownership,
cancel work when ownership becomes uncertain, and choose a durable terminal or
retry disposition.

## Definition registry

`WorkflowRegistry` maps the exact pair of workflow name and caller-managed
version to one `WorkflowDefinition`. Duplicate registrations fail.

A definition owns:

- the immutable `Workflow`;
- root `Budget`;
- root `CapabilitySet`;
- optional durable `Journal`;
- explicit ambiguous-resume policy;
- explicit ordinary-failure policy.

An unknown definition is not executed. The task returns to the queue after a
configurable delay so a correctly configured worker may claim it later.

## Fenced checkpoint data plane

`WorkflowStore` now includes asynchronous checkpoint load and compare-and-swap
operations. Every operation carries the current `WorkflowLease`.

The PostgreSQL adapter stores the checkpoint and its monotonic revision beside
the task control record. Create and update statements compare:

- checkpoint identity;
- worker identity;
- fencing token;
- unexpired lease;
- expected checkpoint revision.

A stale worker cannot write checkpoint progress after another worker takes
over, even if its suspended future resumes.

Embedded workflows continue to use the synchronous `CheckpointStore`. The
`WorkflowCheckpoint` handle selects the local or distributed backend, while
the execution engine awaits both through one internal cursor. PostgreSQL is
never synchronously blocked inside async Workflow execution.

## Worker cycle

`WorkflowWorker::run_once`:

1. atomically claims at most one task;
2. resolves the exact registered definition;
3. loads the fenced checkpoint;
4. restores cumulative usage into the registered budget;
5. starts a new workflow or resumes the existing checkpoint;
6. renews the lease on a supervised heartbeat loop;
7. persists Completed, Retry, or Failed under the current fence.

The worker API processes one task per call. `WorkflowSupervisor` composes these
cycles with a validated concurrency limit, exponential idle/error backoff, and
a shared cancellation token. Shutdown stops replacement cycles and drains
already-started claims and executions.

The supervisor exposes cumulative lock-free, low-cardinality counters and a
per-run report. Metric snapshots include cycle concurrency, idle polls,
terminal dispositions, lease loss, unavailable definitions, infrastructure
errors, and scheduled backoffs. Exporters can poll the snapshot without adding
an observability dependency to the workflow kernel.

## Lease loss

Any heartbeat failure means ownership is uncertain. The worker:

1. cancels the root `RunContext`;
2. awaits the in-flight Workflow future so structured children observe
   cancellation;
3. does not attempt a terminal write with the stale lease;
4. returns `WorkflowWorkerOutcome::LeaseLost`.

Checkpoint CAS may independently reject a stale write before the next
heartbeat. This also stops execution through the ordinary typed checkpoint
error path.

Fencing protects Runifold's task and checkpoint records. External systems must
still honor the fencing token, an idempotency key, or Runifold's write-ahead
Effect protocol.

## Recovery

When a lease expires, another worker may claim the same task. It receives a
higher fencing token and loads the last accepted checkpoint.

The registered `WorkflowResumePolicy` decides whether an ambiguous in-flight
node is rejected or explicitly retried. Completed nodes and branches are not
executed again.

The new worker restores checkpoint usage before resume. Recovery cannot reset
spent tokens, cost, turns, calls, or delegations to zero.

## Initial invariants

1. Only an exact name/version definition may execute a task.
2. Every distributed checkpoint mutation is lease-fenced and revision-CASed.
3. Heartbeat failure cancels and joins in-flight execution.
4. A new owner restores cumulative usage before resuming.
5. Completed checkpoint work is not repeated.
6. Ambiguous retry requires explicit definition policy.
7. Unknown definitions never execute.
8. Ordinary execution failure policy is explicit.
9. Supervisor concurrency is bounded by construction.
10. Shutdown stops admission and drains already-started cycles.
11. Idle and infrastructure failures cannot create a hot polling loop.

## Deferred

- queue-depth and worker-liveness discovery;
- tenant partitions and admission control;
- Effect fencing propagation helpers.
