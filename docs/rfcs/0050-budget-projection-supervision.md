# RFC 0050: Budget projection supervision

Status: implemented

## Problem

A durable cursor prevents silent progress loss, but compare-and-set alone does
not stop two live processes from reading and recording the same page before
one loses the cursor race. Operational telemetry tolerates replay after a
crash, but avoidable concurrent duplication distorts counters.

A production projector also needs continuous polling, bounded catch-up,
heartbeats, shutdown handling, and failure backoff without tying the workflow
store to an async executor.

## Decision

Every named tenant-budget projection may be claimed under a store-authoritative
lease. A `WorkflowBudgetAuditProjectionLease` carries:

- tenant and projection identity;
- worker identity;
- last acknowledged cursor;
- a monotonic fencing token;
- store-authoritative expiration.

The store exposes claim, heartbeat, fenced cursor advancement, and release.
Only an unowned or expired projection can be claimed. Every takeover increments
the fencing token. Heartbeat and advancement compare owner, token, and
expiration; a superseded process receives `LeaseLost`.

The in-memory store applies these transitions under its admission lock.
PostgreSQL performs each transition with one conditional statement using
`clock_timestamp()`.

## Supervisor

`OtelWorkflowBudgetSupervisor` continuously:

1. attempts to claim one named projection;
2. projects a validated maximum number of bounded pages;
3. renews the lease concurrently while projection is active;
4. advances the cursor only after a complete page is recorded;
5. releases ownership after a cycle or shutdown;
6. sleeps after idle work and backs off after infrastructure failure.

The supervisor accepts the runtime's replaceable `WorkflowWorkerSleeper`, so
tests and runtimes do not depend on hidden blocking sleeps. A
`CancellationToken` stops new work and triggers best-effort immediate release.

## Delivery semantics

Projection remains at-least-once. A process can record a page and crash before
the fenced cursor update, so the successor replays that page. The inverse
ordering is forbidden because advancing first could permanently lose
telemetry.

While a lease is valid, another worker cannot read under the same supervisor
identity. On expiry, the successor receives a higher fencing token, and every
mutation from the old process fails closed.

## Operations

`runifold.workflow.tenant_budget.projection.operations` records low-cardinality
`outcome` values such as `claimed`, `contended`, `completed`, `lease_lost`, and
`store_error`. It never labels tenant, projection, or worker identity.

Prometheus records the fleet-wide projection lease-loss rate and alerts when
losses persist.

`OtelWorkflowBudgetSupervisorMetrics` provides a lock-free process-local
snapshot for readiness and control planes. It exposes current lease belief,
catch-up state, last acknowledged cursor, projected events/pages, claims,
contention, lease loss, and infrastructure errors. Callers can share one
metrics value across supervisor reconstruction without coupling health checks
to the supervisor task.

## Module boundaries

- `runtime` owns shared tracer/meter construction and public factories;
- `workflow_budget` owns metric instruments and bounded cursor projection;
- `workflow_budget_supervisor` owns leases, heartbeat, lifecycle, health, and
  backoff.

The crate root re-exports the same public types, so this internal split does
not change application import paths.

## Invariants

1. At most one unexpired fenced lease exists for a named projection.
2. Every expired-lease takeover increments the fencing token.
3. Heartbeat and cursor advancement fail for stale owner/token pairs.
4. A page is recorded before its cursor is advanced.
5. Shutdown never starts a replacement projection cycle.
6. Catch-up work is bounded by page and per-claim batch limits.
7. Supervisor metric labels contain no control-plane identity.
8. Release never changes the acknowledged cursor.
9. Health snapshots never acquire the projection lease or block projection.
10. Internal module boundaries do not leak into the public import path.
