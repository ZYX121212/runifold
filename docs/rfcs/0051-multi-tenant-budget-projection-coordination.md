# RFC 0051: Multi-tenant budget projection coordination

Status: implemented

## Problem

A single-tenant supervisor is a safe execution primitive, but applications
should not have to enumerate every tenant, spawn an unbounded task per tenant,
or restart the process whenever a tenant gains a budget. A fleet also needs to
divide projection work without making a central scheduler a new availability
dependency.

## Decision

`WorkflowStore::list_tenant_budgets` exposes stable keyset pagination over
budget-enabled tenant identities. Results are ordered by tenant ID, the cursor
is exclusive, and the validated page limit is bounded at 1,000. Tenants that
only have workflow admission policy are not returned.

`OtelWorkflowBudgetCoordinator` repeatedly scans this index. It assigns each
tenant using a fixed FNV-1a hash modulo a configured shard count, then runs one
fenced supervisor cycle for assigned tenants under a validated concurrency
bound. A failed tenant increments the scan report and does not prevent other
tenants from progressing. A discovery failure aborts the current scan so the
next attempt restarts from a stable beginning.

Applications construct coordinators through
`OtelRuntime::workflow_budget_projection_coordinator` or directly when they
need a replaceable sleeper.

## Safety and availability

Shard assignment is an optimization, not the correctness boundary. Every node
for the same telemetry sink uses the same projection ID and a unique worker
ID. The existing store-authoritative projection lease and fencing token remain
authoritative when nodes overlap, shard membership is misconfigured, or the
shard count changes.

Changing the shard count remaps some tenants. During rollout, old and new
owners may both attempt those tenants, but only one can hold an unexpired
lease. This avoids a coordination service while retaining fail-closed cursor
advancement.

Each started discovery scan is drained before shutdown. Shutdown prevents a
new scan and interrupts the interval between scans. This preserves bounded
work and avoids abandoning an in-process projection cycle merely because a
shutdown signal arrived.

## Operational model

The coordinator returns cumulative and per-scan reports containing:

- completed scans and discovered/assigned tenant counts;
- contended tenant cycles;
- projected events and non-empty batches;
- isolated projection failures and scan-level discovery failures.

Tenant identity is never attached to OpenTelemetry metric labels. Operators
can run one shard per replica for modest fleets, or multiple coordinator
instances per process when explicit shard placement is required.

## Invariants

1. Discovery order is stable and cursor-exclusive.
2. Every returned page is bounded by a validated limit.
3. The same tenant ID and shard topology always yield the same assignment.
4. Exactly one shard owns a tenant within one valid topology.
5. At most the configured number of tenant projection futures are active.
6. One tenant failure never aborts projection for the remaining page.
7. Projection leases and fencing, not shard assignment, protect durable state.
8. A completed scan dynamically observes tenants configured since the prior
   scan.
9. Shutdown does not begin another scan after cancellation.
10. Coordinator telemetry contains no tenant, worker, or projection labels.
