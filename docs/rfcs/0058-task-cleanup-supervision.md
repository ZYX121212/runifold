# RFC 0058: Dynamically sharded Task cleanup supervision

- Status: implemented
- Scope: `runifold-workflow`, `runifold-store-postgres`,
  `runifold-observability-otel`
- Depends on: RFC 0057

## Decision

Terminal Task retention is operated by a separate control-plane supervisor.
Workflow workers remain responsible only for execution. The supervisor:

- discovers tenants with terminal Tasks using stable keyset pagination;
- assigns tenants to a deterministic process shard;
- bounds concurrent tenants, batch size, and batches per claim;
- claims one fenced tenant cleanup lease;
- heartbeats ownership using store-authoritative time while cleanup runs;
- drains a started scan before shutdown and backs off after discovery failure.

An operator may run multiple replicas. Deterministic sharding reduces
contention while the PostgreSQL lease and fencing token remain the correctness
boundary during rebalance, duplicate configuration, or process pauses.

## Store contract

`WorkflowTaskRetentionStore` adds two portable primitives:

- `list_task_cleanup_tenants` returns only tenants currently owning terminal
  Tasks, ordered strictly after an opaque tenant key;
- `heartbeat_task_cleanup` renews only the exact unexpired
  tenant/owner/fencing-token lease.

PostgreSQL uses a partial terminal-state index for discovery. Heartbeat and
claim use `clock_timestamp()` so host clock skew cannot create overlapping
owners.

## Bounded work and failure isolation

Configuration validates that heartbeat is positive and shorter than the
lease. Discovery pages, concurrent tenants, cleanup batch size, and batches
per tenant are all non-zero and bounded.

A store failure for one tenant is counted and isolated from other tenants. A
discovery failure aborts the current scan, restarts keyset discovery from the
beginning after backoff, and never resumes from a possibly invalid in-memory
cursor. Lease loss stops that tenant immediately. Best-effort release shortens
recovery after ordinary infrastructure failures.

## Health and OpenTelemetry

The supervisor exposes a lock-free snapshot containing scan activity, scans,
claims, contention, deleted Tasks, lease losses, and infrastructure errors.
An optional synchronous observer receives completed scan reports.

`runifold-observability-otel` supplies the default observer and exports:

- `runifold.workflow.task_cleanup.operations`;
- `runifold.workflow.task_cleanup.tenants`;
- `runifold.workflow.task_cleanup.batches`;
- `runifold.workflow.task_cleanup.deleted`.

Attributes are limited to fixed `outcome` and `state` values. Tenant,
workflow, checkpoint, owner, and fencing identities are never metric
attributes.

## Verification

The disposable PostgreSQL MCP test verifies database-clock heartbeat,
premature-takeover prevention, three-tenant discovery, two-item discovery
pages, bounded two-tenant concurrency, automatic tombstone/delete, empty
post-cleanup discovery, and lock-free health totals. The OTel test verifies
metric names and fixed outcomes without tenant identity.
