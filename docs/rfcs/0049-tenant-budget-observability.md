# RFC 0049: Tenant budget observability and audit

Status: implemented

## Problem

Aggregate budget counters answer whether a tenant may start work, but they do
not explain how the ledger reached its state. Logs and metrics alone are not a
safe audit source: they can be dropped, duplicated, sampled, or exported after
the database transaction fails.

At the same time, exporting tenant and checkpoint identities as metric labels
creates unbounded cardinality and leaks control-plane identities.

## Decision

The workflow store appends an immutable `WorkflowBudgetAuditEvent` in the same
atomic boundary as every successful budget transition. Audit events cover:

- policy configuration;
- new reservation;
- fenced reservation adoption;
- aggregate admission denial;
- observed usage exceeding its reserved envelope;
- actual-usage settlement;
- cancellation and recovery-expiry forfeiture;
- drained-window reset.

Each event contains a monotonic cursor, store-authoritative time, optional
checkpoint, affected usage, reservation age when applicable, active limit,
and the committed/reserved snapshot immediately after the decision.

`list_tenant_budget_audit` reads events strictly after an optional cursor with
a validated maximum page size. Named consumers use
`load_or_create_tenant_budget_audit_projection` to durably register at cursor
zero, then advance only through monotonic compare-and-set.

After downstream retention requirements are satisfied, a control plane may
explicitly call `compact_tenant_budget_audit` with its acknowledged cursor.
Compaction is tenant-scoped, never advances or reuses the sequence, and fails
closed if it would pass the slowest registered projection.

## Atomicity

The in-memory adapter records audit facts while holding the tenant ledger
lock. PostgreSQL writes the ledger and audit row inside the same database
function or data-modifying CTE.

An admission denial does not mutate counters, but its audit row is still
durable before the denial is returned. Expiry maintenance and cancellation
record the exact forfeiture reason and reservation age.

## OpenTelemetry projection

`OtelRuntime::workflow_budget_metrics` creates low-level
`OtelWorkflowBudgetMetrics` instruments. For normal operation,
`OtelRuntime::workflow_budget_projector` creates a bounded projector that
registers its durable cursor, reads pages, records every event, and advances
the cursor only after the entire page has been recorded.

`project_once` provides scheduler-friendly bounded work.
`project_available` catches up through a caller-supplied non-zero maximum
number of batches, preventing an unbounded hot loop while producers continue
to append events.

The projector exports:

- `runifold.workflow.tenant_budget.decisions`;
- `runifold.workflow.tenant_budget.amount`;
- `runifold.workflow.tenant_budget.utilization`;
- `runifold.workflow.tenant_budget.reservation.age`.

Metric attributes are restricted to stable `decision`, `reason`, and
`resource` values. Tenant IDs, checkpoint IDs, worker IDs, model content, and
error messages are never metric attributes.

The durable audit remains the source of truth. Metrics use at-least-once
delivery because an OpenTelemetry SDK and the workflow store cannot share a
transaction. A crash after metric recording but before cursor advancement can
replay the final page; advancing before recording is forbidden because it
would silently lose telemetry. Concurrent instances sharing a projection ID
surface a typed cursor conflict instead of overwriting progress.

Continuous multi-process operation uses the fenced projection leases and
supervisor specified in
[RFC 0050](0050-budget-projection-supervision.md), preventing concurrent live
projection before the cursor update.

## Operational assets

The bundled Prometheus rules compute the fleet-wide budget admission-denial
ratio and recovery-expiry forfeiture rate. Alerts detect sustained reservation
denial and any persistent failure to recover reservations. The Grafana
dashboard includes decision rates, denial ratio, and recovery forfeiture.

Per-tenant investigation uses the tenant-scoped audit API rather than
high-cardinality metrics.

## Invariants

1. A ledger transition and its audit fact are atomic.
2. Audit cursors increase monotonically for each tenant.
3. Pagination never returns an event at or before the supplied cursor.
4. Audit reads are tenant-scoped.
5. Telemetry projection never changes durable state.
6. Metric labels never contain tenant or checkpoint identity.
7. Forfeiture facts distinguish cancellation from recovery expiration.
8. A projection cursor advances only after its complete page is recorded.
9. Cursor advancement is monotonic compare-and-set.
10. Compaction never passes the slowest registered projection.
11. Projection work is bounded by validated page and batch limits.
