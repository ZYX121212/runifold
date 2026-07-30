# RFC 0047: Multi-Tenant Workflow Admission

## Status

Implemented for the workflow domain, in-memory reference store, PostgreSQL
adapter, Worker leases, signals, cancellation, inspection, and retention.

## Problem

A global priority queue is not a multi-tenant scheduler. One tenant can fill
the backlog, consume every worker lease, starve lower-priority tenants, or use
a guessed checkpoint identity against another tenant's control plane.

Tenant isolation must therefore be part of the durable store contract rather
than an optional HTTP-layer convention.

## Tenant identity

`WorkflowTenantId` is a validated portable identity. Every `WorkflowTask`
stores its owner, and every `WorkflowLease` carries the same tenant identity.
Single-tenant applications use the explicit `default` tenant; multi-tenant
hosts assign tasks with `WorkflowTask::with_tenant`.

The following external operations require a tenant:

- signal publication;
- cancellation;
- task inspection;
- signal inspection;
- signal retention compaction.

Supplying an existing resource with the wrong tenant returns
`TenantMismatch`. It is distinct from `NotFound` for trusted host control
planes while the error text does not disclose the actual tenant.

## Admission policy

`WorkflowTenantPolicy` defines two positive limits:

- maximum outstanding non-terminal tasks;
- maximum concurrent unexpired worker leases.

The lease limit cannot exceed the outstanding limit. Tightening a policy does
not kill admitted work or revoke active leases; it stops new admission and
claims until usage falls below the new boundary.

Unconfigured tenants receive the documented default policy. Hosts may
atomically create or replace a policy with `set_tenant_policy`.

## Fair claim order

Claims are tenant-fair before they are task-priority-aware:

1. discard tenants at their active lease limit;
2. choose the eligible tenant with the oldest claim sequence;
3. within equally eligible tenants, prefer higher task priority;
4. break remaining ties with availability time and checkpoint identity.

This preserves useful task priority without allowing one tenant's priority
values to starve every other tenant.

The in-memory adapter applies the algorithm under its queue and admission
locks. PostgreSQL locks both the candidate task and tenant row with
`FOR UPDATE ... SKIP LOCKED`, then advances a database sequence in the same
claim statement.

## Atomic accounting

PostgreSQL stores an outstanding-task counter on the tenant row. Enqueue
increments the counter and inserts the task in one statement; a uniqueness
failure rolls back both changes. Completion, permanent failure, and
cancellation decrement it in the same terminal transition.

Concurrent claim candidates lock the tenant row and count only unexpired
leases. This prevents two workers from crossing a tenant's lease limit.

The in-memory reference adapter derives outstanding and active counts while
holding the corresponding locks, providing the same observable contract.

## Signal and retention isolation

Signals persist their tenant beside the target checkpoint. Publication
requires the target task to belong to the supplied tenant. Signal inspection
and compaction are tenant-scoped; one tenant's retention job cannot delete
another tenant's signal identities.

Cancellation dead-letters only pending signals belonging to the cancelled
task and tenant.

## Invariants

1. A task and every lease derived from it have the same tenant.
2. Wrong-tenant control-plane operations fail closed.
3. Outstanding admission cannot exceed the configured limit.
4. Unexpired leases cannot exceed the configured tenant limit.
5. Tenant fairness is evaluated before task priority.
6. Terminal transitions release outstanding admission exactly once.
7. Tenant retention cannot remove another tenant's signals.
8. Existing fencing and checkpoint CAS rules remain mandatory.

## Security boundary

`WorkflowTenantId` is an isolation key, not an authentication credential.
Applications must derive it from an authenticated principal and authorize
policy changes before calling the store.

## Deferred

- weighted tenant shares and reserved capacity;
- tenant-specific encryption keys;
- paginated tenant usage snapshots and admission metrics.

Aggregate tenant resource ledgers are implemented by
[RFC 0048](0048-durable-tenant-budget-ledger.md).
