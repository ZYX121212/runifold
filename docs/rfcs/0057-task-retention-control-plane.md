# RFC 0057: Fenced Task retention control plane

- Status: implemented
- Scope: `runifold-workflow`, `runifold-store-postgres`
- Depends on: RFC 0056

## Decision

Physical cleanup is separate from MCP `ttlMs` and workflow execution. `ttlMs`
describes how long a protocol Task handle may remain usable from creation.
The retention control plane instead removes only workflows that are already
terminal and whose terminal `updated_at` is older than an operator-selected
retention.

`WorkflowTaskRetentionStore` is an optional extension to `WorkflowStore`.
Execution adapters do not need to implement destructive maintenance. A
retention-capable store exposes:

- exclusive tenant cleanup claims;
- bounded terminal cleanup batches;
- immutable tombstone pagination;
- explicit cleanup lease release.

Retention, cleanup batch size, tombstone page size, cursor, lease, and
tombstone are distinct domain types. This prevents protocol TTL, execution
deadline, deletion retention, and audit pagination from being accidentally
interchanged.

## Ownership and fencing

Cleanup ownership is partitioned by `WorkflowTenantId`. A claim records a
validated `WorkerId`, store-authoritative expiration, and monotonically
increasing fencing token. An active claim cannot be stolen. After expiration,
another process may claim with a higher token.

Every destructive batch compares tenant, owner, fencing token, and database
time. A stale owner receives `WorkflowStoreErrorKind::LeaseLost`; it cannot
delete even if it began work before takeover. Batch selection uses
`FOR UPDATE SKIP LOCKED` and is capped at 1,000 Tasks.

## Atomic deletion and audit

PostgreSQL performs candidate selection, tombstone insertion, dependent-state
cleanup, and Task deletion in one data-modifying CTE statement. Candidates
must be in `completed`, `failed`, or `cancelled` and beyond terminal retention.
Queued, leased, and waiting Tasks are never eligible.

The immutable tombstone stores:

- monotonic audit cursor;
- checkpoint and tenant identity;
- workflow name and version;
- final status;
- creation, terminal, and deletion timestamps.

The Task row is deleted only when that statement inserted its unique
tombstone. A retry therefore cannot delete without audit evidence. Checkpoint
history, pending signal storage, and any leftover budget reservation are
removed in the same statement. Tenant budget audit facts remain independent
and immutable.

After cleanup, ordinary Task lookup returns not found while tombstone audit
remains cursor-paginatable. Tombstones are not automatically compacted in this
RFC; removing audit evidence requires a separate explicitly designed policy.

## Verification

The disposable PostgreSQL MCP test verifies:

- active Task cleanup returns an empty batch;
- one tenant cleanup owner excludes a second;
- expired ownership is taken over with a higher fencing token;
- the stale owner is rejected;
- terminal cancellation is tombstoned and physically removed atomically;
- retrying cleanup is empty;
- tombstone audit remains readable;
- MCP `tasks/get` returns not found after physical deletion.
