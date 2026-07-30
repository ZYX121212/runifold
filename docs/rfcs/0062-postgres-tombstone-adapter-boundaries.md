# RFC 0062: PostgreSQL tombstone adapter boundaries

## Status

Implemented.

## Context

The PostgreSQL tombstone adapter grew beyond one thousand lines while serving
four distinct change paths: legal holds, archive watermarks, reviewer workflow,
and destructive purge execution. Keeping those paths in one file increased
review cost even though they share one public store trait.

## Decision

The public `PostgresWorkflowStore` implementation and
`WorkflowTaskTombstoneGovernanceStore` contract remain unchanged. The private
adapter is organized as:

- `hold_export`: legal holds and monotonic export confirmation;
- `approval`: durable inbox listing, reviewer claims, approval, and rejection;
- `purge`: immutable preparation, atomic execution, and evidence reads;
- `support`: row decoding and typed store-error construction.

The parent module only re-exports crate-internal operation functions. No child
module is public and no new crate or dependency is introduced.

## Invariants

Atomic SQL statements remain contiguous. The split does not move transaction
steps into Rust orchestration, change schema, alter query text, or broaden
visibility beyond the workflow adapter.

## Verification

The real PostgreSQL MCP Task suite exercises archive, hold, prepare, approval
takeover, rejection, execution, evidence, database restart, and fencing after
the physical split.
