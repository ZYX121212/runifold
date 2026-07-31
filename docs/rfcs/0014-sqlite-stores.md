# RFC 0014: Durable SQLite stores

- Status: implemented
- Scope: `runifold-store-sqlite`

## Summary

`SqliteStore` is the first durable reference implementation of Runifold's
three persistence boundaries:

- `EffectStore`;
- `CheckpointStore`;
- `Journal`.

SQLite is not part of the runtime kernel and is not required for ephemeral
execution. It demonstrates that Runifold's recovery and compare-and-swap
contracts survive process restarts using a widely available transactional
database.

## Packaging

The adapter lives in the independent `runifold-store-sqlite` crate. The
`runifold` facade exposes it under the `sqlite` feature. The separate
`sqlite-bundled` feature compiles SQLite from source for applications that
prefer a self-contained build.

Core, Model, Tool, Agent, and Effect crates do not depend on SQLite.

## Storage model

One `SqliteStore` may serve all three traits. It uses independent tables:

- `runifold_effects` stores the canonical EffectRecord and indexed
  capability-scoped idempotency key;
- `runifold_checkpoints` stores the latest canonical Checkpoint revision;
- `runifold_events` stores immutable RunEvent envelopes with unique event IDs
  and per-Run sequence numbers.

Canonical records are encoded as JSON. Identity, revision, and lookup fields
are duplicated into relational columns for constraints and efficient access.
The serialized record remains the source used to reconstruct the public Rust
type.

## Atomicity and concurrency

Effect and checkpoint compare-and-swap operations use an immediate SQLite
transaction:

1. acquire the write transaction;
2. read the current revision;
3. validate the create or update precondition;
4. validate the effect idempotency owner when applicable;
5. insert or update with the expected revision;
6. commit.

This protects CAS semantics across clones, separately opened connections, and
processes. Connections use a bounded busy timeout rather than waiting
indefinitely for a writer.

Journal insertion relies on database uniqueness for immutable event identity
and `(run_id, sequence)`.

## Crash-recovery proof

The integration suite launches a child process with a shared SQLite database.
The child:

1. writes a real Tool side effect to a file;
2. durably completes the corresponding EffectRecord;
3. exits the process before the Agent can persist its stable post-turn
   checkpoint.

The parent process opens a fresh SQLite connection, resumes the in-flight
checkpoint with explicit retry authority, and supplies the same logical model
call. `EffectExecutor` replays the completed result. The Agent reaches its
terminal response while the side-effect file still contains exactly one
write.

This verifies recovery across an actual process boundary rather than only
across store clones or object reconstruction.

## Failure mapping

- absent checkpoints map to `CheckpointErrorKind::NotFound`;
- stale checkpoint revisions map to `CheckpointErrorKind::Conflict`;
- database failures map to checkpoint Storage, effect Store, or JournalError;
- malformed stored JSON maps to InvalidPayload or Protocol;
- effect idempotency ownership collisions map to IdempotencyConflict.

Error messages never include record payloads.

## Operational scope

SQLite is appropriate for:

- command-line tools and desktop applications;
- local Agent runtimes;
- single-node services;
- development and deterministic crash-recovery testing.

The adapter also exposes `SqliteWorkflowStore` for the complete local
`WorkflowStore` control plane. It persists queue state, fenced leases, tenant
budgets and audit projections, timers, signals, human interrupts, cancellation,
checkpoint history, and workflow forks in an atomic versioned snapshot. Each
operation uses an immediate transaction and the SQLite clock; synchronous
database work crosses an explicit Tokio blocking boundary.

It is not the intended coordination mechanism for horizontally scaled,
high-write deployments. Those should use another implementation of the same
traits, such as PostgreSQL.

## Invariants

1. Core crates have no SQLite dependency.
2. Effect and checkpoint CAS is transactionally enforced.
3. Effect idempotency keys have one owner per capability.
4. Journal events are immutable and uniquely sequenced per Run.
5. Persisted public records round-trip without domain-specific projection.
6. Reopening the database preserves recovery state.
7. A completed Tool effect is not physically repeated after process exit and
   checkpoint retry.
8. A workflow mutation either commits its complete state transition or leaves
   the previous snapshot intact.
9. Unknown snapshot format versions fail closed and are never treated as empty
   workflow state.
10. Concurrent connections produce at most one successful workflow claim.

## Deferred decisions

- schema migrations beyond version one;
- database encryption and payload-level encryption;
- retention, compaction, and archival APIs;
- multi-record transactions spanning effects, checkpoints, and events;
- asynchronous connection pooling;
- PostgreSQL and distributed lease implementations.
