# RFC 0011: Agent checkpoints and conservative recovery

- Status: Accepted for initial implementation
- Scope: `runifold-core`, `runifold-agent`

## Summary

Runifold persists versioned Agent state at turn boundaries. Recovery is
conservative: a checkpoint written before external work is marked in-flight
until the entire model-and-callable turn reaches a stable transcript.

An interrupted in-flight turn is never retried implicitly. The caller must
choose `ResumePolicy::RetryInterruptedTurn`, acknowledging that model cost,
Tool effects, or Agent delegation may be repeated.

## Storage envelope

The domain-neutral `Checkpoint` contains:

- stable UUIDv7 `CheckpointId`;
- owning Run ID;
- monotonic revision;
- namespaced kind;
- schema version;
- JSON payload;
- update timestamp.

`CheckpointStore::compare_and_swap` provides optimistic concurrency:

- `expected_revision = None` is create-only;
- updates require the exact current revision;
- the new revision must be exactly one greater.

This prevents an old worker from overwriting state written by a newer worker.

## Agent state

`AgentCheckpointState` stores:

- Agent and Model identity;
- canonical transcript;
- completed turn, Tool, and delegation counters;
- shared usage snapshot;
- execution phase.

The initial phases are:

- `ReadyForTurn`;
- `TurnInFlight { turn }`;
- `Completed { response }`.

The state intentionally stores canonical provider-neutral content.

## Write-ahead protocol

For a checkpointed Agent run:

1. create `ReadyForTurn` revision zero;
2. before consuming turn budget or calling a model, persist
   `TurnInFlight`;
3. execute the model and every callable emitted by that response;
4. after all Tool results are appended, persist `ReadyForTurn`;
5. for a terminal response, persist `Completed`.

The stable transcript therefore never claims that a partially completed turn
finished.

## Recovery

Completed checkpoints are idempotent: `resume` reconstructs `AgentOutcome`
without invoking a model.

Stable checkpoints continue with the next model turn. Budget usage must exactly
match the checkpoint snapshot. A restarted process can construct its tracker
with `BudgetTracker::restore`.

For `TurnInFlight`:

- `RejectAmbiguous` returns `AmbiguousCheckpoint`;
- `RetryInterruptedTurn` retries from the last stable transcript.

The retry path accepts usage greater than the pre-turn snapshot because some
cost may already have been accounted before interruption. It never decreases
usage.

## Privacy

Unlike default Journal events, a checkpoint contains the transcript required
for recovery and may therefore contain prompts, generated content, Tool
results, and provider-preserved data. Production stores must provide
appropriate encryption, access control, retention, and deletion.

## Initial invariants

1. Checkpoint revisions cannot be overwritten by stale writers.
2. Agent and Model identity must match during resume.
3. Stable resume cannot reset or reduce budget usage.
4. Completed resume performs no model or Tool call.
5. In-flight retry requires explicit caller authority.
6. A stable revision is written only after the full turn is represented in the
   transcript.
7. Checkpoint payloads remain provider-neutral.

## Limitations and deferred decisions

- The in-flight checkpoint unit is an entire model-and-callable turn. Explicit
  retry may repeat model cost, but completed callables are replayed when the
  same EffectStore is available.
- A callable left in `Started` remains ambiguous unless its effect class and
  explicit recovery policy permit retry.
- The built-in store is in-memory; durable database and object-store adapters
  are deferred.
- Automatic pause, approval wakeup, distributed leases, checkpoint deletion,
  encryption, migration, compaction, and branching are deferred.
