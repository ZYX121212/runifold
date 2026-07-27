# RFC 0024: Budget Reservations and Durable Parallel Workflows

## Status

Implemented for fail-fast fan-out/fan-in execution.

## Problem

Atomic consumption protects shared counters from corruption, but it does not
give concurrent siblings deterministic resource ownership. Without
reservation, the branch that happens to be polled first can consume the entire
remaining budget. A restart can also lose which branches completed and replay
model charges or external effects unnecessarily.

Parallel workflow execution therefore requires both resource isolation and
durable branch progress.

## Budget reservation

`BudgetTracker::try_reserve` creates a scoped `BudgetReservation`.
`BudgetTracker::try_reserve_batch` creates several reservations atomically and
in input order. A batch either reserves every requested share or changes
nothing.

Committed usage, aggregate outstanding reservations, and individual
reservation balances are protected by one ledger lock. Validation includes
both committed and reserved resources, so work outside a reservation cannot
spend resources promised to a sibling.

A tracker obtained from `BudgetReservation::tracker` can consume only that
reservation's remaining share. Nested reservations are allowed but cannot
exceed the parent share.

Reservations are leases:

- consumption converts reserved resources into committed run-tree usage;
- cloning a scoped tracker shares the same lease;
- dropping the final reservation or scoped tracker releases its unused
  balance to the run tree;
- released nested capacity returns to the run tree, not to an already
  subdivided parent lease.

`RunContext::child_reserved` binds a child run to a scoped tracker and rejects
a reservation created by another run tree.

## Parallel node contract

`WorkflowBuilder::parallel` creates one durable fan-out/fan-in node with at
least two uniquely named `ParallelBranch` values.

Every branch:

- receives the same canonical input value;
- declares an exact capability set;
- declares its maximum budget reservation;
- executes in a capability-attenuated child run.

All branch reservations are acquired as one batch before any branch starts.
If the batch cannot be funded, no branch runs.

Branches are polled concurrently. Completion order never defines output order:
the joined canonical output is a JSON object keyed by `StepId`, whose
`BTreeMap` representation has stable key order.

The initial policy is fail-fast. A failed branch produces a typed
`WorkflowError::ParallelBranch`, marks the failed branch durably, and cancels
every unfinished sibling child run. Cancellation is cooperative: an external
request already accepted by a remote service may still have a cost or effect.

## Checkpoint protocol

Workflow checkpoint schema version 3 retains the parallel phase introduced by
version 2:

```text
ParallelInFlight {
    step,
    branches: {
        branch_id: InFlight | Completed { output } | Failed
    }
}
```

The scheduler persists all branches as in-flight before launching any work.
Each successful output is persisted independently. The parallel node is
committed only after every branch has a durable completed output.

Recovery follows these rules:

1. Completed branches are never re-executed.
2. If any branch is incomplete, `RejectAmbiguous` rejects recovery.
3. `RetryInterruptedStep` explicitly authorizes only incomplete branches to
   run again.
4. If every branch output is already durable, the node can finish under
   `RejectAmbiguous`; no external work is replayed.
5. Resume acquires a fresh atomic reservation batch only for incomplete
   branches.
6. Restored usage never decreases below the checkpoint snapshot.

Version 3 also persists branch failure explanations for race recovery.
Versions 1 and 2 are intentionally rejected rather than guessed into the
current branch-state model.

## First-success race

Side-effect-safe first-success competition is implemented in
[RFC 0025](0025-safe-first-success-race.md). It admits only `Pure` and
`ReadOnly` branch capabilities and conservatively forfeits every losing
reservation because remote cancellation may be unconfirmed.

## Invariants

1. A reservation batch is all-or-nothing.
2. A scoped tracker cannot consume a sibling's reservation.
3. Unused reservation capacity is eventually released.
4. Parallel authority amplification fails before any branch executes.
5. Parallel budget failure occurs before any branch executes.
6. Output order is independent of branch completion order.
7. Every completed branch output is persisted independently.
8. Stable recovery never re-executes a completed branch.
9. Ambiguous incomplete branches require explicit retry authority.
10. A branch failure cancels every unfinished sibling run.
