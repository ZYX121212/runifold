# RFC 0025: Side-Effect-Safe First-Success Race

## Status

Implemented.

## Problem

A naïve race drops every losing future after the first result. That does not
prove a remote model request, Tool call, or delegated Agent stopped. The
losing work may still mutate state or incur usage after its terminal event is
no longer observable.

Runifold must therefore treat race as a governed execution primitive rather
than a convenience wrapper around `select`.

## Contract

`WorkflowBuilder::race` executes at least two `ParallelBranch` values and
returns the first successful canonical output.

Every external operation performed by a `WorkflowStep` must be represented by
the branch's declared capability set. Race construction accepts only
capabilities classified as:

- `EffectClass::Pure`;
- `EffectClass::ReadOnly`.

`IdempotentWrite`, `NonIdempotentWrite`, `Destructive`, and `Unknown`
capabilities fail during workflow construction. Idempotency prevents duplicate
writes; it does not make an unwanted losing write acceptable.

An Agent may participate only with a capability-attenuated run that cannot
invoke write-capable Tools or child Agents. Model calls remain potentially
billable read operations and are governed by reservation forfeiture.

## Fair start

All race branches acquire their reservations atomically. An initial-poll
barrier then polls every branch once before any ready result can win.

This prevents branch registration order from turning a race into a biased
sequential selection when one future completes immediately.

## First-success behavior

A failed branch is persisted and does not end the race while another branch
can still succeed. The first successful branch becomes the sole winner.

If every branch fails, the workflow returns
`WorkflowError::RaceAllFailed` with stable branch identifiers and safe
persisted failure explanations.

## Losing work and conservative accounting

After a winner is selected, Runifold:

1. cancels every active losing child run;
2. atomically converts each losing reservation's unconsumed balance into
   committed usage;
3. records each losing branch as `Cancelled`;
4. persists the winner, branch states, and conservative usage snapshot.

Forfeiture is deliberate. A provider may continue remotely after local
cancellation and may never deliver a terminal usage event. Charging the full
remaining reservation preserves a hard upper-bound interpretation instead of
reporting an optimistic value.

The reservation must therefore reflect the application's tolerated worst-case
waste for that branch.

The race scheduler also observes parent cancellation directly. Even a custom
step that ignores its child token cannot keep the workflow pending forever;
active branches are cancelled, conservatively charged, checkpointed, and the
workflow returns `WorkflowError::Cancelled`.

## Recovery

Checkpoint schema version 3 adds:

```text
RaceInFlight {
    step,
    branches: {
        branch_id:
            InFlight
            | Completed { output }
            | Failed { message }
            | Cancelled
    }
}
```

Recovery rules:

1. Exactly one completed branch is a durable winner and can be committed
   without re-execution.
2. An all-failed state reproduces the aggregate failure without retrying.
3. A race with no winner and incomplete branches is ambiguous under
   `RejectAmbiguous`.
4. `RetryInterruptedStep` reruns only incomplete or cancelled branches and
   preserves known failures.
5. A winner checkpoint cannot contain another in-flight branch.
6. Restored usage must match a terminal race checkpoint exactly.

Schemas 1 and 2 remain explicitly unsupported rather than being inferred.

## Scope

This contract makes race safe against undeclared write capabilities and
unobserved losing usage. It cannot prove a third-party provider honored
cancellation, and it does not make hidden side effects inside a dishonest
custom `WorkflowStep` safe.

Future provider adapters may expose confirmed remote cancellation, allowing
unused losing reservations to be released instead of forfeited.

## Invariants

1. No branch starts unless every branch reservation succeeds.
2. Every branch is initially polled before a winner is accepted.
3. Only `Pure` and `ReadOnly` capability sets may race.
4. At most one durable winner exists.
5. Early failures do not mask a later success.
6. Known failed branches are not replayed during recovery.
7. Losing active branches are cancelled and conservatively charged.
8. A durable winner is never re-executed.
9. All-failed recovery is deterministic.
10. Parent cancellation terminates the race independently of step cooperation.
