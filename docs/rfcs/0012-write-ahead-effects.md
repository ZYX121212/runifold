# RFC 0012: Write-ahead effects and idempotent recovery

- Status: Accepted for initial implementation
- Scope: `runifold-effect`, `runifold-core`

## Summary

Runifold coordinates external effects with a persisted state machine. The goal
is not to promise unconditional exactly-once execution, which cannot be
guaranteed across arbitrary external systems. The goal is to make uncertainty
explicit and use idempotency contracts where they exist.

## State machine

Each logical effect has a revisioned `EffectRecord`:

```text
Prepared
   │ CAS
   ▼
Started ───────────────► Completed(output)
   │
   └───────────────────► Failed(error)
```

`Prepared` is durable before handler execution. `Started` is durable before
the handler is called. If a process disappears while the record is `Started`,
the handler may or may not have produced an external effect.

## Store contract

`EffectStore` supports:

- load by Effect ID;
- lookup by capability-scoped idempotency key;
- compare-and-swap creation and update.

The idempotency index key is:

```text
(CapabilityId, idempotency_key)
```

This prevents unrelated capabilities from accidentally sharing a key
namespace. Reusing a key or Effect ID for different kind, input, effect class,
or capability is rejected.

## Execution order

The initial executor:

1. checks Run capability, cancellation, and deadline;
2. resolves Effect ID or idempotency key;
3. creates `Prepared` if no record exists;
4. emits `Effect::Requested`;
5. atomically writes `Started`;
6. emits `Effect::Started`;
7. invokes the handler;
8. atomically writes `Completed` or `Failed`;
9. emits the corresponding terminal event.

A completed record returns its stored output without executing the handler.

The EffectStore always retains the output required for recovery. Journal output
capture is separate: `EffectEventPayloadPolicy::Redacted` is the default and
writes only a marker. `Full` must be selected explicitly.

## Recovery

`EffectRecoveryPolicy::RejectAmbiguous` refuses every `Started` record.

`RetrySafe` permits retry only for:

- `Pure`;
- `ReadOnly`;
- `IdempotentWrite` with an idempotency key.

Applications whose remote system exposes operation lookup can use
`EffectExecutor::execute_reconciled`. Its `EffectReconciler` queries by the
stable remote/idempotency identity before recovery:

- `Completed(output)` durably records and replays the observed remote result;
- `NotExecuted` is proof that even a non-idempotent handler may now execute;
- `Ambiguous` retains the normal recovery policy and never grants retry by
  itself;
- reconciliation transport failures return `Reconciliation` and leave the
  Effect in `Started` for a later attempt.

Handler failures classified as `RequiresIdempotency`,
`UnsafeAfterVisibleOutput`, `UnsafeAfterSideEffect`, or `Unknown` also retain
the durable `Started` state. They are never converted into a false terminal
`Failed` record merely because the response channel closed. A later call can
reconcile the remote operation or apply the explicit safe-retry policy.

The reconciler is a recovery boundary, not a distributed transaction. A
remote system that cannot query a stable operation identity cannot close the
post-commit/pre-`Completed` crash window safely.

It rejects:

- `NonIdempotentWrite`;
- `Destructive`;
- `Unknown`;
- `IdempotentWrite` without a key.

Retrying a `Started` record performs another CAS transition to claim a new
attempt. Concurrent stale workers conflict instead of both silently advancing
the record.

## Cancellation

If cancellation wins while a handler future is active, the record remains
`Started`. Runifold does not mark it failed because dropping a local future
cannot prove that a remote side effect did not occur.

Subsequent recovery therefore follows the same ambiguity rules.

## Observability failure

The EffectStore is the recovery source of truth. A Journal failure after a
state transition stops further orchestration but does not roll back the
persisted transition. Recovery inspects the EffectRecord rather than inferring
state solely from events.

## Initial invariants

1. Capability denial occurs before persistence or handler execution.
2. Handler execution never begins before a durable `Started` record.
3. Completed effects return durable output without handler execution.
4. Idempotency keys cannot alias different logical work.
5. Ambiguous unsafe effects are never automatically retried.
6. Cancellation during execution leaves an ambiguous `Started` record.
7. Revision CAS rejects stale writers.
8. Effect events use the owning Run's Journal.
9. Complete effect output is not copied into the Journal by default.

## Limitations and deferred decisions

- The in-memory store is not process durable.
- External exactly-once behavior still depends on the remote system honoring
  its idempotency key.
- Leases, heartbeats, attempt identities, backoff, and distributed ownership
  are deferred.
- Tool and Agent delegation execution use this boundary as specified by
  RFC 0013.
- SQLite/PostgreSQL stores and transactional Journal integration are deferred.
