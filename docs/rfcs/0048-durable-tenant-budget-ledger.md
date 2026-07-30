# RFC 0048: Durable tenant budget ledger

Status: implemented

## Problem

Recording model and Agent usage only after execution cannot enforce a hard
aggregate tenant budget. Concurrent workflows can all observe remaining
capacity, start expensive work, and overspend before any completion is
recorded. Crashes also make post-hoc accounting ambiguous.

## Decision

`WorkflowTenantBudgetPolicy` configures a fixed draining window, a hard
multi-resource limit, and a crash-recovery grace period. The controlled
dimensions are tokens, micro-US-dollar cost, attributed duration, Agent turns,
tool calls, and delegations.

Before workflow code runs, the Worker reserves the definition's remaining
finite envelope with `WorkflowStore::reserve_budget`. After checkpointed
execution it calls `settle_budget` with cumulative durable usage, committing
the observed delta and releasing unused capacity. A definition must provide a
finite limit for every dimension controlled by its tenant policy.

Tenants without a budget policy retain backward-compatible unmetered
admission.

## Ledger model

Each tenant window contains:

- `committed`: settled usage plus conservatively forfeited reservations;
- `reserved`: unused upper bounds held by active workflows;
- one reservation per checkpoint with its cumulative baseline, remaining
  amount, and recovery expiration.

New work is admitted only when `committed + reserved + request` fits every
configured tenant limit. PostgreSQL serializes these mutations on the tenant
row inside database functions, so concurrent workers cannot oversell a
dimension.

The window resets only after its duration elapses and no reservation remains.
This draining rule avoids erasing liability that began in the previous window.

## Recovery and fencing

A reservation belongs to the checkpoint, not a process. A new fencing lease
may adopt it after a worker crash. Adoption compares the new cumulative
checkpoint baseline with the stored baseline, commits the observed controlled
delta, and preserves the original remaining envelope. Changing the definition
envelope during recovery is rejected.

Heartbeats extend reservation recovery expiration with the lease. Settlement,
adoption, and heartbeat require the current unexpired owner and fencing token.
A stale worker cannot spend, settle, or release a successor's capacity.

If no successor adopts before the grace period ends, the remaining reservation
is moved to committed usage. Cancellation applies the same conservative
forfeiture. This intentionally prefers false-positive charging over silent
overspend when the store cannot prove what external work occurred.

## Worker behavior

- Budget exhaustion requeues the task after a configurable delay without
  executing workflow code.
- A definition missing a tenant-controlled finite limit fails before
  execution.
- Successful, failed, retried, and suspended executions settle usage before
  releasing their lease.
- Heartbeat loss leaves the reservation recoverable by a fenced successor.
- Finishing a task with an active reservation is rejected.

## Persistence

The in-memory adapter is the executable reference model. The PostgreSQL
adapter persists policy and counters on the tenant row and reservations in a
checkpoint-keyed table. Database-time maintenance atomically forfeits expired
reservations and resets drained windows.

Schema creation remains explicit through `ensure_schema`; runtime queue
operations never perform hidden migrations.

Every state-changing budget decision also appends the immutable audit fact
defined by [RFC 0049](0049-tenant-budget-observability.md) in the same
in-memory critical section or PostgreSQL transaction.

## Invariants

1. Controlled aggregate usage never exceeds the configured hard limit at
   admission time.
2. A checkpoint has at most one recoverable reservation.
3. Only the current fenced lease can reserve, adopt, or settle.
4. Cumulative usage cannot move backwards.
5. Settlement cannot charge more than the reserved envelope.
6. A task cannot release its lease while a reservation remains active.
7. Expiry and cancellation never release uncertain spend as unused capacity.
8. Window reset never discards an active reservation.
9. A successful ledger mutation and its audit fact are committed atomically.
