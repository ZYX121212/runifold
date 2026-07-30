# RFC 0059: Task tombstone lifecycle governance

- Status: implemented
- Scope: `runifold-workflow`, `runifold-store-postgres`
- Depends on: RFC 0057, RFC 0058

## Decision

Detailed Task tombstones are retained indefinitely by default. Physical purge
is available only through the optional
`WorkflowTaskTombstoneGovernanceStore`; ordinary workflow execution and Task
cleanup stores do not gain audit-destruction methods.

Purge requires all of the following:

1. a trusted principal monotonically confirms an external archive receipt;
2. the tombstone is older than a separate audit-retention duration;
3. no active legal hold protects it;
4. a current fenced cleanup owner prepares an exact bounded member set;
5. a different principal approves that set before its deadline;
6. a current fenced cleanup owner executes it;
7. the store atomically rechecks membership and holds, deletes the detailed
   rows, and inserts immutable aggregate evidence.

## Legal holds

A legal hold is tenant- and checkpoint-scoped and records the placing
principal, bounded reason, and database time. Release records a separate
principal and database time rather than deleting the hold row. Multiple
place/release generations therefore remain auditable.

Preparation excludes active holds. Execution checks active holds again in the
same PostgreSQL statement as deletion. A hold placed after approval therefore
blocks execution until explicitly released.

## Export confirmation

An export confirmation contains tenant, global tombstone cursor, opaque
archive receipt, confirming principal, and database time. The cursor is a
monotonic watermark:

- a higher cursor advances it;
- an exact cursor and receipt retry is idempotent;
- a lower cursor or replacement receipt is rejected.

Only tombstones at or below the tenant watermark may enter a purge intent.
Future tombstones always have greater global sequences and are not
accidentally authorized by an earlier export.

## Prepared sets and independent approval

Preparation runs under an exact tenant/owner/fencing-token lease. It freezes
at most 1,000 eligible sequences in an intent and records count, first/last
cursor, captured export watermark, deadline, and a deterministic ordered-set
fingerprint. The fingerprint is an integrity correlation value, not a
cryptographic signature.

The preparer cannot approve its own intent. Approval is idempotent only for
the same independent principal and is rejected after expiration. Pending
unexpired intents prevent the same tombstone from entering another prepared
set.

## Fenced execution and evidence

Execution may be recovered by a new cleanup owner after process failure. It is
not tied to the preparation lease, but it always requires a current lease.
The stale owner receives `LeaseLost`.

PostgreSQL verifies that the intent remains approved and unexpired, every
prepared item still exists under the same tenant, item count exactly matches
the prepared count, and no item has an active hold. Detailed deletion and
evidence insertion are one data-modifying CTE operation. Evidence records:

- purge and tenant identity;
- preparer, approver, and executor;
- exact count and cursor range;
- captured export watermark and fingerprint;
- database execution time.

Successful execution is idempotent: a retry returns the same evidence.
Evidence remains after detailed tombstones and prepared items are removed.

## Verification

The disposable PostgreSQL test covers missing export, monotonic export
rollback rejection, active-hold exclusion, self-approval rejection,
independent approval, a legal hold racing after approval, hold release,
preparer lease release, higher-token executor takeover, stale executor
rejection, atomic purge, idempotent retry, durable evidence lookup, and
survival of the independently held tombstone.
