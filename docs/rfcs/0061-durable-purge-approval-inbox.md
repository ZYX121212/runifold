# RFC 0061: Durable purge approval inbox

## Status

Implemented.

## Problem

An approval API keyed only by a purge ID is not an operator workflow. It does
not provide discovery, bounded ownership, stale-reviewer recovery, rejection
evidence, or protection against two reviewers deciding the same request after
a process pause.

## Decision

Runifold models purge review as a tenant-scoped durable inbox. Preparing a purge
creates an approval row alongside the immutable intent. Reviewers may list a
bounded inbox and atomically claim the oldest eligible request.

An approval claim contains the tenant and purge identity, authenticated
reviewer, monotonically increasing fencing token, and store-authoritative
expiration. Approval and rejection compare all four properties in the same SQL
statement that records the decision.

## Timeout recovery

A claim query may select either a pending row or an expired claimed row.
`FOR UPDATE SKIP LOCKED` serializes competing reviewers, and the winning update
increments the fencing token. A delayed reviewer therefore cannot decide after
another process takes over.

The immutable purge approval window remains the outer deadline. Reviewer claims
use `LEAST(claim_deadline, intent_deadline)` and cannot extend it.

## Four-eyes and rejection

The preparer is excluded from claim candidates. Approval repeats that
independent-principal check at the durable mutation boundary.

Rejection records the authenticated reviewer, a validated bounded reason, and
database time. A rejected request remains visible for audit and cannot be
claimed again.

## Authorization and telemetry

The control plane defines separate tenant permissions for reading the inbox,
claiming review work, approving, and rejecting. Policy failures fail closed.
Telemetry records only fixed operation and outcome dimensions; tenant,
reviewer, purge, and fencing identities are excluded.

## Upgrade behavior

Schema creation adds the approval table idempotently. Inbox operations backfill
rows for pre-existing pending and approved intents with
`ON CONFLICT DO NOTHING`.

## Verification

The real PostgreSQL suite verifies pending discovery, expired-claim takeover
with a greater fencing token, stale-reviewer rejection, durable rejection
identity and reason, cross-reviewer claim misuse, and successful approval.
