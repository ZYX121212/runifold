# RFC 0060: Authorized Task governance and archive delivery

- Status: implemented
- Scope: `runifold-workflow`, `runifold-observability-otel`
- Depends on: RFC 0059

## Decision

`WorkerId` is an audit identity, not proof of authority. Applications perform
tombstone governance through `WorkflowTaskGovernanceControlPlane`, which
checks an authenticated principal, exact tenant, and exact operation with a
pluggable `WorkflowTaskGovernanceAuthorizer` before touching the archive or
store.

Permissions are deliberately narrow:

- `PlaceHold`;
- `ReleaseHold`;
- `Export`;
- `PreparePurge`;
- `ApprovePurge`;
- `ExecutePurge`;
- `ReadEvidence`.

The built-in static authorizer is deny-by-default and supports explicit
principal/tenant/permission grants. Production applications may implement the
same asynchronous boundary using their policy engine. A policy-backend error
is distinct from denial but both fail closed.

## Identity integrity

Control-plane requests do not accept an independent audit actor. The store
actor is always cloned from the authenticated principal after authorization.
Callers therefore cannot ask as one principal and persist another identity.

Prepare and execute additionally require the fenced cleanup lease owner to
equal the authenticated principal. A grant cannot be combined with another
process's stolen or accidentally forwarded lease.

Four-eyes approval remains store-enforced. An authorized prepare principal
still cannot approve its own intent.

## Archive delivery

`WorkflowTaskTombstoneArchive` receives an exact ordered batch containing:

- tenant;
- detailed tombstones;
- a stable idempotency key derived from tenant and first/last cursor.

The archive must persist the exact batch or replay its original receipt for
that key. The control plane advances the durable export watermark only after
the receipt returns. If the process crashes after archive success but before
confirmation, retry reads the same tombstone page, produces the same key, and
reuses the same receipt. Durable confirmation is monotonic and idempotent.

An empty page performs no archive call and does not manufacture a receipt or
watermark.

## Observability

`OtelWorkflowTaskGovernanceMetrics` records
`runifold.workflow.task_governance.operations` with only fixed `operation`
and `outcome` dimensions. Outcomes distinguish success, policy denial,
authorization outage, store failure, and archive failure.

Principal, tenant, checkpoint, purge, batch, and receipt identities are never
metric attributes.

## Verification

The disposable PostgreSQL test verifies:

- ungranted export is rejected before the archive is called;
- authorizer outage fails closed before the archive is called;
- two replayed exports produce the same batch key, receipt, report, and
  monotonic durable confirmation;
- a hold's persisted actor equals the authenticated principal;
- a principal with prepare permission cannot use another owner's cleanup
  lease.

The OTel test verifies fixed operation/outcome attributes without tenant
identity.
