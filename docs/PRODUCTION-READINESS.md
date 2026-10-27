# Production readiness review

Review date: 2026-10-26. Scope: current local checkout, including pre-existing
uncommitted changes. This records bounded audit passes, not a release attestation
or a full security review. The package version comes from the workspace manifest;
published API documentation can describe a different revision.

## Assessment

The proposed focus on verifiable execution semantics is consistent with the
[charter](CHARTER.md). Numerical maturity scores without workload measurements
do not establish readiness. Distinguish missing code, a configured verification
gate, and evidence that the gate passed on the candidate revision.

Several proposed prerequisites already have implementations and executable
tests: SQLite forced-process recovery, PostgreSQL restart recovery, lease
fencing, receipt reconciliation, durable publication, and bounded soak runs.
See [reliability evidence](RELIABILITY.md), [testing](TESTING.md), and the
[release contract](RELEASE-0.10.md). These need candidate-specific results,
not duplicate subsystems.

Capability grants govern controlled runtime entry points in a trusted host.
They cannot isolate arbitrary Rust/Python/Shell code. An application executing
untrusted code needs a separately enforced process/container/VM boundary,
filesystem and network restrictions, credential isolation and resource limits.
Whether to ship an adapter is an application/deployment decision; the runtime
must not describe capability checks as an operating-system sandbox.

Long-term semantic memory remains separate from conversations and execution
journals. A domain memory engine, general remote service and Python/TypeScript
SDKs require use-case and protocol design; they are not prerequisites for every
embedded Rust application. Keep them outside the correctness fix below.

## First confirmed defect and fix

`EffectExecutor` previously checked only that a capability ID was present. An
Effect request could specify a different classification from the grant. In
particular, a non-idempotent write labelled `Pure` could enter `RetrySafe`
recovery. This is a safety-contract defect even though the host application is
responsible for truthful handlers and descriptors.

Preflight now rejects every classification mismatch as `CapabilityDenied`
before store access, dispatch, remote reconciliation or completed-result replay.
No public type or persistence schema changes. Correctly classified requests
keep their existing behavior. Previously inconsistent integrations must align
their request with the current grant, not weaken the grant to preserve retries.

Regressions cover all 30 unequal pairs of the six current effect classes,
relabelled started-write recovery, and changed-grant denial of both remote
reconciliation and completed-result replay. The mismatch regression was run
before the fix and failed because the handler executed; it passes after the fix.

## Recovery failure matrix examined

| Boundary | Executable evidence | Limit |
| --- | --- | --- |
| External write succeeds; local completion write fails | SQLite `effect_commit_failure`: real trigger abort, fresh connection, unsafe retry rejection, receipt reconciliation, second reopen/replay | Local filesystem publisher; no remote API or power-loss claim |
| Completed Tool effect; Agent checkpoint not yet acknowledged | SQLite `process_crash_recovery`: parent kills synchronized child; completion replays once | Specific SQLite boundary |
| Review in flight or approved Tool plan before dispatch | SQLite `terminal_review_process_crash`: forced kill and recovery | Does not establish every approval/cancellation race |
| Worker dies with lease and reserved budget | SQLite `workflow_process_crash`: successor takes a higher fence and adopts reservation | Local process/database fixture |
| Multiple workflow claimants | SQLite `workflow_store` and Workflow unit suite | Deployment-scale contention still needs measurement |
| PostgreSQL restart and persistence | Container-backed store tests, including stop/restart, concurrent claims and receipt persistence | Executed in the follow-up below; no deployment-scale or long-soak claim |

Run the first-pass boundary checks with:

```sh
cargo test -p runifold-effect --locked
cargo test -p runifold-agent -p runifold-workflow -p runifold-store-sqlite \
  --features runifold-store-sqlite/bundled --locked
```

Local validation for this change passed:

- Effect, Agent, Workflow and bundled SQLite tests, including the forced-process
  crash fixtures and the new SQLite completion-failure integration test;
- Clippy for those four crates with `--all-targets --all-features --locked -- -D warnings`;
- `cargo fmt --all -- --check`;
- `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked`;
- AI knowledge generation/check and the ten `scripts/test-ai-system.py` tests.

The existing executor file remains one coordination responsibility with a local
test module; the additional lines are boundary regressions. No module-tree,
crate ownership or dependency change is needed for this correction.

## Follow-up: grant propagation and recovery contention

A second confirmed defect allowed a child grant to retain a parent's capability
ID but redefine its descriptor. The new core regression failed before the fix.
Subset validation now checks the complete descriptor without cloning it.
Normal and budget-reserved children, Gateway routes and Workflow steps all
share that validation. There is no public signature or schema change.
Integrations that rewrote a parent descriptor must instead select the original
grant; use Run metadata for child-specific execution information.

The public facade's `authority_contract` tests verify rejection before model
or Workflow step execution and before delegation budget consumption. They
also verify that an unchanged selected grant still preserves lineage, reserved
budget and hierarchical cancellation. The kernel's descriptor mutation matrix
covers name, version, kind, both schemas, effect, risk and policy metadata.

The stronger check exposed an MCP completion adapter that used a projected
completion descriptor under the referenced resource's ID. Completion now keeps
the original granted descriptor, checks reference identity/kind/version, and
rejects failed request scoping without falling back to server-wide authority.
Mismatched MCP Tool descriptors return a protocol denial rather than panicking
when child creation fails. Public MCP regressions cover original grant
preservation, absent ambient grants, conflicting completion references and
rejection before provider/Tool work. Existing dynamic-context tests still pass.

The new SQLite `effect_concurrent_recovery` test forces two independent
connections to read the same `Started` revision before reconciling. Both
completed-result and proven-not-executed branches have one local commit winner;
the stale caller cannot dispatch a second handler. Fresh connection replay
keeps the winning result. This does not establish recovery exclusion when an
old external owner is still active: `NotExecuted` now explicitly requires
proof that no previous attempt can still commit.

The kernel contract also executes in Node on `wasm32-unknown-unknown` with the
declared Rust 1.88 toolchain and pinned `wasm-bindgen-test-runner` 0.2.126. Both
edge tests, including child descriptor integrity, passed locally. The real
PostgreSQL store suite passed against disposable containers, including restart,
concurrent lease takeover, tenant limits and durable checkpoint/Effect state.

The current working tree has concurrent/pre-existing model and observability
work. Final validation therefore records a source fingerprint as well as Git
HEAD and dirty status, rather than attributing all local tests to the commit.
Local logs and the evidence manifest live under
`target/reliability-evidence/production-readiness-*`. They are local verification
artifacts, not published CI or independent production evidence.

### Follow-up verification result

The frozen candidate passed `cargo test --workspace --all-features --locked`:
932 tests passed, zero failed, and four were explicitly ignored (live MinIO,
live Ark, live OpenAI, and one macro documentation example). Workspace Clippy
with all targets/features and warnings denied, workspace rustdoc with warnings
denied, formatting, AI knowledge checks and all ten AI-system tests also passed.
Two additional kernel edge tests passed under Rust 1.88 in Node.

`target/reliability-evidence/production-readiness-local.json` records the source
fingerprint, dirty origin, compiler/runtime identities, counts and log hashes.
The frozen source fingerprint stayed unchanged during verification. All 13
security implementation, regression and evidence-writer files checked against
the original working tree matched the verified snapshot. This does not certify
unrelated edits made to the shared working tree after the snapshot was taken.

## Workflow upgrade acceptance

The next audit found a version-label defect: `WorkflowCheckpointCursor::save`
used the generic envelope's `next` method, which retained a legacy schema number
even after the current runtime advanced its payload. The new regression failed
with schema 3 where schema 5 was required. The owning Workflow writer now changes
schema and payload together in the existing CAS; a failed CAS leaves its cursor
and the winning stored revision intact.

The new adapter matrices cover schema v3/v4 Ready states with earlier completed
work, schema v5 as a control, and future schema v6. SQLite and real PostgreSQL
retain completed outputs and known usage, advance supported states to v5,
replay completion without another step, fence the drained owner's writes, and
reject future state before work without replacing its envelope. SQLite also
checks immutable revision history. The PostgreSQL fixture removes the pre-0.10
missing admission helper: claim fails without queue mutation until explicit
`ensure_schema` installs it. This is the helper boundary, not a complete copy of
every old database DDL layout.

`scripts/verify-workflow-upgrade.py` additionally builds the actual Workflow
reader library from the resolved v0.7.2 Git object. That reader accepts v3/v4
Ready fixtures, then rejects their v5 Completed successors produced by the
current canonical resume path. Both readers leave fixture files unchanged.
The current version is derived from Cargo metadata; the historical commit and
source fingerprint are saved with the reader matrix. Missing prerequisites
replace cached pass evidence with failure; a dedicated regression verifies this.

Local validation passed: 919 default/all-target tests and 938 all-feature tests
(four explicitly ignored in the all-feature run), workspace Clippy and rustdoc
with warnings denied, full-workspace/all-target/all-feature Rust 1.88 checks,
formatting, AI knowledge and AI-system checks, and the upgrade gate regression.
The configured `Workflow upgrade compatibility` CI job runs the same gate and
uploads its result; no hosted CI execution is asserted here. Evidence is in
`target/reliability-evidence/workflow-upgrade.json` and the associated validation
logs. Active mixed-version pools, arbitrary historical DDL migrations, external
operation fencing and long soaks remain outside this acceptance.

## Next acceptance work

Framework operational follow-up adds locked offline dependency/source identities
(`runifold ai environment`), exact vendor patch detection, safe default Provider
failure messages, and a repeated 16896-request resource fixture. The fixture
requires complete queue/capacity release, exact physical-attempt counts, bounded
post-warmup RSS growth (8 MiB) and a 30-second deadlock/recovery deadline.
SQLite and real PostgreSQL now also exercise legacy/future Session admission
rejection without record mutation or model dispatch, plus current PostgreSQL
Session replay after reconnect without another Provider call. These are scoped
framework contracts; arbitrary deployment isolation or live vendor billing does
not follow from them.

1. **P0: deployed recovery ownership.** Extend the matrix to cancellation versus
   approval/receipt persistence and recovery callers observing different revisions.
   Same-revision contention is now covered. Include active
   old-owner handling; local CAS is not a fence on an arbitrary external API.
2. **P0: broader upgrades.** Workflow Ready-state, historical-reader, PostgreSQL
   helper and Session admission rejection/replay boundaries have explicit tests.
   Any additional promised historical database layout needs its own migration
   fixture. Keep mixed-version pools unsupported and retain external-effect
   reconciliation in rollback procedures.
3. **P0 for untrusted-code applications: isolation acceptance.** Select an
   execution backend and prove denied file reads, secret access, network egress
   and process/resource escape against that backend. No general isolation claim
   follows from this review.
4. **P1: sustained evidence.** Run existing PostgreSQL/SQLite/Provider suites and
   the existing 180-minute release soak on the same candidate revision. Define
   resource-growth, loss and recovery-time thresholds before adding a longer
   72-hour deployment workload; duration alone is not a correctness proof.
5. **P1: operational reconstruction and privacy.** Verify journal/trace gaps,
   redaction and operator recovery on failed executions, using a realistic
   application. A trace replay must not implicitly repeat external writes.

Acceptance requires saved command results and candidate/environment identity.
Neither this review nor the existence of CI configuration establishes a
long soak, independent benchmark, cloud deployment, security audit or production
adoption. PostgreSQL execution is the bounded local result described above.
