# Runifold 0.10: supported execution contracts

Runifold 0.10 graduates from pre-alpha to a maintained release for production
integration. This is a bounded support commitment, not a claim that every
application, model, or deployment is production-ready. All 22 public crates
retain synchronized versions and the pre-1.0 SemVer policy: patches preserve
public contracts; incompatible changes require a new minor version and migration
notes. This release does not promise 1.x API stability or a long-term support
window for older minors.

## Supported paths and responsibilities

| Path | Runtime contract | Application responsibility |
| --- | --- | --- |
| `AgentSession` with durable storage | Admission before model/tool work, request/input binding, completed-result replay, explicit owner-exit recovery | Authenticate callers; bind IDs and namespaces to tenants; route every writer through Session; stop/fence the old owner before recovery |
| Durable streams | `ConversationCommitted` follows persistence; Session emits it after admission release | Treat model deltas as provisional; buffer content if it must pass review before disclosure; poll execution and explicitly recover dropped streams |
| Tools and child Agents | Capability checks on controlled invocation, shared resource accounting, bounded execution and attenuated delegation | Assign truthful tool effect classes, schemas, stable IDs and versions; implement resource/parameter Policy and current grants |
| Reviewer | Bounded plan/output review and checkpoint-bound reviewer configuration | Supply the acceptance rubric; Reviewer approval never grants authority or proves a business effect happened |
| Effect executor | Write-ahead intent, durable result replay, explicit ambiguity, optional read-only reconciliation | Correlate remote receipts; implement external idempotency/fencing where available; re-authorize new work and handle compensation |
| Automatic Session summaries | Checkpointed batches, bounded passes, shared known usage, acknowledgement-loss recovery | Set context limits and budgets; resolve conflicting summaries and unknown external usage |
| Concurrent tools | Opt-in bounded contiguous read-only batches, ordered results, serial write barriers, sibling draining | Declare effects accurately; default execution remains serial |

Low-level model clients, direct store writes, and custom host code do not
implicitly acquire Session admission or application Policy. Runifold is embedded
in a trusted Rust host; capabilities are not a sandbox for arbitrary host code.
Direct Effect invocation checks capability and lifecycle; callers must account
for application-specific resource costs. There is no universal parameter Policy
or approval service shared by every low-level entry point.

Model completion, Reviewer approval, conversation commit and external publication
are distinct facts. Only the application can define which facts satisfy its
business task. Cancellation stops future local work; it does not undo an external
operation already accepted by another system.

## Compatibility and deployment

- Rust 1.88 is the MSRV. Native workspace checks, isolated Provider features,
  and the documented WASM/browser matrix are release acceptance boundaries.
- SQLite recovery is exercised across independent connections and process kills.
  PostgreSQL durability, restarts and leases have server-backed tests. The new
  Session-specific PostgreSQL admission stress matrix remains outside this
  release's verified claims; use the documented store contracts and validate
  the intended deployment before expanding beyond the tested combinations.
- Agent checkpoints must match `AgentRecoveryContract`, Agent/model identity,
  and Reviewer configuration. Stable descriptors are an application obligation;
  this comparison does not fingerprint custom code or provider-side changes.
- Session admission schema 3 binds the main Agent and Reviewer definitions before
  the first Agent checkpoint, plus summary configuration and progress. Earlier
  unpublished schema 1/2 records are rejected. Do not silently migrate them.
- Before upgrading from 0.9, drain active Agent work with the original workers.
  Retain the old worker build for existing checkpoints, including completed-result
  access that still requires the old execution contract. Back up persistent data.
- After 0.10 advances state, do not send it to older workers. Roll back by routing
  untouched old work to its compatible deployment; use a forward fix for advanced
  work. Restoring a database snapshot requires independent reconciliation of
  external effects and may otherwise duplicate operations.
- Workflow schema v5 and its existing v3/v4 reader compatibility are unchanged.
  The older v0.7/v0.8 mixed-worker restriction in `RELEASING.md` still applies.

For an occupied Session: first stop or fence its owner, inspect the active request
and revision, restore known budget usage with `recovery_usage`, then call
`recover_after_owner_exit` with the exact bound input, revision and explicit
`ResumePolicy`. A crash between transcript commit and admission release can leave
the next request blocked even though the previous result committed. Replaying the
same request retrieves that result; explicit recovery releases admission.

## Executable reference application

```sh
cargo build -p runifold --example durable_publish --features sqlite-bundled --locked
python3 scripts/verify-durable-publish.py target/debug/examples/durable_publish
```

The application uses a scripted model, deterministic Reviewer, a durable Session,
and a separately persisted local report publisher. The harness runs actual child
processes and kills the publisher after its receipt is durable but before the
runtime can record Effect completion. Recovery reads that receipt, verifies the
accepted report, and completes without publishing or generating again. A second
physical write would fail (`create_new`), so accidental re-execution cannot hide
behind an idempotent test double.

The same harness checks ordinary publication and replay, revoked authority,
recovery with mismatched external content, and repeated completed recovery. Its
report is `target/reliability-evidence/durable-publish.json`. The example does not
claim real model quality, a cloud publishing integration, power-loss durability,
a multi-host fence, or an end-user approval inbox. It demonstrates how the current
contracts compose; the publishing adapter and final receipt acceptance belong to
the application.

For automatic history compaction and request replay:

```sh
cargo run -p runifold --example durable_session --features sqlite-bundled -- /tmp/runifold-session.sqlite
```

## Release acceptance and evidence

A candidate must pass `scripts/release-check.sh`, the complete CI matrix and
security checks, a completed 180-minute reliability soak, and an executable
standalone Rig comparison on the same source revision before publication.
Published workflow artifacts are the authoritative results; a configured job or
a test listed in this document is not itself pass evidence. Comparative speed is
not a correctness or release-support promise.

The release gate includes both workspaces, default and all-feature builds,
Clippy, documentation, MSRV, public API compatibility and crate packaging. CI
also executes the durable publication reference and platform-specific suites.
Soak failure reports include only test identifiers, numeric summaries, revision,
exit code and last suite; raw prompts, panic payloads and connection URLs are not
copied into that evidence.

Durable event cursors, background execution independent of subscription,
automatic external fencing, a general approval service, automatic checkpoint
migration and arbitrary multi-Agent scheduling remain future work. Their absence
does not weaken the narrower contracts above; applications must not assume them.
