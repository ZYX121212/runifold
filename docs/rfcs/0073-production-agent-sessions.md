# RFC 0073: Production Agent sessions and durable streaming

Status: implemented for 0.10; see [release contracts and migration](../RELEASE-0.10.md).

## Problem

A final transcript compare-and-swap detects competing writers only after model
and tool work. Separate streaming and durable APIs also force applications to
assemble their own commit acknowledgement, retry identity, and recovery boundary.
A matching Agent name and model is insufficient when tool contracts or prompts
change across deployments.

## Public entry point

`AgentSession::new(agent, durable_store, conversation_id, namespace, context_policy)`
binds the existing abstractions without introducing another execution engine.
`run(request_id, input, run_context)` and `stream(...)` acquire admission before
retrieval, model calls, or tools. Keep both conversation and request IDs in
application storage. Retrying an already committed ID returns its saved result;
a different input or conversation fails. All writers must use the session API:
low-level `run_durable_conversation` and direct store mutations do not participate
in admission. Session construction does not supply capabilities or a budget.

The conversation UUID is reserved in checkpoint storage as the session admission
record. Request checkpoint IDs must be distinct. CAS makes admission portable
across the existing SQLite and PostgreSQL stores without schema migrations.
Namespace mismatches and unknown record versions fail closed. Losing a creation
race may report a checkpoint conflict rather than `Busy`; callers may retry the
same request to inspect the current owner.

## State and failure semantics

Admission is idle or bound to a request ID and SHA-256 input digest. A successful
terminal checkpoint and transcript commit is followed by a CAS admission release.
Two records are involved: release is not part of the transcript transaction.
If the process exits between commit and release, a duplicate can read the committed
result, but subsequent requests remain blocked until explicit recovery releases
admission. Storage failure after commit may therefore return an error despite a
committed result. Retry the same request ID to determine the outcome.

Failures and dropped futures retain admission. `Busy` includes the active request
ID and admission revision. `recover_after_owner_exit` claims that exact revision
and resumes the existing checkpoint, or starts the bound input if the process died
before creating an Agent checkpoint. The host must first terminate or otherwise
fence the previous execution. CAS alone cannot stop a prior worker or an already
sent HTTP request. There is no timer-based takeover or exactly-once remote-side-
effect claim. Restore checkpoint budget usage and retain the application's
capability grants. Ambiguous model turns still require explicit `ResumePolicy`;
effect retry policy continues to govern remote operations.

`stream_durable_conversation` and `stream_resume_durable_conversation` run the same
execution and recovery code as their non-streaming equivalents. They suppress
`AgentStreamEvent::Completed` and emit `ConversationCommitted` only after the
atomic terminal transaction. Session streams emit that event after admission
release as well. Model completion events and the existing execution Journal
lifecycle are not transaction acknowledgements. Commit errors retain their
conversation error type and recoverable generated outcome.

Streams remain poll-driven. Dropping a stream stops local polling and does not
create a background worker. Events are not durable cursor-addressable subscriptions.
Replayed completed requests emit a committed result rather than reconstructing
historical token deltas.

## Recovery compatibility

Checkpoints now include `AgentRecoveryContract`: instructions, static context,
retriever capabilities and limits, full local tool descriptors (including identity,
version, schema, effect and risk), child route descriptors, generation options,
provider tools/options, output mode/schema, local execution bounds, tool concurrency,
completion policy, and effect recovery policy. Existing reviewer checks remain.
Missing or changed contracts reject recovery before further external work.

Legacy checkpoints remain deserializable for inspection but cannot be automatically
resumed. Drain active work before upgrading; keep the previous worker version
available for old checkpoints. Do not remove the check or silently inject a current
contract into old data. Descriptor identity must survive process restarts.

This is a declarative contract comparison, not a hash of executable code. Child
implementations, middleware, provider-side model changes, and custom decoders are
not proven identical. Hosts must update descriptor versions when behavior changes
and route outstanding work to compatible deployments. Stored options and prompts
are sensitive application data and inherit checkpoint-store access controls.

## Checkpointed summary preparation

`AgentSession::with_summary_agent(agent, max_passes)` enables automatic compaction
using the existing summary prompt and bounded output validation. It deliberately
accepts an Agent rather than an arbitrary summarizer callback: each batch runs
through the canonical checkpointed Agent engine, including its capability, model,
review, effect, cancellation, and budget policies. The main Agent starts only after
the required summary batches have committed. Immutable history is not rewritten.

Admission schema 3 binds the main Agent and Reviewer definitions before any
Agent checkpoint, and persists the context policy, summary Agent identity and contract,
reviewer bindings, maximum pass count, completed passes, pending batch prompt and
checkpoint ID, and cumulative known usage. Existing schema-1/2 admission records
from the earlier unreleased prototype are rejected; drain them with that build
before switching. There is no automatic migration of occupied requests.

A pending batch is recorded before its Agent checkpoint is created. Recovery can
then create that checkpoint if no summary work started, resume an interrupted
Agent only under the explicit `ResumePolicy`, or reuse an already completed summary.
After generation, the summary store checks the original transcript version. If
summary commit succeeded but session progress acknowledgement failed, recovery
recognizes the exact committed prefix, source version, and content and preserves
its summary ID. A conflicting transcript update is not silently rebased, retried,
or overwritten. The saved output remains inspectable for an operator decision.

`ConversationSummaryStarted` exposes the summary checkpoint ID and prefix boundary;
`ConversationSummaryCommitted` follows persistence of both the summary and session
progress. These are preparation events, not the final turn acknowledgement. They
may recur during recovery and do not form a persistent event subscription. Summary
model token deltas are not forwarded as main-Agent output.

Summary and main work use one caller-supplied budget tracker. Between summary
batches, usage is persisted with session progress. If summary execution returns an
error, the session also saves usage known at that point: the Agent's write-ahead
checkpoint may precede that expense. `recovery_usage(request_id)` takes componentwise
maxima of the session and current Agent snapshots, never their sum. Restore this
usage before recovery. A process killed during an external request can still leave
unknown remote usage; the persisted value is a lower bound, not an exact billing
reconciliation. Failed progress writes likewise cannot guarantee persistence of
newly observed usage.

Pass limits survive retries of the same logical request. Changing context policy,
summary identity/definition/reviewer configuration, or the limit during recovery
is rejected. Exhaustion and transcript conflicts keep admission occupied; this
version does not offer automatic rebasing, limit escalation, or an abandonment UI.
Without `with_summary_agent`, overflow continues to report `SummaryRequired`.

## Bounded tool concurrency

`Agent::tool_concurrency(NonZeroUsize)` and the builder equivalent opt into bounded
batches of contiguous read-only local tools. Default one preserves serial behavior.
Writes, unknown tools, pure tools, and child Agents are serial barriers in this
first implementation. Input order determines effect keys and transcript result
order, independent of completion order. Started siblings finish before a batch
error is returned; subsequent batches do not start. All use the same atomic budget,
cancellation, effect executor, and capability checks. Host declarations must be
accurate. This is not a scheduler for arbitrary multi-Agent dependencies.

## Evidence and remaining scope

SQLite integration tests verify commit acknowledgement, failed-commit streams,
replay without another model call, independent-connection admission, dropped-stream
recovery, changed/legacy contract rejection, and existing process-kill recovery.
Agent tests exercise concurrency bounds, ordering, write barriers, sibling draining,
and atomic shared budgets. The offline facade example runs without credentials.

Not delivered here: durable event cursors, subscription/execution separation,
automatic external fencing, end-user
approval inboxes, Postgres server-backed admission stress tests, or a same-application
Rig benchmark. Those require separate evidence before claiming production parity
or superiority. Existing low-level summary APIs remain available. See checkpointed summary
preparation above for the opt-in session path and its recovery limits.

## Historical development validation

The following is the pre-release implementation record. Current release
acceptance is tracked by the 0.10 release workflows and evidence artifacts.

On the current worktree:

- `cargo test -p runifold-agent -p runifold-store-sqlite`: 126 tests passed,
  including subprocess-kill recovery tests (95 Agent and 31 SQLite tests).
- `cargo clippy -p runifold-agent -p runifold-store-sqlite --all-targets -- -D warnings`: passed.
- `cargo check --workspace --all-targets`: passed with default feature selections.
- `cargo run -p runifold --example durable_session --features sqlite-bundled -- /tmp/runifold-summary-example.sqlite`: executed successfully and asserted one summary model call and one main model call across the original request and its replay.

No live model-provider requests, server-backed PostgreSQL tests, all-features or
WASM-target checks, or Rig comparative benchmarks were run for this change.
