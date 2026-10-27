# RFC 0077: Budgeted, observable Agent context

Status: implemented, opt-in.

## Problem and scope

A conversation store is an append-only record, not a model input window. Counting
messages cannot bound a long report, Tool schema, rich result, or retrieval batch.
Reusing RunContext does not preserve conversation history. RunContext remains an
execution/authority/accounting scope. Context selection belongs to runifold-agent,
inside its existing canonical execution path; no new runtime or store is added.

This RFC adds a deterministic, budgeted projection before **every** model turn,
including Tool continuations, provider pause turns, and completion repairs. It is
shared by prompt, run, stream, checkpoint and AgentSession. Existing applications
retain their behavior until they opt in through AgentBuilder::context_manager.

## Contracts

ContextBudget validates `window > output reserve + safety margin`. The available
input budget is their difference. All request controls, callable schemas,
provider tools/options and structured output schema must be accounted for by the
counter. An unset output maximum becomes the explicit reserve; a larger configured
maximum is rejected. The input budget is separate from cumulative RunContext usage:
estimation does not debit actual tokens or alter a parent's grants.

ContextTokenCounter has a stable descriptor (id/version), counts request overhead
and individual messages additively, and declares whether its accounting is an
estimate. Implementations are synchronous, deterministic, local and bounded. A
provider-specific implementation must include wire framing and multimodal costs.
SerializedBytesCounter is an explicitly approximate text-only bootstrap: canonical
JSON bytes plus a fixed framing allowance, not a universal tokenizer or guaranteed
provider upper bound. It rejects media, artifact references and unknown opaque
content, including nested Tool results. No network fetch or hidden tokenization
call is allowed. Applications choose window/reserve/margin for their actual model;
routing to a model with a smaller window requires a correspondingly smaller budget.

ContextSelectionStrategy returns an eviction order over complete, optional groups.
The runtime rejects duplicate, out-of-range or mandatory group indices. Empty order
is a strict fail-on-overflow policy. RecentContext evicts oldest historical turns
first, then retrieval groups in reverse order. Application strategies can change
this ordering without modifying content or bypassing mandatory protection. A
selected suffix need not include every recent turn if a custom policy says so;
all omissions are observable. Selection never invokes a summarizer implicitly.
A bounded store window may begin in the middle of an older turn. Such a leading
prefix is classified as IncompleteHistory and excluded before budget eviction
when no summary Agent has been configured;
its original messages remain in the archive and its indices are reported. This
prevents orphaned historical Tool results from blocking a new request. The
excluded prefix does not correlate calls into a later independent turn. Custom
eviction strategies may list an already-excluded group; it is never charged or
subtracted twice. No strategy can re-include an incomplete prefix.
When an explicit summary Agent is configured, managed sessions and automatic
conversation runs first advance the rolling summary through that prefix in
bounded batches. This preserves its Tool evidence in the summary instead of
dropping it from the next analysis. Extra batches consume the existing shared
budget and summary pass limit; exhaustion stops before the main model call.
No storage schema or unmanaged conversation behavior changes.

Mandatory groups are system instructions, committed summaries, and all current
request messages, including Tool calls/results, provider continuation data and
repair feedback. Optional transient reference documents are isolated from the
current turn. Prior turns are grouped from each genuine user message to the next;
repair feedback is not a user-turn boundary. Interleaved reference or system
messages do not split a historical turn; its indices can be non-contiguous.
Unsafe call/result crossings are
rejected if a projection would split them. Signed/opaque history stays atomic with
its owning turn and is never rewritten. Tool correlation uses original call IDs;
selection may never leave a result without its call or a call without its result
in the outgoing request. Mandatory current-turn exchanges still fail explicitly
if incomplete; only a window-truncated historical prefix is excluded.

No message is sliced, flattened, summarized or promoted to a higher trust role.
Retrieval results remain untrusted. A committed summary is mandatory and retains
its through-sequence provenance. Retrieval is grouped per document so low-priority
results can be excluded without losing a whole batch. Custom analysis state can be
provided as reference context; its business schema belongs to the application.

## Projection, reports and recovery

The canonical transcript stays complete. Only the outgoing ModelRequest is
projected. ContextPlan reports budget/reservations, counter/strategy identities,
estimated input usage, and each group's original message indices, category,
accounting, reference IDs, summary coverage sequence and inclusion decision. It
contains no message bodies or tool arguments. Applications must use non-sensitive
opaque source IDs because reports can enter operational journals.
Reports are emitted as ContextPrepared stream events and context.prepared domain
events, and available from AgentOutcome::context_plans(). A context.rejected domain
event records safe typed failure classification before dispatch.

A runtime-owned marker binds the original current input to its execution. Plans
are stored on that marker in existing checkpoint transcript metadata, with a digest
of the complete unprojected request and a separate checksum of the selected plan.
Recovery reconstructs group boundaries and checks source IDs, summary coverage,
protection and accounting before reusing a plan. The checksum detects altered
reports; it is not authentication of an untrusted checkpoint store. Older plans
without this checksum are accepted only after local accounting revalidation and
then upgraded, without running the selection strategy again. Legacy plans whose
groups already split a historical turn or include an unanchored leading prefix
fail explicitly rather than being
reinterpreted. Runtime metadata is stripped before accounting
and model transport, and stripped from conversation appends. This avoids adding
required fields to public outcome/checkpoint structs or changing store schemas.
The recovery contract binds budget, counter and strategy descriptors. Disabled
management has a serde default so compatible legacy contracts remain compatible.

A plan is persisted at ReadyForTurn before the existing TurnInFlight boundary.
Explicit recovery of an interrupted model request reuses the stored selection and
validates its request digest; retrieval and selection are not rerun. A definition
or request mismatch fails before transport. Counting and request hashing stream
canonical serialization into bounded writers, preserving existing JSON byte
accounting and digest compatibility. Non-message overhead borrows request
controls without copying history. Selection moves outgoing messages after the
one canonical request copy, without cloning selected private payloads again.
Runtime report metadata is filtered before that copy, so accumulated reports are
not copied just to be discarded. A configured zero output maximum is rejected
before dispatch.
Existing ambiguous model/effect retry
rules remain unchanged. If a pre-dispatch context error follows earlier external
effects, retrying the entire Agent is not automatically safe. Completed session
requests replay their original outcome and reports without another model request.

## Continuous analysis recipe

Use AgentSession for durable multi-turn admission and rolling history summaries.
Use a bounded ConversationWindow as the storage-loading bound and ContextManager
as the model-input bound. Summary generation has its own Agent and can be given
its own ContextManager; it shares the caller's actual usage budget and deadline.
Current-turn mandatory data that does not fit produces an explicit error: retrieve
smaller evidence, reduce Tool results, increase the model window, or explicitly
start a new request with an approved summary. No hidden mid-turn compaction occurs.

The continuous_analysis example uses typed facts with source IDs, hypotheses with
supporting fact IDs, unresolved questions and next actions. Each iteration supplies
the previous validated state and new evidence, validates references and monotonic revision in the new
state, and retains raw evidence independently. Applications persist accepted state
and raw evidence separately when they need process-restart recovery of that state. Model conclusions remain hypotheses
until application validation promotes them. This demonstrates composition, not a
framework-owned research ontology or an automatic truth checker.

## Validation and limits

Tests cover schema overhead, output reservation, mandatory overflow before model
opening, whole-turn eviction, Tool correlation, nested-media rejection, invalid
strategies, stream/report parity, immutable history, request replay and recovery
with a selection strategy that must not be invoked again. Public facade imports,
serde recovery compatibility and an executable offline analysis example guard DX.

The counter contract is trusted application code, not a security sandbox. Exact
provider tokenization and automatic model-window discovery are adapter work; the
bootstrap estimator makes neither claim. Selection reports cover the bounded view
loaded by ConversationStore, not archive entries excluded by its window/summary
policy. Summary content carries coverage sequence; summary lifecycle events retain
commit boundaries. Omitted archived history remains available through the store.
Future semantic ranking can implement the strategy contract. Distributed memory,
automatic fact promotion, automatic mid-turn summarization and provider-native
compaction are outside this change.
