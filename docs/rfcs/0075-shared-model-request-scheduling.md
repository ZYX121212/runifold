# RFC 0075: Shared Model Request Scheduling

- Status: Implemented for process-local physical-request admission
- Scope: `runifold-model`, public facade, offline conformance tests

## Problem

Agent concurrency and Workflow worker concurrency do not bound physical model
requests. A Workflow may fan out, run Tools without a model request, or wait for
a signal. Independent runtimes may also share one provider account. Applications
need explicit, shared admission at the physical model boundary.

The objective is useful throughput within configured constraints, with bounded
memory, cancellation, deadlines, visible accounting, and existing retry safety.
The scheduler must not create another Agent loop or choose business work.

## Placement and ownership

Internal organization in the existing `runifold-model` crate:

```text
src/scheduling.rs          public contracts and configuration
src/scheduling/state.rs    atomic queue and capacity accounting
src/scheduling/model.rs    canonical Model stream wrapper
src/scheduling/tests.rs    deterministic concurrency and time tests
```

No new crate, provider dependency, Tokio dependency, or background service is
required. The facade re-exports the contracts and offers explicit scheduled
ProviderRuntime construction through `ProviderModelExt::scheduled`. Existing unscheduled APIs remain compatible.

```text
Agent / Workflow
  -> ProviderRuntime / ModelRouter
    -> retry and fallback select one physical attempt
      -> scheduled Model
        -> physical provider Model::stream
```

Wrapping an entire retrying router would hold capacity during backoff and count
logical calls instead of physical attempts. The preferred integration wraps
each physical provider route beneath retry. A standalone wrapper remains useful
for explicit Model injection, with placement documented.

## Shared identity and multiple constraints

A scheduler is explicitly shared application state. Named capacity pools can
represent an account, deployment, or model. A physical route binds to one or
more pools. All applicable constraints are acquired atomically; a request must
never hold one pool's concurrency while waiting for another pool.

Names are configuration identity, not credentials or authentication. Distinct
runtime instances share capacity only when they receive the same scheduler.
Two independent schedulers do not coordinate, even when pool names match.
Bind all pools once per physical path. Nested scheduler wrappers are rejected
before waiting, using private invocation admission state, so a request cannot
deadlock waiting for capacity it already holds or acquire different schedulers
in opposite orders.

Each pool has a positive concurrency ceiling and optional request/token sliding
window limits. The waiting queue has a finite bound. Configuration rejects zero
limits through nonzero types; duplicate/unknown pools, empty bindings, invalid
windows, impossible token reservations, and exhausted admission identities are
rejected before provider dispatch.

## Queue and dispatch

Requests are registered only when polled. The scheduler owns bounded request
metadata and wakeups, never another copy of prompt or private provider payloads.
Admission checks all pools in one critical section and reserves every resource
before releasing the lock. No lock is held across an await, provider call, or
user callback. Wakeups, raw wake-object cloning and final destruction all run
outside the accounting lock. Shared Arc handles keep queue bookkeeping free of
user-defined wake-object callbacks. Expiry histories are ordered by timestamp
even when outside-lock clock reads commit in a different thread order.

Dispatch is work conserving among eligible requests. A request blocked by one
pool must not block unrelated pools. Ordering and starvation protection must be
explicit and tested; strict FIFO, priorities, and short-job preference are not
interchangeable. The implemented policy prefers stable arrival order among
eligible requests and bounds bypass of blocked requests sharing capacity with
a configurable aging barrier (30 seconds by default). Aging may intentionally
leave capacity idle to let an old large request accumulate enough tokens.

Cancellation removes queued admission without consuming provider quota.
Deadlines apply during queueing, opening, and consuming the stream. Dropping an
admission future removes its waiter; dropping a stream releases its concurrency.
Queue saturation fails immediately with a safe structured admission error and
a corrective action. `ModelErrorKind::AdmissionDenied` is safe, but routing and
retry do not automatically repeat or route around it. Inspect
`runifold.scheduler.reason` or diagnostic `RF-PROVIDER-010` for corrective action.

## Permit lifecycle and retry safety

One permit corresponds to one physical attempt. Its lifecycle includes opening
the transport and consuming the canonical response stream. It is released on
terminal response, first error, premature EOF, cancellation, deadline, or drop.
Release occurs before returning the terminal item to the consumer, so a caller
holding a completed stream cannot pin capacity indefinitely.

An admission error before dispatch is safe. After dispatch, cancellation and
deadline errors must retain conservative safety; after visible output they are
unsafe to replay. Lifecycle is rechecked after opening and after each physical
stream poll, including Pending and terminal results. Usage observed during a
losing cancellation/deadline race is still charged before output is discarded.
The wrapper forwards all canonical content, provider events,
warnings, and rich Tool results. It neither retries nor converts partial output
into an apparently successful response.

Opening and streaming cancellation must work even for a custom inner Model
that ignores its ModelCallContext. Drop must signal the attempt's child token
and drop the inner future/stream before publishing released capacity. This does
not prove that a remote server stopped processing: the limit bounds local
in-flight attempts, not confirmed remote execution after a disconnect. Like
other canonical streams, callers must poll or drop the returned stream to
observe cancellation/deadlines. A retained unpolled stream holds its slot; there
is no background execution service.

The existing router remains responsible for retry and fallback authorization.
Every permitted retry goes through fresh admission. Backoff holds no concurrency
permit; prior attempts still consume appropriate request/token rate allowance.

## Request and token windows

Request allowance is charged at atomic admission immediately before invoking
the physical Model, never refunded simply
because the transport failed. The configured window is a rolling local policy,
not a claim to reproduce every provider's billing or burst algorithm.

Token-limited admission requires an explicit estimator/reservation policy; a
generic byte-count heuristic is insufficient for multimodal content, provider
Tools, reasoning, and different tokenizers. Estimate the billable input plus
bounded output for the actual request. Estimation runs outside the lock.

Active token reservations remain charged even when a long stream crosses a
window boundary. The default `ModelTokenSettlement::Conservative` retains at
least the reservation even after terminal completion: a cumulative UsageUpdated
event may contain only input or intermediate usage. A terminal event alone does
not certify the completeness of an earlier usage snapshot.

Applications may explicitly select `ModelTokenSettlement::ReportedUsage` only
for adapters verified to report complete final billable input/output usage. On
terminal completion with reported usage this policy replaces the reservation
with the largest cumulative usage, never summing cumulative snapshots. Missing
usage, errors, cancellation and drop always retain at least the reservation.
Settlements remain charged for a full window from release. The `unknown_usage`
counter includes terminal calls whose reported usage was not explicitly trusted.
Never infer zero cost from absent or incomplete usage or an ambiguous failure.

Cumulative usage is tracked without summing snapshots or truncating a total
above `u64::MAX`; observed debt is charged while the stream is still active.
Actual usage above reservation creates observable debt and blocks new work as
needed; it must not be clamped away. A hard upstream token guarantee requires
an upper-bound reservation and matching provider metering. Unknown actual usage
can exceed estimates; the API and examples must state this limitation.

Rate windows expire through a runtime-neutral clock/timer boundary. Waiters
must wake when the earliest relevant charge expires, without a polling loop or
new request being required. Large requests that can never fit fail immediately.

## Observability

Expose payload-free snapshots of queue depth, active attempts, admission count,
rejections, cancellation/deadline removals, cumulative queue wait, pool request
charges, active token reservations, settled tokens, unknown usage, and token
overruns. Accounting snapshots must reflect one consistent state. Do not log
prompts, API keys, or provider payloads through scheduling diagnostics.

Errors returned by the scheduled opening/stream path include
`runifold.request.phase` (`admission`, `opening`, `waiting_first_event`, `streaming`),
`provider_invoked`, `local_capacity_released`, `output_observed`,
`remote_outcome`, `queue_wait_ms` and `execution_ms`, each under the same
`runifold.request.*` namespace. Local transport and permits are dropped before
the error is returned. Queue wait and execution use the scheduler's monotonic
clock and are reported separately as saturating integer milliseconds.

`waiting_first_event` means no canonical event has yet reached the consumer;
`streaming` means at least one canonical event was delivered, including a
heartbeat or response-start event rather than necessarily user-visible text.
`provider_invoked` means the inner Model was called, not that a remote server
received the request. Before that call, `remote_outcome` is `not_started`;
afterward failures conservatively report `unknown`. No adapter invocation,
local cancellation, slot release or absent usage proves remote termination or
zero cost. A collector/outer wrapper may win its own cancellation race and
return a generic cancellation instead; absent lifecycle metadata remains
unknown, not inferred. Dropped streams have accounting snapshots but cannot
return an error to an absent consumer.

## Verification contract

Offline tests must cover:

1. Many Agents/runtimes sharing a pool cannot exceed its ceiling.
2. A short attempt releases a slot before unrelated long attempts finish.
3. Multi-pool admission is atomic; disjoint pools can proceed independently.
4. Queue overflow, impossible reservations, and invalid bindings do no provider work.
5. Cancelled/dropped waiters do not leak queue entries or reservations.
6. Cancellation/deadline during opening and streaming stop uncooperative Models.
7. Terminal events, errors, EOF, and stream drop release exactly once.
8. Holding an already-terminal stream does not retain a concurrency permit.
9. Request windows and token settlement wake at deterministic boundaries.
10. Long streams retain active reservations across rate windows.
11. Cumulative, partial terminal, missing and underestimated usage, explicit
    settlement trust, and arithmetic bounds.
12. Retry/fallback acquires per attempt and releases during backoff; visible
    output still prevents unsafe replay.
13. Public facade construction and both invoke/stream follow this same boundary.
14. Runtime-neutral and WASM compilation remain supported.
15. Reentrant wake callbacks and final wake-object destructors never run under
    the scheduler lock; out-of-order clock reads cannot delay expired charges.
16. A provider that cancels or crosses the deadline inside opening/polling cannot
    return a successful result afterward or erase observed Token debt.

## Deliberate limits

This implementation adds process-local physical-request admission. It does not claim
distributed quota enforcement, arbitrary Workflow DAG scheduling, capacity-
optimized model selection, or a self-tuning concurrency window. Those features
need separate contracts and workload evidence. Provider feedback must distinguish
RPM/TPM exhaustion from concurrent-request overload before adaptive reduction is
safe. No automatic Agent work reduction or output-quality tradeoff is authorized.

## External references and policy ownership

[OpenAI rate limits](https://developers.openai.com/api/docs/guides/rate-limits)
documents separate request/token limits, organization/project scope, shared model
limits, and failed requests consuming per-minute allowance. These motivate
explicit shared pools; they do not specify this scheduler's algorithm.

Concurrency ceilings, rolling local windows, the aging barrier, conservative
settlement, and process-local coordination are Runifold policies. They do not
reproduce OpenAI's server-side metering or establish account-specific limits.
Configure pools from the actual provider/account limits and use an appropriate
reservation policy; final billable usage alone need not match upstream rate-limit
accounting.

## Public usage and regression entry points

Use `provider.scheduled(&scheduler, pools)?.runtime(model)` for the preferred
provider-backed path. For custom injection use `scheduler.bind(model, pools)?`
with the existing Agent builder. Configure `ModelCapacityLimits::requests` and
`::tokens` with positive `ModelRateLimit` windows; a minute is 60 seconds.
For token-limited routes attach a trusted `ModelTokenEstimator` through
`ScheduledModel::token_estimator`. It sees the final physical request after
runtime defaults and routing. Canonical input/output usage provides the local
settlement basis; provider-specific billing units need a matching reservation
policy and may require a future settlement-policy adapter.

The complete offline example is
[`shared_model_capacity.rs`](../../crates/runifold/examples/shared_model_capacity.rs).
Its 100 Agents use the normal ProviderRuntime path with a shared concurrency of
four. Public Agent/invoke/stream contracts are verified in
[`model_scheduling.rs`](../../crates/runifold/tests/model_scheduling.rs), with
local queue, rate, fairness, lifecycle, debt, backoff and fallback tests in
[`scheduling/tests.rs`](../../crates/runifold-model/src/scheduling/tests.rs).
[`wasm_model_scheduling.rs`](../../crates/runifold/tests/wasm_model_scheduling.rs)
executes the production timer and terminal release under WASM. Existing
provider retry/circuit configuration and rich content remain on the canonical
path. Pool identities and capacity grants do not grant Run capabilities or
increase execution budgets.
