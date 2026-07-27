# RFC 0020: Stream-Safe Model Router

## Status

Implemented.

## Problem

Provider fallback looks like a simple loop until streaming, cancellation,
cost, and capability promises are considered. Retrying after output is visible
can duplicate tokens. Reusing an invocation identity across providers can
confuse idempotency and tracing. Automatically retrying an error with unknown
safety can silently double model cost.

The router must remain a `Model`; Agent, Tool, checkpoint, and streaming code
must not grow a second execution path.

## Logical and physical identity

`ModelRouter` owns one logical `ModelRef` and an ordered list of named physical
routes. It accepts requests only for that logical identity, then clones and
rewrites the request for the selected physical target.

Every physical attempt receives a new `InvocationId` through
`ModelCallContext::child_attempt`. Run identity, deadline, and hierarchical
cancellation are inherited.

The terminal `ModelResponse.model` remains the physical model that produced
the response. Logical identity and attempt history are not substituted for
provider truth.

## Fallback authority

`ModelFallbackPolicy::safe_only` is the default. It permits another route only
when `ModelError.retry_safety` is `Safe`.

`allow_unknown(kind)` explicitly authorizes fallback for a selected
`ModelErrorKind` whose safety is `Unknown`. This is authority to risk an
additional provider charge; it is not interpreted as proof of idempotency.

The policy never overrides:

- cancellation;
- `RequiresIdempotency`;
- `UnsafeAfterVisibleOutput`;
- `UnsafeAfterSideEffect`;
- future retry-safety variants unknown to this implementation.

## Streaming commit point

Fallback is allowed only before a candidate emits its first canonical event.
Opening failures, first-item errors, and empty streams may therefore select the
next route if policy allows.

The first successful event is the commit point. After it:

1. the router emits a `runifold.router/route.selected` provider event;
2. all subsequent events come from that physical route;
3. any later error is marked `UnsafeAfterVisibleOutput`;
4. no fallback is attempted.

This boundary is intentionally stricter than “first text token.”
`ResponseStarted`, warnings, and provider events are observable and affect the
canonical accumulator, so switching after any of them could create an invalid
or misleading stream.

Cancellation is checked before every physical attempt. The ordinary
`Model::invoke` cancellation race continues to protect a currently open or
blocked stream.

## Observability and privacy

Successful responses retain a canonical provider event containing:

- selected route name and index;
- physical target;
- distinct attempt identity;
- safe summaries of prior failures.

Failure summaries contain route, target, error kind, and retry safety. Error
messages and model content are not copied. When routing fails, the same
summaries are attached to `ModelError.metadata`.

## Capabilities

The logical model reports the conservative intersection of every physical
route:

- the weakest support level wins;
- known context length is the minimum;
- extensions absent from any route are removed;
- differing generic constraints degrade support to `Unknown`.

This prevents a logical route from advertising a feature or limit that a
fallback target cannot honor.

## Deferred policy layers

This RFC deliberately does not add hidden exponential retry, randomized load
balancing, health probes, or a circuit breaker. Those require clocks, state,
budgets, and observable policy decisions. They can be layered over this
deterministic safety boundary without changing Agent execution semantics.
