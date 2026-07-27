# RFC 0022: Router Retry and Backoff

## Status

Implemented.

## Problem

Some transient failures recover on the same endpoint and should not
immediately consume a fallback provider. Retrying without strict limits,
backoff, cancellation, or stream awareness can amplify an outage, exceed a
deadline, duplicate visible output, and silently add provider charges.

Retry must compose with the existing Model Router and Circuit Breaker rather
than creating another invocation path.

## Opt-in authority

Retry is disabled by default. `ModelRetryPolicy` defines:

- total `max_attempts`, including the initial call;
- initial and maximum backoff;
- an integer exponential multiplier;
- no jitter or deterministic full jitter;
- explicit error kinds allowed when retry safety is `Unknown`.

Errors marked `Safe` are eligible. Cancellation and errors marked
`RequiresIdempotency`, `UnsafeAfterVisibleOutput`, or
`UnsafeAfterSideEffect` are never retried. Unknown safety requires
`allow_unknown(kind)`, which is explicit authority to risk another provider
charge.

## Commit point

Same-route retry is possible only for:

- failure while opening the stream;
- an error as the first stream item;
- a stream ending before its first event.

The first successful canonical event commits the route and attempt. Any later
failure is marked `UnsafeAfterVisibleOutput`; neither retry nor fallback is
allowed.

Every retry receives a new physical `InvocationId` while inheriting the
logical run, deadline, and cancellation scope.

## Backoff and jitter

For retry number `n`, exponential delay begins at `initial_backoff`, grows by
the configured integer multiplier, and saturates at `max_backoff` without
overflow.

`RetryJitter::Full` chooses a value from zero through that cap using stable
entropy derived from logical invocation identity, route name, and attempt.
This spreads independent calls without global random state and makes policy
tests reproducible.

## Provider Retry-After

The canonical metadata key is `retry.after_ms`. Effective delay is:

```text
max(local exponential delay, provider retry-after)
```

The OpenAI-compatible adapter parses the standard `Retry-After` header as
either delta seconds or an HTTP date and writes the canonical millisecond
value. It also preserves the configured provider identity for OpenAI, Ark,
Qwen, and custom endpoints.

## Deadline and cancellation

Before waiting, the router compares effective delay with
`ModelCallContext::remaining`. If the delay would consume or cross the
remaining deadline, it returns `DeadlineExceeded` without sleeping or issuing
another request.

Cancellation races the asynchronous `RouterSleeper`. Production uses
`SystemRouterSleeper`, backed by a runtime-neutral timer. Tests can inject a
recording or manually controlled sleeper and never depend on wall-clock sleep.

## Circuit Breaker interaction

Each physical attempt acquires its own circuit permit and records its result.
A retry may therefore reach the route failure threshold. If the circuit opens,
the next retry attempt skips that route and normal fallback selection
continues.

Only terminal `ResponseCompleted` resets route health. Failed retry attempts
remain visible in the selected-route provider event with their route attempt
number, error kind, and retry safety.

## Budget semantics

The hard attempt bound is the retry policy's local budget. Provider errors may
not report token or cost usage, so Runifold does not invent accounting values.
Applications can inspect attempt summaries and provider billing telemetry.
Future provider adapters may attach reliable failed-attempt cost metadata to
the canonical usage model.
