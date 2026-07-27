# RFC 0003: Model invocation boundary

- Status: Accepted for initial implementation
- Scope: `runifold-model`, `runifold-testkit`

## Summary

Provider adapters implement one object-safe `Model` trait. Canonical streaming
events are the source of truth; a non-streaming invocation is produced by
strictly accumulating the same events. This prevents the streaming and
non-streaming paths from acquiring different normalization, error, or metadata
semantics.

## Boundary

`Model` exposes:

1. asynchronous capability discovery for a provider-qualified model;
2. asynchronous opening of a canonical event stream;
3. a default `invoke` collector over that stream.

The trait returns boxed futures and streams deliberately. Provider clients are
runtime edges and need dynamic composition, middleware, routing, and test
doubles more than they need zero-allocation monomorphization.

`ModelCallContext` carries only execution concerns:

- invocation identity;
- optional owning run identity;
- effective deadline;
- hierarchical cancellation.

Credentials, HTTP clients, endpoints, provider options, and retry policies do
not belong in this context. They are adapter or middleware configuration.

## Invariants

1. A successful invocation contains exactly one response-start event and one
   response-completed event.
2. A stream ending without a terminal event is a protocol error.
3. The default collector preserves provider events, warnings, usage, output
   order, and terminal metadata.
4. Cancellation races both stream establishment and every subsequent event.
5. A call-scoped cancellation token is a descendant of its owning run token.
6. Cancelling a call never cancels its owning run or sibling calls.
7. Adapters observe the cancellation token and translate deadlines into their
   transport timeout mechanism.

## Deterministic testing

`ScriptedModel` is a queue-backed implementation of the same public trait.
Each invocation consumes one script, so retries, fallbacks, and multi-step
agent loops can be tested without special branches in production code.

## Deferred decisions

- transport-independent deadline wakeups;
- capability-negotiation middleware;
- retry and fallback middleware;
- provider connection pooling;
- request hedging;
- resumable provider streams.
