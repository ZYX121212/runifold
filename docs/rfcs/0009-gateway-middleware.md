# RFC 0009: Gateway around-middleware

- Status: Accepted for initial implementation
- Scope: `runifold-agent`

## Summary

`AgentGateway` supports ordered, object-safe around-middleware. Middleware can
implement governance concerns without changing the Agent loop or provider
adapters:

- audit and telemetry;
- authorization and human approval;
- rate and concurrency limits;
- input projection and data-loss prevention;
- retries, fallbacks, and circuit breakers;
- result inspection.

The extension boundary is deliberately "around" rather than a pair of
before/after hooks. A middleware receives a `DelegationRequest` and a
`GatewayNext`, and decides whether, when, and how often to continue.

## Immutable authority

A `DelegationRequest` owns:

- the resolved `AgentDescriptor`;
- model-visible child input;
- a clone of the parent `RunContext`.

Applications cannot directly construct this type. Public mutation is limited
to `with_input`. Middleware may inspect route identity, metadata, parent
authority, deadline, and budget, but cannot substitute a route or replace the
captured parent context.

This prevents an input-transform or retry layer from becoming an accidental
authority-escalation API.

## Chain semantics

Middleware executes in registration order before `next` and reverse order
after it:

```text
outer.before
  inner.before
    protected terminal delegation
  inner.after
outer.after
```

A middleware may:

- call `next` once for normal around behavior;
- not call `next` to short-circuit;
- call `next` more than once for an explicit retry or fallback policy.

The continuation is copyable, while the request is cloneable. Retry behavior
therefore remains visible in middleware code rather than hidden in the
gateway.

## Protected terminal boundary

Middleware never replaces the terminal executor. Every call to `next`
eventually rechecks:

1. cancellation and deadline;
2. Agent capability possession;
3. child-authority attenuation;
4. delegation depth;
5. shared delegation budget;
6. child Run creation.

Calling `next` twice consumes two delegation units and creates two child Runs.
If the second attempt exceeds budget or observes cancellation, it is rejected
before child model execution.

## Policy adapter

`GatewayPolicy` is an object-safe asynchronous decision interface.
`PolicyMiddleware` adapts it into the around chain. Policies return:

- `GatewayDecision::Allow`;
- `GatewayDecision::Deny { reason }`.

Denial becomes `GatewayErrorKind::PolicyDenied` and performs no downstream
work or delegation accounting. A policy may also return a structured
`GatewayError` when its own dependency fails.

## Initial invariants

1. Route resolution occurs before middleware.
2. Middleware cannot replace route identity or parent authority.
3. Short-circuit denial performs no child work.
4. Every terminal attempt independently enforces lifecycle and authority.
5. Every terminal attempt independently consumes delegation budget.
6. Around ordering is deterministic.
7. Middleware is provider-neutral and object-safe.
8. Policy denial is a hard gateway failure, not a model-visible child error.

## Deferred decisions

- richer audit middleware with configurable payload capture;
- approval request/resume protocol;
- keyed rate and concurrency limiters;
- standard exponential-backoff retry policy;
- circuit-breaker state and distributed coordination;
- OpenTelemetry span conventions;
- middleware configuration and dynamic reloading.
