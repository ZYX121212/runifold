# RFC 0021: Router Circuit Breaker

## Status

Implemented.

## Problem

Fallback alone still sends every logical invocation to an unhealthy primary.
That adds latency, consumes connection capacity, and amplifies provider
outages. A circuit breaker must suppress known-bad routes without creating a
second model-execution path or introducing nondeterministic tests.

Naive counters are incorrect under concurrency. A late failure from an old
request can reopen a route after a newer recovery probe succeeded, and
multiple requests can accidentally become half-open probes.

## Opt-in policy

Circuit breaking is disabled by default. `CircuitBreakerConfig` enables the
same policy independently for every physical `ModelRoute`.

Configuration includes:

- a non-zero consecutive failure threshold;
- a non-zero cooldown;
- explicit `ModelErrorKind` values that count as route-health failures.

The default counted kinds are transport, provider, protocol, and stream-state
failures. Cancellation and caller/configuration failures do not damage route
health unless an application deliberately replaces the counted-kind set.
Cancellation itself is always excluded.

## State machine

Each route has one shared state across cloned routers:

- `Closed { failures }`: requests may enter concurrently;
- `Open { until }`: requests skip the route;
- `HalfOpen`: one recovery probe owns the route.

When the threshold is reached, the route enters `Open`. After cooldown,
the first caller atomically transitions it to `HalfOpen`; concurrent callers
observe the route as unavailable and continue through normal fallback.

A terminal `ResponseCompleted` closes the circuit and clears the failure
count. A counted probe failure reopens it. A non-counted probe error or a
dropped probe also reopens it conservatively, preventing a permanently stuck
`HalfOpen` route.

Ordinary requests dropped by consumers do not count as provider failures.

## Generation safety

Every state transition that invalidates in-flight results advances a
generation number. A permit captures the generation it entered under.
Successes and failures mutate state only when their generation is still
current.

Therefore:

1. a failure that opens the route invalidates other older requests;
2. a successful half-open probe starts a new healthy generation;
3. late failures from the old generation are ignored;
4. stale successes cannot erase a newer outage.

This provides deterministic last-transition semantics without holding a lock
across network I/O.

## Streaming integration

The breaker wraps the same canonical `Model::stream` path as fallback:

- opening errors, first-item errors, and premature stream endings record
  failures when configured;
- errors after the streaming commit point are also recorded, while fallback
  remains forbidden;
- only terminal completion records success;
- the permit remains alive for the stream lifetime.

Half-open selection is included in the `runifold.router/route.selected`
provider event as `circuit_probe`. Skipped open routes appear in prior attempt
summaries as `circuit_open`.

## Clock and observability

`RouterClock` is a monotonic clock boundary. Production uses
`SystemRouterClock`; tests can inject a manually advanced clock without sleeps.

`ModelRouter::route_health` returns route name, target, state, consecutive
failures, and remaining cooldown. Snapshots are read-only and do not trigger a
state transition; an expired open route becomes `HalfOpen` only when a caller
acquires the probe.

## Persistence

Circuit state is currently process-local. Persisting health across restarts is
deliberately deferred: outage memory has different consistency and expiry
requirements from Agent checkpoints and write-ahead effects. Restarting begins
with closed circuits.

Applications must also retain the same `ModelRouter` or one of its clones
across requests. Clones share each route's synchronized health state. Building
a new router creates new routes and therefore intentionally starts with closed
circuits; a router is application state, not request-local state.
