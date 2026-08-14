# RFC 0053: Provider runtime contract

## Status

Implemented.

## Problem

A provider adapter must not reimplement Agent orchestration, retry, circuit
breaking, budget accounting, observability, or durable workflow behavior.
Duplicating those layers creates semantic drift and makes the newest provider
the least reliable provider.

## Decision

Provider integration has two responsibilities:

1. Implement `Model`, whose canonical stream normalizes content blocks,
   reasoning, usage, warnings, provider events, completion, and typed errors.
2. Implement `ProviderModel`, which exposes the stable provider namespace used
   in `ModelRef` and a reviewed `ProviderRuntimeProfile`.

The `ProviderModelExt` blanket implementation then supplies:

- provider-qualified Agent construction;
- a single-route resilient builder;
- a default `ProviderRuntime`.

`ProviderRuntime` composes bounded same-route retry and an independent circuit
breaker around the canonical stream. It remains a `Model`, so the following
layers stay provider-neutral:

- `OtelModel` instruments calls, routing, usage, and failures;
- `Agent` enforces capabilities, cancellation, budgets, tools, delegation, and
  canonical model loops;
- `AgentStep` executes the same Agent inside durable workflows;
- checkpoint, journal, and effect stores supply recovery and idempotency.

## Safe defaults

The default retry policy makes at most three attempts with full jitter,
starting at 100 ms and capped at 2 seconds. It retries only errors explicitly
marked retry-safe by the adapter. Unknown and unsafe failures are not retried.

The default circuit breaker opens after five consecutive transport, provider,
protocol, stream-state, or malformed Tool-argument failures and permits a
recovery probe after 30 seconds. Cancellation never contributes to circuit
health.

The generic profile also owns response delivery mode, feature policy, and
provider request defaults. A concrete adapter can therefore encode protocol-
safe behavior without leaking a provider-specific policy type into the facade.
`ProviderModelExt::runtime` consumes the adapter profile automatically;
`runtime_with_profile` is the explicit application override boundary. Retry
and circuit defaults can also be replaced through the returned
`ModelRouterBuilder` when building custom multi-route topologies.

## Invariants

- Streaming is the source of truth; non-streaming invocation uses the shared
  canonical accumulator.
- A stream cannot silently succeed without a terminal completion event.
- Streaming retry and fallback cannot begin after the first canonical event
  commits a stream. Complete delivery is fully accumulated and validated
  before that commit point, so pre-commit terminal failures remain visible to
  retry and circuit-breaker policy.
- Provider identity in requests, responses, errors, and raw events is stable.
- Reasoning is not mixed into visible answer text.
- Detailed usage does not double-count reasoning or cached tokens.
- Observability capture remains redacted by default.
- Budget limits and capabilities come from `RunContext`; a provider cannot
  amplify either.
- Durable workflow recovery reuses the same Agent and effect boundaries rather
  than provider-specific replay.

## Adapter acceptance

A new adapter is accepted only when tests demonstrate:

- request encoding and provider identity;
- fragmented streaming and terminal completion;
- reasoning and usage normalization when supported;
- tool-call fragmentation when supported;
- typed HTTP/provider errors and retry safety;
- timeout, cancellation, truncation, and credential redaction;
- concurrent invocation isolation;
- compatibility with `ProviderModelExt::runtime`.

Live provider tests should be separate from deterministic offline cassette
tests. A live test passing does not replace protocol and failure-path tests.

`runifold-provider-testkit::{verify_success, verify_error}` provides the shared
machine-readable acceptance boundary. Reports enumerate the exact checks that
passed instead of reducing support to one boolean.
