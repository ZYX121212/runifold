# RFC 0019: Typed Structured Output

## Status

Implemented.

## Problem

A provider accepting a JSON Schema does not prove that the returned bytes
match the application's Rust type. Provider support varies by model, strict
modes differ across compatible endpoints, and responses may contain refusals
or malformed JSON.

An Agent runtime must therefore keep two boundaries distinct:

1. request-time output constraints sent to the provider;
2. local validation before application code receives a typed value.

## Decision

`OutputFormat::typed::<T>(name)` derives JSON Schema from `T` and requests the
provider's strict mode. `ModelRequest::structured_output` exposes the same
operation at the model boundary. `Agent::structured_output` and
`AgentBuilder::structured_output` propagate that format to every model turn in
the canonical Agent loop.

The type used to produce a schema only needs `JsonSchema`. Decoding is a
separate operation requiring `DeserializeOwned`; this avoids claiming that a
request configuration itself guarantees a valid response.

`ModelResponse::structured::<T>`:

1. concatenates canonical text parts in order;
2. excludes reasoning, citations, and opaque provider data;
3. rejects an explicit refusal even if text is also present;
4. rejects missing or whitespace-only text;
5. deserializes locally with `serde_json`.

`AgentOutcome::into_structured::<T>` returns `StructuredAgentOutcome<T>`.
The wrapper retains the original canonical outcome alongside the decoded
value, so typing does not discard transcript, usage, warnings, provider
metadata, or execution counters.

For the compile-time-safe path, `AgentBuilder::build_structured::<T>` returns
`StructuredAgent<T>`. Its `run` method always decodes the same `T` used to
derive the provider schema, eliminating schema/decoder type drift.

## Error and privacy semantics

`StructuredOutputError` has stable categories for missing text, refusal, and
invalid output. JSON line and column are retained for diagnostics. The error
does not copy the response body or the deserializer's value-bearing message,
preventing model output from entering logs through ordinary error formatting.

Local decode failures are terminal application-boundary failures. Runifold
does not silently repair JSON or issue another billable model call.

## Capability semantics

Structured output remains subject to the existing feature policy. Unknown
model support fails under `Strict`, may proceed with a visible warning under
`BestEffort`, and can be declared explicitly through model capabilities.
Generating a schema never upgrades an unknown provider capability.

## Consequences

- The same Rust type drives the provider contract and local decoding.
- Provider-compatible OpenAI, Ark, Qwen, and custom endpoints reuse their
  existing protocol encoders.
- Applications cannot accidentally treat provider-side enforcement as a
  trusted validation boundary.
- Retry or repair policies remain explicit future orchestration features
  rather than hidden behavior in the decoding primitive.
