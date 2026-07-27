# RFC 0002: Lossless model IR and streaming

- Status: Accepted for initial implementation
- Scope: `runifold-model`

## Summary

Runifold uses a provider-neutral intermediate representation for model
requests, responses, multimodal content, capabilities, and stream events.
Provider adapters translate between wire protocols and this representation.

The representation is intentionally strict about lifecycle errors and
intentionally permissive about information it cannot yet normalize.

## Goals

- preserve text, media, reasoning, tool calls, tool results, refusals,
  citations, usage, finish reasons, and unknown provider data;
- make unsupported and emulated features visible;
- give streaming and non-streaming calls one canonical response type;
- make malformed or incomplete streams fail explicitly;
- allow provider adapters to evolve independently of the runtime kernel.

## Non-goals

- executing tools or implementing an agent loop;
- choosing retry, fallback, or model-routing policy;
- standardizing provider-specific options prematurely;
- exposing provider wire types in public kernel APIs;
- treating a stream as a sequence of text strings.

## Content model

Messages contain ordered content parts. Known parts use normalized variants.
Unknown provider data uses a namespaced opaque part and is never silently
dropped.

Binary content is represented by URL, base64 payload, or artifact reference.
Artifact storage is outside this RFC.

## Feature support

Feature support has four states:

- native;
- emulated;
- unsupported;
- unknown.

Requests select strict, allow-emulation, or best-effort behavior. Adapters must
report emulation, ignored fields, and other degradation as response warnings.

## Streaming state machine

A valid stream follows this lifecycle:

1. exactly one response-start event;
2. zero or more uniquely indexed content blocks or completed parts;
3. zero or more usage snapshots, heartbeats, and provider events;
4. exactly one response-completed event;
5. no events after completion.

Delta events must match an open block of the corresponding type. Completing a
response with open blocks is an error. Tool arguments are accumulated as raw
JSON text and parsed only when their block completes.

The accumulator orders final content by block index, retains provider events,
and produces the same `ModelResponse` type used by non-streaming adapters.

## Forward compatibility

Public enums are non-exhaustive. Provider-specific options and events are
namespaced. New normalized variants may be added without removing the opaque
escape hatch.

## Deferred decisions

- incremental JSON parsing for very large tool arguments;
- audio and image output delta formats;
- artifact-store ownership and streaming binary content;
- typed model invocation trait and asynchronous cancellation wakeups;
- schema normalization across provider-specific JSON Schema subsets.

