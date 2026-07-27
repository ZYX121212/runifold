# RFC 0026: Native Anthropic adapter and provider protocol testkit

- Status: implemented
- Scope: `runifold-provider-anthropic`, `runifold-provider-testkit`

## Decision

Runifold implements Anthropic as a native Messages API adapter. It does not
route Anthropic through OpenAI-compatible request or response shapes. Provider
wire types remain outside `runifold-model`; the adapter translates at the
existing `Model` boundary.

Provider conformance tests use a separate offline HTTP cassette crate. The
testkit owns protocol fixtures and fault injection, but production providers
continue to own their HTTP clients and native protocol state machines.

## Request translation

The adapter maps canonical content as follows:

- system messages become top-level Anthropic system blocks;
- user and assistant messages retain ordered content blocks;
- canonical tool definitions become Anthropic tools and tool choice;
- assistant tool calls become `tool_use`;
- user or tool-role results become `tool_result`;
- base64 and URL images become native image sources;
- signed thinking blocks are retained for valid round trips;
- `provider_options.anthropic` may add non-owned fields.

Adapter-owned fields cannot be replaced through provider options. Unsupported
audio, document-beta, structured-output, citation, or foreign opaque content
fails locally instead of being silently dropped.

Anthropic requires `max_tokens`; canonical requests do not. The configuration
therefore contains an explicit default, initially 1024, which a request-level
output limit overrides.

## Stream translation

The SSE decoder is stateful and fail-closed. It handles:

- message start and cumulative usage;
- text deltas;
- fragmented tool JSON;
- thinking and signature deltas;
- redacted thinking preservation;
- content-block completion;
- stop-reason normalization;
- heartbeats and unknown provider events;
- structured stream errors.

End-of-body is not success. A valid stream must receive `message_stop` with no
open content blocks. Truncation therefore becomes a protocol error rather than
a partial successful response.

## Transport and diagnostics

Credentials use `x-api-key` and never appear in configuration debug output.
The adapter sends an explicit Anthropic version, observes cancellation and
deadlines, captures request IDs, parses `Retry-After`, and retains structured
Anthropic error types in namespaced metadata.

## Provider testkit

The testkit binds only to an ephemeral loopback address and performs no
internet access. A cassette is an ordered list of expected requests and
scripted responses. It supports:

- method, path, and exact JSON-body assertions;
- real chunked HTTP response bodies;
- delay before each response fragment;
- socket close before the terminating HTTP chunk;
- captured request inspection;
- automatic redaction of authorization and API-key headers.

This deliberately small HTTP/1.1 implementation is test-only. It avoids
forcing production adapters behind a shared transport abstraction and permits
faults that high-level mock servers often normalize away.

## Verification

The Anthropic adapter is accepted only with offline tests for:

1. text response accumulation and usage;
2. fragmented tool arguments;
3. structured rate-limit errors and request IDs;
4. stream truncation;
5. cancellation during a delayed response;
6. an already elapsed deadline;
7. credential redaction and exact request translation.

Future Gemini and Ollama adapters reuse the cassette behaviors, not Anthropic
wire types.
