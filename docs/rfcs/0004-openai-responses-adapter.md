# RFC 0004: OpenAI Responses adapter

- Status: Accepted for initial implementation
- Scope: `runifold-provider-openai`

## Summary

The first production provider edge targets the OpenAI Responses API. It
translates Runifold's canonical model request into a streaming Responses
request and translates typed server-sent events back into canonical model
events.

The adapter uses the public `Model` boundary. No OpenAI wire type enters the
runtime kernel, tool runtime, or future agent API.

## Why Responses

Responses exposes typed semantic streaming events rather than an untyped text
delta alone. Its event model covers response lifecycle, content blocks,
function arguments, refusals, usage, and future provider-specific output.
This maps cleanly onto Runifold's strict stream state machine.

References used for the initial wire contract:

- <https://developers.openai.com/api/docs/guides/streaming-responses?api-mode=responses>
- <https://developers.openai.com/api/docs/guides/function-calling#streaming>
- <https://developers.openai.com/api/docs/guides/migrate-to-responses#7-update-streaming-consumers>
- <https://api.openai.com/v1/responses>

## Configuration invariants

1. Credentials are explicit and stored in a secret wrapper.
2. Debug formatting always redacts credentials.
3. The default base URL is `https://api.openai.com/v1/`.
4. A custom HTTP(S) base URL enables compatible endpoints.
5. Organization and project headers are optional and explicit.
6. The HTTP client can be injected to configure transport policy.

## Request translation

The adapter owns `model`, `input`, `stream`, tools, tool choice, output format,
and common generation fields. `provider_options.openai` may add fields but may
not replace adapter-owned fields.

Canonical features without a lossless mapping fail before network I/O. The
initial adapter rejects generic audio, unresolved artifacts, generic reasoning
round trips, stop sequences, and deterministic seeds. OpenAI-native items that
do not yet have a canonical representation can be supplied explicitly as an
`openai/input_item` opaque part.

## Streaming translation

The decoder handles:

- `response.created`;
- output-text and refusal content blocks;
- streamed function-call arguments;
- completed and incomplete responses;
- usage, cached tokens, and reasoning-token details;
- provider failures and protocol errors.

Unknown events and unknown content parts are retained as provider events.
Adding a new backwards-compatible OpenAI event therefore does not erase data
or break an otherwise valid stream.

## Observability

Every request sends the Runifold invocation ID as `X-Client-Request-Id`.
The adapter captures OpenAI's `x-request-id` response header and emits it into
the provider event stream. Structured HTTP error fields and request IDs are
retained in `ModelError.metadata`.

## Capability truthfulness

The adapter claims only protocol-level streaming support by default. Features
that depend on the selected model, such as tools, structured output, images,
documents, and reasoning, remain `Unknown`; generic audio is `Unsupported`
until its canonical mapping is implemented.

Applications may declare model-specific capabilities explicitly. Under the
default strict policy, unknown required features fail. Best-effort mode permits
unknown features only with a visible warning.

## Deferred decisions

- model capability catalog and cache invalidation;
- resumable streams;
- retry and idempotency middleware;
- WebSocket Responses transport;
- hosted tools and MCP wire types;
- generic audio and encrypted reasoning round trips.
