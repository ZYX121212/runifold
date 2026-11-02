# RFC 0005: Provider identity and wire protocol are orthogonal

- Status: Accepted for initial implementation
- Scope: `runifold-providers::openai`

## Summary

Runifold does not equate an API vendor with an HTTP JSON shape. A configured
model edge has two independent identities:

1. the canonical provider (`openai`, `ark`, `qwen`, or an application-defined
   name);
2. the wire protocol (`responses` or `chat_completions`).

This distinction is required because vendors may implement multiple protocols,
models on the same vendor may expose different protocol subsets, and custom
gateways frequently implement Chat Completions without implementing Responses.

## Supported configurations

The initial compatible adapter supports:

- OpenAI Responses;
- Volcengine Ark Responses;
- Alibaba Model Studio Responses for models and regions that expose it;
- Alibaba Model Studio Chat Completions;
- custom HTTP(S) endpoints implementing either supported wire protocol.

Official references:

- Ark Chat Completions:
  <https://api.volcengine.com/api-docs/view?action=ChatCompletions&serviceCode=ark&version=2024-01-01>
- Ark Responses tool calling:
  <https://www.volcengine.com/docs/82379/1958524?lang=zh>
- Qwen text-generation API reference:
  <https://www.alibabacloud.com/help/en/model-studio/qwen-api-reference>
- Qwen OpenAI-compatible Chat Completions:
  <https://www.alibabacloud.com/help/en/model-studio/text-generation>

## Invariants

1. `ModelRef.provider` must equal the configured provider identity.
2. Provider identity is retained in canonical responses, errors, and opaque
   events.
3. Endpoint paths are selected from the explicit wire protocol.
4. Request options may be namespaced under the provider identity or
   `openai-compatible`.
5. Unknown vendor fields remain visible in provider events.
6. Compatibility never implies that every model supports every feature.
7. Forced function-tool choices without a declared tool fail locally; Chat Completions tool-role input requires a correlated tool result rather than free text.

## Why not one universal JSON client

The two protocols have materially different streaming semantics. Responses
emits lifecycle and content-block events. Chat Completions emits delta chunks
and terminates with `[DONE]`. They therefore use separate, stateful decoders
that converge only at Runifold's canonical event protocol.

## Deferred decisions

- Anthropic Messages wire protocol;
- Gemini native protocol;
- provider discovery and model catalogs;
- endpoint health routing;
- dialect quirks such as alternate token-limit fields;
- conformance test suites for self-hosted gateways.

## Model-scoped Responses continuation state

Provider identity alone does not authorize replay of private reasoning across
models on a shared compatible gateway. The HTTP and native WebSocket Model
adapters record the physical request `ModelRef` in terminal metadata under
`runifold.response.origin_model.v1`. `ModelResponse::assistant_message` preserves
that identity, falling back to the response model for manually constructed
responses. Provider-reported deployment names remain in `ModelResponse.model`;
they do not override the request alias used for continuation compatibility.
Nested routers preserve the adapter's physical identity.

At each Responses encoding boundary, only `input_item` opaque values whose
`type` is `reasoning` are scoped by that origin. Exact provider and model matches
round-trip unchanged. Foreign-model reasoning is excluded from the sending view;
text, other opaque items and Tool calls/results retain their relative order and
call IDs. A reasoning-only message may contribute no wire items. Canonical
messages, stored history, checkpoints, usage and retry policies are unchanged.
Model invocation reports `openai.reasoning_model_projection` with an omitted-item
count, without private contents. Direct `encode_request` callers receive only the
wire projection and must retain their canonical request for audit.

The `openai-compatible` opaque namespace permits protocol adaptation, not
cross-model reasoning reuse. Missing, malformed or empty origin metadata on
reasoning-bearing history is an `InvalidRequest` before transport. To migrate old
messages, explicitly attach a verified serialized `ModelRef` under the metadata
key, or remove private reasoning in a derived input while retaining original
history. Never infer origin from the next request's target or rewrite recovery
records. Aliases are compared exactly; equivalence between different alias names
is not assumed. This contract covers Responses reasoning input items, not an
inference that every HTTP 400 or other protocol's private state has the same cause.
