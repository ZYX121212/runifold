# RFC 0005: Provider identity and wire protocol are orthogonal

- Status: Accepted for initial implementation
- Scope: `runifold-provider-openai`

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
