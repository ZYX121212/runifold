# RFC 0077: Existing provider protocol audit

- Status: Implemented, pending release
- Reviewed: 2026-10-14
- Scope: existing transport interfaces; independent Live, Interactions, hosted
  Agent, Files and Batch services are outside this audit.

## Contract

Vendor wire changes belong in `runifold-providers`. Run and stream share the
canonical accumulator, cancellation and retry rules. Provider-native history
must survive `ModelResponse::assistant_message`, serde persistence and another
request. Model availability and model-specific feature support remain separate
from transport support. No default model IDs or regional credentials change.

## Corrections and official sources

| Interface | Source and resulting adapter behavior |
| --- | --- |
| Anthropic Messages | [Structured outputs](https://platform.claude.com/docs/en/build-with-claude/structured-outputs): use GA `output_config.format`, retain schema unchanged, merge non-conflicting effort options. Explicit JSON Schema is required; JSON-only mode remains unsupported. [Strict tools](https://platform.claude.com/docs/en/agents-and-tools/tool-use/strict-tool-use) enables `anthropic.strict` metadata. |
| Anthropic thinking/tools | [Extended thinking](https://platform.claude.com/docs/en/build-with-claude/extended-thinking): preserve encrypted redacted blocks and signature-only thinking. [Programmatic calling](https://platform.claude.com/docs/en/agents-and-tools/tool-use/programmatic-tool-calling): retain caller metadata; tool descriptor metadata can carry allowed callers, defer loading and examples. This does not implement a hosted tool executor. |
| Anthropic documents/usage | [PDF support](https://platform.claude.com/docs/en/build-with-claude/pdf-support): PDF URL/base64 documents use native blocks; other unsupported media keep existing explicit projection. [Caching](https://platform.claude.com/docs/en/build-with-claude/prompt-caching): total input includes uncached, cache reads and cache writes exactly once across incremental updates. |
| Gemini GenerateContent | [Thought signatures](https://ai.google.dev/gemini-api/docs/generate-content/thought-signatures): replay original signed function-call part, including absence of a native ID; matching results also omit an invented ID. Preserve signed-part boundaries and opaque native output parts. [Generation config and usage](https://ai.google.dev/api/generate-content): support signed-32-bit seed and nested optional generation config without overriding canonical fields; unsupported multiple candidates fail locally. Input includes tool prompt tokens; output includes thoughts, which remain an output subset. |
| Bedrock ConverseStream | [Structured outputs](https://docs.aws.amazon.com/bedrock/latest/userguide/structured-output.html): use `outputConfig.textFormat` and `bedrock.strict` tool metadata. JSON Schema is serialized without removing constraints. [Reasoning content](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_ReasoningContentBlock.html): preserve signed text and fragmented encrypted bytes through binary EventStream and replay. Private buffers are bounded and decoder Debug output omits payloads. |
| Cohere Rerank v2 | [Rerank reference](https://docs.cohere.com/reference/rerank): canonical `api.cohere.com` endpoint; explicit `with_max_tokens_per_doc` and priority 0–999. Stream response reads under a 4 MiB bound with cancellation; reject credentials/query/fragment in base URLs. |
| DeepSeek Chat | [Thinking mode](https://api-docs.deepseek.com/guides/thinking_mode/): retain assistant `reasoning_content` for subsequent turns, including after persistence. [Agent integrations](https://api-docs.deepseek.com/quick_start/agent_integrations/oh_my_pi/): omit default auto tool choice, and implement no-tools choice by removing tools; explicit thinking plus forced tool choice fails locally. Cache-hit tokens map to cached input, not cache writes. |
| OpenRouter Chat | [Reasoning tokens](https://openrouter.ai/docs/guides/best-practices/reasoning-tokens): preserve reasoning and indexed `reasoning_details`, including encrypted data; assemble string fragments while rejecting identity/type drift. |
| Qwen Chat | [Deep thinking](https://www.alibabacloud.com/help/en/model-studio/deep-thinking): preserve `reasoning_content`; callers explicitly set `preserve_thinking` for supported models. [Function calling](https://docs.modelstudio.console.alibabacloud.com/en/model-studio/qwen-function-calling): reject forced choice with explicit thinking enabled. Existing China/international endpoints remain available; workspace-specific URLs use the existing base-URL override. |
| MiniMax Chat | [OpenAI-compatible API](https://platform.minimax.io/docs/api-reference/text-openai-api): preserve separate `reasoning_content` and complete content with embedded thinking tags. Do not rewrite model-specific thinking settings. |
| Zhipu Chat | [Thinking](https://docs.z.ai/guides/capabilities/thinking-mode): preserve `reasoning_content`, including when caller opts into retained thinking. The China profile keeps its original region. |
| Mistral Chat | [Chat reference](https://docs.mistral.ai/api/endpoint/chat): map canonical seed to `random_seed`. Existing tools, response formats and stream fields retain their wire mapping. |
| Groq Chat | [Compatibility](https://console.groq.com/docs/openai): use current `max_completion_tokens` for output limits; other compatible fields remain unchanged. |
| xAI Chat | [Chat reference](https://docs.x.ai/developers/rest-api-reference/inference/chat-completions): reconcile output against total minus input, retaining separately reported reasoning without double counting. Existing Chat endpoint remains supported; no automatic migration to Responses. |

| Ollama Chat | [Native wire types](https://github.com/ollama/ollama/blob/main/api/types.go): preserve assistant tool-call `id` and result `tool_call_id`, including parallel same-name calls across persisted history. Existing thinking, format and options mappings remain unchanged. |

## Interfaces checked without a transport change

These retain their existing protocol families and pass the provider suite.
An upstream document alone is not evidence that every hosted model supports
all canonical features.

- [OpenAI Chat](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create)
  and the existing Responses, media and Realtime surfaces retain the 0.10.2
  contracts and regression coverage.
- [Azure Responses](https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/responses?view=foundry-classic)
  retains v1 Responses and explicit authentication.
- [Ark Responses](https://docs.volcengine.com/docs/ark/create-model-responses-api?lang=zh)
  and the [official SDK guide](https://github.com/volcengine/ark-runtime-python/blob/main/docs/usage.md)
  retain the existing Responses interface and offline conformance contract.
  The dynamic API reference was only partially retrievable, so the official
  SDK guide supplements it; no exhaustive live-vendor acceptance claim is made.
- [Together compatibility](https://docs.together.ai/docs/inference/openai-compatibility)
  retains Chat, tools, JSON and streaming. Advanced model-native reasoning options
  remain explicit provider options; no universal reasoning-history guarantee.
- [Perplexity Sonar](https://docs.perplexity.ai/docs/agent-api/migrate-from-sonar/overview)
  retains `/v1/sonar`; the independent Agent interface is excluded.
- [SiliconFlow China](https://docs.siliconflow.cn/docs/api/chat-completions-post)
  retains `/v1/chat/completions` and optional thinking controls. Its
  [reasoning guide](https://docs.siliconflow.cn/docs/userguide/capabilities/reasoning)
  demonstrates visible-response history; a universal cross-model reasoning replay
  contract is not asserted. Unsupported history fails explicitly.
- [Hugging Face](https://huggingface.co/docs/inference-providers/en/tasks/chat-completion)
  retains `router.huggingface.co/v1`; routing and feature availability depend on
  the selected model and upstream provider.
- [vLLM](https://github.com/vllm-project/vllm/blob/main/docs/serving/online_serving/openai_compatible_server.md),
  [llama.cpp](https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md)
  and [llamafile](https://github.com/mozilla-ai/llamafile/blob/main/README.md)
  retain caller-selected compatible protocols. Server version, templates and
  enabled parsers determine local feature support; custom endpoints remain
  caller-owned contracts.

## Acceptance boundaries

Regression tests use actual loopback HTTP, including binary AWS frames, both
Chat response modes and serialized second-turn replay. They verify adapter
contracts without vendor credentials. Native Anthropic/Gemini/Ollama use SSE or
NDJSON internally for both application modes. Native model/schema support and
real account integration still require opt-in live tests. Unsupported JSON
Schema constraints are never silently stripped; vendor validation may reject
schemas outside its supported subset. No release has been published by this
change.

## Local validation

- Provider all-feature suite: 316 passed; two opt-in live tests ignored.
- Neutral model boundary suite: 59 passed.
- Rust 1.99 provider Clippy, all targets/features, warnings denied: passed.
- Rust 1.88 provider all-feature compilation: passed.
- Browser/WASM provider compilation (OpenAI, Anthropic, Gemini, Ollama, Cohere): passed.
  Bedrock remains native-only.
- Workspace public rustdoc with warnings denied, formatting and generated AI
  knowledge checks: passed.
- Full workspace database/container tests and vendor live tests are not local
  acceptance evidence for this change; CI and opt-in live gates remain distinct.

## Transport dependency audit

CI discovered four newly published RustSec advisories on 2026-10-14:
[RUSTSEC-2026-0346](https://github.com/RustSec/advisory-db/blob/main/crates/rustls/RUSTSEC-2026-0346.md),
[RUSTSEC-2026-0347](https://github.com/RustSec/advisory-db/blob/main/crates/rustls/RUSTSEC-2026-0347.md),
[RUSTSEC-2026-0348](https://github.com/RustSec/advisory-db/blob/main/crates/rustls/RUSTSEC-2026-0348.md)
and [RUSTSEC-2026-0349](https://github.com/RustSec/advisory-db/blob/main/crates/rustls/RUSTSEC-2026-0349.md).
The workspace and standalone benchmark locks now resolve rustls 0.23.46, the
patched version for all four. Security policy is unchanged.

An additional standalone benchmark audit found its older h2 lock affected by
[RUSTSEC-2026-0258](https://github.com/RustSec/advisory-db/blob/main/crates/h2/RUSTSEC-2026-0258.md)
and a yanked chacha20 0.10.1. Its lock now resolves h2 0.4.20 and chacha20 0.10.2.
The benchmark tests and its Rust 1.88 compilation are verified independently.
