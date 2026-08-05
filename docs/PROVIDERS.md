# Provider support

Provider-only installations can select the lightweight kernel with
`--no-default-features`. The compatible default includes `runtime` for examples
using `ProviderModelExt::runtime`, Agent, Tool, Effect, Retrieval, or Workflow
APIs.

Runifold separates provider identity from wire protocol. Native providers keep
their native event semantics; OpenAI-compatible providers share one hardened
transport and decoder without creating one crate per endpoint.

## Matrix

| Provider | Cargo feature | Wire protocol | Public constructor | Verification level |
| --- | --- | --- | --- | --- |
| OpenAI | `openai` | Responses | `OpenAiClient::from_api_key` | offline real HTTP cassette |
| Azure OpenAI | `azure` | v1 Responses | `azure::api_key_client` / `azure::entra_client` | offline real HTTP cassette |
| Amazon Bedrock | `bedrock` | Converse Stream | `BedrockClient::new` / `from_credentials` | offline real HTTP binary cassette |
| Anthropic | `anthropic` | Messages SSE | `AnthropicClient::from_api_key` | offline real HTTP cassette |
| Gemini | `gemini` | GenerateContent SSE | `GeminiClient::from_api_key` | offline real HTTP cassette |
| Ollama | `ollama` | Chat NDJSON | `OllamaClient::new` | offline real HTTP cassette |
| Volcengine Ark | `ark` | Responses | `ark::client` | offline HTTP cassette + manual live canary |
| Alibaba Qwen | `qwen` | Chat Completions | `qwen::client` | protocol conformance |
| DeepSeek | `deepseek` | Chat Completions | `deepseek::client` | protocol conformance |
| OpenRouter | `openrouter` | Chat Completions | `openrouter::client` | protocol conformance |
| xAI | `xai` | Chat Completions | `xai::client` | protocol conformance |
| Groq | `groq` | Chat Completions | `groq::client` | protocol conformance |
| Mistral | `mistral` | Chat Completions | `mistral::client` | protocol conformance |
| Together AI | `together` | Chat Completions | `together::client` | protocol conformance |
| Perplexity Sonar | `perplexity` | Chat Completions | `perplexity::client` | protocol conformance |
| MiniMax | `minimax` | Chat Completions | `minimax::client` | protocol conformance |
| Zhipu AI | `zhipu` | Chat Completions | `zhipu::client` | protocol conformance |
| SiliconFlow | `siliconflow` | Chat Completions | `siliconflow::client` | protocol conformance |
| Custom endpoint | `openai` | Responses or Chat Completions | `OpenAiConfig::custom` | caller-owned contract |

“Offline real HTTP cassette” means the adapter is exercised through an actual
loopback HTTP server, including streaming fragmentation and transport failure
classification. “Protocol conformance” means endpoint selection, canonical
request encoding, fragmented stream decoding, provider identity, tools,
reasoning fields, and detailed token usage are deterministic tests; it does not
claim a live request against the vendor on every CI run.

Ark additionally has a manually dispatched live gate covering strict JSON
Schema, native `web_search`, mixed native/function tools, and both streamed and
complete Responses delivery. Its artifact excludes credentials and response
content.

## Ark Responses example

Ark's verified Responses baseline declares function tools, structured output,
image/document input, and hosted web search as native. Model-specific limits
can still be narrowed with `with_capabilities`.

```rust,no_run
use runifold::{ProviderModelExt, ResponseMode, ark};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let runtime = ark::client(std::env::var("ARK_API_KEY")?)?
    .runtime("doubao-seed-2-0-lite-260428")?;

let answer = runtime
    .agent("researcher")
    .system("Return precise, sourced JSON.")
    .provider_tool(ark::ArkWebSearchTool::new().limit(8).max_keyword(5).into())
    .temperature(0.2)
    .max_output_tokens(4_096)
    .response_mode(ResponseMode::Complete)
    .provider_options("ark", serde_json::json!({"thinking": {"type": "enabled"}}))
    .prompt_text("Research the requested company.")
    .await?;
# let _ = answer;
# Ok(())
# }
```

Function tools registered through `.tool(...)` and Ark hosted tools registered
through `.provider_tool(...)` are encoded into one `tools` array without using
conflicting raw `provider_options`.

The Bedrock binary cassette executes the AWS SDK over a real loopback HTTP
socket. It verifies `SigV4` request construction, temporary-credential
redaction, arbitrary EventStream fragmentation, usage, truncation,
deadline enforcement, and concurrent stream isolation without requiring live
AWS credentials.

The shared `runifold-provider-testkit` conformance API verifies canonical
provider identity, visible-text/reasoning separation, detailed usage, retained
raw provider events, normalized error kinds, and retry safety. OpenAI-compatible,
Anthropic, Gemini, and Ollama success paths all run through this same contract;
Anthropic rate limiting also exercises the shared error contract over real
loopback HTTP.

## Azure OpenAI example

Azure uses the current `/openai/v1/responses` contract and accepts either a
resource API key or an application-provided Microsoft Entra bearer token:

```rust,no_run
use runifold::{ProviderModelExt, azure};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let runtime = azure::api_key_client(
    "https://my-resource.openai.azure.com",
    std::env::var("AZURE_OPENAI_API_KEY")?,
)?
.runtime("my-gpt-deployment")?;

let answer = runtime
    .agent("assistant")
    .system("Answer precisely.")
    .prompt_text("Why does idempotency matter?")
    .await?;
# let _ = answer;
# Ok(())
# }
```

The model string is the Azure deployment name. The constructor owns the
`/openai/v1/` suffix, so pass the resource or Foundry project endpoint before
that suffix. `entra_client` accepts a bearer token but intentionally does not
acquire or refresh it; credential lifecycle remains an application concern.
Use `OpenAiConfig::with_azure_api_version(AzureOpenAiApiVersion::Preview)` only
when preview behavior is explicitly required.

## Amazon Bedrock example

Bedrock uses the native Converse Stream protocol and the AWS SDK's `SigV4`
implementation. Applications retain ownership of the standard AWS credential
chain and pass the resulting service configuration into Runifold:

```rust,ignore
use runifold::{
    ProviderModelExt,
    bedrock::{BedrockClient, BedrockSdkConfig},
};

let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
    .load()
    .await;
let config = BedrockSdkConfig::from(&shared);
let runtime = BedrockClient::new(&config)
    .runtime("us.anthropic.claude-sonnet-4-20250514-v1:0")?;

let answer = runtime
    .agent("assistant")
    .system("Answer precisely.")
    .prompt_text("Why must retries share one budget authority?")
    .await?;
```

The model string may be a Bedrock model ID, inference-profile ID or ARN,
provisioned-model ARN, or prompt ARN accepted by Converse. `BedrockClient::new`
disables SDK retries so Runifold remains the sole retry, circuit-breaker,
budget, and observability authority. `from_sdk_client` is an escape hatch for
application-owned SDK policy; callers must avoid layering a second retry loop.
`from_credentials` supports explicit temporary credentials and session tokens,
but production applications should prefer short-lived credentials from their
standard AWS chain.

## Compatible provider example

```rust,no_run
use runifold::deepseek::{DeepSeekAgentExt, client};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let agent = client(std::env::var("DEEPSEEK_API_KEY")?)?
    .agent("reasoner", "deepseek-reasoner")
    .system("Solve carefully, then give a concise answer.");

let answer = agent.prompt_text("What is 37 * 41?").await?;
# let _ = answer;
# Ok(())
# }
```

## Automatic runtime composition

Every concrete adapter implements the same `Model + ProviderModel` contract.
That unlocks one provider-neutral composition path:

```rust,no_run
use runifold::{
    ProviderModelExt,
    deepseek::client,
};

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let runtime = client(std::env::var("DEEPSEEK_API_KEY")?)?
    .runtime("deepseek-reasoner")?;

let agent = runtime
    .agent("reasoner")
    .system("Solve carefully, then answer concisely.");
# let _ = agent;
# Ok(())
# }
```

The runtime applies bounded, jittered retries only to failures marked
retry-safe by the adapter and maintains an independent circuit breaker for the
physical route. Its canonical stream carries reasoning, usage, warnings,
provider events, and normalized errors. The resulting Agent uses the same
budget, capability, cancellation, observability, effect, and durable workflow
boundaries as every other Runifold Agent.

With the `otel` feature, call `runtime.with_otel()` before building the Agent.
The instrumentation wraps routing and retries rather than bypassing them.

Regional providers make endpoint location explicit:

```rust,no_run
use runifold::qwen::{QwenAgentExt, QwenRegion, client};

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let agent = client(QwenRegion::China, std::env::var("DASHSCOPE_API_KEY")?)?
    .agent("assistant", "qwen-plus");
# let _ = agent;
# Ok(())
# }
```

OpenRouter attribution is opt-in and validated before transport:

```rust,no_run
use runifold::openai::{OpenAiClient, OpenAiCompatibleProfile, OpenAiConfig};

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let config = OpenAiConfig::from_profile(
    OpenAiCompatibleProfile::OpenRouter,
    std::env::var("OPENROUTER_API_KEY")?,
)?
.with_openrouter_attribution("https://example.com", "My Runifold App")?;
let client = OpenAiClient::new(config);
# let _ = client;
# Ok(())
# }
```

## Capability contract

Provider-wide model capabilities are intentionally not guessed. Streaming is a
transport capability, while tools, structured output, reasoning, multimodal
input, and context limits may vary by model even behind one endpoint. The
default compatible client therefore reports those model-dependent features as
unknown. Applications can attach verified model-specific capabilities with
`OpenAiClient::with_capabilities`.

Unknown provider fields remain available as canonical provider events. This
lets Runifold add new normalization without discarding data or silently
pretending that two vendors have identical semantics.
