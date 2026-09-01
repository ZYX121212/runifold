<h1 align="center">Runifold</h1>

<p align="center">
  A typed, observable, cancellable, and budget-aware runtime for models, tools,
  agents, and workflows in Rust.
</p>

<p align="center">
  <a href="https://runifold-docs.hiayun.chatgpt.site/"><img src="https://img.shields.io/badge/docs-Runifold-dca282.svg" alt="Runifold technical documentation"></a>&nbsp;
  <a href="https://docs.rs/runifold/latest/runifold/"><img src="https://img.shields.io/badge/docs-API%20Reference-dca282.svg" alt="API reference"></a>&nbsp;
  <a href="https://crates.io/crates/runifold"><img src="https://img.shields.io/crates/v/runifold.svg?color=dca282" alt="crates.io version"></a>&nbsp;
  <a href="https://crates.io/crates/runifold"><img src="https://img.shields.io/crates/d/runifold.svg?color=dca282" alt="crates.io downloads"></a>&nbsp;
  <a href="https://github.com/ZYX121212/runifold/actions/workflows/ci.yml"><img src="https://github.com/ZYX121212/runifold/actions/workflows/ci.yml/badge.svg" alt="CI status"></a>&nbsp;
  <a href="https://github.com/ZYX121212/runifold#license"><img src="https://img.shields.io/crates/l/runifold.svg?color=dca282" alt="license"></a>
  <br>
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/built%20with-Rust-dca282.svg?logo=rust" alt="built with Rust"></a>&nbsp;
  <a href="https://github.com/ZYX121212/runifold"><img src="https://img.shields.io/github/stars/ZYX121212/runifold?style=social" alt="GitHub stars"></a>
</p>

<p align="center">
  <a href="https://runifold-docs.hiayun.chatgpt.site/">Technical docs</a>
  &nbsp;&bull;&nbsp;
  <a href="https://docs.rs/runifold/latest/runifold/">API Reference</a>
  &nbsp;&bull;&nbsp;
  <a href="https://crates.io/crates/runifold">Crates.io</a>
  &nbsp;&bull;&nbsp;
  <a href="https://github.com/ZYX121212/runifold/blob/main/docs/PROVIDERS.md">Providers</a>
  &nbsp;&bull;&nbsp;
  <a href="https://github.com/ZYX121212/runifold/blob/main/docs/RELIABILITY.md">Reliability</a>
  &nbsp;&bull;&nbsp;
  <a href="https://github.com/ZYX121212/runifold/blob/main/CHANGELOG.md">Changelog</a>
</p>

> [!WARNING]
> Runifold is pre-alpha. Public APIs may change before 1.0; breaking changes are
> documented in the [changelog](CHANGELOG.md) and released with a new minor
> version.

## Contents

- [What is Runifold?](#what-is-runifold)
- [Why Runifold?](#why-runifold)
- [Quickstart](#quickstart)
- [Choose the right layer](#choose-the-right-layer)
- [Provider support](#provider-support)
- [Production boundaries](#production-boundaries)
- [Technical documentation](#technical-documentation)
- [Workspace](#workspace)
- [Contributing](#contributing)
- [License](#license)

## What is Runifold?

Runifold provides one execution model for calling models, executing typed
tools, running agents, and composing durable workflows. The name combines
**run** with **manifold**: models, tools, agents, and flows are different
surfaces over the same execution space.

Simple APIs use the same runtime path as advanced ones. A quick prompt can
therefore grow into a capability-limited, observable, recoverable workflow
without replacing its model or tool abstractions.

## Why Runifold?

- **Typed, provider-neutral contracts.** Models, multimodal content, tools,
  retrieval, effects, and workflows remain independent of vendor wire types.
- **Explicit authority.** Capabilities, budgets, deadlines, and cancellation
  propagate through a structured run tree; child runs cannot gain authority
  their parent did not grant.
- **Durable execution.** Write-ahead effects, checkpoints, leases, fencing,
  timers, signals, and human review support conservative crash recovery.
- **Observable by construction.** Canonical events, optional OpenTelemetry
  instrumentation, read-only operational tooling, and value-free diffs explain
  what happened without making prompt capture the default.
- **Failure-aware delivery.** Provider adapters expose retry safety, strict
  stream lifecycles, bounded retries, circuit breakers, and visible capability
  degradation instead of silently hiding failures.
- **Testable offline.** Deterministic models, fault injection, real loopback
  protocol cassettes, evaluation gates, and reproducible benchmarks are part of
  the public testing surface.

## Quickstart

Add the facade and the provider adapters your application needs:

```console
cargo add runifold
cargo add runifold-providers --features openai
```

Create an OpenAI-backed agent:

```rust,no_run
use runifold::ProviderModelExt;
use runifold_providers::openai::OpenAiClient;

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = OpenAiClient::from_api_key(std::env::var("OPENAI_API_KEY")?)?
        .runtime("gpt-5")?;

    let answer = runtime
        .agent("assistant")
        .system("Answer precisely and expose uncertainty.")
        .prompt_text("Why is durable execution useful?")
        .await?;

    println!("{answer}");
    Ok(())
}
```

`ProviderRuntime` is long-lived application state. Construct it once at
startup and clone it into request handlers. Clones share retry and
circuit-breaker state; rebuilding it for every request resets route health.

OpenAI-compatible providers use the same runtime and Agent path. For example:

```rust,no_run
use runifold::ProviderModelExt;
use runifold_providers::deepseek;

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let answer = deepseek::client(std::env::var("DEEPSEEK_API_KEY")?)?
        .runtime("deepseek-reasoner")?
        .agent("reasoner")
        .prompt_text("Why does idempotency matter?")
        .await?;

    println!("{answer}");
    Ok(())
}
```

The root run created by this ergonomic path receives only the Tool,
retrieval, and child-Agent capabilities explicitly registered on the Agent.
Use `agent.run(input, &context)` when the application must supply a tighter
budget, narrower capabilities, a deadline, durable journaling, or shared
run-tree identity.

## Choose the right layer

| Need | Start with |
| --- | --- |
| Agents, typed Tools, retrieval, effects, and workflows | `runifold` |
| Low-level provider-neutral model invocation only | `runifold-model` |
| OpenAI, Anthropic, Gemini, Ollama, Bedrock, or compatible transports | `runifold-providers` |
| MCP clients, servers, Sampling, Resources, Prompts, and durable Tasks | `runifold-mcp` or the `mcp` facade feature |
| Durable local state | `runifold-store-sqlite` or the `sqlite` facade feature |
| Distributed workflows and Agent state | `runifold-store-postgres` and the `workflow-postgres` facade feature |
| PostgreSQL/pgvector retrieval | `runifold-retrieval-pgvector` or the `pgvector` facade feature |
| OpenTelemetry GenAI signals | `runifold-observability-otel` or the `otel` facade feature |
| Deterministic tests and evaluations | `runifold-testkit` and `runifold-eval-cli` |

For a lightweight model-only application:

```console
cargo add runifold-model
cargo add runifold-providers --features openai
```

Add the facade only when Agent, Tool, Effect, Retrieval, or Workflow
composition is required.

## Provider support

Runifold separates provider identity from wire protocol. Native adapters keep
vendor event semantics, while compatible providers share a hardened transport
without creating one crate per endpoint.

- Native adapters: OpenAI Responses, Azure OpenAI, Anthropic Messages, Gemini
  GenerateContent, Ollama Chat, Amazon Bedrock Converse Stream, and Cohere
  Rerank.
- OpenAI-compatible profiles: Ark, Qwen, DeepSeek, OpenRouter, xAI, Groq,
  Mistral, Together AI, Perplexity Sonar, MiniMax, Zhipu AI, SiliconFlow, and
  Hugging Face Inference Providers.
- Application-owned endpoints: vLLM, llama.cpp, llamafile, gateways, and
  custom Responses or Chat Completions services.

Capabilities are declared per model when they vary by model. Unknown model
names retain conservative adapter defaults instead of inheriting guessed
features. See the [provider support matrix](docs/PROVIDERS.md) for constructors,
Cargo features, protocol boundaries, regional endpoints, and verification
levels.

## Production boundaries

Runifold treats correctness claims as executable contracts:

- Provider streams, cancellation, disconnects, and concurrent requests run
  through real loopback HTTP/SSE/NDJSON/binary EventStream cassettes.
- SQLite recovery includes forced process termination; PostgreSQL behavior is
  verified against disposable real databases with outage and restart tests.
- The provider-neutral facade compiles for `wasm32-unknown-unknown`; supported
  browser Provider paths run in pinned headless Chrome through real CORS,
  Fetch, WebSocket, and WebRTC boundaries.
- Rich Tool results and durable artifacts retain text, images, audio,
  documents, resources, structured content, and stable artifact references
  without flattening everything into a JSON string.
- The framework-neutral benchmark contract includes a standalone,
  release-mode [Rig comparison](docs/BENCHMARKING.md) with equivalent requests,
  alternating rounds, confidence intervals, and retained raw evidence.

These claims are deliberately narrower than the feature list. The
[reliability matrix](docs/RELIABILITY.md) distinguishes mandatory CI evidence,
scheduled or manual gates, planned work, and areas requiring independent
reproduction. Browser deployments must also follow the documented
[application-gateway credential boundary](docs/EDGE.md).

## Technical documentation

- [Runifold technical site](https://runifold-docs.hiayun.chatgpt.site/)
- [API reference](https://docs.rs/runifold/latest/runifold/)
- [Project charter and architectural laws](docs/CHARTER.md)
- [Provider support matrix](docs/PROVIDERS.md)
- [Reliability evidence](docs/RELIABILITY.md)
- [Browser and edge deployment](docs/EDGE.md)
- [Benchmarking contract](docs/BENCHMARKING.md)
- [Testing guide](docs/TESTING.md)
- [Operations and SLO runbook](docs/operations-slo.md)
- [RFC index](docs/rfcs/)
- [Release history](CHANGELOG.md)

The RFC series records the stable kernel, model and provider boundaries,
Agent and Tool semantics, MCP, retrieval, durable workflows, multi-tenant
budgets, governance, storage, evaluation, and release integrity. The API
reference is the best entry point for type-level usage; the RFCs explain why
the boundaries exist.

## Workspace

| Area | Crates |
| --- | --- |
| Public facade and kernel | `runifold`, `runifold-core`, `runifold-model` |
| Agent execution | `runifold-agent`, `runifold-tool`, `runifold-macros`, `runifold-effect`, `runifold-workflow` |
| Providers and protocols | `runifold-providers`, `runifold-provider-testkit`, `runifold-mcp` |
| Retrieval | `runifold-retrieval`, `runifold-retrieval-text`, `runifold-retrieval-qdrant`, `runifold-retrieval-pgvector` |
| Persistence | `runifold-store-sqlite`, `runifold-store-postgres` |
| Observability and operations | `runifold-observability-otel`, `runifold-ops`, `runifold-cli` |
| Testing and evaluation | `runifold-testkit`, `runifold-eval-cli` |

Planned edge crates include A2A transports and additional persistence
backends. They remain outside the kernel until their dependency and protocol
boundaries are clear.

## Contributing

Start with the [project charter](docs/CHARTER.md), then use the
[testing guide](docs/TESTING.md) for the relevant reliability boundary.
Changes to public behavior should update the corresponding RFC and
[changelog](CHANGELOG.md). Release maintainers should follow the
[release runbook](docs/RELEASING.md).

Issues and proposals are welcome in the
[GitHub repository](https://github.com/ZYX121212/runifold/issues).

## License

Licensed under either [Apache License 2.0](LICENSE-APACHE) or
[MIT](LICENSE-MIT), at your option.
