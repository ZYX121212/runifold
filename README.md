# Runifold

Runifold is a typed, observable, cancellable, and budget-aware runtime kernel
for models, tools, agents, and workflows in Rust.

The name combines **run** with **manifold**: models, tools, agents, and flows
are different surfaces over the same execution space.

## Status

Runifold is pre-alpha. The implemented foundation includes:

- stable run identity and causal event envelopes;
- hierarchical cancellation and deadlines;
- explicit capability grants;
- budget accounting;
- effect descriptions;
- in-memory journaling and deterministic test helpers;
- provider-neutral multimodal messages and model requests;
- explicit capability and degradation semantics;
- strict streaming content-block accumulation;
- lossless provider-event escape hatches;
- an object-safe asynchronous model invocation boundary;
- wakeable hierarchical cancellation;
- a queue-backed scripted model for deterministic invocation tests;
- Responses and Chat Completions adapters for OpenAI, Ark, Qwen, and custom endpoints;
- a native Anthropic Messages adapter with text, images, tools, thinking, and strict SSE decoding;
- native Gemini GenerateContent SSE and Ollama chat NDJSON adapters;
- an offline real-HTTP provider cassette with delays, disconnects, and credential redaction;
- concurrent real-HTTP provider stress tests with timeout, offline, and truncation classification;
- optional OpenTelemetry GenAI spans and metrics for models, agents, tools, and workflows;
- capability-safe MCP 2025-11-25 Tools, Resources, Prompts, pagination, Resource Templates, subscriptions, Completion, and client-owned Sampling over in-process, stdio, and Streamable HTTP transports;
- a capability-gated, object-safe tool runtime and deterministic registry;
- a bounded Model → Tool → Model agent loop;
- capability-gated Agent → Gateway → Agent delegation with child-run authority attenuation;
- composable around-middleware and asynchronous policies for Gateway governance;
- opt-in structured execution journals with cross-run causal links;
- revision-safe Agent checkpoints with explicit ambiguous-retry policy;
- capability-gated write-ahead effects with idempotent replay and conservative recovery;
- Tool and Agent delegation execution coordinated through the write-ahead effect boundary;
- optional durable SQLite stores for effects, checkpoints, and journals;
- cross-process crash recovery proving completed Tool effects are not re-executed;
- fluent Agent construction across OpenAI, Ark, Qwen, and custom compatible clients;
- typed async Rust Tools with generated JSON Schemas and an attribute macro;
- host-only Tool state injection and explicit application-error normalization;
- backpressured Agent streaming across model, Tool, delegation, usage, and terminal events;
- Rust-type-derived structured outputs with local fail-closed decoding;
- deterministic multi-provider routing with explicit, stream-safe fallback authority;
- optional per-route circuit breakers with deterministic half-open recovery;
- bounded same-route retry with exponential backoff, jitter, `Retry-After`, and deadline truncation;
- durable sequential and conditional workflows with explicit per-step authority;
- Agent-backed workflow steps, causal child runs, and conservative checkpoint recovery;
- atomic scoped budget reservations for concurrent child runs;
- durable fail-fast parallel workflows with stable joins and per-branch recovery;
- side-effect-safe first-success races with fair start, conservative losing-budget accounting, and durable winners.

See [RFC 0003](docs/rfcs/0003-model-invocation-boundary.md) for the model
invocation boundary and [RFC 0008](docs/rfcs/0008-agent-delegation-gateway.md)
for delegation semantics. Gateway governance is specified in
[RFC 0009](docs/rfcs/0009-gateway-middleware.md), and run-tree events in
[RFC 0010](docs/rfcs/0010-run-observability.md). Recovery semantics are in
[RFC 0011](docs/rfcs/0011-agent-checkpoints.md), effect recovery in
[RFC 0012](docs/rfcs/0012-write-ahead-effects.md), and callable integration in
[RFC 0013](docs/rfcs/0013-agent-callable-effects.md). The SQLite adapter is
specified in [RFC 0014](docs/rfcs/0014-sqlite-stores.md), and the ergonomic
Agent surface in [RFC 0015](docs/rfcs/0015-agent-builder.md). Typed functions
are specified in [RFC 0016](docs/rfcs/0016-typed-function-tools.md), with
state and error boundaries in
[RFC 0017](docs/rfcs/0017-tool-state-and-errors.md). Agent streaming is
specified in [RFC 0018](docs/rfcs/0018-agent-streaming.md), and typed
structured output in
[RFC 0019](docs/rfcs/0019-typed-structured-output.md). Safe model routing is
specified in [RFC 0020](docs/rfcs/0020-model-router.md), and circuit-breaker
semantics in [RFC 0021](docs/rfcs/0021-router-circuit-breaker.md). Retry and
backoff are specified in
[RFC 0022](docs/rfcs/0022-router-retry-backoff.md). Durable orchestration is
specified in
[RFC 0023](docs/rfcs/0023-durable-workflow-orchestration.md), with budget
reservation and parallel recovery in
[RFC 0024](docs/rfcs/0024-budget-reservations-and-parallel-workflows.md), and
safe first-success competition in
[RFC 0025](docs/rfcs/0025-safe-first-success-race.md). Native Anthropic
translation and provider conformance testing are specified in
[RFC 0026](docs/rfcs/0026-anthropic-and-provider-testkit.md).
Gemini and Ollama native protocol boundaries are specified in
[RFC 0027](docs/rfcs/0027-gemini-and-ollama-providers.md).
Provider transport reliability and concurrency contracts are specified in
[RFC 0028](docs/rfcs/0028-provider-transport-reliability.md).
OpenTelemetry GenAI signal, privacy, and dependency boundaries are specified in
[RFC 0029](docs/rfcs/0029-opentelemetry-genai-observability.md).
Native MCP Tools and stdio transport semantics are specified in
[RFC 0030](docs/rfcs/0030-mcp-tools-edge.md). Streamable HTTP sessions,
authentication, SSE resumption, and network failure semantics are specified in
[RFC 0031](docs/rfcs/0031-mcp-streamable-http.md).
Capability-safe Resources and user-controlled Prompts are specified in
[RFC 0032](docs/rfcs/0032-mcp-resources-prompts.md).
Session-bound pagination, Resource Templates, subscriptions, and Completion are
specified in [RFC 0033](docs/rfcs/0033-mcp-dynamic-context.md).
Client-owned model selection, dual approval, resource limits, and bidirectional
Sampling transports are specified in
[RFC 0034](docs/rfcs/0034-mcp-sampling.md).
Versioned Agent evaluation datasets, async scorers, Run/Trace correlation, and
relative regression gates are specified in
[RFC 0035](docs/rfcs/0035-agent-quality-evaluation.md).
JSONL execution, external Candidate protocol, CI exit codes, and
JSON/JUnit/Markdown reporting are specified in
[RFC 0036](docs/rfcs/0036-evaluation-cli-ci.md).
Seeded repetitions, confidence gates, deterministic sharding, resumable
checkpoints, and evidence-validating shard merge are specified in
[RFC 0037](docs/rfcs/0037-reproducible-evaluation-experiments.md).
Case-level recovery, latency/Token/cost evidence, resource budgets, and the
reusable GitHub Actions gate are specified in
[RFC 0038](docs/rfcs/0038-evaluation-resources-case-recovery-ci.md).
Release integrity, MSRV, SemVer, supply-chain policy, SBOMs, and controlled
crates.io publication are specified in
[RFC 0039](docs/rfcs/0039-release-integrity-and-supply-chain.md); maintainers
should follow the [release runbook](docs/RELEASING.md).

Enable the `otel` feature to decorate model calls and durable run events:

```rust,ignore
use std::sync::Arc;
use runifold::{InMemoryJournal, Model, otel::OtelRuntime};

let telemetry = OtelRuntime::new();
let observed_model: Arc<dyn Model> = Arc::new(telemetry.model(provider));
let observed_journal = Arc::new(telemetry.journal(InMemoryJournal::new()));
```

Models and journals created from the same `OtelRuntime` share causal Run
correlation, so Turn, model, Tool, delegation, child-Agent, Router fallback,
and scoped MCP Sampling operations appear in one causal trace. The runtime
uses global OpenTelemetry providers by default and also supports explicit
tracer and meter injection. Prompt, response, Tool definition, Tool argument,
Tool result, and exception-message capture is disabled by default.
Low-cardinality operational metrics cover Agent and Turn duration, usage and
cost, errors and budget exhaustion, plus MCP Sampling requests, duration, and
failures. Run, invocation, call, and entity identities remain trace-only.
Recommended histogram buckets are enabled by default. Versioned Prometheus
recording/alert rules and a Grafana dashboard are embedded as
`otel::slo::PROMETHEUS_RULES` and `otel::slo::GRAFANA_DASHBOARD`; see the
[operations runbook](docs/operations-slo.md).

Offline quality evaluation stays separate from operational telemetry:

```rust,ignore
use runifold_testkit::{
    EvaluationCase, EvaluationDataset, EvaluationOutput, EvaluationRunner,
    JsonExactMatchScorer, RegressionPolicy,
};

let dataset = EvaluationDataset::new(
    "support-answers",
    "2026-07-26",
    vec![
        EvaluationCase::new("refund-policy", serde_json::json!("question"))?
            .with_expected(serde_json::json!("approved answer")),
    ],
)?;
let runner = EvaluationRunner::new(|case: EvaluationCase| async move {
    let output = run_candidate(case.input()).await?;
    Ok(EvaluationOutput::new(output.value).with_run_id(output.run_id))
})
.with_scorer(JsonExactMatchScorer);

let candidate = runner.run(&dataset, "prompt-v2").await?;
let comparison = candidate.compare(
    &baseline,
    &RegressionPolicy::new(0.02, 0.05, 0.0)?,
)?;
assert!(comparison.passed);
```

Reports retain Case and Run IDs for trace lookup but omit raw inputs,
references, outputs, prompts, and transcripts.
Built-in `TokenOverlapScorer` and weighted `JsonRuleScorer` cover deterministic
checks. `ModelJudgeScorer` uses any canonical Provider or Router with strict
structured output and local validation. `FileEvaluationRepository` persists
immutable, versioned datasets and reports with traversal-safe paths and
conflict detection.

Run the same gate from CI without linking an application into the CLI:

```bash
runifold-eval run \
  --dataset evals/support.jsonl \
  --dataset-name support \
  --dataset-version 2026-07-26 \
  --candidate-version prompt-v2 \
  --output artifacts/evaluation.json \
  --junit artifacts/evaluation.xml \
  --markdown artifacts/evaluation.md \
  -- ./target/release/my-eval-candidate
```

The Candidate receives only Case ID, input, and tags—never the reference
answer. Exit code `0` passes, `1` means CLI configuration or artifact failure,
and `2` means a Candidate, scorer, quality, or regression gate failed.

Measure sampling variance and resume interrupted evaluations with:

```bash
runifold-eval experiment \
  --dataset evals/support.jsonl \
  --dataset-name support \
  --dataset-version 2026-07-27 \
  --candidate-version prompt-v3 \
  --samples 10 \
  --seed 42 \
  --cache-dir .runifold/eval-cache \
  --min-confidence-lower-bound 0.85 \
  --max-flaky-case-rate 0.02 \
  --max-p95-latency-ms 3000 \
  --max-total-tokens 250000 \
  --max-total-cost-usd 10.00 \
  --output artifacts/experiment.json \
  --junit artifacts/experiment.xml \
  --markdown artifacts/experiment.md \
  -- ./target/release/my-eval-candidate
```

The Candidate receives a stable `sample_index` and per-case `seed`. It may
return `input_tokens`, `output_tokens`, and `cost_usd`; host latency is measured
independently. Every completed Case and Sample is checkpointed under a
fingerprint of the dataset content, Candidate command, scorer, seed, shard, and
process limits. Corrupt or
configuration-mismatched cache entries fail closed. Use `--shard-index` and
`--shard-count` for distributed execution, then `runifold-eval merge` to
validate and combine every shard before applying the final confidence and
flakiness gates. Resource gates fail closed when required usage is missing.

The same gate is available to same-repository GitHub Actions callers:

```yaml
jobs:
  evaluate:
    uses: ./.github/workflows/runifold-evaluation.yml
    with:
      dataset: evals/support.jsonl
      dataset-name: support
      dataset-version: 2026-07-27
      candidate-version: prompt-v4
      candidate-build-command: cargo build --locked --release -p my-eval-candidate
      candidate-command: ./target/release/my-eval-candidate
      samples: 10
      max-p95-latency-ms: "3000"
      max-total-tokens: "250000"
      max-total-cost-usd: "10.00"
    secrets:
      openai-api-key: ${{ secrets.OPENAI_API_KEY }}
```

Enable the `mcp` feature to expose authorized local Tools or import explicitly
classified remote Tools:

```rust,ignore
use std::sync::Arc;
use runifold::mcp::{
    Implementation, McpClient, McpClientConfig, McpRemoteTool, McpServer,
    RemoteToolPolicy,
};

let session = McpServer::new(
    Arc::new(local_tools),
    server_authority,
    Implementation::new("my-server", "0.1.0"),
).session();

let client = McpClient::new(
    Arc::new(session),
    McpClientConfig::new(Implementation::new("my-client", "0.1.0")),
);
client.initialize().await?;
let discovered = client.list_tools().await?;

let remote = McpRemoteTool::new(
    client,
    discovered.into_iter().next().ok_or("no tools")?,
    RemoteToolPolicy::new(
        runifold::core::EffectClass::ReadOnly,
        runifold::core::RiskLevel::Medium,
    ),
)?;
```

MCP annotations remain untrusted. The host must select effect and risk policy,
and an MCP server lists only capabilities granted by its `RunContext`.

The same client works over a production HTTP boundary:

```rust,ignore
use std::sync::Arc;
use runifold::mcp::{
    Implementation, McpClient, McpClientConfig, StreamableHttpTransport,
};

let transport = Arc::new(StreamableHttpTransport::new(
    "http://127.0.0.1:3000/mcp",
)?);
let client = McpClient::new(
    transport,
    McpClientConfig::new(Implementation::new("my-client", "0.1.0")),
);
client.initialize().await?;
let tools = client.list_tools().await?;
```

`StreamableHttpTransport` accepts JSON and SSE responses, retains opaque
session state, and supports resumable server notifications. It never retries a
request implicitly: an expired session is returned as
`McpError::SessionExpired`, so a host cannot accidentally duplicate a Tool
effect. `McpHttpServerConfig` rejects unknown browser origins by default and
can require a bearer `HttpAuthorizer`. Public deployments should terminate TLS
at the process or a trusted reverse proxy.

Resources and Prompts use the same negotiated client:

```rust,ignore
use std::collections::BTreeMap;

let resources = client.list_resources().await?;
let templates = client.list_resource_templates().await?;
let content = client.read_resource(&resources[0].uri).await?;

let prompts = client.list_prompts().await?;
let rendered = client
    .get_prompt(
        &prompts[0].name,
        BTreeMap::from([("code".into(), "fn main() {}".into())]),
    )
    .await?;
```

Resource and Prompt registries repeat authorization at execution time and
create child runs containing only the selected capability. Prompt results are
returned to the host and are never inserted into a model request
automatically.

All list methods follow opaque pagination cursors automatically; matching
`*_page` methods expose one page when a host needs incremental discovery.
Resource updates require an explicit per-session subscription and are delivered
through `client.notifications()`.

Basic Sampling lets a server request a host-controlled model call without
receiving model credentials or choosing the final model:

```rust,ignore
use std::sync::Arc;
use runifold::mcp::{
    CreateMessageParams, FixedSamplingModel, McpClientConfig, ModelSamplingProvider,
    SamplingMessage, SamplingPolicy, SamplingService,
};
use runifold::model::ModelRef;

let provider = Arc::new(ModelSamplingProvider::new(
    host_model,
    Arc::new(FixedSamplingModel::new(ModelRef::new("anthropic", "claude-sonnet"))),
));
let sampling = Arc::new(SamplingService::new(
    host_approver,
    provider,
    SamplingPolicy::default(),
));
let config = McpClientConfig::new(client_implementation).with_sampling(sampling);

let result = initialized_session
    .sampling_client()
    .create_message(CreateMessageParams::new(
        vec![SamplingMessage::user_text("Summarize the approved input")],
        512,
    ))
    .await?;
```

`SamplingApprover` reviews both the request and the generated response. The
client advertises only basic Sampling: Tool-enabled Sampling and ambient
context inclusion remain fail-closed until their separate authority and loop
contracts are implemented.

Enable the first provider edge with the `openai` feature:

```rust,no_run
use runifold::{
    Budget, BudgetTracker, RunContext,
    openai::{OpenAiAgentExt, OpenAiClient, OpenAiConfig},
};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let config = OpenAiConfig::new(std::env::var("OPENAI_API_KEY")?)?;
let agent = OpenAiClient::new(config)
    .agent("assistant", "gpt-5")
    .system("Answer precisely and expose uncertainty.")
    .max_turns(8)
    .build()?;
let run = RunContext::root(
    BudgetTracker::new(Budget::default()),
    agent.callable_capabilities(),
);
let response = agent.run("Why is durable execution useful?", &run).await?;
# let _ = response;
# Ok(())
# }
```

The library does not read credentials implicitly. Applications decide how
secrets enter their process.

Anthropic uses its native Messages protocol behind the `anthropic` feature:

```rust,no_run
use runifold::anthropic::{AnthropicAgentExt, AnthropicClient, AnthropicConfig};

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let config = AnthropicConfig::new(std::env::var("ANTHROPIC_API_KEY")?)?;
let agent = AnthropicClient::new(config)
    .agent("researcher", "claude-sonnet-4-5")
    .system("Separate evidence from inference.")
    .build()?;
# let _ = agent;
# Ok(())
# }
```

Runifold keeps error responsibilities at the correct boundary:

- public library APIs expose typed `thiserror` errors that callers can match,
  serialize where supported, and inspect for retry safety;
- application and example code may aggregate those errors with
  `anyhow::Result` and add operational context;
- model, Tool, Agent, Gateway, checkpoint, effect, and store failures are not
  erased into opaque strings inside the library.

See `crates/runifold/examples/error_context.rs` for the application-boundary
pattern.

Terminal model output can be constrained and decoded as a Rust type:

```rust,ignore
use runifold::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
struct ResearchAnswer {
    summary: String,
    confidence: f64,
}

let agent = client
    .agent("researcher", "your-model")
    .build_structured::<ResearchAnswer>("research_answer")?;

let typed = agent
    .run("Assess the evidence", &run)
    .await?;

println!("{} ({})", typed.output.summary, typed.output.confidence);
```

The Rust type generates the provider-facing JSON Schema, but provider
acceptance is never treated as proof. Runifold assembles only canonical text,
fails closed on refusals, and deserializes locally before returning the typed
value. The full response, transcript, counters, and usage remain available in
`typed.outcome`.

Typed Tools are ordinary async Rust functions:

```rust,ignore
use runifold::{JsonSchema, ToolContext, ToolError};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JsonSchema)]
struct AddInput {
    left: i64,
    right: i64,
}

#[derive(JsonSchema, Serialize)]
struct AddOutput {
    sum: i64,
}

#[runifold::tool(
    description = "Add two signed integers",
    effect = "pure",
    risk = "low"
)]
async fn add(
    input: AddInput,
    _context: ToolContext,
) -> Result<AddOutput, ToolError> {
    Ok(AddOutput {
        sum: input.left + input.right,
    })
}

let agent = client
    .agent("assistant", "gpt-5")
    .tool(add_tool())
    .build()?;
```

The macro generates `add_tool()`. Input is validated before the handler runs,
output is serialized after it succeeds, and both JSON Schemas become part of
the Tool capability contract. `FunctionTool` exposes the same mechanism
without using the attribute macro.

Application services can be injected without exposing them to the model:

```rust,ignore
use std::sync::Arc;
use runifold::{IntoToolError, State, ToolContext, ToolError};

#[runifold::tool(
    description = "Search the application index",
    effect = "read_only",
    risk = "low"
)]
async fn search(
    state: State<SearchService>,
    input: SearchInput,
    context: ToolContext,
) -> Result<SearchOutput, SearchError> {
    state.search(input, context).await
}

impl IntoToolError for SearchError {
    fn into_tool_error(self) -> ToolError {
        // Select an intentionally safe category, message, and retry policy.
        ToolError::local(
            runifold::ToolErrorKind::Execution,
            "search is temporarily unavailable",
        )
    }
}

let tool = search_tool(Arc::new(SearchService::new()));
```

`State<T>` never enters the model schema, transcript, or Effect input.
Application errors are not converted through `Display` automatically because
their text may contain credentials, queries, or internal implementation data.

The same Agent loop can be consumed as a backpressured event stream:

```rust,ignore
use futures_util::StreamExt;
use runifold::{AgentStreamEvent, model::ModelStreamEvent};

let mut events = agent.stream("Explain the recovery design", &run);
while let Some(event) = events.next().await {
    match event? {
        AgentStreamEvent::Model {
            event: ModelStreamEvent::TextDelta { text, .. },
            ..
        } => print!("{text}"),
        AgentStreamEvent::CallableStarted { kind, call, .. } => {
            eprintln!("{kind:?} {} started", call.name);
        }
        AgentStreamEvent::UsageUpdated { usage } => {
            eprintln!("tokens: {}", usage.tokens);
        }
        AgentStreamEvent::Completed { outcome } => {
            eprintln!("completed in {} turns", outcome.turns);
        }
        _ => {}
    }
}
```

Every visible event introduces a poll boundary. A slow consumer therefore
slows Agent execution instead of growing an unbounded event buffer.

Provider identity and wire protocol are configured independently:

```rust,no_run
use runifold::openai::{OpenAiConfig, OpenAiWireProtocol};

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let ark = OpenAiConfig::ark(std::env::var("ARK_API_KEY")?)?;

let qwen = OpenAiConfig::qwen(
    std::env::var("DASHSCOPE_API_KEY")?,
    "https://dashscope.aliyuncs.com/compatible-mode/v1",
    OpenAiWireProtocol::ChatCompletions,
)?;

let custom = OpenAiConfig::custom(
    "my-gateway",
    "https://llm.example.com/v1",
    OpenAiWireProtocol::ChatCompletions,
)?;
# let _ = (ark, qwen, custom);
# Ok(())
# }
```

Multiple physical models can sit behind one logical model identity:

```rust,ignore
use std::{sync::Arc, time::Duration};
use runifold::{
    Agent, CircuitBreakerConfig, ModelFallbackPolicy, ModelRef, ModelRetryPolicy,
    ModelRouter, RetryJitter,
    model::ModelErrorKind,
};

let logical = ModelRef::new("router", "assistant");
let router = ModelRouter::builder(logical.clone())
    .route(
        "openai-primary",
        Arc::new(openai_client),
        ModelRef::new("openai", "your-primary-model"),
    )
    .route(
        "ark-backup",
        Arc::new(ark_client),
        ModelRef::new("ark", "your-backup-model"),
    )
    .fallback_policy(
        ModelFallbackPolicy::safe_only()
            .allow_unknown(ModelErrorKind::Transport)
            .allow_unknown(ModelErrorKind::Provider),
    )
    .circuit_breaker(CircuitBreakerConfig::new(
        3,
        Duration::from_secs(30),
    )?)
    .retry_policy(
        ModelRetryPolicy::exponential(
            3,
            Duration::from_millis(100),
            Duration::from_secs(2),
            2,
        )?
        .jitter(RetryJitter::Full)
        .allow_unknown(ModelErrorKind::Transport),
    )
    .build()?;

let agent = Agent::builder("assistant", Arc::new(router), logical)
    .build()?;
```

The default policy only falls back for errors explicitly marked retry-safe.
Allowing an error kind with unknown safety is deliberate authority to risk a
second provider charge. Cancellation never falls back, and after the first
canonical stream event Runifold locks the selected route to prevent duplicate
visible output. The selected route and safe summaries of earlier failures are
retained as canonical provider events.

Circuit breakers are opt-in and independent per physical route. After the
configured number of consecutive counted failures, a route is skipped until
its cooldown expires. Exactly one request becomes the half-open recovery
probe; all concurrent requests continue to other routes. Terminal success
closes the circuit, while a failed or abandoned probe reopens it.
`router.route_health()` returns immutable health snapshots suitable for
metrics and readiness diagnostics.

Retry is also opt-in. `max_attempts` includes the initial call, and every retry
gets a distinct invocation identity. The effective wait is the greater of
local backoff and provider `Retry-After`. Runifold stops before sleeping when
the delay would cross the invocation deadline, observes cancellation during
the wait, and never retries after the first canonical stream event.

Child agents are exposed through an explicit gateway route. The route itself
is an `Agent` capability, while the child receives only the configured subset
of the parent's capabilities:

```rust,no_run
use std::sync::Arc;

use runifold::{
    Agent, AgentDescriptor, AgentGateway, AgentRoute, Model, ModelRef,
};

# fn example(
#     parent_model: Arc<dyn Model>,
#     child_model: Arc<dyn Model>,
# ) -> Result<(), Box<dyn std::error::Error>> {
let child = Arc::new(Agent::new(
    "researcher",
    child_model,
    ModelRef::new("qwen", "your-child-model"),
));
let descriptor = AgentDescriptor::new(
    "ask_researcher",
    "Delegate focused research to the researcher agent",
);
let mut gateway = AgentGateway::new();
gateway.register(AgentRoute::new(descriptor.clone(), child))?;

// Grant `descriptor.capability()` to the parent RunContext before execution.
let parent = Agent::new(
    "coordinator",
    parent_model,
    ModelRef::new("ark", "your-parent-model"),
)
.agents(gateway);
# let _ = (parent, descriptor);
# Ok(())
# }
```

Gateway middleware uses an around-call boundary. It may inspect or transform
input, deny execution, observe results, or explicitly retry:

```rust,ignore
impl GatewayMiddleware for AuditLayer {
    fn handle<'a>(
        &'a self,
        request: DelegationRequest,
        next: GatewayNext<'a>,
    ) -> GatewayFuture<'a, Result<AgentOutcome, GatewayError>> {
        Box::pin(async move {
            self.record_started(&request);
            let result = next.run(request).await;
            self.record_finished(&result);
            result
        })
    }
}
```

Route identity and parent authority cannot be replaced by middleware. Every
call to `next` re-enters the protected lifecycle, capability, authority, depth,
and budget boundary.

Attach a journal to the root Run to observe the complete execution tree:

```rust,no_run
use std::sync::Arc;

use runifold::{
    Budget, BudgetTracker, CapabilitySet, InMemoryJournal, RunContext,
};

let journal = InMemoryJournal::new();
let run = RunContext::root(
    BudgetTracker::new(Budget::default()),
    CapabilitySet::new(),
)
.with_journal(Arc::new(journal.clone()));

// Run an Agent with `run`, then inspect or export `journal.events()`.
# let _ = run;
```

Agent events contain identities, state transitions, normalized error kinds,
usage, and counters. Prompt text, tool arguments, and output bodies are not
recorded by default.

Checkpointed execution persists the canonical transcript and local counters:

```rust,no_run
use std::sync::Arc;

use runifold::{
    AgentCheckpoint, InMemoryCheckpointStore, ResumePolicy,
};

# async fn example(
#     agent: &runifold::Agent,
#     run: &runifold::RunContext,
# ) -> Result<(), runifold::AgentError> {
let checkpoint = AgentCheckpoint::new(Arc::new(
    InMemoryCheckpointStore::new(),
));

let outcome = agent
    .run_checkpointed("perform the task", run, &checkpoint)
    .await?;

// Reopening a completed checkpoint is idempotent and performs no model call.
let same_outcome = agent
    .resume(&checkpoint, run, ResumePolicy::RejectAmbiguous)
    .await?;
# let _ = (outcome, same_outcome);
# Ok(())
# }
```

If interruption occurs during a model/tool/delegation turn, normal resume
returns `AmbiguousCheckpoint`. Retrying that turn requires the explicit
`RetryInterruptedTurn` policy because it may repeat model cost. Completed Tool
and delegation effects are replayed without handler execution when the Agent
uses the same effect store.

External effects can use a finer-grained write-ahead protocol:

```rust,ignore
let executor = EffectExecutor::new(Arc::new(
    InMemoryEffectStore::new(),
));

let outcome = executor
    .execute(
        effect_request,
        &run,
        &handler,
        EffectRecoveryPolicy::RejectAmbiguous,
    )
    .await?;
```

Completed effects are replayed from durable state. A `Started` effect is
retried only with explicit `RetrySafe` policy and only when it is Pure,
ReadOnly, or an IdempotentWrite with an idempotency key.

Agents create their own in-memory executor by default. Inject a shared,
durable executor with `Agent::effect_executor` when recovery must survive
process restarts. Callable keys are derived from execution identity, Agent
name, turn, and call position; a different request at the same position is
rejected instead of replaying the wrong result.

For durable local execution, enable `sqlite` (system SQLite) or
`sqlite-bundled`, then share one store across the three persistence roles:

```rust,ignore
use std::sync::Arc;

use runifold::{
    AgentCheckpoint, EffectExecutor,
    sqlite::SqliteStore,
};

let store = Arc::new(SqliteStore::open("runifold.db")?);
let checkpoint = AgentCheckpoint::new(store.clone());
let executor = EffectExecutor::new(store.clone());
let run = run.with_journal(store);
let agent = agent.effect_executor(executor);
```

SQLite is an optional local adapter, not part of the runtime kernel. A service
can implement the same traits with PostgreSQL or another transactional store.

## Design principles

1. Every execution is a `Run`.
2. Every fact is an `Event`.
3. Every external action is an `Effect`.
4. Every permission is an explicit `Capability`.
5. No silent degradation or information loss.
6. Parent and child runs use structured concurrency.
7. Policies are separate from mechanisms.
8. External protocols are adapters, not core types.
9. Testability is a product feature.
10. Stable kernel, replaceable edges.

See [the project charter](docs/CHARTER.md) and
[RFC 0001](docs/rfcs/0001-runtime-kernel.md).

## Workspace

| Crate | Purpose |
|---|---|
| `runifold` | Ergonomic public facade |
| `runifold-agent` | Bounded model-tool loop, capability-gated delegation, middleware governance, and canonical transcript |
| `runifold-core` | Run, event, effect, capability, budget, and cancellation primitives |
| `runifold-effect` | Write-ahead effects, idempotency, recovery policy, and durable result replay |
| `runifold-eval-cli` | JSONL evaluation runner, external Candidate protocol, and CI quality gates |
| `runifold-model` | Provider-neutral model requests, content, capabilities, and stream accumulation |
| `runifold-macros` | Attribute macros for typed async Rust Tools |
| `runifold-mcp` | Capability-safe MCP Tools, Resources, Templates, Prompts, Completion, Sampling, stdio, and Streamable HTTP |
| `runifold-observability-otel` | Optional OpenTelemetry GenAI spans and metrics |
| `runifold-provider-anthropic` | Native Anthropic Messages requests and semantic SSE streams |
| `runifold-provider-gemini` | Native Gemini GenerateContent requests and SSE responses |
| `runifold-provider-ollama` | Native Ollama chat requests, NDJSON, local models, and thinking |
| `runifold-provider-openai` | Responses and Chat Completions for OpenAI-compatible providers |
| `runifold-provider-testkit` | Offline real-HTTP cassettes, protocol assertions, delays, and disconnect injection |
| `runifold-store-sqlite` | Optional durable effects, checkpoints, and journals in one SQLite database |
| `runifold-testkit` | Deterministic runtime helpers, quality datasets, async scorers, and regression gates |
| `runifold-tool` | Tool descriptors, capability gating, registry, and execution |

Planned edge crates include A2A transports and additional persistence backends.

## License

Licensed under either Apache-2.0 or MIT, at your option.
