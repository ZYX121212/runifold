# Runifold

Runifold is a typed, observable, cancellable, and budget-aware runtime kernel
for models, tools, agents, and workflows in Rust.

The name combines **run** with **manifold**: models, tools, agents, and flows
are different surfaces over the same execution space.

## Quickstart

Add the facade and the providers your application needs:

```console
cargo add runifold
cargo add runifold-providers --features openai
```

The ergonomic path automatically creates a root run with authority limited to
the Tool and child-Agent capabilities explicitly registered on the Agent:

```rust,no_run
use runifold::ProviderModelExt;
use runifold_providers::openai::OpenAiClient;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let runtime = OpenAiClient::from_api_key(std::env::var("OPENAI_API_KEY")?)?
    .runtime("gpt-5")?;
let agent = runtime
    .agent("assistant")
    .system("Answer precisely and expose uncertainty.");

let answer = agent
    .prompt_text("Why is durable execution useful?")
    .await?;
# let _ = answer;
# Ok(())
# }
```

`ProviderRuntime` is long-lived application state. Construct it once during
startup and clone it into request handlers. Clones share retry and
circuit-breaker state; rebuilding it for every request resets route health.
This includes a single physical route: an open single-route circuit fails fast
until its cooldown probe instead of silently creating a second health scope.

Tools can return ordered text, images, audio, documents, resource links, and a
separate structured value without flattening everything into a JSON string.
For large or durable media, configure one shared artifact store on the Agent:

```rust,ignore
use std::sync::Arc;
use runifold::{ArtifactScope, ArtifactStore, ProviderModelExt, sqlite::SqliteStore};

let store = Arc::new(SqliteStore::open("runifold.db")?);
let artifacts: Arc<dyn ArtifactStore> = store.clone();
let scope = ArtifactScope::parse("tenant.acme")?;
let agent = client
    .runtime("gpt-5")?
    .agent("assistant")
    .artifacts(scope, artifacts);
```

Tools access that store through `ToolContext::artifact_store`, write with a
stable idempotency key, and return `ArtifactRef::media_source()`. Runifold
keeps references in durable transcripts/checkpoints and verifies/resolves the
bytes only at the Provider transport boundary. See
[RFC 0072](docs/rfcs/0072-rich-tool-results-and-artifacts.md).

Applications that only need a low-level model client can omit the Agent,
Effect, Retrieval, Tool, macro, and Workflow crates:

```console
cargo add runifold-model
cargo add runifold-providers --features openai
```

This lightweight configuration contains only the provider-neutral model
contract and the selected protocol adapter. Use `ModelRequest` plus
`Model::invoke` directly. Add the `runifold` facade only when Agent, Tool,
Effect, Retrieval, or Workflow composition is required.

Compatible providers have first-class modules without separate crates:

```console
cargo add runifold
cargo add runifold-providers --features openai
```

```rust,no_run
use runifold::ProviderModelExt;
use runifold_providers::deepseek::client;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let answer = client(std::env::var("DEEPSEEK_API_KEY")?)?
    .runtime("deepseek-reasoner")?
    .agent("reasoner")
    .prompt_text("Why does idempotency matter?")
    .await?;
# let _ = answer;
# Ok(())
# }
```

See the [Provider support matrix](docs/PROVIDERS.md) for native versus
compatible protocols, regional endpoints, and verification levels.
Independent image generation, speech synthesis, and transcription use the
provider-neutral `ImageGenerationModel`, `SpeechModel`, and
`TranscriptionModel` traits. OpenAI implements these under the existing
`openai` protocol feature; no extra modality feature is required. Exact
per-model capability declarations use `ModelCapabilityCatalog`, while unknown
model names retain conservative adapter defaults.
OpenAI-compatible image, speech, and transcription dialects use the separate
exact `OpenAiMediaCapabilityCatalog`; undeclared models receive fail-closed
portable subsets rather than model-name inference.
Repeatable latency, throughput, reliability, and cross-framework comparison
rules, plus the standalone release-mode Rig executor, are documented in the
[benchmarking contract](docs/BENCHMARKING.md).
Production claims, mandatory fault tests, machine-readable evidence, and
explicitly unverified areas are tracked in the
[reliability matrix](docs/RELIABILITY.md).
The provider-neutral facade is compiled for `wasm32-unknown-unknown` on the
declared Rust 1.88 MSRV, with core identity, authority, cancellation, and
budget semantics executed in the mandatory edge-runtime CI gate.
OpenAI-compatible, Anthropic, Gemini and Ollama Agent paths plus native
embeddings are exercised in pinned headless Chrome through real CORS, Fetch,
SSE and NDJSON. Browser deployments must use the documented
[application-gateway credential boundary](docs/EDGE.md).

Use `agent.prompt(...)` when the canonical transcript, usage, warnings, and
provider events matter. Use `agent.run(input, &context)` when the application
must supply a tighter budget, narrower capabilities, a deadline, durable
journaling, or shared run-tree identity.

Static and dynamic grounding use the same Agent path:

```rust,ignore
let agent = client
    .agent("support", "gpt-5")
    .system("Answer only from relevant evidence.")
    .context("Returns are accepted within 30 days.")
    .dynamic_context(5, application_retriever)
    .build()?;

let answer = agent.prompt_text("Can I return an item after two weeks?").await?;
```

`dynamic_context` accepts any provider-neutral `Retriever`, including the
deterministic `InMemoryVectorIndex`. Retrieved documents are labelled as
untrusted user-level data and retain stable document IDs; they can never
become system instructions. The ergonomic prompt path grants only registered
retrievers. An explicit `RunContext` must grant each retriever capability.

Native embedding adapters reuse provider clients rather than configuration
values:

```rust,ignore
use std::sync::Arc;
use runifold::{Document, InMemoryVectorIndex, RetrievalContext};
use runifold_providers::openai::OpenAiClient;

let client = OpenAiClient::from_api_key(std::env::var("OPENAI_API_KEY")?)?;
let embedder = Arc::new(client.embedding_model("text-embedding-3-small")?);
let built = InMemoryVectorIndex::build(
    "product-docs",
    embedder,
    vec![
        Document::new("returns", "Returns are accepted within 30 days.")?,
        Document::new("shipping", "Standard shipping takes three days.")?,
    ],
    RetrievalContext::new(),
)
.await?;

let agent = client
    .agent("support", "gpt-5")
    .dynamic_context(4, built.index)
    .build()?;
```

Gemini and Ollama expose the same `client.embedding_model(...)` path. Index
construction is tagged `RetrievalDocument`; lookup is tagged
`RetrievalQuery`, allowing providers such as Gemini to tune the two sides
correctly. Silent truncation is disabled by default.

For persistent retrieval, select a replaceable vector-store adapter:

```console
cargo add runifold --features qdrant
cargo add runifold-providers --features openai
```

```rust,ignore
use std::sync::Arc;
use runifold::{
    Document, RetrievalContext, VectorRetriever,
    qdrant::{QdrantConfig, QdrantVectorStore},
};
use runifold_providers::openai::OpenAiClient;

let embedder = Arc::new(
    OpenAiClient::from_api_key(api_key)?
        .embedding_model("text-embedding-3-small")?
);
let store = Arc::new(QdrantVectorStore::new(
    QdrantConfig::new("http://localhost:6333")?,
    "product-docs",
)?);
let retriever = VectorRetriever::new("product-docs", embedder, store);

retriever
    .index_documents(documents, RetrievalContext::new())
    .await?;
```

Use the `pgvector` feature and `PgVectorStore` for PostgreSQL. Schema creation
and HNSW index creation are explicit setup operations; lookup and upsert never
perform hidden migrations.

Reranking composes with every Retriever through one provider-neutral boundary:

```rust,ignore
use std::sync::Arc;
use runifold::{RerankingRetriever, Retriever};

let retrieval: Arc<dyn Retriever> = Arc::new(RerankingRetriever::new(
    "product-search",
    vector_retriever,
    reranker,
    4,
)?);
```

The first stage fetches a bounded candidate set; the second stage may only
return unique candidates from that set, with finite scores and attributable
usage. `HybridRetriever` concurrently fuses lexical and vector sources with a
validated weighted reciprocal-rank policy. `runifold-retrieval-text` supplies
bounded UTF-8, Markdown-section, and JSON Lines ingestion plus Unicode-safe
chunks with stable source IDs and character offsets. Cohere v2 implements the
same `Reranker` boundary behind the native `cohere` provider feature.

Retrieval quality can be measured independently of the model and Agent:

```rust,ignore
use runifold_testkit::{RetrievalEvaluationCase, RetrievalEvaluationRunner};

let report = RetrievalEvaluationRunner::new(Arc::new(retriever))
    .run(&cases)
    .await?;

assert!(report.mean_recall_at_k >= 0.90);
assert!(report.mean_reciprocal_rank >= 0.85);
```

## Status

Runifold is pre-alpha. The implemented foundation includes:

- stable run identity and causal event envelopes;
- hierarchical cancellation and deadlines;
- explicit capability grants;
- core-enforced child authority attenuation;
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
- Responses and Chat Completions adapters for OpenAI, Azure OpenAI, Ark, Qwen, DeepSeek,
  OpenRouter, xAI, Groq, Mistral, Together AI, Perplexity Sonar, MiniMax,
  Zhipu AI, SiliconFlow, and custom endpoints;
- a native Anthropic Messages adapter with text, images, tools, thinking, and strict SSE decoding;
- native Gemini GenerateContent SSE and Ollama chat NDJSON adapters;
- native Amazon Bedrock Converse Stream through the AWS SDK, including
  `SigV4`, temporary credentials, Tools, reasoning, usage, cancellation, and deadlines;
- offline real-HTTP Bedrock binary EventStream cassettes covering arbitrary
  frame fragmentation, truncation, deadlines, and concurrent SDK streams;
- offline real-HTTP provider cassettes, including Azure API-key and Entra
  authentication, streaming fragmentation, delays, disconnects, and credential redaction;
- a shared Provider Conformance Kit covering identity, visible/reasoning
  separation, usage, raw events, error kinds, and retry safety;
- a framework-neutral Provider benchmark contract with TTFT, p50/p95/p99
  latency, throughput, reliability evidence, environment metadata, and
  baseline regression gates;
- an isolated release-mode Rig 0.40 comparison executor with equivalent-request
  validation, alternating paired rounds, bootstrap confidence intervals, and
  retained raw JSON evidence;
- concurrent real-HTTP provider stress tests with timeout, offline, and truncation classification;
- optional OpenTelemetry GenAI spans and metrics for models, agents, tools, and workflows;
- capability-safe MCP 2025-11-25 plus the 2026-07-28 stateless core, including
  Tools, Resources, Prompts, pagination, Resource Templates, subscriptions,
  Completion, Sampling, schema-driven `Mcp-Param-*` routing, bounded MRTR,
  authorization-partitioned response caching, durable Tasks backed by
  Runifold workflows, typed `notifications/tasks` state streams, and filtered
  `subscriptions/listen` over in-process, stdio, and Streamable HTTP
  transports;
- a capability-gated, object-safe tool runtime and deterministic registry;
- a bounded Model → Tool → Model agent loop;
- capability-gated Agent → Gateway → Agent delegation with child-run authority attenuation;
- composable around-middleware and asynchronous policies for Gateway governance;
- opt-in structured execution journals with cross-run causal links;
- revision-safe Agent checkpoints with explicit ambiguous-retry policy;
- capability-gated write-ahead effects with idempotent replay and conservative recovery;
- Tool and Agent delegation execution coordinated through the write-ahead effect boundary;
- optional durable SQLite stores for effects, checkpoints, journals, and the
  complete local Workflow control plane;
- cross-process crash recovery proving completed Tool effects are not re-executed;
- fluent Agent construction across OpenAI, Ark, Qwen, and custom compatible clients;
- provider-neutral embeddings, capability-gated Agent retrieval, and a deterministic in-memory vector index;
- native OpenAI-compatible, Gemini, and Ollama batch embedding adapters;
- typed OpenAI-compatible model discovery, bounded multipart file upload, and
  Batch create/inspect/cancel operations with browser-safe Gateway execution;
- typed OpenAI GA Realtime WebSocket sessions with bounded frames and browser
  receive queues, strict lifecycle validation, text, function-call, bounded
  PCM24/PCMU/PCMA audio and transcript deltas, redacted short-lived
  client-secret creation, cancellation/deadlines, and explicit ambiguous
  reconnect classification;
- browser-native OpenAI GA Realtime WebRTC with microphone capture, remote
  audio playback, bounded `oai-events`, direct ephemeral-secret negotiation,
  credential-free Gateway or unified server-side SDP exchange, validated
  STUN/TURN configuration, relay-only policy, observable Peer/ICE state, and
  phase-aware recovery safety verified against pinned coturn and a real relay
  network partition;
- a safety-first Realtime reconnect controller with bounded exponential
  backoff, per-invocation full jitter, cancellation/deadline enforcement,
  fresh credential/SDP negotiation on every factory invocation, redacted
  lifecycle events, fail-closed handling of ambiguous in-flight output, and a
  browser Gateway helper that rebuilds Peer/SDP/DataChannel resources while
  retrying only 408/429/5xx SDP exchange responses;
- a manual, opt-in live OpenAI Realtime canary that mints two short-lived
  client secrets, proves credential and effective-session rotation, validates
  bounded TTL, and emits only credential-free evidence;
- optional Qdrant and PostgreSQL/pgvector storage with one provider-neutral retriever;
- deterministic Recall@K, Precision@K, MRR, nDCG, latency, and usage evaluation;
- typed async Rust Tools with generated JSON Schemas and an attribute macro;
- host-only Tool state injection and explicit application-error normalization;
- backpressured Agent streaming across model, Tool, delegation, usage, and terminal events;
- Rust-type-derived structured outputs with local fail-closed decoding;
- deterministic multi-provider routing with explicit, stream-safe fallback authority;
- optional per-route circuit breakers with deterministic half-open recovery;
- bounded same-route retry with exponential backoff, jitter, `Retry-After`, and deadline truncation;
- one `Model + ProviderModel` integration contract that automatically unlocks
  canonical streaming, resilient routing, Agent construction, budgets,
  OpenTelemetry instrumentation, and durable workflow execution;
- durable sequential and conditional workflows with explicit per-step authority;
- Agent-backed workflow steps, causal child runs, and conservative checkpoint recovery;
- atomic scoped budget reservations for concurrent child runs;
- durable fail-fast parallel workflows with stable joins and per-branch recovery;
- side-effect-safe first-success races with fair start, conservative losing-budget accounting, and durable winners;
- provider-neutral distributed workflow claims with leases, heartbeats, delayed retries, and fencing tokens;
- a definition-registered worker runtime with fenced checkpoints, automatic heartbeat, lease-loss cancellation, crash resume, bounded supervision, graceful drain, and operational metrics;
- lease-free durable timers and idempotent external signals that survive process restarts and early webhook delivery;
- store-authoritative signal-or-timeout races, externally fenced cancellation, and auditable signal dead letters with safe retention.
- durable human review with inspectable interrupt state, typed approve/edit/reject decisions, idempotent delivery, and crash-safe resume;
- immutable checkpoint history with bounded state inspection, idempotent fork/replay, explicit ambiguous-effect authority, and durable lineage;
- typed multi-turn conversations with append-only transcripts, summary-buffer backpressure, bounded windows, and provenance-required cross-session semantic memory;
- tenant-scoped workflow admission with outstanding/concurrent quotas, fair claims, and fail-closed control-plane isolation;
- durable tenant token, cost, duration, turn, tool-call, and delegation budgets with atomic reservation, settlement, and crash recovery.
- cursor-paginated tenant budget audits and identity-safe OpenTelemetry metrics for admission, utilization, reservation age, and recovery forfeiture.
- restart-safe bounded OTel budget projection with named durable cursors, monotonic CAS, explicit at-least-once semantics, and compaction protection for slow consumers.
- fenced terminal Task retention with bounded PostgreSQL cleanup batches and immutable tombstone audit.
- dynamically sharded Task cleanup supervision with database-clock heartbeat, bounded concurrency, health snapshots, and low-cardinality OpenTelemetry metrics.
- governed tombstone lifecycle with legal holds, monotonic archive receipts, independent approval, fenced purge recovery, and immutable deletion evidence.
- fail-closed tenant-scoped governance authorization, authenticated audit actors, idempotent archive delivery, and low-cardinality governance telemetry.
- durable purge approval inboxes with bounded discovery, independent reviewer claims, timeout takeover fencing, and auditable approve/reject decisions.
- optional S3-compatible WORM tombstone archives with pre-signed PUT/HEAD authority, SHA-256 reconciliation, encryption, and Object Lock.
- native SDK-independent S3 SigV4 signing for AWS, MinIO, temporary credentials, and custom path-style endpoints.
- bounded S3 archive I/O with typed failure classes and automatic reconciliation when a commit succeeds but its response is lost.
- exclusively leased budget projection supervisors with database-clock expiry, heartbeat renewal, fencing-token takeover, cancellation-safe release, and low-cardinality lease-loss alerts.
- lock-free live projection health snapshots for readiness and control planes, including lease ownership, catch-up state, last acknowledged cursor, throughput, and failures.
- dynamically discovered multi-tenant budget projection with stable keyset pagination, deterministic no-coordinator sharding, bounded concurrency, and lease-safe rebalancing.

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
The feature-gated provider crate topology and companion-crate threshold
are specified in
[RFC 0052](docs/rfcs/0052-provider-crate-topology.md).
The provider identity contract and automatic resilient runtime composition are
specified in
[RFC 0053](docs/rfcs/0053-provider-runtime-contract.md).
The native Amazon Bedrock SDK boundary is specified in
[RFC 0054](docs/rfcs/0054-amazon-bedrock-provider.md).
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
should follow the [release runbook](docs/RELEASING.md). Provider-neutral
embedding, retrieval authority, untrusted context, and recovery semantics are
specified in [RFC 0040](docs/rfcs/0040-provider-neutral-retrieval.md). Native
embedding adapter behavior is specified in
[RFC 0041](docs/rfcs/0041-native-embedding-providers.md). Replaceable vector
stores and retrieval evaluation are specified in
[RFC 0042](docs/rfcs/0042-vector-stores-and-retrieval-evaluation.md).
Distributed workflow claims, authoritative leases, heartbeats, and fencing are
specified in
[RFC 0043](docs/rfcs/0043-distributed-workflow-leases.md). Worker execution,
fenced checkpoint CAS, and recovery supervision are specified in
[RFC 0044](docs/rfcs/0044-workflow-worker-runtime.md). Lease-free timers,
buffered signals, wake recovery, and idempotency are specified in
[RFC 0045](docs/rfcs/0045-durable-timers-and-signals.md). Deadline races,
external cancellation, and signal lifecycle governance are specified in
[RFC 0046](docs/rfcs/0046-durable-wait-governance.md). Multi-tenant workflow
admission, fairness, and control-plane isolation are specified in
[RFC 0047](docs/rfcs/0047-multi-tenant-workflow-admission.md).
Durable aggregate tenant budget reservation and settlement are specified in
[RFC 0048](docs/rfcs/0048-durable-tenant-budget-ledger.md).
Durable budget audit and low-cardinality telemetry projection are specified in
[RFC 0049](docs/rfcs/0049-tenant-budget-observability.md). Projection leases,
heartbeats, fencing, and continuous supervision are specified in
[RFC 0050](docs/rfcs/0050-budget-projection-supervision.md). Multi-tenant
discovery, deterministic sharding, and bounded projection coordination are
specified in
[RFC 0051](docs/rfcs/0051-multi-tenant-budget-projection-coordination.md).

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
let mode = client.connect().await?;
let tools = client.list_tools().await?;
```

`connect()` discovers the server and prefers the 2026-07-28 stateless request
data plane. Tools, Resources, Prompts, Completion, pagination, per-request
client metadata, result metadata, and the standard HTTP routing headers operate
without an initialization handshake or HTTP session. If discovery identifies a
legacy-only server, Runifold falls back to the finalized 2025-11-25
initialization flow. The returned `McpProtocolMode` makes that choice explicit.

`StreamableHttpTransport` accepts JSON and request-scoped SSE responses. Modern
requests validate mirrored protocol, method, name, and schema-designated Tool
parameter headers against the body. The HTTP client accepts only statically
reachable primitive `x-mcp-header` declarations, excludes an invalid Tool from
discovery, and safely encodes Unicode, whitespace, control characters, and the
Base64 sentinel itself. Legacy mode retains opaque session state and resumable
server notifications. The transport never retries a request implicitly: an
expired legacy session is returned as `McpError::SessionExpired`, so a host
cannot accidentally duplicate a Tool effect. Do not annotate secrets with
`x-mcp-header`, because infrastructure may record HTTP headers.
`McpHttpServerConfig` rejects unknown browser origins by default and can require
a bearer `HttpAuthorizer`. Public deployments should terminate TLS at the
process or a trusted reverse proxy.

Modern server-to-client interaction is explicit and request-scoped.
`McpClient::listen` opens a filtered `subscriptions/listen` stream; the server
acknowledges only supported and authorized notification classes, and every
event is correlated to the listen request. Multiple stdio subscriptions are
demultiplexed independently, while HTTP uses one POST/SSE response per
subscription and allocates no protocol session.

MRTR incomplete results are handled under one total deadline and bounded round
count. Each retry receives a fresh JSON-RPC ID, only the latest keyed
`inputResponses`, and the exact opaque `requestState` returned by the server.
Hosts can install a generic `MrtrInputHandler`; an existing `SamplingService`
automatically resolves `sampling/createMessage`. On the server,
`MrtrToolGate` runs with attenuated Tool authority and must validate any echoed
state before returning `Proceed`. The canonical Tool is invoked only after the
gate proceeds.

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

Client-side Sampling lets a server request a host-controlled model call without
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

`SamplingApprover` reviews both the request and the generated response.
`ModelSamplingProvider` advertises and maps Tool-enabled Sampling, including
Tool declarations, Tool choice, and balanced `tool_use`/`tool_result` history.
Ambient context remains fail-closed unless the host installs a
`SamplingContextProvider`; resolved messages are inserted before review and
model execution. Unknown non-empty MCP input blocks use a versioned visible
envelope, while non-inline model media uses a lossless MCP extension block;
neither is silently discarded.

Long-running Sampling can use the official MCP Tasks augmentation. Installing
an `McpSamplingTaskBackend` with `with_sampling_tasks(...)` advertises
`tasks.requests.sampling.createMessage` and `tasks.cancel`; callers set
`CreateMessageParams::task`, receive `CreateMessageOutcome::Task`, and use
`wait_task`, `get_task`, `task_result`, or `cancel_task`. The backend must make
the Task durable before returning its handle, and recovered results are
revalidated and response-approved against the persisted approved request before
disclosure. With `workflow-tasks`, `WorkflowTaskAdapter` and
`WorkflowSamplingTaskRoute` provide the built-in durable implementation over
SQLite, PostgreSQL, or another `WorkflowStore`. For create-response loss,
configure a private deployment-stable `SamplingTaskIdempotencyNamespace` and
attach a retained UUIDv4/v7 with
`CreateMessageParams::with_task_idempotency_key`. Retries recover the same
server-owned Task, while key reuse with different approved content is rejected.
Approved results and `WorkflowSamplingTaskResult::Error` values survive store
and adapter recreation. Result approval is cross-instance leased using the
store clock: only one reviewer is active, expired owners are fenced, takeover
is crash-safe, and claim/completion records are protected from ordinary signal
compaction. An external human-approval service should still treat the Task ID
as an idempotency key because no local lease can atomically commit an
uncooperative remote side effect.

Add the OpenAI protocol adapter from the provider crate:

```rust,no_run
use runifold::{
    Budget, BudgetTracker, ProviderModelExt, RunContext,
};
use runifold_providers::openai::OpenAiClient;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let agent = OpenAiClient::from_api_key(std::env::var("OPENAI_API_KEY")?)?
    .runtime("gpt-5")?
    .agent("assistant")
    .system("Answer precisely and expose uncertainty.")
    .max_turns(8)
    .build()?;
let answer = agent.prompt_text("Why is durable execution useful?").await?;
# let _ = answer;

// Advanced execution keeps authority and accounting explicit.
let run = RunContext::root(
    BudgetTracker::new(Budget::default()),
    agent.callable_capabilities(),
);
let response = agent.run("Why is durable execution useful?", &run).await?;
# let _ = response;
# Ok(())
# }
```

An Agent can also make successful local Tool use part of its completion
contract instead of relying on a prompt instruction:

```rust,ignore
let agent = client
    .agent("researcher", "model")
    .tool(search)
    .min_successful_tool_calls(3)
    .max_turns(8)
    .build()?;
```

While fewer than three successful calls have completed, Runifold sends
`ToolChoice::Required`. It switches back to `Auto` after the requirement is
satisfied so the model can produce terminal output. Application-error Tool
results, child-Agent delegations, provider-hosted Tools, and results from an
earlier conversation turn do not count. An impossible shared Tool-call budget
and a Provider that terminates early both fail explicitly before an unchecked
answer can escape.

The library does not read credentials implicitly. Applications decide how
secrets enter their process.

Anthropic uses its native Messages protocol behind the provider crate's
`anthropic` feature:

```rust,no_run
use runifold::ProviderModelExt;
use runifold_providers::anthropic::AnthropicClient;

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let agent = AnthropicClient::from_api_key(std::env::var("ANTHROPIC_API_KEY")?)?
    .runtime("claude-sonnet-4-5")?
    .agent("researcher")
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

Applications that need a stable business classification do not need to match
every `AgentError` variant. `AgentError::run_error_kind()` returns the same
normalized `RunErrorKind` used by lifecycle observability,
`AgentError::retry_safety()` preserves explicit retry safety, and
`AgentError::to_run_error()` returns the complete normalized contract.

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
    .completion_requirement(
        runifold::CompletionRequirement::new()
            .max_repairs(2)
            .retry_empty_response(true),
    )
    .build_structured::<ResearchAnswer>("research_answer")?;

let typed = agent
    .run("Assess the evidence", &run)
    .await?;

println!("{} ({})", typed.output.summary, typed.output.confidence);
```

The Rust type generates the provider-facing JSON Schema, but provider
acceptance is never treated as proof. Runifold assembles only canonical text,
fails closed on refusals, and deserializes locally before committing a
completed Agent checkpoint. Invalid or empty terminal candidates can consume
an explicitly bounded repair turn; every repair remains subject to the normal
turn, Token, cost, duration, deadline, and cancellation budgets and never
restarts completed Tool effects. The full response, transcript, counters,
repair count (through `typed.outcome.terminal_repairs()`), and usage remain
available in `typed.outcome`.

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
without using the attribute macro. Implement the object-safe `Tool` trait
directly only for dynamic or non-function capabilities that cannot use this
typed boundary; ordinary business services should use `State<T>` as shown
below instead of hand-writing `Arc<dyn Fn>`, boxed futures, JSON erasure, and a
`ToolDescriptor`.

Functions that return images, audio, documents, resources, or mixed content
use the explicit rich-output mode. The returned `ToolOutput` remains canonical
media instead of being flattened into JSON text:

```rust,ignore
use runifold::{
    ContentPart, JsonSchema, MediaSource, ToolContext, ToolError, ToolOutput,
};
use serde::Deserialize;

#[derive(Deserialize, JsonSchema)]
struct KlineInput {
    symbol: String,
}

#[runifold::tool(
    description = "Return the existing K-line chart for a symbol",
    output = "rich",
    effect = "read_only",
    risk = "low"
)]
async fn kline_chart(
    input: KlineInput,
    _context: ToolContext,
) -> Result<ToolOutput, ToolError> {
    Ok(ToolOutput::rich(vec![
        ContentPart::text(format!("K-line chart for {}", input.symbol)),
        ContentPart::Image {
            source: MediaSource::Url {
                url: "https://example.com/kline.png".into(),
                media_type: Some("image/png".into()),
            },
        },
    ]))
}
```

The constructor API is `FunctionTool::new_rich`. It uses the same input
schema, capability, Effect, risk, cancellation, output-size, Agent, Artifact,
and Provider boundaries as ordinary typed Tools. Call `.output_schema(...)`
when rich content also carries typed `structured_content` that must be
validated.

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
use runifold_providers::openai::{
    OpenAiChatDialect, OpenAiConfig, OpenAiResponsesDialect, OpenAiWireProtocol,
};

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
let openai_style_gateway = OpenAiConfig::custom(
    "openai-style-gateway",
    "https://gateway.example.com/v1",
    OpenAiWireProtocol::ChatCompletions,
)?
.with_chat_dialect(OpenAiChatDialect::OpenAi);
let openai_responses_gateway = OpenAiConfig::custom(
    "openai-responses-gateway",
    "https://responses.example.com/v1",
    OpenAiWireProtocol::Responses,
)?
.with_responses_dialect(OpenAiResponsesDialect::OpenAi);
# let _ = (ark, qwen, custom, openai_style_gateway, openai_responses_gateway);
# Ok(())
# }
```

Concrete adapters publish a reviewed runtime profile, so the ordinary
provider-neutral `runtime` path is configuration-complete:

```rust,no_run
use runifold::ProviderModelExt;
use runifold_providers::ark;

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = ark::client(std::env::var("ARK_API_KEY")?)?;
let runtime = client.runtime("doubao-model")?;
let agent = runtime.agent("assistant");
# let _ = agent;
# Ok(())
# }
```

For OpenAI-compatible Responses, the adapter profile uses Complete delivery as
an atomic Router commit,
sets `parallel_tool_calls=false`, uses `FeaturePolicy::BestEffort` so unknown or
emulated model capabilities proceed with visible warnings, retains the bounded
default retry/circuit policy, and explicitly permits retrying malformed Tool
arguments. Capabilities declared unsupported still fail before transport. That
retry may incur another provider charge, but cannot expose partial output or
execute a Tool from the rejected attempt. Chat Completions defaults to
streaming and also supports explicit Complete delivery for atomic validation;
malformed Tool-call retry remains restricted to Responses. Public OpenAI and
generic compatible endpoints use separate explicit lifecycle and request-field
dialects, so strict OpenAI invariants are not guessed for custom servers.
Applications with a reviewed deployment-specific policy can pass a generic
`ProviderRuntimeProfile` to `runtime_with_profile`; this makes every override
explicit without coupling the runtime facade to an OpenAI-specific policy type.

Standard workload profiles layer over those adapter-owned invariants:

```rust,ignore
use runifold::{BatchProfile, InteractiveProfile, ProviderModelExt};

let interactive = client.clone().runtime_with_preset("model", InteractiveProfile)?;
let batch = client.runtime_with_preset("model", BatchProfile)?;
let audit = batch.capability_audit().await?;
assert!(audit.review_required().all(|entry| !entry.recommendation.is_empty()));
```

`ProductionProfile` is the normal adapter recommendation, `InteractiveProfile`
commits streamed canonical events promptly, and `BatchProfile` validates a
complete response before Router commit. Capability audits are stable,
serializable deployment evidence; they do not guess model support.

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
Build the router once during application startup and reuse it or its clones;
clones share route health, while rebuilding starts with closed circuits.

Retry is also opt-in. `max_attempts` includes the initial call, and every retry
gets a distinct invocation identity. The effective wait is the greater of
local backoff and provider `Retry-After`. Runifold stops before sleeping when
the delay would cross the invocation deadline, observes cancellation during
the wait, and never retries after the first canonical stream event.
Complete delivery moves that commit point after terminal validation: the
Router buffers and validates the canonical response first, then replays its
events. Pre-commit malformed responses can therefore be retried when policy
explicitly authorizes their error kind. Streaming retains the original
first-visible-event commitment rule.

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
let stable_id = "018f6f7e-6f1d-7f2a-9c40-7f4f8f0a3d21".parse()?;
let descriptor = AgentDescriptor::new(
    "ask_researcher",
    "Delegate focused research to the researcher agent",
)
.with_id(stable_id);
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

`AgentDescriptor::new` generates a fresh identity and is appropriate for
ephemeral routes. Applications that persist grants, policies, or audit records
must load a stable `CapabilityId` from configuration or storage and apply it
with `with_id`, as above.

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
The same `SqliteStore` also implements `ConversationStore`. For a turn that
must recover without splitting the transcript from its terminal checkpoint,
use the combined durable entry point:

```rust,ignore
let checkpoint_id = CheckpointId::new();
let turn = agent
    .run_durable_conversation(
        "Continue our Rust design discussion",
        &run,
        store.clone(),
        DurableConversationRequest {
            checkpoint_id,
            conversation_id,
            namespace,
            policy,
        },
    )
    .await?;

// After response loss or process restart, this returns a committed outcome
// without invoking the model again.
let same_turn = agent
    .resume_durable_conversation(
        store,
        checkpoint_id,
        &recovered_run,
        ResumePolicy::RejectAmbiguous,
    )
    .await?;
```

Runifold 0.3.x uses `rusqlite` 0.39 / `libsqlite3-sys` 0.37. It can coexist with
SQLx 0.9, but not SQLx 0.8.x, whose SQLite driver selects the incompatible
`libsqlite3-sys` 0.30 line. Because both native packages declare
`links = "sqlite3"`, applications using SQLx 0.8 must upgrade SQLx or disable
Runifold's `sqlite` and `sqlite-bundled` features.

The combined atomic `DurableConversationStore` implementation is available for
both `SqliteStore` and `PostgresConversationStore`. Each terminal transcript
append and checkpoint compare-and-swap shares one real database transaction.
`PostgresConversationStore` also implements durable checkpoint and Effect CAS,
including capability-scoped idempotency indexing.

Intermediate revisions remain write-ahead checkpoints. The final transcript
append and `Completed` checkpoint revision share one SQLite transaction. An
in-flight external model turn remains explicitly ambiguous and is rejected
unless the caller selects `RetryInterruptedTurn`.

Local and edge Workflow workers can persist their complete control plane in
SQLite without deploying PostgreSQL. `SqliteWorkflowStore` covers queue state,
fenced leases, heartbeats, tenant budgets, durable timers, signals, HITL,
checkpoint history, cancellation, and fork/replay:

```rust,ignore
use std::{sync::Arc, time::Duration};
use runifold::{
    LeaseDuration, WorkerId, WorkflowWorker,
    sqlite::SqliteWorkflowStore,
};

let store = Arc::new(SqliteWorkflowStore::open("runifold.db")?);
let worker = WorkflowWorker::new(
    store,
    registry,
    WorkerId::parse("local-worker")?,
    LeaseDuration::new(Duration::from_secs(30))?,
    Duration::from_secs(10),
)?;
```

SQLite serializes write transactions and is intended for local, desktop,
edge, and low-contention multi-process deployments. Horizontally scaled
workers should use PostgreSQL.

Distributed Workflow workers use the `workflow-postgres` feature:

```rust,ignore
use std::{sync::Arc, time::Duration};
use runifold::{
    Budget, CancellationToken, CapabilitySet, LeaseDuration, WorkerId,
    WorkflowDefinition, WorkflowRegistry, WorkflowResumePolicy,
    WorkflowSupervisor, WorkflowSupervisorConfig, WorkflowWorker,
    postgres::PostgresWorkflowStore,
};

let store = Arc::new(
    PostgresWorkflowStore::connect(&database_url, "runifold_workflows").await?
);
store.ensure_schema().await?;

let mut registry = WorkflowRegistry::new();
registry.register(
    WorkflowDefinition::new(
        Arc::new(workflow),
        Budget::default(),
        CapabilitySet::new(),
    )
    .with_resume_policy(WorkflowResumePolicy::RetryInterruptedStep),
)?;

let worker = WorkflowWorker::new(
    store,
    registry,
    WorkerId::parse("worker-01")?,
    LeaseDuration::new(Duration::from_secs(30))?,
    Duration::from_secs(10),
)?;

let shutdown = CancellationToken::new();
let supervisor = WorkflowSupervisor::new(
    Arc::new(worker),
    WorkflowSupervisorConfig::new(16)?
        .with_backoff(Duration::from_millis(25), Duration::from_secs(5))?,
);
let report = supervisor.run(&shutdown).await;
```

`run_once` remains available for embedded hosts and claims at most one task.
`WorkflowSupervisor` adds continuous polling, bounded concurrency, exponential
idle/error backoff, a low-cardinality metric snapshot, and graceful shutdown
that stops admission before draining started cycles. A failed heartbeat
cancels and joins the in-flight Workflow before returning `LeaseLost`. Every
distributed checkpoint write also validates the worker fencing token
independently.

Durable waits are definition nodes, not sleeping worker futures:

```rust,ignore
let tenant = WorkflowTenantId::parse("acme")?;
store
    .set_tenant_policy(
        tenant.clone(),
        WorkflowTenantPolicy::new(10_000, 100)?,
    )
    .await?;

let workflow = Workflow::builder("approval")
    .timer("cooldown", Duration::from_secs(30))
    .wait_for_signal_or_timeout(
        "approval",
        "approved",
        Duration::from_secs(24 * 60 * 60),
    )
    .agent("fulfill", fulfillment_agent, capabilities)
    .build()?;

let signal = WorkflowSignal::new(
    workflow_checkpoint_id,
    WorkflowSignalName::parse("approved")?,
    serde_json::json!({"approved_by": "operator-7"}),
)?;
let outcome = store.publish_signal(tenant.clone(), signal).await?;
```

Timers use store-authoritative time and hold no lease while waiting. Signals
target a workflow checkpoint and carry a stable publication identity:
duplicates with identical content are accepted idempotently, conflicting reuse
is rejected, and signals received before the wait are buffered. A
signal-or-timeout node emits a typed `WorkflowWaitOutcome`, with the store
choosing exactly one winner. `WorkflowStore::cancel` fences leased work;
`inspect_signal` exposes lifecycle metadata without payloads; and
`compact_signals` deletes only expired consumed or dead-letter identities.
Every external control-plane operation also requires a `WorkflowTenantId`.
Claims rotate across eligible tenants before applying task priority, while
each tenant's outstanding-task and unexpired-lease limits are enforced
independently.

Human review uses the same durable wake and fencing machinery:

```rust,ignore
let workflow = Workflow::builder("transfer")
    .interrupt("review", "Review the proposed transfer")
    .agent("continue", transfer_agent, capabilities)
    .build()?;

let snapshot = store.inspect(tenant.clone(), workflow_checkpoint_id).await?;
let request = snapshot.interrupt.expect("workflow is awaiting review");
let command = WorkflowInterruptCommand::new(
    workflow_checkpoint_id,
    request.interrupt_id,
    WorkflowInterruptDecision::edit(serde_json::json!({"amount": 40}))?,
)?;
let outcome = store.decide_interrupt(tenant, command).await?;
```

The prompt, proposal, and stable interrupt identity are checkpointed before
the worker lease is released. A decision ID is independently stable, so an
operator can safely retry the same command after a timeout. The downstream
node receives a typed `WorkflowInterruptOutcome`, preserving the distinction
between approval, edit, and rejection.

Checkpoint time travel creates a new execution instead of mutating history:

```rust,ignore
let revisions = store
    .list_checkpoint_history(
        tenant.clone(),
        workflow_checkpoint_id,
        None,
        WorkflowCheckpointHistoryLimit::new(64)?,
    )
    .await?;

let selected = &revisions[3];
let command = WorkflowForkCommand::new(
    workflow_checkpoint_id,
    selected.revision,
    WorkflowForkPolicy::RejectAmbiguous,
);
let fork = store.fork_workflow(tenant, command).await?;
```

Every revision is immutable. The fork receives a new checkpoint and Run
identity, keeps the source workflow version, accumulated usage, and capability
ceiling, and records `WorkflowLineage` back to the exact parent revision.
Completed steps are not replayed. A serial `StepInFlight` revision is rejected
unless the caller explicitly selects `RetryInterruptedStep`; in-flight
parallel and race revisions remain fail-closed. Forked timers and timeouts
restart from branch creation, while signal and human-review waits remain
durably suspended under the new task identity.

Multi-turn Agent context uses a separate `ConversationStore` boundary:

```rust,ignore
let store = InMemoryConversationStore::new();
let conversation_id = ConversationId::new();
let namespace = MemoryNamespace::parse("tenant.user-42")?;
let policy = ConversationContextPolicy::new(
    ConversationWindow::new(16)?,
)
.with_semantic_memory(4)?;

let turn = agent
    .run_conversation(
        "Continue our Rust design discussion",
        &run,
        &store,
        conversation_id,
        namespace,
        policy,
    )
    .await?;
```

The transcript is immutable model-visible conversation data. `Journal`
continues to contain execution facts and is never stored as conversation
history. A `ConversationSummary` is a lossy, monotonically advancing view over
a transcript prefix and never deletes that prefix. `summary_buffer` contains
older unsummarized entries outside the bounded live window; Agents fail closed
with `SummaryRequired` instead of silently dropping them. `SemanticMemory`
requires explicit upsert and immutable transcript provenance, is searchable
across conversations only inside its `MemoryNamespace`, and is injected as
untrusted transient context rather than masquerading as prior dialogue.

Production deployments can persist the same contract in `PostgreSQL`:

```rust,ignore
let store = PostgresConversationStore::connect(
    &database_url,
    "runifold_conversations",
).await?;
store.ensure_schema().await?; // explicit deployment step, never hidden in a turn

let automatic_summary = AutomaticConversationSummary::new(
    ConversationContextPolicy::new(ConversationWindow::new(16)?)
        .with_summary_batch(ConversationSummaryBatch::new(32)?),
    &summary_agent,
)
.with_pass_limit(ConversationSummaryPassLimit::new(8)?);
let turn = agent
    .run_conversation_with_summary(
        "Continue our Rust design discussion",
        &run,
        &store,
        conversation_id,
        namespace,
        automatic_summary,
    )
    .await?;
```

The PostgreSQL adapter uses atomic compare-and-swap transcript commits,
monotonic summary commits, namespace-isolated semantic memory, and explicit
schema setup. `summary_agent` implements `ConversationSummarizer`; because it
runs through the canonical Agent engine with the same `RunContext`, summary
generation remains subject to cancellation, deadlines, budgets, authority,
and journaling. Transcript content is marked as untrusted data in the summary
prompt, and a concurrent transcript append causes an explicit summary CAS
conflict rather than committing a stale summary.

Both the live window and each summary batch are bounded independently.
`summary_backlog` reports how many older entries remain without loading them,
and automatic compaction stops with `SummaryPassLimitExceeded` before the main
model runs when the configured pass limit is insufficient.

PostgreSQL semantic memory can opt into native pgvector search:

```rust,ignore
let store = PostgresConversationStore::connect(
    &database_url,
    "runifold_conversations",
)
.await?
.with_semantic_memory_embedder(Arc::new(embedding_model));
store.ensure_schema().await?;
store
    .ensure_semantic_memory_vector_schema(NonZeroU32::new(1536)?)
    .await?;

let stored = store
    .upsert_memory_scoped(command, RetrievalContext::for_run(&run))
    .await?;
```

Scoped memory writes and searches use `RetrievalDocument` and
`RetrievalQuery` embedding tasks respectively, persist the memory and vector
in one PostgreSQL statement, and return attributable embedding/database
`Usage`. Conversational Agent lookup uses the scoped path automatically, so
embedding tokens, cost, duration, cancellation, and deadlines participate in
the caller's run. Without an embedder the same API retains deterministic
lexical search.

## Fault scenarios and operations

`runifold-testkit` exposes the same failure boundaries used by the workspace:

```rust,ignore
use runifold_testkit::{FaultScenario, RecoveryHarness};

let faults = FaultScenario::new()
    .disconnect_after_tool_call()
    .fail_tool_on_invocation("charge", 2, injected_error);
let model = faults.model(scripted_model);
let mut runtime = RecoveryHarness::new(runtime_factory, faults.clone());

runtime.restart();
faults.assert_tool_executed_exactly("charge", 1)?;
```

`GoldenTrace` removes generated identities and timestamps while preserving
causal event behavior, so regressions identify the first divergent event.

The separate operations CLI reads exported JSON or pages canonical journals by
`run_id` without linking Provider credentials or executing effects. SQLite is
opened with read-only flags; PostgreSQL queries never perform migrations:

```console
runifold run inspect --events events.json
runifold run tail --events events.json --limit 50
runifold run inspect --sqlite runifold.db --run-id 019...
runifold run inspect --postgres "$RUNIFOLD_POSTGRES_URL" --run-id 019...
runifold run replay --events events.json --output replay-evidence.json
runifold checkpoint diff before.json after.json
runifold budget explain budget.json usage.json
runifold doctor --events events.json
```

Checkpoint diffs report JSON Pointers and change kinds without printing values.
The replay command produces validated, side-effect-free evidence; actual Effect
re-execution remains behind the runtime's explicit recovery policy.

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
[RFC 0001](docs/rfcs/0001-runtime-kernel.md). Persistence and fault-injection
requirements are documented in the [testing guide](docs/TESTING.md).

## Workspace

| Crate | Purpose |
|---|---|
| `runifold` | Ergonomic public facade |
| `runifold-agent` | Bounded model-tool loop, capability-gated delegation, middleware governance, and canonical transcript |
| `runifold-core` | Run, event, effect, capability, budget, and cancellation primitives |
| `runifold-effect` | Write-ahead effects, idempotency, recovery policy, and durable result replay |
| `runifold-eval-cli` | JSONL evaluation runner, external Candidate protocol, and CI quality gates |
| `runifold-cli` | Read-only run inspection, replay evidence, checkpoint diff, budget explanation, and doctor commands |
| `runifold-model` | Provider-neutral model requests, content, capabilities, and stream accumulation |
| `runifold-macros` | Attribute macros for typed async Rust Tools |
| `runifold-mcp` | Capability-safe MCP Tools, Resources, Templates, Prompts, Completion, Sampling, stdio, and Streamable HTTP |
| `runifold-observability-otel` | Optional OpenTelemetry GenAI spans and metrics |
| `runifold-ops` | Stable operational summaries, causal validation, budget explanation, and value-free checkpoint diffs |
| `runifold-providers` | Feature-gated HTTP and SDK-backed model provider adapters |
| `runifold-provider-testkit` | Offline real-HTTP cassettes, protocol assertions, delays, and disconnect injection |
| `runifold-retrieval` | Provider-neutral embeddings, capability-safe retrieval, and a deterministic reference vector index |
| `runifold-retrieval-pgvector` | Explicit PostgreSQL/pgvector persistence and cosine/HNSW retrieval |
| `runifold-retrieval-qdrant` | Qdrant REST upsert and query adapter with stable document identity |
| `runifold-retrieval-text` | Bounded UTF-8 loading and deterministic Unicode/provenance-aware chunking |
| `runifold-store-postgres` | PostgreSQL conversations, semantic memory, atomic Agent checkpoints, write-ahead effects, workflow claims, fenced checkpoints, leases, heartbeats, and fencing tokens |
| `runifold-store-sqlite` | Durable local effects, checkpoints, journals, atomic Agent conversations, fenced Workflow tasks, budgets, HITL, history, and fork/replay in SQLite |
| `runifold-testkit` | Deterministic runtime helpers, quality datasets, async scorers, and regression gates |
| `runifold-tool` | Tool descriptors, capability gating, registry, and execution |

Planned edge crates include A2A transports and additional persistence backends.

## License

Licensed under either Apache-2.0 or MIT, at your option.
