use std::{
    env, fs,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use rig_core::{
    client::CompletionClient,
    completion::{CompletionError, CompletionModel as _},
    providers::openai,
    streaming::StreamedAssistantContent,
};
use runifold_model::{Message, ModelRef, ModelRequest};
use runifold_provider_testkit::{
    BenchmarkFailureKind, BenchmarkFuture, BenchmarkInvocation, BenchmarkPlan,
    BenchmarkRegressionComparison, BenchmarkRegressionPolicy, BenchmarkTarget, CassetteServer,
    HttpExchange, ModelBenchmarkTarget, ObservedRequest, ProviderBenchmarkReport, ResponseChunk,
    ScriptedResponse, benchmark, compare_benchmarks,
};
use runifold_providers::openai::{OpenAiClient, OpenAiConfig, OpenAiWireProtocol};
use serde::Serialize;
use serde_json::{Value, json};

const MODEL: &str = "benchmark-model";
const PROVIDER: &str = "benchmark";
const EXPECTED_OUTPUT: &str = "Hello";
const DEFAULT_MEASURED_RUNS: usize = 100;
const DEFAULT_WARMUP_RUNS: usize = 10;
const DEFAULT_CONCURRENCY: usize = 16;
const DEFAULT_ROUNDS: usize = 10;
const BOOTSTRAP_SAMPLES: usize = 10_000;

#[derive(Clone)]
struct RigTarget {
    model: openai::completion::CompletionModel,
}

impl BenchmarkTarget for RigTarget {
    fn execute(&self) -> BenchmarkFuture<'_> {
        Box::pin(run_rig(self.model.clone()))
    }
}

#[derive(Clone, Copy, Debug)]
struct Settings {
    measured_runs: NonZeroUsize,
    warmup_runs: usize,
    concurrency: NonZeroUsize,
    rounds: NonZeroUsize,
    starting_order: ExecutionOrder,
    enforce: bool,
}

#[derive(Clone, Copy, Debug)]
enum ExecutionOrder {
    RunifoldFirst,
    RigFirst,
}

impl ExecutionOrder {
    fn from_env() -> Result<Self> {
        match env::var("RUNIFOLD_BENCH_ORDER") {
            Ok(value) if value == "runifold-first" => Ok(Self::RunifoldFirst),
            Ok(value) if value == "rig-first" => Ok(Self::RigFirst),
            Ok(_) => bail!("RUNIFOLD_BENCH_ORDER must be one of: runifold-first, rig-first"),
            Err(env::VarError::NotPresent) => Ok(Self::RunifoldFirst),
            Err(error) => Err(error).context("failed to read RUNIFOLD_BENCH_ORDER"),
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::RunifoldFirst => "runifold-first",
            Self::RigFirst => "rig-first",
        }
    }

    const fn alternate(self) -> Self {
        match self {
            Self::RunifoldFirst => Self::RigFirst,
            Self::RigFirst => Self::RunifoldFirst,
        }
    }

    const fn for_round(self, round_index: usize) -> Self {
        if round_index.is_multiple_of(2) {
            self
        } else {
            self.alternate()
        }
    }
}

impl Settings {
    fn from_env() -> Result<Self> {
        Ok(Self {
            measured_runs: nonzero_env("RUNIFOLD_BENCH_RUNS", DEFAULT_MEASURED_RUNS)?,
            warmup_runs: usize_env("RUNIFOLD_BENCH_WARMUP", DEFAULT_WARMUP_RUNS)?,
            concurrency: nonzero_env("RUNIFOLD_BENCH_CONCURRENCY", DEFAULT_CONCURRENCY)?,
            rounds: nonzero_env("RUNIFOLD_BENCH_ROUNDS", DEFAULT_ROUNDS)?,
            starting_order: ExecutionOrder::from_env()?,
            enforce: bool_env("RUNIFOLD_BENCH_ENFORCE")?,
        })
    }

    fn request_count(self) -> Result<usize> {
        self.warmup_runs
            .checked_add(self.measured_runs.get())
            .context("benchmark request count overflowed usize")
    }

    fn total_request_count(self) -> Result<usize> {
        self.request_count()?
            .checked_mul(self.rounds.get())
            .context("total benchmark request count overflowed usize")
    }

    fn plan(self, framework: &str, round: usize, order: ExecutionOrder) -> BenchmarkPlan {
        BenchmarkPlan::new(self.measured_runs)
            .with_warmup(self.warmup_runs)
            .with_concurrency(self.concurrency)
            .with_environment("benchmark.kind", "loopback_openai_sse")
            .with_environment("framework", framework)
            .with_environment("model", MODEL)
            .with_environment("protocol", "openai_chat_completions_sse")
            .with_environment("execution.order", order.as_str())
            .with_environment("execution.round", round.to_string())
            .with_environment("rust.version", rust_version())
            .with_environment(
                "target",
                format!("{}-{}", env::consts::ARCH, env::consts::OS),
            )
    }
}

#[derive(Debug, Serialize)]
struct PairedRound {
    round: usize,
    order: &'static str,
    runifold: ProviderBenchmarkReport,
    rig: ProviderBenchmarkReport,
    comparison: BenchmarkRegressionComparison,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum MetricDirection {
    LargerIsBetter,
    SmallerIsBetter,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct ConfidenceInterval {
    lower: f64,
    upper: f64,
}

#[derive(Debug, Serialize)]
struct PairedMetricSummary {
    name: &'static str,
    direction: MetricDirection,
    rounds: usize,
    runifold_median: f64,
    rig_median: f64,
    paired_relative_delta_median: f64,
    paired_relative_delta_bootstrap_95: ConfidenceInterval,
    favorable_rounds: usize,
}

#[derive(Debug, Serialize)]
struct AggregateEvidence {
    rounds: usize,
    bootstrap_samples: usize,
    metrics: Vec<PairedMetricSummary>,
    non_regression_passed: bool,
    outperformance_supported: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::from_env()?;
    let policy = BenchmarkRegressionPolicy::new(0.0, 0.1, 0.1, 0.1)
        .context("benchmark regression policy is invalid")?;
    let total_request_count = settings.total_request_count()?;
    let runifold_server = cassette(total_request_count)?;
    let runifold_target = runifold_target(&runifold_server)?;
    let rig_server = cassette(total_request_count)?;
    let rig_target = rig_target(&rig_server)?;
    let mut rounds = Vec::with_capacity(settings.rounds.get());
    for round_index in 0..settings.rounds.get() {
        let order = settings.starting_order.for_round(round_index);
        rounds.push(
            benchmark_pair(
                settings,
                round_index + 1,
                order,
                policy,
                Arc::clone(&runifold_target),
                Arc::clone(&rig_target),
            )
            .await?,
        );
    }
    validate_requests(&runifold_server.observed_requests(), total_request_count)
        .context("runifold emitted a non-equivalent request")?;
    runifold_server
        .assert_finished()
        .context("runifold cassette did not finish cleanly")?;
    validate_requests(&rig_server.observed_requests(), total_request_count)
        .context("rig emitted a non-equivalent request")?;
    rig_server
        .assert_finished()
        .context("rig cassette did not finish cleanly")?;
    let aggregate = aggregate_evidence(&rounds)?;
    let artifact = json!({
        "contract": {
            "baseline": "rig-core-0.40.0",
            "candidate": "runifold-local",
            "rounds": settings.rounds,
            "starting_order": settings.starting_order.as_str(),
            "order_strategy": "alternating",
            "expected_output": EXPECTED_OUTPUT,
            "request": {
                "model": MODEL,
                "stream": true,
                "messages": [{"role": "user", "content": "hello"}]
            },
            "regression_policy": policy
        },
        "aggregate": aggregate,
        "round_reports": rounds,
    });
    let output_dir = write_artifact(&artifact)?;

    println!("{}", serde_json::to_string_pretty(&aggregate)?);
    println!("artifact_dir={}", output_dir.display());

    if settings.enforce && !aggregate.non_regression_passed {
        bail!("Runifold failed the aggregate Rig non-regression gate");
    }
    Ok(())
}

async fn benchmark_pair(
    settings: Settings,
    round: usize,
    order: ExecutionOrder,
    policy: BenchmarkRegressionPolicy,
    runifold_target: Arc<dyn BenchmarkTarget>,
    rig_target: Arc<dyn BenchmarkTarget>,
) -> Result<PairedRound> {
    let (runifold, rig) = match order {
        ExecutionOrder::RunifoldFirst => {
            let runifold =
                benchmark_framework("runifold-local", runifold_target, settings, round, order)
                    .await?;
            let rig =
                benchmark_framework("rig-core-0.40.0", rig_target, settings, round, order).await?;
            (runifold, rig)
        }
        ExecutionOrder::RigFirst => {
            let rig =
                benchmark_framework("rig-core-0.40.0", rig_target, settings, round, order).await?;
            let runifold =
                benchmark_framework("runifold-local", runifold_target, settings, round, order)
                    .await?;
            (runifold, rig)
        }
    };
    let comparison = compare_benchmarks(&rig, &runifold, policy);
    Ok(PairedRound {
        round,
        order: order.as_str(),
        runifold,
        rig,
        comparison,
    })
}

async fn benchmark_framework(
    label: &'static str,
    target: Arc<dyn BenchmarkTarget>,
    settings: Settings,
    round: usize,
    order: ExecutionOrder,
) -> Result<ProviderBenchmarkReport> {
    benchmark(label, target, settings.plan(label, round, order))
        .await
        .with_context(|| format!("{label} benchmark failed"))
}

fn aggregate_evidence(rounds: &[PairedRound]) -> Result<AggregateEvidence> {
    if rounds.is_empty() {
        bail!("at least one paired benchmark round is required");
    }
    let metrics = vec![
        summarize_metric(
            "success_rate",
            MetricDirection::LargerIsBetter,
            rounds,
            |report| Some(report.success_rate),
            0x17d4_5b89_a3c6_e2f1,
        )?,
        summarize_metric(
            "throughput_per_second",
            MetricDirection::LargerIsBetter,
            rounds,
            |report| Some(report.throughput_per_second),
            0x8bf2_1497_c5d0_36ae,
        )?,
        summarize_metric(
            "p95_ttft_us",
            MetricDirection::SmallerIsBetter,
            rounds,
            |report| report.ttft.map(|latency| latency.p95_us as f64),
            0x3e91_ab72_6cf8_405d,
        )?,
        summarize_metric(
            "p95_total_latency_us",
            MetricDirection::SmallerIsBetter,
            rounds,
            |report| report.total_latency.map(|latency| latency.p95_us as f64),
            0xc487_2da0_19be_f653,
        )?,
    ];
    let success = metric_named(&metrics, "success_rate")?;
    let throughput = metric_named(&metrics, "throughput_per_second")?;
    let ttft = metric_named(&metrics, "p95_ttft_us")?;
    let total = metric_named(&metrics, "p95_total_latency_us")?;
    let non_regression_passed = success.paired_relative_delta_bootstrap_95.lower >= 0.0
        && throughput.paired_relative_delta_bootstrap_95.lower >= -0.1
        && ttft.paired_relative_delta_bootstrap_95.upper <= 0.1
        && total.paired_relative_delta_bootstrap_95.upper <= 0.1;
    let outperformance_supported = success.paired_relative_delta_bootstrap_95.lower >= 0.0
        && throughput.paired_relative_delta_bootstrap_95.lower > 0.0
        && ttft.paired_relative_delta_bootstrap_95.upper < 0.0
        && total.paired_relative_delta_bootstrap_95.upper < 0.0;
    Ok(AggregateEvidence {
        rounds: rounds.len(),
        bootstrap_samples: BOOTSTRAP_SAMPLES,
        metrics,
        non_regression_passed,
        outperformance_supported,
    })
}

fn summarize_metric(
    name: &'static str,
    direction: MetricDirection,
    rounds: &[PairedRound],
    value: fn(&ProviderBenchmarkReport) -> Option<f64>,
    seed: u64,
) -> Result<PairedMetricSummary> {
    let pairs = rounds
        .iter()
        .map(|round| {
            let runifold = value(&round.runifold)
                .with_context(|| format!("round {} lacks Runifold {name}", round.round))?;
            let rig = value(&round.rig)
                .with_context(|| format!("round {} lacks Rig {name}", round.round))?;
            if !runifold.is_finite() || !rig.is_finite() {
                bail!("round {} contains non-finite {name}", round.round);
            }
            Ok((runifold, rig))
        })
        .collect::<Result<Vec<_>>>()?;
    let runifold_values = pairs.iter().map(|(runifold, _)| *runifold).collect();
    let rig_values = pairs.iter().map(|(_, rig)| *rig).collect();
    let deltas = pairs
        .iter()
        .map(|(runifold, rig)| relative_delta(*runifold, *rig))
        .collect::<Vec<_>>();
    let favorable_rounds = deltas
        .iter()
        .filter(|delta| match direction {
            MetricDirection::LargerIsBetter => **delta > 0.0,
            MetricDirection::SmallerIsBetter => **delta < 0.0,
        })
        .count();
    Ok(PairedMetricSummary {
        name,
        direction,
        rounds: pairs.len(),
        runifold_median: median(runifold_values)?,
        rig_median: median(rig_values)?,
        paired_relative_delta_median: median(deltas.clone())?,
        paired_relative_delta_bootstrap_95: bootstrap_median_ci(&deltas, seed)?,
        favorable_rounds,
    })
}

fn metric_named<'a>(
    metrics: &'a [PairedMetricSummary],
    name: &str,
) -> Result<&'a PairedMetricSummary> {
    metrics
        .iter()
        .find(|metric| metric.name == name)
        .with_context(|| format!("aggregate metric `{name}` is missing"))
}

fn relative_delta(candidate: f64, baseline: f64) -> f64 {
    (candidate - baseline) / baseline.abs().max(f64::MIN_POSITIVE)
}

fn median(mut values: Vec<f64>) -> Result<f64> {
    if values.is_empty() {
        bail!("cannot compute a median without values");
    }
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        Ok((values[middle - 1] + values[middle]) / 2.0)
    } else {
        Ok(values[middle])
    }
}

fn bootstrap_median_ci(values: &[f64], seed: u64) -> Result<ConfidenceInterval> {
    if values.is_empty() {
        bail!("cannot bootstrap a confidence interval without values");
    }
    let mut rng = DeterministicRng::new(seed);
    let mut medians = Vec::with_capacity(BOOTSTRAP_SAMPLES);
    let mut sample = Vec::with_capacity(values.len());
    for _ in 0..BOOTSTRAP_SAMPLES {
        sample.clear();
        for _ in 0..values.len() {
            sample.push(values[rng.next_index(values.len())]);
        }
        medians.push(median(sample.clone())?);
    }
    medians.sort_by(f64::total_cmp);
    Ok(ConfidenceInterval {
        lower: percentile(&medians, 25, 1_000)?,
        upper: percentile(&medians, 975, 1_000)?,
    })
}

fn percentile(values: &[f64], numerator: usize, denominator: usize) -> Result<f64> {
    if values.is_empty() || denominator == 0 || numerator > denominator {
        bail!("invalid percentile input");
    }
    let index = values.len().saturating_sub(1).saturating_mul(numerator) / denominator;
    Ok(values[index])
}

struct DeterministicRng(u64);

impl DeterministicRng {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_index(&mut self, upper_bound: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 as usize) % upper_bound
    }
}

fn cassette(request_count: usize) -> Result<CassetteServer> {
    let response = ScriptedResponse::ok(vec![
        ResponseChunk::text(concat!(
            "data: {\"id\":\"chatcmpl-benchmark\",\"object\":\"chat.completion.chunk\",",
            "\"created\":0,\"model\":\"benchmark-model\",\"choices\":[{\"index\":0,",
            "\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}\n\n",
        )),
        ResponseChunk::text(concat!(
            "data: {\"id\":\"chatcmpl-benchmark\",\"object\":\"chat.completion.chunk\",",
            "\"created\":0,\"model\":\"benchmark-model\",\"choices\":[{\"index\":0,",
            "\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n",
        ))
        .after(Duration::from_millis(1)),
        ResponseChunk::text(concat!(
            "data: {\"id\":\"chatcmpl-benchmark\",\"object\":\"chat.completion.chunk\",",
            "\"created\":0,\"model\":\"benchmark-model\",\"choices\":[{\"index\":0,",
            "\"delta\":{},\"finish_reason\":\"stop\"}],",
            "\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n",
            "data: [DONE]\n\n",
        )),
    ])
    .with_header("content-type", "text/event-stream");
    CassetteServer::start_repeating(
        HttpExchange::new("POST", "/v1/chat/completions", response),
        request_count,
    )
    .context("failed to start loopback cassette")
}

fn runifold_target(server: &CassetteServer) -> Result<Arc<dyn BenchmarkTarget>> {
    let base_url = format!("{}v1/", server.base_url());
    let config = OpenAiConfig::compatible(
        PROVIDER,
        "benchmark-key",
        &base_url,
        OpenAiWireProtocol::ChatCompletions,
    )
    .context("failed to configure Runifold OpenAI-compatible client")?;
    let client = OpenAiClient::new(config);
    let request = ModelRequest::new(ModelRef::new(PROVIDER, MODEL), Message::user("hello"));
    Ok(Arc::new(ModelBenchmarkTarget::new(
        Arc::new(client),
        request,
    )))
}

fn rig_target(server: &CassetteServer) -> Result<Arc<dyn BenchmarkTarget>> {
    let base_url = format!("{}v1", server.base_url());
    let client = openai::CompletionsClient::builder()
        .api_key("benchmark-key")
        .base_url(base_url)
        .build()
        .context("failed to configure Rig OpenAI Chat Completions client")?;
    Ok(Arc::new(RigTarget {
        model: client.completion_model(MODEL),
    }))
}

async fn run_rig(model: openai::completion::CompletionModel) -> BenchmarkInvocation {
    let started = Instant::now();
    let mut stream = match model.completion_request("hello").stream().await {
        Ok(stream) => stream,
        Err(error) => {
            return BenchmarkInvocation::failure(classify_rig_error(&error), started.elapsed());
        }
    };
    let mut ttft = None;
    let mut output = String::new();
    let mut completed = false;
    while let Some(item) = stream.next().await {
        match item {
            Ok(StreamedAssistantContent::Text(text)) => {
                if !text.text.is_empty() {
                    ttft.get_or_insert_with(|| started.elapsed());
                    output.push_str(&text.text);
                }
            }
            Ok(StreamedAssistantContent::ToolCall { .. })
            | Ok(StreamedAssistantContent::ToolCallDelta { .. })
            | Ok(StreamedAssistantContent::Reasoning(_))
            | Ok(StreamedAssistantContent::ReasoningDelta { .. })
            | Ok(StreamedAssistantContent::Unknown(_)) => {
                ttft.get_or_insert_with(|| started.elapsed());
            }
            Ok(StreamedAssistantContent::Final(_)) => completed = true,
            Err(error) => {
                return BenchmarkInvocation::failure(classify_rig_error(&error), started.elapsed());
            }
        }
    }
    let total = started.elapsed();
    if !completed || output != EXPECTED_OUTPUT {
        return BenchmarkInvocation::failure(BenchmarkFailureKind::Stream, total);
    }
    BenchmarkInvocation::success(ttft, total)
        .unwrap_or_else(|_| BenchmarkInvocation::failure(BenchmarkFailureKind::Other, total))
}

fn classify_rig_error(error: &CompletionError) -> BenchmarkFailureKind {
    match error {
        CompletionError::HttpError(_) | CompletionError::UrlError(_) => {
            BenchmarkFailureKind::Transport
        }
        CompletionError::JsonError(_) | CompletionError::ResponseError(_) => {
            BenchmarkFailureKind::Protocol
        }
        CompletionError::ProviderError(_) | CompletionError::ProviderResponse(_) => {
            BenchmarkFailureKind::Provider
        }
        CompletionError::RequestError(_) => BenchmarkFailureKind::InvalidRequest,
        _ => BenchmarkFailureKind::Other,
    }
}

fn validate_requests(requests: &[ObservedRequest], expected_count: usize) -> Result<()> {
    if requests.len() != expected_count {
        bail!(
            "expected {expected_count} requests, captured {}",
            requests.len()
        );
    }
    for (index, request) in requests.iter().enumerate() {
        let body = request
            .json_body()
            .with_context(|| format!("request {index} body was not JSON"))?;
        if body.get("model").and_then(Value::as_str) != Some(MODEL) {
            bail!("request {index} used a different model");
        }
        if body.get("stream").and_then(Value::as_bool) != Some(true) {
            bail!("request {index} was not streaming");
        }
        let messages = body
            .get("messages")
            .and_then(Value::as_array)
            .with_context(|| format!("request {index} omitted messages"))?;
        if messages.len() != 1
            || messages[0].get("role").and_then(Value::as_str) != Some("user")
            || messages[0].get("content").and_then(Value::as_str) != Some("hello")
        {
            bail!("request {index} did not contain the canonical user message");
        }
        if body
            .get("tools")
            .is_some_and(|tools| tools.as_array().is_none_or(|tools| !tools.is_empty()))
        {
            bail!("request {index} unexpectedly enabled tools");
        }
    }
    Ok(())
}

fn write_artifact(artifact: &Value) -> Result<PathBuf> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock predates the Unix epoch")?
        .as_nanos();
    let output_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("benchmark-reports")
        .join(timestamp.to_string());
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;
    let output_path = output_dir.join("comparison.json");
    fs::write(&output_path, serde_json::to_vec_pretty(artifact)?)
        .with_context(|| format!("failed to write {}", output_path.display()))?;
    Ok(output_dir)
}

fn nonzero_env(name: &str, default: usize) -> Result<NonZeroUsize> {
    NonZeroUsize::new(usize_env(name, default)?)
        .with_context(|| format!("{name} must be greater than zero"))
}

fn usize_env(name: &str, default: usize) -> Result<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{name} must be a non-negative integer")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("failed to read {name}")),
    }
}

fn bool_env(name: &str) -> Result<bool> {
    match env::var(name) {
        Ok(value) => match value.as_str() {
            "1" | "true" => Ok(true),
            "0" | "false" => Ok(false),
            _ => bail!("{name} must be one of: 0, 1, false, true"),
        },
        Err(env::VarError::NotPresent) => Ok(false),
        Err(error) => Err(error).with_context(|| format!("failed to read {name}")),
    }
}

fn rust_version() -> String {
    Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|version| version.trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_handles_odd_even_and_empty_samples() {
        assert_eq!(median(vec![3.0, 1.0, 2.0]).unwrap(), 2.0);
        assert_eq!(median(vec![4.0, 1.0, 3.0, 2.0]).unwrap(), 2.5);
        assert!(median(Vec::new()).is_err());
    }

    #[test]
    fn bootstrap_interval_is_deterministic_for_constant_samples() {
        let interval = bootstrap_median_ci(&[0.25, 0.25, 0.25], 42).unwrap();

        assert_eq!(interval.lower, 0.25);
        assert_eq!(interval.upper, 0.25);
    }

    #[test]
    fn execution_order_alternates_from_selected_start() {
        let start = ExecutionOrder::RigFirst;

        assert!(matches!(start.for_round(0), ExecutionOrder::RigFirst));
        assert!(matches!(start.for_round(1), ExecutionOrder::RunifoldFirst));
        assert!(matches!(start.for_round(2), ExecutionOrder::RigFirst));
    }

    #[test]
    fn relative_delta_preserves_performance_direction() {
        assert_eq!(relative_delta(120.0, 100.0), 0.2);
        assert_eq!(relative_delta(80.0, 100.0), -0.2);
    }
}
