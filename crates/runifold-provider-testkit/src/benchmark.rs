//! Framework-neutral latency, throughput, and reliability benchmark contract.

use std::{
    collections::BTreeMap,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::{StreamExt, stream};
use runifold_model::{
    Model, ModelCallContext, ModelErrorKind, ModelRequest, ModelStreamAccumulator, ModelStreamEvent,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const REPORT_SCHEMA_VERSION: u32 = 1;

/// A boxed benchmark invocation future.
pub type BenchmarkFuture<'a> = Pin<Box<dyn Future<Output = BenchmarkInvocation> + Send + 'a>>;

/// Framework-neutral operation executed by the benchmark runner.
///
/// External comparison adapters can implement this trait for another runtime,
/// including Rig, while retaining identical scheduling and report semantics.
pub trait BenchmarkTarget: Send + Sync {
    /// Executes one isolated invocation.
    fn execute(&self) -> BenchmarkFuture<'_>;
}

/// Canonical Runifold model adapter for [`BenchmarkTarget`].
#[derive(Clone)]
pub struct ModelBenchmarkTarget {
    model: Arc<dyn Model>,
    request: ModelRequest,
}

impl ModelBenchmarkTarget {
    /// Creates a target from one model and repeatable canonical request.
    pub fn new(model: Arc<dyn Model>, request: ModelRequest) -> Self {
        Self { model, request }
    }
}

impl std::fmt::Debug for ModelBenchmarkTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ModelBenchmarkTarget")
            .field("model", &self.request.model)
            .finish_non_exhaustive()
    }
}

impl BenchmarkTarget for ModelBenchmarkTarget {
    fn execute(&self) -> BenchmarkFuture<'_> {
        Box::pin(run_model(self.model.as_ref(), self.request.clone()))
    }
}

/// Stable framework-neutral failure category.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum BenchmarkFailureKind {
    /// Local request validation failed.
    InvalidRequest,
    /// A required feature was unsupported.
    UnsupportedFeature,
    /// Network or transport failure.
    Transport,
    /// Provider wire-protocol failure.
    Protocol,
    /// Stream lifecycle or output assembly failure.
    Stream,
    /// Provider-side rejection or failure.
    Provider,
    /// Invocation cancellation.
    Cancelled,
    /// Invocation deadline exceeded.
    DeadlineExceeded,
    /// Runtime-specific failure without a more portable classification.
    Other,
}

impl From<&ModelErrorKind> for BenchmarkFailureKind {
    fn from(kind: &ModelErrorKind) -> Self {
        match kind {
            ModelErrorKind::InvalidRequest => Self::InvalidRequest,
            ModelErrorKind::UnsupportedFeature => Self::UnsupportedFeature,
            ModelErrorKind::Transport => Self::Transport,
            ModelErrorKind::Protocol => Self::Protocol,
            ModelErrorKind::StreamState | ModelErrorKind::MalformedToolArguments => Self::Stream,
            ModelErrorKind::Provider => Self::Provider,
            ModelErrorKind::Cancelled => Self::Cancelled,
            ModelErrorKind::DeadlineExceeded => Self::DeadlineExceeded,
            _ => Self::Other,
        }
    }
}

/// Result and host-observed timing for one invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BenchmarkInvocation {
    ttft: Option<Duration>,
    total: Duration,
    failure: Option<BenchmarkFailureKind>,
}

impl BenchmarkInvocation {
    /// Creates one successful invocation.
    ///
    /// # Errors
    ///
    /// Returns [`BenchmarkInvocationError`] when time to first output exceeds
    /// total invocation time.
    pub fn success(
        ttft: Option<Duration>,
        total: Duration,
    ) -> Result<Self, BenchmarkInvocationError> {
        if ttft.is_some_and(|ttft| ttft > total) {
            return Err(BenchmarkInvocationError::TtftAfterCompletion);
        }
        Ok(Self {
            ttft,
            total,
            failure: None,
        })
    }

    /// Creates one failed invocation.
    pub const fn failure(kind: BenchmarkFailureKind, total: Duration) -> Self {
        Self {
            ttft: None,
            total,
            failure: Some(kind),
        }
    }
}

/// Invalid timing evidence supplied by a benchmark adapter.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum BenchmarkInvocationError {
    /// The first output was observed after invocation completion.
    #[error("time to first output cannot exceed total invocation time")]
    TtftAfterCompletion,
}

/// Bounded benchmark execution plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BenchmarkPlan {
    measured_runs: NonZeroUsize,
    warmup_runs: usize,
    concurrency: NonZeroUsize,
    environment: BTreeMap<String, String>,
}

impl BenchmarkPlan {
    /// Creates a sequential plan with no warmup.
    pub const fn new(measured_runs: NonZeroUsize) -> Self {
        Self {
            measured_runs,
            warmup_runs: 0,
            concurrency: NonZeroUsize::MIN,
            environment: BTreeMap::new(),
        }
    }

    /// Adds unmeasured warmup invocations.
    #[must_use]
    pub const fn with_warmup(mut self, warmup_runs: usize) -> Self {
        self.warmup_runs = warmup_runs;
        self
    }

    /// Bounds concurrently executing measured invocations.
    #[must_use]
    pub const fn with_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Records one stable environment fact in the generated report.
    ///
    /// Use this for framework version, Rust toolchain, target, runtime,
    /// hardware class, provider endpoint, and model revision.
    #[must_use]
    pub fn with_environment(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.environment.insert(key.into(), value.into());
        self
    }
}

/// Latency distribution expressed in integer microseconds.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LatencyDistribution {
    /// Minimum observed latency.
    pub min_us: u64,
    /// Nearest-rank 50th percentile.
    pub p50_us: u64,
    /// Nearest-rank 95th percentile.
    pub p95_us: u64,
    /// Nearest-rank 99th percentile.
    pub p99_us: u64,
    /// Maximum observed latency.
    pub max_us: u64,
}

/// Count of failures in one normalized category.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BenchmarkFailureCount {
    /// Normalized failure category.
    pub kind: BenchmarkFailureKind,
    /// Number of measured invocations in this category.
    pub count: usize,
}

/// Stable, serialization-safe benchmark evidence.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ProviderBenchmarkReport {
    /// Report schema version.
    pub schema_version: u32,
    /// User-controlled implementation or framework label.
    pub label: String,
    /// Number of measured invocations.
    pub measured_runs: usize,
    /// Maximum configured concurrency.
    pub concurrency: usize,
    /// Stable environment facts required to reproduce the run.
    pub environment: BTreeMap<String, String>,
    /// Successful invocation count.
    pub successes: usize,
    /// Failed invocation count.
    pub failures: usize,
    /// Successful invocations divided by measured invocations.
    pub success_rate: f64,
    /// Complete measured wall-clock interval.
    pub wall_time_us: u64,
    /// Measured invocations completed per second.
    pub throughput_per_second: f64,
    /// Successful invocation latency distribution.
    pub total_latency: Option<LatencyDistribution>,
    /// Successful time-to-first-output distribution.
    pub ttft: Option<LatencyDistribution>,
    /// Successful invocations which completed without visible output.
    pub successes_without_output: usize,
    /// Deterministically ordered normalized failure counts.
    pub failure_counts: Vec<BenchmarkFailureCount>,
}

/// Benchmark construction or comparison failure.
#[derive(Clone, Debug, Error, PartialEq)]
#[non_exhaustive]
pub enum ProviderBenchmarkError {
    /// Report label was blank.
    #[error("benchmark label cannot be empty")]
    EmptyLabel,
    /// Regression policy included a non-finite or out-of-range ratio.
    #[error("benchmark regression ratio `{field}` must be finite and between zero and one")]
    InvalidRegressionRatio {
        /// Invalid policy field.
        field: &'static str,
    },
}

/// Runs warmup and measured invocations against one framework-neutral target.
///
/// # Errors
///
/// Returns [`ProviderBenchmarkError`] when `label` is blank.
pub async fn benchmark(
    label: impl Into<String>,
    target: Arc<dyn BenchmarkTarget>,
    plan: BenchmarkPlan,
) -> Result<ProviderBenchmarkReport, ProviderBenchmarkError> {
    let label = label.into();
    if label.trim().is_empty() {
        return Err(ProviderBenchmarkError::EmptyLabel);
    }
    for _ in 0..plan.warmup_runs {
        let _ = target.execute().await;
    }

    let started = Instant::now();
    let outcomes = stream::iter(0..plan.measured_runs.get())
        .map(|_| {
            let target = Arc::clone(&target);
            async move { target.execute().await }
        })
        .buffer_unordered(plan.concurrency.get())
        .collect::<Vec<_>>()
        .await;
    let wall_time = started.elapsed();
    Ok(build_report(label, &plan, wall_time, &outcomes))
}

/// Allowed degradation from a baseline report.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct BenchmarkRegressionPolicy {
    /// Maximum absolute success-rate decrease.
    pub max_success_rate_drop: f64,
    /// Maximum throughput decrease relative to baseline.
    pub max_throughput_drop: f64,
    /// Maximum p95 TTFT increase relative to baseline.
    pub max_p95_ttft_increase: f64,
    /// Maximum p95 total-latency increase relative to baseline.
    pub max_p95_total_latency_increase: f64,
}

impl BenchmarkRegressionPolicy {
    /// Creates a validated comparison policy.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderBenchmarkError`] when any ratio is non-finite or
    /// outside zero through one.
    pub fn new(
        max_success_rate_drop: f64,
        max_throughput_drop: f64,
        max_p95_ttft_increase: f64,
        max_p95_total_latency_increase: f64,
    ) -> Result<Self, ProviderBenchmarkError> {
        for (field, value) in [
            ("max_success_rate_drop", max_success_rate_drop),
            ("max_throughput_drop", max_throughput_drop),
            ("max_p95_ttft_increase", max_p95_ttft_increase),
            (
                "max_p95_total_latency_increase",
                max_p95_total_latency_increase,
            ),
        ] {
            if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                return Err(ProviderBenchmarkError::InvalidRegressionRatio { field });
            }
        }
        Ok(Self {
            max_success_rate_drop,
            max_throughput_drop,
            max_p95_ttft_increase,
            max_p95_total_latency_increase,
        })
    }
}

/// One comparable benchmark metric and its gate decision.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BenchmarkRegressionMetric {
    /// Stable metric name.
    pub name: String,
    /// Baseline value.
    pub baseline: Option<f64>,
    /// Candidate value.
    pub candidate: Option<f64>,
    /// Whether the candidate satisfies the configured policy.
    pub passed: bool,
}

/// Complete baseline-to-candidate benchmark decision.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BenchmarkRegressionComparison {
    /// Baseline report label.
    pub baseline: String,
    /// Candidate report label.
    pub candidate: String,
    /// Individual metric decisions.
    pub metrics: Vec<BenchmarkRegressionMetric>,
    /// Whether every metric passed.
    pub passed: bool,
}

/// Compares two reports using one explicit regression policy.
pub fn compare_benchmarks(
    baseline: &ProviderBenchmarkReport,
    candidate: &ProviderBenchmarkReport,
    policy: BenchmarkRegressionPolicy,
) -> BenchmarkRegressionComparison {
    let mut metrics = vec![
        larger_is_better(
            "success_rate",
            Some(baseline.success_rate),
            Some(candidate.success_rate),
            baseline.success_rate - policy.max_success_rate_drop,
        ),
        larger_is_better(
            "throughput_per_second",
            Some(baseline.throughput_per_second),
            Some(candidate.throughput_per_second),
            baseline.throughput_per_second * (1.0 - policy.max_throughput_drop),
        ),
        smaller_is_better(
            "p95_ttft_us",
            baseline.ttft.map(|latency| u64_as_f64(latency.p95_us)),
            candidate.ttft.map(|latency| u64_as_f64(latency.p95_us)),
            policy.max_p95_ttft_increase,
        ),
        smaller_is_better(
            "p95_total_latency_us",
            baseline
                .total_latency
                .map(|latency| u64_as_f64(latency.p95_us)),
            candidate
                .total_latency
                .map(|latency| u64_as_f64(latency.p95_us)),
            policy.max_p95_total_latency_increase,
        ),
    ];
    let passed = metrics.iter().all(|metric| metric.passed);
    BenchmarkRegressionComparison {
        baseline: baseline.label.clone(),
        candidate: candidate.label.clone(),
        metrics: std::mem::take(&mut metrics),
        passed,
    }
}

async fn run_model(model: &dyn Model, request: ModelRequest) -> BenchmarkInvocation {
    let started = Instant::now();
    let mut stream = match model.stream(request, ModelCallContext::new()).await {
        Ok(stream) => stream,
        Err(error) => {
            return BenchmarkInvocation::failure((&error.kind).into(), started.elapsed());
        }
    };
    let mut accumulator = ModelStreamAccumulator::new();
    let mut ttft = None;
    while let Some(item) = stream.next().await {
        let event = match item {
            Ok(event) => event,
            Err(error) => {
                return BenchmarkInvocation::failure((&error.kind).into(), started.elapsed());
            }
        };
        if ttft.is_none() && is_model_output(&event) {
            ttft = Some(started.elapsed());
        }
        match accumulator.push(event) {
            Ok(Some(_)) => {
                return BenchmarkInvocation::success(ttft, started.elapsed()).unwrap_or_else(
                    |_| {
                        BenchmarkInvocation::failure(BenchmarkFailureKind::Other, started.elapsed())
                    },
                );
            }
            Ok(None) => {}
            Err(error) => {
                return BenchmarkInvocation::failure((&error.kind).into(), started.elapsed());
            }
        }
    }
    BenchmarkInvocation::failure(BenchmarkFailureKind::Stream, started.elapsed())
}

fn is_model_output(event: &ModelStreamEvent) -> bool {
    matches!(
        event,
        ModelStreamEvent::TextDelta { .. }
            | ModelStreamEvent::ReasoningDelta { .. }
            | ModelStreamEvent::ToolArgumentsDelta { .. }
            | ModelStreamEvent::RefusalDelta { .. }
            | ModelStreamEvent::ContentPartCompleted { .. }
    )
}

fn build_report(
    label: String,
    plan: &BenchmarkPlan,
    wall_time: Duration,
    outcomes: &[BenchmarkInvocation],
) -> ProviderBenchmarkReport {
    let mut total = Vec::new();
    let mut ttft = Vec::new();
    let mut successes_without_output = 0;
    let mut failures = BTreeMap::new();
    for outcome in outcomes {
        if let Some(kind) = outcome.failure {
            *failures.entry(kind).or_insert(0) += 1;
        } else {
            total.push(duration_us(outcome.total));
            if let Some(value) = outcome.ttft {
                ttft.push(duration_us(value));
            } else {
                successes_without_output += 1;
            }
        }
    }
    let successes = total.len();
    let failure_count = outcomes.len() - successes;
    let wall_seconds = wall_time.as_secs_f64().max(f64::EPSILON);
    ProviderBenchmarkReport {
        schema_version: REPORT_SCHEMA_VERSION,
        label,
        measured_runs: outcomes.len(),
        concurrency: plan.concurrency.get(),
        environment: plan.environment.clone(),
        successes,
        failures: failure_count,
        success_rate: usize_as_f64(successes) / usize_as_f64(outcomes.len()),
        wall_time_us: duration_us(wall_time),
        throughput_per_second: usize_as_f64(outcomes.len()) / wall_seconds,
        total_latency: distribution(&mut total),
        ttft: distribution(&mut ttft),
        successes_without_output,
        failure_counts: failures
            .into_iter()
            .map(|(kind, count)| BenchmarkFailureCount { kind, count })
            .collect(),
    }
}

fn distribution(values: &mut [u64]) -> Option<LatencyDistribution> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    Some(LatencyDistribution {
        min_us: values[0],
        p50_us: nearest_rank(values, 50),
        p95_us: nearest_rank(values, 95),
        p99_us: nearest_rank(values, 99),
        max_us: values[values.len() - 1],
    })
}

fn nearest_rank(values: &[u64], percentile: usize) -> u64 {
    let rank = values.len().saturating_mul(percentile).saturating_add(99) / 100;
    values[rank.saturating_sub(1).min(values.len() - 1)]
}

fn duration_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn usize_as_f64(value: usize) -> f64 {
    value.to_string().parse().unwrap_or(f64::MAX)
}

fn u64_as_f64(value: u64) -> f64 {
    value.to_string().parse().unwrap_or(f64::MAX)
}

fn larger_is_better(
    name: &str,
    baseline: Option<f64>,
    candidate: Option<f64>,
    minimum: f64,
) -> BenchmarkRegressionMetric {
    BenchmarkRegressionMetric {
        name: name.into(),
        baseline,
        candidate,
        passed: candidate.is_some_and(|candidate| candidate >= minimum),
    }
}

fn smaller_is_better(
    name: &str,
    baseline: Option<f64>,
    candidate: Option<f64>,
    allowed_increase: f64,
) -> BenchmarkRegressionMetric {
    let passed = match (baseline, candidate) {
        (Some(baseline), Some(candidate)) => candidate <= baseline * (1.0 + allowed_increase),
        (None, None) => true,
        _ => false,
    };
    BenchmarkRegressionMetric {
        name: name.into(),
        baseline,
        candidate,
        passed,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures_executor::block_on;

    use super::*;

    struct SequenceTarget {
        next: AtomicUsize,
        outcomes: Vec<BenchmarkInvocation>,
    }

    impl BenchmarkTarget for SequenceTarget {
        fn execute(&self) -> BenchmarkFuture<'_> {
            let index = self.next.fetch_add(1, Ordering::Relaxed) % self.outcomes.len();
            let outcome = self.outcomes[index];
            Box::pin(async move { outcome })
        }
    }

    fn success(ttft_us: u64, total_us: u64) -> BenchmarkInvocation {
        BenchmarkInvocation::success(
            Some(Duration::from_micros(ttft_us)),
            Duration::from_micros(total_us),
        )
        .unwrap()
    }

    #[test]
    fn benchmark_report_is_stable_bounded_and_failure_aware() {
        let target = Arc::new(SequenceTarget {
            next: AtomicUsize::new(0),
            outcomes: vec![
                success(10, 20),
                success(20, 40),
                success(30, 60),
                BenchmarkInvocation::failure(
                    BenchmarkFailureKind::Transport,
                    Duration::from_micros(5),
                ),
            ],
        });
        let plan = BenchmarkPlan::new(NonZeroUsize::new(4).unwrap())
            .with_concurrency(NonZeroUsize::new(2).unwrap());

        let report = block_on(benchmark("candidate", target, plan)).unwrap();

        assert_eq!(report.schema_version, 1);
        assert_eq!(report.successes, 3);
        assert_eq!(report.failures, 1);
        assert!((report.success_rate - 0.75).abs() < f64::EPSILON);
        assert_eq!(report.ttft.unwrap().p95_us, 30);
        assert_eq!(report.total_latency.unwrap().p50_us, 40);
        assert_eq!(
            report.failure_counts,
            vec![BenchmarkFailureCount {
                kind: BenchmarkFailureKind::Transport,
                count: 1,
            }]
        );
        assert!(serde_json::to_value(report).is_ok());
    }

    #[test]
    fn regression_gate_rejects_slow_or_unreliable_candidates() {
        let baseline = report("baseline", 1.0, 100.0, 10, 20);
        let candidate = report("candidate", 0.8, 70.0, 15, 30);
        let policy = BenchmarkRegressionPolicy::new(0.05, 0.1, 0.1, 0.1).unwrap();

        let comparison = compare_benchmarks(&baseline, &candidate, policy);

        assert!(!comparison.passed);
        assert!(comparison.metrics.iter().all(|metric| !metric.passed));
    }

    fn report(
        label: &str,
        success_rate: f64,
        throughput: f64,
        p95_ttft: u64,
        p95_total: u64,
    ) -> ProviderBenchmarkReport {
        ProviderBenchmarkReport {
            schema_version: 1,
            label: label.into(),
            measured_runs: 10,
            concurrency: 1,
            environment: BTreeMap::new(),
            successes: 10,
            failures: 0,
            success_rate,
            wall_time_us: 100,
            throughput_per_second: throughput,
            total_latency: Some(LatencyDistribution {
                min_us: p95_total,
                p50_us: p95_total,
                p95_us: p95_total,
                p99_us: p95_total,
                max_us: p95_total,
            }),
            ttft: Some(LatencyDistribution {
                min_us: p95_ttft,
                p50_us: p95_ttft,
                p95_us: p95_ttft,
                p99_us: p95_ttft,
                max_us: p95_ttft,
            }),
            successes_without_output: 0,
            failure_counts: Vec::new(),
        }
    }
}
