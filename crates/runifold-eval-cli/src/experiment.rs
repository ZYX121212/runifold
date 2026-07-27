use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use clap::Args;
use futures_util::{StreamExt, stream};
use runifold_testkit::{
    EvaluationCaseResult, EvaluationDataset, EvaluationFailureStage, EvaluationReport,
    EvaluationRunner, EvaluationScoreSummary, JsonExactMatchScorer, TokenOverlapScorer,
};
use serde::{Deserialize, Serialize};

use crate::{ScorerKind, candidate::ProcessCandidate, dataset, render};

mod cache;
mod output;
mod resources;

use resources::{ResourceBudget, ResourceStatistics};

const EXPERIMENT_SCHEMA_VERSION: u32 = 1;
const CONFIDENCE_LEVEL: f64 = 0.95;

#[derive(Debug, Args)]
pub(crate) struct ExperimentArgs {
    /// JSONL dataset path.
    #[arg(long)]
    dataset: PathBuf,
    /// Stable dataset name.
    #[arg(long)]
    dataset_name: String,
    /// Immutable dataset version.
    #[arg(long)]
    dataset_version: String,
    /// Candidate model, prompt, Agent, or application version.
    #[arg(long)]
    candidate_version: String,
    /// Output experiment JSON report.
    #[arg(long)]
    output: PathBuf,
    /// Optional `JUnit` XML output.
    #[arg(long)]
    junit: Option<PathBuf>,
    /// Optional Markdown summary output.
    #[arg(long)]
    markdown: Option<PathBuf>,
    /// Independent repetitions per case.
    #[arg(long, default_value = "5")]
    samples: NonZeroUsize,
    /// Base seed used to derive a stable seed for every case and sample.
    #[arg(long, default_value_t = 0)]
    seed: u64,
    /// Zero-based shard index.
    #[arg(long, default_value_t = 0)]
    shard_index: usize,
    /// Total deterministic shards.
    #[arg(long, default_value = "1")]
    shard_count: NonZeroUsize,
    /// Directory for fingerprinted per-sample checkpoints.
    #[arg(long)]
    cache_dir: Option<PathBuf>,
    /// Concurrent Candidate processes within one sample.
    #[arg(long, default_value = "4")]
    concurrency: NonZeroUsize,
    /// Per-case Candidate timeout in milliseconds.
    #[arg(long, default_value_t = 60_000)]
    timeout_ms: u64,
    /// Maximum Candidate stdout bytes per case.
    #[arg(long, default_value_t = 1_048_576)]
    max_output_bytes: usize,
    /// Built-in scorer.
    #[arg(long, value_enum, default_value_t = ScorerKind::Exact)]
    scorer: ScorerKind,
    /// Per-case threshold for the token-overlap scorer.
    #[arg(long, default_value_t = 1.0)]
    score_threshold: f64,
    /// Optional minimum 95% confidence lower bound for every score.
    #[arg(long)]
    min_confidence_lower_bound: Option<f64>,
    /// Maximum fraction of cases whose pass/fail result changes across samples.
    #[arg(long, default_value_t = 1.0)]
    max_flaky_case_rate: f64,
    /// Optional maximum p95 Candidate latency in milliseconds.
    #[arg(long)]
    max_p95_latency_ms: Option<f64>,
    /// Optional maximum total input plus output tokens.
    #[arg(long)]
    max_total_tokens: Option<u64>,
    /// Optional maximum total Candidate-reported cost in USD.
    #[arg(long)]
    max_total_cost_usd: Option<f64>,
    /// Candidate executable and arguments.
    #[arg(required = true, last = true)]
    candidate_command: Vec<OsString>,
}

#[derive(Debug, Args)]
pub(crate) struct MergeArgs {
    /// Complete shard experiment reports.
    #[arg(required = true, long, num_args = 1..)]
    inputs: Vec<PathBuf>,
    /// Output merged experiment JSON report.
    #[arg(long)]
    output: PathBuf,
    /// Optional `JUnit` XML output.
    #[arg(long)]
    junit: Option<PathBuf>,
    /// Optional Markdown summary output.
    #[arg(long)]
    markdown: Option<PathBuf>,
    /// Optional minimum 95% confidence lower bound for every score.
    #[arg(long)]
    min_confidence_lower_bound: Option<f64>,
    /// Maximum fraction of flaky cases.
    #[arg(long, default_value_t = 1.0)]
    max_flaky_case_rate: f64,
    /// Optional maximum p95 Candidate latency in milliseconds.
    #[arg(long)]
    max_p95_latency_ms: Option<f64>,
    /// Optional maximum total input plus output tokens.
    #[arg(long)]
    max_total_tokens: Option<u64>,
    /// Optional maximum total Candidate-reported cost in USD.
    #[arg(long)]
    max_total_cost_usd: Option<f64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Shard {
    index: usize,
    count: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct ExperimentScorer {
    name: String,
    threshold: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct ScoreStatistics {
    name: String,
    sample_count: usize,
    mean: f64,
    pass_rate: f64,
    standard_deviation: Option<f64>,
    confidence_level: f64,
    confidence_lower_bound: Option<f64>,
    confidence_upper_bound: Option<f64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ExperimentSample {
    index: usize,
    report: EvaluationReport,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ExperimentReport {
    schema_version: u32,
    dataset_name: String,
    dataset_version: String,
    candidate_version: String,
    base_seed: u64,
    requested_samples: usize,
    shard: Option<Shard>,
    scorer: ExperimentScorer,
    case_ids: Vec<String>,
    samples: Vec<ExperimentSample>,
    statistics: Vec<ScoreStatistics>,
    flaky_case_rate: f64,
    #[serde(default)]
    resources: ResourceStatistics,
}

pub(crate) async fn run(args: ExperimentArgs) -> Result<bool> {
    validate_ratio_option(
        "minimum confidence lower bound",
        args.min_confidence_lower_bound,
    )?;
    validate_ratio("maximum flaky case rate", args.max_flaky_case_rate)?;
    let budget = resource_budget(
        args.max_p95_latency_ms,
        args.max_total_tokens,
        args.max_total_cost_usd,
    )?;
    ensure!(
        args.shard_index < args.shard_count.get(),
        "shard index must be less than shard count"
    );
    let dataset =
        dataset::load_jsonl(&args.dataset, &args.dataset_name, &args.dataset_version).await?;
    let shard = (args.shard_count.get() > 1).then_some(Shard {
        index: args.shard_index,
        count: args.shard_count.get(),
    });
    let dataset = select_shard(&dataset, shard)?;
    let scorer = scorer_config(args.scorer, args.score_threshold)?;
    let fingerprint = cache::fingerprint(
        &dataset,
        &args.candidate_version,
        args.seed,
        shard,
        &scorer,
        &args.candidate_command,
        args.timeout_ms,
        args.max_output_bytes,
    )?;
    let mut samples = Vec::with_capacity(args.samples.get());
    for sample_index in 0..args.samples.get() {
        samples.push(execute_sample(&args, &dataset, &fingerprint, sample_index).await?);
    }
    let report = ExperimentReport::new(
        &dataset,
        args.candidate_version,
        args.seed,
        shard,
        scorer,
        args.samples.get(),
        samples,
    )?;
    output::write_report(
        &report,
        &args.output,
        args.junit.as_deref(),
        args.markdown.as_deref(),
        args.min_confidence_lower_bound,
        args.max_flaky_case_rate,
        &budget,
    )
    .await?;
    Ok(report.passes(
        args.min_confidence_lower_bound,
        args.max_flaky_case_rate,
        &budget,
    ))
}

async fn execute_sample(
    args: &ExperimentArgs,
    dataset: &EvaluationDataset,
    fingerprint: &str,
    sample_index: usize,
) -> Result<ExperimentSample> {
    let cached = match &args.cache_dir {
        Some(root) => {
            cache::load_sample(
                root,
                fingerprint,
                sample_index,
                dataset,
                &args.candidate_version,
            )
            .await?
        }
        None => None,
    };
    let report = if let Some(report) = cached {
        report
    } else {
        let report = execute_cases(args, dataset, fingerprint, sample_index).await?;
        if let Some(root) = &args.cache_dir {
            cache::store_sample(root, fingerprint, sample_index, &report).await?;
        }
        report
    };
    Ok(ExperimentSample {
        index: sample_index,
        report,
    })
}

async fn execute_cases(
    args: &ExperimentArgs,
    dataset: &EvaluationDataset,
    fingerprint: &str,
    sample_index: usize,
) -> Result<EvaluationReport> {
    let mut reports = stream::iter(dataset.cases().iter().cloned().enumerate())
        .map(|(index, case)| async move {
            let single = EvaluationDataset::new(dataset.name(), dataset.version(), vec![case])
                .context("single-case dataset invariants failed")?;
            let cached = match &args.cache_dir {
                Some(root) => {
                    cache::load_case(
                        root,
                        fingerprint,
                        sample_index,
                        &single,
                        &args.candidate_version,
                    )
                    .await?
                }
                None => None,
            };
            let report = if let Some(report) = cached {
                report
            } else {
                let report = evaluate_dataset(args, &single, sample_index).await?;
                if let Some(root) = &args.cache_dir {
                    cache::store_case(root, fingerprint, sample_index, &single, &report).await?;
                }
                report
            };
            Ok::<_, anyhow::Error>((index, report))
        })
        .buffer_unordered(args.concurrency.get())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    reports.sort_by_key(|(index, _)| *index);
    let cases = reports
        .into_iter()
        .flat_map(|(_, report)| report.cases)
        .collect();
    Ok(build_evaluation_report(
        dataset.name(),
        dataset.version(),
        &args.candidate_version,
        cases,
    ))
}

async fn evaluate_dataset(
    args: &ExperimentArgs,
    dataset: &EvaluationDataset,
    sample_index: usize,
) -> Result<EvaluationReport> {
    let candidate = ProcessCandidate::new(
        args.candidate_command.clone(),
        Duration::from_millis(args.timeout_ms),
        args.max_output_bytes,
    )
    .context("invalid Candidate process configuration")?
    .with_sample_context(sample_index, args.seed);
    let runner = EvaluationRunner::new(candidate);
    match args.scorer {
        ScorerKind::Exact => Ok(runner
            .with_scorer(JsonExactMatchScorer)
            .run(dataset, &args.candidate_version)
            .await?),
        ScorerKind::TokenOverlap => Ok(runner
            .with_scorer(TokenOverlapScorer::new(
                "token_overlap",
                args.score_threshold,
            )?)
            .run(dataset, &args.candidate_version)
            .await?),
    }
}

pub(crate) async fn merge(args: MergeArgs) -> Result<bool> {
    validate_ratio_option(
        "minimum confidence lower bound",
        args.min_confidence_lower_bound,
    )?;
    validate_ratio("maximum flaky case rate", args.max_flaky_case_rate)?;
    let budget = resource_budget(
        args.max_p95_latency_ms,
        args.max_total_tokens,
        args.max_total_cost_usd,
    )?;
    let mut reports = Vec::with_capacity(args.inputs.len());
    for path in &args.inputs {
        reports.push(load_report(path).await?);
    }
    let report = ExperimentReport::merge(reports)?;
    output::write_report(
        &report,
        &args.output,
        args.junit.as_deref(),
        args.markdown.as_deref(),
        args.min_confidence_lower_bound,
        args.max_flaky_case_rate,
        &budget,
    )
    .await?;
    Ok(report.passes(
        args.min_confidence_lower_bound,
        args.max_flaky_case_rate,
        &budget,
    ))
}

impl ExperimentReport {
    fn new(
        dataset: &EvaluationDataset,
        candidate_version: String,
        base_seed: u64,
        shard: Option<Shard>,
        scorer: ExperimentScorer,
        requested_samples: usize,
        samples: Vec<ExperimentSample>,
    ) -> Result<Self> {
        let case_ids = dataset
            .cases()
            .iter()
            .map(|case| case.id().as_str().to_owned())
            .collect();
        let statistics = statistics(&samples);
        let flaky_case_rate = flaky_case_rate(&samples);
        let resources = resources::summarize(&samples)?;
        let report = Self {
            schema_version: EXPERIMENT_SCHEMA_VERSION,
            dataset_name: dataset.name().to_owned(),
            dataset_version: dataset.version().to_owned(),
            candidate_version,
            base_seed,
            requested_samples,
            shard,
            scorer,
            case_ids,
            samples,
            statistics,
            flaky_case_rate,
            resources,
        };
        report.validate()?;
        Ok(report)
    }

    fn merge(mut reports: Vec<Self>) -> Result<Self> {
        ensure!(!reports.is_empty(), "at least one shard report is required");
        for report in &reports {
            report.validate()?;
        }
        reports.sort_by_key(|report| report.shard.map(|shard| shard.index));
        let first = reports.first().context("missing first shard report")?;
        let shard_count = first
            .shard
            .context("merge inputs must be unmerged shard reports")?
            .count;
        ensure!(
            reports.len() == shard_count,
            "merge requires exactly {shard_count} shard reports"
        );
        for (expected_index, report) in reports.iter().enumerate() {
            let shard = report
                .shard
                .context("merge inputs must be unmerged shard reports")?;
            ensure!(
                shard
                    == (Shard {
                        index: expected_index,
                        count: shard_count,
                    }),
                "merge inputs must cover every shard exactly once"
            );
            ensure!(
                report.dataset_name == first.dataset_name
                    && report.dataset_version == first.dataset_version
                    && report.candidate_version == first.candidate_version
                    && report.base_seed == first.base_seed
                    && report.requested_samples == first.requested_samples
                    && report.scorer == first.scorer,
                "shard experiment identities do not match"
            );
        }
        let mut case_ids = reports
            .iter()
            .flat_map(|report| report.case_ids.iter().cloned())
            .collect::<Vec<_>>();
        case_ids.sort();
        ensure!(
            case_ids.windows(2).all(|pair| pair[0] != pair[1]),
            "shard reports contain duplicate case IDs"
        );
        let mut samples = Vec::with_capacity(first.requested_samples);
        for sample_index in 0..first.requested_samples {
            let cases = reports
                .iter()
                .flat_map(|report| report.samples[sample_index].report.cases.iter().cloned())
                .collect::<Vec<_>>();
            samples.push(ExperimentSample {
                index: sample_index,
                report: combine_sample(first, cases),
            });
        }
        let report = Self {
            schema_version: EXPERIMENT_SCHEMA_VERSION,
            dataset_name: first.dataset_name.clone(),
            dataset_version: first.dataset_version.clone(),
            candidate_version: first.candidate_version.clone(),
            base_seed: first.base_seed,
            requested_samples: first.requested_samples,
            shard: None,
            scorer: first.scorer.clone(),
            case_ids,
            statistics: statistics(&samples),
            flaky_case_rate: flaky_case_rate(&samples),
            resources: resources::summarize(&samples)?,
            samples,
        };
        report.validate()?;
        Ok(report)
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == EXPERIMENT_SCHEMA_VERSION,
            "unsupported experiment schema version"
        );
        ensure!(
            self.requested_samples > 0 && self.samples.len() == self.requested_samples,
            "experiment sample count is inconsistent"
        );
        ensure!(!self.case_ids.is_empty(), "experiment contains no cases");
        ensure!(
            self.case_ids.iter().all(|id| !id.trim().is_empty()),
            "experiment contains an empty case ID"
        );
        ensure!(
            self.case_ids.iter().collect::<BTreeSet<_>>().len() == self.case_ids.len(),
            "experiment contains duplicate case IDs"
        );
        if let Some(shard) = self.shard {
            ensure!(
                shard.count > 1 && shard.index < shard.count,
                "experiment shard identity is invalid"
            );
            ensure!(
                self.case_ids
                    .iter()
                    .all(|case_id| stable_bucket(case_id, shard.count) == shard.index),
                "experiment contains a case assigned to a different shard"
            );
        }
        validate_ratio("experiment scorer threshold", self.scorer.threshold)?;
        validate_ratio("experiment flaky case rate", self.flaky_case_rate)?;
        for (expected_index, sample) in self.samples.iter().enumerate() {
            ensure!(
                sample.index == expected_index,
                "experiment sample indexes are inconsistent"
            );
            sample.report.validate()?;
            ensure!(
                sample.report.dataset_name == self.dataset_name
                    && sample.report.dataset_version == self.dataset_version
                    && sample.report.candidate_version == self.candidate_version,
                "sample report identity does not match experiment"
            );
            let ids = sample
                .report
                .cases
                .iter()
                .map(|case| case.case_id.as_str())
                .collect::<BTreeSet<_>>();
            ensure!(
                ids == self.case_ids.iter().map(String::as_str).collect(),
                "sample report cases do not match experiment"
            );
            ensure!(
                sample
                    .report
                    .cases
                    .iter()
                    .flat_map(|case| &case.scores)
                    .all(|score| score.name == self.scorer.name
                        && (score.threshold - self.scorer.threshold).abs() <= 1e-12),
                "sample score configuration does not match experiment"
            );
            ensure!(
                sample.report.summaries.len() <= 1
                    && sample
                        .report
                        .summaries
                        .iter()
                        .all(|summary| summary.name == self.scorer.name),
                "sample summary configuration does not match experiment"
            );
        }
        ensure!(
            statistics(&self.samples) == self.statistics,
            "experiment statistics contradict sample evidence"
        );
        ensure!(
            (flaky_case_rate(&self.samples) - self.flaky_case_rate).abs() <= 1e-12,
            "experiment flaky rate contradicts sample evidence"
        );
        ensure!(
            resources::summarize(&self.samples)? == self.resources,
            "experiment resource statistics contradict sample evidence"
        );
        Ok(())
    }

    fn passes(
        &self,
        minimum_lower_bound: Option<f64>,
        max_flaky_case_rate: f64,
        budget: &ResourceBudget,
    ) -> bool {
        self.samples
            .iter()
            .all(|sample| render::absolute_passed(&sample.report))
            && self.flaky_case_rate <= max_flaky_case_rate
            && budget.passes(&self.resources)
            && minimum_lower_bound.is_none_or(|minimum| {
                !self.statistics.is_empty()
                    && self.statistics.iter().all(|statistics| {
                        statistics
                            .confidence_lower_bound
                            .is_some_and(|lower| lower >= minimum)
                    })
            })
    }
}

fn select_shard(dataset: &EvaluationDataset, shard: Option<Shard>) -> Result<EvaluationDataset> {
    let Some(shard) = shard else {
        return Ok(dataset.clone());
    };
    let cases = dataset
        .cases()
        .iter()
        .filter(|case| stable_bucket(case.id().as_str(), shard.count) == shard.index)
        .cloned()
        .collect::<Vec<_>>();
    ensure!(
        !cases.is_empty(),
        "shard {} of {} contains no cases",
        shard.index,
        shard.count
    );
    EvaluationDataset::new(dataset.name(), dataset.version(), cases)
        .context("sharded dataset invariants failed")
}

fn stable_bucket(case_id: &str, count: usize) -> usize {
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&blake3::hash(case_id.as_bytes()).as_bytes()[..8]);
    usize::try_from(u64::from_le_bytes(bytes) % u64::try_from(count).unwrap_or(u64::MAX))
        .unwrap_or_default()
}

fn scorer_config(kind: ScorerKind, threshold: f64) -> Result<ExperimentScorer> {
    match kind {
        ScorerKind::Exact => Ok(ExperimentScorer {
            name: "json_exact_match".into(),
            threshold: 1.0,
        }),
        ScorerKind::TokenOverlap => {
            validate_ratio("token-overlap threshold", threshold)?;
            Ok(ExperimentScorer {
                name: "token_overlap".into(),
                threshold,
            })
        }
    }
}

fn statistics(samples: &[ExperimentSample]) -> Vec<ScoreStatistics> {
    let names = samples
        .iter()
        .flat_map(|sample| {
            sample
                .report
                .summaries
                .iter()
                .map(|summary| summary.name.clone())
        })
        .collect::<BTreeSet<_>>();
    names
        .into_iter()
        .filter_map(|name| {
            let summaries = samples
                .iter()
                .filter_map(|sample| {
                    sample
                        .report
                        .summaries
                        .iter()
                        .find(|summary| summary.name == name)
                })
                .collect::<Vec<_>>();
            if summaries.len() != samples.len() {
                return None;
            }
            let means = summaries
                .iter()
                .map(|summary| summary.mean)
                .collect::<Vec<_>>();
            let mean = arithmetic_mean(&means);
            let pass_rate = arithmetic_mean(
                &summaries
                    .iter()
                    .map(|summary| summary.pass_rate)
                    .collect::<Vec<_>>(),
            );
            let (standard_deviation, lower, upper) = confidence_interval(&means, mean);
            Some(ScoreStatistics {
                name,
                sample_count: samples.len(),
                mean,
                pass_rate,
                standard_deviation,
                confidence_level: CONFIDENCE_LEVEL,
                confidence_lower_bound: lower,
                confidence_upper_bound: upper,
            })
        })
        .collect()
}

fn arithmetic_mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.iter().fold(0.0, |count, _| count + 1.0)
}

fn confidence_interval(values: &[f64], mean: f64) -> (Option<f64>, Option<f64>, Option<f64>) {
    if values.len() < 2 {
        return (None, None, None);
    }
    let count = values.iter().fold(0.0, |total, _| total + 1.0);
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (count - 1.0);
    let standard_deviation = variance.sqrt();
    let margin = student_t_95(values.len() - 1) * standard_deviation / count.sqrt();
    (
        Some(standard_deviation),
        Some((mean - margin).clamp(0.0, 1.0)),
        Some((mean + margin).clamp(0.0, 1.0)),
    )
}

fn student_t_95(degrees_of_freedom: usize) -> f64 {
    const CRITICAL: [f64; 30] = [
        12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.160,
        2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056,
        2.052, 2.048, 2.045, 2.042,
    ];
    CRITICAL
        .get(degrees_of_freedom.saturating_sub(1))
        .copied()
        .unwrap_or(1.96)
}

fn flaky_case_rate(samples: &[ExperimentSample]) -> f64 {
    let Some(first) = samples.first() else {
        return 0.0;
    };
    let flaky = first
        .report
        .cases
        .iter()
        .filter(|case| {
            let decisions = samples
                .iter()
                .filter_map(|sample| {
                    sample
                        .report
                        .cases
                        .iter()
                        .find(|candidate| candidate.case_id == case.case_id)
                        .map(case_passed)
                })
                .collect::<BTreeSet<_>>();
            decisions.len() > 1
        })
        .count();
    let flaky = (0..flaky).fold(0.0, |total, _| total + 1.0);
    let total = first.report.cases.iter().fold(0.0, |total, _| total + 1.0);
    flaky / total
}

fn case_passed(case: &EvaluationCaseResult) -> bool {
    case.failures.is_empty()
        && !case.scores.is_empty()
        && case.scores.iter().all(|score| score.passed)
}

fn combine_sample(
    experiment: &ExperimentReport,
    cases: Vec<EvaluationCaseResult>,
) -> EvaluationReport {
    build_evaluation_report(
        &experiment.dataset_name,
        &experiment.dataset_version,
        &experiment.candidate_version,
        cases,
    )
}

fn build_evaluation_report(
    dataset_name: &str,
    dataset_version: &str,
    candidate_version: &str,
    mut cases: Vec<EvaluationCaseResult>,
) -> EvaluationReport {
    cases.sort_by(|left, right| left.case_id.cmp(&right.case_id));
    let total = cases.iter().fold(0.0, |total, _| total + 1.0);
    let execution_success_rate = cases
        .iter()
        .filter(|case| {
            !case
                .failures
                .iter()
                .any(|failure| failure.stage == EvaluationFailureStage::Target)
        })
        .fold(0.0, |count, _| count + 1.0)
        / total;
    let mut aggregate = BTreeMap::<String, (usize, f64, f64, f64)>::new();
    for score in cases.iter().flat_map(|case| &case.scores) {
        let entry = aggregate.entry(score.name.clone()).or_default();
        entry.0 += 1;
        entry.1 += score.value;
        entry.2 += 1.0;
        entry.3 += f64::from(score.passed);
    }
    let summaries = aggregate
        .into_iter()
        .map(
            |(name, (scored_cases, sum, scored_ratio, passed))| EvaluationScoreSummary {
                name,
                scored_cases,
                total_cases: cases.len(),
                mean: sum / scored_ratio,
                pass_rate: passed / total,
            },
        )
        .collect();
    EvaluationReport {
        dataset_name: dataset_name.to_owned(),
        dataset_version: dataset_version.to_owned(),
        candidate_version: candidate_version.to_owned(),
        execution_success_rate,
        cases,
        summaries,
    }
}

async fn load_report(path: &Path) -> Result<ExperimentReport> {
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("failed to read experiment {}", path.display()))?;
    let report = serde_json::from_slice::<ExperimentReport>(&bytes)
        .with_context(|| format!("invalid experiment JSON {}", path.display()))?;
    report
        .validate()
        .with_context(|| format!("experiment invariants failed {}", path.display()))?;
    Ok(report)
}

fn validate_ratio(name: &str, value: f64) -> Result<()> {
    if value.is_finite() && (0.0..=1.0).contains(&value) {
        Ok(())
    } else {
        bail!("{name} must be finite and between 0 and 1")
    }
}

fn validate_ratio_option(name: &str, value: Option<f64>) -> Result<()> {
    value.map_or(Ok(()), |value| validate_ratio(name, value))
}

fn resource_budget(
    max_p95_latency_ms: Option<f64>,
    max_total_tokens: Option<u64>,
    max_total_cost_usd: Option<f64>,
) -> Result<ResourceBudget> {
    let budget = ResourceBudget {
        latency_p95_ms: max_p95_latency_ms,
        total_tokens: max_total_tokens,
        total_cost_usd: max_total_cost_usd,
    };
    budget.validate()?;
    Ok(budget)
}

#[cfg(test)]
#[path = "experiment_tests.rs"]
mod tests;
