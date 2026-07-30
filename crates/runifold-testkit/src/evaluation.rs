use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
};

use futures_util::{StreamExt, stream};
use runifold_core::RunId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Owned asynchronous evaluation operation.
pub type EvaluationFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Evaluation configuration or execution failure.
#[derive(Clone, Debug, Error, PartialEq)]
#[non_exhaustive]
pub enum EvaluationError {
    /// A required name or version was empty.
    #[error("{field} must not be empty")]
    EmptyField {
        /// Invalid field name.
        field: &'static str,
    },
    /// A dataset contains no cases.
    #[error("evaluation dataset must contain at least one case")]
    EmptyDataset,
    /// A rule scorer contains no rules.
    #[error("evaluation rule scorer must contain at least one rule")]
    EmptyRules,
    /// A serialized report contradicts its per-case evidence.
    #[error("evaluation report is inconsistent: {message}")]
    InconsistentReport {
        /// Stable inconsistency explanation.
        message: &'static str,
    },
    /// A runner without scorers cannot measure quality.
    #[error("evaluation runner must contain at least one scorer")]
    NoScorers,
    /// A case identifier occurs more than once.
    #[error("duplicate evaluation case id: {case_id}")]
    DuplicateCase {
        /// Duplicated case identifier.
        case_id: String,
    },
    /// A scorer name occurs more than once.
    #[error("duplicate evaluation scorer name: {scorer}")]
    DuplicateScorer {
        /// Duplicated scorer name.
        scorer: String,
    },
    /// A score or threshold is not finite and within zero through one.
    #[error("{field} must be finite and between 0 and 1, got {value}")]
    InvalidRatio {
        /// Invalid field name.
        field: &'static str,
        /// Rejected value.
        value: f64,
    },
    /// A duration or monetary metric was negative or non-finite.
    #[error("{field} must be finite and non-negative, got {value}")]
    InvalidMetric {
        /// Invalid field name.
        field: &'static str,
        /// Rejected value.
        value: f64,
    },
    /// The evaluated target failed to produce an output.
    #[error("evaluation target failed: {message}")]
    Target {
        /// Safe failure explanation.
        message: String,
    },
    /// A scorer could not evaluate one output.
    #[error("evaluation scorer {scorer} failed: {message}")]
    Scorer {
        /// Stable scorer name.
        scorer: String,
        /// Safe failure explanation.
        message: String,
    },
    /// Reports from different dataset identities cannot be compared.
    #[error(
        "evaluation dataset mismatch: baseline {baseline_name}@{baseline_version}, candidate {candidate_name}@{candidate_version}"
    )]
    DatasetMismatch {
        /// Baseline dataset name.
        baseline_name: String,
        /// Baseline dataset version.
        baseline_version: String,
        /// Candidate dataset name.
        candidate_name: String,
        /// Candidate dataset version.
        candidate_version: String,
    },
}

/// Stable case identity within a dataset.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct EvaluationCaseId(String);

impl EvaluationCaseId {
    /// Creates a non-empty case identifier.
    ///
    /// # Errors
    ///
    /// Returns [`EvaluationError::EmptyField`] for an empty identifier.
    pub fn new(value: impl Into<String>) -> Result<Self, EvaluationError> {
        let value = value.into();
        ensure_not_empty("case id", &value)?;
        Ok(Self(value))
    }

    /// Returns the identifier text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EvaluationCaseId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// One immutable evaluation input and its optional reference answer.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EvaluationCase {
    id: EvaluationCaseId,
    input: Value,
    expected: Option<Value>,
    tags: BTreeSet<String>,
}

impl EvaluationCase {
    /// Creates one case without a reference answer.
    ///
    /// # Errors
    ///
    /// Returns an error when `id` is empty.
    pub fn new(id: impl Into<String>, input: Value) -> Result<Self, EvaluationError> {
        Ok(Self {
            id: EvaluationCaseId::new(id)?,
            input,
            expected: None,
            tags: BTreeSet::new(),
        })
    }

    /// Adds a reference answer for deterministic scorers.
    #[must_use]
    pub fn with_expected(mut self, expected: Value) -> Self {
        self.expected = Some(expected);
        self
    }

    /// Adds a low-cardinality dataset tag.
    ///
    /// # Errors
    ///
    /// Returns an error when the tag is empty.
    pub fn with_tag(mut self, tag: impl Into<String>) -> Result<Self, EvaluationError> {
        let tag = tag.into();
        ensure_not_empty("case tag", &tag)?;
        self.tags.insert(tag);
        Ok(self)
    }

    /// Returns the case identifier.
    pub const fn id(&self) -> &EvaluationCaseId {
        &self.id
    }

    /// Returns the target input.
    pub const fn input(&self) -> &Value {
        &self.input
    }

    /// Returns the optional reference answer.
    pub const fn expected(&self) -> Option<&Value> {
        self.expected.as_ref()
    }

    /// Returns stable case tags.
    pub const fn tags(&self) -> &BTreeSet<String> {
        &self.tags
    }
}

/// Versioned, duplicate-free evaluation dataset.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EvaluationDataset {
    name: String,
    version: String,
    cases: Vec<EvaluationCase>,
}

impl EvaluationDataset {
    /// Creates a non-empty versioned dataset.
    ///
    /// # Errors
    ///
    /// Returns an error for empty fields, no cases, or duplicate case IDs.
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
        cases: Vec<EvaluationCase>,
    ) -> Result<Self, EvaluationError> {
        let name = name.into();
        let version = version.into();
        ensure_not_empty("dataset name", &name)?;
        ensure_not_empty("dataset version", &version)?;
        if cases.is_empty() {
            return Err(EvaluationError::EmptyDataset);
        }
        let mut ids = BTreeSet::new();
        for case in &cases {
            ensure_not_empty("case id", case.id.as_str())?;
            for tag in &case.tags {
                ensure_not_empty("case tag", tag)?;
            }
            if !ids.insert(case.id.clone()) {
                return Err(EvaluationError::DuplicateCase {
                    case_id: case.id.to_string(),
                });
            }
        }
        Ok(Self {
            name,
            version,
            cases,
        })
    }

    /// Returns the dataset name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the dataset version.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Returns cases in stable dataset order.
    pub fn cases(&self) -> &[EvaluationCase] {
        &self.cases
    }

    /// Validates a dataset loaded from an external artifact.
    ///
    /// # Errors
    ///
    /// Returns the same invariant errors as [`Self::new`].
    pub fn validate(&self) -> Result<(), EvaluationError> {
        Self::new(&self.name, &self.version, self.cases.clone()).map(|_| ())
    }
}

/// Target output plus optional Run/Trace correlation.
#[derive(Clone, Debug)]
pub struct EvaluationOutput {
    value: Value,
    run_id: Option<RunId>,
    metadata: BTreeMap<String, Value>,
    metrics: Option<EvaluationMetrics>,
}

impl EvaluationOutput {
    /// Creates an output without Run correlation.
    pub fn new(value: Value) -> Self {
        Self {
            value,
            run_id: None,
            metadata: BTreeMap::new(),
            metrics: None,
        }
    }

    /// Correlates this output with a Runifold Run and its trace.
    #[must_use]
    pub const fn with_run_id(mut self, run_id: RunId) -> Self {
        self.run_id = Some(run_id);
        self
    }

    /// Adds scorer-visible metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the metadata key is empty.
    pub fn with_metadata(
        mut self,
        key: impl Into<String>,
        value: Value,
    ) -> Result<Self, EvaluationError> {
        let key = key.into();
        ensure_not_empty("output metadata key", &key)?;
        self.metadata.insert(key, value);
        Ok(self)
    }

    /// Returns the canonical output value.
    pub const fn value(&self) -> &Value {
        &self.value
    }

    /// Returns the correlated Run identifier.
    pub const fn run_id(&self) -> Option<RunId> {
        self.run_id
    }

    /// Returns scorer-visible metadata.
    pub const fn metadata(&self) -> &BTreeMap<String, Value> {
        &self.metadata
    }

    /// Attaches host-measured latency and optional Candidate usage.
    #[must_use]
    pub const fn with_metrics(mut self, metrics: EvaluationMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Returns resource evidence associated with this execution.
    pub const fn metrics(&self) -> Option<&EvaluationMetrics> {
        self.metrics.as_ref()
    }
}

/// Non-sensitive resource evidence for one successful target execution.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EvaluationMetrics {
    /// Host-observed target duration in milliseconds.
    pub duration_ms: f64,
    /// Provider-reported input tokens when available.
    pub input_tokens: Option<u64>,
    /// Provider-reported output tokens when available.
    pub output_tokens: Option<u64>,
    /// Provider- or application-reported cost in US dollars.
    pub cost_usd: Option<f64>,
}

impl EvaluationMetrics {
    /// Creates metrics with host-observed duration and no Provider usage.
    ///
    /// # Errors
    ///
    /// Returns an error when duration is negative or non-finite.
    pub fn new(duration_ms: f64) -> Result<Self, EvaluationError> {
        ensure_non_negative("evaluation duration milliseconds", duration_ms)?;
        Ok(Self {
            duration_ms,
            input_tokens: None,
            output_tokens: None,
            cost_usd: None,
        })
    }

    /// Adds Provider token usage.
    #[must_use]
    pub const fn with_tokens(mut self, input_tokens: u64, output_tokens: u64) -> Self {
        self.input_tokens = Some(input_tokens);
        self.output_tokens = Some(output_tokens);
        self
    }

    /// Adds monetary cost.
    ///
    /// # Errors
    ///
    /// Returns an error when cost is negative or non-finite.
    pub fn with_cost_usd(mut self, cost_usd: f64) -> Result<Self, EvaluationError> {
        ensure_non_negative("evaluation cost USD", cost_usd)?;
        self.cost_usd = Some(cost_usd);
        Ok(self)
    }

    fn validate(&self) -> Result<(), EvaluationError> {
        ensure_non_negative("evaluation duration milliseconds", self.duration_ms)?;
        if self.input_tokens.is_some() != self.output_tokens.is_some() {
            return Err(EvaluationError::InconsistentReport {
                message: "evaluation token metrics must include input and output together",
            });
        }
        if let Some(cost_usd) = self.cost_usd {
            ensure_non_negative("evaluation cost USD", cost_usd)?;
        }
        Ok(())
    }
}

/// Asynchronous system-under-evaluation boundary.
pub trait EvaluationTarget: Send + Sync {
    /// Executes one owned case.
    fn execute(
        &self,
        case: EvaluationCase,
    ) -> EvaluationFuture<Result<EvaluationOutput, EvaluationError>>;
}

impl<F, Fut> EvaluationTarget for F
where
    F: Fn(EvaluationCase) -> Fut + Send + Sync,
    Fut: Future<Output = Result<EvaluationOutput, EvaluationError>> + Send + 'static,
{
    fn execute(
        &self,
        case: EvaluationCase,
    ) -> EvaluationFuture<Result<EvaluationOutput, EvaluationError>> {
        Box::pin(self(case))
    }
}

/// Validated score value and optional evaluator rationale.
#[derive(Clone, Debug)]
pub struct ScoreValue {
    value: f64,
    rationale: Option<String>,
}

impl ScoreValue {
    /// Creates a finite score between zero and one.
    ///
    /// # Errors
    ///
    /// Returns [`EvaluationError::InvalidRatio`] for an invalid value.
    pub fn new(value: f64) -> Result<Self, EvaluationError> {
        ensure_ratio("score", value)?;
        Ok(Self {
            value,
            rationale: None,
        })
    }

    /// Returns the normalized score.
    pub const fn value(&self) -> f64 {
        self.value
    }

    /// Returns the optional evaluator rationale.
    pub fn rationale(&self) -> Option<&str> {
        self.rationale.as_deref()
    }

    /// Adds an evaluator rationale.
    #[must_use]
    pub fn with_rationale(mut self, rationale: impl Into<String>) -> Self {
        self.rationale = Some(rationale.into());
        self
    }
}

/// Asynchronous scorer boundary.
pub trait EvaluationScorer: Send + Sync {
    /// Stable score name.
    fn name(&self) -> &str;

    /// Per-case passing threshold.
    fn threshold(&self) -> f64;

    /// Scores one target output.
    fn score(
        &self,
        case: EvaluationCase,
        output: EvaluationOutput,
    ) -> EvaluationFuture<Result<ScoreValue, EvaluationError>>;
}

/// Closure-backed asynchronous scorer.
pub struct FnScorer<F> {
    name: String,
    threshold: f64,
    scorer: F,
}

impl<F> FnScorer<F> {
    /// Creates a scorer with a stable name and per-case threshold.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty name or invalid threshold.
    pub fn new(
        name: impl Into<String>,
        threshold: f64,
        scorer: F,
    ) -> Result<Self, EvaluationError> {
        let name = name.into();
        ensure_not_empty("scorer name", &name)?;
        ensure_ratio("score threshold", threshold)?;
        Ok(Self {
            name,
            threshold,
            scorer,
        })
    }
}

impl<F, Fut> EvaluationScorer for FnScorer<F>
where
    F: Fn(EvaluationCase, EvaluationOutput) -> Fut + Send + Sync,
    Fut: Future<Output = Result<ScoreValue, EvaluationError>> + Send + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn threshold(&self) -> f64 {
        self.threshold
    }

    fn score(
        &self,
        case: EvaluationCase,
        output: EvaluationOutput,
    ) -> EvaluationFuture<Result<ScoreValue, EvaluationError>> {
        Box::pin((self.scorer)(case, output))
    }
}

/// Deterministic JSON equality scorer.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonExactMatchScorer;

impl EvaluationScorer for JsonExactMatchScorer {
    fn name(&self) -> &'static str {
        "json_exact_match"
    }

    fn threshold(&self) -> f64 {
        1.0
    }

    fn score(
        &self,
        case: EvaluationCase,
        output: EvaluationOutput,
    ) -> EvaluationFuture<Result<ScoreValue, EvaluationError>> {
        Box::pin(async move {
            let expected = case.expected.ok_or_else(|| EvaluationError::Scorer {
                scorer: "json_exact_match".into(),
                message: "case has no reference answer".into(),
            })?;
            ScoreValue::new(if expected == output.value { 1.0 } else { 0.0 })
        })
    }
}

/// One persisted per-case score.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EvaluationScore {
    /// Stable scorer name.
    pub name: String,
    /// Normalized score from zero through one.
    pub value: f64,
    /// Per-case passing threshold.
    pub threshold: f64,
    /// Whether this score meets its threshold.
    pub passed: bool,
    /// Optional evaluator explanation.
    pub rationale: Option<String>,
}

mod regression;
mod runner;
#[cfg(test)]
mod tests;
mod validation;

pub use regression::{MetricRegression, RegressionComparison, RegressionPolicy};
pub use runner::EvaluationRunner;
use validation::{
    ensure_close, ensure_non_negative, ensure_not_empty, ensure_ratio, validate_case_metrics,
};

/// Evaluation failure stage.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum EvaluationFailureStage {
    /// The target did not produce an output.
    Target,
    /// One scorer failed.
    Scorer,
}

/// Safe per-case execution or scorer failure.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EvaluationFailure {
    /// Failure stage.
    pub stage: EvaluationFailureStage,
    /// Scorer name for scorer failures.
    pub scorer: Option<String>,
    /// Safe operator-facing explanation.
    pub message: String,
}

/// Output-free per-case evaluation result.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EvaluationCaseResult {
    /// Stable case identity.
    pub case_id: EvaluationCaseId,
    /// Run/Trace correlation when the target supplied one.
    pub run_id: Option<RunId>,
    /// Host latency and optional Provider usage for successful execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<EvaluationMetrics>,
    /// Successful scores sorted by scorer name.
    pub scores: Vec<EvaluationScore>,
    /// Target and scorer failures.
    pub failures: Vec<EvaluationFailure>,
}

/// Aggregate score statistics.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EvaluationScoreSummary {
    /// Stable scorer name.
    pub name: String,
    /// Cases that produced this score.
    pub scored_cases: usize,
    /// Total cases in the dataset.
    pub total_cases: usize,
    /// Mean over successfully scored cases.
    pub mean: f64,
    /// Passing cases divided by all dataset cases.
    pub pass_rate: f64,
}

/// Deterministic candidate evaluation report.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EvaluationReport {
    /// Dataset name.
    pub dataset_name: String,
    /// Dataset version.
    pub dataset_version: String,
    /// Candidate model, prompt, Agent, or application version.
    pub candidate_version: String,
    /// Target executions that produced output.
    pub execution_success_rate: f64,
    /// Case results in dataset order.
    pub cases: Vec<EvaluationCaseResult>,
    /// Score summaries sorted by scorer name.
    pub summaries: Vec<EvaluationScoreSummary>,
}

impl EvaluationReport {
    /// Serializes this output-free report as stable, pretty JSON.
    ///
    /// # Errors
    ///
    /// Returns a JSON error only if serialization unexpectedly fails.
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Compares this candidate with a baseline of the same dataset identity.
    ///
    /// # Errors
    ///
    /// Returns [`EvaluationError::DatasetMismatch`] when dataset identities
    /// differ.
    pub fn compare(
        &self,
        baseline: &Self,
        policy: &RegressionPolicy,
    ) -> Result<RegressionComparison, EvaluationError> {
        self.validate()?;
        baseline.validate()?;
        policy.validate()?;
        if self.dataset_name != baseline.dataset_name
            || self.dataset_version != baseline.dataset_version
        {
            return Err(EvaluationError::DatasetMismatch {
                baseline_name: baseline.dataset_name.clone(),
                baseline_version: baseline.dataset_version.clone(),
                candidate_name: self.dataset_name.clone(),
                candidate_version: self.dataset_version.clone(),
            });
        }
        let metrics = baseline
            .summaries
            .iter()
            .map(|baseline_summary| {
                let candidate = self
                    .summaries
                    .iter()
                    .find(|summary| summary.name == baseline_summary.name);
                let candidate_mean = candidate.map_or(0.0, |summary| summary.mean);
                let candidate_pass_rate = candidate.map_or(0.0, |summary| summary.pass_rate);
                let mean_delta = candidate_mean - baseline_summary.mean;
                let pass_rate_delta = candidate_pass_rate - baseline_summary.pass_rate;
                MetricRegression {
                    name: baseline_summary.name.clone(),
                    baseline_mean: baseline_summary.mean,
                    candidate_mean,
                    mean_delta,
                    baseline_pass_rate: baseline_summary.pass_rate,
                    candidate_pass_rate,
                    pass_rate_delta,
                    passed: mean_delta >= -policy.max_mean_drop
                        && pass_rate_delta >= -policy.max_pass_rate_drop,
                }
            })
            .collect::<Vec<_>>();
        let execution_success_rate_delta =
            self.execution_success_rate - baseline.execution_success_rate;
        let passed = execution_success_rate_delta >= -policy.max_execution_success_drop
            && metrics.iter().all(|metric| metric.passed);
        Ok(RegressionComparison {
            baseline_version: baseline.candidate_version.clone(),
            candidate_version: self.candidate_version.clone(),
            execution_success_rate_delta,
            metrics,
            passed,
        })
    }

    /// Validates a report loaded from an external artifact.
    ///
    /// # Errors
    ///
    /// Returns an error for empty identities, invalid ratios, or duplicate
    /// score summaries.
    pub fn validate(&self) -> Result<(), EvaluationError> {
        ensure_not_empty("dataset name", &self.dataset_name)?;
        ensure_not_empty("dataset version", &self.dataset_version)?;
        ensure_not_empty("candidate version", &self.candidate_version)?;
        ensure_ratio("execution success rate", self.execution_success_rate)?;
        if self.cases.is_empty() {
            return Err(EvaluationError::InconsistentReport {
                message: "report contains no cases",
            });
        }
        let total_cases = self.cases.iter().fold(0.0, |total, _| total + 1.0);
        let mut successful_cases = 0.0;
        let mut case_ids = BTreeSet::new();
        let mut aggregate = BTreeMap::<&str, (usize, f64, f64, f64)>::new();
        for case in &self.cases {
            ensure_not_empty("report case id", case.case_id.as_str())?;
            if !case_ids.insert(case.case_id.as_str()) {
                return Err(EvaluationError::InconsistentReport {
                    message: "report contains duplicate case IDs",
                });
            }
            if !case
                .failures
                .iter()
                .any(|failure| failure.stage == EvaluationFailureStage::Target)
            {
                successful_cases += 1.0;
            }
            validate_case_metrics(case)?;
            let mut score_names = BTreeSet::new();
            for score in &case.scores {
                ensure_not_empty("report score name", &score.name)?;
                ensure_ratio("report score", score.value)?;
                ensure_ratio("report score threshold", score.threshold)?;
                if score.passed != (score.value >= score.threshold) {
                    return Err(EvaluationError::InconsistentReport {
                        message: "stored score decision contradicts its threshold",
                    });
                }
                if !score_names.insert(score.name.as_str()) {
                    return Err(EvaluationError::InconsistentReport {
                        message: "one case contains duplicate score names",
                    });
                }
                let entry = aggregate.entry(&score.name).or_default();
                entry.0 += 1;
                entry.1 += score.value;
                entry.2 += 1.0;
                entry.3 += if score.passed { 1.0 } else { 0.0 };
            }
        }
        ensure_close(
            self.execution_success_rate,
            successful_cases / total_cases,
            "execution success rate contradicts case failures",
        )?;
        let mut names = BTreeSet::new();
        for summary in &self.summaries {
            ensure_not_empty("score summary name", &summary.name)?;
            ensure_ratio("score mean", summary.mean)?;
            ensure_ratio("score pass rate", summary.pass_rate)?;
            if !names.insert(summary.name.as_str()) {
                return Err(EvaluationError::DuplicateScorer {
                    scorer: summary.name.clone(),
                });
            }
            let Some((scored_cases, total, scored_cases_ratio, passed)) =
                aggregate.get(summary.name.as_str())
            else {
                return Err(EvaluationError::InconsistentReport {
                    message: "score summary has no per-case evidence",
                });
            };
            if summary.scored_cases != *scored_cases || summary.total_cases != self.cases.len() {
                return Err(EvaluationError::InconsistentReport {
                    message: "score summary case counts are inconsistent",
                });
            }
            ensure_close(
                summary.mean,
                total / scored_cases_ratio,
                "score summary mean contradicts case scores",
            )?;
            ensure_close(
                summary.pass_rate,
                passed / total_cases,
                "score summary pass rate contradicts case scores",
            )?;
        }
        if names.len() != aggregate.len() {
            return Err(EvaluationError::InconsistentReport {
                message: "per-case score is missing its summary",
            });
        }
        Ok(())
    }
}
