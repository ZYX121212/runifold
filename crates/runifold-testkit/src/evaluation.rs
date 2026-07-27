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

/// Allowed relative quality drops.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct RegressionPolicy {
    /// Maximum allowed mean score decrease.
    pub max_mean_drop: f64,
    /// Maximum allowed pass-rate decrease.
    pub max_pass_rate_drop: f64,
    /// Maximum allowed target execution-success decrease.
    pub max_execution_success_drop: f64,
}

impl RegressionPolicy {
    /// Creates a validated regression policy.
    ///
    /// # Errors
    ///
    /// Returns an error when any allowed drop is outside zero through one.
    pub fn new(
        max_mean_drop: f64,
        max_pass_rate_drop: f64,
        max_execution_success_drop: f64,
    ) -> Result<Self, EvaluationError> {
        let policy = Self {
            max_mean_drop,
            max_pass_rate_drop,
            max_execution_success_drop,
        };
        policy.validate()?;
        Ok(policy)
    }

    fn validate(&self) -> Result<(), EvaluationError> {
        ensure_ratio("maximum mean drop", self.max_mean_drop)?;
        ensure_ratio("maximum pass-rate drop", self.max_pass_rate_drop)?;
        ensure_ratio(
            "maximum execution-success drop",
            self.max_execution_success_drop,
        )
    }
}

/// One baseline-to-candidate score comparison.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MetricRegression {
    /// Stable scorer name.
    pub name: String,
    /// Baseline mean.
    pub baseline_mean: f64,
    /// Candidate mean.
    pub candidate_mean: f64,
    /// Candidate minus baseline mean.
    pub mean_delta: f64,
    /// Baseline pass rate.
    pub baseline_pass_rate: f64,
    /// Candidate pass rate.
    pub candidate_pass_rate: f64,
    /// Candidate minus baseline pass rate.
    pub pass_rate_delta: f64,
    /// Whether both drops satisfy policy.
    pub passed: bool,
}

/// Complete relative regression decision.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RegressionComparison {
    /// Baseline candidate version.
    pub baseline_version: String,
    /// New candidate version.
    pub candidate_version: String,
    /// Candidate minus baseline target execution success.
    pub execution_success_rate_delta: f64,
    /// Per-score comparisons.
    pub metrics: Vec<MetricRegression>,
    /// Whether every relative regression gate passed.
    pub passed: bool,
}

/// Concurrent deterministic evaluation orchestrator.
pub struct EvaluationRunner {
    target: Arc<dyn EvaluationTarget>,
    scorers: Vec<Arc<dyn EvaluationScorer>>,
    concurrency: NonZeroUsize,
}

impl EvaluationRunner {
    /// Creates a runner with sequential case execution.
    pub fn new(target: impl EvaluationTarget + 'static) -> Self {
        Self {
            target: Arc::new(target),
            scorers: Vec::new(),
            concurrency: NonZeroUsize::MIN,
        }
    }

    /// Adds one scorer.
    #[must_use]
    pub fn with_scorer(mut self, scorer: impl EvaluationScorer + 'static) -> Self {
        self.scorers.push(Arc::new(scorer));
        self
    }

    /// Bounds concurrently executing cases.
    #[must_use]
    pub const fn with_concurrency(mut self, concurrency: NonZeroUsize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Evaluates all cases and returns a stable output-free report.
    ///
    /// Target and scorer failures are captured per case rather than cancelling
    /// unrelated cases.
    ///
    /// # Errors
    ///
    /// Returns an error when `candidate_version` is empty.
    pub async fn run(
        &self,
        dataset: &EvaluationDataset,
        candidate_version: impl Into<String>,
    ) -> Result<EvaluationReport, EvaluationError> {
        let candidate_version = candidate_version.into();
        ensure_not_empty("candidate version", &candidate_version)?;
        validate_scorers(&self.scorers)?;
        let target = Arc::clone(&self.target);
        let scorers = self.scorers.clone();
        let mut indexed = stream::iter(dataset.cases.iter().cloned().enumerate())
            .map(|(index, case)| {
                let target = Arc::clone(&target);
                let scorers = scorers.clone();
                async move { (index, evaluate_case(target, scorers, case).await) }
            })
            .buffer_unordered(self.concurrency.get())
            .collect::<Vec<_>>()
            .await;
        indexed.sort_by_key(|(index, _)| *index);
        let cases = indexed
            .into_iter()
            .map(|(_, result)| result)
            .collect::<Vec<_>>();
        Ok(build_report(dataset, candidate_version, cases))
    }
}

impl fmt::Debug for EvaluationRunner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EvaluationRunner")
            .field("scorers", &self.scorers.len())
            .field("concurrency", &self.concurrency)
            .finish_non_exhaustive()
    }
}

async fn evaluate_case(
    target: Arc<dyn EvaluationTarget>,
    scorers: Vec<Arc<dyn EvaluationScorer>>,
    case: EvaluationCase,
) -> EvaluationCaseResult {
    let output = match target.execute(case.clone()).await {
        Ok(output) => output,
        Err(error) => {
            return EvaluationCaseResult {
                case_id: case.id,
                run_id: None,
                metrics: None,
                scores: Vec::new(),
                failures: vec![EvaluationFailure {
                    stage: EvaluationFailureStage::Target,
                    scorer: None,
                    message: error.to_string(),
                }],
            };
        }
    };
    let run_id = output.run_id;
    let metrics = output.metrics.clone();
    let scorer_concurrency = scorers.len().max(1);
    let scored = stream::iter(scorers)
        .map(|scorer| {
            let case = case.clone();
            let output = output.clone();
            async move {
                let name = scorer.name().to_owned();
                let threshold = scorer.threshold();
                let result = scorer.score(case, output).await;
                (name, threshold, result)
            }
        })
        .buffer_unordered(scorer_concurrency)
        .collect::<Vec<_>>()
        .await;
    let mut case_scores = Vec::new();
    let mut failures = Vec::new();
    for (name, threshold, result) in scored {
        match result {
            Ok(score) => case_scores.push(EvaluationScore {
                name,
                value: score.value,
                threshold,
                passed: score.value >= threshold,
                rationale: score.rationale,
            }),
            Err(error) => failures.push(EvaluationFailure {
                stage: EvaluationFailureStage::Scorer,
                scorer: Some(name),
                message: error.to_string(),
            }),
        }
    }
    case_scores.sort_by(|left, right| left.name.cmp(&right.name));
    failures.sort_by(|left, right| left.scorer.cmp(&right.scorer));
    EvaluationCaseResult {
        case_id: case.id,
        run_id,
        metrics,
        scores: case_scores,
        failures,
    }
}

fn build_report(
    dataset: &EvaluationDataset,
    candidate_version: String,
    cases: Vec<EvaluationCaseResult>,
) -> EvaluationReport {
    let total_cases = cases.len();
    let total_cases_ratio = cases.iter().fold(0.0, |total, _| total + 1.0);
    let successful = cases.iter().fold(0.0, |total, result| {
        if result
            .failures
            .iter()
            .any(|failure| failure.stage == EvaluationFailureStage::Target)
        {
            total
        } else {
            total + 1.0
        }
    });
    let mut aggregate = BTreeMap::<String, (usize, f64, f64, f64)>::new();
    for score in cases.iter().flat_map(|result| &result.scores) {
        let entry = aggregate.entry(score.name.clone()).or_default();
        entry.0 += 1;
        entry.1 += score.value;
        entry.2 += 1.0;
        entry.3 += if score.passed { 1.0 } else { 0.0 };
    }
    let summaries = aggregate
        .into_iter()
        .map(
            |(name, (scored_cases, total, scored_cases_ratio, passed))| EvaluationScoreSummary {
                name,
                scored_cases,
                total_cases,
                mean: total / scored_cases_ratio,
                pass_rate: passed / total_cases_ratio,
            },
        )
        .collect();
    EvaluationReport {
        dataset_name: dataset.name.clone(),
        dataset_version: dataset.version.clone(),
        candidate_version,
        execution_success_rate: successful / total_cases_ratio,
        cases,
        summaries,
    }
}

fn ensure_not_empty(field: &'static str, value: &str) -> Result<(), EvaluationError> {
    if value.trim().is_empty() {
        return Err(EvaluationError::EmptyField { field });
    }
    Ok(())
}

fn validate_case_metrics(case: &EvaluationCaseResult) -> Result<(), EvaluationError> {
    if case.metrics.is_some()
        && case
            .failures
            .iter()
            .any(|failure| failure.stage == EvaluationFailureStage::Target)
    {
        return Err(EvaluationError::InconsistentReport {
            message: "target failure cannot contain successful execution metrics",
        });
    }
    if let Some(metrics) = &case.metrics {
        metrics.validate()?;
    }
    Ok(())
}

fn ensure_ratio(field: &'static str, value: f64) -> Result<(), EvaluationError> {
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(EvaluationError::InvalidRatio { field, value });
    }
    Ok(())
}

fn ensure_non_negative(field: &'static str, value: f64) -> Result<(), EvaluationError> {
    if !value.is_finite() || value < 0.0 {
        return Err(EvaluationError::InvalidMetric { field, value });
    }
    Ok(())
}

fn ensure_close(actual: f64, expected: f64, message: &'static str) -> Result<(), EvaluationError> {
    const REPORT_RATIO_TOLERANCE: f64 = 1e-12;
    if (actual - expected).abs() > REPORT_RATIO_TOLERANCE {
        return Err(EvaluationError::InconsistentReport { message });
    }
    Ok(())
}

fn validate_scorers(scorers: &[Arc<dyn EvaluationScorer>]) -> Result<(), EvaluationError> {
    if scorers.is_empty() {
        return Err(EvaluationError::NoScorers);
    }
    let mut names = BTreeSet::new();
    for scorer in scorers {
        ensure_not_empty("scorer name", scorer.name())?;
        ensure_ratio("score threshold", scorer.threshold())?;
        if !names.insert(scorer.name()) {
            return Err(EvaluationError::DuplicateScorer {
                scorer: scorer.name().to_owned(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use runifold_core::RunId;

    use super::{
        EvaluationCase, EvaluationDataset, EvaluationError, EvaluationOutput, EvaluationRunner,
        JsonExactMatchScorer, RegressionPolicy, ScoreValue,
    };

    #[test]
    fn dataset_rejects_duplicate_case_ids() {
        let first = EvaluationCase::new("same", serde_json::json!("one")).unwrap();
        let second = EvaluationCase::new("same", serde_json::json!("two")).unwrap();

        let error = EvaluationDataset::new("dataset", "1", vec![first, second]).unwrap_err();

        assert!(matches!(error, EvaluationError::DuplicateCase { .. }));
    }

    #[test]
    fn score_rejects_non_finite_or_out_of_range_values() {
        for value in [-0.1, 1.1, f64::NAN, f64::INFINITY] {
            assert!(matches!(
                ScoreValue::new(value),
                Err(EvaluationError::InvalidRatio { .. })
            ));
        }
    }

    #[test]
    fn metrics_reject_negative_or_non_finite_values() {
        for value in [-0.1, f64::NAN, f64::INFINITY] {
            assert!(matches!(
                super::EvaluationMetrics::new(value),
                Err(EvaluationError::InvalidMetric { .. })
            ));
        }
        assert!(
            super::EvaluationMetrics::new(1.0)
                .unwrap()
                .with_cost_usd(-0.1)
                .is_err()
        );
    }

    #[test]
    fn runner_requires_at_least_one_scorer() {
        let dataset = EvaluationDataset::new(
            "answers",
            "1",
            vec![EvaluationCase::new("one", serde_json::json!("answer")).unwrap()],
        )
        .unwrap();
        let runner = EvaluationRunner::new(|case: EvaluationCase| async move {
            Ok(EvaluationOutput::new(case.input().clone()))
        });

        let error = futures_executor::block_on(runner.run(&dataset, "candidate")).unwrap_err();

        assert_eq!(error, EvaluationError::NoScorers);
    }

    #[test]
    fn concurrent_runner_is_ordered_correlated_and_output_free() {
        let dataset = EvaluationDataset::new(
            "answers",
            "2026-07-26",
            vec![
                EvaluationCase::new("first", serde_json::json!("secret-one"))
                    .unwrap()
                    .with_expected(serde_json::json!("secret-one")),
                EvaluationCase::new("second", serde_json::json!("secret-two"))
                    .unwrap()
                    .with_expected(serde_json::json!("secret-two")),
            ],
        )
        .unwrap();
        let runner = EvaluationRunner::new(|case: EvaluationCase| async move {
            let output = EvaluationOutput::new(case.input().clone());
            Ok(if case.id().as_str() == "first" {
                output.with_run_id(RunId::new())
            } else {
                output
            })
        })
        .with_scorer(JsonExactMatchScorer)
        .with_concurrency(NonZeroUsize::new(2).unwrap());

        let report = futures_executor::block_on(runner.run(&dataset, "candidate-a")).unwrap();
        let json = report.to_json_pretty().unwrap();

        assert_eq!(report.cases[0].case_id.as_str(), "first");
        assert_eq!(report.cases[1].case_id.as_str(), "second");
        assert!(report.cases[0].run_id.is_some());
        assert!(report.cases[1].run_id.is_none());
        assert!((report.execution_success_rate - 1.0).abs() < 1e-12);
        assert!((report.summaries[0].mean - 1.0).abs() < 1e-12);
        assert!(!json.contains("secret-one"));
        assert!(!json.contains("secret-two"));
    }

    #[test]
    fn relative_gate_detects_mean_and_pass_rate_regression() {
        let baseline = report("baseline", 1.0, 1.0);
        let candidate = report("candidate", 0.8, 0.5);
        let policy = RegressionPolicy::new(0.05, 0.1, 0.0).unwrap();

        let comparison = candidate.compare(&baseline, &policy).unwrap();

        assert!(!comparison.passed);
        assert!((comparison.metrics[0].mean_delta - -0.2).abs() < 1e-12);
        assert!((comparison.metrics[0].pass_rate_delta - -0.5).abs() < 1e-12);
    }

    #[test]
    fn externally_loaded_report_cannot_forge_aggregate_quality() {
        let mut forged = report("candidate", 0.8, 0.5);
        forged.summaries[0].mean = 1.0;

        assert!(matches!(
            forged.validate(),
            Err(EvaluationError::InconsistentReport { .. })
        ));
    }

    fn report(candidate: &str, mean: f64, pass_rate: f64) -> super::EvaluationReport {
        let values = if pass_rate > 0.75 {
            [mean, mean]
        } else {
            [mean - 0.1, mean + 0.1]
        };
        let cases = values
            .into_iter()
            .enumerate()
            .map(|(index, value)| super::EvaluationCaseResult {
                case_id: super::EvaluationCaseId::new(format!("case-{index}")).unwrap(),
                run_id: None,
                metrics: None,
                scores: vec![super::EvaluationScore {
                    name: "quality".into(),
                    value,
                    threshold: 0.8,
                    passed: value >= 0.8,
                    rationale: None,
                }],
                failures: Vec::new(),
            })
            .collect();
        super::EvaluationReport {
            dataset_name: "answers".into(),
            dataset_version: "1".into(),
            candidate_version: candidate.into(),
            execution_success_rate: 1.0,
            cases,
            summaries: vec![super::EvaluationScoreSummary {
                name: "quality".into(),
                scored_cases: 2,
                total_cases: 2,
                mean,
                pass_rate,
            }],
        }
    }
}
