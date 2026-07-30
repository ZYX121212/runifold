use super::{Deserialize, EvaluationError, Serialize, ensure_ratio};

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

    pub(super) fn validate(&self) -> Result<(), EvaluationError> {
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
