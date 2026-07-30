use super::{
    Arc, BTreeMap, BTreeSet, EvaluationCase, EvaluationCaseResult, EvaluationDataset,
    EvaluationError, EvaluationFailure, EvaluationFailureStage, EvaluationReport, EvaluationScore,
    EvaluationScoreSummary, EvaluationScorer, EvaluationTarget, NonZeroUsize, StreamExt,
    ensure_not_empty, ensure_ratio, fmt, stream,
};

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
