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
