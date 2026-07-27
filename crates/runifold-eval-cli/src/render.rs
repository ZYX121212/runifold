use std::fmt::Write as _;

use runifold_testkit::{EvaluationCaseResult, EvaluationReport, RegressionComparison};

pub(crate) fn absolute_passed(report: &EvaluationReport) -> bool {
    report.cases.iter().all(|case| {
        case.failures.is_empty()
            && !case.scores.is_empty()
            && case.scores.iter().all(|score| score.passed)
    })
}

pub(crate) fn junit(
    report: &EvaluationReport,
    comparison: Option<&RegressionComparison>,
) -> String {
    let failures = report
        .cases
        .iter()
        .filter(|case| !case_passed(case))
        .count()
        + usize::from(comparison.is_some_and(|comparison| !comparison.passed));
    let mut output = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="{}@{}" tests="{}" failures="{}">
"#,
        xml(&report.dataset_name),
        xml(&report.candidate_version),
        report.cases.len() + usize::from(comparison.is_some()),
        failures
    );
    for case in &report.cases {
        let _ = write!(
            output,
            r#"  <testcase classname="{}" name="{}">"#,
            xml(&report.dataset_name),
            xml(case.case_id.as_str())
        );
        if !case_passed(case) {
            output.push_str(r#"<failure message="quality gate failed"/>"#);
        }
        output.push_str("</testcase>\n");
    }
    if let Some(comparison) = comparison {
        output.push_str(r#"  <testcase classname="regression" name="baseline">"#);
        if !comparison.passed {
            output.push_str(r#"<failure message="baseline regression detected"/>"#);
        }
        output.push_str("</testcase>\n");
    }
    output.push_str("</testsuite>\n");
    output
}

pub(crate) fn markdown(
    report: &EvaluationReport,
    comparison: Option<&RegressionComparison>,
) -> String {
    let mut output = format!(
        "# Runifold Evaluation\n\n- Dataset: `{}` @ `{}`\n- Candidate: `{}`\n- Execution success: {:.2}%\n- Absolute gate: **{}**\n\n",
        report.dataset_name,
        report.dataset_version,
        report.candidate_version,
        report.execution_success_rate * 100.0,
        if absolute_passed(report) {
            "PASS"
        } else {
            "FAIL"
        }
    );
    output.push_str("| Score | Mean | Pass rate | Cases |\n|---|---:|---:|---:|\n");
    for summary in &report.summaries {
        let _ = writeln!(
            output,
            "| {} | {:.4} | {:.2}% | {}/{} |",
            summary.name,
            summary.mean,
            summary.pass_rate * 100.0,
            summary.scored_cases,
            summary.total_cases
        );
    }
    if let Some(comparison) = comparison {
        let _ = write!(
            output,
            "\n## Baseline regression: **{}**\n\n| Score | Mean Δ | Pass-rate Δ | Gate |\n|---|---:|---:|---|\n",
            if comparison.passed { "PASS" } else { "FAIL" }
        );
        for metric in &comparison.metrics {
            let _ = writeln!(
                output,
                "| {} | {:+.4} | {:+.2}% | {} |",
                metric.name,
                metric.mean_delta,
                metric.pass_rate_delta * 100.0,
                if metric.passed { "PASS" } else { "FAIL" }
            );
        }
    }
    output
}

fn case_passed(case: &EvaluationCaseResult) -> bool {
    case.failures.is_empty()
        && !case.scores.is_empty()
        && case.scores.iter().all(|score| score.passed)
}

fn xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use runifold_testkit::{
        EvaluationCaseId, EvaluationCaseResult, EvaluationFailure, EvaluationFailureStage,
        EvaluationReport,
    };

    use super::{absolute_passed, junit, markdown};

    #[test]
    fn renderers_fail_closed_and_escape_xml() {
        let report = EvaluationReport {
            dataset_name: "unsafe<&".into(),
            dataset_version: "1".into(),
            candidate_version: "candidate".into(),
            execution_success_rate: 0.0,
            cases: vec![EvaluationCaseResult {
                case_id: EvaluationCaseId::new("case\"one").unwrap(),
                run_id: None,
                metrics: None,
                scores: Vec::new(),
                failures: vec![EvaluationFailure {
                    stage: EvaluationFailureStage::Target,
                    scorer: None,
                    message: "failed".into(),
                }],
            }],
            summaries: Vec::new(),
        };

        assert!(!absolute_passed(&report));
        let xml = junit(&report, None);
        assert!(xml.contains("unsafe&lt;&amp;"));
        assert!(xml.contains("case&quot;one"));
        assert!(xml.contains("<failure"));
        assert!(markdown(&report, None).contains("Absolute gate: **FAIL**"));
    }
}
