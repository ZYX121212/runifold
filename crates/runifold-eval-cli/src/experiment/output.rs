use std::path::Path;

use anyhow::Result;

use super::{ExperimentReport, resources::ResourceBudget};
use crate::dataset;

pub(super) async fn write_report(
    report: &ExperimentReport,
    output: &Path,
    junit: Option<&Path>,
    markdown: Option<&Path>,
    minimum_lower_bound: Option<f64>,
    max_flaky_case_rate: f64,
    budget: &ResourceBudget,
) -> Result<()> {
    dataset::write(output, serde_json::to_string_pretty(report)?.as_bytes()).await?;
    if let Some(path) = junit {
        dataset::write(
            path,
            junit_report(report, minimum_lower_bound, max_flaky_case_rate, budget).as_bytes(),
        )
        .await?;
    }
    if let Some(path) = markdown {
        dataset::write(
            path,
            markdown_report(report, minimum_lower_bound, max_flaky_case_rate, budget).as_bytes(),
        )
        .await?;
    }
    Ok(())
}

fn junit_report(
    report: &ExperimentReport,
    minimum_lower_bound: Option<f64>,
    max_flaky_case_rate: f64,
    budget: &ResourceBudget,
) -> String {
    let passed = report.passes(minimum_lower_bound, max_flaky_case_rate, budget);
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<testsuite name=\"runifold-experiment\" tests=\"1\" failures=\"{}\">\n  <testcase classname=\"{}\" name=\"{}\">{}</testcase>\n</testsuite>\n",
        usize::from(!passed),
        xml(&report.dataset_name),
        xml(&report.candidate_version),
        if passed {
            String::new()
        } else {
            "<failure message=\"experiment quality gate failed\"/>".into()
        }
    )
}

fn markdown_report(
    report: &ExperimentReport,
    minimum_lower_bound: Option<f64>,
    max_flaky_case_rate: f64,
    budget: &ResourceBudget,
) -> String {
    let latency = report
        .resources
        .latency_ms
        .as_ref()
        .map_or_else(|| "n/a".into(), |value| format!("{:.2} ms", value.p95));
    let total_tokens = report
        .resources
        .input_tokens
        .checked_add(report.resources.output_tokens)
        .map_or_else(|| "overflow".into(), |value| value.to_string());
    let mut output = format!(
        "# Runifold Experiment\n\n- Dataset: `{}` @ `{}`\n- Candidate: `{}`\n- Samples: {}\n- Cases: {}\n- Flaky case rate: {:.2}%\n- p95 latency: {}\n- Total tokens: {}\n- Total cost: ${:.6}\n- Gate: **{}**\n\n| Score | Mean | Pass rate | Std dev | 95% CI |\n|---|---:|---:|---:|---:|\n",
        report.dataset_name,
        report.dataset_version,
        report.candidate_version,
        report.requested_samples,
        report.case_ids.len(),
        report.flaky_case_rate * 100.0,
        latency,
        total_tokens,
        report.resources.cost_usd,
        if report.passes(minimum_lower_bound, max_flaky_case_rate, budget) {
            "PASS"
        } else {
            "FAIL"
        }
    );
    for statistics in &report.statistics {
        use std::fmt::Write as _;
        let interval = match (
            statistics.confidence_lower_bound,
            statistics.confidence_upper_bound,
        ) {
            (Some(lower), Some(upper)) => format!("[{lower:.4}, {upper:.4}]"),
            _ => "n/a".into(),
        };
        let deviation = statistics
            .standard_deviation
            .map_or_else(|| "n/a".into(), |value| format!("{value:.4}"));
        let _ = writeln!(
            output,
            "| {} | {:.4} | {:.2}% | {} | {} |",
            statistics.name,
            statistics.mean,
            statistics.pass_rate * 100.0,
            deviation,
            interval
        );
    }
    output
}

fn xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
