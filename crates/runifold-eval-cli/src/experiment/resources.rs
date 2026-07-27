use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};

use super::ExperimentSample;

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DistributionStatistics {
    pub(super) mean: f64,
    pub(super) p50: f64,
    pub(super) p95: f64,
    pub(super) max: f64,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ResourceStatistics {
    pub(super) expected_observations: usize,
    pub(super) latency_observations: usize,
    pub(super) latency_ms: Option<DistributionStatistics>,
    pub(super) token_observations: usize,
    pub(super) input_tokens: u64,
    pub(super) output_tokens: u64,
    pub(super) cost_observations: usize,
    pub(super) cost_usd: f64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct ResourceBudget {
    pub(super) latency_p95_ms: Option<f64>,
    pub(super) total_tokens: Option<u64>,
    pub(super) total_cost_usd: Option<f64>,
}

impl ResourceBudget {
    pub(super) fn validate(&self) -> Result<()> {
        validate_optional_non_negative("maximum p95 latency milliseconds", self.latency_p95_ms)?;
        validate_optional_non_negative("maximum total cost USD", self.total_cost_usd)
    }

    pub(super) fn passes(&self, statistics: &ResourceStatistics) -> bool {
        self.latency_p95_ms.is_none_or(|maximum| {
            statistics.latency_observations == statistics.expected_observations
                && statistics
                    .latency_ms
                    .as_ref()
                    .is_some_and(|latency| latency.p95 <= maximum)
        }) && self.total_tokens.is_none_or(|maximum| {
            statistics.token_observations == statistics.expected_observations
                && statistics
                    .input_tokens
                    .checked_add(statistics.output_tokens)
                    .is_some_and(|total| total <= maximum)
        }) && self.total_cost_usd.is_none_or(|maximum| {
            statistics.cost_observations == statistics.expected_observations
                && statistics.cost_usd <= maximum
        })
    }
}

pub(super) fn summarize(samples: &[ExperimentSample]) -> Result<ResourceStatistics> {
    let expected_observations = samples
        .iter()
        .map(|sample| sample.report.cases.len())
        .try_fold(0_usize, |total, count| {
            total
                .checked_add(count)
                .ok_or_else(|| anyhow::anyhow!("resource observation count overflowed"))
        })?;
    let mut latencies = Vec::new();
    let mut token_observations = 0_usize;
    let mut input_tokens = 0_u64;
    let mut output_tokens = 0_u64;
    let mut cost_observations = 0_usize;
    let mut cost_usd = 0.0;
    for metrics in samples
        .iter()
        .flat_map(|sample| &sample.report.cases)
        .filter_map(|case| case.metrics.as_ref())
    {
        latencies.push(metrics.duration_ms);
        if let (Some(input), Some(output)) = (metrics.input_tokens, metrics.output_tokens) {
            token_observations = token_observations
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("token observation count overflowed"))?;
            input_tokens = input_tokens
                .checked_add(input)
                .ok_or_else(|| anyhow::anyhow!("input token total overflowed"))?;
            output_tokens = output_tokens
                .checked_add(output)
                .ok_or_else(|| anyhow::anyhow!("output token total overflowed"))?;
        }
        if let Some(cost) = metrics.cost_usd {
            cost_observations = cost_observations
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("cost observation count overflowed"))?;
            cost_usd += cost;
            ensure!(cost_usd.is_finite(), "cost total overflowed");
        }
    }
    let latency_observations = latencies.len();
    let latency_ms = distribution(&mut latencies);
    Ok(ResourceStatistics {
        expected_observations,
        latency_observations,
        latency_ms,
        token_observations,
        input_tokens,
        output_tokens,
        cost_observations,
        cost_usd,
    })
}

fn distribution(values: &mut [f64]) -> Option<DistributionStatistics> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let count = values.iter().fold(0.0, |total, _| total + 1.0);
    Some(DistributionStatistics {
        mean: values.iter().sum::<f64>() / count,
        p50: nearest_rank(values, 50),
        p95: nearest_rank(values, 95),
        max: values[values.len() - 1],
    })
}

fn nearest_rank(values: &[f64], percentile: usize) -> f64 {
    let rank = values.len().saturating_mul(percentile).saturating_add(99) / 100;
    values[rank.saturating_sub(1).min(values.len() - 1)]
}

fn validate_optional_non_negative(name: &str, value: Option<f64>) -> Result<()> {
    match value {
        Some(value) if !value.is_finite() || value < 0.0 => {
            bail!("{name} must be finite and non-negative")
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use runifold_testkit::{
        EvaluationCaseId, EvaluationCaseResult, EvaluationMetrics, EvaluationReport,
    };

    use super::{ResourceBudget, summarize};
    use crate::experiment::ExperimentSample;

    #[test]
    fn resource_summary_and_budget_fail_closed_on_missing_usage() {
        let samples = vec![ExperimentSample {
            index: 0,
            report: EvaluationReport {
                dataset_name: "data".into(),
                dataset_version: "1".into(),
                candidate_version: "candidate".into(),
                execution_success_rate: 1.0,
                cases: vec![EvaluationCaseResult {
                    case_id: EvaluationCaseId::new("one").unwrap(),
                    run_id: None,
                    metrics: Some(EvaluationMetrics::new(12.0).unwrap()),
                    scores: Vec::new(),
                    failures: Vec::new(),
                }],
                summaries: Vec::new(),
            },
        }];

        let statistics = summarize(&samples).unwrap();

        assert!((statistics.latency_ms.as_ref().unwrap().p95 - 12.0).abs() < 1e-12);
        assert!(
            ResourceBudget {
                latency_p95_ms: Some(12.0),
                ..ResourceBudget::default()
            }
            .passes(&statistics)
        );
        assert!(
            !ResourceBudget {
                total_tokens: Some(1),
                ..ResourceBudget::default()
            }
            .passes(&statistics)
        );
    }
}
