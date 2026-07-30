//! Stable operational metric contracts and deployment-ready SLO assets.
//!
//! Histogram boundaries are instrument advice. An application can override
//! them with OpenTelemetry SDK Views when its latency profile differs.

/// Agent end-to-end duration buckets in seconds.
pub const AGENT_OPERATION_DURATION_SECONDS: &[f64] = &[
    0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0, 300.0, 600.0,
];

/// One Agent turn duration buckets in seconds.
pub const AGENT_TURN_DURATION_SECONDS: &[f64] =
    &[0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0];

/// One model operation duration buckets in seconds.
pub const MODEL_OPERATION_DURATION_SECONDS: &[f64] = &[
    0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0, 300.0,
];

/// Time-to-first-chunk buckets in seconds.
pub const MODEL_TIME_TO_FIRST_CHUNK_SECONDS: &[f64] =
    &[0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0];

/// Scoped MCP Sampling duration buckets in seconds.
pub const MCP_SAMPLING_DURATION_SECONDS: &[f64] =
    &[0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0];

/// Attributed Agent cost buckets in US dollars.
pub const AGENT_COST_USD: &[f64] = &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 50.0];

/// Aggregate tenant-budget utilization ratios.
pub const WORKFLOW_BUDGET_UTILIZATION: &[f64] = &[0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 1.0, 1.25];

/// Reservation age buckets in seconds.
pub const WORKFLOW_BUDGET_RESERVATION_AGE_SECONDS: &[f64] =
    &[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0, 3_600.0];

/// Stable OpenTelemetry instrument names used by the SLO assets.
pub mod metric_names {
    /// End-to-end Agent run duration.
    pub const AGENT_OPERATION_DURATION: &str = "runifold.agent.operation.duration";
    /// Duration of one Agent turn.
    pub const AGENT_TURN_DURATION: &str = "runifold.agent.turn.duration";
    /// Turns in one completed Agent run.
    pub const AGENT_TURNS: &str = "runifold.agent.turns";
    /// Tool calls in one completed Agent run.
    pub const AGENT_TOOL_CALLS: &str = "runifold.agent.tool.calls";
    /// Delegations in one completed Agent run.
    pub const AGENT_DELEGATIONS: &str = "runifold.agent.delegations";
    /// Attributed Agent cost.
    pub const AGENT_COST: &str = "runifold.agent.cost";
    /// Failed or cancelled Agent runs.
    pub const AGENT_ERRORS: &str = "runifold.agent.errors";
    /// Agent runs stopped by budget exhaustion.
    pub const AGENT_BUDGET_EXHAUSTIONS: &str = "runifold.agent.budget.exhaustions";
    /// Model operation duration.
    pub const MODEL_OPERATION_DURATION: &str = "gen_ai.client.operation.duration";
    /// Model time to first visible chunk.
    pub const MODEL_TIME_TO_FIRST_CHUNK: &str = "runifold.model.time_to_first_chunk";
    /// Scoped MCP Sampling duration.
    pub const MCP_SAMPLING_DURATION: &str = "runifold.mcp.sampling.duration";
    /// Scoped MCP Sampling requests.
    pub const MCP_SAMPLING_REQUESTS: &str = "runifold.mcp.sampling.requests";
    /// Failed scoped MCP Sampling requests.
    pub const MCP_SAMPLING_FAILURES: &str = "runifold.mcp.sampling.failures";
    /// Durable workflow tenant-budget decisions.
    pub const WORKFLOW_BUDGET_DECISIONS: &str = "runifold.workflow.tenant_budget.decisions";
    /// Resource amount attached to a budget decision.
    pub const WORKFLOW_BUDGET_AMOUNT: &str = "runifold.workflow.tenant_budget.amount";
    /// Aggregate utilization after a budget decision.
    pub const WORKFLOW_BUDGET_UTILIZATION: &str = "runifold.workflow.tenant_budget.utilization";
    /// Reservation age when observed, settled, or forfeited.
    pub const WORKFLOW_BUDGET_RESERVATION_AGE: &str =
        "runifold.workflow.tenant_budget.reservation.age";
    /// Projection ownership, completion, and failure operations.
    pub const WORKFLOW_BUDGET_PROJECTION_OPERATIONS: &str =
        "runifold.workflow.tenant_budget.projection.operations";
    /// Terminal Task cleanup scan, claim, and failure outcomes.
    pub const WORKFLOW_TASK_CLEANUP_OPERATIONS: &str = "runifold.workflow.task_cleanup.operations";
    /// Terminal Task cleanup tenant discovery and shard assignment.
    pub const WORKFLOW_TASK_CLEANUP_TENANTS: &str = "runifold.workflow.task_cleanup.tenants";
    /// Non-empty terminal Task cleanup batches.
    pub const WORKFLOW_TASK_CLEANUP_BATCHES: &str = "runifold.workflow.task_cleanup.batches";
    /// Terminal Tasks atomically tombstoned and deleted.
    pub const WORKFLOW_TASK_CLEANUP_DELETED: &str = "runifold.workflow.task_cleanup.deleted";
    /// Authorized tombstone governance operations and terminal outcomes.
    pub const WORKFLOW_TASK_GOVERNANCE_OPERATIONS: &str =
        "runifold.workflow.task_governance.operations";
}

/// Prometheus recording and alert rules for the default Runifold SLOs.
pub const PROMETHEUS_RULES: &str = include_str!("../assets/prometheus-rules.yaml");

/// Importable Grafana dashboard for Runifold Agent operations.
pub const GRAFANA_DASHBOARD: &str = include_str!("../assets/grafana-dashboard.json");

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Instant};

    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use prometheus::{Encoder, TextEncoder};
    use runifold_core::{LifecycleEvent, RetrySafety, RunError, RunErrorKind};
    use serde_json::Value as JsonValue;
    use yaml_rust2::YamlLoader;

    use super::{GRAFANA_DASHBOARD, PROMETHEUS_RULES, metric_names};
    use crate::journal_metrics::JournalInstruments;

    #[test]
    fn embedded_slo_assets_are_machine_parseable() {
        let rules = YamlLoader::load_from_str(PROMETHEUS_RULES).unwrap();
        let dashboard: JsonValue = serde_json::from_str(GRAFANA_DASHBOARD).unwrap();

        assert_eq!(rules.len(), 1);
        assert!(!rules[0]["groups"].is_badvalue());
        assert_eq!(dashboard["uid"], "runifold-operations");
        assert!(
            dashboard["panels"]
                .as_array()
                .is_some_and(|panels| !panels.is_empty())
        );
    }

    #[test]
    fn assets_reference_the_exported_prometheus_metric_contract() {
        for metric in [
            "runifold_agent_operation_duration_seconds",
            "runifold_agent_errors_total",
            "runifold_agent_budget_exhaustions_total",
            "runifold_mcp_sampling_requests_total",
            "runifold_mcp_sampling_failures_total",
            "runifold_workflow_tenant_budget_decisions_total",
        ] {
            assert!(PROMETHEUS_RULES.contains(metric));
            assert!(GRAFANA_DASHBOARD.contains(metric));
        }
        assert!(
            PROMETHEUS_RULES
                .contains("runifold_workflow_tenant_budget_projection_operations_total")
        );
    }

    #[test]
    fn default_prometheus_exporter_produces_the_template_metric_names() {
        let registry = prometheus::Registry::new();
        let exporter = opentelemetry_prometheus::exporter()
            .with_registry(registry.clone())
            .build()
            .unwrap();
        let provider = SdkMeterProvider::builder().with_reader(exporter).build();
        let meter = provider.meter("runifold.test");
        let instruments = JournalInstruments::new(&meter);

        instruments.record_agent(
            Instant::now(),
            &LifecycleEvent::Completed {
                output: serde_json::json!({
                    "turns": 1,
                    "tool_calls": 1,
                    "delegations": 0,
                    "usage": {"cost_microusd": 1_000}
                }),
            },
        );
        instruments.record_agent(
            Instant::now(),
            &LifecycleEvent::Failed {
                error: RunError {
                    kind: RunErrorKind::BudgetExceeded,
                    message: "not exported".into(),
                    retry_safety: RetrySafety::Unknown,
                    metadata: BTreeMap::new(),
                },
            },
        );
        instruments.sampling_started();
        instruments.record_sampling(
            Instant::now(),
            true,
            Some("remote"),
            Some("model_execution"),
        );
        meter
            .f64_histogram(metric_names::MODEL_OPERATION_DURATION)
            .with_unit("s")
            .build()
            .record(1.0, &[]);

        let mut encoded = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut encoded)
            .unwrap();
        let scrape = String::from_utf8(encoded).unwrap();
        for metric in [
            "runifold_agent_operation_duration_seconds",
            "runifold_agent_errors_total",
            "runifold_agent_budget_exhaustions_total",
            "runifold_mcp_sampling_requests_total",
            "runifold_mcp_sampling_failures_total",
            "gen_ai_client_operation_duration_seconds",
        ] {
            assert!(
                scrape.contains(metric),
                "missing Prometheus metric {metric}"
            );
        }
        assert!(!scrape.contains("not exported"));
    }
}
