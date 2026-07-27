use std::time::Instant;

use opentelemetry::{
    Context, KeyValue,
    metrics::{Counter, Histogram, Meter},
};
use runifold_core::{LifecycleEvent, RunError, RunErrorKind};

use crate::slo::{
    AGENT_COST_USD, AGENT_OPERATION_DURATION_SECONDS, AGENT_TURN_DURATION_SECONDS,
    MCP_SAMPLING_DURATION_SECONDS, metric_names,
};

#[derive(Debug)]
pub(crate) struct ActiveOperation {
    pub(crate) context: Context,
    pub(crate) started: Instant,
    pub(crate) metric_recorded: bool,
}

impl ActiveOperation {
    pub(crate) fn new(context: Context) -> Self {
        Self {
            context,
            started: Instant::now(),
            metric_recorded: false,
        }
    }
}

#[derive(Debug)]
pub(crate) struct JournalInstruments {
    agent_duration: Histogram<f64>,
    agent_turn_duration: Histogram<f64>,
    agent_turns: Histogram<u64>,
    agent_tool_calls: Histogram<u64>,
    agent_delegations: Histogram<u64>,
    agent_cost: Histogram<f64>,
    agent_errors: Counter<u64>,
    agent_budget_exhaustions: Counter<u64>,
    sampling_duration: Histogram<f64>,
    sampling_requests: Counter<u64>,
    sampling_failures: Counter<u64>,
}

impl JournalInstruments {
    pub(crate) fn new(meter: &Meter) -> Self {
        Self {
            agent_duration: meter
                .f64_histogram(metric_names::AGENT_OPERATION_DURATION)
                .with_unit("s")
                .with_description("End-to-end Agent run duration.")
                .with_boundaries(AGENT_OPERATION_DURATION_SECONDS.to_vec())
                .build(),
            agent_turn_duration: meter
                .f64_histogram(metric_names::AGENT_TURN_DURATION)
                .with_unit("s")
                .with_description("Duration of one Agent turn.")
                .with_boundaries(AGENT_TURN_DURATION_SECONDS.to_vec())
                .build(),
            agent_turns: meter
                .u64_histogram(metric_names::AGENT_TURNS)
                .with_unit("{turn}")
                .with_description("Number of turns in a completed Agent run.")
                .build(),
            agent_tool_calls: meter
                .u64_histogram(metric_names::AGENT_TOOL_CALLS)
                .with_unit("{call}")
                .with_description("Number of Tool calls in a completed Agent run.")
                .build(),
            agent_delegations: meter
                .u64_histogram(metric_names::AGENT_DELEGATIONS)
                .with_unit("{delegation}")
                .with_description("Number of delegations in a completed Agent run.")
                .build(),
            agent_cost: meter
                .f64_histogram(metric_names::AGENT_COST)
                .with_unit("USD")
                .with_description("Attributed Agent run cost in US dollars.")
                .with_boundaries(AGENT_COST_USD.to_vec())
                .build(),
            agent_errors: meter
                .u64_counter(metric_names::AGENT_ERRORS)
                .with_description("Agent runs that terminated with an error.")
                .build(),
            agent_budget_exhaustions: meter
                .u64_counter(metric_names::AGENT_BUDGET_EXHAUSTIONS)
                .with_description("Agent runs terminated by budget exhaustion.")
                .build(),
            sampling_duration: meter
                .f64_histogram(metric_names::MCP_SAMPLING_DURATION)
                .with_unit("s")
                .with_description("Duration of one scoped MCP Sampling request.")
                .with_boundaries(MCP_SAMPLING_DURATION_SECONDS.to_vec())
                .build(),
            sampling_requests: meter
                .u64_counter(metric_names::MCP_SAMPLING_REQUESTS)
                .with_description("Scoped MCP Sampling requests started.")
                .build(),
            sampling_failures: meter
                .u64_counter(metric_names::MCP_SAMPLING_FAILURES)
                .with_description("Scoped MCP Sampling requests that failed.")
                .build(),
        }
    }

    pub(crate) fn sampling_started(&self) {
        self.sampling_requests.add(1, &[]);
    }

    pub(crate) fn record_sampling(
        &self,
        started: Instant,
        failed: bool,
        error_type: Option<&str>,
        stage: Option<&str>,
    ) {
        let status = if failed { "error" } else { "ok" };
        let mut attributes = vec![KeyValue::new("status", status)];
        if let Some(error_type) = error_type {
            attributes.push(KeyValue::new(
                "error.type",
                normalized_sampling_error_type(error_type),
            ));
        }
        if let Some(stage) = stage {
            attributes.push(KeyValue::new(
                "runifold.mcp.sampling.stage",
                normalized_sampling_stage(stage),
            ));
        }
        self.sampling_duration
            .record(started.elapsed().as_secs_f64(), &attributes);
        if failed {
            self.sampling_failures.add(1, &attributes);
        }
    }

    pub(crate) fn record_turn(&self, started: Instant, failed: bool) {
        self.agent_turn_duration.record(
            started.elapsed().as_secs_f64(),
            &[KeyValue::new("status", if failed { "error" } else { "ok" })],
        );
    }

    pub(crate) fn record_agent(&self, started: Instant, lifecycle: &LifecycleEvent) {
        let (status, error_type) = match lifecycle {
            LifecycleEvent::Completed { .. } => ("ok", None),
            LifecycleEvent::Cancelled => ("cancelled", Some("cancelled")),
            LifecycleEvent::Failed { error } => ("error", Some(run_error_type(error))),
            _ => return,
        };
        let mut attributes = vec![KeyValue::new("status", status)];
        if let Some(error_type) = error_type {
            attributes.push(KeyValue::new("error.type", error_type));
        }
        self.agent_duration
            .record(started.elapsed().as_secs_f64(), &attributes);

        match lifecycle {
            LifecycleEvent::Completed { output } => self.record_agent_outcome(output, &attributes),
            LifecycleEvent::Failed { error } => {
                self.agent_errors.add(1, &attributes);
                if matches!(error.kind, RunErrorKind::BudgetExceeded) {
                    self.agent_budget_exhaustions.add(1, &attributes);
                }
            }
            LifecycleEvent::Cancelled => self.agent_errors.add(1, &attributes),
            _ => {}
        }
    }

    fn record_agent_outcome(&self, output: &serde_json::Value, attributes: &[KeyValue]) {
        for (field, instrument) in [
            ("turns", &self.agent_turns),
            ("tool_calls", &self.agent_tool_calls),
            ("delegations", &self.agent_delegations),
        ] {
            if let Some(value) = output.get(field).and_then(serde_json::Value::as_u64) {
                instrument.record(value, attributes);
            }
        }
        if let Some(cost_microusd) = output
            .get("usage")
            .and_then(|usage| usage.get("cost_microusd"))
            .and_then(serde_json::Value::as_u64)
        {
            let cost_usd = std::time::Duration::from_micros(cost_microusd).as_secs_f64();
            self.agent_cost.record(cost_usd, attributes);
        }
    }
}

fn normalized_sampling_error_type(error_type: &str) -> &'static str {
    match error_type {
        "transport" => "transport",
        "protocol" => "protocol",
        "remote" => "remote",
        "timeout" => "timeout",
        "cancelled" => "cancelled",
        "lifecycle" => "lifecycle",
        "unsupported_version" => "unsupported_version",
        "authentication" => "authentication",
        "session_expired" => "session_expired",
        "observability" => "observability",
        "operation_abandoned" => "operation_abandoned",
        _ => "_OTHER",
    }
}

fn normalized_sampling_stage(stage: &str) -> &'static str {
    match stage {
        "request_validation" => "request_validation",
        "request_review" => "request_review",
        "budget_reservation" => "budget_reservation",
        "model_execution" => "model_execution",
        "response_validation" => "response_validation",
        "response_review" => "response_review",
        "lifecycle" => "lifecycle",
        _ => "_OTHER",
    }
}

pub(crate) fn run_error_type(error: &RunError) -> &'static str {
    match &error.kind {
        RunErrorKind::InvalidInput => "invalid_input",
        RunErrorKind::CapabilityDenied => "capability_denied",
        RunErrorKind::BudgetExceeded => "budget_exceeded",
        RunErrorKind::Transport => "transport",
        RunErrorKind::Protocol => "protocol",
        RunErrorKind::Invocation => "invocation",
        RunErrorKind::Cancelled => "cancelled",
        RunErrorKind::DeadlineExceeded => "timeout",
        RunErrorKind::Extension(_) => "extension",
        _ => "_OTHER",
    }
}
