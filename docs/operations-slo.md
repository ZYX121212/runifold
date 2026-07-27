# Runifold Operations and SLO Runbook

Runifold ships a conservative baseline for operating Agent workloads. Treat
these objectives as starting points and tune them after measuring representative
production traffic.

## Default objectives

| Signal | Objective | Window |
| --- | ---: | ---: |
| Agent run success | 99% | 30 days |
| Agent end-to-end P95 | at most 30 seconds | rolling 5 minutes |
| MCP Sampling success | 99% | 30 days |
| Agent budget exhaustion | below 2% | rolling 15 minutes |

An Agent run is successful when its terminal lifecycle status is `ok`.
Cancellation counts as an unsuccessful run because the requested outcome was
not delivered. Budget exhaustion is a separate subset of failures so capacity
or policy pressure is visible without inspecting traces.

## Deployment

The crate embeds two versioned assets:

- `runifold_observability_otel::slo::PROMETHEUS_RULES`;
- `runifold_observability_otel::slo::GRAFANA_DASHBOARD`.

The source files are
[`prometheus-rules.yaml`](../crates/runifold-observability-otel/assets/prometheus-rules.yaml)
and
[`grafana-dashboard.json`](../crates/runifold-observability-otel/assets/grafana-dashboard.json).

Load the rules with Prometheus, a Prometheus Operator `PrometheusRule`, Mimir,
or another PromQL-compatible ruler. Import the dashboard into Grafana and
select the Prometheus-compatible data source that receives OpenTelemetry
metrics.

The expressions assume default OpenTelemetry Prometheus name and unit
translation, including `_seconds` and `_total` suffixes. If a collector
configuration renames metrics, inspect its `/metrics` output and update the
expressions at the deployment boundary.

## Alert response

### Agent error-budget burn

1. Split `runifold_agent_errors_total` by `error_type`.
2. Use an exemplar or correlated trace to locate affected Runs.
3. Check Provider transport/protocol failures and Router fallback events.
4. Check Tool and delegation child spans before changing retry policy.
5. Do not retry failures marked unsafe after visible output or side effects.

Fast-burn alerts page because both the 5-minute and 1-hour windows exceed a
14.4x burn rate for a 99% objective. Slow-burn alerts warn when both 30-minute
and 6-hour windows exceed 6x.

### Agent latency

1. Compare Agent P95 with model duration and time-to-first-chunk.
2. Inspect Turn counts for loop growth.
3. Inspect Tool, delegation, and MCP Sampling spans for the slow branch.
4. Check queueing and exporter delay before increasing Agent deadlines.

### Budget exhaustion

1. Compare turns, Tool calls, delegations, tokens, cost, and duration.
2. Confirm whether the limit is intentionally protecting the workload.
3. Fix loops or overly broad context before raising the budget.
4. Keep per-run identifiers in traces rather than adding them as metric labels.

### MCP Sampling failures

Split failures by `error_type` and `runifold_mcp_sampling_stage`. Request-review
and response-review failures usually indicate policy; model-execution failures
usually indicate the selected local Provider; lifecycle failures indicate
deadline, cancellation, or observability boundaries.

## Cardinality rules

Prometheus labels are limited to normalized status, error type, and Sampling
stage. Never promote Run IDs, invocation IDs, call IDs, model request IDs,
Agent names, Tool names, prompt fragments, or user-controlled extension values
to labels. Use traces for those dimensions.
