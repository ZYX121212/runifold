//! Durable tenant-budget audit metrics and bounded projection.

use std::{num::NonZeroU32, sync::Arc};

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter},
};
use runifold_workflow::{
    WorkflowBudgetAuditCursor, WorkflowBudgetAuditEvent, WorkflowBudgetAuditKind,
    WorkflowBudgetAuditLimit, WorkflowBudgetAuditProjectionId, WorkflowBudgetForfeitReason,
    WorkflowStore, WorkflowStoreError, WorkflowTenantId,
};
use thiserror::Error;

use crate::slo::{
    WORKFLOW_BUDGET_RESERVATION_AGE_SECONDS, WORKFLOW_BUDGET_UTILIZATION, metric_names,
};

/// Low-cardinality OpenTelemetry projection of durable tenant-budget audit facts.
#[derive(Clone, Debug)]
pub struct OtelWorkflowBudgetMetrics {
    decisions: Counter<u64>,
    projection_operations: Counter<u64>,
    amount: Histogram<u64>,
    utilization: Histogram<f64>,
    reservation_age: Histogram<f64>,
}

impl OtelWorkflowBudgetMetrics {
    pub(crate) fn new(meter: &Meter) -> Self {
        Self {
            decisions: meter
                .u64_counter(metric_names::WORKFLOW_BUDGET_DECISIONS)
                .with_description("Durable tenant-budget decisions.")
                .build(),
            projection_operations: meter
                .u64_counter(metric_names::WORKFLOW_BUDGET_PROJECTION_OPERATIONS)
                .with_description("Tenant-budget projection ownership and processing outcomes.")
                .build(),
            amount: meter
                .u64_histogram(metric_names::WORKFLOW_BUDGET_AMOUNT)
                .with_description("Resource amount associated with a tenant-budget decision.")
                .build(),
            utilization: meter
                .f64_histogram(metric_names::WORKFLOW_BUDGET_UTILIZATION)
                .with_unit("1")
                .with_description("Tenant-budget utilization after a durable decision.")
                .with_boundaries(WORKFLOW_BUDGET_UTILIZATION.to_vec())
                .build(),
            reservation_age: meter
                .f64_histogram(metric_names::WORKFLOW_BUDGET_RESERVATION_AGE)
                .with_unit("s")
                .with_description("Age of a reservation when it is observed or released.")
                .with_boundaries(WORKFLOW_BUDGET_RESERVATION_AGE_SECONDS.to_vec())
                .build(),
        }
    }

    /// Records one already-durable audit fact.
    ///
    /// Tenant and checkpoint identities are deliberately excluded from metric
    /// attributes to bound cardinality and avoid exporting control-plane
    /// identities.
    pub fn observe(&self, event: &WorkflowBudgetAuditEvent) {
        let decision = budget_decision(event.kind);
        let mut decision_attributes = vec![KeyValue::new("decision", decision)];
        if let WorkflowBudgetAuditKind::Forfeited(reason) = event.kind {
            decision_attributes.push(KeyValue::new("reason", forfeit_reason(reason)));
        }
        self.decisions.add(1, &decision_attributes);
        for (resource, amount, used, limit) in budget_resource_samples(event) {
            let attributes = [
                KeyValue::new("decision", decision),
                KeyValue::new("resource", resource),
            ];
            self.amount.record(amount, &attributes);
            if let Some(utilization) = utilization(used, limit) {
                self.utilization.record(utilization, &attributes);
            }
        }
        if let Some(age_ms) = event.reservation_age_ms {
            self.reservation_age.record(
                std::time::Duration::from_millis(age_ms).as_secs_f64(),
                &decision_attributes,
            );
        }
    }

    pub(crate) fn record_projection_operation(&self, outcome: &'static str) {
        self.projection_operations
            .add(1, &[KeyValue::new("outcome", outcome)]);
    }
}

/// Failure while incrementally projecting durable budget audit facts.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OtelWorkflowBudgetProjectionError {
    /// Reading or advancing durable state failed.
    #[error("tenant-budget audit projection store failed: {0}")]
    Store(#[from] WorkflowStoreError),
    /// Another projector advanced the same named cursor concurrently.
    #[error("tenant-budget audit projection cursor changed concurrently")]
    CursorConflict,
    /// Supervisor timing or bounds are unsafe.
    #[error("invalid tenant-budget projection supervisor configuration: {0}")]
    InvalidConfig(&'static str),
}

/// Progress made by one or more bounded projection batches.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OtelWorkflowBudgetProjectionReport {
    /// Number of durable facts recorded into OpenTelemetry instruments.
    pub events_projected: u64,
    /// Number of non-empty audit pages projected.
    pub batches_projected: u32,
    /// Last fully projected durable cursor.
    pub cursor: Option<WorkflowBudgetAuditCursor>,
    /// Whether the final read proved that no complete page remained.
    pub caught_up: bool,
}

/// Bounded, restart-safe projection of durable budget audit facts into `OTel`.
///
/// The durable cursor advances only after every event in a page is recorded.
/// This provides at-least-once delivery: a crash before cursor advancement can
/// replay the final page, while a successfully checkpointed page is skipped
/// after restart.
#[derive(Debug)]
pub struct OtelWorkflowBudgetProjector<S> {
    store: Arc<S>,
    tenant_id: WorkflowTenantId,
    projection_id: WorkflowBudgetAuditProjectionId,
    page_limit: WorkflowBudgetAuditLimit,
    metrics: OtelWorkflowBudgetMetrics,
}

impl<S> OtelWorkflowBudgetProjector<S>
where
    S: WorkflowStore,
{
    /// Creates a named projection with a default page size of 100 events.
    pub fn new(
        store: Arc<S>,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
        metrics: OtelWorkflowBudgetMetrics,
    ) -> Self {
        Self {
            store,
            tenant_id,
            projection_id,
            page_limit: WorkflowBudgetAuditLimit::default(),
            metrics,
        }
    }

    /// Sets the bounded audit page size.
    #[must_use]
    pub fn with_page_limit(mut self, page_limit: WorkflowBudgetAuditLimit) -> Self {
        self.page_limit = page_limit;
        self
    }

    /// Projects at most one page and durably advances its cursor.
    ///
    /// # Errors
    ///
    /// Returns a typed store failure or a cursor conflict when another
    /// instance uses the same projection identity concurrently.
    pub async fn project_once(
        &self,
    ) -> Result<OtelWorkflowBudgetProjectionReport, OtelWorkflowBudgetProjectionError> {
        let expected = self
            .store
            .load_or_create_tenant_budget_audit_projection(
                self.tenant_id.clone(),
                self.projection_id.clone(),
            )
            .await?;
        let events = self
            .store
            .list_tenant_budget_audit(self.tenant_id.clone(), Some(expected), self.page_limit)
            .await?;
        let Some(last) = events.last() else {
            return Ok(OtelWorkflowBudgetProjectionReport {
                cursor: Some(expected),
                caught_up: true,
                ..OtelWorkflowBudgetProjectionReport::default()
            });
        };
        let next = last.cursor;
        for event in &events {
            self.metrics.observe(event);
        }
        if !self
            .store
            .advance_tenant_budget_audit_projection(
                self.tenant_id.clone(),
                self.projection_id.clone(),
                expected,
                next,
            )
            .await?
        {
            return Err(OtelWorkflowBudgetProjectionError::CursorConflict);
        }
        Ok(OtelWorkflowBudgetProjectionReport {
            events_projected: u64::try_from(events.len()).unwrap_or(u64::MAX),
            batches_projected: 1,
            cursor: Some(next),
            caught_up: events.len() < usize::try_from(self.page_limit.get()).unwrap_or(usize::MAX),
        })
    }

    /// Projects available events using at most `max_batches` pages.
    ///
    /// # Errors
    ///
    /// Returns the first typed projection failure without advancing the
    /// failing page's durable cursor.
    pub async fn project_available(
        &self,
        max_batches: NonZeroU32,
    ) -> Result<OtelWorkflowBudgetProjectionReport, OtelWorkflowBudgetProjectionError> {
        let mut report = OtelWorkflowBudgetProjectionReport::default();
        for _ in 0..max_batches.get() {
            let batch = self.project_once().await?;
            report.events_projected = report
                .events_projected
                .saturating_add(batch.events_projected);
            report.batches_projected = report
                .batches_projected
                .saturating_add(batch.batches_projected);
            report.cursor = batch.cursor.or(report.cursor);
            report.caught_up = batch.caught_up;
            if batch.caught_up {
                break;
            }
        }
        Ok(report)
    }
}

fn budget_decision(kind: WorkflowBudgetAuditKind) -> &'static str {
    match kind {
        WorkflowBudgetAuditKind::PolicyConfigured => "policy_configured",
        WorkflowBudgetAuditKind::Reserved => "reserved",
        WorkflowBudgetAuditKind::Adopted => "adopted",
        WorkflowBudgetAuditKind::AdmissionDenied => "admission_denied",
        WorkflowBudgetAuditKind::UsageExceeded => "usage_exceeded",
        WorkflowBudgetAuditKind::Settled => "settled",
        WorkflowBudgetAuditKind::Forfeited(_) => "forfeited",
        WorkflowBudgetAuditKind::WindowReset => "window_reset",
        _ => "_OTHER",
    }
}

fn forfeit_reason(reason: WorkflowBudgetForfeitReason) -> &'static str {
    match reason {
        WorkflowBudgetForfeitReason::Cancelled => "cancelled",
        WorkflowBudgetForfeitReason::RecoveryExpired => "recovery_expired",
        _ => "_OTHER",
    }
}

fn budget_resource_samples(event: &WorkflowBudgetAuditEvent) -> Vec<(&'static str, u64, u64, u64)> {
    let total = usage_saturating_add(event.committed, event.reserved);
    let duration_limit = event
        .limit
        .duration
        .map(|duration| u64::try_from(duration.as_micros()).unwrap_or(u64::MAX));
    [
        (
            "tokens",
            event.usage.tokens,
            total.tokens,
            event.limit.tokens,
        ),
        (
            "cost",
            event.usage.cost_microusd,
            total.cost_microusd,
            event.limit.cost_microusd,
        ),
        (
            "duration",
            event.usage.duration_micros,
            total.duration_micros,
            duration_limit,
        ),
        ("turns", event.usage.turns, total.turns, event.limit.turns),
        (
            "tool_calls",
            event.usage.tool_calls,
            total.tool_calls,
            event.limit.tool_calls,
        ),
        (
            "delegations",
            event.usage.delegations,
            total.delegations,
            event.limit.delegations,
        ),
    ]
    .into_iter()
    .filter_map(|(resource, amount, used, limit)| {
        limit.map(|limit| (resource, amount, used, limit))
    })
    .collect()
}

fn usage_saturating_add(
    left: runifold_core::Usage,
    right: runifold_core::Usage,
) -> runifold_core::Usage {
    runifold_core::Usage {
        tokens: left.tokens.saturating_add(right.tokens),
        cost_microusd: left.cost_microusd.saturating_add(right.cost_microusd),
        duration_micros: left.duration_micros.saturating_add(right.duration_micros),
        turns: left.turns.saturating_add(right.turns),
        tool_calls: left.tool_calls.saturating_add(right.tool_calls),
        delegations: left.delegations.saturating_add(right.delegations),
    }
}

fn utilization(used: u64, limit: u64) -> Option<f64> {
    if limit == 0 {
        return None;
    }
    Some(
        std::time::Duration::from_nanos(used).as_secs_f64()
            / std::time::Duration::from_nanos(limit).as_secs_f64(),
    )
}
