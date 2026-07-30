//! Workflow-budget persistence representation and domain conversions.

use std::time::Duration;

use runifold_core::{Budget, Usage};
use runifold_workflow::{
    WorkflowBudgetAuditKind, WorkflowBudgetForfeitReason, WorkflowBudgetReservationOutcome,
    WorkflowStoreError, WorkflowStoreErrorKind,
};
use serde::{Deserialize, Serialize};

use super::support::lease_lost;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct StoredBudgetLimit {
    pub(super) tokens: Option<u64>,
    pub(super) cost_microusd: Option<u64>,
    pub(super) duration_micros: Option<u64>,
    pub(super) turns: Option<u64>,
    pub(super) tool_calls: Option<u64>,
    pub(super) delegations: Option<u64>,
}

impl StoredBudgetLimit {
    pub(super) fn from_budget(budget: Budget) -> Result<Self, WorkflowStoreError> {
        Ok(Self {
            tokens: budget.tokens,
            cost_microusd: budget.cost_microusd,
            duration_micros: postgres_duration_micros(budget.duration)?,
            turns: budget.turns,
            tool_calls: budget.tool_calls,
            delegations: budget.delegations,
        })
    }

    pub(super) fn into_budget(self) -> Budget {
        Budget {
            tokens: self.tokens,
            cost_microusd: self.cost_microusd,
            duration: self.duration_micros.map(Duration::from_micros),
            turns: self.turns,
            tool_calls: self.tool_calls,
            delegations: self.delegations,
        }
    }
}

pub(super) fn decode_budget_audit_kind(
    kind: &str,
    reason: Option<&str>,
) -> Result<WorkflowBudgetAuditKind, WorkflowStoreError> {
    match (kind, reason) {
        ("policy_configured", None) => Ok(WorkflowBudgetAuditKind::PolicyConfigured),
        ("reserved", None) => Ok(WorkflowBudgetAuditKind::Reserved),
        ("adopted", None) => Ok(WorkflowBudgetAuditKind::Adopted),
        ("admission_denied", None) => Ok(WorkflowBudgetAuditKind::AdmissionDenied),
        ("usage_exceeded", None) => Ok(WorkflowBudgetAuditKind::UsageExceeded),
        ("settled", None) => Ok(WorkflowBudgetAuditKind::Settled),
        ("window_reset", None) => Ok(WorkflowBudgetAuditKind::WindowReset),
        ("forfeited", Some("cancelled")) => Ok(WorkflowBudgetAuditKind::Forfeited(
            WorkflowBudgetForfeitReason::Cancelled,
        )),
        ("forfeited", Some("recovery_expired")) => Ok(WorkflowBudgetAuditKind::Forfeited(
            WorkflowBudgetForfeitReason::RecoveryExpired,
        )),
        _ => Err(WorkflowStoreError::new(
            WorkflowStoreErrorKind::Storage,
            "stored workflow budget audit kind is unsupported",
        )),
    }
}

pub(super) fn decode_budget_reservation_status(
    status: &str,
) -> Result<WorkflowBudgetReservationOutcome, WorkflowStoreError> {
    match status {
        "reserved" => Ok(WorkflowBudgetReservationOutcome::Reserved),
        "not_configured" => Ok(WorkflowBudgetReservationOutcome::NotConfigured),
        "lease_lost" => Err(lease_lost()),
        "admission_denied" | "reservation_exceeded" => Err(WorkflowStoreError::new(
            WorkflowStoreErrorKind::AdmissionDenied,
            "workflow tenant aggregate budget is exhausted",
        )),
        "baseline_backwards" | "envelope_changed" | "conflict" | "policy_changed" => {
            Err(WorkflowStoreError::new(
                WorkflowStoreErrorKind::Conflict,
                "workflow budget reservation conflicts with durable state",
            ))
        }
        _ => Err(WorkflowStoreError::new(
            WorkflowStoreErrorKind::Storage,
            "PostgreSQL returned an unknown budget reservation status",
        )),
    }
}

pub(super) fn decode_budget_settlement_status(status: &str) -> Result<(), WorkflowStoreError> {
    match status {
        "settled" => Ok(()),
        "lease_lost" => Err(lease_lost()),
        "reservation_exceeded" => Err(WorkflowStoreError::new(
            WorkflowStoreErrorKind::AdmissionDenied,
            "workflow checkpoint usage exceeded its reserved tenant budget",
        )),
        "missing_reservation" | "baseline_backwards" => Err(WorkflowStoreError::new(
            WorkflowStoreErrorKind::Conflict,
            "workflow budget settlement conflicts with durable state",
        )),
        _ => Err(WorkflowStoreError::new(
            WorkflowStoreErrorKind::Storage,
            "PostgreSQL returned an unknown budget settlement status",
        )),
    }
}

pub(super) fn postgres_budget_request(
    tenant_limit: Budget,
    workflow_limit: Budget,
    baseline: Usage,
) -> Result<Usage, WorkflowStoreError> {
    Ok(Usage {
        tokens: postgres_remaining_budget(
            "tokens",
            tenant_limit.tokens,
            workflow_limit.tokens,
            baseline.tokens,
        )?,
        cost_microusd: postgres_remaining_budget(
            "cost",
            tenant_limit.cost_microusd,
            workflow_limit.cost_microusd,
            baseline.cost_microusd,
        )?,
        duration_micros: postgres_remaining_budget(
            "duration",
            postgres_duration_micros(tenant_limit.duration)?,
            postgres_duration_micros(workflow_limit.duration)?,
            baseline.duration_micros,
        )?,
        turns: postgres_remaining_budget(
            "turns",
            tenant_limit.turns,
            workflow_limit.turns,
            baseline.turns,
        )?,
        tool_calls: postgres_remaining_budget(
            "tool calls",
            tenant_limit.tool_calls,
            workflow_limit.tool_calls,
            baseline.tool_calls,
        )?,
        delegations: postgres_remaining_budget(
            "delegations",
            tenant_limit.delegations,
            workflow_limit.delegations,
            baseline.delegations,
        )?,
    })
}

fn postgres_duration_micros(duration: Option<Duration>) -> Result<Option<u64>, WorkflowStoreError> {
    duration
        .map(|duration| {
            u64::try_from(duration.as_micros()).map_err(|_| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::InvalidInput,
                    "budget duration exceeds supported microseconds",
                )
            })
        })
        .transpose()
}

fn postgres_remaining_budget(
    resource: &str,
    tenant_limit: Option<u64>,
    workflow_limit: Option<u64>,
    used: u64,
) -> Result<u64, WorkflowStoreError> {
    if tenant_limit.is_none() {
        return Ok(0);
    }
    let workflow_limit = workflow_limit.ok_or_else(|| {
        WorkflowStoreError::new(
            WorkflowStoreErrorKind::InvalidInput,
            format!("tenant-controlled {resource} requires a finite workflow definition limit"),
        )
    })?;
    workflow_limit.checked_sub(used).ok_or_else(|| {
        WorkflowStoreError::new(
            WorkflowStoreErrorKind::Conflict,
            format!("persisted workflow {resource} exceeds its definition limit"),
        )
    })
}

pub(super) fn budget_encoding(error: serde_json::Error) -> WorkflowStoreError {
    drop(error);
    WorkflowStoreError::new(
        WorkflowStoreErrorKind::InvalidInput,
        "workflow budget cannot be encoded",
    )
}

pub(super) fn budget_decoding(error: serde_json::Error) -> WorkflowStoreError {
    drop(error);
    WorkflowStoreError::new(
        WorkflowStoreErrorKind::Storage,
        "stored workflow budget violates domain invariants",
    )
}
