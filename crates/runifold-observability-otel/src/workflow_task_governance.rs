//! Low-cardinality telemetry for Task tombstone governance decisions.

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Meter},
};
use runifold_workflow::{
    WorkflowTaskGovernanceObserver, WorkflowTaskGovernanceOutcome, WorkflowTaskGovernancePermission,
};

use crate::slo::metric_names;

/// OpenTelemetry observer for authorized tombstone governance operations.
///
/// Principal, tenant, checkpoint, purge, and receipt identities are excluded.
#[derive(Clone, Debug)]
pub struct OtelWorkflowTaskGovernanceMetrics {
    operations: Counter<u64>,
}

impl OtelWorkflowTaskGovernanceMetrics {
    pub(crate) fn new(meter: &Meter) -> Self {
        Self {
            operations: meter
                .u64_counter(metric_names::WORKFLOW_TASK_GOVERNANCE_OPERATIONS)
                .with_description(
                    "Authorized Task tombstone governance operations and terminal outcomes.",
                )
                .build(),
        }
    }
}

impl WorkflowTaskGovernanceObserver for OtelWorkflowTaskGovernanceMetrics {
    fn observe(
        &self,
        permission: WorkflowTaskGovernancePermission,
        outcome: WorkflowTaskGovernanceOutcome,
    ) {
        self.operations.add(
            1,
            &[
                KeyValue::new("operation", permission_name(permission)),
                KeyValue::new("outcome", outcome_name(outcome)),
            ],
        );
    }
}

fn permission_name(permission: WorkflowTaskGovernancePermission) -> &'static str {
    match permission {
        WorkflowTaskGovernancePermission::PlaceHold => "place_hold",
        WorkflowTaskGovernancePermission::ReleaseHold => "release_hold",
        WorkflowTaskGovernancePermission::Export => "export",
        WorkflowTaskGovernancePermission::PreparePurge => "prepare_purge",
        WorkflowTaskGovernancePermission::ApprovePurge => "approve_purge",
        WorkflowTaskGovernancePermission::ReadApprovalInbox => "read_approval_inbox",
        WorkflowTaskGovernancePermission::ClaimPurgeApproval => "claim_purge_approval",
        WorkflowTaskGovernancePermission::RejectPurge => "reject_purge",
        WorkflowTaskGovernancePermission::ExecutePurge => "execute_purge",
        WorkflowTaskGovernancePermission::ReadEvidence => "read_evidence",
        _ => "unknown",
    }
}

fn outcome_name(outcome: WorkflowTaskGovernanceOutcome) -> &'static str {
    match outcome {
        WorkflowTaskGovernanceOutcome::Succeeded => "succeeded",
        WorkflowTaskGovernanceOutcome::Denied => "denied",
        WorkflowTaskGovernanceOutcome::AuthorizationError => "authorization_error",
        WorkflowTaskGovernanceOutcome::StoreError => "store_error",
        WorkflowTaskGovernanceOutcome::ArchiveError => "archive_error",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

    use super::*;

    #[test]
    fn exports_only_fixed_permission_and_outcome_dimensions() {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let metrics = OtelWorkflowTaskGovernanceMetrics::new(&provider.meter("runifold.test"));
        metrics.observe(
            WorkflowTaskGovernancePermission::ExecutePurge,
            WorkflowTaskGovernanceOutcome::Denied,
        );
        metrics.observe(
            WorkflowTaskGovernancePermission::ClaimPurgeApproval,
            WorkflowTaskGovernanceOutcome::Succeeded,
        );
        provider.force_flush().unwrap();
        let dump = format!("{:?}", exporter.get_finished_metrics().unwrap());
        assert!(dump.contains(metric_names::WORKFLOW_TASK_GOVERNANCE_OPERATIONS));
        assert!(dump.contains("execute_purge"));
        assert!(dump.contains("denied"));
        assert!(dump.contains("claim_purge_approval"));
        assert!(!dump.contains("tenant-a"));
    }
}
