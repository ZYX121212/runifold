//! Low-cardinality telemetry for the terminal Task cleanup control plane.

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Meter},
};
use runifold_workflow::{WorkflowTaskCleanupObserver, WorkflowTaskCleanupSupervisorReport};

use crate::slo::metric_names;

/// OpenTelemetry observer for dynamically sharded terminal Task cleanup.
///
/// Tenant, workflow, and checkpoint identities are deliberately excluded.
#[derive(Clone, Debug)]
pub struct OtelWorkflowTaskCleanupMetrics {
    operations: Counter<u64>,
    tenants: Counter<u64>,
    batches: Counter<u64>,
    tasks_deleted: Counter<u64>,
}

impl OtelWorkflowTaskCleanupMetrics {
    pub(crate) fn new(meter: &Meter) -> Self {
        Self {
            operations: meter
                .u64_counter(metric_names::WORKFLOW_TASK_CLEANUP_OPERATIONS)
                .with_description("Task cleanup scan, claim, contention, and failure outcomes.")
                .build(),
            tenants: meter
                .u64_counter(metric_names::WORKFLOW_TASK_CLEANUP_TENANTS)
                .with_description("Tenants discovered and assigned for Task cleanup.")
                .build(),
            batches: meter
                .u64_counter(metric_names::WORKFLOW_TASK_CLEANUP_BATCHES)
                .with_description("Non-empty terminal Task cleanup batches committed.")
                .build(),
            tasks_deleted: meter
                .u64_counter(metric_names::WORKFLOW_TASK_CLEANUP_DELETED)
                .with_description("Terminal Tasks atomically tombstoned and deleted.")
                .build(),
        }
    }

    fn record_operation(&self, outcome: &'static str, count: u64) {
        if count > 0 {
            self.operations
                .add(count, &[KeyValue::new("outcome", outcome)]);
        }
    }
}

impl WorkflowTaskCleanupObserver for OtelWorkflowTaskCleanupMetrics {
    fn observe_scan(&self, report: WorkflowTaskCleanupSupervisorReport) {
        self.record_operation("scan_completed", report.scans);
        self.record_operation("claimed", report.claims);
        self.record_operation("contended", report.contended);
        self.record_operation("lease_lost", report.leases_lost);
        self.record_operation("store_error", report.infrastructure_errors);
        self.tenants.add(
            report.tenants_discovered,
            &[KeyValue::new("state", "discovered")],
        );
        self.tenants.add(
            report.tenants_assigned,
            &[KeyValue::new("state", "assigned")],
        );
        self.batches.add(report.batches_cleaned, &[]);
        self.tasks_deleted.add(report.tasks_deleted, &[]);
    }

    fn observe_discovery_error(&self) {
        self.record_operation("discovery_error", 1);
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

    use super::*;

    #[test]
    fn exports_cleanup_outcomes_without_tenant_identity() {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        let metrics = OtelWorkflowTaskCleanupMetrics::new(&provider.meter("runifold.test"));
        metrics.observe_scan(WorkflowTaskCleanupSupervisorReport {
            scans: 1,
            tenants_discovered: 3,
            tenants_assigned: 2,
            claims: 2,
            contended: 1,
            batches_cleaned: 2,
            tasks_deleted: 7,
            ..WorkflowTaskCleanupSupervisorReport::default()
        });
        metrics.observe_discovery_error();
        provider.force_flush().unwrap();
        let dump = format!("{:?}", exporter.get_finished_metrics().unwrap());
        assert!(dump.contains(metric_names::WORKFLOW_TASK_CLEANUP_OPERATIONS));
        assert!(dump.contains(metric_names::WORKFLOW_TASK_CLEANUP_DELETED));
        assert!(dump.contains("scan_completed"));
        assert!(dump.contains("discovery_error"));
        assert!(!dump.contains("tenant-a"));
    }
}
