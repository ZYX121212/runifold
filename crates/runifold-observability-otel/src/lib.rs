//! OpenTelemetry `GenAI` instrumentation for Runifold.

mod config;
mod correlation;
mod journal;
mod journal_metrics;
mod model;
mod runtime;
pub mod slo;
#[cfg(test)]
mod test_support;
mod workflow_budget;
mod workflow_budget_coordinator;
mod workflow_budget_supervisor;
mod workflow_task_cleanup;
mod workflow_task_governance;

pub use config::{ContentCapture, OtelConfig};
pub(crate) use correlation::CorrelationRegistry;
pub use journal::OtelJournal;
pub use model::OtelModel;
pub use runtime::OtelRuntime;
pub use workflow_budget::{
    OtelWorkflowBudgetMetrics, OtelWorkflowBudgetProjectionError,
    OtelWorkflowBudgetProjectionReport, OtelWorkflowBudgetProjector,
};
pub use workflow_budget_coordinator::{
    OtelWorkflowBudgetCoordinator, OtelWorkflowBudgetCoordinatorConfig,
    OtelWorkflowBudgetCoordinatorReport, OtelWorkflowBudgetShard,
};
pub use workflow_budget_supervisor::{
    OtelWorkflowBudgetSupervisor, OtelWorkflowBudgetSupervisorConfig,
    OtelWorkflowBudgetSupervisorCycleOutcome, OtelWorkflowBudgetSupervisorMetricSnapshot,
    OtelWorkflowBudgetSupervisorMetrics, OtelWorkflowBudgetSupervisorReport,
};
pub use workflow_task_cleanup::OtelWorkflowTaskCleanupMetrics;
pub use workflow_task_governance::OtelWorkflowTaskGovernanceMetrics;
