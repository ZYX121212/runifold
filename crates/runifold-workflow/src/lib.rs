//! Durable, capability-safe workflow and Agent orchestration for Runifold.

mod checkpoint;
mod error;
mod execution;
mod governance;
mod outcome;
mod parallel;
mod race;
mod remediation;
mod reviewer;
mod step;
mod store;
mod task_retention;
mod tombstone;
mod wait;
mod worker;
mod workflow;

pub use checkpoint::{
    ParallelBranchCheckpoint, WorkflowCheckpoint, WorkflowCheckpointHistoryLimit,
    WorkflowCheckpointPhase, WorkflowCheckpointRevision, WorkflowCheckpointState,
    WorkflowForkCommand, WorkflowForkOutcome, WorkflowForkPolicy, WorkflowLineage,
    WorkflowResumePolicy,
};
pub use error::{WorkflowBuildError, WorkflowError, WorkflowStepError};
pub use execution::WorkflowFuture;
pub use governance::{
    StaticWorkflowTaskGovernanceAuthorizer, WorkflowTaskGovernanceAuthorizationError,
    WorkflowTaskGovernanceAuthorizationFuture, WorkflowTaskGovernanceAuthorizer,
    WorkflowTaskGovernanceControlPlane, WorkflowTaskGovernanceError,
    WorkflowTaskGovernanceObserver, WorkflowTaskGovernanceOutcome,
    WorkflowTaskGovernancePermission, WorkflowTaskTombstoneArchive,
    WorkflowTaskTombstoneArchiveBatch, WorkflowTaskTombstoneArchiveBatchId,
    WorkflowTaskTombstoneArchiveError, WorkflowTaskTombstoneArchiveErrorKind,
    WorkflowTaskTombstoneArchiveFuture, WorkflowTaskTombstoneArchiveReport,
};
pub use outcome::WorkflowOutcome;
pub use remediation::{
    WorkflowRemediationCheckpoint, WorkflowRemediationPolicy, WorkflowRepairInput,
    WorkflowReviewError, WorkflowReviewFuture, WorkflowReviewRequest, WorkflowReviewVerdict,
    WorkflowReviewer,
};
pub use reviewer::{
    AgentReviewDecision, AgentReviewDecisionKind, AgentReviewer, CompositeReviewMode,
    CompositeReviewer, ReviewFinding, ReviewRubric, ReviewSeverity, RuleReviewer,
};
pub use step::{
    AgentStep, AgentStepOutput, PredicateCondition, StepId, WorkflowCondition, WorkflowStep,
    WorkflowStepFuture,
};
pub use store::{
    ClaimedWorkflow, InMemoryWorkflowStore, LeaseDuration, SystemWorkflowClock, WorkerId,
    WorkflowBudgetAuditCursor, WorkflowBudgetAuditEvent, WorkflowBudgetAuditKind,
    WorkflowBudgetAuditLimit, WorkflowBudgetAuditProjectionId, WorkflowBudgetAuditProjectionLease,
    WorkflowBudgetForfeitReason, WorkflowBudgetReservationOutcome, WorkflowCancelOutcome,
    WorkflowClock, WorkflowDisposition, WorkflowLease, WorkflowStore, WorkflowStoreError,
    WorkflowStoreErrorKind, WorkflowStoreFuture, WorkflowTask, WorkflowTaskCleanupLease,
    WorkflowTaskCleanupLimit, WorkflowTaskRetention, WorkflowTaskRetentionStore,
    WorkflowTaskSnapshot, WorkflowTaskStatus, WorkflowTaskTombstone, WorkflowTaskTombstoneCursor,
    WorkflowTaskTombstoneLimit, WorkflowTenantBudgetPolicy, WorkflowTenantBudgetSnapshot,
    WorkflowTenantId, WorkflowTenantListLimit, WorkflowTenantPolicy,
};
pub use task_retention::{
    WorkflowTaskCleanupObserver, WorkflowTaskCleanupShard, WorkflowTaskCleanupSupervisor,
    WorkflowTaskCleanupSupervisorConfig, WorkflowTaskCleanupSupervisorMetricSnapshot,
    WorkflowTaskCleanupSupervisorMetrics, WorkflowTaskCleanupSupervisorReport,
};
pub use tombstone::{
    WorkflowTaskLegalHold, WorkflowTaskLegalHoldReason, WorkflowTaskTombstoneApprovalInboxItem,
    WorkflowTaskTombstoneApprovalInboxLimit, WorkflowTaskTombstoneApprovalLease,
    WorkflowTaskTombstoneApprovalState, WorkflowTaskTombstoneApprovalWindow,
    WorkflowTaskTombstoneExport, WorkflowTaskTombstoneExportReceipt,
    WorkflowTaskTombstoneGovernanceStore, WorkflowTaskTombstonePurgeEvidence,
    WorkflowTaskTombstonePurgeId, WorkflowTaskTombstonePurgeIntent,
    WorkflowTaskTombstonePurgeLimit, WorkflowTaskTombstoneRejectionReason,
    WorkflowTaskTombstoneRetention,
};
pub use wait::{
    WorkflowInterruptCommand, WorkflowInterruptDecision, WorkflowInterruptDecisionOutcome,
    WorkflowInterruptId, WorkflowInterruptOutcome, WorkflowInterruptRequest, WorkflowSignal,
    WorkflowSignalId, WorkflowSignalName, WorkflowSignalOutcome, WorkflowSignalRetention,
    WorkflowSignalSnapshot, WorkflowSignalState, WorkflowWait, WorkflowWaitError,
    WorkflowWaitOutcome, WorkflowWake,
};
pub use worker::{
    SystemWorkflowWorkerSleeper, WorkflowDefinition, WorkflowFailurePolicy, WorkflowRegistry,
    WorkflowSupervisor, WorkflowSupervisorConfig, WorkflowSupervisorMetricSnapshot,
    WorkflowSupervisorMetrics, WorkflowSupervisorReport, WorkflowWorker, WorkflowWorkerError,
    WorkflowWorkerOutcome, WorkflowWorkerSleepFuture, WorkflowWorkerSleeper,
};
pub use workflow::{ParallelBranch, Workflow, WorkflowBuilder};

#[cfg(test)]
mod tests;
