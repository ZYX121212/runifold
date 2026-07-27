use runifold_agent::AgentError;
use std::collections::BTreeMap;

use runifold_core::{BudgetExceeded, BudgetReservationMismatch, CheckpointError, JournalError};
use thiserror::Error;

use crate::StepId;

/// Invalid workflow definition.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowBuildError {
    /// The workflow name is blank.
    #[error("workflow name cannot be empty")]
    EmptyName,
    /// A step identifier is invalid.
    #[error("invalid workflow step identifier `{0}`")]
    InvalidStepId(String),
    /// The same step identifier occurs more than once.
    #[error("workflow step `{0}` is registered more than once")]
    DuplicateStep(StepId),
    /// The workflow has no executable nodes.
    #[error("workflow requires at least one step")]
    NoSteps,
    /// Version zero is reserved for invalid or unversioned definitions.
    #[error("workflow version must be greater than zero")]
    InvalidVersion,
    /// A parallel group requires at least two branches.
    #[error("parallel workflow step `{0}` requires at least two branches")]
    TooFewParallelBranches(StepId),
    /// A first-success race requires at least two branches.
    #[error("race workflow step `{0}` requires at least two branches")]
    TooFewRaceBranches(StepId),
    /// A parallel branch identifier is invalid.
    #[error("invalid parallel branch identifier `{0}`")]
    InvalidParallelBranchId(String),
    /// A parallel branch identifier occurs more than once in its group.
    #[error("parallel branch `{branch}` is registered more than once in step `{step}`")]
    DuplicateParallelBranch {
        /// Parallel group identity.
        step: StepId,
        /// Duplicate branch identity.
        branch: StepId,
    },
    /// A race branch requested a capability that may mutate external state.
    #[error(
        "race branch `{branch}` in step `{step}` cannot abandon capability `{capability}` with external write effects"
    )]
    UnsafeRaceCapability {
        /// Race node identity.
        step: StepId,
        /// Unsafe branch identity.
        branch: StepId,
        /// Rejected capability name.
        capability: String,
    },
}

/// Failure produced by one workflow step.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum WorkflowStepError {
    /// An Agent step failed.
    #[error("Agent execution failed: {0}")]
    Agent(#[from] AgentError),
    /// The step rejected its canonical JSON input.
    #[error("invalid step input: {0}")]
    InvalidInput(String),
    /// The step could not produce a canonical downstream value.
    #[error("invalid step output: {0}")]
    InvalidOutput(String),
    /// The step failed while converting its output.
    #[error("step output serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    /// A custom step failed with a safe application-facing explanation.
    #[error("step execution failed: {0}")]
    Execution(String),
}

/// Failure of workflow execution or recovery.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum WorkflowError {
    /// A workflow definition is invalid.
    #[error(transparent)]
    Build(#[from] WorkflowBuildError),
    /// A child requested authority absent from its parent run.
    #[error("workflow step `{step}` requested capability `{capability}` not held by its parent")]
    AuthorityEscalation {
        /// Step whose grant was rejected.
        step: StepId,
        /// Missing capability name.
        capability: String,
    },
    /// A step failed.
    #[error("workflow step `{step}` failed: {source}")]
    Step {
        /// Stable failed step identifier.
        step: StepId,
        /// Typed step failure.
        #[source]
        source: Box<WorkflowStepError>,
    },
    /// One branch of a parallel node failed.
    #[error("parallel branch `{branch}` in workflow step `{step}` failed: {source}")]
    ParallelBranch {
        /// Stable parallel node identifier.
        step: StepId,
        /// Stable failed branch identifier.
        branch: StepId,
        /// Typed branch failure.
        #[source]
        source: Box<WorkflowStepError>,
    },
    /// Every branch in a first-success race failed.
    #[error("every branch in workflow race step `{step}` failed")]
    RaceAllFailed {
        /// Stable race node identifier.
        step: StepId,
        /// Safe branch failure explanations.
        failures: BTreeMap<StepId, String>,
    },
    /// The workflow was cancelled.
    #[error("workflow execution was cancelled")]
    Cancelled,
    /// The workflow deadline elapsed.
    #[error("workflow execution deadline elapsed")]
    DeadlineExceeded,
    /// Parallel budget reservation or consumption exceeded a hard limit.
    #[error("workflow budget exceeded: {0}")]
    Budget(#[from] BudgetExceeded),
    /// An internal reservation did not belong to this workflow's run tree.
    #[error("workflow budget reservation is invalid: {0}")]
    BudgetReservation(#[from] BudgetReservationMismatch),
    /// Recovery would silently retry a possibly partial step.
    #[error("checkpoint contains ambiguous in-flight workflow step `{step}`")]
    AmbiguousCheckpoint {
        /// Interrupted step identifier.
        step: StepId,
    },
    /// Persisted state does not belong to this workflow definition.
    #[error("workflow checkpoint does not match the current workflow definition")]
    CheckpointIdentityMismatch,
    /// Persisted usage is incompatible with the supplied run context.
    #[error("workflow checkpoint usage is incompatible with the supplied run")]
    CheckpointUsageMismatch,
    /// Structured event recording failed.
    #[error("workflow observability failed: {0}")]
    Journal(#[from] JournalError),
    /// Checkpoint persistence or validation failed.
    #[error("workflow checkpoint failed: {0}")]
    Checkpoint(#[from] CheckpointError),
}
