//! Durable, capability-safe workflow and Agent orchestration for Runifold.

mod checkpoint;
mod error;
mod execution;
mod outcome;
mod parallel;
mod race;
mod step;
mod workflow;

pub use checkpoint::{
    ParallelBranchCheckpoint, WorkflowCheckpoint, WorkflowCheckpointPhase, WorkflowCheckpointState,
    WorkflowResumePolicy,
};
pub use error::{WorkflowBuildError, WorkflowError, WorkflowStepError};
pub use execution::WorkflowFuture;
pub use outcome::WorkflowOutcome;
pub use step::{
    AgentStep, AgentStepOutput, PredicateCondition, StepId, WorkflowCondition, WorkflowStep,
    WorkflowStepFuture,
};
pub use workflow::{ParallelBranch, Workflow, WorkflowBuilder};

#[cfg(test)]
mod tests;
