use std::{collections::BTreeMap, fmt, sync::Arc};

use runifold_core::{
    Checkpoint, CheckpointError, CheckpointErrorKind, CheckpointId, CheckpointStore, RunContext,
    Usage,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{StepId, WorkflowError, WorkflowOutcome};

const CHECKPOINT_KIND: &str = "runifold.workflow";
const CHECKPOINT_SCHEMA_VERSION: u32 = 3;

/// Recovery behavior for a workflow interrupted inside one node.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowResumePolicy {
    /// Reject recovery that could duplicate model cost or external effects.
    #[default]
    RejectAmbiguous,
    /// Explicitly retry only the interrupted workflow node.
    RetryInterruptedStep,
}

/// Persisted workflow execution phase.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
#[non_exhaustive]
pub enum WorkflowCheckpointPhase {
    /// Stable output is ready for the next node.
    Ready,
    /// One node may have partially executed.
    StepInFlight {
        /// Stable interrupted node identity.
        step: StepId,
    },
    /// A parallel node has one or more incomplete branches.
    ParallelInFlight {
        /// Stable parallel node identity.
        step: StepId,
        /// Durable branch progress keyed independently of completion order.
        branches: BTreeMap<StepId, ParallelBranchCheckpoint>,
    },
    /// A first-success race has no durable winner yet, or awaits commit.
    RaceInFlight {
        /// Stable race node identity.
        step: StepId,
        /// Durable branch progress keyed independently of completion order.
        branches: BTreeMap<StepId, ParallelBranchCheckpoint>,
    },
    /// The workflow reached a terminal output.
    Completed {
        /// Complete canonical workflow result.
        outcome: WorkflowOutcome,
    },
}

/// Persisted state of one branch inside an in-flight parallel node.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ParallelBranchCheckpoint {
    /// The branch may have partially executed.
    InFlight,
    /// The branch returned a stable canonical output.
    Completed {
        /// Canonical branch output.
        output: Value,
    },
    /// The branch returned a known failure.
    Failed {
        /// Safe persisted failure explanation.
        message: String,
    },
    /// The branch was abandoned after another branch won.
    Cancelled,
}

/// Versioned workflow state stored in a domain-neutral checkpoint envelope.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkflowCheckpointState {
    /// Stable workflow definition name.
    pub workflow: String,
    /// Caller-managed definition version.
    pub workflow_version: u32,
    /// Ordered node layout used to reject incompatible definitions.
    pub layout: Vec<StepId>,
    /// Index of the next node to execute.
    pub next_index: usize,
    /// Canonical value presented to the next node.
    pub value: Value,
    /// Stable outputs of all completed nodes.
    pub outputs: BTreeMap<StepId, Value>,
    /// Shared usage snapshot at persistence time.
    pub usage: Usage,
    /// Current recovery phase.
    pub phase: WorkflowCheckpointPhase,
}

impl WorkflowCheckpointState {
    pub(crate) fn outcome(&self) -> Option<WorkflowOutcome> {
        match &self.phase {
            WorkflowCheckpointPhase::Completed { outcome } => Some(outcome.clone()),
            _ => None,
        }
    }
}

/// Stable handle binding one workflow checkpoint identity to a store.
#[derive(Clone)]
pub struct WorkflowCheckpoint {
    id: CheckpointId,
    store: Arc<dyn CheckpointStore>,
}

impl WorkflowCheckpoint {
    /// Creates a new workflow checkpoint handle.
    pub fn new(store: Arc<dyn CheckpointStore>) -> Self {
        Self {
            id: CheckpointId::new(),
            store,
        }
    }

    /// Reconnects to an existing workflow checkpoint.
    pub fn existing(id: CheckpointId, store: Arc<dyn CheckpointStore>) -> Self {
        Self { id, store }
    }

    /// Returns the stable checkpoint identity.
    pub const fn id(&self) -> CheckpointId {
        self.id
    }

    /// Loads and validates the latest typed workflow state.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError`] for storage or payload failures.
    pub fn load(&self) -> Result<(Checkpoint, WorkflowCheckpointState), CheckpointError> {
        let checkpoint = self.store.load(self.id)?;
        if checkpoint.kind != CHECKPOINT_KIND
            || checkpoint.schema_version != CHECKPOINT_SCHEMA_VERSION
        {
            return Err(CheckpointError::new(
                CheckpointErrorKind::InvalidPayload,
                "checkpoint kind or schema version is not supported",
            ));
        }
        let state = serde_json::from_value(checkpoint.payload.clone()).map_err(|error| {
            CheckpointError::new(CheckpointErrorKind::InvalidPayload, error.to_string())
        })?;
        Ok((checkpoint, state))
    }
}

impl fmt::Debug for WorkflowCheckpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowCheckpoint")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

pub(crate) struct WorkflowCheckpointCursor {
    handle: WorkflowCheckpoint,
    envelope: Checkpoint,
}

impl WorkflowCheckpointCursor {
    pub(crate) fn create(
        handle: &WorkflowCheckpoint,
        run: &RunContext,
        state: &WorkflowCheckpointState,
    ) -> Result<Self, WorkflowError> {
        let envelope = Checkpoint::initial(
            handle.id,
            run.run_id(),
            CHECKPOINT_KIND,
            CHECKPOINT_SCHEMA_VERSION,
            serialize(state)?,
        );
        handle.store.compare_and_swap(&envelope, None)?;
        Ok(Self {
            handle: handle.clone(),
            envelope,
        })
    }

    pub(crate) fn loaded(handle: &WorkflowCheckpoint, envelope: Checkpoint) -> Self {
        Self {
            handle: handle.clone(),
            envelope,
        }
    }

    pub(crate) fn save(&mut self, state: &WorkflowCheckpointState) -> Result<(), WorkflowError> {
        let next = self.envelope.next(serialize(state)?)?;
        self.handle
            .store
            .compare_and_swap(&next, Some(self.envelope.revision))?;
        self.envelope = next;
        Ok(())
    }
}

fn serialize(state: &WorkflowCheckpointState) -> Result<Value, WorkflowError> {
    serde_json::to_value(state).map_err(|error| {
        CheckpointError::new(CheckpointErrorKind::InvalidPayload, error.to_string()).into()
    })
}
