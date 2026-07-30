use std::{collections::BTreeMap, fmt, num::NonZeroU16, sync::Arc};

use runifold_core::{
    Checkpoint, CheckpointError, CheckpointErrorKind, CheckpointId, CheckpointStore, RunContext,
    RunId, Usage,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{StepId, WorkflowError, WorkflowOutcome, WorkflowWait};
use crate::{WorkflowLease, WorkflowStore};

const CHECKPOINT_KIND: &str = "runifold.workflow";
const CHECKPOINT_SCHEMA_VERSION: u32 = 4;
const MIN_CHECKPOINT_SCHEMA_VERSION: u32 = 3;

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

/// Maximum number of immutable checkpoint revisions returned by one query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkflowCheckpointHistoryLimit(NonZeroU16);

impl WorkflowCheckpointHistoryLimit {
    /// Creates a bounded history page limit.
    ///
    /// # Errors
    ///
    /// Rejects zero and values above 256.
    pub fn new(value: u16) -> Result<Self, CheckpointError> {
        NonZeroU16::new(value)
            .filter(|value| value.get() <= 256)
            .map(Self)
            .ok_or_else(|| {
                CheckpointError::new(
                    CheckpointErrorKind::InvalidPayload,
                    "workflow checkpoint history limit must be in 1..=256",
                )
            })
    }

    /// Returns the validated page size.
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

/// Immutable, typed view of one historical workflow checkpoint revision.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkflowCheckpointRevision {
    /// Workflow checkpoint whose history owns this revision.
    pub checkpoint_id: CheckpointId,
    /// Monotonic revision within the checkpoint.
    pub revision: u64,
    /// Run that produced the immutable revision.
    pub run_id: RunId,
    /// Store timestamp carried by the checkpoint envelope.
    pub updated_at_ms: u64,
    /// Validated workflow state at this revision.
    pub state: WorkflowCheckpointState,
}

impl WorkflowCheckpointRevision {
    #[doc(hidden)]
    pub fn from_checkpoint(checkpoint: Checkpoint) -> Result<Self, CheckpointError> {
        decode_revision(checkpoint)
    }
}

/// Explicit safety policy for forking a historical checkpoint.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum WorkflowForkPolicy {
    /// Fork only from a stable boundary that cannot repeat an ambiguous node.
    #[default]
    RejectAmbiguous,
    /// Re-run one serial node whose checkpoint was persisted as in-flight.
    RetryInterruptedStep,
}

/// Idempotent command that creates a new execution from immutable history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowForkCommand {
    /// Caller-stable identity of the new execution branch.
    pub fork_checkpoint_id: CheckpointId,
    /// Existing workflow whose history is being selected.
    pub source_checkpoint_id: CheckpointId,
    /// Exact immutable source revision.
    pub source_revision: u64,
    /// Explicit ambiguous-replay policy.
    pub policy: WorkflowForkPolicy,
}

impl WorkflowForkCommand {
    /// Creates a fork command with a generated target identity.
    pub fn new(
        source_checkpoint_id: CheckpointId,
        source_revision: u64,
        policy: WorkflowForkPolicy,
    ) -> Self {
        Self::with_id(
            CheckpointId::new(),
            source_checkpoint_id,
            source_revision,
            policy,
        )
    }

    /// Creates a retryable fork command with a caller-owned target identity.
    pub const fn with_id(
        fork_checkpoint_id: CheckpointId,
        source_checkpoint_id: CheckpointId,
        source_revision: u64,
        policy: WorkflowForkPolicy,
    ) -> Self {
        Self {
            fork_checkpoint_id,
            source_checkpoint_id,
            source_revision,
            policy,
        }
    }

    #[doc(hidden)]
    pub fn prepare_checkpoint(&self, source: Checkpoint) -> Result<Checkpoint, CheckpointError> {
        fork_checkpoint(source, self.fork_checkpoint_id, self.policy)
    }
}

/// Immutable parent relationship of one forked workflow execution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowLineage {
    /// Source workflow checkpoint.
    pub parent_checkpoint_id: CheckpointId,
    /// Exact parent revision selected for the fork.
    pub parent_revision: u64,
    /// Safety policy used to create the child.
    pub policy: WorkflowForkPolicy,
}

/// Result of an idempotent workflow fork command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowForkOutcome {
    /// A new workflow branch was atomically created.
    Created {
        /// New branch identity.
        checkpoint_id: CheckpointId,
    },
    /// The same target identity was already bound to the same source.
    Duplicate {
        /// Existing branch identity.
        checkpoint_id: CheckpointId,
    },
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
    /// The worker lease was released while this node awaits a durable wake.
    Waiting {
        /// Stable waiting node identity.
        step: StepId,
        /// Durable wake condition.
        wait: WorkflowWait,
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
    backend: WorkflowCheckpointBackend,
}

#[derive(Clone)]
enum WorkflowCheckpointBackend {
    Local(Arc<dyn CheckpointStore>),
    Distributed {
        store: Arc<dyn WorkflowStore>,
        lease: WorkflowLease,
    },
}

impl WorkflowCheckpoint {
    /// Creates a new workflow checkpoint handle.
    pub fn new(store: Arc<dyn CheckpointStore>) -> Self {
        Self {
            id: CheckpointId::new(),
            backend: WorkflowCheckpointBackend::Local(store),
        }
    }

    /// Reconnects to an existing workflow checkpoint.
    pub fn existing(id: CheckpointId, store: Arc<dyn CheckpointStore>) -> Self {
        Self {
            id,
            backend: WorkflowCheckpointBackend::Local(store),
        }
    }

    /// Binds a distributed checkpoint to the current fenced workflow lease.
    pub fn distributed(store: Arc<dyn WorkflowStore>, lease: WorkflowLease) -> Self {
        Self {
            id: lease.checkpoint_id,
            backend: WorkflowCheckpointBackend::Distributed { store, lease },
        }
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
        let WorkflowCheckpointBackend::Local(store) = &self.backend else {
            return Err(CheckpointError::new(
                CheckpointErrorKind::Storage,
                "distributed workflow checkpoints must be loaded asynchronously",
            ));
        };
        let checkpoint = store.load(self.id)?;
        decode(checkpoint)
    }

    /// Loads and validates either a local or distributed checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError`] for ownership, storage, or payload failures.
    pub async fn load_async(
        &self,
    ) -> Result<(Checkpoint, WorkflowCheckpointState), CheckpointError> {
        let checkpoint = match &self.backend {
            WorkflowCheckpointBackend::Local(store) => store.load(self.id)?,
            WorkflowCheckpointBackend::Distributed { store, lease } => {
                store.load_checkpoint(lease.clone()).await?
            }
        };
        decode(checkpoint)
    }

    async fn compare_and_swap(
        &self,
        checkpoint: &Checkpoint,
        expected_revision: Option<u64>,
    ) -> Result<(), CheckpointError> {
        match &self.backend {
            WorkflowCheckpointBackend::Local(store) => {
                store.compare_and_swap(checkpoint, expected_revision)
            }
            WorkflowCheckpointBackend::Distributed { store, lease } => {
                store
                    .compare_and_swap_checkpoint(
                        lease.clone(),
                        checkpoint.clone(),
                        expected_revision,
                    )
                    .await
            }
        }
    }
}

fn decode(
    checkpoint: Checkpoint,
) -> Result<(Checkpoint, WorkflowCheckpointState), CheckpointError> {
    if checkpoint.kind != CHECKPOINT_KIND
        || !(MIN_CHECKPOINT_SCHEMA_VERSION..=CHECKPOINT_SCHEMA_VERSION)
            .contains(&checkpoint.schema_version)
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

pub(crate) fn decode_revision(
    checkpoint: Checkpoint,
) -> Result<WorkflowCheckpointRevision, CheckpointError> {
    let (checkpoint, state) = decode(checkpoint)?;
    Ok(WorkflowCheckpointRevision {
        checkpoint_id: checkpoint.id,
        revision: checkpoint.revision,
        run_id: checkpoint.run_id,
        updated_at_ms: checkpoint.updated_at_ms,
        state,
    })
}

pub(crate) fn fork_checkpoint(
    source: Checkpoint,
    target: CheckpointId,
    policy: WorkflowForkPolicy,
) -> Result<Checkpoint, CheckpointError> {
    let (_, mut state) = decode(source)?;
    match &state.phase {
        WorkflowCheckpointPhase::StepInFlight { .. }
            if policy == WorkflowForkPolicy::RetryInterruptedStep =>
        {
            state.phase = WorkflowCheckpointPhase::Ready;
        }
        WorkflowCheckpointPhase::StepInFlight { .. }
        | WorkflowCheckpointPhase::ParallelInFlight { .. }
        | WorkflowCheckpointPhase::RaceInFlight { .. } => {
            return Err(CheckpointError::new(
                CheckpointErrorKind::Conflict,
                "workflow checkpoint is ambiguous and cannot be forked safely",
            ));
        }
        _ => {}
    }
    Ok(Checkpoint::initial(
        target,
        RunId::new(),
        CHECKPOINT_KIND,
        CHECKPOINT_SCHEMA_VERSION,
        serde_json::to_value(state).map_err(|error| {
            CheckpointError::new(CheckpointErrorKind::InvalidPayload, error.to_string())
        })?,
    ))
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
    pub(crate) async fn create(
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
        handle.compare_and_swap(&envelope, None).await?;
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

    pub(crate) async fn save(
        &mut self,
        state: &WorkflowCheckpointState,
    ) -> Result<(), WorkflowError> {
        let next = self.envelope.next(serialize(state)?)?;
        self.handle
            .compare_and_swap(&next, Some(self.envelope.revision))
            .await?;
        self.envelope = next;
        Ok(())
    }
}

fn serialize(state: &WorkflowCheckpointState) -> Result<Value, WorkflowError> {
    serde_json::to_value(state).map_err(|error| {
        CheckpointError::new(CheckpointErrorKind::InvalidPayload, error.to_string()).into()
    })
}

#[cfg(test)]
mod tests {
    use runifold_core::{CheckpointId, RunId, Usage};
    use serde_json::json;

    use super::*;

    fn in_flight_checkpoint() -> Checkpoint {
        let state = WorkflowCheckpointState {
            workflow: "fork-test".into(),
            workflow_version: 1,
            layout: vec![StepId::parse("charge").unwrap()],
            next_index: 0,
            value: json!({"amount": 42}),
            outputs: BTreeMap::new(),
            usage: Usage {
                tokens: 7,
                ..Usage::default()
            },
            phase: WorkflowCheckpointPhase::StepInFlight {
                step: StepId::parse("charge").unwrap(),
            },
        };
        Checkpoint::initial(
            CheckpointId::new(),
            RunId::new(),
            CHECKPOINT_KIND,
            CHECKPOINT_SCHEMA_VERSION,
            serde_json::to_value(state).unwrap(),
        )
    }

    #[test]
    fn fork_rejects_ambiguous_replay_unless_explicitly_authorized() {
        let source = in_flight_checkpoint();
        let error = fork_checkpoint(
            source.clone(),
            CheckpointId::new(),
            WorkflowForkPolicy::RejectAmbiguous,
        )
        .unwrap_err();
        assert_eq!(error.kind, CheckpointErrorKind::Conflict);

        let forked = fork_checkpoint(
            source,
            CheckpointId::new(),
            WorkflowForkPolicy::RetryInterruptedStep,
        )
        .unwrap();
        let revision = decode_revision(forked).unwrap();
        assert!(matches!(
            revision.state.phase,
            WorkflowCheckpointPhase::Ready
        ));
        assert_eq!(revision.state.usage.tokens, 7);
        assert_eq!(revision.state.next_index, 0);
    }
}
