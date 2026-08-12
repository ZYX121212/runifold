use std::{fmt, sync::Arc};

use runifold_core::{
    Checkpoint, CheckpointError, CheckpointErrorKind, CheckpointId, CheckpointStore, RunContext,
    Usage,
};
use runifold_model::{Message, ModelRef, ModelResponse};
use serde::{Deserialize, Serialize};

use crate::conversation::{ConversationId, ConversationVersion, MemoryNamespace};
use crate::{AgentError, AgentOutcome, TerminalRequirementFailure};

const CHECKPOINT_KIND: &str = "runifold.agent";
const CHECKPOINT_SCHEMA_VERSION: u32 = 1;

/// Recovery behavior for a checkpoint captured during an external turn.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ResumePolicy {
    /// Reject recovery that could duplicate model cost or external effects.
    #[default]
    RejectAmbiguous,
    /// Explicitly retry the entire interrupted model-and-callable turn.
    RetryInterruptedTurn,
}

/// Persisted Agent execution phase.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentCheckpointPhase {
    /// Transcript is stable and ready for the next model turn.
    ReadyForTurn,
    /// A model-and-callable turn may have partially executed.
    TurnInFlight {
        /// One-based turn number that may have partially executed.
        turn: u32,
    },
    /// The Agent reached a terminal response.
    Completed {
        /// Final canonical model response.
        response: Box<ModelResponse>,
    },
    /// A terminal candidate failed its completion contract and cannot be
    /// repaired under the configured policy.
    TerminalRequirementFailed {
        /// Safe failure details retained without the generated body.
        failure: TerminalRequirementFailure,
        /// Repair turns completed before exhaustion.
        attempts: u32,
    },
}

/// Conversation commit preconditions carried through crash recovery.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DurableConversationCheckpoint {
    /// Conversation receiving the completed turn.
    pub conversation_id: ConversationId,
    /// Isolation namespace loaded before execution.
    pub namespace: MemoryNamespace,
    /// Transcript version loaded before execution.
    pub expected_version: ConversationVersion,
    /// Number of leading runtime-only messages excluded from persistence.
    pub persisted_prefix_len: u64,
}

/// Versioned Agent state stored in a domain-neutral checkpoint envelope.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AgentCheckpointState {
    /// Stable logical execution identity used for callable idempotency.
    pub execution_id: String,
    /// Agent identity expected during recovery.
    pub agent: String,
    /// Model identity expected during recovery.
    pub model: ModelRef,
    /// Canonical transcript at the last stable boundary.
    pub transcript: Vec<Message>,
    /// Completed model turns.
    pub turns: u32,
    /// Completed local tool attempts.
    pub tool_calls: u32,
    /// Completed successful delegations.
    pub delegations: u32,
    /// Shared usage snapshot at persistence time.
    pub usage: Usage,
    /// Current recovery phase.
    pub phase: AgentCheckpointPhase,
    /// Atomic conversation commit metadata, when this is a durable turn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durable_conversation: Option<DurableConversationCheckpoint>,
}

impl AgentCheckpointState {
    pub(crate) fn outcome(&self) -> Option<AgentOutcome> {
        match &self.phase {
            AgentCheckpointPhase::Completed { response } => Some(AgentOutcome {
                response: response.as_ref().clone(),
                transcript: self.transcript.clone(),
                turns: self.turns,
                tool_calls: self.tool_calls,
                delegations: self.delegations,
                usage: self.usage,
            }),
            _ => None,
        }
    }

    pub(crate) fn terminal_failure(&self) -> Option<AgentError> {
        match &self.phase {
            AgentCheckpointPhase::TerminalRequirementFailed {
                failure, attempts, ..
            } => Some(super::agent::completion::failure_error(failure, *attempts)),
            _ => None,
        }
    }
}

/// Stable handle binding one checkpoint identity to a store.
#[derive(Clone)]
pub struct AgentCheckpoint {
    id: CheckpointId,
    store: Arc<dyn CheckpointStore>,
}

impl AgentCheckpoint {
    /// Creates a new checkpoint handle with a unique identity.
    pub fn new(store: Arc<dyn CheckpointStore>) -> Self {
        Self {
            id: CheckpointId::new(),
            store,
        }
    }

    /// Reconnects to an existing checkpoint identity.
    pub fn existing(id: CheckpointId, store: Arc<dyn CheckpointStore>) -> Self {
        Self { id, store }
    }

    /// Returns the stable checkpoint identity.
    pub const fn id(&self) -> CheckpointId {
        self.id
    }

    /// Loads and validates the latest typed Agent state.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError`] when storage or payload validation fails.
    pub fn load(&self) -> Result<(Checkpoint, AgentCheckpointState), CheckpointError> {
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

impl fmt::Debug for AgentCheckpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentCheckpoint")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

pub(crate) struct CheckpointCursor {
    handle: AgentCheckpoint,
    envelope: Checkpoint,
}

impl CheckpointCursor {
    pub(crate) fn create(
        handle: &AgentCheckpoint,
        run: &RunContext,
        state: &AgentCheckpointState,
    ) -> Result<Self, AgentError> {
        let payload = serialize(state)?;
        let envelope = Checkpoint::initial(
            handle.id,
            run.run_id(),
            CHECKPOINT_KIND,
            CHECKPOINT_SCHEMA_VERSION,
            payload,
        );
        handle.store.compare_and_swap(&envelope, None)?;
        Ok(Self {
            handle: handle.clone(),
            envelope,
        })
    }

    pub(crate) fn loaded(handle: &AgentCheckpoint, envelope: Checkpoint) -> Self {
        Self {
            handle: handle.clone(),
            envelope,
        }
    }

    pub(crate) fn save(&mut self, state: &AgentCheckpointState) -> Result<(), AgentError> {
        let next = self.envelope.next(serialize(state)?)?;
        self.handle
            .store
            .compare_and_swap(&next, Some(self.envelope.revision))?;
        self.envelope = next;
        Ok(())
    }

    pub(crate) fn next(&self, state: &AgentCheckpointState) -> Result<Checkpoint, AgentError> {
        self.envelope.next(serialize(state)?).map_err(Into::into)
    }

    pub(crate) const fn revision(&self) -> u64 {
        self.envelope.revision
    }

    pub(crate) const fn id(&self) -> CheckpointId {
        self.envelope.id
    }
}

fn serialize(state: &AgentCheckpointState) -> Result<serde_json::Value, AgentError> {
    serde_json::to_value(state).map_err(|error| {
        CheckpointError::new(CheckpointErrorKind::InvalidPayload, error.to_string()).into()
    })
}
