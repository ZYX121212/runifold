//! Agent checkpoint state conversion, persistence, and usage validation.

use super::{
    AgentCheckpointState, AgentError, AgentOutcome, CheckpointCursor, Message, ModelResponse, Usage,
};

pub(super) struct AgentProgress {
    pub(super) execution_id: String,
    pub(super) transcript: Vec<Message>,
    pub(super) turns: u32,
    pub(super) tool_calls: u32,
    pub(super) delegations: u32,
}

impl From<AgentCheckpointState> for AgentProgress {
    fn from(state: AgentCheckpointState) -> Self {
        Self {
            execution_id: state.execution_id,
            transcript: state.transcript,
            turns: state.turns,
            tool_calls: state.tool_calls,
            delegations: state.delegations,
        }
    }
}

impl AgentProgress {
    pub(super) fn outcome(self, response: ModelResponse, usage: Usage) -> AgentOutcome {
        AgentOutcome {
            response,
            transcript: self.transcript,
            turns: self.turns,
            tool_calls: self.tool_calls,
            delegations: self.delegations,
            usage,
        }
    }
}

pub(super) fn save_checkpoint(
    checkpoint: &mut Option<&mut CheckpointCursor>,
    state: &AgentCheckpointState,
) -> Result<(), AgentError> {
    if let Some(checkpoint) = checkpoint.as_deref_mut() {
        checkpoint.save(state)?;
    }
    Ok(())
}

pub(super) fn validate_exact_usage(expected: Usage, actual: Usage) -> Result<(), AgentError> {
    if expected != actual {
        return Err(checkpoint_payload_error(
            "Run budget usage does not match the checkpoint snapshot",
        ));
    }
    Ok(())
}

pub(super) fn validate_usage_floor(floor: Usage, actual: Usage) -> Result<(), AgentError> {
    let covers = actual.tokens >= floor.tokens
        && actual.cost_microusd >= floor.cost_microusd
        && actual.duration_micros >= floor.duration_micros
        && actual.turns >= floor.turns
        && actual.tool_calls >= floor.tool_calls
        && actual.delegations >= floor.delegations;
    if !covers {
        return Err(checkpoint_payload_error(
            "Run budget usage is below the in-flight checkpoint snapshot",
        ));
    }
    Ok(())
}

fn checkpoint_payload_error(message: &str) -> AgentError {
    runifold_core::CheckpointError::new(runifold_core::CheckpointErrorKind::InvalidPayload, message)
        .into()
}
