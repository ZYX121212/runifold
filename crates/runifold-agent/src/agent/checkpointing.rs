//! Agent checkpoint state conversion, persistence, and usage validation.

use super::{
    AgentCheckpointState, AgentError, AgentOutcome, CheckpointCursor,
    DurableConversationCheckpoint, Message, ModelResponse, Usage,
};

pub(super) struct AgentProgress {
    pub(super) execution_id: String,
    pub(super) transcript: Vec<Message>,
    pub(super) turns: u32,
    pub(super) tool_calls: u32,
    pub(super) delegations: u32,
    pub(super) terminal_repairs: u32,
    pub(super) turn_review_repairs: u32,
    pub(super) terminal_review_repairs: u32,
    pub(super) durable_conversation: Option<DurableConversationCheckpoint>,
}

impl From<AgentCheckpointState> for AgentProgress {
    fn from(state: AgentCheckpointState) -> Self {
        let terminal_repairs = terminal_repair_count(&state.transcript);
        let turn_review_repairs = turn_review_repair_count(&state.transcript);
        let terminal_review_repairs = terminal_review_repair_count(&state.transcript);
        Self {
            execution_id: state.execution_id,
            transcript: state.transcript,
            turns: state.turns,
            tool_calls: state.tool_calls,
            delegations: state.delegations,
            terminal_repairs,
            turn_review_repairs,
            terminal_review_repairs,
            durable_conversation: state.durable_conversation,
        }
    }
}

impl AgentProgress {
    pub(super) fn clone_outcome(&self, response: ModelResponse, usage: Usage) -> AgentOutcome {
        AgentOutcome {
            response,
            transcript: self.transcript.clone(),
            turns: self.turns,
            tool_calls: self.tool_calls,
            delegations: self.delegations,
            usage,
        }
    }
}

fn terminal_repair_count(transcript: &[Message]) -> u32 {
    transcript
        .iter()
        .filter(|message| {
            message.metadata.get("runifold.terminal_repair") == Some(&serde_json::Value::Bool(true))
        })
        .count()
        .try_into()
        .unwrap_or(u32::MAX)
}

fn terminal_review_repair_count(transcript: &[Message]) -> u32 {
    transcript
        .iter()
        .filter(|message| {
            message.metadata.get("runifold.terminal_review_repair")
                == Some(&serde_json::Value::Bool(true))
        })
        .count()
        .try_into()
        .unwrap_or(u32::MAX)
}

fn turn_review_repair_count(transcript: &[Message]) -> u32 {
    transcript
        .iter()
        .filter(|message| {
            message.metadata.get("runifold.turn_review_repair")
                == Some(&serde_json::Value::Bool(true))
        })
        .count()
        .try_into()
        .unwrap_or(u32::MAX)
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
