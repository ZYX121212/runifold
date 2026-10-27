//! Persist context decisions before the canonical model-in-flight boundary.

use std::io::{BufWriter, Write};

use runifold_model::ModelRequest;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::checkpointing::{AgentProgress, save_checkpoint};
use super::context::{INPUT_MARKER, PLANS, strip_runtime_metadata};
use super::observability::record_domain;
use super::{
    Agent, AgentCheckpointPhase, AgentError, AgentObserver, AgentStreamEvent, CheckpointCursor,
    EventId, RunContext, emit_agent_event,
};
use crate::{ContextError, ContextManager, ContextPlan};

#[derive(Deserialize, Serialize)]
struct StoredPlan {
    digest: [u8; 32],
    plan: ContextPlan,
    // Old snapshots remain readable; validate their accounting before upgrading.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    plan_digest: Option<[u8; 32]>,
}

impl Agent {
    pub(super) async fn prepare_model_context(
        &self,
        progress: &mut AgentProgress,
        run: &RunContext,
        checkpoint: &mut Option<&mut CheckpointCursor>,
        caused_by: Option<EventId>,
        observer: &dyn AgentObserver,
    ) -> Result<ModelRequest, AgentError> {
        Self::check_lifecycle(run)?;
        let choice = self.next_tool_choice(progress, run)?;
        let mut request = self.request(&progress.transcript, choice)?;
        let Some(manager) = &self.context_manager else {
            return Ok(request);
        };
        let plan = match project_context(manager, progress, &mut request) {
            Ok(plan) => plan,
            Err(error) => {
                record_domain(
                    run,
                    "context.rejected",
                    serde_json::json!({"agent": self.name, "turn": progress.turns + 1, "error": error.to_string()}),
                    caused_by,
                )?;
                return Err(error.into());
            }
        };
        Self::check_lifecycle(run)?;
        save_checkpoint(
            checkpoint,
            &self.checkpoint_state(progress, run, AgentCheckpointPhase::ReadyForTurn),
        )?;
        record_domain(
            run,
            "context.prepared",
            serde_json::json!({"agent": self.name, "plan": plan}),
            caused_by,
        )?;
        emit_agent_event(observer, AgentStreamEvent::ContextPrepared { plan }).await;
        Ok(request)
    }
}

fn project_context(
    manager: &ContextManager,
    progress: &mut AgentProgress,
    request: &mut ModelRequest,
) -> Result<ContextPlan, ContextError> {
    let mut markers = progress
        .transcript
        .iter()
        .enumerate()
        .filter(|(_, message)| {
            message
                .metadata
                .get(INPUT_MARKER)
                .and_then(serde_json::Value::as_str)
                == Some(progress.execution_id.as_str())
        });
    let current = markers
        .next()
        .map(|(index, _)| index)
        .ok_or(ContextError::RecoveryMismatch)?;
    if markers.next().is_some() {
        return Err(ContextError::RecoveryMismatch);
    }
    for message in &mut request.messages {
        strip_runtime_metadata(message);
    }
    manager.reserve_output(request)?;
    let turn = progress
        .turns
        .checked_add(1)
        .ok_or(ContextError::Accounting)?;
    let key = turn.to_string();
    let existing = progress.transcript[current]
        .metadata
        .get(PLANS)
        .map(|plans| plans.as_object().ok_or(ContextError::RecoveryMismatch))
        .transpose()?
        .and_then(|plans| plans.get(&key));
    let stored = prepare_plan(manager, request, current, turn, existing)?;
    let mut selected = vec![false; request.messages.len()];
    for group in &stored.plan.groups {
        if group.selected {
            for &index in &group.message_indices {
                let slot = selected
                    .get_mut(index)
                    .ok_or(ContextError::RecoveryMismatch)?;
                if *slot {
                    return Err(ContextError::RecoveryMismatch);
                }
                *slot = true;
            }
        }
    }
    // Move selected messages; canonical history and private content are not cloned again.
    request.messages = std::mem::take(&mut request.messages)
        .into_iter()
        .enumerate()
        .filter_map(|(index, message)| selected[index].then_some(message))
        .collect();
    let value = serde_json::to_value(&stored).map_err(|_| ContextError::Accounting)?;
    let plans = progress.transcript[current]
        .metadata
        .entry(PLANS.into())
        .or_insert_with(|| serde_json::json!({}));
    plans
        .as_object_mut()
        .ok_or(ContextError::RecoveryMismatch)?
        .insert(key, value);
    Ok(stored.plan)
}

fn prepare_plan(
    manager: &ContextManager,
    request: &ModelRequest,
    current: usize,
    turn: u32,
    existing: Option<&serde_json::Value>,
) -> Result<StoredPlan, ContextError> {
    let digest = serialized_digest(request)?;
    let mut stored = if let Some(value) = existing {
        let stored: StoredPlan =
            serde_json::from_value(value.clone()).map_err(|_| ContextError::RecoveryMismatch)?;
        if stored.digest != digest || stored.plan.turn != turn {
            return Err(ContextError::RecoveryMismatch);
        }
        manager.validate_recovered_plan(request, current, &stored.plan)?;
        if let Some(expected) = stored.plan_digest {
            if serialized_digest(&stored.plan)? != expected {
                return Err(ContextError::RecoveryMismatch);
            }
        } else {
            manager.validate_legacy_accounting(request, &stored.plan)?;
        }
        stored
    } else {
        StoredPlan {
            digest,
            plan: manager.plan(request, current, turn)?,
            plan_digest: None,
        }
    };
    if stored.plan_digest.is_none() {
        stored.plan_digest = Some(serialized_digest(&stored.plan)?);
    }
    Ok(stored)
}

pub(super) fn serialized_digest(value: &impl Serialize) -> Result<[u8; 32], ContextError> {
    struct HashWriter(Sha256);
    impl Write for HashWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = BufWriter::new(HashWriter(Sha256::new()));
    serde_json::to_writer(&mut writer, value).map_err(|_| ContextError::Accounting)?;
    let writer = writer.into_inner().map_err(|_| ContextError::Accounting)?;
    Ok(writer.0.finalize().into())
}

/// Read only reports for the latest managed request in a canonical outcome.
pub(crate) fn outcome_plans(
    messages: &[runifold_model::Message],
) -> Result<Vec<ContextPlan>, ContextError> {
    let Some(plans) = messages
        .iter()
        .rev()
        .find_map(|message| message.metadata.get(PLANS))
    else {
        return Ok(Vec::new());
    };
    let object = plans.as_object().ok_or(ContextError::RecoveryMismatch)?;
    let mut reports = object
        .values()
        .map(|value| {
            let stored: StoredPlan = serde_json::from_value(value.clone())
                .map_err(|_| ContextError::RecoveryMismatch)?;
            if let Some(digest) = stored.plan_digest
                && serialized_digest(&stored.plan)? != digest
            {
                return Err(ContextError::RecoveryMismatch);
            }
            Ok(stored.plan)
        })
        .collect::<Result<Vec<_>, _>>()?;
    reports.sort_by_key(|plan| plan.turn);
    Ok(reports)
}
