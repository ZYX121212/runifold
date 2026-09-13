//! Checkpointed summary batches within an admitted conversation request.

use super::{AgentSession, AgentSessionError};
use crate::conversation::summarizer::{summary_prompt, validate_summary_output};
use crate::stream::{AgentObserver, emit_agent_event};
use crate::{
    Agent, AgentCheckpoint, AgentConversationError, AgentRecoveryContract, AgentStreamEvent,
    ConversationSequence, ConversationSummaryCommit, ConversationSummaryPassLimit,
    ConversationSummaryRequest, ConversationVersion, ResumePolicy,
};
use runifold_core::{Checkpoint, CheckpointErrorKind, CheckpointId, RunContext};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone)]
pub(super) struct SummaryConfig {
    pub(super) agent: Agent,
    pub(super) max_passes: ConversationSummaryPassLimit,
}

#[derive(Deserialize, PartialEq, Serialize)]
pub(super) struct SummaryContract {
    agent: AgentDefinition,
    max_passes: u16,
}

#[derive(Deserialize, PartialEq, Serialize)]
pub(super) struct AgentDefinition {
    name: String,
    model: runifold_model::ModelRef,
    definition: AgentRecoveryContract,
    turn_review: Option<ReviewBinding<crate::TurnReviewPolicy>>,
    terminal_review: Option<ReviewBinding<crate::TerminalReviewPolicy>>,
}

#[derive(Deserialize, PartialEq, Serialize)]
struct ReviewBinding<P> {
    descriptor: crate::TerminalReviewerDescriptor,
    policy: P,
    capabilities: Vec<runifold_core::CapabilityId>,
}

impl AgentDefinition {
    pub(super) fn new(agent: &Agent) -> Self {
        Self {
            name: agent.name().to_owned(),
            model: agent.model_ref().clone(),
            definition: agent.recovery_contract(),
            turn_review: agent.turn_review.as_ref().map(|review| ReviewBinding {
                descriptor: review.descriptor.clone(),
                policy: review.policy,
                capabilities: review
                    .capabilities
                    .iter()
                    .map(|capability| capability.id)
                    .collect(),
            }),
            terminal_review: agent.terminal_review.as_ref().map(|review| ReviewBinding {
                descriptor: review.descriptor.clone(),
                policy: review.policy,
                capabilities: review
                    .capabilities
                    .iter()
                    .map(|capability| capability.id)
                    .collect(),
            }),
        }
    }
}

impl SummaryConfig {
    pub(super) fn contract(&self) -> SummaryContract {
        SummaryContract {
            agent: AgentDefinition::new(&self.agent),
            max_passes: self.max_passes.get(),
        }
    }
}

#[derive(Deserialize, Serialize)]
pub(super) struct SummaryProgress {
    pub(super) contract: SummaryContract,
    completed: u16,
    pub(super) pending: Option<SummaryPass>,
}

impl SummaryProgress {
    pub(super) fn new(config: &SummaryConfig) -> Self {
        Self {
            contract: config.contract(),
            completed: 0,
            pending: None,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub(super) struct SummaryPass {
    pub(super) checkpoint_id: CheckpointId,
    transcript_version: ConversationVersion,
    through_sequence: ConversationSequence,
    prompt: String,
}

impl AgentSession {
    pub(super) async fn compact_summary(
        &self,
        admission: &mut Checkpoint,
        run: &RunContext,
        observer: &dyn AgentObserver,
        resume: ResumePolicy,
    ) -> Result<(), AgentSessionError> {
        let mut active = self
            .read_admission(admission)?
            .active
            .ok_or(AgentSessionError::InvalidAdmission)?;
        let Some(config) = &self.summary else {
            return Ok(());
        };
        loop {
            let progress = active
                .summary
                .as_mut()
                .ok_or(AgentSessionError::ConfigurationMismatch)?;
            if progress.pending.is_none() {
                if active.usage != run.budget().usage() {
                    return Err(AgentSessionError::UsageMismatch);
                }
                let Some(pass) = self
                    .next_summary_pass(progress.completed, config.max_passes)
                    .await?
                else {
                    return Ok(());
                };
                progress.pending = Some(pass);
                self.save_admission(admission, active)?;
                active = self
                    .read_admission(admission)?
                    .active
                    .ok_or(AgentSessionError::InvalidAdmission)?;
            }
            let progress = active
                .summary
                .as_mut()
                .ok_or(AgentSessionError::InvalidAdmission)?;
            let pass = progress
                .pending
                .as_ref()
                .ok_or(AgentSessionError::InvalidAdmission)?;
            let checkpoint_store: Arc<dyn runifold_core::CheckpointStore> = self.store.clone();
            let checkpoint = AgentCheckpoint::existing(pass.checkpoint_id, checkpoint_store);
            let exists = match checkpoint.load() {
                Ok(_) => true,
                Err(error) if error.kind == CheckpointErrorKind::NotFound => false,
                Err(error) => return Err(error.into()),
            };
            if usage_floor(active.usage, run.budget().usage()) != run.budget().usage()
                || (!exists && active.usage != run.budget().usage())
            {
                return Err(AgentSessionError::UsageMismatch);
            }
            emit_agent_event(
                observer,
                AgentStreamEvent::ConversationSummaryStarted {
                    checkpoint_id: pass.checkpoint_id,
                    through_sequence: pass.through_sequence,
                },
            )
            .await;
            let usage_before = run.budget().usage();
            let outcome = if exists {
                config.agent.resume(&checkpoint, run, resume).await
            } else {
                config
                    .agent
                    .run_checkpointed(pass.prompt.clone(), run, &checkpoint)
                    .await
            };
            let outcome = match outcome {
                Ok(outcome) => outcome,
                Err(error) => {
                    // The Agent's write-ahead snapshot may precede billed work.
                    // Retain every resource amount known when execution returns.
                    if run.budget().usage() != usage_before {
                        active.usage = run.budget().usage();
                        self.save_admission(admission, active)?;
                    }
                    return Err(AgentConversationError::Run(error).into());
                }
            };
            let content =
                validate_summary_output(&outcome.text()).map_err(AgentConversationError::from)?;
            self.commit_summary_pass(pass, content).await?;
            let through_sequence = pass.through_sequence;
            progress.completed = progress.completed.saturating_add(1);
            progress.pending = None;
            active.usage = run.budget().usage();
            self.save_admission(admission, active)?;
            emit_agent_event(
                observer,
                AgentStreamEvent::ConversationSummaryCommitted {
                    through_sequence,
                    usage: run.budget().usage(),
                },
            )
            .await;
            active = self
                .read_admission(admission)?
                .active
                .ok_or(AgentSessionError::InvalidAdmission)?;
        }
    }
    async fn next_summary_pass(
        &self,
        completed: u16,
        max_passes: ConversationSummaryPassLimit,
    ) -> Result<Option<SummaryPass>, AgentSessionError> {
        self.store
            .create(self.conversation_id, self.namespace.clone())
            .await
            .map_err(AgentConversationError::from)?;
        let view = self
            .store
            .load_view(
                self.conversation_id,
                self.namespace.clone(),
                self.policy.window,
                self.policy.summary_batch,
            )
            .await
            .map_err(AgentConversationError::from)?;
        let Some(last) = view.summary_buffer.last() else {
            return Ok(None);
        };
        if completed >= max_passes.get() {
            return Err(AgentConversationError::SummaryPassLimitExceeded {
                conversation_id: self.conversation_id,
                remaining_entries: u64::try_from(view.summary_buffer.len())
                    .unwrap_or(u64::MAX)
                    .saturating_add(view.summary_backlog),
            }
            .into());
        }
        let through_sequence = last.sequence;
        let transcript_version = view.version;
        let prompt = summary_prompt(&ConversationSummaryRequest {
            transcript_version,
            previous_summary: view.summary,
            entries: view.summary_buffer,
        })
        .map_err(AgentConversationError::from)?;
        Ok(Some(SummaryPass {
            checkpoint_id: CheckpointId::new(),
            transcript_version,
            through_sequence,
            prompt,
        }))
    }

    async fn commit_summary_pass(
        &self,
        pass: &SummaryPass,
        content: String,
    ) -> Result<(), AgentSessionError> {
        let view = self
            .store
            .load_view(
                self.conversation_id,
                self.namespace.clone(),
                self.policy.window,
                self.policy.summary_batch,
            )
            .await
            .map_err(AgentConversationError::from)?;
        // A commit may have succeeded immediately before the worker died.
        // Recognize exactly that commit without regenerating or overwriting it.
        let already_committed = view.summary.as_ref().is_some_and(|summary| {
            summary.through_sequence == pass.through_sequence
                && summary.transcript_version == pass.transcript_version
                && summary.content == content
        });
        if !already_committed {
            self.store
                .commit_summary(
                    self.namespace.clone(),
                    ConversationSummaryCommit {
                        conversation_id: self.conversation_id,
                        expected_version: pass.transcript_version,
                        through_sequence: pass.through_sequence,
                        content,
                    },
                )
                .await
                .map_err(AgentConversationError::from)?;
        }
        Ok(())
    }
}

// Both snapshots are cumulative usage for the same run, so use component maxima,
// never a sum (which would double-charge work already present in both).
pub(super) fn usage_floor(
    left: runifold_core::Usage,
    right: runifold_core::Usage,
) -> runifold_core::Usage {
    runifold_core::Usage {
        tokens: left.tokens.max(right.tokens),
        cost_microusd: left.cost_microusd.max(right.cost_microusd),
        duration_micros: left.duration_micros.max(right.duration_micros),
        turns: left.turns.max(right.turns),
        tool_calls: left.tool_calls.max(right.tool_calls),
        delegations: left.delegations.max(right.delegations),
    }
}
