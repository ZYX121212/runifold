//! Durable per-conversation admission and request identity.

use std::sync::Arc;

use runifold_core::{Checkpoint, CheckpointError, CheckpointErrorKind, CheckpointId, RunContext};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::stream::{AgentObserver, BufferedObserver, NoopObserver};
use crate::{
    Agent, AgentCheckpoint, AgentCheckpointPhase, AgentConversationError, AgentConversationOutcome,
    AgentEventStream, AgentFuture, AgentStreamEvent, ConversationContextPolicy, ConversationId,
    DurableConversationRequest, DurableConversationStore, MemoryNamespace, ResumePolicy,
};

mod summary;
use summary::{SummaryConfig, SummaryProgress};

const KIND: &str = "runifold.conversation.admission";

/// A durable conversation with exclusive admission before any model or tool work.
///
/// All writers must use this boundary. Low-level conversation APIs intentionally
/// remain available and do not honor this admission record. The conversation's
/// UUID is reserved as its admission checkpoint ID; request IDs must differ.
#[derive(Clone)]
pub struct AgentSession {
    agent: Agent,
    store: Arc<dyn DurableConversationStore>,
    conversation_id: ConversationId,
    namespace: MemoryNamespace,
    policy: ConversationContextPolicy,
    summary: Option<SummaryConfig>,
}

/// Admission and execution errors from a durable session.
#[derive(Debug, Error)]
pub enum AgentSessionError {
    /// Another request owns the conversation, possibly after a crash.
    #[error("conversation is occupied by request {request_id} at revision {revision}")]
    Busy {
        /// Request requiring completion or explicit recovery.
        request_id: CheckpointId,
        /// Exact revision required for an explicit recovery attempt.
        revision: u64,
    },
    /// The request identity is already bound to another input or conversation.
    #[error("request identity does not match its original input or conversation")]
    RequestMismatch,
    /// Persisted admission data cannot be interpreted safely.
    #[error("invalid session admission record")]
    InvalidAdmission,
    /// Session context or summary configuration differs from the admitted request.
    #[error("session configuration does not match the admitted request")]
    ConfigurationMismatch,
    /// Restored budget is inconsistent with the latest stable session snapshot.
    #[error("run usage does not match the session checkpoint")]
    UsageMismatch,
    /// The underlying checkpoint store rejected the operation.
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    /// The admitted execution failed. Admission remains held for recovery.
    #[error(transparent)]
    Conversation(Box<AgentConversationError>),
}

impl From<AgentConversationError> for AgentSessionError {
    fn from(error: AgentConversationError) -> Self {
        Self::Conversation(Box::new(error))
    }
}

#[derive(Deserialize, Serialize)]
struct Admission {
    namespace: MemoryNamespace,
    active: Option<AdmittedRequest>,
}

#[derive(Deserialize, Serialize)]
struct AdmittedRequest {
    id: CheckpointId,
    input_digest: [u8; 32],
    agent: summary::AgentDefinition,
    context: (u16, u16, Option<u16>),
    summary: Option<SummaryProgress>,
    usage: runifold_core::Usage,
}

impl AgentSession {
    /// Binds an Agent and durable store to one conversation and context policy.
    pub fn new(
        agent: Agent,
        store: Arc<dyn DurableConversationStore>,
        conversation_id: ConversationId,
        namespace: MemoryNamespace,
        policy: ConversationContextPolicy,
    ) -> Self {
        Self {
            agent,
            store,
            conversation_id,
            namespace,
            policy,
            summary: None,
        }
    }

    /// Compacts older transcript batches with a checkpointed Agent before a turn.
    /// Summary work shares the caller's capabilities, budget, and deadline.
    /// The pass limit applies across retries of the same request.
    #[must_use]
    pub fn with_summary_agent(
        mut self,
        agent: Agent,
        max_passes: crate::ConversationSummaryPassLimit,
    ) -> Self {
        self.summary = Some(SummaryConfig { agent, max_passes });
        self
    }

    /// Returns the latest persisted usage for an occupied request.
    /// In-flight remote work can consume more than this snapshot; reconcile that
    /// uncertainty before explicitly authorizing an interrupted-turn retry.
    ///
    /// # Errors
    /// Returns an error for an unoccupied or mismatched request, invalid admission
    /// data, or an unreadable checkpoint.
    pub fn recovery_usage(
        &self,
        request_id: CheckpointId,
    ) -> Result<runifold_core::Usage, AgentSessionError> {
        let admission =
            self.read_admission(&self.store.load(self.conversation_id.as_checkpoint_id())?)?;
        let active = admission
            .active
            .ok_or(AgentSessionError::InvalidAdmission)?;
        if active.id != request_id {
            return Err(AgentSessionError::RequestMismatch);
        }
        for id in std::iter::once(request_id).chain(
            active
                .summary
                .as_ref()
                .and_then(|state| state.pending.as_ref())
                .map(|pass| pass.checkpoint_id),
        ) {
            let store: Arc<dyn runifold_core::CheckpointStore> = self.store.clone();
            match AgentCheckpoint::existing(id, store).load() {
                Ok((_, state)) => return Ok(summary::usage_floor(active.usage, state.usage)),
                Err(error) if error.kind == CheckpointErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(active.usage)
    }

    /// Runs one request. Reuse the same ID and exact input for transport retries.
    /// Completed requests replay their stored result; active ones return `Busy`.
    /// Failure or cancellation retains admission until explicit recovery.
    pub fn run<'a>(
        &'a self,
        request_id: CheckpointId,
        input: impl Into<String> + Send + 'a,
        run: &'a RunContext,
    ) -> AgentFuture<'a, Result<AgentConversationOutcome, AgentSessionError>> {
        self.execute(request_id, input.into(), run, Arc::new(NoopObserver), None)
    }

    /// Streams an admitted request. Dropping it retains admission for recovery.
    /// `ConversationCommitted` is emitted after commit and admission release.
    pub fn stream<'a>(
        &'a self,
        request_id: CheckpointId,
        input: impl Into<String> + Send + 'a,
        run: &'a RunContext,
    ) -> AgentEventStream<'a, AgentSessionError> {
        let observer = Arc::new(BufferedObserver::durable());
        let events = observer.events();
        let execution = self.execute(request_id, input.into(), run, observer.clone(), None);
        AgentEventStream::new(
            Box::pin(async move {
                let result = execution.await?;
                observer.emit(AgentStreamEvent::ConversationCommitted {
                    outcome: result.outcome.clone(),
                    conversation_version: result.conversation_version,
                });
                Ok(result.outcome)
            }),
            events,
        )
    }

    /// Recovers an occupied request after the host has stopped its prior owner.
    ///
    /// The host MUST ensure the previous execution can no longer run. Revision
    /// CAS prevents competing recovery claims; it cannot fence external services.
    /// Restore the run's budget using [`Self::recovery_usage`] before calling this.
    /// A request interrupted before checkpoint creation starts from its bound input.
    pub fn recover_after_owner_exit<'a>(
        &'a self,
        request_id: CheckpointId,
        input: impl Into<String> + Send + 'a,
        run: &'a RunContext,
        expected_revision: u64,
        policy: ResumePolicy,
    ) -> AgentFuture<'a, Result<AgentConversationOutcome, AgentSessionError>> {
        self.execute(
            request_id,
            input.into(),
            run,
            Arc::new(NoopObserver),
            Some((expected_revision, policy)),
        )
    }

    fn execute<'a>(
        &'a self,
        request_id: CheckpointId,
        input: String,
        run: &'a RunContext,
        observer: Arc<dyn AgentObserver>,
        recovery: Option<(u64, ResumePolicy)>,
    ) -> AgentFuture<'a, Result<AgentConversationOutcome, AgentSessionError>> {
        Box::pin(async move {
            if request_id == self.conversation_id.as_checkpoint_id() {
                return Err(AgentSessionError::RequestMismatch);
            }
            let existing = self.existing_request(request_id, &input)?;
            if existing == Some(true) && recovery.is_none() {
                return Ok(self
                    .agent
                    .resume_durable_conversation(
                        self.store.clone(),
                        request_id,
                        run,
                        ResumePolicy::RejectAmbiguous,
                    )
                    .await?);
            }
            let mut admission = self.claim(
                request_id,
                &input,
                run,
                recovery.map(|(revision, _)| revision),
            )?;
            // Re-read after admission: another process may have committed between
            // our initial lookup and claiming the idle record.
            let existing = self.existing_request(request_id, &input)?;
            let result = if existing.is_some() {
                self.agent
                    .resume_durable_conversation_observed(
                        self.store.clone(),
                        request_id,
                        run,
                        recovery.map_or(ResumePolicy::RejectAmbiguous, |(_, policy)| policy),
                        observer,
                    )
                    .await?
            } else {
                self.compact_summary(
                    &mut admission,
                    run,
                    observer.as_ref(),
                    recovery.map_or(ResumePolicy::RejectAmbiguous, |(_, policy)| policy),
                )
                .await?;
                self.agent
                    .run_durable_conversation_observed(
                        input,
                        run,
                        self.store.clone(),
                        DurableConversationRequest {
                            checkpoint_id: request_id,
                            conversation_id: self.conversation_id,
                            namespace: self.namespace.clone(),
                            policy: self.policy,
                        },
                        observer,
                    )
                    .await?
            };
            let idle = admission.next(self.payload(None)?)?;
            self.store
                .compare_and_swap(&idle, Some(admission.revision))?;
            Ok(result)
        })
    }

    fn existing_request(
        &self,
        id: CheckpointId,
        input: &str,
    ) -> Result<Option<bool>, AgentSessionError> {
        let store: Arc<dyn runifold_core::CheckpointStore> = self.store.clone();
        let checkpoint = AgentCheckpoint::existing(id, store);
        let (_, state) = match checkpoint.load() {
            Ok(value) => value,
            Err(error) if error.kind == CheckpointErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let durable = state
            .durable_conversation
            .as_ref()
            .ok_or(AgentSessionError::RequestMismatch)?;
        let index = usize::try_from(durable.persisted_prefix_len)
            .map_err(|_| AgentSessionError::RequestMismatch)?;
        // Context retrieval can insert transient messages before the original user
        // message. Locate the first non-transient message in the appended suffix.
        let message = state
            .transcript
            .iter()
            .skip(index)
            .find(|message| !super::is_transient_context(message));
        if durable.conversation_id != self.conversation_id
            || durable.namespace != self.namespace
            || message != Some(&runifold_model::Message::user(input))
        {
            return Err(AgentSessionError::RequestMismatch);
        }
        Ok(Some(matches!(
            state.phase,
            AgentCheckpointPhase::Completed { .. }
        )))
    }

    fn claim(
        &self,
        id: CheckpointId,
        input: &str,
        run: &RunContext,
        recovery: Option<u64>,
    ) -> Result<Checkpoint, AgentSessionError> {
        let gate_id = self.conversation_id.as_checkpoint_id();
        let digest: [u8; 32] = Sha256::digest(input.as_bytes()).into();
        let payload = self.payload(Some(AdmittedRequest {
            id,
            input_digest: digest,
            agent: summary::AgentDefinition::new(&self.agent),
            context: self.context_contract(),
            summary: self.summary.as_ref().map(SummaryProgress::new),
            usage: run.budget().usage(),
        }))?;
        let current = match self.store.load(gate_id) {
            Ok(current) => current,
            Err(error) if error.kind == CheckpointErrorKind::NotFound && recovery.is_none() => {
                let first = Checkpoint::initial(gate_id, run.run_id(), KIND, 3, payload);
                self.store.compare_and_swap(&first, None)?;
                return Ok(first);
            }
            Err(error) => return Err(error.into()),
        };
        let state = self.read_admission(&current)?;
        let mut payload = payload;
        if let Some(active) = state.active {
            if active.id == id && active.input_digest != digest {
                return Err(AgentSessionError::RequestMismatch);
            }
            if recovery != Some(current.revision) || active.id != id {
                return Err(AgentSessionError::Busy {
                    request_id: active.id,
                    revision: current.revision,
                });
            }
            if active.agent != summary::AgentDefinition::new(&self.agent)
                || active.context != self.context_contract()
                || active.summary.as_ref().map(|state| &state.contract)
                    != self.summary.as_ref().map(SummaryConfig::contract).as_ref()
            {
                return Err(AgentSessionError::ConfigurationMismatch);
            }
            // A recovery claim preserves summary checkpoint identity and usage.
            payload = self.payload(Some(active))?;
        } else if recovery.is_some() {
            return Err(AgentSessionError::InvalidAdmission);
        }
        let next = current.next(payload)?;
        self.store.compare_and_swap(&next, Some(current.revision))?;
        Ok(next)
    }

    fn context_contract(&self) -> (u16, u16, Option<u16>) {
        (
            self.policy.window.get(),
            self.policy.summary_batch.get(),
            self.policy
                .semantic_memory_limit
                .map(std::num::NonZeroU16::get),
        )
    }

    fn read_admission(&self, checkpoint: &Checkpoint) -> Result<Admission, AgentSessionError> {
        if checkpoint.kind != KIND || checkpoint.schema_version != 3 {
            return Err(AgentSessionError::InvalidAdmission);
        }
        let state: Admission = serde_json::from_value(checkpoint.payload.clone())
            .map_err(|_| AgentSessionError::InvalidAdmission)?;
        if state.namespace != self.namespace {
            return Err(AgentSessionError::RequestMismatch);
        }
        Ok(state)
    }

    fn save_admission(
        &self,
        checkpoint: &mut Checkpoint,
        active: AdmittedRequest,
    ) -> Result<(), AgentSessionError> {
        let next = checkpoint.next(self.payload(Some(active))?)?;
        self.store
            .compare_and_swap(&next, Some(checkpoint.revision))?;
        *checkpoint = next;
        Ok(())
    }

    fn payload(
        &self,
        active: Option<AdmittedRequest>,
    ) -> Result<serde_json::Value, AgentSessionError> {
        serde_json::to_value(Admission {
            namespace: self.namespace.clone(),
            active,
        })
        .map_err(|_| AgentSessionError::InvalidAdmission)
    }
}

impl std::fmt::Debug for AgentSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentSession")
            .field("conversation_id", &self.conversation_id)
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}
