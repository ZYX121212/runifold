//! Canonical Agent execution engine and its private runtime helpers.

use super::checkpointing::{
    AgentProgress, save_checkpoint, validate_exact_usage, validate_usage_floor,
};
use super::completion::TerminalCompletionContext;
use super::observability::{consume_budget, emit_usage, record_domain, terminal_event};
use super::{
    Agent, AgentCheckpoint, AgentCheckpointPhase, AgentCheckpointState, AgentError,
    AgentEventStream, AgentFuture, AgentObserver, AgentOutcome, AgentStreamEvent, Arc,
    BufferedObserver, CheckpointCursor, ContentPart, DurableConversationCheckpoint, Either,
    EventId, Instant, InvocationId, LifecycleEvent, Message, ModelCallContext, ModelError,
    ModelErrorKind, ModelRequest, ModelResponse, ModelStreamAccumulator, NoopObserver,
    ResumePolicy, Role, RunContext, RunEventKind, StreamExt, TOOL_RESULT_EXECUTION_ID_METADATA,
    ToolCall, ToolChoice, Usage, emit_agent_event, select,
};
use crate::conversation::{
    AgentConversationError, AgentConversationOutcome, AutomaticConversationSummary,
    ConversationAppend, ConversationContextPolicy, ConversationId, ConversationStore,
    ConversationSummaryCommit, ConversationSummaryRequest, DurableConversationCommit,
    DurableConversationRequest, DurableConversationStore, MemoryNamespace, SemanticMemoryQuery,
    is_transient_context, semantic_memory_message, summary_message,
};
use runifold_core::{CheckpointId, CheckpointStore};
use runifold_retrieval::RetrievalContext;

impl Agent {
    /// Runs a user text turn with a default root context.
    ///
    /// This is the ergonomic surface for one-off prompts. It grants only
    /// callables registered on this Agent and applies no hard budget limit.
    /// Use [`Self::run`] when the caller must provide explicit authority,
    /// budget, deadline, observability, or run-tree identity.
    pub fn prompt<'a>(
        &'a self,
        input: impl Into<String> + Send + 'a,
    ) -> AgentFuture<'a, Result<AgentOutcome, AgentError>> {
        let input = input.into();
        Box::pin(async move {
            let run = self.default_run_context();
            self.run(input, &run).await
        })
    }

    /// Runs an ergonomic prompt and returns only model-visible text.
    ///
    /// Rich content, usage, warnings, the canonical transcript, and provider
    /// events are intentionally discarded. Use [`Self::prompt`] when that
    /// information matters.
    pub fn prompt_text<'a>(
        &'a self,
        input: impl Into<String> + Send + 'a,
    ) -> AgentFuture<'a, Result<String, AgentError>> {
        let input = input.into();
        Box::pin(async move { self.prompt(input).await.map(AgentOutcome::into_text) })
    }

    /// Runs a user text turn inside an existing runtime context.
    pub fn run<'a>(
        &'a self,
        input: impl Into<String> + Send + 'a,
        run: &'a RunContext,
    ) -> AgentFuture<'a, Result<AgentOutcome, AgentError>> {
        let input = input.into();
        let state = self.initial_state(input, InvocationId::new().to_string());
        Box::pin(async move {
            self.execute_state(state, run, None, Arc::new(NoopObserver), true, true)
                .await
        })
    }

    /// Runs and atomically commits one bounded multi-turn conversation.
    ///
    /// Transcript messages remain append-only. Execution-journal events stay
    /// in [`runifold_core::Journal`], summaries remain lossy derived views,
    /// and semantic memory is injected only as explicitly untrusted context.
    pub fn run_conversation<'a>(
        &'a self,
        input: impl Into<String> + Send + 'a,
        run: &'a RunContext,
        store: &'a dyn ConversationStore,
        conversation_id: ConversationId,
        namespace: MemoryNamespace,
        policy: ConversationContextPolicy,
    ) -> AgentFuture<'a, Result<AgentConversationOutcome, AgentConversationError>> {
        let input = input.into();
        Box::pin(async move {
            store.create(conversation_id, namespace.clone()).await?;
            let view = store
                .load_view(
                    conversation_id,
                    namespace.clone(),
                    policy.window,
                    policy.summary_batch,
                )
                .await?;
            if view.requires_summary() {
                return Err(AgentConversationError::SummaryRequired {
                    conversation_id,
                    buffered_entries: u64::try_from(view.summary_buffer.len())
                        .unwrap_or(u64::MAX)
                        .saturating_add(view.summary_backlog),
                });
            }
            let mut transcript = self.instructions.clone();
            if let Some(summary) = &view.summary {
                transcript.push(summary_message(summary));
            }
            if let Some(limit) = policy.semantic_memory_limit {
                let query =
                    SemanticMemoryQuery::new(namespace.clone(), input.clone(), limit.get())?;
                let search = store
                    .search_memory_scoped(query, RetrievalContext::for_run(run))
                    .await?;
                if search.usage != Usage::default() {
                    consume_budget(run, search.usage, None).map_err(AgentConversationError::Run)?;
                }
                if let Some(message) = semantic_memory_message(&search.memories) {
                    transcript.push(message);
                }
            }
            transcript.extend(view.window.iter().map(|entry| entry.message.clone()));
            let persisted_prefix_len = transcript.len();
            transcript.push(Message::user(input));
            let state =
                self.initial_state_from_transcript(transcript, InvocationId::new().to_string());
            let outcome = self
                .execute_state(state, run, None, Arc::new(NoopObserver), true, true)
                .await
                .map_err(AgentConversationError::Run)?;
            let messages = outcome
                .transcript
                .iter()
                .skip(persisted_prefix_len)
                .filter(|message| !is_transient_context(message))
                .cloned()
                .collect();
            let append = ConversationAppend {
                conversation_id,
                expected_version: view.version,
                messages,
            };
            match store.append(namespace, append).await {
                Ok(conversation_version) => Ok(AgentConversationOutcome {
                    outcome,
                    conversation_version,
                }),
                Err(source) => Err(AgentConversationError::Commit {
                    source,
                    outcome: Box::new(outcome),
                }),
            }
        })
    }

    /// Summarizes an overflowing prefix before running a conversational turn.
    ///
    /// Summary generation uses the supplied [`AutomaticConversationSummary`]
    /// and the same [`RunContext`], preserving cancellation, deadline, budget,
    /// and journal behavior. The immutable transcript is never rewritten.
    pub fn run_conversation_with_summary<'a>(
        &'a self,
        input: impl Into<String> + Send + 'a,
        run: &'a RunContext,
        store: &'a dyn ConversationStore,
        conversation_id: ConversationId,
        namespace: MemoryNamespace,
        automatic_summary: AutomaticConversationSummary<'a>,
    ) -> AgentFuture<'a, Result<AgentConversationOutcome, AgentConversationError>> {
        let input = input.into();
        Box::pin(async move {
            let policy = automatic_summary.context;
            store.create(conversation_id, namespace.clone()).await?;
            for pass in 0..automatic_summary.max_passes.get() {
                let view = store
                    .load_view(
                        conversation_id,
                        namespace.clone(),
                        policy.window,
                        policy.summary_batch,
                    )
                    .await?;
                let Some(through_sequence) = view.summary_buffer.last().map(|entry| entry.sequence)
                else {
                    break;
                };
                let summary_backlog = view.summary_backlog;
                let summary = automatic_summary
                    .summarizer
                    .summarize(
                        ConversationSummaryRequest {
                            transcript_version: view.version,
                            previous_summary: view.summary,
                            entries: view.summary_buffer,
                        },
                        run,
                    )
                    .await?;
                store
                    .commit_summary(
                        namespace.clone(),
                        ConversationSummaryCommit {
                            conversation_id,
                            expected_version: view.version,
                            through_sequence,
                            content: summary,
                        },
                    )
                    .await?;
                if summary_backlog == 0 {
                    break;
                }
                if pass + 1 == automatic_summary.max_passes.get() {
                    return Err(AgentConversationError::SummaryPassLimitExceeded {
                        conversation_id,
                        remaining_entries: summary_backlog,
                    });
                }
            }
            self.run_conversation(input, run, store, conversation_id, namespace, policy)
                .await
        })
    }

    /// Streams real-time events while driving the canonical Agent loop.
    pub fn stream<'a>(
        &'a self,
        input: impl Into<String> + Send + 'a,
        run: &'a RunContext,
    ) -> AgentEventStream<'a> {
        let state = self.initial_state(input.into(), InvocationId::new().to_string());
        let observer = BufferedObserver::default();
        let events = observer.events();
        let execution =
            Box::pin(self.execute_state(state, run, None, Arc::new(observer), true, true));
        AgentEventStream::new(execution, events)
    }

    /// Runs one conversational turn with atomic transcript and checkpoint commit.
    ///
    /// Intermediate checkpoints are written ahead of model and callable work.
    /// The terminal checkpoint and transcript append are committed together by
    /// [`DurableConversationStore`].
    pub fn run_durable_conversation<'a>(
        &'a self,
        input: impl Into<String> + Send + 'a,
        run: &'a RunContext,
        store: Arc<dyn DurableConversationStore>,
        request: DurableConversationRequest,
    ) -> AgentFuture<'a, Result<AgentConversationOutcome, AgentConversationError>> {
        let input = input.into();
        Box::pin(async move {
            let DurableConversationRequest {
                checkpoint_id,
                conversation_id,
                namespace,
                policy,
            } = request;
            store.create(conversation_id, namespace.clone()).await?;
            let view = store
                .load_view(
                    conversation_id,
                    namespace.clone(),
                    policy.window,
                    policy.summary_batch,
                )
                .await?;
            if view.requires_summary() {
                return Err(AgentConversationError::SummaryRequired {
                    conversation_id,
                    buffered_entries: u64::try_from(view.summary_buffer.len())
                        .unwrap_or(u64::MAX)
                        .saturating_add(view.summary_backlog),
                });
            }
            let mut transcript = self.instructions.clone();
            if let Some(summary) = &view.summary {
                transcript.push(summary_message(summary));
            }
            if let Some(limit) = policy.semantic_memory_limit {
                let query =
                    SemanticMemoryQuery::new(namespace.clone(), input.clone(), limit.get())?;
                let search = store
                    .search_memory_scoped(query, RetrievalContext::for_run(run))
                    .await?;
                if search.usage != Usage::default() {
                    consume_budget(run, search.usage, None).map_err(AgentConversationError::Run)?;
                }
                if let Some(message) = semantic_memory_message(&search.memories) {
                    transcript.push(message);
                }
            }
            transcript.extend(view.window.iter().map(|entry| entry.message.clone()));
            let persisted_prefix_len = u64::try_from(transcript.len()).map_err(|_| {
                AgentConversationError::Run(checkpoint_payload_error(
                    "conversation context length exceeds durable checkpoint range",
                ))
            })?;
            transcript.push(Message::user(input));
            let durable = DurableConversationCheckpoint {
                conversation_id,
                namespace,
                expected_version: view.version,
                persisted_prefix_len,
            };
            let mut state =
                self.initial_state_from_transcript(transcript, checkpoint_id.to_string());
            state.durable_conversation = Some(durable.clone());
            state.usage = run.budget().usage();
            let checkpoint_store: Arc<dyn CheckpointStore> = store.clone();
            let checkpoint = AgentCheckpoint::existing(checkpoint_id, checkpoint_store);
            let mut cursor = CheckpointCursor::create(&checkpoint, run, &state)
                .map_err(AgentConversationError::Run)?;
            let outcome = self
                .execute_state(
                    state,
                    run,
                    Some(&mut cursor),
                    Arc::new(NoopObserver),
                    true,
                    false,
                )
                .await
                .map_err(AgentConversationError::Run)?;
            self.commit_durable_outcome(store.as_ref(), run, &cursor, durable, outcome)
                .await
        })
    }

    /// Resumes a durable conversational turn from its write-ahead checkpoint.
    pub fn resume_durable_conversation<'a>(
        &'a self,
        store: Arc<dyn DurableConversationStore>,
        checkpoint_id: CheckpointId,
        run: &'a RunContext,
        policy: ResumePolicy,
    ) -> AgentFuture<'a, Result<AgentConversationOutcome, AgentConversationError>> {
        Box::pin(async move {
            let checkpoint_store: Arc<dyn CheckpointStore> = store.clone();
            let checkpoint = AgentCheckpoint::existing(checkpoint_id, checkpoint_store);
            let (envelope, mut state) = checkpoint
                .load()
                .map_err(AgentError::from)
                .map_err(AgentConversationError::Run)?;
            self.validate_checkpoint_identity(&state)
                .map_err(AgentConversationError::Run)?;
            let durable = state.durable_conversation.clone().ok_or_else(|| {
                AgentConversationError::Run(checkpoint_payload_error(
                    "checkpoint is not a durable conversation turn",
                ))
            })?;
            if let Some(outcome) = state.outcome() {
                let conversation_version = durable
                    .expected_version
                    .get()
                    .checked_add(1)
                    .map(crate::ConversationVersion::new)
                    .ok_or_else(|| {
                        AgentConversationError::Run(checkpoint_payload_error(
                            "durable conversation version overflow",
                        ))
                    })?;
                return Ok(AgentConversationOutcome {
                    outcome,
                    conversation_version,
                });
            }
            if let Some(error) = state.terminal_failure() {
                validate_exact_usage(state.usage, run.budget().usage())
                    .map_err(AgentConversationError::Run)?;
                return Err(AgentConversationError::Run(error));
            }
            if let AgentCheckpointPhase::TurnInFlight { turn } = state.phase {
                if policy == ResumePolicy::RejectAmbiguous {
                    return Err(AgentConversationError::Run(
                        AgentError::AmbiguousCheckpoint { turn },
                    ));
                }
                validate_usage_floor(state.usage, run.budget().usage())
                    .map_err(AgentConversationError::Run)?;
                state.usage = run.budget().usage();
                state.phase = AgentCheckpointPhase::ReadyForTurn;
            } else {
                validate_exact_usage(state.usage, run.budget().usage())
                    .map_err(AgentConversationError::Run)?;
            }
            let mut cursor = CheckpointCursor::loaded(&checkpoint, envelope);
            let outcome = self
                .execute_state(
                    state,
                    run,
                    Some(&mut cursor),
                    Arc::new(NoopObserver),
                    false,
                    false,
                )
                .await
                .map_err(AgentConversationError::Run)?;
            self.commit_durable_outcome(store.as_ref(), run, &cursor, durable, outcome)
                .await
        })
    }

    /// Runs with write-ahead checkpoint persistence.
    pub fn run_checkpointed<'a>(
        &'a self,
        input: impl Into<String> + Send + 'a,
        run: &'a RunContext,
        checkpoint: &'a AgentCheckpoint,
    ) -> AgentFuture<'a, Result<AgentOutcome, AgentError>> {
        let input = input.into();
        Box::pin(async move {
            let mut state = self.initial_state(input, checkpoint.id().to_string());
            state.usage = run.budget().usage();
            let mut cursor = CheckpointCursor::create(checkpoint, run, &state)?;
            self.execute_state(
                state,
                run,
                Some(&mut cursor),
                Arc::new(NoopObserver),
                true,
                true,
            )
            .await
        })
    }

    /// Resumes a persisted Agent execution.
    pub fn resume<'a>(
        &'a self,
        checkpoint: &'a AgentCheckpoint,
        run: &'a RunContext,
        policy: ResumePolicy,
    ) -> AgentFuture<'a, Result<AgentOutcome, AgentError>> {
        Box::pin(async move {
            let (envelope, mut state) = checkpoint.load()?;
            self.validate_checkpoint_identity(&state)?;
            if let Some(outcome) = state.outcome() {
                validate_exact_usage(state.usage, run.budget().usage())?;
                return Ok(outcome);
            }
            if let Some(error) = state.terminal_failure() {
                validate_exact_usage(state.usage, run.budget().usage())?;
                return Err(error);
            }
            if let AgentCheckpointPhase::TurnInFlight { turn } = state.phase {
                if policy == ResumePolicy::RejectAmbiguous {
                    return Err(AgentError::AmbiguousCheckpoint { turn });
                }
                validate_usage_floor(state.usage, run.budget().usage())?;
                state.usage = run.budget().usage();
                state.phase = AgentCheckpointPhase::ReadyForTurn;
            } else {
                validate_exact_usage(state.usage, run.budget().usage())?;
            }
            let mut cursor = CheckpointCursor::loaded(checkpoint, envelope);
            self.execute_state(
                state,
                run,
                Some(&mut cursor),
                Arc::new(NoopObserver),
                false,
                true,
            )
            .await
        })
    }

    fn initial_state(&self, input: String, execution_id: String) -> AgentCheckpointState {
        let mut transcript = self.instructions.clone();
        transcript.push(Message::user(input));
        self.initial_state_from_transcript(transcript, execution_id)
    }

    fn initial_state_from_transcript(
        &self,
        transcript: Vec<Message>,
        execution_id: String,
    ) -> AgentCheckpointState {
        AgentCheckpointState {
            execution_id,
            agent: self.name.clone(),
            model: self.model_ref.clone(),
            transcript,
            turns: 0,
            tool_calls: 0,
            delegations: 0,
            usage: Usage::default(),
            phase: AgentCheckpointPhase::ReadyForTurn,
            durable_conversation: None,
        }
    }

    async fn execute_state(
        &self,
        state: AgentCheckpointState,
        run: &RunContext,
        mut checkpoint: Option<&mut CheckpointCursor>,
        observer: Arc<dyn AgentObserver>,
        retrieve_context: bool,
        persist_terminal_checkpoint: bool,
    ) -> Result<AgentOutcome, AgentError> {
        let started = run
            .record(
                RunEventKind::Lifecycle(LifecycleEvent::Started),
                run.caused_by(),
            )?
            .map(|event| event.meta.event_id);
        emit_agent_event(
            observer.as_ref(),
            AgentStreamEvent::Started {
                agent: self.name.clone(),
            },
        )
        .await;
        let result = async {
            let has_context = !self.context.is_empty() || !self.dynamic_context.is_empty();
            let state = if retrieve_context && has_context {
                let mut prepared = self
                    .prepare_context(state, run, started, observer.as_ref())
                    .await?;
                prepared.usage = run.budget().usage();
                save_checkpoint(&mut checkpoint, &prepared)?;
                prepared
            } else {
                state
            };
            self.run_loop(
                state,
                run,
                started,
                checkpoint,
                observer.as_ref(),
                persist_terminal_checkpoint,
            )
            .await
        }
        .await;
        let terminal = terminal_event(&self.name, &result);
        run.record(terminal, started)?;
        if let Ok(outcome) = &result {
            emit_agent_event(
                observer.as_ref(),
                AgentStreamEvent::Completed {
                    outcome: outcome.clone(),
                },
            )
            .await;
        }
        result
    }

    async fn run_loop(
        &self,
        state: AgentCheckpointState,
        run: &RunContext,
        caused_by: Option<EventId>,
        mut checkpoint: Option<&mut CheckpointCursor>,
        observer: &dyn AgentObserver,
        persist_terminal_checkpoint: bool,
    ) -> Result<AgentOutcome, AgentError> {
        self.validate_config()?;
        let mut progress = AgentProgress::from(state);

        loop {
            Self::check_lifecycle(run)?;
            let tool_choice = self.next_tool_choice(&progress, run)?;
            let requires_tool = matches!(tool_choice, ToolChoice::Required);
            save_checkpoint(
                &mut checkpoint,
                &self.checkpoint_state(
                    &progress,
                    run,
                    AgentCheckpointPhase::TurnInFlight {
                        turn: progress.turns + 1,
                    },
                ),
            )?;
            consume_budget(
                run,
                Usage {
                    turns: 1,
                    ..Usage::default()
                },
                caused_by,
            )?;
            progress.turns += 1;
            emit_agent_event(
                observer,
                AgentStreamEvent::TurnStarted {
                    turn: progress.turns,
                },
            )
            .await;
            emit_usage(observer, run).await;
            record_domain(
                run,
                "turn.started",
                serde_json::json!({"agent": self.name, "turn": progress.turns}),
                caused_by,
            )?;

            let response = self
                .invoke_model(
                    &progress.transcript,
                    run,
                    progress.turns,
                    tool_choice,
                    caused_by,
                    observer,
                )
                .await?;

            let calls = tool_calls_from(&response.content);
            if calls.is_empty() {
                if matches!(
                    response.finish_reason,
                    runifold_model::FinishReason::ToolCalls
                ) && !response.content.is_empty()
                {
                    return Err(AgentError::Protocol(
                        "model stopped for tool calls without emitting a tool call".into(),
                    ));
                }
                if requires_tool {
                    return Err(AgentError::ToolRequirementUnsatisfied {
                        required: self.min_successful_tool_calls,
                        successful: self.successful_local_tool_calls(&progress)?,
                    });
                }
                if let Some(outcome) = self
                    .complete_terminal_candidate(
                        response,
                        run,
                        &mut progress,
                        &mut checkpoint,
                        TerminalCompletionContext {
                            caused_by,
                            observer,
                            persist_terminal_checkpoint,
                        },
                    )
                    .await?
                {
                    return Ok(outcome);
                }
                continue;
            }

            let assistant = Message::new(Role::Assistant, response.content.clone())
                .map_err(|error| AgentError::Protocol(error.to_string()))?;
            progress.transcript.push(assistant);

            self.execute_calls(calls, run, caused_by, &mut progress, observer)
                .await?;
            save_checkpoint(
                &mut checkpoint,
                &self.checkpoint_state(&progress, run, AgentCheckpointPhase::ReadyForTurn),
            )?;
        }
    }

    async fn invoke_model(
        &self,
        transcript: &[Message],
        run: &RunContext,
        turn: u32,
        tool_choice: ToolChoice,
        caused_by: Option<EventId>,
        observer: &dyn AgentObserver,
    ) -> Result<ModelResponse, AgentError> {
        record_domain(
            run,
            "model.started",
            serde_json::json!({
                "agent": self.name,
                "turn": turn,
                "provider": self.model_ref.provider,
                "model": self.model_ref.name,
            }),
            caused_by,
        )?;
        let response = match self
            .stream_model_response(self.request(transcript, tool_choice)?, run, turn, observer)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                record_domain(
                    run,
                    "model.failed",
                    serde_json::json!({
                        "agent": self.name,
                        "turn": turn,
                        "kind": format!("{:?}", error.kind),
                    }),
                    caused_by,
                )?;
                return Err(error.into());
            }
        };
        record_domain(
            run,
            "model.completed",
            serde_json::json!({
                "agent": self.name,
                "turn": turn,
                "finish_reason": response.finish_reason,
                "usage": response.usage,
            }),
            caused_by,
        )?;
        consume_budget(run, response.usage.into(), caused_by)?;
        emit_usage(observer, run).await;
        Ok(response)
    }

    async fn stream_model_response(
        &self,
        request: ModelRequest,
        run: &RunContext,
        turn: u32,
        observer: &dyn AgentObserver,
    ) -> Result<ModelResponse, ModelError> {
        let context = ModelCallContext::for_run(run);
        let cancellation = context.cancellation().clone();
        let opening = self.model.stream(request, context);
        let mut stream = match select(Box::pin(cancellation.cancelled()), Box::pin(opening)).await {
            Either::Left(_) => return Err(cancelled_model_error()),
            Either::Right((result, _)) => result?,
        };
        let mut accumulator = ModelStreamAccumulator::new();
        loop {
            let next = stream.next();
            let event = match select(Box::pin(cancellation.cancelled()), Box::pin(next)).await {
                Either::Left(_) => return Err(cancelled_model_error()),
                Either::Right((Some(event), _)) => event?,
                Either::Right((None, _)) => {
                    return Err(ModelError::local(
                        ModelErrorKind::Protocol,
                        "model stream ended before a terminal response event",
                    ));
                }
            };
            let response = accumulator.push(event.clone())?;
            emit_agent_event(observer, AgentStreamEvent::Model { turn, event }).await;
            if let Some(response) = response {
                return Ok(response);
            }
        }
    }

    fn validate_config(&self) -> Result<(), AgentError> {
        if self.name.trim().is_empty() {
            return Err(AgentError::InvalidConfig(
                "agent name cannot be empty".into(),
            ));
        }
        if self.config.max_turns == 0 {
            return Err(AgentError::InvalidConfig(
                "max_turns must be greater than zero".into(),
            ));
        }
        if self.min_successful_tool_calls > 0 && self.tools.is_empty() {
            return Err(AgentError::InvalidConfig(format!(
                "min_successful_tool_calls={} requires at least one registered local Tool",
                self.min_successful_tool_calls
            )));
        }
        if let Some(collision) = self
            .agents
            .model_specs()
            .into_iter()
            .find(|spec| self.tools.contains(&spec.name))
        {
            return Err(AgentError::InvalidConfig(format!(
                "callable name `{}` is registered as both a tool and an agent",
                collision.name
            )));
        }
        Ok(())
    }

    fn validate_checkpoint_identity(&self, state: &AgentCheckpointState) -> Result<(), AgentError> {
        if state.agent != self.name || state.model != self.model_ref {
            return Err(runifold_core::CheckpointError::new(
                runifold_core::CheckpointErrorKind::InvalidPayload,
                "checkpoint Agent or model identity does not match",
            )
            .into());
        }
        Ok(())
    }

    pub(super) fn checkpoint_state(
        &self,
        progress: &AgentProgress,
        run: &RunContext,
        phase: AgentCheckpointPhase,
    ) -> AgentCheckpointState {
        AgentCheckpointState {
            execution_id: progress.execution_id.clone(),
            agent: self.name.clone(),
            model: self.model_ref.clone(),
            transcript: progress.transcript.clone(),
            turns: progress.turns,
            tool_calls: progress.tool_calls,
            delegations: progress.delegations,
            usage: run.budget().usage(),
            phase,
            durable_conversation: progress.durable_conversation.clone(),
        }
    }

    async fn commit_durable_outcome(
        &self,
        store: &dyn DurableConversationStore,
        run: &RunContext,
        cursor: &CheckpointCursor,
        durable: DurableConversationCheckpoint,
        outcome: AgentOutcome,
    ) -> Result<AgentConversationOutcome, AgentConversationError> {
        let persisted_prefix_len = usize::try_from(durable.persisted_prefix_len).map_err(|_| {
            AgentConversationError::Run(checkpoint_payload_error(
                "durable conversation prefix does not fit this platform",
            ))
        })?;
        if persisted_prefix_len >= outcome.transcript.len() {
            return Err(AgentConversationError::Run(checkpoint_payload_error(
                "durable conversation checkpoint has an invalid transcript prefix",
            )));
        }
        let messages = outcome
            .transcript
            .iter()
            .skip(persisted_prefix_len)
            .filter(|message| !is_transient_context(message))
            .cloned()
            .collect();
        let state = AgentCheckpointState {
            execution_id: cursor.id().to_string(),
            agent: self.name.clone(),
            model: self.model_ref.clone(),
            transcript: outcome.transcript.clone(),
            turns: outcome.turns,
            tool_calls: outcome.tool_calls,
            delegations: outcome.delegations,
            usage: run.budget().usage(),
            phase: AgentCheckpointPhase::Completed {
                response: Box::new(outcome.response.clone()),
            },
            durable_conversation: Some(durable.clone()),
        };
        let checkpoint = cursor.next(&state).map_err(AgentConversationError::Run)?;
        let command = DurableConversationCommit {
            namespace: durable.namespace,
            append: ConversationAppend {
                conversation_id: durable.conversation_id,
                expected_version: durable.expected_version,
                messages,
            },
            checkpoint,
            expected_checkpoint_revision: cursor.revision(),
        };
        match store.commit_durable_turn(command).await {
            Ok(conversation_version) => Ok(AgentConversationOutcome {
                outcome,
                conversation_version,
            }),
            Err(source) => Err(AgentConversationError::Commit {
                source,
                outcome: Box::new(outcome),
            }),
        }
    }

    pub(super) fn check_lifecycle(run: &RunContext) -> Result<(), AgentError> {
        let error = if run.cancellation().is_cancelled() {
            Some((
                runifold_model::ModelErrorKind::Cancelled,
                "agent run was cancelled",
            ))
        } else if run
            .deadline()
            .is_some_and(|deadline| deadline <= Instant::now())
        {
            Some((
                runifold_model::ModelErrorKind::DeadlineExceeded,
                "agent run deadline elapsed",
            ))
        } else {
            None
        };
        if let Some((kind, message)) = error {
            return Err(runifold_model::ModelError::local(kind, message).into());
        }
        Ok(())
    }

    fn request(
        &self,
        transcript: &[Message],
        tool_choice: ToolChoice,
    ) -> Result<ModelRequest, AgentError> {
        let (first, rest) = transcript
            .split_first()
            .ok_or_else(|| AgentError::Protocol("agent transcript is empty".into()))?;
        let mut request = ModelRequest::new(self.model_ref.clone(), first.clone());
        request.messages.extend_from_slice(rest);
        request.tools = self.tools.model_specs();
        request.tools.extend(self.agents.model_specs());
        request.tool_choice = tool_choice;
        for tool in &self.provider_tools {
            request = request.provider_tool(tool.clone());
        }
        request.generation.clone_from(&self.generation);
        request = request.response_mode(self.response_mode);
        request.provider_options.clone_from(&self.provider_options);
        request.feature_policy = self.config.feature_policy;
        request.output_format.clone_from(&self.output_format);
        Ok(request)
    }

    fn successful_local_tool_calls(&self, progress: &AgentProgress) -> Result<u32, AgentError> {
        let count = progress
            .transcript
            .iter()
            .filter(|message| {
                message
                    .metadata
                    .get(TOOL_RESULT_EXECUTION_ID_METADATA)
                    .and_then(serde_json::Value::as_str)
                    == Some(progress.execution_id.as_str())
            })
            .flat_map(|message| &message.content)
            .filter(|part| {
                matches!(
                    part,
                    ContentPart::ToolResult(result)
                        if !result.is_error
                            && result
                                .name
                                .as_deref()
                                .is_some_and(|name| self.tools.contains(name))
                )
            })
            .count();
        u32::try_from(count)
            .map_err(|_| AgentError::Protocol("successful Tool-call counter overflow".into()))
    }

    fn next_tool_choice(
        &self,
        progress: &AgentProgress,
        run: &RunContext,
    ) -> Result<ToolChoice, AgentError> {
        let successful = self.successful_local_tool_calls(progress)?;
        let remaining_required = self.min_successful_tool_calls.saturating_sub(successful);
        Self::validate_tool_requirement_budget(remaining_required, run)?;
        if progress.turns >= self.config.max_turns {
            if remaining_required > 0 {
                return Err(AgentError::ToolRequirementUnsatisfied {
                    required: self.min_successful_tool_calls,
                    successful,
                });
            }
            return Err(AgentError::MaxTurns {
                max_turns: self.config.max_turns,
            });
        }
        Ok(if remaining_required > 0 {
            ToolChoice::Required
        } else {
            ToolChoice::Auto
        })
    }

    fn validate_tool_requirement_budget(
        remaining_required: u32,
        run: &RunContext,
    ) -> Result<(), AgentError> {
        let Some(limit) = run.budget().limit().tool_calls else {
            return Ok(());
        };
        let remaining = limit.saturating_sub(run.budget().usage().tool_calls);
        if u64::from(remaining_required) > remaining {
            return Err(AgentError::ToolRequirementExceedsBudget {
                required: remaining_required,
                remaining,
            });
        }
        Ok(())
    }
}

fn checkpoint_payload_error(message: &str) -> AgentError {
    runifold_core::CheckpointError::new(runifold_core::CheckpointErrorKind::InvalidPayload, message)
        .into()
}

fn cancelled_model_error() -> ModelError {
    ModelError::local(ModelErrorKind::Cancelled, "model invocation was cancelled")
}

fn tool_calls_from(content: &[ContentPart]) -> Vec<ToolCall> {
    content
        .iter()
        .filter_map(|part| match part {
            ContentPart::ToolCall(call) => Some(call.clone()),
            _ => None,
        })
        .collect()
}
