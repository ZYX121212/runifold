//! Canonical Agent execution engine and its private runtime helpers.

use super::checkpointing::{
    AgentProgress, save_checkpoint, validate_exact_usage, validate_usage_floor,
};
use super::observability::{consume_budget, emit_usage, record_domain, terminal_event};
use super::{
    Agent, AgentCheckpoint, AgentCheckpointPhase, AgentCheckpointState, AgentError,
    AgentEventStream, AgentFuture, AgentObserver, AgentOutcome, AgentStreamEvent, Arc,
    BufferedObserver, CheckpointCursor, ContentPart, Either, EventId, Instant, LifecycleEvent,
    Message, ModelCallContext, ModelError, ModelErrorKind, ModelRequest, ModelResponse,
    ModelStreamAccumulator, NoopObserver, ResumePolicy, Role, RunContext, RunEventKind, StreamExt,
    ToolCall, Usage, emit_agent_event, select,
};

impl Agent {
    /// Runs a user text turn inside an existing runtime context.
    pub fn run<'a>(
        &'a self,
        input: impl Into<String> + Send + 'a,
        run: &'a RunContext,
    ) -> AgentFuture<'a, Result<AgentOutcome, AgentError>> {
        let input = input.into();
        let state = self.initial_state(input, run.root_run_id().to_string());
        Box::pin(async move {
            self.execute_state(state, run, None, Arc::new(NoopObserver))
                .await
        })
    }

    /// Streams real-time events while driving the canonical Agent loop.
    pub fn stream<'a>(
        &'a self,
        input: impl Into<String> + Send + 'a,
        run: &'a RunContext,
    ) -> AgentEventStream<'a> {
        let state = self.initial_state(input.into(), run.root_run_id().to_string());
        let observer = BufferedObserver::default();
        let events = observer.events();
        let execution = Box::pin(self.execute_state(state, run, None, Arc::new(observer)));
        AgentEventStream::new(execution, events)
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
            self.execute_state(state, run, Some(&mut cursor), Arc::new(NoopObserver))
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
            self.execute_state(state, run, Some(&mut cursor), Arc::new(NoopObserver))
                .await
        })
    }

    fn initial_state(&self, input: String, execution_id: String) -> AgentCheckpointState {
        let mut transcript = self.instructions.clone();
        transcript.push(Message::user(input));
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
        }
    }

    async fn execute_state(
        &self,
        state: AgentCheckpointState,
        run: &RunContext,
        checkpoint: Option<&mut CheckpointCursor>,
        observer: Arc<dyn AgentObserver>,
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
        let result = self
            .run_loop(state, run, started, checkpoint, observer.as_ref())
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
    ) -> Result<AgentOutcome, AgentError> {
        self.validate_config()?;
        let mut progress = AgentProgress::from(state);

        loop {
            Self::check_lifecycle(run)?;
            if progress.turns >= self.config.max_turns {
                return Err(AgentError::MaxTurns {
                    max_turns: self.config.max_turns,
                });
            }
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
                    caused_by,
                    observer,
                )
                .await?;

            let calls = tool_calls_from(&response.content);
            let assistant = Message::new(Role::Assistant, response.content.clone())
                .map_err(|error| AgentError::Protocol(error.to_string()))?;
            progress.transcript.push(assistant);

            if calls.is_empty() {
                if matches!(
                    response.finish_reason,
                    runifold_model::FinishReason::ToolCalls
                ) {
                    return Err(AgentError::Protocol(
                        "model stopped for tool calls without emitting a tool call".into(),
                    ));
                }
                save_checkpoint(
                    &mut checkpoint,
                    &self.checkpoint_state(
                        &progress,
                        run,
                        AgentCheckpointPhase::Completed {
                            response: Box::new(response.clone()),
                        },
                    ),
                )?;
                return Ok(progress.outcome(response, run.budget().usage()));
            }

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
            .stream_model_response(self.request(transcript)?, run, turn, observer)
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

    fn checkpoint_state(
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

    fn request(&self, transcript: &[Message]) -> Result<ModelRequest, AgentError> {
        let (first, rest) = transcript
            .split_first()
            .ok_or_else(|| AgentError::Protocol("agent transcript is empty".into()))?;
        let mut request = ModelRequest::new(self.model_ref.clone(), first.clone());
        request.messages.extend_from_slice(rest);
        request.tools = self.tools.model_specs();
        request.tools.extend(self.agents.model_specs());
        request.feature_policy = self.config.feature_policy;
        request.output_format.clone_from(&self.output_format);
        Ok(request)
    }
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
