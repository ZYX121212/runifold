//! Agent event emission, budget accounting, and error normalization.

use super::{
    AgentError, AgentObserver, AgentOutcome, AgentStreamEvent, BTreeMap, BudgetEvent, DomainEvent,
    EffectExecutorErrorKind, EventId, GatewayErrorKind, LifecycleEvent, RetrySafety, RunContext,
    RunError, RunErrorKind, RunEventKind, ToolCall, ToolErrorKind, ToolOutput, Usage,
    emit_agent_event,
};

pub(super) async fn emit_usage(observer: &dyn AgentObserver, run: &RunContext) {
    emit_agent_event(
        observer,
        AgentStreamEvent::UsageUpdated {
            usage: run.budget().usage(),
        },
    )
    .await;
}

pub(super) fn terminal_event(
    agent: &str,
    result: &Result<AgentOutcome, AgentError>,
) -> RunEventKind {
    match result {
        Ok(outcome) => RunEventKind::Lifecycle(LifecycleEvent::Completed {
            output: serde_json::json!({
                "agent": agent,
                "turns": outcome.turns,
                "tool_calls": outcome.tool_calls,
                "delegations": outcome.delegations,
                "terminal_repairs": outcome.terminal_repairs(),
                "usage": outcome.usage,
            }),
        }),
        Err(error) if agent_error_kind(error) == RunErrorKind::Cancelled => {
            RunEventKind::Lifecycle(LifecycleEvent::Cancelled)
        }
        Err(error) => RunEventKind::Lifecycle(LifecycleEvent::Failed {
            error: agent_run_error(error),
        }),
    }
}

pub(super) fn consume_budget(
    run: &RunContext,
    usage: Usage,
    caused_by: Option<EventId>,
) -> Result<(), AgentError> {
    let usage = run.budget().try_consume(usage)?;
    run.record(
        RunEventKind::Budget(BudgetEvent::Updated { usage }),
        caused_by,
    )?;
    Ok(())
}

pub(super) fn record_domain(
    run: &RunContext,
    name: &str,
    payload: serde_json::Value,
    caused_by: Option<EventId>,
) -> Result<(), AgentError> {
    run.record(
        RunEventKind::Domain(DomainEvent {
            namespace: "runifold.agent".into(),
            name: name.into(),
            payload,
        }),
        caused_by,
    )?;
    Ok(())
}

pub(super) fn record_callable(
    run: &RunContext,
    event: &str,
    agent: &str,
    callable_kind: &str,
    call: &ToolCall,
    caused_by: Option<EventId>,
) -> Result<(), AgentError> {
    let mut payload = serde_json::json!({
        "agent": agent,
        "call_id": call.id,
    });
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            callable_kind.into(),
            serde_json::Value::from(call.name.clone()),
        );
    }
    record_domain(run, event, payload, caused_by)
}

pub(super) fn record_tool_outcome<E>(
    run: &RunContext,
    event: &str,
    agent: &str,
    call: &ToolCall,
    result: &Result<ToolOutput, E>,
    caused_by: Option<EventId>,
) -> Result<(), AgentError> {
    let mut payload = serde_json::json!({
        "agent": agent,
        "call_id": call.id,
        "tool": call.name,
    });
    if let (Some(object), Ok(output)) = (payload.as_object_mut(), result) {
        let mut media_count = 0_u64;
        let mut artifact_count = 0_u64;
        for part in &output.content {
            match part {
                super::ContentPart::Image { source }
                | super::ContentPart::Audio { source }
                | super::ContentPart::Document { source, .. } => {
                    media_count = media_count.saturating_add(1);
                    if matches!(source, runifold_model::MediaSource::Artifact { .. }) {
                        artifact_count = artifact_count.saturating_add(1);
                    }
                }
                super::ContentPart::ResourceLink { .. } => {
                    artifact_count = artifact_count.saturating_add(1);
                }
                _ => {}
            }
        }
        object.insert("content_count".into(), output.content.len().into());
        object.insert("media_count".into(), media_count.into());
        object.insert("artifact_count".into(), artifact_count.into());
        object.insert(
            "structured_content".into(),
            output.structured_content.is_some().into(),
        );
        object.insert("application_error".into(), output.is_error.into());
    }
    record_domain(run, event, payload, caused_by)
}

fn agent_run_error(error: &AgentError) -> RunError {
    RunError {
        kind: agent_error_kind(error),
        message: error.to_string(),
        retry_safety: agent_retry_safety(error),
        metadata: BTreeMap::new(),
    }
}

fn agent_retry_safety(error: &AgentError) -> RetrySafety {
    match error {
        AgentError::Model(error) => error.retry_safety,
        AgentError::Tool(error) => error.retry_safety,
        AgentError::Effect(error) => error
            .source_error
            .as_ref()
            .map_or(RetrySafety::Unknown, |error| error.retry_safety),
        _ => RetrySafety::Unknown,
    }
}

fn agent_error_kind(error: &AgentError) -> RunErrorKind {
    match error {
        AgentError::Model(error) => match error.kind {
            runifold_model::ModelErrorKind::InvalidRequest
            | runifold_model::ModelErrorKind::UnsupportedFeature => RunErrorKind::InvalidInput,
            runifold_model::ModelErrorKind::Transport => RunErrorKind::Transport,
            runifold_model::ModelErrorKind::Cancelled => RunErrorKind::Cancelled,
            runifold_model::ModelErrorKind::DeadlineExceeded => RunErrorKind::DeadlineExceeded,
            runifold_model::ModelErrorKind::Protocol
            | runifold_model::ModelErrorKind::StreamState
            | runifold_model::ModelErrorKind::MalformedToolArguments => RunErrorKind::Protocol,
            _ => RunErrorKind::Invocation,
        },
        AgentError::Tool(error) => match error.kind {
            ToolErrorKind::InvalidInput => RunErrorKind::InvalidInput,
            ToolErrorKind::CapabilityDenied => RunErrorKind::CapabilityDenied,
            ToolErrorKind::Cancelled => RunErrorKind::Cancelled,
            ToolErrorKind::DeadlineExceeded => RunErrorKind::DeadlineExceeded,
            ToolErrorKind::NotFound | ToolErrorKind::Execution | ToolErrorKind::InvalidOutput => {
                RunErrorKind::Invocation
            }
            _ => RunErrorKind::Invocation,
        },
        AgentError::Retrieval(error) => match error {
            runifold_retrieval::RetrievalError::EmptyDocumentId
            | runifold_retrieval::RetrievalError::EmptyDocumentText { .. }
            | runifold_retrieval::RetrievalError::EmptyQuery
            | runifold_retrieval::RetrievalError::ZeroLimit
            | runifold_retrieval::RetrievalError::EmptyEmbedding
            | runifold_retrieval::RetrievalError::NonFiniteEmbedding { .. }
            | runifold_retrieval::RetrievalError::EmbeddingCoordinateOutOfRange { .. }
            | runifold_retrieval::RetrievalError::ZeroNormEmbedding
            | runifold_retrieval::RetrievalError::DimensionMismatch { .. }
            | runifold_retrieval::RetrievalError::EmbeddingCountMismatch { .. }
            | runifold_retrieval::RetrievalError::EmptyEmbeddingInput { .. }
            | runifold_retrieval::RetrievalError::DuplicateDocument(_) => {
                RunErrorKind::InvalidInput
            }
            runifold_retrieval::RetrievalError::UsageOverflow => RunErrorKind::BudgetExceeded,
            runifold_retrieval::RetrievalError::CapabilityDenied { .. } => {
                RunErrorKind::CapabilityDenied
            }
            runifold_retrieval::RetrievalError::Cancelled => RunErrorKind::Cancelled,
            runifold_retrieval::RetrievalError::DeadlineExceeded => RunErrorKind::DeadlineExceeded,
            _ => RunErrorKind::Invocation,
        },
        AgentError::Budget(_)
        | AgentError::MaxTurns { .. }
        | AgentError::ToolRequirementExceedsBudget { .. } => RunErrorKind::BudgetExceeded,
        AgentError::Gateway(error) => match error.kind {
            GatewayErrorKind::CapabilityDenied
            | GatewayErrorKind::AuthorityEscalation
            | GatewayErrorKind::PolicyDenied => RunErrorKind::CapabilityDenied,
            GatewayErrorKind::BudgetExceeded | GatewayErrorKind::MaxDepth => {
                RunErrorKind::BudgetExceeded
            }
            GatewayErrorKind::Cancelled => RunErrorKind::Cancelled,
            GatewayErrorKind::DeadlineExceeded => RunErrorKind::DeadlineExceeded,
            GatewayErrorKind::InvalidInput => RunErrorKind::InvalidInput,
            GatewayErrorKind::NotFound | GatewayErrorKind::ChildFailed => RunErrorKind::Invocation,
            GatewayErrorKind::ObservabilityFailed => {
                RunErrorKind::Extension("runifold.observability".into())
            }
        },
        AgentError::InvalidConfig(_) => RunErrorKind::InvalidInput,
        AgentError::Protocol(_)
        | AgentError::ToolRequirementUnsatisfied { .. }
        | AgentError::EmptyTerminalResponse { .. }
        | AgentError::StructuredOutputUnsatisfied { .. }
        | AgentError::ToolOutputNotVisible { .. } => RunErrorKind::Protocol,
        AgentError::Journal(_) => RunErrorKind::Extension("runifold.observability".into()),
        AgentError::Checkpoint(_) | AgentError::AmbiguousCheckpoint { .. } => {
            RunErrorKind::Extension("runifold.checkpoint".into())
        }
        AgentError::Effect(error) => match error.kind {
            EffectExecutorErrorKind::CapabilityDenied => RunErrorKind::CapabilityDenied,
            EffectExecutorErrorKind::Cancelled => RunErrorKind::Cancelled,
            EffectExecutorErrorKind::DeadlineExceeded => RunErrorKind::DeadlineExceeded,
            EffectExecutorErrorKind::IdempotencyConflict | EffectExecutorErrorKind::Protocol => {
                RunErrorKind::Protocol
            }
            EffectExecutorErrorKind::Handler => error
                .source_error
                .as_ref()
                .map_or(RunErrorKind::Invocation, |error| error.kind.clone()),
            EffectExecutorErrorKind::Ambiguous
            | EffectExecutorErrorKind::Store
            | EffectExecutorErrorKind::Observability => {
                RunErrorKind::Extension("runifold.effect".into())
            }
            _ => RunErrorKind::Extension("runifold.effect".into()),
        },
    }
}
