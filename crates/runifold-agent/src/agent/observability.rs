//! Agent event emission, budget accounting, and error normalization.

use super::{
    AgentError, AgentObserver, AgentOutcome, AgentStreamEvent, BudgetEvent, DomainEvent, EventId,
    LifecycleEvent, RunContext, RunErrorKind, RunEventKind, ToolCall, ToolOutput, Usage,
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
        Err(error) if error.run_error_kind() == RunErrorKind::Cancelled => {
            RunEventKind::Lifecycle(LifecycleEvent::Cancelled)
        }
        Err(error) => RunEventKind::Lifecycle(LifecycleEvent::Failed {
            error: error.to_run_error(),
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
