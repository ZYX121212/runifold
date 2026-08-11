//! Tool and child-Agent callable dispatch and effect execution.

use super::checkpointing::AgentProgress;
use super::observability::{consume_budget, emit_usage, record_callable, record_tool_outcome};
use super::{
    Agent, AgentError, AgentGateway, AgentObserver, AgentStreamEvent, BTreeMap, CallableKind,
    ContentPart, Deserialize, EffectExecutionContext, EffectFuture, EffectHandler, EffectId,
    EffectKind, EffectRequest, EventId, GatewayError, GatewayErrorKind, InvocationId, Message,
    RetrySafety, Role, RunContext, RunError, RunErrorKind, Serialize,
    TOOL_RESULT_EXECUTION_ID_METADATA, ToolCall, ToolError, ToolErrorKind, ToolErrorPolicy,
    ToolOutput, ToolRegistry, ToolResult, Usage, emit_agent_event,
};

impl Agent {
    pub(super) async fn execute_calls(
        &self,
        calls: Vec<ToolCall>,
        run: &RunContext,
        caused_by: Option<EventId>,
        progress: &mut AgentProgress,
        observer: &dyn AgentObserver,
    ) -> Result<(), AgentError> {
        for (call_index, call) in calls.into_iter().enumerate() {
            Self::check_lifecycle(run)?;
            let effect_key = format!(
                "{}:agent:{}:turn:{}:call:{call_index}",
                progress.execution_id, self.name, progress.turns
            );
            let context = CallableExecutionContext {
                run,
                caused_by,
                effect_key: &effect_key,
                turn: progress.turns,
                observer,
            };
            let result = if self.agents.contains(&call.name) {
                self.execute_delegation_call(&call, &mut progress.delegations, &context)
                    .await?
            } else {
                self.execute_local_tool_call(&call, &mut progress.tool_calls, &context)
                    .await?
            };
            progress.transcript.push(tool_result_message(
                call.id,
                call.name,
                result,
                &progress.execution_id,
            )?);
        }
        Ok(())
    }

    async fn execute_delegation_call(
        &self,
        call: &ToolCall,
        delegations: &mut u32,
        context: &CallableExecutionContext<'_>,
    ) -> Result<Result<ToolOutput, String>, AgentError> {
        record_callable(
            context.run,
            "delegation.started",
            &self.name,
            "delegation",
            call,
            context.caused_by,
        )?;
        emit_agent_event(
            context.observer,
            AgentStreamEvent::CallableStarted {
                turn: context.turn,
                kind: CallableKind::Agent,
                call: call.clone(),
            },
        )
        .await;
        let usage_before = context.run.budget().usage();
        let execution = self
            .execute_agent_effect(call, context.run, context.effect_key)
            .await;
        if context.run.budget().usage() != usage_before {
            emit_usage(context.observer, context.run).await;
        }
        let result = match execution {
            Ok(result) => result,
            Err(error) => {
                record_callable(
                    context.run,
                    "delegation.failed",
                    &self.name,
                    "delegation",
                    call,
                    context.caused_by,
                )?;
                return Err(error);
            }
        };
        record_callable(
            context.run,
            if result.is_ok() {
                "delegation.completed"
            } else {
                "delegation.failed"
            },
            &self.name,
            "delegation",
            call,
            context.caused_by,
        )?;
        emit_agent_event(
            context.observer,
            AgentStreamEvent::CallableCompleted {
                turn: context.turn,
                kind: CallableKind::Agent,
                call_id: call.id.clone(),
                name: call.name.clone(),
                success: result.is_ok(),
            },
        )
        .await;
        if result.is_ok() {
            *delegations = delegations
                .checked_add(1)
                .ok_or_else(|| AgentError::Protocol("local delegation counter overflow".into()))?;
        }
        Ok(result.map_err(|error| error.message))
    }

    async fn execute_local_tool_call(
        &self,
        call: &ToolCall,
        tool_calls: &mut u32,
        context: &CallableExecutionContext<'_>,
    ) -> Result<Result<ToolOutput, String>, AgentError> {
        consume_budget(
            context.run,
            Usage {
                tool_calls: 1,
                ..Usage::default()
            },
            context.caused_by,
        )?;
        emit_usage(context.observer, context.run).await;
        *tool_calls = tool_calls
            .checked_add(1)
            .ok_or_else(|| AgentError::Protocol("local tool-call counter overflow".into()))?;
        record_callable(
            context.run,
            "tool.started",
            &self.name,
            "tool",
            call,
            context.caused_by,
        )?;
        emit_agent_event(
            context.observer,
            AgentStreamEvent::CallableStarted {
                turn: context.turn,
                kind: CallableKind::Tool,
                call: call.clone(),
            },
        )
        .await;
        let result = match self
            .execute_tool_effect(call, context.run, context.effect_key)
            .await
        {
            Ok(result) => result,
            Err(error) => {
                record_callable(
                    context.run,
                    "tool.failed",
                    &self.name,
                    "tool",
                    call,
                    context.caused_by,
                )?;
                return Err(error);
            }
        };
        let succeeded = matches!(&result, Ok(output) if !output.is_error);
        record_tool_outcome(
            context.run,
            if succeeded {
                "tool.completed"
            } else {
                "tool.failed"
            },
            &self.name,
            call,
            &result,
            context.caused_by,
        )?;
        emit_agent_event(
            context.observer,
            AgentStreamEvent::CallableCompleted {
                turn: context.turn,
                kind: CallableKind::Tool,
                call_id: call.id.clone(),
                name: call.name.clone(),
                success: succeeded,
            },
        )
        .await;
        Ok(result.map_err(|error| error.message))
    }

    async fn execute_agent_effect(
        &self,
        call: &ToolCall,
        run: &RunContext,
        effect_key: &str,
    ) -> Result<Result<ToolOutput, GatewayError>, AgentError> {
        let input = delegation_input(call)?;
        let descriptor = self.agents.descriptor(&call.name).ok_or_else(|| {
            GatewayError::new(
                GatewayErrorKind::NotFound,
                format!("agent route `{}` is not registered", call.name),
            )
        })?;
        if !run.capabilities().contains(descriptor.id) {
            return Err(GatewayError::new(
                GatewayErrorKind::CapabilityDenied,
                format!("run is not granted agent capability `{}`", call.name),
            )
            .into());
        }
        let handler = AgentEffectHandler {
            gateway: self.agents.clone(),
            route: call.name.clone(),
            input: input.into(),
            run: run.clone(),
        };
        let outcome = self
            .effects
            .execute(
                effect_request(
                    EffectKind::Agent,
                    descriptor.id,
                    descriptor.capability().effect,
                    call.arguments.clone(),
                    effect_key,
                ),
                run,
                &handler,
                self.effect_recovery,
            )
            .await?;
        let result: DelegationEffectResult = serde_json::from_value(outcome.output)
            .map_err(|error| AgentError::Protocol(error.to_string()))?;
        let result = result.into_result();
        match &result {
            Err(error) if !recoverable_gateway(error) => Err(error.clone().into()),
            _ => Ok(result),
        }
    }

    async fn execute_tool_effect(
        &self,
        call: &ToolCall,
        run: &RunContext,
        effect_key: &str,
    ) -> Result<Result<ToolOutput, ToolError>, AgentError> {
        let Some(descriptor) = self.tools.descriptor(&call.name).cloned() else {
            return self.execute_tool(call, run).await;
        };
        if !run.capabilities().contains(descriptor.id) {
            return self.apply_tool_policy(
                Err(ToolError::local(
                    ToolErrorKind::CapabilityDenied,
                    format!("run is not granted tool capability `{}`", call.name),
                )),
                &call.name,
            );
        }
        let handler = ToolEffectHandler {
            tools: self.tools.clone(),
            tool: call.name.clone(),
            run: run.clone(),
        };
        let outcome = self
            .effects
            .execute(
                effect_request(
                    EffectKind::Tool,
                    descriptor.id,
                    descriptor.effect,
                    call.arguments.clone(),
                    effect_key,
                ),
                run,
                &handler,
                self.effect_recovery,
            )
            .await?;
        let result: ToolEffectResult = serde_json::from_value(outcome.output)
            .map_err(|error| AgentError::Protocol(error.to_string()))?;
        self.apply_tool_policy(result.into_result(), &call.name)
    }

    async fn execute_tool(
        &self,
        call: &ToolCall,
        run: &RunContext,
    ) -> Result<Result<ToolOutput, ToolError>, AgentError> {
        let result = self
            .tools
            .invoke(&call.name, call.arguments.clone(), run)
            .await;
        self.apply_tool_policy(result, &call.name)
    }

    fn apply_tool_policy(
        &self,
        result: Result<ToolOutput, ToolError>,
        tool: &str,
    ) -> Result<Result<ToolOutput, ToolError>, AgentError> {
        match (&result, self.config.tool_error_policy) {
            (Err(error), ToolErrorPolicy::FailFast) => Err(error.clone().into()),
            (Err(error), ToolErrorPolicy::ReturnToModel) if !recoverable(error) => {
                Err(error.clone().into())
            }
            (Ok(output), _) if !output.model_visible => {
                Err(AgentError::ToolOutputNotVisible { tool: tool.into() })
            }
            _ => Ok(result),
        }
    }
}

struct CallableExecutionContext<'a> {
    run: &'a RunContext,
    caused_by: Option<EventId>,
    effect_key: &'a str,
    turn: u32,
    observer: &'a dyn AgentObserver,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum ToolEffectResult {
    Success { output: ToolOutput },
    Failure { error: ToolError },
}

impl ToolEffectResult {
    fn into_result(self) -> Result<ToolOutput, ToolError> {
        match self {
            Self::Success { output } => Ok(output),
            Self::Failure { error } => Err(error),
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum DelegationEffectResult {
    Success { output: ToolOutput },
    Failure { error: GatewayError },
}

impl DelegationEffectResult {
    fn into_result(self) -> Result<ToolOutput, GatewayError> {
        match self {
            Self::Success { output } => Ok(output),
            Self::Failure { error } => Err(error),
        }
    }
}

struct ToolEffectHandler {
    tools: ToolRegistry,
    tool: String,
    run: RunContext,
}

impl EffectHandler for ToolEffectHandler {
    fn execute(
        &self,
        request: &EffectRequest,
        _context: EffectExecutionContext,
    ) -> EffectFuture<'_, Result<serde_json::Value, RunError>> {
        let tools = self.tools.clone();
        let tool = self.tool.clone();
        let run = self.run.clone();
        let input = request.input.clone();
        Box::pin(async move {
            let result = match tools.invoke(&tool, input, &run).await {
                Ok(output) => ToolEffectResult::Success { output },
                Err(error) => ToolEffectResult::Failure { error },
            };
            serde_json::to_value(result).map_err(|error| protocol_run_error(&error))
        })
    }
}

struct AgentEffectHandler {
    gateway: AgentGateway,
    route: String,
    input: String,
    run: RunContext,
}

impl EffectHandler for AgentEffectHandler {
    fn execute(
        &self,
        _request: &EffectRequest,
        _context: EffectExecutionContext,
    ) -> EffectFuture<'_, Result<serde_json::Value, RunError>> {
        let gateway = self.gateway.clone();
        let route = self.route.clone();
        let input = self.input.clone();
        let run = self.run.clone();
        Box::pin(async move {
            let result = match gateway.delegate(&route, input, &run).await {
                Ok(outcome) => DelegationEffectResult::Success {
                    output: ToolOutput::model_visible(serde_json::json!({
                        "agent": route,
                        "content": outcome.response.content,
                        "turns": outcome.turns,
                        "tool_calls": outcome.tool_calls,
                        "delegations": outcome.delegations,
                    })),
                },
                Err(error) => DelegationEffectResult::Failure { error },
            };
            serde_json::to_value(result).map_err(|error| protocol_run_error(&error))
        })
    }
}

fn effect_request(
    kind: EffectKind,
    capability_id: runifold_core::CapabilityId,
    effect_class: runifold_core::EffectClass,
    input: serde_json::Value,
    idempotency_key: &str,
) -> EffectRequest {
    EffectRequest {
        effect_id: EffectId::new(),
        invocation_id: InvocationId::new(),
        kind,
        capability_id,
        input,
        effect_class,
        idempotency_key: Some(idempotency_key.into()),
    }
}

fn delegation_input(call: &ToolCall) -> Result<&str, AgentError> {
    call.arguments
        .as_object()
        .and_then(|arguments| arguments.get("input"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            GatewayError::new(
                GatewayErrorKind::InvalidInput,
                format!(
                    "agent `{}` requires an object with a string `input` field",
                    call.name
                ),
            )
            .into()
        })
}

fn protocol_run_error(error: &serde_json::Error) -> RunError {
    RunError {
        kind: RunErrorKind::Protocol,
        message: error.to_string(),
        retry_safety: RetrySafety::Unknown,
        metadata: BTreeMap::new(),
    }
}

fn recoverable(error: &ToolError) -> bool {
    matches!(
        error.kind,
        ToolErrorKind::NotFound
            | ToolErrorKind::InvalidInput
            | ToolErrorKind::Execution
            | ToolErrorKind::InvalidOutput
    )
}

fn recoverable_gateway(error: &GatewayError) -> bool {
    matches!(
        error.kind,
        GatewayErrorKind::NotFound | GatewayErrorKind::InvalidInput | GatewayErrorKind::ChildFailed
    )
}

fn tool_result_message(
    call_id: String,
    name: String,
    result: Result<ToolOutput, String>,
    execution_id: &str,
) -> Result<Message, AgentError> {
    let (content, structured_content, metadata, is_error) = match result {
        Ok(output) => (
            output.content,
            output.structured_content,
            output.metadata,
            output.is_error,
        ),
        Err(message) => (
            vec![ContentPart::text(message)],
            None,
            BTreeMap::new(),
            true,
        ),
    };
    let mut message = Message::new(
        Role::Tool,
        vec![ContentPart::ToolResult(ToolResult {
            call_id,
            name: Some(name),
            content,
            structured_content,
            is_error,
            metadata,
        })],
    )
    .map_err(|error| AgentError::Protocol(error.to_string()))?;
    message.metadata.insert(
        TOOL_RESULT_EXECUTION_ID_METADATA.into(),
        serde_json::Value::String(execution_id.into()),
    );
    Ok(message)
}

#[cfg(test)]
mod rich_result_tests {
    use runifold_model::{MediaSource, ToolResult};
    use serde_json::json;

    use super::*;

    #[test]
    fn agent_preserves_rich_and_structured_tool_output() {
        let output = ToolOutput::rich(vec![ContentPart::Image {
            source: MediaSource::Base64 {
                media_type: "image/png".into(),
                data: "cG5n".into(),
            },
        }])
        .with_structured_content(json!({"width":1}));
        let message =
            tool_result_message("call-1".into(), "render".into(), Ok(output), "execution-1")
                .unwrap();

        let ContentPart::ToolResult(ToolResult {
            content,
            structured_content,
            ..
        }) = &message.content[0]
        else {
            panic!("Agent must create a canonical ToolResult");
        };
        assert!(matches!(content[0], ContentPart::Image { .. }));
        assert_eq!(structured_content, &Some(json!({"width":1})));
    }
}
