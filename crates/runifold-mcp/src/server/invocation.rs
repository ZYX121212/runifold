use super::JsonRpcResponse;
use super::{
    Arc, CapabilitySet, CreateTaskResult, GetPromptParams, GetTaskResult, INTERNAL_ERROR,
    INVALID_PARAMS, InflightGuard, METHOD_NOT_FOUND, MISSING_REQUIRED_CLIENT_CAPABILITY,
    McpResultType, McpSession, McpTaskBackend, MrtrCallToolParams, MrtrToolDecision,
    MrtrToolRequest, RequestEra, RequestId, RunContext, TaskIdParams, ToolTaskRequest,
    UpdateTaskParams, Value, decode_params, json, missing_tasks_capability, prompt_error_response,
    record_mcp_tool_event, request_id_label, serialize_result, task_backend_error_response,
    task_capability_declared, task_capability_declared_in_value, tool_invocation_response,
};

impl McpSession {
    pub(super) async fn get_prompt(
        &self,
        id: RequestId,
        params: Option<Value>,
        era: RequestEra,
    ) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id, era) {
            return response;
        }
        let Some(prompts) = &self.inner.server.prompts else {
            return JsonRpcResponse::error(id, METHOD_NOT_FOUND, "method not found", None);
        };
        let params: GetPromptParams = match decode_params(params) {
            Ok(params) => params,
            Err(error) => {
                return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
            }
        };
        let scope = prompts
            .descriptor(&params.name)
            .and_then(|descriptor| self.scoped_request(&id, &descriptor.capability()));
        let authority = scope
            .as_ref()
            .map_or(&self.inner.server.authority, |(context, _guard)| context);
        match prompts
            .render(
                &params.name,
                params.arguments.unwrap_or_default(),
                authority,
            )
            .await
        {
            Ok(result) => serialize_result(id, &result),
            Err(error) => prompt_error_response(id, error),
        }
    }

    pub(super) async fn call_tool(
        &self,
        id: RequestId,
        params: Option<Value>,
        era: RequestEra,
    ) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id, era) {
            return response;
        }
        let params: MrtrCallToolParams = match decode_params(params) {
            Ok(params) => params,
            Err(error) => {
                return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
            }
        };
        let Some(descriptor) = self.inner.server.registry.descriptor(&params.name) else {
            return JsonRpcResponse::error(id, METHOD_NOT_FOUND, "tool not found", None);
        };
        if !self
            .inner
            .server
            .authority
            .capabilities()
            .contains(descriptor.id)
        {
            return JsonRpcResponse::error(id, METHOD_NOT_FOUND, "tool not found", None);
        }

        let mut capabilities = CapabilitySet::new();
        capabilities.grant(descriptor.capability());
        let child = self
            .inner
            .server
            .authority
            .child(capabilities)
            .expect("Tool capability presence was verified before child creation");
        let cancellation = child.cancellation().clone();
        self.lock_inflight().insert(id.clone(), cancellation);
        let _guard = InflightGuard::new(id.clone(), Arc::clone(&self.inner.inflight));
        if era == RequestEra::Stateless
            && let Some(response) = self.mrtr_tool_response(&id, &params, child.clone()).await
        {
            return response;
        }
        if let Some(response) = self
            .task_tool_response(&id, &params, era, child.clone())
            .await
        {
            return response;
        }
        let arguments = params.arguments.unwrap_or_default();
        let input = Value::Object(arguments);
        let call_id = request_id_label(&id);
        if record_mcp_tool_event(&child, "tool.started", &call_id, &params.name).is_err() {
            return JsonRpcResponse::error(
                id,
                INTERNAL_ERROR,
                "failed to durably record Tool start",
                None,
            );
        }
        let invocation = self
            .inner
            .server
            .registry
            .invoke(&params.name, input, &child)
            .await;
        if record_mcp_tool_event(
            &child,
            if invocation.is_ok() {
                "tool.completed"
            } else {
                "tool.failed"
            },
            &call_id,
            &params.name,
        )
        .is_err()
        {
            return JsonRpcResponse::error(
                id,
                INTERNAL_ERROR,
                "failed to durably record Tool completion",
                None,
            );
        }
        tool_invocation_response(id, invocation)
    }

    async fn task_tool_response(
        &self,
        id: &RequestId,
        params: &MrtrCallToolParams,
        era: RequestEra,
        context: RunContext,
    ) -> Option<JsonRpcResponse> {
        let backend = self
            .inner
            .server
            .task_backend
            .as_ref()
            .filter(|backend| backend.handles_tool(&params.name))?;
        if era != RequestEra::Stateless || !task_capability_declared(params.metadata.as_ref()) {
            return Some(missing_tasks_capability(id.clone()));
        }
        let task = backend
            .create_tool_task(ToolTaskRequest {
                name: params.name.clone(),
                arguments: params.arguments.clone().unwrap_or_default(),
                context,
            })
            .await
            .and_then(|task| {
                task.validate()?;
                Ok(task)
            });
        Some(match task {
            Ok(task) => serialize_result(
                id.clone(),
                &CreateTaskResult {
                    result_type: McpResultType::Task,
                    task,
                },
            ),
            Err(error) => task_backend_error_response(id.clone(), error),
        })
    }

    pub(super) async fn get_task(
        &self,
        id: RequestId,
        params: Option<Value>,
        era: RequestEra,
    ) -> JsonRpcResponse {
        let backend = match self.task_backend_for_request(&id, params.as_ref(), era) {
            Ok(backend) => backend,
            Err(response) => return response,
        };
        let params: TaskIdParams = match decode_params(params) {
            Ok(params) => params,
            Err(error) => {
                return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
            }
        };
        match backend.get(params.task_id).await.and_then(|task| {
            task.validate()?;
            Ok(task)
        }) {
            Ok(task) => serialize_result(
                id,
                &GetTaskResult {
                    result_type: McpResultType::Complete,
                    task,
                },
            ),
            Err(error) => task_backend_error_response(id, error),
        }
    }

    pub(super) async fn update_task(
        &self,
        id: RequestId,
        params: Option<Value>,
        era: RequestEra,
    ) -> JsonRpcResponse {
        let backend = match self.task_backend_for_request(&id, params.as_ref(), era) {
            Ok(backend) => backend,
            Err(response) => return response,
        };
        let params: UpdateTaskParams = match decode_params(params) {
            Ok(params) => params,
            Err(error) => {
                return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
            }
        };
        match backend.update(params.task_id, params.input_responses).await {
            Ok(()) => serialize_result(id, &json!({})),
            Err(error) => task_backend_error_response(id, error),
        }
    }

    pub(super) async fn cancel_task(
        &self,
        id: RequestId,
        params: Option<Value>,
        era: RequestEra,
    ) -> JsonRpcResponse {
        let backend = match self.task_backend_for_request(&id, params.as_ref(), era) {
            Ok(backend) => backend,
            Err(response) => return response,
        };
        let params: TaskIdParams = match decode_params(params) {
            Ok(params) => params,
            Err(error) => {
                return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
            }
        };
        match backend.cancel(params.task_id).await {
            Ok(()) => serialize_result(id, &json!({})),
            Err(error) => task_backend_error_response(id, error),
        }
    }

    fn task_backend_for_request(
        &self,
        id: &RequestId,
        params: Option<&Value>,
        era: RequestEra,
    ) -> Result<&Arc<dyn McpTaskBackend>, JsonRpcResponse> {
        if era != RequestEra::Stateless || !task_capability_declared_in_value(params) {
            return Err(missing_tasks_capability(id.clone()));
        }
        self.inner.server.task_backend.as_ref().ok_or_else(|| {
            JsonRpcResponse::error(id.clone(), METHOD_NOT_FOUND, "method not found", None)
        })
    }

    async fn mrtr_tool_response(
        &self,
        id: &RequestId,
        params: &MrtrCallToolParams,
        context: RunContext,
    ) -> Option<JsonRpcResponse> {
        let gate = self.inner.server.mrtr_tool_gates.get(&params.name)?;
        let decision = gate
            .evaluate(MrtrToolRequest {
                name: params.name.clone(),
                arguments: params.arguments.clone().unwrap_or_default(),
                input_responses: params.input_responses.clone(),
                request_state: params.request_state.clone(),
                context: context.clone(),
            })
            .await;
        let incomplete = match decision {
            Ok(MrtrToolDecision::Proceed) => return None,
            Ok(MrtrToolDecision::InputRequired(incomplete)) => incomplete,
            Err(_) => {
                return Some(JsonRpcResponse::error(
                    id.clone(),
                    INTERNAL_ERROR,
                    "MRTR Tool preflight failed",
                    None,
                ));
            }
        };
        if let Err(error) = incomplete.validate(64) {
            return Some(JsonRpcResponse::error(
                id.clone(),
                INTERNAL_ERROR,
                error.to_string(),
                None,
            ));
        }
        let client_capabilities = params
            .metadata
            .as_ref()
            .map(|metadata| &metadata.client_capabilities)
            .expect("stateless request metadata was validated before dispatch");
        let missing = incomplete.missing_capabilities(client_capabilities);
        if !missing.is_empty() {
            return Some(JsonRpcResponse::error(
                id.clone(),
                MISSING_REQUIRED_CLIENT_CAPABILITY,
                "missing required client capability",
                Some(json!({"requiredCapabilities": missing})),
            ));
        }
        if record_mcp_tool_event(
            &context,
            "tool.input_required",
            &request_id_label(id),
            &params.name,
        )
        .is_err()
        {
            return Some(JsonRpcResponse::error(
                id.clone(),
                INTERNAL_ERROR,
                "failed to durably record Tool input requirement",
                None,
            ));
        }
        Some(serialize_result(id.clone(), &incomplete))
    }
}
