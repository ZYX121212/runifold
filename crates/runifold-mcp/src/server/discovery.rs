use super::{
    BTreeMap, CacheHint, CacheOperation, Collection, CompleteParams, DiscoverMetadata,
    DiscoverParams, DiscoverResult, INVALID_PARAMS, InitializeParams, InitializeResult,
    JsonRpcResponse, LATEST_PROTOCOL_VERSION, LIFECYCLE_ERROR, ListPromptsParams,
    ListPromptsResult, ListResourceTemplatesParams, ListResourceTemplatesResult,
    ListResourcesParams, ListResourcesResult, ListToolsParams, ListToolsResult, METHOD_NOT_FOUND,
    McpResultType, McpSession, McpTool, PromptsCapability, ReadResourceParams, RequestEra,
    RequestId, ResourceDescriptorKind, ResourceSubscriptionParams, ResourcesCapability,
    STATELESS_PROTOCOL_VERSION, SUPPORTED_PROTOCOL_VERSIONS, ServerCapabilities, SessionState,
    TASKS_EXTENSION_ID, ToolsCapability, Value, completion_error_response, decode_optional_params,
    decode_params, json, pagination, resource_error_response, serialize_result,
};

impl McpSession {
    pub(super) fn discover(&self, id: RequestId, params: Option<Value>) -> JsonRpcResponse {
        let params: DiscoverParams = match decode_params(params) {
            Ok(params) => params,
            Err(error) => {
                return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
            }
        };
        if params.metadata.protocol_version != STATELESS_PROTOCOL_VERSION {
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "unsupported discovery protocol version",
                Some(json!({
                    "requested": params.metadata.protocol_version,
                    "discoveryVersion": STATELESS_PROTOCOL_VERSION,
                })),
            );
        }
        let hint = self
            .inner
            .server
            .cache_hints
            .get(&CacheOperation::ServerDiscover)
            .copied()
            .unwrap_or_else(CacheHint::no_store);
        let result = DiscoverResult {
            result_type: McpResultType::Complete,
            supported_versions: SUPPORTED_PROTOCOL_VERSIONS
                .iter()
                .map(ToString::to_string)
                .collect(),
            capabilities: self.server_capabilities(RequestEra::Stateless),
            metadata: DiscoverMetadata {
                server_info: self.inner.server.implementation.clone(),
            },
            instructions: self.inner.server.instructions.clone(),
            ttl_ms: Some(hint.ttl_ms),
            cache_scope: Some(hint.cache_scope),
        };
        serialize_result(id, &result)
    }

    /// Handles one JSON-RPC notification.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when the notification violates lifecycle or has
    /// malformed parameters.
    pub(super) fn initialize(&self, id: RequestId, params: Option<Value>) -> JsonRpcResponse {
        {
            let mut state = self.lock_state();
            if !matches!(*state, SessionState::Created) {
                return JsonRpcResponse::error(
                    id,
                    LIFECYCLE_ERROR,
                    "initialize must be the first request",
                    None,
                );
            }
            *state = SessionState::Initializing;
        }
        let params: InitializeParams = match decode_params(params) {
            Ok(params) => params,
            Err(error) => {
                *self.lock_state() = SessionState::Created;
                return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
            }
        };
        if params.protocol_version != LATEST_PROTOCOL_VERSION {
            *self.lock_state() = SessionState::Created;
            return JsonRpcResponse::error(
                id,
                INVALID_PARAMS,
                "unsupported protocol version",
                Some(json!({
                    "requested": params.protocol_version,
                    "supported": [LATEST_PROTOCOL_VERSION],
                })),
            );
        }
        *self.lock_client_capabilities() = Some(params.capabilities);
        *self.lock_state() = SessionState::AwaitingInitialized;
        let result = InitializeResult {
            protocol_version: LATEST_PROTOCOL_VERSION.into(),
            capabilities: self.server_capabilities(RequestEra::Legacy),
            server_info: self.inner.server.implementation.clone(),
            instructions: self.inner.server.instructions.clone(),
        };
        serialize_result(id, &result)
    }

    pub(super) fn server_capabilities(&self, era: RequestEra) -> ServerCapabilities {
        let extensions = if era == RequestEra::Stateless && self.inner.server.task_backend.is_some()
        {
            BTreeMap::from([(TASKS_EXTENSION_ID.into(), json!({}))])
        } else {
            BTreeMap::new()
        };
        ServerCapabilities {
            tools: Some(ToolsCapability { list_changed: true }),
            resources: self
                .inner
                .server
                .resources
                .as_ref()
                .map(|_| ResourcesCapability {
                    subscribe: era == RequestEra::Legacy,
                    list_changed: true,
                }),
            prompts: self
                .inner
                .server
                .prompts
                .as_ref()
                .map(|_| PromptsCapability { list_changed: true }),
            completions: self
                .inner
                .server
                .completions
                .as_ref()
                .filter(|registry| !registry.is_empty())
                .map(|_| BTreeMap::new()),
            extensions,
            ..ServerCapabilities::default()
        }
    }

    pub(super) fn list_tools(
        &self,
        id: RequestId,
        params: Option<Value>,
        era: RequestEra,
    ) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id, era) {
            return response;
        }
        let params = match params {
            Some(value) => match serde_json::from_value::<ListToolsParams>(value) {
                Ok(params) => params,
                Err(error) => {
                    return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
                }
            },
            None => ListToolsParams::default(),
        };
        let tools = self
            .inner
            .server
            .registry
            .model_specs()
            .into_iter()
            .filter(|spec| {
                self.inner
                    .server
                    .registry
                    .descriptor(&spec.name)
                    .is_some_and(|descriptor| {
                        self.inner
                            .server
                            .authority
                            .capabilities()
                            .contains(descriptor.id)
                    })
            })
            .map(|spec| McpTool {
                name: spec.name,
                title: None,
                description: Some(spec.description),
                input_schema: spec.input_schema,
                output_schema: spec.output_schema,
                annotations: None,
            })
            .collect::<Vec<_>>();
        let (tools, next_cursor) = match pagination::page(
            tools,
            params.cursor.as_deref(),
            self.cursor_namespace(era),
            Collection::Tools,
            self.inner.server.page_size,
        ) {
            Ok(page) => page,
            Err(error) => {
                return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
            }
        };
        let (ttl_ms, cache_scope) = self.cache_fields(era, CacheOperation::ToolsList);
        serialize_result(
            id,
            &ListToolsResult {
                tools,
                next_cursor,
                ttl_ms,
                cache_scope,
            },
        )
    }

    pub(super) fn list_resources(
        &self,
        id: RequestId,
        params: Option<Value>,
        era: RequestEra,
    ) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id, era) {
            return response;
        }
        let Some(resources) = &self.inner.server.resources else {
            return JsonRpcResponse::error(id, METHOD_NOT_FOUND, "method not found", None);
        };
        let params = match params {
            Some(value) => match serde_json::from_value::<ListResourcesParams>(value) {
                Ok(params) => params,
                Err(error) => {
                    return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
                }
            },
            None => ListResourcesParams::default(),
        };
        let (resources, next_cursor) = match pagination::page(
            resources.list_authorized(&self.inner.server.authority),
            params.cursor.as_deref(),
            self.cursor_namespace(era),
            Collection::Resources,
            self.inner.server.page_size,
        ) {
            Ok(page) => page,
            Err(error) => {
                return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
            }
        };
        serialize_result(
            id,
            &ListResourcesResult {
                resources,
                next_cursor,
                ttl_ms: self.cache_fields(era, CacheOperation::ResourcesList).0,
                cache_scope: self.cache_fields(era, CacheOperation::ResourcesList).1,
            },
        )
    }

    pub(super) fn list_resource_templates(
        &self,
        id: RequestId,
        params: Option<Value>,
        era: RequestEra,
    ) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id, era) {
            return response;
        }
        let Some(resources) = &self.inner.server.resources else {
            return JsonRpcResponse::error(id, METHOD_NOT_FOUND, "method not found", None);
        };
        let params = match decode_optional_params::<ListResourceTemplatesParams>(params) {
            Ok(params) => params,
            Err(error) => {
                return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
            }
        };
        let (resource_templates, next_cursor) = match pagination::page(
            resources.list_templates_authorized(&self.inner.server.authority),
            params.cursor.as_deref(),
            self.cursor_namespace(era),
            Collection::ResourceTemplates,
            self.inner.server.page_size,
        ) {
            Ok(page) => page,
            Err(error) => {
                return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
            }
        };
        serialize_result(
            id,
            &ListResourceTemplatesResult {
                resource_templates,
                next_cursor,
                ttl_ms: self
                    .cache_fields(era, CacheOperation::ResourceTemplatesList)
                    .0,
                cache_scope: self
                    .cache_fields(era, CacheOperation::ResourceTemplatesList)
                    .1,
            },
        )
    }

    pub(super) async fn read_resource(
        &self,
        id: RequestId,
        params: Option<Value>,
        era: RequestEra,
    ) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id, era) {
            return response;
        }
        let Some(resources) = &self.inner.server.resources else {
            return JsonRpcResponse::error(id, METHOD_NOT_FOUND, "method not found", None);
        };
        let params: ReadResourceParams = match decode_params(params) {
            Ok(params) => params,
            Err(error) => {
                return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
            }
        };
        let scope = resources
            .descriptor(&params.uri)
            .map(ResourceDescriptorKind::Exact)
            .or_else(|| {
                resources
                    .template_descriptor_for_uri(&params.uri)
                    .map(ResourceDescriptorKind::Template)
            })
            .and_then(|descriptor| self.scoped_request(&id, &descriptor.capability()));
        let authority = scope
            .as_ref()
            .map_or(&self.inner.server.authority, |(context, _guard)| context);
        match resources.read(&params.uri, authority).await {
            Ok(mut result) => {
                (result.ttl_ms, result.cache_scope) =
                    self.cache_fields(era, CacheOperation::ResourceRead);
                serialize_result(id, &result)
            }
            Err(error) => resource_error_response(id, &error),
        }
    }

    pub(super) fn subscribe_resource(
        &self,
        id: RequestId,
        params: Option<Value>,
        era: RequestEra,
    ) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id, era) {
            return response;
        }
        let Some(resources) = &self.inner.server.resources else {
            return JsonRpcResponse::error(id, METHOD_NOT_FOUND, "method not found", None);
        };
        let params: ResourceSubscriptionParams = match decode_params(params) {
            Ok(params) => params,
            Err(error) => {
                return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
            }
        };
        if !resources.contains_authorized_uri(&params.uri, &self.inner.server.authority) {
            return JsonRpcResponse::error(id, METHOD_NOT_FOUND, "resource not found", None);
        }
        self.lock_subscriptions().insert(params.uri);
        JsonRpcResponse::success(id, json!({}))
    }

    pub(super) fn unsubscribe_resource(
        &self,
        id: RequestId,
        params: Option<Value>,
        era: RequestEra,
    ) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id, era) {
            return response;
        }
        if self.inner.server.resources.is_none() {
            return JsonRpcResponse::error(id, METHOD_NOT_FOUND, "method not found", None);
        }
        let params: ResourceSubscriptionParams = match decode_params(params) {
            Ok(params) => params,
            Err(error) => {
                return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
            }
        };
        self.lock_subscriptions().remove(&params.uri);
        JsonRpcResponse::success(id, json!({}))
    }

    pub(super) fn list_prompts(
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
        let params = match params {
            Some(value) => match serde_json::from_value::<ListPromptsParams>(value) {
                Ok(params) => params,
                Err(error) => {
                    return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
                }
            },
            None => ListPromptsParams::default(),
        };
        let (prompts, next_cursor) = match pagination::page(
            prompts.list_authorized(&self.inner.server.authority),
            params.cursor.as_deref(),
            self.cursor_namespace(era),
            Collection::Prompts,
            self.inner.server.page_size,
        ) {
            Ok(page) => page,
            Err(error) => {
                return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
            }
        };
        serialize_result(
            id,
            &ListPromptsResult {
                prompts,
                next_cursor,
                ttl_ms: self.cache_fields(era, CacheOperation::PromptsList).0,
                cache_scope: self.cache_fields(era, CacheOperation::PromptsList).1,
            },
        )
    }

    pub(super) async fn complete(
        &self,
        id: RequestId,
        params: Option<Value>,
        era: RequestEra,
    ) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id, era) {
            return response;
        }
        let Some(completions) = &self.inner.server.completions else {
            return JsonRpcResponse::error(id, METHOD_NOT_FOUND, "method not found", None);
        };
        let params: CompleteParams = match decode_params(params) {
            Ok(params) => params,
            Err(error) => {
                return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
            }
        };
        if !self.valid_completion_reference(&params) {
            return JsonRpcResponse::error(id, METHOD_NOT_FOUND, "completion not found", None);
        }
        let scope = completions
            .descriptor(&params.reference)
            .and_then(|descriptor| self.scoped_request(&id, &descriptor.capability()));
        let authority = scope
            .as_ref()
            .map_or(&self.inner.server.authority, |(context, _guard)| context);
        match completions.complete(params, authority).await {
            Ok(result) => serialize_result(id, &result),
            Err(error) => completion_error_response(id, error),
        }
    }

    fn valid_completion_reference(&self, params: &CompleteParams) -> bool {
        match &params.reference {
            crate::CompletionReference::Prompt { name } => self
                .inner
                .server
                .prompts
                .as_ref()
                .and_then(|prompts| prompts.descriptor(name))
                .is_some_and(|descriptor| {
                    descriptor
                        .prompt
                        .arguments
                        .iter()
                        .any(|argument| argument.name == params.argument.name)
                }),
            crate::CompletionReference::Resource { uri } => self
                .inner
                .server
                .resources
                .as_ref()
                .is_some_and(|resources| {
                    resources.template_has_variable(uri, &params.argument.name)
                }),
        }
    }
}
