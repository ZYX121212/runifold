use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use runifold_core::{
    CancellationToken, CapabilityDescriptor, CapabilitySet, DomainEvent, RunContext, RunEventKind,
    RunId,
};
use runifold_tool::ToolRegistry;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::sync::{Notify, broadcast};

use crate::{
    CallToolParams, ClientCapabilities, CompleteParams, CompletionRegistry, CreateMessageParams,
    GetPromptParams, Implementation, IncludeContext, InitializeParams, InitializeResult,
    JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, LATEST_PROTOCOL_VERSION,
    ListPromptsParams, ListPromptsResult, ListResourceTemplatesParams, ListResourceTemplatesResult,
    ListResourcesParams, ListResourcesResult, ListToolsParams, ListToolsResult, McpError,
    McpSamplingClient, McpTool, McpTransport, PeerRequestHandler, PromptRegistry,
    PromptsCapability, ReadResourceParams, RequestId, ResourceRegistry, ResourceSubscriptionParams,
    ResourcesCapability, ServerCapabilities, ServerNotificationStream, ToolsCapability,
    TransportFuture,
    pagination::{self, Collection},
    server_response::{
        completion_error_response, prompt_error_response, resource_error_response,
        serialize_result, tool_invocation_response,
    },
    transport::ClientPeerTransport,
};

const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
const INTERNAL_ERROR: i64 = -32603;
const LIFECYCLE_ERROR: i64 = -32002;
const DEFAULT_PAGE_SIZE: usize = 50;

/// Capability-safe MCP Tool server.
#[derive(Clone, Debug)]
pub struct McpServer {
    registry: Arc<ToolRegistry>,
    authority: RunContext,
    implementation: Implementation,
    instructions: Option<String>,
    resources: Option<Arc<ResourceRegistry>>,
    prompts: Option<Arc<PromptRegistry>>,
    completions: Option<Arc<CompletionRegistry>>,
    page_size: usize,
}

impl McpServer {
    /// Creates a server over an existing Tool registry and explicit authority.
    ///
    /// Only tools whose capabilities are granted to `authority` are listed or
    /// callable.
    pub fn new(
        registry: Arc<ToolRegistry>,
        authority: RunContext,
        implementation: Implementation,
    ) -> Self {
        Self {
            registry,
            authority,
            implementation,
            instructions: None,
            resources: None,
            prompts: None,
            completions: None,
            page_size: DEFAULT_PAGE_SIZE,
        }
    }

    /// Adds optional instructions returned during initialization.
    #[must_use]
    pub fn with_instructions(mut self, instructions: impl Into<String>) -> Self {
        self.instructions = Some(instructions.into());
        self
    }

    /// Exposes a capability-gated resource registry.
    #[must_use]
    pub fn with_resource_registry(mut self, resources: Arc<ResourceRegistry>) -> Self {
        self.resources = Some(resources);
        self
    }

    /// Exposes a capability-gated prompt registry.
    #[must_use]
    pub fn with_prompt_registry(mut self, prompts: Arc<PromptRegistry>) -> Self {
        self.prompts = Some(prompts);
        self
    }

    /// Exposes capability-gated prompt and resource-template completion.
    #[must_use]
    pub fn with_completion_registry(mut self, completions: Arc<CompletionRegistry>) -> Self {
        self.completions = Some(completions);
        self
    }

    /// Selects the server-controlled list page size.
    #[must_use]
    pub const fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = if page_size == 0 { 1 } else { page_size };
        self
    }

    /// Creates one isolated MCP connection lifecycle.
    pub fn session(&self) -> McpSession {
        let (notifications, _) = broadcast::channel(256);
        McpSession {
            inner: Arc::new(SessionInner {
                server: self.clone(),
                state: Mutex::new(SessionState::Created),
                inflight: Arc::new(Mutex::new(HashMap::new())),
                cursor_namespace: RunId::new().to_string(),
                subscriptions: Mutex::new(HashSet::new()),
                notifications,
                client_capabilities: Mutex::new(None),
                client_peer: Mutex::new(None),
                active: Notify::new(),
            }),
        }
    }
}

/// One stateful MCP connection.
#[derive(Clone, Debug)]
pub struct McpSession {
    inner: Arc<SessionInner>,
}

impl McpSession {
    /// Handles one JSON-RPC request.
    pub async fn handle_request(&self, request: JsonRpcRequest) -> JsonRpcResponse {
        let id = request.id.clone();
        if request.jsonrpc != "2.0" {
            return JsonRpcResponse::error(id, INVALID_REQUEST, "jsonrpc must be `2.0`", None);
        }
        match request.method.as_str() {
            "initialize" => self.initialize(id, request.params),
            "ping" => JsonRpcResponse::success(id, json!({})),
            "tools/list" => self.list_tools(id, request.params),
            "tools/call" => self.call_tool(id, request.params).await,
            "resources/list" => self.list_resources(id, request.params),
            "resources/templates/list" => self.list_resource_templates(id, request.params),
            "resources/read" => self.read_resource(id, request.params).await,
            "resources/subscribe" => self.subscribe_resource(id, request.params),
            "resources/unsubscribe" => self.unsubscribe_resource(id, request.params),
            "prompts/list" => self.list_prompts(id, request.params),
            "prompts/get" => self.get_prompt(id, request.params).await,
            "completion/complete" => self.complete(id, request.params).await,
            _ => JsonRpcResponse::error(id, METHOD_NOT_FOUND, "method not found", None),
        }
    }

    /// Handles one JSON-RPC notification.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when the notification violates lifecycle or has
    /// malformed parameters.
    pub fn handle_notification(&self, notification: JsonRpcNotification) -> Result<(), McpError> {
        if notification.jsonrpc != "2.0" {
            return Err(McpError::protocol("jsonrpc must be `2.0`"));
        }
        match notification.method.as_str() {
            "notifications/initialized" => {
                let mut state = self.lock_state();
                if !matches!(*state, SessionState::AwaitingInitialized) {
                    return Err(McpError::lifecycle(
                        "initialized notification arrived outside initialization",
                    ));
                }
                *state = SessionState::Active;
                self.inner.active.notify_waiters();
                Ok(())
            }
            "notifications/cancelled" => {
                let params: CancelledParams = decode_params(notification.params)?;
                if let Some(token) = self.lock_inflight().get(&params.request_id) {
                    token.cancel();
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn initialize(&self, id: RequestId, params: Option<Value>) -> JsonRpcResponse {
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
            capabilities: ServerCapabilities {
                tools: Some(ToolsCapability::default()),
                resources: self
                    .inner
                    .server
                    .resources
                    .as_ref()
                    .map(|_| ResourcesCapability {
                        subscribe: true,
                        list_changed: true,
                    }),
                prompts: self
                    .inner
                    .server
                    .prompts
                    .as_ref()
                    .map(|_| PromptsCapability::default()),
                completions: self
                    .inner
                    .server
                    .completions
                    .as_ref()
                    .filter(|registry| !registry.is_empty())
                    .map(|_| BTreeMap::new()),
                ..ServerCapabilities::default()
            },
            server_info: self.inner.server.implementation.clone(),
            instructions: self.inner.server.instructions.clone(),
        };
        serialize_result(id, &result)
    }

    fn list_tools(&self, id: RequestId, params: Option<Value>) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id) {
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
            &self.inner.cursor_namespace,
            Collection::Tools,
            self.inner.server.page_size,
        ) {
            Ok(page) => page,
            Err(error) => {
                return JsonRpcResponse::error(id, INVALID_PARAMS, error.to_string(), None);
            }
        };
        serialize_result(id, &ListToolsResult { tools, next_cursor })
    }

    fn list_resources(&self, id: RequestId, params: Option<Value>) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id) {
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
            &self.inner.cursor_namespace,
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
            },
        )
    }

    fn list_resource_templates(&self, id: RequestId, params: Option<Value>) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id) {
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
            &self.inner.cursor_namespace,
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
            },
        )
    }

    async fn read_resource(&self, id: RequestId, params: Option<Value>) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id) {
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
            Ok(result) => serialize_result(id, &result),
            Err(error) => resource_error_response(id, &error),
        }
    }

    fn subscribe_resource(&self, id: RequestId, params: Option<Value>) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id) {
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

    fn unsubscribe_resource(&self, id: RequestId, params: Option<Value>) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id) {
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

    fn list_prompts(&self, id: RequestId, params: Option<Value>) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id) {
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
            &self.inner.cursor_namespace,
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
            },
        )
    }

    async fn complete(&self, id: RequestId, params: Option<Value>) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id) {
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

    /// Opens this session's server-to-client notification stream.
    pub fn subscribe_notifications(&self) -> ServerNotificationStream {
        let mut receiver = self.inner.notifications.subscribe();
        Box::pin(async_stream::stream! {
            loop {
                match receiver.recv().await {
                    Ok(notification) => yield Ok(notification),
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        yield Err(McpError::protocol(format!(
                            "MCP notification receiver lagged by {skipped} messages"
                        )));
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }

    /// Emits a resource update only when this session subscribed to `uri`.
    pub fn notify_resource_updated(&self, uri: &str) -> bool {
        if !self.lock_subscriptions().contains(uri) {
            return false;
        }
        self.inner
            .notifications
            .send(JsonRpcNotification::new(
                "notifications/resources/updated",
                Some(json!({"uri": uri})),
            ))
            .is_ok()
    }

    pub(crate) fn is_resource_subscribed(&self, uri: &str) -> bool {
        self.lock_subscriptions().contains(uri)
    }

    /// Emits a resource-list change notification.
    pub fn notify_resource_list_changed(&self) -> bool {
        self.inner
            .notifications
            .send(JsonRpcNotification::new(
                "notifications/resources/list_changed",
                None,
            ))
            .is_ok()
    }

    async fn get_prompt(&self, id: RequestId, params: Option<Value>) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id) {
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

    async fn call_tool(&self, id: RequestId, params: Option<Value>) -> JsonRpcResponse {
        if let Some(response) = self.require_active(&id) {
            return response;
        }
        let params: CallToolParams = match decode_params(params) {
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
        let child = self.inner.server.authority.child(capabilities);
        let cancellation = child.cancellation().clone();
        self.lock_inflight().insert(id.clone(), cancellation);
        let _guard = InflightGuard::new(id.clone(), Arc::clone(&self.inner.inflight));
        let input = Value::Object(params.arguments.unwrap_or_default());
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

    fn require_active(&self, id: &RequestId) -> Option<JsonRpcResponse> {
        (!matches!(*self.lock_state(), SessionState::Active)).then(|| {
            JsonRpcResponse::error(
                id.clone(),
                LIFECYCLE_ERROR,
                "session is not initialized",
                None,
            )
        })
    }

    fn scoped_request(
        &self,
        id: &RequestId,
        capability: &CapabilityDescriptor,
    ) -> Option<(RunContext, InflightGuard)> {
        if !self
            .inner
            .server
            .authority
            .capabilities()
            .contains(capability.id)
        {
            return None;
        }
        let mut capabilities = CapabilitySet::new();
        capabilities.grant(capability.clone());
        let child = self.inner.server.authority.child(capabilities);
        self.lock_inflight()
            .insert(id.clone(), child.cancellation().clone());
        let guard = InflightGuard::new(id.clone(), Arc::clone(&self.inner.inflight));
        Some((child, guard))
    }

    fn lock_state(&self) -> MutexGuard<'_, SessionState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_inflight(&self) -> MutexGuard<'_, HashMap<RequestId, CancellationToken>> {
        self.inner
            .inflight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_subscriptions(&self) -> MutexGuard<'_, HashSet<String>> {
        self.inner
            .subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_client_capabilities(&self) -> MutexGuard<'_, Option<ClientCapabilities>> {
        self.inner
            .client_capabilities
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_client_peer(&self) -> MutexGuard<'_, Option<Arc<dyn ClientPeerTransport>>> {
        self.inner
            .client_peer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn install_client_peer(&self, peer: Arc<dyn ClientPeerTransport>) {
        *self.lock_client_peer() = Some(peer);
    }

    pub(crate) async fn request_peer(
        &self,
        request: JsonRpcRequest,
    ) -> Result<JsonRpcResponse, McpError> {
        let peer = self
            .lock_client_peer()
            .clone()
            .ok_or_else(|| McpError::protocol("MCP client peer is not connected"))?;
        peer.request_client(request).await
    }

    pub(crate) async fn notify_peer(
        &self,
        notification: JsonRpcNotification,
    ) -> Result<(), McpError> {
        let peer = self
            .lock_client_peer()
            .clone()
            .ok_or_else(|| McpError::protocol("MCP client peer is not connected"))?;
        peer.notify_client(notification).await
    }

    pub(crate) fn ensure_sampling_supported(
        &self,
        params: &CreateMessageParams,
    ) -> Result<(), McpError> {
        let capabilities = self.lock_client_capabilities();
        let sampling = capabilities
            .as_ref()
            .and_then(|capabilities| capabilities.sampling.as_ref())
            .ok_or_else(|| McpError::protocol("client did not negotiate Sampling"))?;
        if (!params.tools.is_empty() || params.tool_choice.is_some()) && sampling.tools.is_none() {
            return Err(McpError::protocol(
                "client did not negotiate Tool-enabled Sampling",
            ));
        }
        if !matches!(params.include_context, IncludeContext::None) && sampling.context.is_none() {
            return Err(McpError::protocol(
                "client did not negotiate Sampling context inclusion",
            ));
        }
        Ok(())
    }

    pub(crate) async fn await_active(&self, timeout: Duration) -> Result<(), McpError> {
        let notified = self.inner.active.notified();
        match *self.lock_state() {
            SessionState::Active => return Ok(()),
            SessionState::AwaitingInitialized => {}
            SessionState::Created | SessionState::Initializing => {
                return Err(McpError::lifecycle("session is not initialized"));
            }
        }
        tokio::time::timeout(timeout, notified)
            .await
            .map_err(|_| McpError::DeadlineExceeded)?;
        if matches!(*self.lock_state(), SessionState::Active) {
            Ok(())
        } else {
            Err(McpError::lifecycle("session is not initialized"))
        }
    }

    /// Creates a server-to-client Sampling requester bound to this session.
    pub fn sampling_client(&self) -> McpSamplingClient {
        McpSamplingClient::new(self.clone())
    }
}

impl McpTransport for McpSession {
    fn request(&self, request: JsonRpcRequest) -> TransportFuture<'_, JsonRpcResponse> {
        Box::pin(async move { Ok(self.handle_request(request).await) })
    }

    fn notify(&self, notification: JsonRpcNotification) -> TransportFuture<'_, ()> {
        Box::pin(async move { self.handle_notification(notification) })
    }

    fn subscribe(&self) -> TransportFuture<'_, ServerNotificationStream> {
        Box::pin(async move { Ok(self.subscribe_notifications()) })
    }

    fn install_peer_handler(&self, handler: Arc<dyn PeerRequestHandler>) -> Result<(), McpError> {
        self.install_client_peer(Arc::new(InProcessClientPeer { handler }));
        Ok(())
    }
}

struct InProcessClientPeer {
    handler: Arc<dyn PeerRequestHandler>,
}

impl ClientPeerTransport for InProcessClientPeer {
    fn request_client(&self, request: JsonRpcRequest) -> TransportFuture<'_, JsonRpcResponse> {
        self.handler.handle(request)
    }

    fn notify_client(&self, notification: JsonRpcNotification) -> TransportFuture<'_, ()> {
        Box::pin(async move { self.handler.notify(notification) })
    }
}

struct SessionInner {
    server: McpServer,
    state: Mutex<SessionState>,
    inflight: Arc<Mutex<HashMap<RequestId, CancellationToken>>>,
    cursor_namespace: String,
    subscriptions: Mutex<HashSet<String>>,
    notifications: broadcast::Sender<JsonRpcNotification>,
    client_capabilities: Mutex<Option<ClientCapabilities>>,
    client_peer: Mutex<Option<Arc<dyn ClientPeerTransport>>>,
    active: Notify,
}

impl std::fmt::Debug for SessionInner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionInner")
            .field("server", &self.server)
            .field("state", &self.state)
            .field("cursor_namespace", &self.cursor_namespace)
            .field("subscriptions", &self.subscriptions)
            .field(
                "client_peer",
                &self
                    .client_peer
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                    .map(|_| "<connected>"),
            )
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug)]
enum SessionState {
    Created,
    Initializing,
    AwaitingInitialized,
    Active,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct CancelledParams {
    request_id: RequestId,
    #[allow(dead_code)]
    reason: Option<String>,
}

struct InflightGuard {
    id: RequestId,
    inflight: Arc<Mutex<HashMap<RequestId, CancellationToken>>>,
}

impl InflightGuard {
    fn new(id: RequestId, inflight: Arc<Mutex<HashMap<RequestId, CancellationToken>>>) -> Self {
        Self { id, inflight }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        let mut inflight = self
            .inflight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(cancellation) = inflight.remove(&self.id) {
            cancellation.cancel();
        }
    }
}

fn decode_params<T: DeserializeOwned>(params: Option<Value>) -> Result<T, McpError> {
    serde_json::from_value(params.unwrap_or(Value::Null)).map_err(McpError::from)
}

fn decode_optional_params<T: DeserializeOwned + Default>(
    params: Option<Value>,
) -> Result<T, McpError> {
    params.map_or_else(
        || Ok(T::default()),
        |value| serde_json::from_value(value).map_err(Into::into),
    )
}

enum ResourceDescriptorKind<'a> {
    Exact(&'a crate::ResourceDescriptor),
    Template(&'a crate::ResourceTemplateDescriptor),
}

impl ResourceDescriptorKind<'_> {
    fn capability(&self) -> CapabilityDescriptor {
        match self {
            Self::Exact(descriptor) => descriptor.capability(),
            Self::Template(descriptor) => descriptor.capability(),
        }
    }
}

fn record_mcp_tool_event(
    context: &RunContext,
    name: &str,
    call_id: &str,
    tool: &str,
) -> Result<(), runifold_core::JournalError> {
    context.record(
        RunEventKind::Domain(DomainEvent {
            namespace: "runifold.mcp".into(),
            name: name.into(),
            payload: json!({
                "call_id": call_id,
                "tool": tool,
                "protocol_version": LATEST_PROTOCOL_VERSION,
            }),
        }),
        context.caused_by(),
    )?;
    Ok(())
}

fn request_id_label(id: &RequestId) -> String {
    match id {
        RequestId::Null => "null".into(),
        RequestId::Number(number) => number.to_string(),
        RequestId::String(value) => value.clone(),
    }
}
