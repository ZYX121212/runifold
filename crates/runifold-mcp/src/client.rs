use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use runifold_core::CancellationToken;
use runifold_tool::ToolContext;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;
use tokio::sync::Mutex;

use crate::{
    CallToolParams, CallToolResult, ClientCapabilities, CompleteParams, CompleteResult,
    CreateMessageParams, GetPromptParams, GetPromptResult, Implementation, InitializeParams,
    InitializeResult, JsonRpcNotification, JsonRpcRequest, LATEST_PROTOCOL_VERSION,
    ListPromptsParams, ListPromptsResult, ListResourceTemplatesParams, ListResourceTemplatesResult,
    ListResourcesParams, ListResourcesResult, ListToolsParams, ListToolsResult, McpError,
    McpPrompt, McpResource, McpResourceTemplate, McpTool, McpTransport, PeerRequestHandler,
    ReadResourceParams, ReadResourceResult, RequestId, ResourceSubscriptionParams,
    SamplingCapability, SamplingError, SamplingErrorKind, SamplingService,
    ServerNotificationStream, TransportFuture,
};

/// MCP client policy.
#[derive(Clone, Debug)]
pub struct McpClientConfig {
    implementation: Implementation,
    request_timeout: Duration,
    max_pagination_pages: usize,
    sampling: Option<Arc<SamplingService>>,
}

impl McpClientConfig {
    /// Creates a client policy with a 30-second request timeout.
    pub fn new(implementation: Implementation) -> Self {
        Self {
            implementation,
            request_timeout: Duration::from_secs(30),
            max_pagination_pages: 1024,
            sampling: None,
        }
    }

    /// Enables client-side basic Sampling with explicit host approval policy.
    #[must_use]
    pub fn with_sampling(mut self, sampling: Arc<SamplingService>) -> Self {
        self.sampling = Some(sampling);
        self
    }

    /// Bounds automatic list traversal and protects against cursor loops.
    #[must_use]
    pub const fn with_max_pagination_pages(mut self, pages: usize) -> Self {
        self.max_pagination_pages = if pages == 0 { 1 } else { pages };
        self
    }

    /// Replaces the maximum request duration.
    #[must_use]
    pub const fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
}

/// Stateful MCP Tools client.
#[derive(Clone)]
pub struct McpClient {
    inner: Arc<ClientInner>,
}

impl McpClient {
    /// Creates a client over a pluggable transport.
    pub fn new(transport: Arc<dyn McpTransport>, config: McpClientConfig) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                transport,
                config,
                next_id: AtomicU64::new(1),
                state: Mutex::new(ClientState::Created),
            }),
        }
    }

    /// Negotiates the finalized MCP protocol and Tool capability.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for transport, protocol, version, timeout, or
    /// lifecycle failures.
    pub async fn initialize(&self) -> Result<InitializeResult, McpError> {
        {
            let mut state = self.inner.state.lock().await;
            if !matches!(*state, ClientState::Created) {
                return Err(McpError::lifecycle(
                    "client initialization may only run once",
                ));
            }
            *state = ClientState::Initializing;
        }
        let params = InitializeParams {
            protocol_version: LATEST_PROTOCOL_VERSION.into(),
            capabilities: ClientCapabilities {
                sampling: self
                    .inner
                    .config
                    .sampling
                    .as_ref()
                    .map(|_| SamplingCapability::default()),
                ..ClientCapabilities::default()
            },
            client_info: self.inner.config.implementation.clone(),
        };
        let initialized = self
            .request_typed::<_, InitializeResult>(
                "initialize",
                &params,
                self.inner.config.request_timeout,
            )
            .await;
        let initialized = match initialized {
            Ok(initialized) => initialized,
            Err(error) => {
                *self.inner.state.lock().await = ClientState::Created;
                return Err(error);
            }
        };
        if initialized.protocol_version != LATEST_PROTOCOL_VERSION {
            *self.inner.state.lock().await = ClientState::Created;
            return Err(McpError::UnsupportedVersion {
                selected: initialized.protocol_version,
            });
        }
        if let Some(sampling) = &self.inner.config.sampling {
            let handler = Arc::new(ClientPeerHandler::new(Arc::clone(sampling)));
            if let Err(error) = self.inner.transport.install_peer_handler(handler) {
                *self.inner.state.lock().await = ClientState::Created;
                return Err(error);
            }
            if let Err(error) = self.inner.transport.start_peer().await {
                *self.inner.state.lock().await = ClientState::Created;
                return Err(error);
            }
        }
        if let Err(error) = self
            .inner
            .transport
            .notify(JsonRpcNotification::new("notifications/initialized", None))
            .await
        {
            *self.inner.state.lock().await = ClientState::Created;
            return Err(error);
        }
        *self.inner.state.lock().await = ClientState::Active {
            server: Box::new(initialized.clone()),
        };
        Ok(initialized)
    }

    /// Returns the negotiated server information.
    pub async fn server_info(&self) -> Option<InitializeResult> {
        match &*self.inner.state.lock().await {
            ClientState::Active { server } => Some((**server).clone()),
            ClientState::Created | ClientState::Initializing => None,
        }
    }

    /// Lists available remote tools.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when the client is not initialized or the peer
    /// rejects the request.
    pub async fn list_tools(&self) -> Result<Vec<McpTool>, McpError> {
        self.require_active().await?;
        let mut tools = Vec::new();
        let mut cursor = None;
        let mut seen = HashSet::new();
        for _ in 0..self.inner.config.max_pagination_pages {
            let page = self.list_tools_page(cursor).await?;
            tools.extend(page.tools);
            let Some(next) = page.next_cursor else {
                return Ok(tools);
            };
            if !seen.insert(next.clone()) {
                return Err(McpError::protocol("server repeated a pagination cursor"));
            }
            cursor = Some(next);
        }
        Err(McpError::protocol(
            "tool list exceeded the configured pagination limit",
        ))
    }

    /// Fetches one tool-list page without following its cursor.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for lifecycle, transport, cursor, or peer failures.
    pub async fn list_tools_page(
        &self,
        cursor: Option<String>,
    ) -> Result<ListToolsResult, McpError> {
        self.require_active().await?;
        self.request_typed(
            "tools/list",
            &ListToolsParams { cursor },
            self.inner.config.request_timeout,
        )
        .await
    }

    /// Lists authorized remote resources.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when resources were not negotiated, pagination is
    /// returned, or the request fails.
    pub async fn list_resources(&self) -> Result<Vec<McpResource>, McpError> {
        self.require_resources().await?;
        let mut resources = Vec::new();
        let mut cursor = None;
        let mut seen = HashSet::new();
        for _ in 0..self.inner.config.max_pagination_pages {
            let page = self.list_resources_page(cursor).await?;
            resources.extend(page.resources);
            let Some(next) = page.next_cursor else {
                return Ok(resources);
            };
            if !seen.insert(next.clone()) {
                return Err(McpError::protocol("server repeated a pagination cursor"));
            }
            cursor = Some(next);
        }
        Err(McpError::protocol(
            "resource list exceeded the configured pagination limit",
        ))
    }

    /// Fetches one resource-list page without following its cursor.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for lifecycle, transport, cursor, or peer failures.
    pub async fn list_resources_page(
        &self,
        cursor: Option<String>,
    ) -> Result<ListResourcesResult, McpError> {
        self.require_resources().await?;
        self.request_typed(
            "resources/list",
            &ListResourcesParams { cursor },
            self.inner.config.request_timeout,
        )
        .await
    }

    /// Lists all authorized resource templates across pagination.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for capability, transport, pagination, or peer failures.
    pub async fn list_resource_templates(&self) -> Result<Vec<McpResourceTemplate>, McpError> {
        self.require_resources().await?;
        let mut templates = Vec::new();
        let mut cursor = None;
        let mut seen = HashSet::new();
        for _ in 0..self.inner.config.max_pagination_pages {
            let page = self.list_resource_templates_page(cursor).await?;
            templates.extend(page.resource_templates);
            let Some(next) = page.next_cursor else {
                return Ok(templates);
            };
            if !seen.insert(next.clone()) {
                return Err(McpError::protocol("server repeated a pagination cursor"));
            }
            cursor = Some(next);
        }
        Err(McpError::protocol(
            "resource-template list exceeded the configured pagination limit",
        ))
    }

    /// Fetches one resource-template-list page without following its cursor.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for capability, transport, cursor, or peer failures.
    pub async fn list_resource_templates_page(
        &self,
        cursor: Option<String>,
    ) -> Result<ListResourceTemplatesResult, McpError> {
        self.require_resources().await?;
        self.request_typed(
            "resources/templates/list",
            &ListResourceTemplatesParams { cursor },
            self.inner.config.request_timeout,
        )
        .await
    }

    /// Reads one exact remote resource URI.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when resources were not negotiated or the peer
    /// rejects the URI.
    pub async fn read_resource(
        &self,
        uri: impl Into<String>,
    ) -> Result<ReadResourceResult, McpError> {
        self.require_resources().await?;
        self.request_typed(
            "resources/read",
            &ReadResourceParams { uri: uri.into() },
            self.inner.config.request_timeout,
        )
        .await
    }

    /// Subscribes to updates for one exact authorized resource URI.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when subscriptions were not negotiated or the peer rejects the URI.
    pub async fn subscribe_resource(&self, uri: impl Into<String>) -> Result<(), McpError> {
        self.require_resource_subscriptions().await?;
        let _: serde_json::Value = self
            .request_typed(
                "resources/subscribe",
                &ResourceSubscriptionParams { uri: uri.into() },
                self.inner.config.request_timeout,
            )
            .await?;
        Ok(())
    }

    /// Removes one resource update subscription.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when subscriptions were not negotiated or the request fails.
    pub async fn unsubscribe_resource(&self, uri: impl Into<String>) -> Result<(), McpError> {
        self.require_resource_subscriptions().await?;
        let _: serde_json::Value = self
            .request_typed(
                "resources/unsubscribe",
                &ResourceSubscriptionParams { uri: uri.into() },
                self.inner.config.request_timeout,
            )
            .await?;
        Ok(())
    }

    /// Opens the transport's server-to-client notification stream.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when the client is inactive or the transport cannot subscribe.
    pub async fn notifications(&self) -> Result<ServerNotificationStream, McpError> {
        self.require_active().await?;
        self.inner.transport.subscribe().await
    }

    /// Lists authorized remote prompts.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when prompts were not negotiated, pagination is
    /// returned, or the request fails.
    pub async fn list_prompts(&self) -> Result<Vec<McpPrompt>, McpError> {
        self.require_prompts().await?;
        let mut prompts = Vec::new();
        let mut cursor = None;
        let mut seen = HashSet::new();
        for _ in 0..self.inner.config.max_pagination_pages {
            let page = self.list_prompts_page(cursor).await?;
            prompts.extend(page.prompts);
            let Some(next) = page.next_cursor else {
                return Ok(prompts);
            };
            if !seen.insert(next.clone()) {
                return Err(McpError::protocol("server repeated a pagination cursor"));
            }
            cursor = Some(next);
        }
        Err(McpError::protocol(
            "prompt list exceeded the configured pagination limit",
        ))
    }

    /// Fetches one prompt-list page without following its cursor.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for capability, transport, cursor, or peer failures.
    pub async fn list_prompts_page(
        &self,
        cursor: Option<String>,
    ) -> Result<ListPromptsResult, McpError> {
        self.require_prompts().await?;
        self.request_typed(
            "prompts/list",
            &ListPromptsParams { cursor },
            self.inner.config.request_timeout,
        )
        .await
    }

    /// Renders one user-selected remote prompt with string arguments.
    ///
    /// This method returns protocol content only; it never injects messages
    /// into a model request automatically.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when prompts were not negotiated, arguments are
    /// invalid, or the peer rejects the request.
    pub async fn get_prompt(
        &self,
        name: impl Into<String>,
        arguments: BTreeMap<String, String>,
    ) -> Result<GetPromptResult, McpError> {
        self.require_prompts().await?;
        self.request_typed(
            "prompts/get",
            &GetPromptParams {
                name: name.into(),
                arguments: (!arguments.is_empty()).then_some(arguments),
            },
            self.inner.config.request_timeout,
        )
        .await
    }

    /// Completes one prompt or resource-template argument.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when completion was not negotiated or the peer rejects the request.
    pub async fn complete(&self, params: CompleteParams) -> Result<CompleteResult, McpError> {
        self.require_completions().await?;
        self.request_typed(
            "completion/complete",
            &params,
            self.inner.config.request_timeout,
        )
        .await
    }

    /// Calls one remote tool with the configured timeout.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for lifecycle, transport, protocol, timeout, or
    /// peer failures.
    pub async fn call_tool(&self, params: CallToolParams) -> Result<CallToolResult, McpError> {
        self.require_active().await?;
        self.request_typed("tools/call", &params, self.inner.config.request_timeout)
            .await
    }

    pub(crate) async fn call_tool_scoped(
        &self,
        params: CallToolParams,
        context: &ToolContext,
    ) -> Result<CallToolResult, McpError> {
        self.require_active().await?;
        let timeout = context
            .remaining()
            .map_or(self.inner.config.request_timeout, |remaining| {
                remaining.min(self.inner.config.request_timeout)
            });
        let id = self.next_id();
        let request = request_with_params(id.clone(), "tools/call", &params)?;
        let request_future = self.inner.transport.request(request);
        tokio::select! {
            response = request_future => {
                decode_response(&id, response?)
            }
            () = context.cancellation().cancelled() => {
                self.cancel_request(&id, "Runifold Tool invocation cancelled").await;
                Err(McpError::Cancelled)
            }
            () = tokio::time::sleep(timeout) => {
                self.cancel_request(&id, "Runifold Tool invocation deadline exceeded").await;
                Err(McpError::DeadlineExceeded)
            }
        }
    }

    async fn request_typed<P, R>(
        &self,
        method: &str,
        params: &P,
        timeout: Duration,
    ) -> Result<R, McpError>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let id = self.next_id();
        let request = request_with_params(id.clone(), method, params)?;
        let Ok(response) =
            tokio::time::timeout(timeout, self.inner.transport.request(request)).await
        else {
            self.cancel_request(&id, "Runifold MCP request timed out")
                .await;
            return Err(McpError::DeadlineExceeded);
        };
        decode_response(&id, response?)
    }

    async fn require_active(&self) -> Result<(), McpError> {
        if matches!(*self.inner.state.lock().await, ClientState::Active { .. }) {
            Ok(())
        } else {
            Err(McpError::lifecycle("client is not initialized"))
        }
    }

    async fn require_resources(&self) -> Result<(), McpError> {
        match &*self.inner.state.lock().await {
            ClientState::Active { server } if server.capabilities.resources.is_some() => Ok(()),
            ClientState::Active { .. } => Err(McpError::protocol(
                "server did not negotiate the resources capability",
            )),
            ClientState::Created | ClientState::Initializing => {
                Err(McpError::lifecycle("client is not initialized"))
            }
        }
    }

    async fn require_prompts(&self) -> Result<(), McpError> {
        match &*self.inner.state.lock().await {
            ClientState::Active { server } if server.capabilities.prompts.is_some() => Ok(()),
            ClientState::Active { .. } => Err(McpError::protocol(
                "server did not negotiate the prompts capability",
            )),
            ClientState::Created | ClientState::Initializing => {
                Err(McpError::lifecycle("client is not initialized"))
            }
        }
    }

    async fn require_resource_subscriptions(&self) -> Result<(), McpError> {
        match &*self.inner.state.lock().await {
            ClientState::Active { server }
                if server
                    .capabilities
                    .resources
                    .as_ref()
                    .is_some_and(|capability| capability.subscribe) =>
            {
                Ok(())
            }
            ClientState::Active { .. } => Err(McpError::protocol(
                "server did not negotiate resource subscriptions",
            )),
            ClientState::Created | ClientState::Initializing => {
                Err(McpError::lifecycle("client is not initialized"))
            }
        }
    }

    async fn require_completions(&self) -> Result<(), McpError> {
        match &*self.inner.state.lock().await {
            ClientState::Active { server } if server.capabilities.completions.is_some() => Ok(()),
            ClientState::Active { .. } => Err(McpError::protocol(
                "server did not negotiate the completions capability",
            )),
            ClientState::Created | ClientState::Initializing => {
                Err(McpError::lifecycle("client is not initialized"))
            }
        }
    }

    async fn cancel_request(&self, id: &RequestId, reason: &str) {
        let _ = self
            .inner
            .transport
            .notify(JsonRpcNotification::new(
                "notifications/cancelled",
                Some(json!({
                    "requestId": id,
                    "reason": reason,
                })),
            ))
            .await;
    }

    fn next_id(&self) -> RequestId {
        RequestId::String(format!(
            "runifold-{}",
            self.inner.next_id.fetch_add(1, Ordering::Relaxed)
        ))
    }
}

struct ClientPeerHandler {
    sampling: Arc<SamplingService>,
    inflight: Arc<StdMutex<HashMap<RequestId, CancellationToken>>>,
}

impl ClientPeerHandler {
    fn new(sampling: Arc<SamplingService>) -> Self {
        Self {
            sampling,
            inflight: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    fn lock_inflight(&self) -> std::sync::MutexGuard<'_, HashMap<RequestId, CancellationToken>> {
        self.inflight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl PeerRequestHandler for ClientPeerHandler {
    fn handle(&self, request: JsonRpcRequest) -> TransportFuture<'_, crate::JsonRpcResponse> {
        Box::pin(async move {
            let id = request.id.clone();
            if request.jsonrpc != "2.0" {
                return Ok(crate::JsonRpcResponse::error(
                    id,
                    -32600,
                    "jsonrpc must be `2.0`",
                    None,
                ));
            }
            if request.method != "sampling/createMessage" {
                return Ok(crate::JsonRpcResponse::error(
                    id,
                    -32601,
                    "method not found",
                    None,
                ));
            }
            let params: CreateMessageParams = match request.params.map_or_else(
                || serde_json::from_value(serde_json::Value::Null),
                serde_json::from_value,
            ) {
                Ok(params) => params,
                Err(error) => {
                    return Ok(crate::JsonRpcResponse::error(
                        id,
                        -32602,
                        error.to_string(),
                        None,
                    ));
                }
            };
            let cancellation = CancellationToken::new();
            self.lock_inflight()
                .insert(id.clone(), cancellation.clone());
            let _guard = ClientInflightGuard {
                id: id.clone(),
                inflight: Arc::clone(&self.inflight),
            };
            match self.sampling.execute(params, cancellation).await {
                Ok(result) => match serde_json::to_value(result) {
                    Ok(result) => Ok(crate::JsonRpcResponse::success(id, result)),
                    Err(_) => Ok(crate::JsonRpcResponse::error(
                        id,
                        -32603,
                        "failed to encode Sampling result",
                        None,
                    )),
                },
                Err(error) => Ok(sampling_error_response(id, error)),
            }
        })
    }

    fn notify(&self, notification: JsonRpcNotification) -> Result<(), McpError> {
        if notification.method != "notifications/cancelled" {
            return Ok(());
        }
        let Some(params) = notification.params else {
            return Err(McpError::protocol(
                "cancelled notification omitted parameters",
            ));
        };
        let request_id = params
            .get("requestId")
            .cloned()
            .ok_or_else(|| McpError::protocol("cancelled notification omitted requestId"))
            .and_then(|value| serde_json::from_value(value).map_err(Into::into))?;
        if let Some(cancellation) = self.lock_inflight().get(&request_id) {
            cancellation.cancel();
        }
        Ok(())
    }
}

struct ClientInflightGuard {
    id: RequestId,
    inflight: Arc<StdMutex<HashMap<RequestId, CancellationToken>>>,
}

impl Drop for ClientInflightGuard {
    fn drop(&mut self) {
        self.inflight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}

fn sampling_error_response(id: RequestId, error: SamplingError) -> crate::JsonRpcResponse {
    let data = error
        .stage
        .and_then(|stage| serde_json::to_value(stage).ok())
        .map(|stage| serde_json::json!({"stage": stage}));
    let (code, message) = match error.kind {
        SamplingErrorKind::Rejected => (-1, error.message),
        SamplingErrorKind::InvalidRequest => (-32602, error.message),
        SamplingErrorKind::LimitExceeded => (-32000, error.message),
        SamplingErrorKind::Cancelled => (-32800, "Sampling request cancelled".into()),
        SamplingErrorKind::DeadlineExceeded => (-32001, "Sampling deadline elapsed".into()),
        SamplingErrorKind::Execution | SamplingErrorKind::InvalidOutput => {
            (-32603, "Sampling failed".into())
        }
    };
    crate::JsonRpcResponse::error(id, code, message, data)
}

impl std::fmt::Debug for McpClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpClient")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}

struct ClientInner {
    transport: Arc<dyn McpTransport>,
    config: McpClientConfig,
    next_id: AtomicU64,
    state: Mutex<ClientState>,
}

#[derive(Clone, Debug)]
enum ClientState {
    Created,
    Initializing,
    Active { server: Box<InitializeResult> },
}

fn request_with_params<P>(
    id: RequestId,
    method: &str,
    params: &P,
) -> Result<JsonRpcRequest, McpError>
where
    P: Serialize + ?Sized,
{
    Ok(JsonRpcRequest::new(
        id,
        method,
        Some(serde_json::to_value(params)?),
    ))
}

fn decode_response<R>(id: &RequestId, response: crate::JsonRpcResponse) -> Result<R, McpError>
where
    R: DeserializeOwned,
{
    if response.id() != id {
        return Err(McpError::protocol("response id does not match request id"));
    }
    serde_json::from_value(response.into_result()?).map_err(McpError::from)
}
