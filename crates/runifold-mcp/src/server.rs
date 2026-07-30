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
    CacheHint, CacheOperation, CacheScope, ClientCapabilities, CompleteParams, CompletionRegistry,
    CreateMessageParams, DiscoverMetadata, DiscoverParams, DiscoverResult, GetPromptParams,
    Implementation, IncludeContext, InitializeParams, InitializeResult, JsonRpcNotification,
    JsonRpcRequest, JsonRpcResponse, LATEST_PROTOCOL_VERSION, ListPromptsParams, ListPromptsResult,
    ListResourceTemplatesParams, ListResourceTemplatesResult, ListResourcesParams,
    ListResourcesResult, ListToolsParams, ListToolsResult, McpError, McpResultType,
    McpSamplingClient, McpTaskBackend, McpTaskBackendError, McpTaskBackendErrorKind, McpTool,
    McpTransport, PeerRequestHandler, PromptRegistry, PromptsCapability, ReadResourceParams,
    RequestId, ResourceRegistry, ResourceSubscriptionParams, ResourcesCapability,
    STATELESS_PROTOCOL_VERSION, SUPPORTED_PROTOCOL_VERSIONS, ServerCapabilities,
    ServerNotificationStream, StatelessRequestMetadata, ToolsCapability, TransportFuture,
    mrtr::{MrtrCallToolParams, MrtrToolDecision, MrtrToolGate, MrtrToolGates, MrtrToolRequest},
    pagination::{self, Collection},
    server_response::{
        completion_error_response, prompt_error_response, resource_error_response,
        serialize_result, tool_invocation_response,
    },
    subscription::{
        SubscriptionFilter, SubscriptionsListenParams, acknowledgement, attach_subscription_id,
    },
    tasks::{
        CreateTaskResult, GetTaskResult, TASKS_EXTENSION_ID, TaskIdParams, ToolTaskRequest,
        UpdateTaskParams,
    },
    transport::ClientPeerTransport,
};

mod discovery;
mod dispatch;
mod invocation;
mod lifecycle;
mod subscriptions;

const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
const INTERNAL_ERROR: i64 = -32603;
const MISSING_REQUIRED_CLIENT_CAPABILITY: i64 = -32021;
const MISSING_TASKS_CAPABILITY: i64 = -32003;
const LIFECYCLE_ERROR: i64 = -32002;
const UNSUPPORTED_PROTOCOL_VERSION: i64 = -32022;
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
    stateless_cursor_namespace: String,
    subscription_events: broadcast::Sender<JsonRpcNotification>,
    mrtr_tool_gates: MrtrToolGates,
    cache_hints: HashMap<CacheOperation, CacheHint>,
    task_backend: Option<Arc<dyn McpTaskBackend>>,
    task_notification_interval: Duration,
    max_task_subscription_ids: usize,
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
        let (subscription_events, _) = broadcast::channel(256);
        Self {
            registry,
            authority,
            implementation,
            instructions: None,
            resources: None,
            prompts: None,
            completions: None,
            page_size: DEFAULT_PAGE_SIZE,
            stateless_cursor_namespace: RunId::new().to_string(),
            subscription_events,
            mrtr_tool_gates: BTreeMap::new(),
            cache_hints: HashMap::new(),
            task_backend: None,
            task_notification_interval: Duration::from_secs(1),
            max_task_subscription_ids: 256,
        }
    }

    pub(crate) fn tool_input_schema(&self, name: &str) -> Option<Value> {
        self.registry
            .model_specs()
            .into_iter()
            .find(|spec| spec.name == name)
            .map(|spec| spec.input_schema)
    }

    /// Publishes a Tool-list change to matching modern subscriptions.
    pub fn notify_tools_list_changed(&self) -> bool {
        self.publish_subscription_notification(JsonRpcNotification::new(
            "notifications/tools/list_changed",
            None,
        ))
    }

    /// Publishes a Prompt-list change to matching modern subscriptions.
    pub fn notify_prompts_list_changed(&self) -> bool {
        self.publish_subscription_notification(JsonRpcNotification::new(
            "notifications/prompts/list_changed",
            None,
        ))
    }

    /// Publishes a Resource-list change to matching modern subscriptions.
    pub fn notify_resources_list_changed(&self) -> bool {
        self.publish_subscription_notification(JsonRpcNotification::new(
            "notifications/resources/list_changed",
            None,
        ))
    }

    /// Publishes an exact Resource update to matching modern subscriptions.
    pub fn notify_resource_updated(&self, uri: impl Into<String>) -> bool {
        self.publish_subscription_notification(JsonRpcNotification::new(
            "notifications/resources/updated",
            Some(json!({"uri": uri.into()})),
        ))
    }

    fn publish_subscription_notification(&self, notification: JsonRpcNotification) -> bool {
        self.subscription_events.send(notification).is_ok()
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

    /// Adds a stateless MRTR preflight for one canonical Tool.
    #[must_use]
    pub fn with_mrtr_tool_gate(
        mut self,
        tool_name: impl Into<String>,
        gate: Arc<dyn MrtrToolGate>,
    ) -> Self {
        self.mrtr_tool_gates.insert(tool_name.into(), gate);
        self
    }

    /// Selects the server-controlled list page size.
    #[must_use]
    pub const fn with_page_size(mut self, page_size: usize) -> Self {
        self.page_size = if page_size == 0 { 1 } else { page_size };
        self
    }

    /// Configures freshness and visibility for one modern cacheable operation.
    ///
    /// Unconfigured operations remain immediately stale and private.
    #[must_use]
    pub fn with_cache_hint(mut self, operation: CacheOperation, hint: CacheHint) -> Self {
        self.cache_hints.insert(operation, hint);
        self
    }

    /// Installs the single durable backend for the MCP Tasks extension.
    #[must_use]
    pub fn with_task_backend(mut self, backend: Arc<dyn McpTaskBackend>) -> Self {
        self.task_backend = Some(backend);
        self
    }

    /// Sets how frequently Task subscriptions derive fresh durable state.
    ///
    /// A zero duration is normalized to one millisecond to prevent a busy loop.
    #[must_use]
    pub fn with_task_notification_interval(mut self, interval: Duration) -> Self {
        self.task_notification_interval = interval.max(Duration::from_millis(1));
        self
    }

    /// Bounds Task IDs accepted by one notification subscription.
    #[must_use]
    pub const fn with_max_task_subscription_ids(mut self, limit: usize) -> Self {
        self.max_task_subscription_ids = if limit == 0 { 1 } else { limit };
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

    fn listen(&self, request: JsonRpcRequest) -> TransportFuture<'_, ServerNotificationStream> {
        Box::pin(async move {
            self.open_subscription(request).map_err(|response| {
                response
                    .into_result()
                    .expect_err("subscription rejection is always a JSON-RPC error")
            })
        })
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestEra {
    Legacy,
    Stateless,
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

fn task_capability_declared(metadata: Option<&StatelessRequestMetadata>) -> bool {
    metadata.is_some_and(|metadata| {
        metadata
            .client_capabilities
            .extensions
            .contains_key(TASKS_EXTENSION_ID)
    })
}

fn task_capability_declared_in_value(params: Option<&Value>) -> bool {
    params
        .and_then(Value::as_object)
        .and_then(|params| params.get("_meta"))
        .cloned()
        .and_then(|metadata| serde_json::from_value(metadata).ok())
        .as_ref()
        .is_some_and(|metadata| task_capability_declared(Some(metadata)))
}

fn missing_tasks_capability(id: RequestId) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        MISSING_TASKS_CAPABILITY,
        "missing required client capability",
        Some(json!({
            "requiredCapabilities": {
                "extensions": {
                    (TASKS_EXTENSION_ID): {}
                }
            }
        })),
    )
}

fn task_backend_error_response(id: RequestId, error: McpTaskBackendError) -> JsonRpcResponse {
    let code = match error.kind {
        McpTaskBackendErrorKind::InvalidInput | McpTaskBackendErrorKind::NotFound => INVALID_PARAMS,
        McpTaskBackendErrorKind::InvalidState | McpTaskBackendErrorKind::Storage => INTERNAL_ERROR,
    };
    JsonRpcResponse::error(id, code, error.message, None)
}
