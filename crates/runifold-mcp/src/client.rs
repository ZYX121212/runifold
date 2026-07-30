use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::StreamExt;
use runifold_core::{CancellationToken, RunId};
use runifold_tool::ToolContext;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;
use tokio::sync::Mutex;

use crate::{
    CacheMode, CallToolParams, CallToolResult, ClientCapabilities, CompleteParams, CompleteResult,
    CreateMessageParams, DiscoverParams, DiscoverResult, GetPromptParams, GetPromptResult,
    Implementation, InitializeParams, InitializeResult, JsonRpcNotification, JsonRpcRequest,
    LATEST_PROTOCOL_VERSION, ListPromptsParams, ListPromptsResult, ListResourceTemplatesParams,
    ListResourceTemplatesResult, ListResourcesParams, ListResourcesResult, ListToolsParams,
    ListToolsResult, McpError, McpPrompt, McpResource, McpResourceTemplate, McpTool, McpTransport,
    PeerRequestHandler, ReadResourceParams, ReadResourceResult, RequestId,
    ResourceSubscriptionParams, STATELESS_PROTOCOL_VERSION, SamplingCapability, SamplingError,
    SamplingErrorKind, SamplingService, ServerNotificationStream, StatelessCancellation,
    StatelessRequestMetadata, TransportFuture,
    cache::{ClientResponseCache, InMemoryResponseCache, ResponseCacheStore},
    mrtr::{InputRequiredResult, MrtrInputHandler},
    subscription::{
        McpSubscription, SubscriptionAcknowledgedParams, SubscriptionFilter,
        SubscriptionsListenParams, notification_subscription_id,
    },
};

mod connection;
mod lifecycle;
mod prompts;
mod request;
mod resources;
mod tools;

/// MCP client policy.
#[derive(Clone, Debug)]
pub struct McpClientConfig {
    implementation: Implementation,
    request_timeout: Duration,
    max_pagination_pages: usize,
    sampling: Option<Arc<SamplingService>>,
    mrtr_input_handler: Option<Arc<dyn MrtrInputHandler>>,
    max_mrtr_rounds: usize,
    max_mrtr_inputs_per_round: usize,
    response_cache: Arc<dyn ResponseCacheStore>,
    cache_namespace: String,
    private_cache_partition: String,
    max_cache_ttl: Duration,
    tasks_enabled: bool,
    min_task_poll_interval: Duration,
    max_task_poll_interval: Duration,
}

impl McpClientConfig {
    /// Creates a client policy with a 30-second request timeout.
    pub fn new(implementation: Implementation) -> Self {
        Self {
            implementation,
            request_timeout: Duration::from_secs(30),
            max_pagination_pages: 1024,
            sampling: None,
            mrtr_input_handler: None,
            max_mrtr_rounds: 10,
            max_mrtr_inputs_per_round: 64,
            response_cache: Arc::new(InMemoryResponseCache::new(512)),
            cache_namespace: RunId::new().to_string(),
            private_cache_partition: RunId::new().to_string(),
            max_cache_ttl: Duration::from_secs(60 * 60),
            tasks_enabled: false,
            min_task_poll_interval: Duration::from_millis(100),
            max_task_poll_interval: Duration::from_secs(30),
        }
    }

    /// Enables client-side basic Sampling with explicit host approval policy.
    #[must_use]
    pub fn with_sampling(mut self, sampling: Arc<SamplingService>) -> Self {
        self.sampling = Some(sampling);
        self
    }

    /// Installs a host-controlled resolver for MRTR Sampling, Elicitation, or Roots requests.
    #[must_use]
    pub fn with_mrtr_input_handler(mut self, handler: Arc<dyn MrtrInputHandler>) -> Self {
        self.mrtr_input_handler = Some(handler);
        self
    }

    /// Bounds incomplete MRTR responses before the logical request fails.
    #[must_use]
    pub const fn with_max_mrtr_rounds(mut self, rounds: usize) -> Self {
        self.max_mrtr_rounds = if rounds == 0 { 1 } else { rounds };
        self
    }

    /// Bounds independently keyed input requests in one MRTR round.
    #[must_use]
    pub const fn with_max_mrtr_inputs_per_round(mut self, inputs: usize) -> Self {
        self.max_mrtr_inputs_per_round = if inputs == 0 { 1 } else { inputs };
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

    /// Replaces the response-cache storage implementation.
    #[must_use]
    pub fn with_response_cache(mut self, cache: Arc<dyn ResponseCacheStore>) -> Self {
        self.response_cache = cache;
        self
    }

    /// Selects the server namespace used by cache keys.
    ///
    /// Clients may share public entries only when this value and their cache
    /// store are the same. It should identify a trusted endpoint, not
    /// self-reported MCP server metadata.
    #[must_use]
    pub fn with_cache_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.cache_namespace = namespace.into();
        self
    }

    /// Selects the authorization partition used by private cache entries.
    #[must_use]
    pub fn with_private_cache_partition(mut self, partition: impl Into<String>) -> Self {
        self.private_cache_partition = partition.into();
        self
    }

    /// Bounds server-provided cache lifetimes.
    #[must_use]
    pub const fn with_max_cache_ttl(mut self, max_ttl: Duration) -> Self {
        self.max_cache_ttl = max_ttl;
        self
    }

    /// Enables the modern MCP Tasks extension and explicit Task APIs.
    #[must_use]
    pub const fn with_tasks(mut self) -> Self {
        self.tasks_enabled = true;
        self
    }

    /// Bounds server-suggested Task polling intervals.
    #[must_use]
    pub const fn with_max_task_poll_interval(mut self, interval: Duration) -> Self {
        self.max_task_poll_interval = interval;
        self
    }

    /// Sets the client-enforced floor for server-suggested Task polling.
    ///
    /// The effective floor never exceeds the configured maximum interval.
    #[must_use]
    pub const fn with_min_task_poll_interval(mut self, interval: Duration) -> Self {
        self.min_task_poll_interval = interval;
        self
    }
}

/// Stateful MCP Tools client.
#[derive(Clone)]
pub struct McpClient {
    inner: Arc<ClientInner>,
}

/// MCP protocol era selected for ordinary requests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpProtocolMode {
    /// Stateless per-request metadata introduced in `2026-07-28`.
    Stateless,
    /// Initialization-based protocol used by `2025-11-25`.
    Legacy,
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
    cache: ClientResponseCache,
    next_id: AtomicU64,
    state: Mutex<ClientState>,
}

#[derive(Clone, Debug)]
enum ClientState {
    Created,
    Discovering,
    Initializing,
    Active {
        server: Box<InitializeResult>,
        mode: McpProtocolMode,
    },
}
