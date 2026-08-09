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
    CacheMode, CallToolParams, CallToolResult, ClientCapabilities, ClientTaskRequestsCapability,
    ClientTaskSamplingRequestsCapability, ClientTasksCapability, CompleteParams, CompleteResult,
    CoreTaskWire, CreateMessageParams, DiscoverParams, DiscoverResult, GetPromptParams,
    GetPromptResult, Implementation, InitializeParams, InitializeResult, JsonRpcNotification,
    JsonRpcRequest, LATEST_PROTOCOL_VERSION, ListPromptsParams, ListPromptsResult,
    ListResourceTemplatesParams, ListResourceTemplatesResult, ListResourcesParams,
    ListResourcesResult, ListToolsParams, ListToolsResult, McpError, McpPrompt, McpResource,
    McpResourceTemplate, McpSamplingTaskBackend, McpTaskBackendError, McpTaskBackendErrorKind,
    McpTool, McpTransport, PeerRequestHandler, ReadResourceParams, ReadResourceResult, RequestId,
    ResourceSubscriptionParams, STATELESS_PROTOCOL_VERSION, SamplingError, SamplingErrorKind,
    SamplingService, SamplingTaskApprovalClaim, SamplingTaskCreation, SamplingTaskRequest,
    SamplingTaskResult, SamplingTaskTerminalResult, ServerNotificationStream,
    StatelessCancellation, StatelessRequestMetadata, TaskIdParams, TaskStatus, TransportFuture,
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
    sampling_tasks: Option<Arc<dyn McpSamplingTaskBackend>>,
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
            sampling_tasks: None,
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

    /// Enables durable task-augmented Sampling on the client receiver.
    ///
    /// The backend must execute requests through the host's configured
    /// Sampling approval and model-selection policy.
    #[must_use]
    pub fn with_sampling_tasks(mut self, backend: Arc<dyn McpSamplingTaskBackend>) -> Self {
        self.sampling_tasks = Some(backend);
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
    sampling_tasks: Option<Arc<dyn McpSamplingTaskBackend>>,
    inflight: Arc<StdMutex<HashMap<RequestId, CancellationToken>>>,
    approved_results: Arc<StdMutex<HashMap<String, crate::CreateMessageResult>>>,
    result_locks: Arc<StdMutex<HashMap<String, Arc<Mutex<()>>>>>,
}

impl ClientPeerHandler {
    fn new(
        sampling: Arc<SamplingService>,
        sampling_tasks: Option<Arc<dyn McpSamplingTaskBackend>>,
    ) -> Self {
        Self {
            sampling,
            sampling_tasks,
            inflight: Arc::new(StdMutex::new(HashMap::new())),
            approved_results: Arc::new(StdMutex::new(HashMap::new())),
            result_locks: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    fn lock_inflight(&self) -> std::sync::MutexGuard<'_, HashMap<RequestId, CancellationToken>> {
        self.inflight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_approved_results(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<String, crate::CreateMessageResult>> {
        self.approved_results
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn result_lock(&self, task_id: &str) -> Arc<Mutex<()>> {
        Arc::clone(
            self.result_locks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(task_id.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        )
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
            let cancellation = CancellationToken::new();
            self.lock_inflight()
                .insert(id.clone(), cancellation.clone());
            let _guard = ClientInflightGuard {
                id: id.clone(),
                inflight: Arc::clone(&self.inflight),
            };
            if request.method != "sampling/createMessage" {
                return Ok(self
                    .handle_task_request(id, &request.method, request.params, cancellation)
                    .await);
            }
            let mut params: CreateMessageParams = match decode_peer_params(request.params) {
                Ok(params) => params,
                Err(error) => return Ok(crate::JsonRpcResponse::error(id, -32602, error, None)),
            };
            if params.task.is_some() {
                if let Some(backend) = &self.sampling_tasks {
                    let approved = match self
                        .sampling
                        .approve_task_request(params, cancellation.clone())
                        .await
                    {
                        Ok(params) => params,
                        Err(error) => return Ok(sampling_error_response(id, error)),
                    };
                    let max_tokens = approved.params.max_tokens;
                    let budget_reserved = approved.budget_reserved;
                    let idempotency_key = approved.idempotency_key;
                    let task = match backend
                        .create_message_task(SamplingTaskRequest {
                            params: approved.params,
                        })
                        .await
                    {
                        Ok(SamplingTaskCreation { task, created }) => {
                            if !created && budget_reserved {
                                self.sampling.rollback_task_budget(max_tokens);
                            }
                            task.validate_metadata().and_then(|()| {
                                if task.status != TaskStatus::Working {
                                    return Err(McpTaskBackendError::new(
                                        McpTaskBackendErrorKind::InvalidState,
                                        "new Sampling task must start in working state",
                                    ));
                                }
                                Ok(task)
                            })
                        }
                        Err(error) => {
                            if error.task_was_not_created() && budget_reserved {
                                self.sampling.rollback_task_budget(max_tokens);
                                if let Some(key) = &idempotency_key {
                                    self.sampling.forget_task_budget_key(key);
                                }
                            }
                            Err(error)
                        }
                    };
                    return Ok(match task {
                        Ok(task) => encode_peer_result(id, &SamplingTaskResult { task }),
                        Err(error) => sampling_task_error_response(id, error),
                    });
                }
                // The MCP Tasks specification requires receivers without the
                // negotiated task capability to process the request normally.
                params.task = None;
            }
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

impl ClientPeerHandler {
    async fn sampling_task_result_response(
        &self,
        id: RequestId,
        backend: &Arc<dyn McpSamplingTaskBackend>,
        task_id: String,
        cancellation: CancellationToken,
    ) -> crate::JsonRpcResponse {
        let result_lock = self.result_lock(&task_id);
        let _result_guard = tokio::select! {
            guard = result_lock.lock() => guard,
            () = cancellation.cancelled() => {
                return sampling_task_result_cancelled(id);
            }
        };
        let durable_approved = match load_durable_approved_result(
            id.clone(),
            backend,
            &task_id,
            &cancellation,
        )
        .await
        {
            Ok(result) => result,
            Err(response) => return response,
        };
        if let Some(result) = durable_approved {
            self.lock_approved_results()
                .insert(task_id.clone(), result.clone());
            return related_task_result(id, result, &task_id);
        }
        if let Some(result) = self.lock_approved_results().get(&task_id).cloned() {
            return related_task_result(id, result, &task_id);
        }
        loop {
            let task = tokio::select! {
                task = backend.get(task_id.clone()) => match task {
                    Ok(task) => task,
                    Err(error) => return sampling_task_error_response(id, error),
                },
                () = cancellation.cancelled() => {
                    return sampling_task_result_cancelled(id);
                }
            };
            if let Err(error) = task.validate_metadata() {
                return sampling_task_error_response(id, error);
            }
            if task.status.is_terminal() {
                break;
            }
            let delay = Duration::from_millis(task.poll_interval_ms.unwrap_or(1000))
                .clamp(Duration::from_millis(100), Duration::from_secs(30));
            tokio::select! {
                () = tokio::time::sleep(delay) => {}
                () = cancellation.cancelled() => {
                    return sampling_task_result_cancelled(id);
                }
            }
        }
        let terminal = tokio::select! {
            terminal = backend.result(task_id.clone()) => match terminal {
                Ok(terminal) => terminal,
                Err(error) => return sampling_task_error_response(id, error),
            },
            () = cancellation.cancelled() => {
                return sampling_task_result_cancelled(id);
            }
        };
        match terminal {
            SamplingTaskTerminalResult::Error(error) => {
                crate::JsonRpcResponse::error(id, error.code, error.message, error.data)
            }
            SamplingTaskTerminalResult::Success(output) => {
                self.approve_sampling_task_output(id, backend, task_id, *output, cancellation)
                    .await
            }
        }
    }

    async fn approve_sampling_task_output(
        &self,
        id: RequestId,
        backend: &Arc<dyn McpSamplingTaskBackend>,
        task_id: String,
        output: crate::SamplingTaskOutput,
        cancellation: CancellationToken,
    ) -> crate::JsonRpcResponse {
        let approval = match claim_sampling_approval(
            id.clone(),
            backend,
            &task_id,
            self.sampling.approval_lease_ms(),
            &cancellation,
        )
        .await
        {
            Ok(approval) => approval,
            Err(response) => return response,
        };
        let token = match approval {
            SamplingApproval::Acquired(token) => token,
            SamplingApproval::Completed(result) => {
                self.lock_approved_results()
                    .insert(task_id.clone(), result.clone());
                return related_task_result(id, result, &task_id);
            }
        };
        let crate::SamplingTaskOutput { request, result } = output;
        let result = match self
            .sampling
            .approve_task_result(request, result, cancellation)
            .await
        {
            Ok(result) => result,
            Err(error) => return sampling_error_response(id, error),
        };
        let result = match backend
            .complete_result_approval(task_id.clone(), token, result)
            .await
        {
            Ok(result) => result,
            Err(error) => return sampling_task_error_response(id, error),
        };
        self.lock_approved_results()
            .insert(task_id.clone(), result.clone());
        related_task_result(id, result, &task_id)
    }

    async fn handle_task_request(
        &self,
        id: RequestId,
        method: &str,
        params: Option<serde_json::Value>,
        cancellation: CancellationToken,
    ) -> crate::JsonRpcResponse {
        let Some(backend) = &self.sampling_tasks else {
            return crate::JsonRpcResponse::error(id, -32601, "method not found", None);
        };
        let params: TaskIdParams = match decode_peer_params(params) {
            Ok(params) => params,
            Err(error) => return crate::JsonRpcResponse::error(id, -32602, error, None),
        };
        if let Err(error) = params.validate() {
            return sampling_task_error_response(id, error);
        }
        match method {
            "tasks/get" => match backend.get(params.task_id).await.and_then(|task| {
                task.validate_metadata()?;
                Ok(task)
            }) {
                Ok(task) => encode_peer_result(id, &CoreTaskWire::from(&task)),
                Err(error) => sampling_task_error_response(id, error),
            },
            "tasks/result" => {
                self.sampling_task_result_response(id, backend, params.task_id, cancellation)
                    .await
            }
            "tasks/cancel" => match backend.cancel(params.task_id.clone()).await {
                Ok(()) => match backend.get(params.task_id).await.and_then(|task| {
                    task.validate_metadata()?;
                    Ok(task)
                }) {
                    Ok(task) => encode_peer_result(id, &CoreTaskWire::from(&task)),
                    Err(error) => sampling_task_error_response(id, error),
                },
                Err(error) => sampling_task_error_response(id, error),
            },
            _ => crate::JsonRpcResponse::error(id, -32601, "method not found", None),
        }
    }
}

async fn load_durable_approved_result(
    id: RequestId,
    backend: &Arc<dyn McpSamplingTaskBackend>,
    task_id: &str,
    cancellation: &CancellationToken,
) -> Result<Option<crate::CreateMessageResult>, crate::JsonRpcResponse> {
    tokio::select! {
        result = backend.approved_result(task_id.to_owned()) => result
            .map_err(|error| sampling_task_error_response(id, error)),
        () = cancellation.cancelled() => Err(crate::JsonRpcResponse::error(
            id,
            -32800,
            "Sampling Task result request cancelled",
            None,
        )),
    }
}

enum SamplingApproval {
    Acquired(String),
    Completed(crate::CreateMessageResult),
}

async fn claim_sampling_approval(
    id: RequestId,
    backend: &Arc<dyn McpSamplingTaskBackend>,
    task_id: &str,
    lease_ms: u64,
    cancellation: &CancellationToken,
) -> Result<SamplingApproval, crate::JsonRpcResponse> {
    loop {
        let claim = tokio::select! {
            claim = backend.claim_result_approval(task_id.to_owned(), lease_ms) => claim
                .map_err(|error| sampling_task_error_response(id.clone(), error))?,
            () = cancellation.cancelled() => {
                return Err(sampling_task_result_cancelled(id));
            }
        };
        match claim {
            SamplingTaskApprovalClaim::Acquired { token } => {
                return Ok(SamplingApproval::Acquired(token));
            }
            SamplingTaskApprovalClaim::Completed(result) => {
                return Ok(SamplingApproval::Completed(result));
            }
            SamplingTaskApprovalClaim::Busy { retry_after_ms } => {
                tokio::select! {
                    () = tokio::time::sleep(Duration::from_millis(retry_after_ms.max(1))) => {}
                    () = cancellation.cancelled() => {
                        return Err(sampling_task_result_cancelled(id));
                    }
                }
            }
        }
    }
}

fn sampling_task_result_cancelled(id: RequestId) -> crate::JsonRpcResponse {
    crate::JsonRpcResponse::error(id, -32800, "Sampling Task result request cancelled", None)
}

fn related_task_result(
    id: RequestId,
    mut result: crate::CreateMessageResult,
    task_id: &str,
) -> crate::JsonRpcResponse {
    result.meta.insert(
        "io.modelcontextprotocol/related-task".into(),
        json!({"taskId": task_id}),
    );
    encode_peer_result(id, &result)
}

fn decode_peer_params<T: DeserializeOwned>(params: Option<serde_json::Value>) -> Result<T, String> {
    serde_json::from_value(params.unwrap_or(serde_json::Value::Null))
        .map_err(|error| error.to_string())
}

fn encode_peer_result<T: Serialize>(id: RequestId, result: &T) -> crate::JsonRpcResponse {
    match serde_json::to_value(result) {
        Ok(result) => crate::JsonRpcResponse::success(id, result),
        Err(_) => crate::JsonRpcResponse::error(id, -32603, "failed to encode result", None),
    }
}

fn sampling_task_error_response(
    id: RequestId,
    error: McpTaskBackendError,
) -> crate::JsonRpcResponse {
    let code = match error.kind {
        McpTaskBackendErrorKind::InvalidInput
        | McpTaskBackendErrorKind::NotFound
        | McpTaskBackendErrorKind::AdmissionDenied => -32602,
        McpTaskBackendErrorKind::InvalidState | McpTaskBackendErrorKind::Storage => -32603,
    };
    crate::JsonRpcResponse::error(id, code, error.message, None)
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
