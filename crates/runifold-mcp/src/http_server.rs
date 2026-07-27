use std::{
    collections::{HashMap, HashSet, VecDeque},
    convert::Infallible,
    sync::{
        Arc, Mutex as StdMutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::State,
    http::{
        HeaderMap, HeaderValue, Request, StatusCode,
        header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, ORIGIN, WWW_AUTHENTICATE},
    },
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::post,
};
use runifold_core::RunId;
use serde_json::Value;
use tokio::sync::{Mutex, RwLock, broadcast, oneshot};

use crate::{
    HttpAuthorizer, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, LATEST_PROTOCOL_VERSION,
    McpError, McpSamplingClient, McpServer, McpSession, RequestId, TransportFuture,
    transport::ClientPeerTransport,
};

/// HTTP header carrying an opaque MCP session identifier.
pub const MCP_SESSION_ID_HEADER: &str = "mcp-session-id";
/// HTTP header carrying the negotiated MCP protocol revision.
pub const MCP_PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";

const LAST_EVENT_ID_HEADER: &str = "last-event-id";
const JSON_MEDIA_TYPE: &str = "application/json";
const SSE_MEDIA_TYPE: &str = "text/event-stream";
const DEFAULT_MAX_BODY_BYTES: usize = 1024 * 1024;

/// Response framing selected for JSON-RPC requests.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum HttpResponseMode {
    /// Return one ordinary JSON response.
    #[default]
    Json,
    /// Return one finite SSE stream containing the response.
    Sse,
}

/// Security and resource policy for an MCP Streamable HTTP endpoint.
#[derive(Clone)]
pub struct McpHttpServerConfig {
    response_mode: HttpResponseMode,
    allowed_origins: HashSet<String>,
    authorizer: Option<Arc<dyn HttpAuthorizer>>,
    replay_capacity: usize,
    notification_capacity: usize,
    max_body_bytes: usize,
}

impl McpHttpServerConfig {
    /// Creates a secure-default policy.
    ///
    /// Requests without `Origin` are accepted. Browser requests carrying
    /// `Origin` are rejected until that exact origin is allowlisted.
    pub fn new() -> Self {
        Self {
            response_mode: HttpResponseMode::Json,
            allowed_origins: HashSet::new(),
            authorizer: None,
            replay_capacity: 256,
            notification_capacity: 256,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
        }
    }

    /// Selects JSON or SSE framing for request responses.
    #[must_use]
    pub const fn with_response_mode(mut self, response_mode: HttpResponseMode) -> Self {
        self.response_mode = response_mode;
        self
    }

    /// Allows one exact browser origin.
    #[must_use]
    pub fn with_allowed_origin(mut self, origin: impl Into<String>) -> Self {
        self.allowed_origins.insert(origin.into());
        self
    }

    /// Requires authorization for every HTTP method.
    #[must_use]
    pub fn with_authorizer(mut self, authorizer: Arc<dyn HttpAuthorizer>) -> Self {
        self.authorizer = Some(authorizer);
        self
    }

    /// Bounds resumable notifications retained per session.
    #[must_use]
    pub const fn with_replay_capacity(mut self, capacity: usize) -> Self {
        self.replay_capacity = capacity;
        self
    }

    /// Bounds each live session notification channel.
    #[must_use]
    pub const fn with_notification_capacity(mut self, capacity: usize) -> Self {
        self.notification_capacity = capacity;
        self
    }

    /// Bounds one inbound JSON message.
    #[must_use]
    pub const fn with_max_body_bytes(mut self, bytes: usize) -> Self {
        self.max_body_bytes = bytes;
        self
    }
}

impl Default for McpHttpServerConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for McpHttpServerConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpHttpServerConfig")
            .field("response_mode", &self.response_mode)
            .field("allowed_origins", &self.allowed_origins)
            .field(
                "authorizer",
                &self.authorizer.as_ref().map(|_| "[REDACTED]"),
            )
            .field("replay_capacity", &self.replay_capacity)
            .field("notification_capacity", &self.notification_capacity)
            .field("max_body_bytes", &self.max_body_bytes)
            .finish()
    }
}

/// Stateful MCP Streamable HTTP server.
#[derive(Clone)]
pub struct McpHttpServer {
    inner: Arc<HttpServerInner>,
}

impl McpHttpServer {
    /// Creates an HTTP adapter for an MCP server.
    pub fn new(server: McpServer, config: McpHttpServerConfig) -> Self {
        Self {
            inner: Arc::new(HttpServerInner {
                server,
                config,
                sessions: RwLock::new(HashMap::new()),
            }),
        }
    }

    /// Builds an Axum router at `path`.
    ///
    /// The returned router supports POST, GET, and DELETE on the same endpoint.
    pub fn router(&self, path: &str) -> Router {
        Router::new()
            .route(
                path,
                post(post_handler).get(get_handler).delete(delete_handler),
            )
            .with_state(Arc::clone(&self.inner))
    }

    /// Publishes a resumable server notification to one active session.
    ///
    /// The returned event ID may be persisted by a custom client, although the
    /// bundled client tracks it automatically.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::SessionExpired`] if the session no longer exists,
    /// or [`McpError`] if the notification cannot be encoded.
    pub async fn send_notification(
        &self,
        session_id: &str,
        notification: JsonRpcNotification,
    ) -> Result<String, McpError> {
        let session = self
            .inner
            .sessions
            .read()
            .await
            .get(session_id)
            .cloned()
            .ok_or(McpError::SessionExpired)?;
        session.publish(notification).await
    }

    /// Publishes an update only when the target session subscribed to `uri`.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when the session expired or encoding fails.
    pub async fn send_resource_updated(
        &self,
        session_id: &str,
        uri: &str,
    ) -> Result<Option<String>, McpError> {
        let session = self
            .inner
            .sessions
            .read()
            .await
            .get(session_id)
            .cloned()
            .ok_or(McpError::SessionExpired)?;
        if !session.mcp.is_resource_subscribed(uri) {
            return Ok(None);
        }
        session
            .publish(JsonRpcNotification::new(
                "notifications/resources/updated",
                Some(serde_json::json!({"uri": uri})),
            ))
            .await
            .map(Some)
    }

    /// Publishes a resource-list change to one active session.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when the session expired or encoding fails.
    pub async fn send_resource_list_changed(&self, session_id: &str) -> Result<String, McpError> {
        self.send_notification(
            session_id,
            JsonRpcNotification::new("notifications/resources/list_changed", None),
        )
        .await
    }

    /// Returns the number of live HTTP sessions.
    pub async fn session_count(&self) -> usize {
        self.inner.sessions.read().await.len()
    }

    /// Returns a Sampling requester for one active HTTP session.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::SessionExpired`] when the session does not exist.
    pub async fn sampling_client(&self, session_id: &str) -> Result<McpSamplingClient, McpError> {
        self.inner
            .sessions
            .read()
            .await
            .get(session_id)
            .map(|session| session.mcp.sampling_client())
            .ok_or(McpError::SessionExpired)
    }
}

impl std::fmt::Debug for McpHttpServer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpHttpServer")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}

struct HttpServerInner {
    server: McpServer,
    config: McpHttpServerConfig,
    sessions: RwLock<HashMap<String, Arc<HttpSession>>>,
}

struct HttpSession {
    id: String,
    mcp: McpSession,
    sender: broadcast::Sender<ServerEvent>,
    replay: Mutex<VecDeque<ServerEvent>>,
    next_event: AtomicU64,
    replay_capacity: usize,
    pending: StdMutex<HashMap<RequestId, oneshot::Sender<Result<JsonRpcResponse, McpError>>>>,
}

impl HttpSession {
    fn new(mcp: McpSession, config: &McpHttpServerConfig) -> Self {
        let (sender, _) = broadcast::channel(config.notification_capacity.max(1));
        Self {
            id: RunId::new().to_string(),
            mcp,
            sender,
            replay: Mutex::new(VecDeque::with_capacity(config.replay_capacity)),
            next_event: AtomicU64::new(1),
            replay_capacity: config.replay_capacity,
            pending: StdMutex::new(HashMap::new()),
        }
    }

    async fn publish(&self, notification: JsonRpcNotification) -> Result<String, McpError> {
        self.publish_message(&notification).await
    }

    async fn publish_message<T: serde::Serialize + ?Sized>(
        &self,
        message: &T,
    ) -> Result<String, McpError> {
        let data = serde_json::to_string(message)?;
        let event = ServerEvent {
            id: format!(
                "{}-{}",
                self.id,
                self.next_event.fetch_add(1, Ordering::Relaxed)
            ),
            data,
        };
        if self.replay_capacity > 0 {
            let mut replay = self.replay.lock().await;
            while replay.len() >= self.replay_capacity {
                replay.pop_front();
            }
            replay.push_back(event.clone());
        }
        let _ = self.sender.send(event.clone());
        Ok(event.id)
    }

    fn complete_client_request(&self, response: JsonRpcResponse) -> bool {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(response.id())
            .is_some_and(|sender| sender.send(Ok(response)).is_ok())
    }

    async fn replay_after(&self, last_event_id: Option<&str>) -> Vec<ServerEvent> {
        let replay = self.replay.lock().await;
        let Some(last_event_id) = last_event_id else {
            return replay.iter().cloned().collect();
        };
        if !last_event_id.starts_with(&format!("{}-", self.id)) {
            return Vec::new();
        }
        replay
            .iter()
            .position(|event| event.id == last_event_id)
            .map_or_else(Vec::new, |index| {
                replay.iter().skip(index + 1).cloned().collect()
            })
    }
}

struct HttpClientPeer {
    session: Weak<HttpSession>,
}

impl ClientPeerTransport for HttpClientPeer {
    fn request_client(&self, request: JsonRpcRequest) -> TransportFuture<'_, JsonRpcResponse> {
        Box::pin(async move {
            let session = self.session.upgrade().ok_or(McpError::SessionExpired)?;
            let id = request.id.clone();
            let (sender, receiver) = oneshot::channel();
            if session
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(id.clone(), sender)
                .is_some()
            {
                return Err(McpError::protocol(
                    "duplicate in-flight HTTP server request id",
                ));
            }
            let mut guard = HttpPendingGuard {
                id,
                session: Arc::downgrade(&session),
                armed: true,
            };
            session.publish_message(&request).await?;
            let result = receiver
                .await
                .map_err(|_| McpError::protocol("HTTP client response channel closed"))?;
            guard.disarm();
            result
        })
    }

    fn notify_client(&self, notification: JsonRpcNotification) -> TransportFuture<'_, ()> {
        Box::pin(async move {
            let session = self.session.upgrade().ok_or(McpError::SessionExpired)?;
            session.publish(notification).await?;
            Ok(())
        })
    }
}

struct HttpPendingGuard {
    id: RequestId,
    session: Weak<HttpSession>,
    armed: bool,
}

impl HttpPendingGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for HttpPendingGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(session) = self.session.upgrade() {
            session
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&self.id);
        }
    }
}

#[derive(Clone)]
struct ServerEvent {
    id: String,
    data: String,
}

async fn post_handler(
    State(state): State<Arc<HttpServerInner>>,
    request: Request<Body>,
) -> Response {
    if let Err(response) = validate_security(&state, request.headers()) {
        return *response;
    }
    if !request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with(JSON_MEDIA_TYPE))
    {
        return status_response(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/json",
        );
    }
    let headers = request.headers().clone();
    let Ok(body) = to_bytes(request.into_body(), state.config.max_body_bytes).await else {
        return status_response(StatusCode::PAYLOAD_TOO_LARGE, "request body too large");
    };
    let value: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return status_response(StatusCode::BAD_REQUEST, "invalid JSON-RPC message"),
    };

    if value.get("id").is_some() && value.get("method").is_none() {
        let response: JsonRpcResponse = match serde_json::from_value(value) {
            Ok(response) => response,
            Err(_) => return status_response(StatusCode::BAD_REQUEST, "invalid JSON-RPC response"),
        };
        let session = match existing_session(&state, &headers).await {
            Ok(session) => session,
            Err(response) => return *response,
        };
        return if session.complete_client_request(response) {
            StatusCode::ACCEPTED.into_response()
        } else {
            status_response(StatusCode::BAD_REQUEST, "unknown JSON-RPC response id")
        };
    }
    if value.get("id").is_some() {
        let request: JsonRpcRequest = match serde_json::from_value(value) {
            Ok(request) => request,
            Err(_) => return status_response(StatusCode::BAD_REQUEST, "invalid JSON-RPC request"),
        };
        return handle_http_request(&state, &headers, request).await;
    }
    let notification: JsonRpcNotification = match serde_json::from_value(value) {
        Ok(notification) => notification,
        Err(_) => {
            return status_response(StatusCode::BAD_REQUEST, "invalid JSON-RPC notification");
        }
    };
    let session = match existing_session(&state, &headers).await {
        Ok(session) => session,
        Err(response) => return *response,
    };
    match session.mcp.handle_notification(notification) {
        Ok(()) => StatusCode::ACCEPTED.into_response(),
        Err(error) => status_response(StatusCode::BAD_REQUEST, &error.to_string()),
    }
}

async fn handle_http_request(
    state: &Arc<HttpServerInner>,
    headers: &HeaderMap,
    request: JsonRpcRequest,
) -> Response {
    if !accepts_response_mode(headers, state.config.response_mode) {
        return status_response(
            StatusCode::NOT_ACCEPTABLE,
            "Accept does not allow configured response framing",
        );
    }
    let is_initialize = request.method == "initialize";
    let (session, is_new) = if is_initialize {
        if headers.contains_key(MCP_SESSION_ID_HEADER) {
            return status_response(
                StatusCode::BAD_REQUEST,
                "initialize must not reuse an existing session",
            );
        }
        let session = Arc::new(HttpSession::new(state.server.session(), &state.config));
        session.mcp.install_client_peer(Arc::new(HttpClientPeer {
            session: Arc::downgrade(&session),
        }));
        (session, true)
    } else {
        match existing_session(state, headers).await {
            Ok(session) => (session, false),
            Err(response) => return *response,
        }
    };

    let response = session.mcp.handle_request(request).await;
    let initialized = is_new && matches!(response, JsonRpcResponse::Success { .. });
    if initialized {
        state
            .sessions
            .write()
            .await
            .insert(session.id.clone(), Arc::clone(&session));
    }
    response_for_request(
        response,
        initialized.then_some(session.id.as_str()),
        state.config.response_mode,
        headers,
    )
}

async fn get_handler(
    State(state): State<Arc<HttpServerInner>>,
    request: Request<Body>,
) -> Response {
    if let Err(response) = validate_security(&state, request.headers()) {
        return *response;
    }
    if !request
        .headers()
        .get(ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| accept.contains(SSE_MEDIA_TYPE))
    {
        return status_response(
            StatusCode::NOT_ACCEPTABLE,
            "Accept must allow text/event-stream",
        );
    }
    let session = match existing_session(&state, request.headers()).await {
        Ok(session) => session,
        Err(response) => return *response,
    };
    let last_event_id = request
        .headers()
        .get(LAST_EVENT_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let replay = session.replay_after(last_event_id.as_deref()).await;
    let mut receiver = session.sender.subscribe();
    let stream = async_stream::stream! {
        for event in replay {
            yield Ok::<_, Infallible>(sse_event(event));
        }
        loop {
            match receiver.recv().await {
                Ok(event) => yield Ok(sse_event(event)),
                Err(broadcast::error::RecvError::Lagged(_)) => break,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

async fn delete_handler(
    State(state): State<Arc<HttpServerInner>>,
    request: Request<Body>,
) -> Response {
    if let Err(response) = validate_security(&state, request.headers()) {
        return *response;
    }
    if let Err(response) = validate_protocol_header(request.headers()) {
        return *response;
    }
    let Some(session_id) = session_header(request.headers()) else {
        return status_response(StatusCode::BAD_REQUEST, "MCP-Session-Id is required");
    };
    if state.sessions.write().await.remove(session_id).is_some() {
        StatusCode::NO_CONTENT.into_response()
    } else {
        status_response(StatusCode::NOT_FOUND, "MCP session not found")
    }
}

async fn existing_session(
    state: &Arc<HttpServerInner>,
    headers: &HeaderMap,
) -> Result<Arc<HttpSession>, Box<Response>> {
    validate_protocol_header(headers)?;
    let session_id = session_header(headers).ok_or_else(|| {
        Box::new(status_response(
            StatusCode::BAD_REQUEST,
            "MCP-Session-Id is required",
        ))
    })?;
    state
        .sessions
        .read()
        .await
        .get(session_id)
        .cloned()
        .ok_or_else(|| {
            Box::new(status_response(
                StatusCode::NOT_FOUND,
                "MCP session not found",
            ))
        })
}

fn validate_security(state: &HttpServerInner, headers: &HeaderMap) -> Result<(), Box<Response>> {
    if let Some(value) = headers.get(ORIGIN) {
        let Ok(origin) = value.to_str() else {
            return Err(Box::new(status_response(
                StatusCode::FORBIDDEN,
                "request Origin is not allowed",
            )));
        };
        if !state.config.allowed_origins.contains(origin) {
            return Err(Box::new(status_response(
                StatusCode::FORBIDDEN,
                "request Origin is not allowed",
            )));
        }
    }
    if let Some(authorizer) = &state.config.authorizer {
        let bearer = headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        if !authorizer.authorize(bearer) {
            let mut response =
                status_response(StatusCode::UNAUTHORIZED, "bearer authorization required");
            response.headers_mut().insert(
                WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"mcp\""),
            );
            return Err(Box::new(response));
        }
    }
    Ok(())
}

fn validate_protocol_header(headers: &HeaderMap) -> Result<(), Box<Response>> {
    match headers
        .get(MCP_PROTOCOL_VERSION_HEADER)
        .and_then(|value| value.to_str().ok())
    {
        Some(LATEST_PROTOCOL_VERSION) => Ok(()),
        Some(_) => Err(Box::new(status_response(
            StatusCode::BAD_REQUEST,
            "unsupported MCP-Protocol-Version",
        ))),
        None => Err(Box::new(status_response(
            StatusCode::BAD_REQUEST,
            "MCP-Protocol-Version is required",
        ))),
    }
}

fn response_for_request(
    response: JsonRpcResponse,
    session_id: Option<&str>,
    mode: HttpResponseMode,
    request_headers: &HeaderMap,
) -> Response {
    let accepts = request_headers
        .get(ACCEPT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    let mut response = match mode {
        HttpResponseMode::Json if accepts.contains(JSON_MEDIA_TYPE) => {
            axum::Json(response).into_response()
        }
        HttpResponseMode::Sse if accepts.contains(SSE_MEDIA_TYPE) => {
            let Ok(data) = serde_json::to_string(&response) else {
                return status_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "failed to encode JSON-RPC response",
                );
            };
            Sse::new(futures_util::stream::once(async move {
                Ok::<_, Infallible>(Event::default().event("message").data(data))
            }))
            .into_response()
        }
        _ => {
            return status_response(
                StatusCode::NOT_ACCEPTABLE,
                "Accept does not allow configured response framing",
            );
        }
    };
    if let Some(session_id) = session_id {
        if let Ok(value) = HeaderValue::from_str(session_id) {
            response.headers_mut().insert(MCP_SESSION_ID_HEADER, value);
        }
    }
    response
}

fn accepts_response_mode(headers: &HeaderMap, mode: HttpResponseMode) -> bool {
    let accepts = headers
        .get(ACCEPT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    match mode {
        HttpResponseMode::Json => accepts.contains(JSON_MEDIA_TYPE),
        HttpResponseMode::Sse => accepts.contains(SSE_MEDIA_TYPE),
    }
}

fn session_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(MCP_SESSION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
}

fn sse_event(event: ServerEvent) -> Event {
    Event::default()
        .event("message")
        .id(event.id)
        .data(event.data)
}

fn status_response(status: StatusCode, message: &str) -> Response {
    (status, message.to_owned()).into_response()
}
