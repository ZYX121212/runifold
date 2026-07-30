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
    McpError, McpSamplingClient, McpServer, McpSession, RequestId, STATELESS_PROTOCOL_VERSION,
    TransportFuture,
    http_headers::{compile_tool_header_rules, decode_header_value},
    transport::ClientPeerTransport,
};

mod handlers;
mod validation;

use handlers::{delete_handler, get_handler, post_handler};

/// HTTP header carrying an opaque MCP session identifier.
pub const MCP_SESSION_ID_HEADER: &str = "mcp-session-id";
/// HTTP header carrying the negotiated MCP protocol revision.
pub const MCP_PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";
/// HTTP header carrying the MCP request method for stateless routing.
pub const MCP_METHOD_HEADER: &str = "mcp-method";
/// HTTP header carrying a Tool, Resource, or Prompt routing name.
pub const MCP_NAME_HEADER: &str = "mcp-name";

const LAST_EVENT_ID_HEADER: &str = "last-event-id";
const JSON_MEDIA_TYPE: &str = "application/json";
const SSE_MEDIA_TYPE: &str = "text/event-stream";
const DEFAULT_MAX_BODY_BYTES: usize = 1024 * 1024;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
const INTERNAL_ERROR: i64 = -32603;
const HEADER_MISMATCH: i64 = -32020;
const MISSING_REQUIRED_CLIENT_CAPABILITY: i64 = -32021;
const UNSUPPORTED_PROTOCOL_VERSION: i64 = -32022;

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
