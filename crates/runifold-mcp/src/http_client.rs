use std::{
    pin::Pin,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use eventsource_stream::Eventsource;
use futures_util::{Stream, StreamExt};
use reqwest::{
    Client, Response, StatusCode, Url,
    header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap},
};
use secrecy::ExposeSecret;
use tokio::sync::{Mutex, broadcast};

use crate::{
    HttpAuthProvider, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse,
    LATEST_PROTOCOL_VERSION, MCP_PROTOCOL_VERSION_HEADER, MCP_SESSION_ID_HEADER, McpError,
    McpTransport, PeerRequestHandler, ServerNotificationStream, TransportFuture,
};

const LAST_EVENT_ID_HEADER: &str = "last-event-id";
const JSON_MEDIA_TYPE: &str = "application/json";
const SSE_MEDIA_TYPE: &str = "text/event-stream";
type ServerMessageStream = Pin<Box<dyn Stream<Item = Result<serde_json::Value, McpError>> + Send>>;

/// MCP Streamable HTTP client transport.
///
/// The transport never retries requests. In particular, a tool call that sees
/// an expired session returns [`McpError::SessionExpired`] to the caller.
#[derive(Clone)]
pub struct StreamableHttpTransport {
    inner: Arc<HttpTransportInner>,
}

impl StreamableHttpTransport {
    /// Creates a transport for one MCP endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when `endpoint` is not an HTTP(S) URL.
    pub fn new(endpoint: impl AsRef<str>) -> Result<Self, McpError> {
        Self::with_client(Client::new(), endpoint)
    }

    /// Creates a transport with an explicitly configured HTTP client.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when `endpoint` is not an HTTP(S) URL.
    pub fn with_client(client: Client, endpoint: impl AsRef<str>) -> Result<Self, McpError> {
        let endpoint = Url::parse(endpoint.as_ref())
            .map_err(|error| McpError::protocol(format!("invalid MCP HTTP endpoint: {error}")))?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err(McpError::protocol(
                "MCP HTTP endpoint must use `http` or `https`",
            ));
        }
        Ok(Self {
            inner: Arc::new(HttpTransportInner {
                client,
                endpoint,
                auth: RwLock::new(None),
                state: Mutex::new(HttpClientState::default()),
                peer_handler: RwLock::new(None),
                peer_started: AtomicBool::new(false),
                notifications: broadcast::channel(256).0,
            }),
        })
    }

    /// Configures a dynamic bearer-token provider.
    #[must_use]
    pub fn with_auth(self, auth: Arc<dyn HttpAuthProvider>) -> Self {
        *self
            .inner
            .auth
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(auth);
        self
    }

    /// Returns the active opaque session identifier.
    pub async fn session_id(&self) -> Option<String> {
        self.inner.state.lock().await.session_id.clone()
    }

    /// Opens the server-to-client SSE notification channel.
    ///
    /// If a prior stream yielded event IDs, the most recent ID is sent through
    /// `Last-Event-ID` so the server can replay only this session's missed
    /// notifications.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for authentication, session, status, or transport
    /// failures. A server may reject GET with status 405.
    pub async fn subscribe(&self) -> Result<ServerNotificationStream, McpError> {
        if self.inner.peer_started.load(Ordering::Acquire) {
            return Ok(notification_broadcast_stream(
                self.inner.notifications.subscribe(),
            ));
        }
        let mut messages = self.open_server_messages().await?;
        Ok(Box::pin(async_stream::try_stream! {
            while let Some(value) = messages.next().await {
                let value = value?;
                if value.get("method").is_some() && value.get("id").is_none() {
                    yield serde_json::from_value::<JsonRpcNotification>(value)?;
                } else {
                    Err(McpError::protocol(
                        "received a server request without an installed peer handler",
                    ))?;
                }
            }
        }))
    }

    async fn open_server_messages(&self) -> Result<ServerMessageStream, McpError> {
        let (session_id, last_event_id) = {
            let state = self.inner.state.lock().await;
            (
                state
                    .session_id
                    .clone()
                    .ok_or_else(|| McpError::lifecycle("HTTP session is not initialized"))?,
                state.last_event_id.clone(),
            )
        };
        let mut request = self
            .inner
            .client
            .get(self.inner.endpoint.clone())
            .header(ACCEPT, SSE_MEDIA_TYPE)
            .header(MCP_SESSION_ID_HEADER, session_id)
            .header(MCP_PROTOCOL_VERSION_HEADER, LATEST_PROTOCOL_VERSION);
        if let Some(last_event_id) = last_event_id {
            request = request.header(LAST_EVENT_ID_HEADER, last_event_id);
        }
        request = self.authorize(request);
        let response = request.send().await?;
        self.check_status(&response).await?;
        ensure_content_type(response.headers(), SSE_MEDIA_TYPE)?;

        let state = Arc::clone(&self.inner);
        let mut events = response.bytes_stream().eventsource();
        Ok(Box::pin(async_stream::try_stream! {
            while let Some(event) = events.next().await {
                let event = event.map_err(|error| {
                    McpError::protocol(format!("invalid MCP SSE stream: {error}"))
                })?;
                if !event.id.is_empty() {
                    state.state.lock().await.last_event_id = Some(event.id);
                }
                if event.data.trim().is_empty() {
                    continue;
                }
                yield serde_json::from_str::<serde_json::Value>(&event.data)?;
            }
        }))
    }

    async fn run_peer(&self, mut messages: ServerMessageStream) {
        while let Some(value) = messages.next().await {
            let Ok(value) = value else {
                break;
            };
            if value.get("method").is_some() && value.get("id").is_some() {
                let Ok(request) = serde_json::from_value::<JsonRpcRequest>(value) else {
                    continue;
                };
                let handler = self
                    .inner
                    .peer_handler
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                let Some(handler) = handler else {
                    continue;
                };
                let transport = self.clone();
                tokio::spawn(async move {
                    let id = request.id.clone();
                    let response = handler.handle(request).await.unwrap_or_else(|_| {
                        JsonRpcResponse::error(id, -32603, "client peer request failed", None)
                    });
                    let _ = transport.send_json(&response, false).await;
                });
                continue;
            }
            if value.get("method").is_some() && value.get("id").is_none() {
                if let Ok(notification) =
                    serde_json::from_value::<JsonRpcNotification>(value.clone())
                {
                    if let Some(handler) = self
                        .inner
                        .peer_handler
                        .read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .as_ref()
                    {
                        let _ = handler.notify(notification.clone());
                    }
                    let _ = self.inner.notifications.send(notification);
                }
            }
        }
        self.inner.peer_started.store(false, Ordering::Release);
    }

    /// Explicitly deletes the current server session.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] if the peer rejects or cannot process the request.
    pub async fn delete_session(&self) -> Result<(), McpError> {
        let Some(session_id) = self.session_id().await else {
            return Ok(());
        };
        let request = self
            .inner
            .client
            .delete(self.inner.endpoint.clone())
            .header(MCP_SESSION_ID_HEADER, session_id)
            .header(MCP_PROTOCOL_VERSION_HEADER, LATEST_PROTOCOL_VERSION);
        let response = self.authorize(request).send().await?;
        if response.status() != StatusCode::NO_CONTENT {
            self.check_status(&response).await?;
        }
        *self.inner.state.lock().await = HttpClientState::default();
        Ok(())
    }

    async fn send_json<T: serde::Serialize>(
        &self,
        message: &T,
        expects_response: bool,
    ) -> Result<Option<JsonRpcResponse>, McpError> {
        let session_id = self.session_id().await;
        let mut request = self
            .inner
            .client
            .post(self.inner.endpoint.clone())
            .header(CONTENT_TYPE, JSON_MEDIA_TYPE)
            .header(ACCEPT, format!("{JSON_MEDIA_TYPE}, {SSE_MEDIA_TYPE}"))
            .json(message);
        if let Some(session_id) = session_id {
            request = request
                .header(MCP_SESSION_ID_HEADER, session_id)
                .header(MCP_PROTOCOL_VERSION_HEADER, LATEST_PROTOCOL_VERSION);
        }
        let response = self.authorize(request).send().await?;
        if response.status() == StatusCode::ACCEPTED && !expects_response {
            return Ok(None);
        }
        self.check_status(&response).await?;

        if let Some(session_id) = response
            .headers()
            .get(MCP_SESSION_ID_HEADER)
            .and_then(|value| value.to_str().ok())
        {
            self.inner.state.lock().await.session_id = Some(session_id.to_owned());
        }
        if !expects_response {
            return Err(McpError::protocol(
                "MCP notification response must use HTTP 202",
            ));
        }
        let content_type = content_type(response.headers())?.to_owned();
        if content_type.starts_with(SSE_MEDIA_TYPE) {
            return parse_sse_response(response).await.map(Some);
        }
        if content_type.starts_with(JSON_MEDIA_TYPE) {
            return response
                .json::<JsonRpcResponse>()
                .await
                .map(Some)
                .map_err(Into::into);
        }
        Err(McpError::protocol(format!(
            "unsupported MCP HTTP response content type `{content_type}`"
        )))
    }

    fn authorize(&self, mut request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let token = self
            .inner
            .auth
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|provider| provider.bearer_token());
        if let Some(token) = token {
            request = request.header(AUTHORIZATION, format!("Bearer {}", token.expose_secret()));
        }
        request
    }

    async fn check_status(&self, response: &Response) -> Result<(), McpError> {
        match response.status() {
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(McpError::Authentication),
            StatusCode::NOT_FOUND if self.session_id().await.is_some() => {
                self.inner.state.lock().await.session_id = None;
                Err(McpError::SessionExpired)
            }
            status if !status.is_success() => Err(McpError::HttpStatus {
                status: status.as_u16(),
                message: status.canonical_reason().unwrap_or("HTTP error").to_owned(),
            }),
            _ => Ok(()),
        }
    }
}

impl McpTransport for StreamableHttpTransport {
    fn request(&self, request: JsonRpcRequest) -> TransportFuture<'_, JsonRpcResponse> {
        Box::pin(async move {
            self.send_json(&request, true)
                .await?
                .ok_or_else(|| McpError::protocol("MCP request returned no response"))
        })
    }

    fn notify(&self, notification: JsonRpcNotification) -> TransportFuture<'_, ()> {
        Box::pin(async move {
            self.send_json(&notification, false).await?;
            Ok(())
        })
    }

    fn subscribe(&self) -> TransportFuture<'_, ServerNotificationStream> {
        Box::pin(async move { StreamableHttpTransport::subscribe(self).await })
    }

    fn install_peer_handler(&self, handler: Arc<dyn PeerRequestHandler>) -> Result<(), McpError> {
        let mut installed = self
            .inner
            .peer_handler
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if installed.is_some() {
            return Err(McpError::lifecycle(
                "HTTP peer request handler is already installed",
            ));
        }
        *installed = Some(handler);
        Ok(())
    }

    fn start_peer(&self) -> TransportFuture<'_, ()> {
        Box::pin(async move {
            if self
                .inner
                .peer_started
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Ok(());
            }
            let messages = match self.open_server_messages().await {
                Ok(messages) => messages,
                Err(error) => {
                    self.inner.peer_started.store(false, Ordering::Release);
                    return Err(error);
                }
            };
            let transport = self.clone();
            tokio::spawn(async move {
                transport.run_peer(messages).await;
            });
            Ok(())
        })
    }
}

impl std::fmt::Debug for StreamableHttpTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamableHttpTransport")
            .field("endpoint", &self.inner.endpoint)
            .field(
                "auth",
                &self
                    .inner
                    .auth
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                    .map(|_| "[REDACTED]"),
            )
            .finish_non_exhaustive()
    }
}

struct HttpTransportInner {
    client: Client,
    endpoint: Url,
    auth: RwLock<Option<Arc<dyn HttpAuthProvider>>>,
    state: Mutex<HttpClientState>,
    peer_handler: RwLock<Option<Arc<dyn PeerRequestHandler>>>,
    peer_started: AtomicBool,
    notifications: broadcast::Sender<JsonRpcNotification>,
}

#[derive(Default)]
struct HttpClientState {
    session_id: Option<String>,
    last_event_id: Option<String>,
}

fn notification_broadcast_stream(
    mut receiver: broadcast::Receiver<JsonRpcNotification>,
) -> ServerNotificationStream {
    Box::pin(async_stream::stream! {
        loop {
            match receiver.recv().await {
                Ok(notification) => yield Ok(notification),
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    yield Err(McpError::protocol(format!(
                        "HTTP notification receiver lagged by {skipped} messages"
                    )));
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}

async fn parse_sse_response(response: Response) -> Result<JsonRpcResponse, McpError> {
    let mut events = response.bytes_stream().eventsource();
    while let Some(event) = events.next().await {
        let event = event
            .map_err(|error| McpError::protocol(format!("invalid MCP SSE response: {error}")))?;
        if event.data.trim().is_empty() {
            continue;
        }
        return serde_json::from_str(&event.data).map_err(Into::into);
    }
    Err(McpError::protocol(
        "MCP SSE response ended before a JSON-RPC response",
    ))
}

fn ensure_content_type(headers: &HeaderMap, expected: &str) -> Result<(), McpError> {
    let actual = content_type(headers)?;
    if actual.starts_with(expected) {
        Ok(())
    } else {
        Err(McpError::protocol(format!(
            "expected `{expected}` but received `{actual}`"
        )))
    }
}

fn content_type(headers: &HeaderMap) -> Result<&str, McpError> {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| McpError::protocol("MCP HTTP response omitted Content-Type"))
}
