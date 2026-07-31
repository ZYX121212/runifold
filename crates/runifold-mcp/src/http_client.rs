use eventsource_stream::Eventsource;
use futures_util::{Stream, StreamExt};
use reqwest::{
    Client, Response, StatusCode, Url,
    header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName},
};
use secrecy::ExposeSecret;
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{Mutex, broadcast};

use crate::{
    HttpAuthProvider, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse,
    LATEST_PROTOCOL_VERSION, MCP_METHOD_HEADER, MCP_PROTOCOL_VERSION_HEADER, MCP_SESSION_ID_HEADER,
    McpError, McpTool, McpTransport, PeerRequestHandler, STATELESS_PROTOCOL_VERSION,
    ServerNotificationStream, StatelessCancellation, TransportFuture,
    http_headers::{ToolHeaderRule, compile_tool_header_rules, encode_header_value},
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
                tool_headers: RwLock::new(HashMap::new()),
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

    async fn listen_stateless(
        &self,
        request: JsonRpcRequest,
    ) -> Result<ServerNotificationStream, McpError> {
        let id = request.id.clone();
        let response = self
            .authorize(
                self.inner
                    .client
                    .post(self.inner.endpoint.clone())
                    .header(CONTENT_TYPE, JSON_MEDIA_TYPE)
                    .header(ACCEPT, SSE_MEDIA_TYPE)
                    .header(MCP_PROTOCOL_VERSION_HEADER, STATELESS_PROTOCOL_VERSION)
                    .header(MCP_METHOD_HEADER, "subscriptions/listen")
                    .json(&request),
            )
            .send()
            .await?;
        self.check_status(&response).await?;
        ensure_content_type(response.headers(), SSE_MEDIA_TYPE)?;
        let mut events = response.bytes_stream().eventsource();
        Ok(Box::pin(async_stream::stream! {
            while let Some(event) = events.next().await {
                let event = match event {
                    Ok(event) => event,
                    Err(error) => {
                        yield Err(McpError::protocol(format!(
                            "invalid MCP subscription SSE stream: {error}"
                        )));
                        break;
                    }
                };
                if event.data.trim().is_empty() {
                    continue;
                }
                let value: serde_json::Value = match serde_json::from_str(&event.data) {
                    Ok(value) => value,
                    Err(error) => {
                        yield Err(error.into());
                        break;
                    }
                };
                if value.get("method").is_some() && value.get("id").is_none() {
                    match serde_json::from_value::<JsonRpcNotification>(value) {
                        Ok(notification) => yield Ok(notification),
                        Err(error) => {
                            yield Err(error.into());
                            break;
                        }
                    }
                    continue;
                }
                let response = match serde_json::from_value::<JsonRpcResponse>(value) {
                    Ok(response) => response,
                    Err(error) => {
                        yield Err(error.into());
                        break;
                    }
                };
                if response.id() != &id {
                    yield Err(McpError::protocol(
                        "subscription close response id does not match its request",
                    ));
                    break;
                }
                if let Err(error) = response.into_result() {
                    yield Err(error);
                }
                break;
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
                    let _ = transport
                        .send_json(&response, false, None, None, None, &[])
                        .await;
                });
                continue;
            }
            if value.get("method").is_some()
                && value.get("id").is_none()
                && let Ok(notification) =
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
        protocol_version: Option<&str>,
        method: Option<&str>,
        name: Option<&str>,
        custom_headers: &[(String, String)],
    ) -> Result<Option<JsonRpcResponse>, McpError> {
        let session_id = self.session_id().await;
        let mut request = self
            .inner
            .client
            .post(self.inner.endpoint.clone())
            .header(CONTENT_TYPE, JSON_MEDIA_TYPE)
            .header(ACCEPT, format!("{JSON_MEDIA_TYPE}, {SSE_MEDIA_TYPE}"))
            .json(message);
        if let Some(protocol_version) = protocol_version {
            request = request.header(MCP_PROTOCOL_VERSION_HEADER, protocol_version);
        }
        if let Some(method) = method {
            request = request.header(MCP_METHOD_HEADER, method);
        }
        if let Some(name) = name {
            request = request.header(crate::MCP_NAME_HEADER, encode_header_value(name));
        }
        for (name, value) in custom_headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| McpError::protocol("invalid compiled MCP parameter header name"))?;
            request = request.header(name, value);
        }
        if protocol_version != Some(STATELESS_PROTOCOL_VERSION)
            && let Some(session_id) = session_id
        {
            request = request
                .header(MCP_SESSION_ID_HEADER, session_id)
                .header(MCP_PROTOCOL_VERSION_HEADER, LATEST_PROTOCOL_VERSION);
        }
        let response = self.authorize(request).send().await?;
        if response.status() == StatusCode::ACCEPTED && !expects_response {
            return Ok(None);
        }
        if response.status() == StatusCode::BAD_REQUEST
            && protocol_version == Some(STATELESS_PROTOCOL_VERSION)
            && expects_response
        {
            let body = response.bytes().await?;
            return match serde_json::from_slice::<JsonRpcResponse>(&body) {
                Ok(response) => Ok(Some(response)),
                Err(_) => Err(McpError::HttpStatus {
                    status: StatusCode::BAD_REQUEST.as_u16(),
                    message: "Bad Request".into(),
                }),
            };
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

    fn tool_parameter_headers(
        &self,
        request: &JsonRpcRequest,
    ) -> Result<Vec<(String, String)>, McpError> {
        if request.method != "tools/call" {
            return Ok(Vec::new());
        }
        let params = request
            .params
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| McpError::protocol("tools/call parameters must be an object"))?;
        let name = params
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| McpError::protocol("tools/call omitted its Tool name"))?;
        let rules = self
            .inner
            .tool_headers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(name)
            .cloned()
            .ok_or_else(|| {
                McpError::protocol(
                    "HTTP Tool metadata is not prepared; call tools/list before tools/call",
                )
            })?;
        let empty = serde_json::Map::new();
        let arguments = params
            .get("arguments")
            .and_then(serde_json::Value::as_object)
            .unwrap_or(&empty);
        let mut headers = Vec::with_capacity(rules.len());
        for rule in &rules {
            let value = rule
                .encoded_value(arguments)
                .map_err(|error| McpError::protocol(error.to_string()))?;
            if let Some(value) = value {
                headers.push((rule.header_name(), value));
            }
        }
        Ok(headers)
    }
}

impl McpTransport for StreamableHttpTransport {
    fn request(&self, request: JsonRpcRequest) -> TransportFuture<'_, JsonRpcResponse> {
        Box::pin(async move {
            let is_stateless = request
                .params
                .as_ref()
                .and_then(serde_json::Value::as_object)
                .and_then(|params| params.get("_meta"))
                .and_then(serde_json::Value::as_object)
                .and_then(|metadata| metadata.get("io.modelcontextprotocol/protocolVersion"))
                .and_then(serde_json::Value::as_str)
                == Some(STATELESS_PROTOCOL_VERSION);
            let name = is_stateless.then(|| request_name(&request)).flatten();
            let custom_headers = if is_stateless {
                self.tool_parameter_headers(&request)?
            } else {
                Vec::new()
            };
            self.send_json(
                &request,
                true,
                is_stateless.then_some(STATELESS_PROTOCOL_VERSION),
                is_stateless.then_some(request.method.as_str()),
                name,
                &custom_headers,
            )
            .await?
            .ok_or_else(|| McpError::protocol("MCP request returned no response"))
        })
    }

    fn notify(&self, notification: JsonRpcNotification) -> TransportFuture<'_, ()> {
        Box::pin(async move {
            self.send_json(&notification, false, None, None, None, &[])
                .await?;
            Ok(())
        })
    }

    fn stateless_cancellation(&self) -> StatelessCancellation {
        StatelessCancellation::DropRequest
    }

    fn prepare_tools(&self, tools: Vec<McpTool>) -> Result<Vec<McpTool>, McpError> {
        let mut prepared = Vec::with_capacity(tools.len());
        let mut cache = self
            .inner
            .tool_headers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for tool in tools {
            let Ok(rules) = compile_tool_header_rules(&tool.input_schema) else {
                cache.remove(&tool.name);
                continue;
            };
            cache.insert(tool.name.clone(), rules);
            prepared.push(tool);
        }
        Ok(prepared)
    }

    fn subscribe(&self) -> TransportFuture<'_, ServerNotificationStream> {
        Box::pin(async move { StreamableHttpTransport::subscribe(self).await })
    }

    fn listen(&self, request: JsonRpcRequest) -> TransportFuture<'_, ServerNotificationStream> {
        Box::pin(async move { self.listen_stateless(request).await })
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

fn request_name(request: &JsonRpcRequest) -> Option<&str> {
    let key = match request.method.as_str() {
        "tools/call" | "prompts/get" => "name",
        "resources/read" => "uri",
        "tasks/get" | "tasks/update" | "tasks/cancel" => "taskId",
        _ => return None,
    };
    request
        .params
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .and_then(|params| params.get(key))
        .and_then(serde_json::Value::as_str)
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
    tool_headers: RwLock<HashMap<String, Vec<ToolHeaderRule>>>,
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

#[cfg(test)]
mod task_routing_tests {
    use serde_json::json;

    use super::request_name;
    use crate::{JsonRpcRequest, RequestId};

    #[test]
    fn task_operations_route_by_exact_task_id() {
        for method in ["tasks/get", "tasks/update", "tasks/cancel"] {
            let request = JsonRpcRequest::new(
                RequestId::Number(1),
                method,
                Some(json!({"taskId": "opaque-task"})),
            );
            assert_eq!(request_name(&request), Some("opaque-task"));
        }
    }
}
