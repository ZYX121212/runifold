use super::validation::{
    accepts_response_mode, json_rpc_status_response, response_for_request, session_header,
    sse_event, status_response, validate_protocol_header, validate_security,
    validate_stateless_headers,
};
use super::{
    ACCEPT, Arc, Body, CONTENT_TYPE, Duration, Event, HeaderMap, HttpClientPeer, HttpServerInner,
    HttpSession, INVALID_PARAMS, Infallible, IntoResponse, JSON_MEDIA_TYPE, JsonRpcNotification,
    JsonRpcRequest, JsonRpcResponse, KeepAlive, LAST_EVENT_ID_HEADER, MCP_PROTOCOL_VERSION_HEADER,
    MCP_SESSION_ID_HEADER, METHOD_NOT_FOUND, MISSING_REQUIRED_CLIENT_CAPABILITY, McpSession,
    Request, Response, SSE_MEDIA_TYPE, STATELESS_PROTOCOL_VERSION, Sse, State, StatusCode,
    UNSUPPORTED_PROTOCOL_VERSION, Value, broadcast, to_bytes,
};

pub(super) async fn post_handler(
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
    let is_modern_listen = request.method == "subscriptions/listen"
        && request
            .params
            .as_ref()
            .and_then(Value::as_object)
            .is_some_and(|params| params.contains_key("_meta"));
    let accepts_listen = headers
        .get(ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| accept.contains(SSE_MEDIA_TYPE));
    if (is_modern_listen && !accepts_listen)
        || (!is_modern_listen && !accepts_response_mode(headers, state.config.response_mode))
    {
        return status_response(
            StatusCode::NOT_ACCEPTABLE,
            "Accept does not allow the required response framing",
        );
    }
    let is_stateless = request
        .params
        .as_ref()
        .and_then(Value::as_object)
        .is_some_and(|params| params.contains_key("_meta"));
    if is_stateless {
        if let Err(response) = validate_stateless_headers(headers, &request, &state.server) {
            return *response;
        }
        let session = state.server.session();
        if request.method == "subscriptions/listen" {
            return subscription_http_response(&session, request);
        }
        let response = session.handle_request(request).await;
        if matches!(
            response,
            JsonRpcResponse::Error {
                error: crate::JsonRpcError {
                    code: METHOD_NOT_FOUND,
                    ..
                },
                ..
            }
        ) {
            return json_rpc_status_response(StatusCode::NOT_FOUND, response);
        }
        if matches!(
            response,
            JsonRpcResponse::Error {
                error: crate::JsonRpcError {
                    code: INVALID_PARAMS
                        | MISSING_REQUIRED_CLIENT_CAPABILITY
                        | UNSUPPORTED_PROTOCOL_VERSION,
                    ..
                },
                ..
            }
        ) {
            return json_rpc_status_response(StatusCode::BAD_REQUEST, response);
        }
        return response_for_request(response, None, state.config.response_mode, headers);
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

fn subscription_http_response(session: &McpSession, request: JsonRpcRequest) -> Response {
    match session.open_subscription(request) {
        Ok(mut notifications) => {
            let stream = async_stream::stream! {
                use futures_util::StreamExt;
                while let Some(notification) = notifications.next().await {
                    match notification {
                        Ok(notification) => {
                            match serde_json::to_string(&notification) {
                                Ok(data) => yield Ok::<_, Infallible>(
                                    Event::default().event("message").data(data)
                                ),
                                Err(_) => break,
                            }
                        }
                        Err(_) => break,
                    }
                }
            };
            Sse::new(stream)
                .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
                .into_response()
        }
        Err(response) => json_rpc_status_response(StatusCode::BAD_REQUEST, response),
    }
}

pub(super) async fn get_handler(
    State(state): State<Arc<HttpServerInner>>,
    request: Request<Body>,
) -> Response {
    if let Err(response) = validate_security(&state, request.headers()) {
        return *response;
    }
    if request
        .headers()
        .get(MCP_PROTOCOL_VERSION_HEADER)
        .and_then(|value| value.to_str().ok())
        == Some(STATELESS_PROTOCOL_VERSION)
    {
        return status_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "modern MCP notifications require subscriptions/listen",
        );
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

pub(super) async fn delete_handler(
    State(state): State<Arc<HttpServerInner>>,
    request: Request<Body>,
) -> Response {
    if let Err(response) = validate_security(&state, request.headers()) {
        return *response;
    }
    if request
        .headers()
        .get(MCP_PROTOCOL_VERSION_HEADER)
        .and_then(|value| value.to_str().ok())
        == Some(STATELESS_PROTOCOL_VERSION)
    {
        return status_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "modern MCP has no HTTP sessions",
        );
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
