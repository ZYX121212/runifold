use super::{
    ACCEPT, AUTHORIZATION, Event, HEADER_MISMATCH, HeaderMap, HeaderValue, HttpResponseMode,
    HttpServerInner, INTERNAL_ERROR, INVALID_PARAMS, Infallible, IntoResponse, JSON_MEDIA_TYPE,
    JsonRpcRequest, JsonRpcResponse, LATEST_PROTOCOL_VERSION, MCP_METHOD_HEADER, MCP_NAME_HEADER,
    MCP_PROTOCOL_VERSION_HEADER, MCP_SESSION_ID_HEADER, McpServer, ORIGIN, RequestId, Response,
    SSE_MEDIA_TYPE, STATELESS_PROTOCOL_VERSION, ServerEvent, Sse, StatusCode,
    UNSUPPORTED_PROTOCOL_VERSION, Value, WWW_AUTHENTICATE, compile_tool_header_rules,
    decode_header_value,
};

pub(super) fn validate_security(
    state: &HttpServerInner,
    headers: &HeaderMap,
) -> Result<(), Box<Response>> {
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

pub(super) fn validate_protocol_header(headers: &HeaderMap) -> Result<(), Box<Response>> {
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

pub(super) fn validate_stateless_headers(
    headers: &HeaderMap,
    request: &JsonRpcRequest,
    server: &McpServer,
) -> Result<(), Box<Response>> {
    let metadata = request
        .params
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|params| params.get("_meta"))
        .and_then(Value::as_object)
        .ok_or_else(|| {
            Box::new(rpc_error_status_response(
                StatusCode::BAD_REQUEST,
                request.id.clone(),
                INVALID_PARAMS,
                "stateless request metadata must be an object",
                None,
            ))
        })?;
    let body_version = metadata
        .get("io.modelcontextprotocol/protocolVersion")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            Box::new(rpc_error_status_response(
                StatusCode::BAD_REQUEST,
                request.id.clone(),
                INVALID_PARAMS,
                "stateless request metadata omitted protocol version",
                None,
            ))
        })?;
    if body_version != STATELESS_PROTOCOL_VERSION {
        return Err(Box::new(rpc_error_status_response(
            StatusCode::BAD_REQUEST,
            request.id.clone(),
            UNSUPPORTED_PROTOCOL_VERSION,
            "unsupported protocol version",
            Some(serde_json::json!({
                "requested": body_version,
                "supported": crate::SUPPORTED_PROTOCOL_VERSIONS,
            })),
        )));
    }
    validate_mirrored_header(headers, MCP_PROTOCOL_VERSION_HEADER, body_version, request)?;
    validate_mirrored_header(headers, MCP_METHOD_HEADER, &request.method, request)?;
    if let Some(expected_name) = stateless_request_name(request)? {
        validate_mirrored_header(headers, MCP_NAME_HEADER, expected_name, request)?;
    }
    validate_tool_parameter_headers(headers, request, server)?;
    Ok(())
}

fn validate_tool_parameter_headers(
    headers: &HeaderMap,
    request: &JsonRpcRequest,
    server: &McpServer,
) -> Result<(), Box<Response>> {
    if request.method != "tools/call" {
        return Ok(());
    }
    let params = request
        .params
        .as_ref()
        .and_then(Value::as_object)
        .ok_or_else(|| {
            Box::new(rpc_error_status_response(
                StatusCode::BAD_REQUEST,
                request.id.clone(),
                INVALID_PARAMS,
                "tools/call parameters must be an object",
                None,
            ))
        })?;
    let name = params.get("name").and_then(Value::as_str).ok_or_else(|| {
        Box::new(rpc_error_status_response(
            StatusCode::BAD_REQUEST,
            request.id.clone(),
            INVALID_PARAMS,
            "tools/call omitted `name`",
            None,
        ))
    })?;
    let Some(schema) = server.tool_input_schema(name) else {
        return Ok(());
    };
    let rules = compile_tool_header_rules(&schema).map_err(|error| {
        Box::new(rpc_error_status_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            request.id.clone(),
            INTERNAL_ERROR,
            format!("Tool `{name}` has an invalid x-mcp-header schema: {error}"),
            None,
        ))
    })?;
    let empty = serde_json::Map::new();
    let arguments = params
        .get("arguments")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    for rule in rules {
        let header_name = rule.header_name();
        let actual = headers
            .get(header_name.as_str())
            .and_then(|value| value.to_str().ok());
        let matches = rule.matches(arguments, actual).map_err(|error| {
            Box::new(rpc_error_status_response(
                StatusCode::BAD_REQUEST,
                request.id.clone(),
                HEADER_MISMATCH,
                error.to_string(),
                None,
            ))
        })?;
        if !matches {
            return Err(Box::new(rpc_error_status_response(
                StatusCode::BAD_REQUEST,
                request.id.clone(),
                HEADER_MISMATCH,
                format!("{header_name} header does not match the request body"),
                None,
            )));
        }
    }
    Ok(())
}

fn stateless_request_name(request: &JsonRpcRequest) -> Result<Option<&str>, Box<Response>> {
    let field = match request.method.as_str() {
        "tools/call" | "prompts/get" => "name",
        "resources/read" => "uri",
        "tasks/get" | "tasks/update" | "tasks/cancel" => "taskId",
        _ => return Ok(None),
    };
    request
        .params
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|params| params.get(field))
        .and_then(Value::as_str)
        .map(Some)
        .ok_or_else(|| {
            Box::new(rpc_error_status_response(
                StatusCode::BAD_REQUEST,
                request.id.clone(),
                INVALID_PARAMS,
                format!("{} omitted `{field}`", request.method),
                None,
            ))
        })
}

fn validate_mirrored_header(
    headers: &HeaderMap,
    header_name: &'static str,
    expected: &str,
    request: &JsonRpcRequest,
) -> Result<(), Box<Response>> {
    let actual = headers
        .get(header_name)
        .and_then(|value| value.to_str().ok())
        .and_then(decode_header_value);
    if actual.as_deref() == Some(expected) {
        return Ok(());
    }
    Err(Box::new(rpc_error_status_response(
        StatusCode::BAD_REQUEST,
        request.id.clone(),
        HEADER_MISMATCH,
        format!("{header_name} header does not match the request body"),
        None,
    )))
}

fn rpc_error_status_response(
    status: StatusCode,
    id: RequestId,
    code: i64,
    message: impl Into<String>,
    data: Option<Value>,
) -> Response {
    json_rpc_status_response(status, JsonRpcResponse::error(id, code, message, data))
}

pub(super) fn json_rpc_status_response(status: StatusCode, response: JsonRpcResponse) -> Response {
    let mut response = axum::Json(response).into_response();
    *response.status_mut() = status;
    response
}

pub(super) fn response_for_request(
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

pub(super) fn accepts_response_mode(headers: &HeaderMap, mode: HttpResponseMode) -> bool {
    let accepts = headers
        .get(ACCEPT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    match mode {
        HttpResponseMode::Json => accepts.contains(JSON_MEDIA_TYPE),
        HttpResponseMode::Sse => accepts.contains(SSE_MEDIA_TYPE),
    }
}

pub(super) fn session_header(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(MCP_SESSION_ID_HEADER)
        .and_then(|value| value.to_str().ok())
}

pub(super) fn sse_event(event: ServerEvent) -> Event {
    Event::default()
        .event("message")
        .id(event.id)
        .data(event.data)
}

pub(super) fn status_response(status: StatusCode, message: &str) -> Response {
    (status, message.to_owned()).into_response()
}
