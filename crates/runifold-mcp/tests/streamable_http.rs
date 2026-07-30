//! Real-loopback conformance tests for MCP Streamable HTTP.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{StreamExt, future::join_all};
use runifold_core::{Budget, BudgetTracker, CapabilitySet, EffectClass, RiskLevel, RunContext};
use runifold_mcp::{
    CallToolParams, ClientCapabilities, HttpResponseMode, Implementation, InputRequest,
    InputRequiredResult, InputResponseFuture, JsonRpcNotification, MCP_METHOD_HEADER,
    MCP_NAME_HEADER, MCP_PROTOCOL_VERSION_HEADER, McpClient, McpClientConfig, McpError,
    McpErrorKind, McpHttpServer, McpHttpServerConfig, McpProtocolMode, McpServer, MrtrInputHandler,
    MrtrToolDecision, MrtrToolFuture, MrtrToolGate, MrtrToolRequest, StaticBearerAuth,
    StreamableHttpTransport,
};
use runifold_tool::{
    Tool, ToolContext, ToolDescriptor, ToolError, ToolFuture, ToolOutput, ToolRegistry,
};
use secrecy::SecretString;
use serde_json::{Value, json};
use tokio::task::JoinHandle;

#[tokio::test(flavor = "multi_thread")]
async fn json_transport_authenticates_and_handles_concurrent_requests() {
    let auth = Arc::new(StaticBearerAuth::new(SecretString::from(
        "correct-token".to_owned(),
    )));
    let (http_server, endpoint, task) = spawn_server(
        empty_server(),
        McpHttpServerConfig::new().with_authorizer(auth.clone()),
    )
    .await;
    let transport = Arc::new(
        StreamableHttpTransport::new(&endpoint)
            .unwrap()
            .with_auth(auth),
    );
    let client = McpClient::new(
        transport.clone(),
        McpClientConfig::new(Implementation::new("http-client", "1")),
    );

    client.initialize().await.unwrap();
    assert!(transport.session_id().await.is_some());
    assert_eq!(http_server.session_count().await, 1);
    let calls = (0..64).map(|_| {
        let client = client.clone();
        async move { client.list_tools().await.unwrap() }
    });
    assert!(
        join_all(calls)
            .await
            .into_iter()
            .all(|tools| tools.is_empty())
    );

    task.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn discovery_uses_stateless_http_without_allocating_a_session() {
    let (http_server, endpoint, task) =
        spawn_server(empty_server(), McpHttpServerConfig::new()).await;
    let transport = Arc::new(StreamableHttpTransport::new(&endpoint).unwrap());
    let client = McpClient::new(
        transport.clone(),
        McpClientConfig::new(Implementation::new("http-discovery-client", "1")),
    );

    let discovered = client.discover().await.unwrap();
    assert_eq!(discovered.supported_versions, ["2026-07-28", "2025-11-25"]);
    assert_eq!(http_server.session_count().await, 0);
    assert!(transport.session_id().await.is_none());

    client.initialize().await.unwrap();
    assert_eq!(http_server.session_count().await, 1);
    assert!(transport.session_id().await.is_some());
    task.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn stateless_http_requests_never_allocate_or_reuse_a_session() {
    let (http_server, endpoint, task) =
        spawn_server(empty_server(), McpHttpServerConfig::new()).await;
    let transport = Arc::new(StreamableHttpTransport::new(&endpoint).unwrap());
    let client = McpClient::new(
        transport.clone(),
        McpClientConfig::new(Implementation::new("modern-http-client", "1")),
    );

    assert_eq!(client.connect().await.unwrap(), McpProtocolMode::Stateless);
    assert!(client.list_tools().await.unwrap().is_empty());
    assert!(client.list_tools().await.unwrap().is_empty());
    assert_eq!(http_server.session_count().await, 0);
    assert!(transport.session_id().await.is_none());
    task.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn stateless_http_rejects_header_mismatch_and_enriches_results() {
    let (http_server, endpoint, task) =
        spawn_server(empty_server(), McpHttpServerConfig::new()).await;
    let body = json!({
        "jsonrpc": "2.0",
        "id": 7,
        "method": "tools/list",
        "params": {
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientInfo": {
                    "name": "raw-client",
                    "version": "1"
                },
                "io.modelcontextprotocol/clientCapabilities": {}
            }
        }
    });
    let http = reqwest::Client::new();
    let mismatched = http
        .post(&endpoint)
        .header("accept", "application/json, text/event-stream")
        .header(MCP_PROTOCOL_VERSION_HEADER, "2026-07-28")
        .header(MCP_METHOD_HEADER, "prompts/list")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(mismatched.status(), reqwest::StatusCode::BAD_REQUEST);
    let error = mismatched.json::<serde_json::Value>().await.unwrap();
    assert_eq!(error["error"]["code"], -32020);

    let valid = http
        .post(&endpoint)
        .header("accept", "application/json, text/event-stream")
        .header(MCP_PROTOCOL_VERSION_HEADER, "2026-07-28")
        .header(MCP_METHOD_HEADER, "tools/list")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(valid.status(), reqwest::StatusCode::OK);
    assert!(valid.headers().get("mcp-session-id").is_none());
    let result = valid.json::<serde_json::Value>().await.unwrap();
    assert_eq!(result["result"]["resultType"], "complete");
    assert_eq!(
        result["result"]["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
        "http-server"
    );
    assert_eq!(http_server.session_count().await, 0);
    task.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn stateless_tool_headers_are_compiled_filtered_and_verified_end_to_end() {
    let (server, valid_name) = header_tool_server();
    let (http_server, endpoint, task) = spawn_server(server, McpHttpServerConfig::new()).await;
    let transport = Arc::new(StreamableHttpTransport::new(&endpoint).unwrap());
    let client = McpClient::new(
        transport,
        McpClientConfig::new(Implementation::new("header-client", "1")),
    );

    assert_eq!(client.connect().await.unwrap(), McpProtocolMode::Stateless);
    let tools = client.list_tools().await.unwrap();
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        vec![valid_name]
    );

    let arguments = serde_json::Map::from_iter([(
        "routing".to_owned(),
        json!({
            "region": "华东",
            "shard": 7,
            "active": true,
            "note": "=?base64?literal?="
        }),
    )]);
    let calls = (0..32).map(|_| {
        let client = client.clone();
        let arguments = arguments.clone();
        async move {
            client
                .call_tool(CallToolParams {
                    name: valid_name.to_owned(),
                    arguments: Some(arguments),
                })
                .await
                .unwrap()
                .structured_content
        }
    });
    assert!(join_all(calls).await.into_iter().all(|output| output
        == Some(json!({"routing": {
            "region": "华东",
            "shard": 7,
            "active": true,
            "note": "=?base64?literal?="
        }}))));
    assert_eq!(http_server.session_count().await, 0);

    let body = stateless_tool_call_body(valid_name, &json!({"routing": {"region": "east"}}));
    let tampered = reqwest::Client::new()
        .post(&endpoint)
        .header("accept", "application/json, text/event-stream")
        .header(MCP_PROTOCOL_VERSION_HEADER, "2026-07-28")
        .header(MCP_METHOD_HEADER, "tools/call")
        .header(MCP_NAME_HEADER, valid_name)
        .header("mcp-param-region", "west")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(tampered.status(), reqwest::StatusCode::BAD_REQUEST);
    let error = tampered.json::<Value>().await.unwrap();
    assert_eq!(error["error"]["code"], -32020);

    let malformed_body = stateless_tool_call_body(valid_name, &json!({"routing": {}}));
    let malformed = reqwest::Client::new()
        .post(&endpoint)
        .header("accept", "application/json, text/event-stream")
        .header(MCP_PROTOCOL_VERSION_HEADER, "2026-07-28")
        .header(MCP_METHOD_HEADER, "tools/call")
        .header(MCP_NAME_HEADER, valid_name)
        .header("mcp-param-region", "=?base64?not-base64?=")
        .json(&malformed_body)
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), reqwest::StatusCode::BAD_REQUEST);
    let error = malformed.json::<Value>().await.unwrap();
    assert_eq!(error["error"]["code"], -32020);

    let valid = reqwest::Client::new()
        .post(&endpoint)
        .header("accept", "application/json, text/event-stream")
        .header(MCP_PROTOCOL_VERSION_HEADER, "2026-07-28")
        .header(MCP_METHOD_HEADER, "tools/call")
        .header(MCP_NAME_HEADER, valid_name)
        .header("mcp-param-region", "east")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(valid.status(), reqwest::StatusCode::OK);
    task.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn stateless_http_mrtr_echoes_opaque_state_and_executes_tool_once() {
    let gate_calls = Arc::new(AtomicUsize::new(0));
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let server = mrtr_server(gate_calls.clone(), tool_calls.clone());
    let (_http_server, endpoint, task) = spawn_server(server, McpHttpServerConfig::new()).await;

    let unsupported = McpClient::new(
        Arc::new(StreamableHttpTransport::new(&endpoint).unwrap()),
        McpClientConfig::new(Implementation::new("no-input-client", "1")),
    );
    unsupported.connect().await.unwrap();
    unsupported.list_tools().await.unwrap();
    let error = unsupported
        .call_tool(CallToolParams {
            name: "mrtr-echo".into(),
            arguments: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(error, McpError::Remote { code: -32021, .. }));
    assert_eq!(tool_calls.load(Ordering::Acquire), 0);

    let client = McpClient::new(
        Arc::new(StreamableHttpTransport::new(&endpoint).unwrap()),
        McpClientConfig::new(Implementation::new("mrtr-client", "1"))
            .with_mrtr_input_handler(Arc::new(TestInputHandler)),
    );
    client.connect().await.unwrap();
    client.list_tools().await.unwrap();
    let result = client
        .call_tool(CallToolParams {
            name: "mrtr-echo".into(),
            arguments: Some(serde_json::Map::from_iter([(
                "operation".into(),
                json!("deploy"),
            )])),
        })
        .await
        .unwrap();

    assert_eq!(
        result.structured_content,
        Some(json!({"operation": "deploy"}))
    );
    assert_eq!(gate_calls.load(Ordering::Acquire), 3);
    assert_eq!(tool_calls.load(Ordering::Acquire), 1);
    task.abort();
}

#[tokio::test]
async fn mrtr_state_only_retries_are_bounded_before_tool_execution() {
    let gate_calls = Arc::new(AtomicUsize::new(0));
    let tool_calls = Arc::new(AtomicUsize::new(0));
    let tool = Arc::new(CountingEchoTool::new(tool_calls.clone()));
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(tool.descriptor().capability());
    let mut registry = ToolRegistry::new();
    registry.register(tool).unwrap();
    let server = McpServer::new(
        Arc::new(registry),
        RunContext::root(BudgetTracker::new(Budget::default()), capabilities),
        Implementation::new("bounded-mrtr-server", "1"),
    )
    .with_mrtr_tool_gate(
        "mrtr-echo",
        Arc::new(StateOnlyGate {
            calls: gate_calls.clone(),
        }),
    );
    let client = McpClient::new(
        Arc::new(server.session()),
        McpClientConfig::new(Implementation::new("bounded-mrtr-client", "1"))
            .with_max_mrtr_rounds(2),
    );

    client.connect().await.unwrap();
    let error = client
        .call_tool(CallToolParams {
            name: "mrtr-echo".into(),
            arguments: None,
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), McpErrorKind::Protocol);
    assert_eq!(gate_calls.load(Ordering::Acquire), 3);
    assert_eq!(tool_calls.load(Ordering::Acquire), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn sse_response_and_resumable_notification_stream_work_over_tcp() {
    let (http_server, endpoint, task) = spawn_server(
        empty_server(),
        McpHttpServerConfig::new().with_response_mode(HttpResponseMode::Sse),
    )
    .await;
    let transport = Arc::new(StreamableHttpTransport::new(&endpoint).unwrap());
    let client = McpClient::new(
        transport.clone(),
        McpClientConfig::new(Implementation::new("sse-client", "1")),
    );
    client.initialize().await.unwrap();
    assert!(client.list_tools().await.unwrap().is_empty());
    let session_id = transport.session_id().await.unwrap();

    http_server
        .send_notification(
            &session_id,
            JsonRpcNotification::new("notifications/tools/list_changed", None),
        )
        .await
        .unwrap();
    let mut first_stream = transport.subscribe().await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), first_stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.method, "notifications/tools/list_changed");
    drop(first_stream);

    http_server
        .send_notification(
            &session_id,
            JsonRpcNotification::new("notifications/progress", Some(json!({"progress": 1}))),
        )
        .await
        .unwrap();
    let mut resumed = transport.subscribe().await.unwrap();
    let replayed = tokio::time::timeout(Duration::from_secs(2), resumed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(replayed.method, "notifications/progress");
    drop(resumed);

    // Closing an SSE connection is not request cancellation or session loss.
    assert!(client.list_tools().await.unwrap().is_empty());
    task.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn secure_defaults_reject_unknown_origin_and_missing_credentials() {
    let auth = Arc::new(StaticBearerAuth::new(SecretString::from(
        "correct-token".to_owned(),
    )));
    let (_server, endpoint, task) = spawn_server(
        empty_server(),
        McpHttpServerConfig::new()
            .with_authorizer(auth)
            .with_allowed_origin("https://allowed.example"),
    )
    .await;
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-11-25",
            "capabilities": {},
            "clientInfo": {"name": "raw", "version": "1"}
        }
    });
    let client = reqwest::Client::new();
    let unauthorized = client
        .post(&endpoint)
        .header("accept", "application/json, text/event-stream")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), reqwest::StatusCode::UNAUTHORIZED);

    let forbidden = client
        .post(&endpoint)
        .bearer_auth("correct-token")
        .header("origin", "https://evil.example")
        .header("accept", "application/json, text/event-stream")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(forbidden.status(), reqwest::StatusCode::FORBIDDEN);

    let allowed = client
        .post(&endpoint)
        .bearer_auth("correct-token")
        .header("origin", "https://allowed.example")
        .header("accept", "application/json, text/event-stream")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(allowed.status(), reqwest::StatusCode::OK);
    task.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn lost_session_is_explicit_and_never_hidden_by_retry() {
    let (http_server, endpoint, task) =
        spawn_server(empty_server(), McpHttpServerConfig::new()).await;
    let transport = Arc::new(StreamableHttpTransport::new(&endpoint).unwrap());
    let client = McpClient::new(
        transport.clone(),
        McpClientConfig::new(Implementation::new("session-client", "1")),
    );
    client.initialize().await.unwrap();
    let session_id = transport.session_id().await.unwrap();

    let deleted = reqwest::Client::new()
        .delete(&endpoint)
        .header("mcp-session-id", session_id)
        .header("mcp-protocol-version", "2025-11-25")
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), reqwest::StatusCode::NO_CONTENT);
    assert_eq!(http_server.session_count().await, 0);

    let error = client.list_tools().await.unwrap_err();
    assert!(matches!(error, McpError::SessionExpired));
    assert_eq!(error.kind(), McpErrorKind::SessionExpired);
    assert_eq!(http_server.session_count().await, 0);
    task.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn http_deadline_sends_explicit_cancellation() {
    let cancelled = Arc::new(AtomicBool::new(false));
    let tool = Arc::new(WaitingTool::new(cancelled.clone()));
    let descriptor = tool.descriptor().clone();
    let mut registry = ToolRegistry::new();
    registry.register(tool).unwrap();
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(descriptor.capability());
    let server = McpServer::new(
        Arc::new(registry),
        RunContext::root(BudgetTracker::new(Budget::default()), capabilities),
        Implementation::new("slow-server", "1"),
    );
    let (_http_server, endpoint, task) = spawn_server(server, McpHttpServerConfig::new()).await;
    let transport = Arc::new(StreamableHttpTransport::new(&endpoint).unwrap());
    let client = McpClient::new(
        transport,
        McpClientConfig::new(Implementation::new("timeout-client", "1"))
            .with_request_timeout(Duration::from_millis(40)),
    );
    client.initialize().await.unwrap();

    let error = client
        .call_tool(CallToolParams {
            name: "wait".into(),
            arguments: None,
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind(), McpErrorKind::DeadlineExceeded);
    tokio::time::timeout(Duration::from_secs(2), async {
        while !cancelled.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
}

async fn spawn_server(
    server: McpServer,
    config: McpHttpServerConfig,
) -> (McpHttpServer, String, JoinHandle<()>) {
    let http_server = McpHttpServer::new(server, config);
    let router = http_server.router("/mcp");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (http_server, format!("http://{address}/mcp"), task)
}

fn empty_server() -> McpServer {
    McpServer::new(
        Arc::new(ToolRegistry::new()),
        RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new()),
        Implementation::new("http-server", "1"),
    )
}

fn header_tool_server() -> (McpServer, &'static str) {
    let valid = Arc::new(HeaderEchoTool::valid());
    let invalid = Arc::new(HeaderEchoTool::invalid());
    let mut registry = ToolRegistry::new();
    registry.register(valid.clone()).unwrap();
    registry.register(invalid.clone()).unwrap();
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(valid.descriptor().capability());
    capabilities.grant(invalid.descriptor().capability());
    (
        McpServer::new(
            Arc::new(registry),
            RunContext::root(BudgetTracker::new(Budget::default()), capabilities),
            Implementation::new("header-server", "1"),
        ),
        "header-echo",
    )
}

fn stateless_tool_call_body(name: &str, arguments: &Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 91,
        "method": "tools/call",
        "params": {
            "name": name,
            "arguments": arguments,
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientInfo": {
                    "name": "raw-header-client",
                    "version": "1"
                },
                "io.modelcontextprotocol/clientCapabilities": {}
            }
        }
    })
}

#[derive(Debug)]
struct HeaderEchoTool {
    descriptor: ToolDescriptor,
}

impl HeaderEchoTool {
    fn valid() -> Self {
        Self::new(
            "header-echo",
            json!({
                "type": "object",
                "properties": {
                    "routing": {
                        "type": "object",
                        "properties": {
                            "region": {"type": "string", "x-mcp-header": "Region"},
                            "shard": {"type": "integer", "x-mcp-header": "Shard"},
                            "active": {"type": "boolean", "x-mcp-header": "Active"},
                            "note": {"type": "string", "x-mcp-header": "Note"}
                        }
                    }
                }
            }),
        )
    }

    fn invalid() -> Self {
        Self::new(
            "invalid-header-tool",
            json!({
                "type": "object",
                "properties": {
                    "values": {
                        "type": "array",
                        "items": {"type": "string", "x-mcp-header": "Value"}
                    }
                }
            }),
        )
    }

    fn new(name: &str, input_schema: Value) -> Self {
        Self {
            descriptor: ToolDescriptor {
                id: runifold_core::CapabilityId::new(),
                name: name.to_owned(),
                version: "1".into(),
                description: "Echo input while exercising MCP parameter headers".into(),
                input_schema,
                output_schema: json!({"type": "object"}),
                effect: EffectClass::ReadOnly,
                risk: RiskLevel::Low,
                metadata: BTreeMap::new(),
            },
        }
    }
}

impl Tool for HeaderEchoTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        input: Value,
        _context: ToolContext,
    ) -> ToolFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async move { Ok(ToolOutput::model_visible(input)) })
    }
}

fn mrtr_server(gate_calls: Arc<AtomicUsize>, tool_calls: Arc<AtomicUsize>) -> McpServer {
    let tool = Arc::new(CountingEchoTool::new(tool_calls));
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(tool.descriptor().capability());
    let mut registry = ToolRegistry::new();
    registry.register(tool).unwrap();
    McpServer::new(
        Arc::new(registry),
        RunContext::root(BudgetTracker::new(Budget::default()), capabilities),
        Implementation::new("mrtr-server", "1"),
    )
    .with_mrtr_tool_gate("mrtr-echo", Arc::new(TestMrtrGate { calls: gate_calls }))
}

#[derive(Debug)]
struct TestInputHandler;

impl MrtrInputHandler for TestInputHandler {
    fn capabilities(&self) -> ClientCapabilities {
        ClientCapabilities {
            roots: Some(BTreeMap::new()),
            elicitation: Some(BTreeMap::new()),
            ..ClientCapabilities::default()
        }
    }

    fn handle(
        &self,
        key: String,
        _request: InputRequest,
        _cancellation: runifold_core::CancellationToken,
    ) -> InputResponseFuture<'_> {
        Box::pin(async move {
            match key.as_str() {
                "approval" => Ok(json!({"action": "accept", "content": {"approved": true}})),
                "roots" => {
                    Ok(json!({"roots": [{"uri": "file:///workspace", "name": "workspace"}]}))
                }
                _ => Err(McpError::Cancelled),
            }
        })
    }
}

#[derive(Debug)]
struct TestMrtrGate {
    calls: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct StateOnlyGate {
    calls: Arc<AtomicUsize>,
}

impl MrtrToolGate for StateOnlyGate {
    fn evaluate(&self, _request: MrtrToolRequest) -> MrtrToolFuture<'_> {
        let round = self.calls.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            Ok(MrtrToolDecision::InputRequired(
                InputRequiredResult::new(BTreeMap::new())
                    .with_request_state(format!("state-{round}")),
            ))
        })
    }
}

impl MrtrToolGate for TestMrtrGate {
    fn evaluate(&self, request: MrtrToolRequest) -> MrtrToolFuture<'_> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move {
            if request.request_state.is_none() {
                assert!(request.input_responses.is_empty());
                return Ok(MrtrToolDecision::InputRequired(
                    InputRequiredResult::new(BTreeMap::from([
                        (
                            "approval".into(),
                            InputRequest::new(
                                "elicitation/create",
                                Some(json!({"mode": "form", "message": "Approve deployment?"})),
                            ),
                        ),
                        (
                            "roots".into(),
                            InputRequest::new("roots/list", Some(json!({}))),
                        ),
                    ]))
                    .with_request_state("opaque-state-123"),
                ));
            }
            assert_eq!(request.request_state.as_deref(), Some("opaque-state-123"));
            assert_eq!(
                request.input_responses["approval"]["action"],
                json!("accept")
            );
            assert_eq!(
                request.input_responses["roots"]["roots"][0]["uri"],
                json!("file:///workspace")
            );
            Ok(MrtrToolDecision::Proceed)
        })
    }
}

#[derive(Debug)]
struct CountingEchoTool {
    descriptor: ToolDescriptor,
    calls: Arc<AtomicUsize>,
}

impl CountingEchoTool {
    fn new(calls: Arc<AtomicUsize>) -> Self {
        Self {
            descriptor: ToolDescriptor {
                id: runifold_core::CapabilityId::new(),
                name: "mrtr-echo".into(),
                version: "1".into(),
                description: "Execute only after MRTR input".into(),
                input_schema: json!({"type": "object"}),
                output_schema: json!({"type": "object"}),
                effect: EffectClass::ReadOnly,
                risk: RiskLevel::Low,
                metadata: BTreeMap::new(),
            },
            calls,
        }
    }
}

impl Tool for CountingEchoTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        input: Value,
        _context: ToolContext,
    ) -> ToolFuture<'_, Result<ToolOutput, ToolError>> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Box::pin(async move { Ok(ToolOutput::model_visible(input)) })
    }
}

#[derive(Debug)]
struct WaitingTool {
    descriptor: ToolDescriptor,
    cancelled: Arc<AtomicBool>,
}

impl WaitingTool {
    fn new(cancelled: Arc<AtomicBool>) -> Self {
        Self {
            descriptor: ToolDescriptor {
                id: runifold_core::CapabilityId::new(),
                name: "wait".into(),
                version: "1".into(),
                description: "Wait until cancelled".into(),
                input_schema: json!({"type": "object"}),
                output_schema: json!({"type": "object"}),
                effect: EffectClass::ReadOnly,
                risk: RiskLevel::Low,
                metadata: BTreeMap::new(),
            },
            cancelled,
        }
    }
}

impl Tool for WaitingTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        _input: Value,
        context: ToolContext,
    ) -> ToolFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async move {
            let _drop_signal = DropSignal(self.cancelled.clone());
            context.cancellation().cancelled().await;
            Err(ToolError::local(
                runifold_tool::ToolErrorKind::Cancelled,
                "cancelled",
            ))
        })
    }
}

struct DropSignal(Arc<AtomicBool>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
