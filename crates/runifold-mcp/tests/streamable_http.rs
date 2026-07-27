//! Real-loopback conformance tests for MCP Streamable HTTP.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::{StreamExt, future::join_all};
use runifold_core::{Budget, BudgetTracker, CapabilitySet, EffectClass, RiskLevel, RunContext};
use runifold_mcp::{
    CallToolParams, HttpResponseMode, Implementation, JsonRpcNotification, McpClient,
    McpClientConfig, McpError, McpErrorKind, McpHttpServer, McpHttpServerConfig, McpServer,
    StaticBearerAuth, StreamableHttpTransport,
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
