//! MCP Tools integration and transport conformance tests.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use futures_util::future::join_all;
use runifold_core::{
    Budget, BudgetTracker, CapabilityId, CapabilitySet, EffectClass, InMemoryJournal, RiskLevel,
    RunContext, RunEventKind,
};
use runifold_mcp::{
    CallToolParams, Implementation, JsonRpcRequest, JsonRpcResponse, McpClient, McpClientConfig,
    McpErrorKind, McpProtocolMode, McpRemoteTool, McpResultType, McpServer, McpTool,
    RemoteToolPolicy, RequestId, STATELESS_PROTOCOL_VERSION, StdioTransport, serve_io,
};
use runifold_tool::{
    Tool, ToolContext, ToolDescriptor, ToolError, ToolErrorKind, ToolFuture, ToolOutput,
    ToolRegistry,
};
use serde_json::{Value, json};
use tokio::io::{BufReader, split};

#[tokio::test]
async fn lifecycle_and_authority_filter_tool_discovery_and_calls() {
    let echo = Arc::new(EchoTool::new("echo"));
    let secret = Arc::new(EchoTool::new("secret"));
    let mut registry = ToolRegistry::new();
    registry.register(echo.clone()).unwrap();
    registry.register(secret).unwrap();
    let authority = authority_for([echo.as_ref().descriptor()]);
    let session = McpServer::new(
        Arc::new(registry),
        authority,
        Implementation::new("test-server", "1"),
    )
    .session();
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("test-client", "1")),
    );

    let error = client.list_tools().await.unwrap_err();
    assert_eq!(error.kind(), McpErrorKind::Lifecycle);

    let initialized = client.initialize().await.unwrap();
    assert!(initialized.capabilities.tools.is_some());
    let tools = client.list_tools().await.unwrap();
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        vec!["echo"]
    );
    let output = client
        .call_tool(CallToolParams {
            name: "echo".into(),
            arguments: Some(serde_json::Map::from_iter([("x".into(), json!(1))])),
        })
        .await
        .unwrap();
    assert_eq!(output.structured_content, Some(json!({"x": 1})));

    let error = client
        .call_tool(CallToolParams {
            name: "secret".into(),
            arguments: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        runifold_mcp::McpError::Remote { code: -32601, .. }
    ));
}

#[tokio::test]
async fn tool_calls_emit_redacted_mcp_domain_events() {
    let echo = Arc::new(EchoTool::new("echo"));
    let mut registry = ToolRegistry::new();
    registry.register(echo.clone()).unwrap();
    let journal = InMemoryJournal::new();
    let authority =
        authority_for([echo.as_ref().descriptor()]).with_journal(Arc::new(journal.clone()));
    let client = McpClient::new(
        Arc::new(
            McpServer::new(
                Arc::new(registry),
                authority,
                Implementation::new("observed-server", "1"),
            )
            .session(),
        ),
        McpClientConfig::new(Implementation::new("observed-client", "1")),
    );
    client.initialize().await.unwrap();
    client
        .call_tool(CallToolParams {
            name: "echo".into(),
            arguments: Some(serde_json::Map::from_iter([(
                "secret".into(),
                json!("not-recorded"),
            )])),
        })
        .await
        .unwrap();

    let events = journal.events();
    let domains = events
        .iter()
        .filter_map(|event| match &event.kind {
            RunEventKind::Domain(domain) if domain.namespace == "runifold.mcp" => Some(domain),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        domains
            .iter()
            .map(|domain| domain.name.as_str())
            .collect::<Vec<_>>(),
        vec!["tool.started", "tool.completed"]
    );
    assert!(
        domains
            .iter()
            .all(|domain| !domain.payload.to_string().contains("not-recorded"))
    );
}

#[tokio::test]
async fn unsupported_versions_fail_during_initialization() {
    let session = McpServer::new(
        Arc::new(ToolRegistry::new()),
        authority_for([]),
        Implementation::new("test-server", "1"),
    )
    .session();
    let response = session
        .handle_request(JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: RequestId::Number(1),
            method: "initialize".into(),
            params: Some(json!({
                "protocolVersion": "2099-01-01",
                "capabilities": {},
                "clientInfo": {"name": "future", "version": "1"}
            })),
        })
        .await;

    assert!(matches!(
        response,
        JsonRpcResponse::Error {
            error: runifold_mcp::JsonRpcError { code: -32602, .. },
            ..
        }
    ));
}

#[tokio::test]
async fn discovery_is_stateless_and_preserves_legacy_initialization() {
    let session = McpServer::new(
        Arc::new(ToolRegistry::new()),
        authority_for([]),
        Implementation::new("discoverable-server", "1"),
    )
    .session();
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("discovering-client", "1")),
    );

    let discovered = client.discover().await.unwrap();
    assert_eq!(discovered.result_type, McpResultType::Complete);
    assert_eq!(discovered.supported_versions, ["2026-07-28", "2025-11-25"]);
    assert_eq!(discovered.metadata.server_info.name, "discoverable-server");

    let initialized = client.initialize().await.unwrap();
    assert_eq!(initialized.protocol_version, "2025-11-25");
}

#[tokio::test]
async fn stateless_connect_lists_and_calls_tools_without_initialization() {
    let echo = Arc::new(EchoTool::new("echo"));
    let mut registry = ToolRegistry::new();
    registry.register(echo.clone()).unwrap();
    let client = McpClient::new(
        Arc::new(
            McpServer::new(
                Arc::new(registry),
                authority_for([echo.as_ref().descriptor()]),
                Implementation::new("stateless-server", "1"),
            )
            .session(),
        ),
        McpClientConfig::new(Implementation::new("stateless-client", "1")),
    );

    assert_eq!(client.connect().await.unwrap(), McpProtocolMode::Stateless);
    assert_eq!(
        client.server_info().await.unwrap().protocol_version,
        STATELESS_PROTOCOL_VERSION
    );
    assert_eq!(client.list_tools().await.unwrap().len(), 1);
    let result = client
        .call_tool(CallToolParams {
            name: "echo".into(),
            arguments: Some(serde_json::Map::from_iter([(
                "mode".into(),
                json!("stateless"),
            )])),
        })
        .await
        .unwrap();
    assert_eq!(
        result.structured_content,
        Some(json!({"mode": "stateless"}))
    );
}

#[tokio::test]
async fn discovery_requires_stateless_request_metadata() {
    let session = McpServer::new(
        Arc::new(ToolRegistry::new()),
        authority_for([]),
        Implementation::new("test-server", "1"),
    )
    .session();
    let response = session
        .handle_request(JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: RequestId::Number(1),
            method: "server/discover".into(),
            params: Some(json!({
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": STATELESS_PROTOCOL_VERSION
                }
            })),
        })
        .await;

    assert!(matches!(
        response,
        JsonRpcResponse::Error {
            error: runifold_mcp::JsonRpcError { code: -32602, .. },
            ..
        }
    ));
}

#[tokio::test]
async fn remote_tools_preserve_explicit_host_risk_and_execute_canonically() {
    let echo = Arc::new(EchoTool::new("echo"));
    let mut server_registry = ToolRegistry::new();
    server_registry.register(echo.clone()).unwrap();
    let session = McpServer::new(
        Arc::new(server_registry),
        authority_for([echo.as_ref().descriptor()]),
        Implementation::new("test-server", "1"),
    )
    .session();
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("test-client", "1")),
    );
    client.initialize().await.unwrap();
    let remote = McpRemoteTool::new(
        client.clone(),
        client.list_tools().await.unwrap().remove(0),
        RemoteToolPolicy::new(EffectClass::ReadOnly, RiskLevel::Medium),
    )
    .unwrap();
    assert_eq!(remote.descriptor().effect, EffectClass::ReadOnly);
    assert_eq!(remote.descriptor().risk, RiskLevel::Medium);

    let remote = Arc::new(remote);
    let mut local_registry = ToolRegistry::new();
    local_registry.register(remote.clone()).unwrap();
    let run = authority_for([remote.as_ref().descriptor()]);
    let output = local_registry
        .invoke("echo", json!({"question": "hello"}), &run)
        .await
        .unwrap();

    assert_eq!(output.value, json!({"question": "hello"}));
}

#[tokio::test]
async fn scoped_deadline_cancels_the_remote_tool_request() {
    let observed_cancellation = Arc::new(AtomicBool::new(false));
    let slow = Arc::new(SlowTool::new(Arc::clone(&observed_cancellation)));
    let mut server_registry = ToolRegistry::new();
    server_registry.register(slow.clone()).unwrap();
    let session = McpServer::new(
        Arc::new(server_registry),
        authority_for([slow.as_ref().descriptor()]),
        Implementation::new("test-server", "1"),
    )
    .session();
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("test-client", "1"))
            .with_request_timeout(Duration::from_secs(2)),
    );
    client.initialize().await.unwrap();
    let remote = Arc::new(
        McpRemoteTool::new(
            client,
            McpTool {
                name: "slow".into(),
                title: None,
                description: Some("Wait for cancellation".into()),
                input_schema: json!({"type": "object"}),
                output_schema: Some(json!({"type": "object"})),
                annotations: Some(json!({"readOnlyHint": true})),
            },
            RemoteToolPolicy::new(EffectClass::NonIdempotentWrite, RiskLevel::High),
        )
        .unwrap(),
    );
    let mut local_registry = ToolRegistry::new();
    local_registry.register(remote.clone()).unwrap();
    let run = authority_for([remote.as_ref().descriptor()])
        .with_deadline(Instant::now() + Duration::from_millis(20));

    let error = local_registry
        .invoke("slow", json!({}), &run)
        .await
        .unwrap_err();
    assert_eq!(error.kind, ToolErrorKind::DeadlineExceeded);
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(observed_cancellation.load(Ordering::SeqCst));
}

#[tokio::test]
async fn non_visible_local_output_never_crosses_mcp() {
    let hidden = Arc::new(HiddenOutputTool::new());
    let mut registry = ToolRegistry::new();
    registry.register(hidden.clone()).unwrap();
    let session = McpServer::new(
        Arc::new(registry),
        authority_for([hidden.as_ref().descriptor()]),
        Implementation::new("test-server", "1"),
    )
    .session();
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("test-client", "1")),
    );
    client.initialize().await.unwrap();

    let result = client
        .call_tool(CallToolParams {
            name: "hidden".into(),
            arguments: None,
        })
        .await
        .unwrap();

    assert!(result.is_error);
    assert!(!format!("{result:?}").contains("private-value"));
}

#[tokio::test]
async fn stdio_transport_multiplexes_concurrent_tool_calls() {
    let echo = Arc::new(EchoTool::new("echo"));
    let mut registry = ToolRegistry::new();
    registry.register(echo.clone()).unwrap();
    let session = McpServer::new(
        Arc::new(registry),
        authority_for([echo.as_ref().descriptor()]),
        Implementation::new("test-server", "1"),
    )
    .session();
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let (server_reader, server_writer) = split(server_io);
    let server_task = tokio::spawn(serve_io(
        session,
        BufReader::new(server_reader),
        server_writer,
    ));
    let (client_reader, client_writer) = split(client_io);
    let transport = Arc::new(StdioTransport::from_io(
        BufReader::new(client_reader),
        client_writer,
    ));
    let client = McpClient::new(
        transport.clone(),
        McpClientConfig::new(Implementation::new("test-client", "1"))
            .with_request_timeout(Duration::from_secs(1)),
    );
    let discovered = client.discover().await.unwrap();
    assert_eq!(discovered.supported_versions, ["2026-07-28", "2025-11-25"]);
    client.initialize().await.unwrap();

    let calls = (0..32).map(|index| {
        let client = client.clone();
        async move {
            client
                .call_tool(CallToolParams {
                    name: "echo".into(),
                    arguments: Some(serde_json::Map::from_iter([("index".into(), json!(index))])),
                })
                .await
        }
    });
    let results = join_all(calls).await;

    for (index, result) in results.into_iter().enumerate() {
        assert_eq!(
            result.unwrap().structured_content,
            Some(json!({"index": index}))
        );
    }
    drop(client);
    transport.shutdown().await.unwrap();
    drop(transport);
    tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

fn authority_for<'a>(descriptors: impl IntoIterator<Item = &'a ToolDescriptor>) -> RunContext {
    let mut capabilities = CapabilitySet::new();
    for descriptor in descriptors {
        capabilities.grant(descriptor.capability());
    }
    RunContext::root(BudgetTracker::new(Budget::default()), capabilities)
}

#[derive(Debug)]
struct EchoTool {
    descriptor: ToolDescriptor,
}

impl EchoTool {
    fn new(name: &str) -> Self {
        Self {
            descriptor: descriptor(name, "Echo structured input"),
        }
    }
}

impl Tool for EchoTool {
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

#[derive(Debug)]
struct SlowTool {
    descriptor: ToolDescriptor,
    observed_cancellation: Arc<AtomicBool>,
}

impl SlowTool {
    fn new(observed_cancellation: Arc<AtomicBool>) -> Self {
        Self {
            descriptor: descriptor("slow", "Wait for cancellation"),
            observed_cancellation,
        }
    }
}

impl Tool for SlowTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        _input: Value,
        context: ToolContext,
    ) -> ToolFuture<'_, Result<ToolOutput, ToolError>> {
        let observed_cancellation = Arc::clone(&self.observed_cancellation);
        Box::pin(async move {
            let _drop_signal = DropSignal(Arc::clone(&observed_cancellation));
            context.cancellation().cancelled().await;
            observed_cancellation.store(true, Ordering::SeqCst);
            Err(ToolError::local(
                ToolErrorKind::Cancelled,
                "slow tool cancelled",
            ))
        })
    }
}

struct DropSignal(Arc<AtomicBool>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[derive(Debug)]
struct HiddenOutputTool {
    descriptor: ToolDescriptor,
}

impl HiddenOutputTool {
    fn new() -> Self {
        Self {
            descriptor: descriptor("hidden", "Return host-only data"),
        }
    }
}

impl Tool for HiddenOutputTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        _input: Value,
        _context: ToolContext,
    ) -> ToolFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async move {
            Ok(ToolOutput {
                value: json!({"secret": "private-value"}),
                model_visible: false,
            })
        })
    }
}

fn descriptor(name: &str, description: &str) -> ToolDescriptor {
    ToolDescriptor {
        id: CapabilityId::new(),
        name: name.into(),
        version: "1".into(),
        description: description.into(),
        input_schema: json!({"type": "object"}),
        output_schema: json!({"type": "object"}),
        effect: EffectClass::Pure,
        risk: RiskLevel::Low,
        metadata: BTreeMap::new(),
    }
}
