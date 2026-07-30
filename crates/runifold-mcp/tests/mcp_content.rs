//! Capability and transport conformance for MCP Resources and Prompts.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::future::join_all;
use runifold_core::{Budget, BudgetTracker, CapabilityId, CapabilitySet, RunContext};
use runifold_mcp::{
    GetPromptResult, Implementation, McpClient, McpClientConfig, McpError, McpHttpServer,
    McpHttpServerConfig, McpProtocolMode, McpServer, PromptArgument, PromptDescriptor,
    PromptFuture, PromptHandler, PromptMessage, PromptRegistry, ReadResourceResult, RequestId,
    ResourceContents, ResourceDescriptor, ResourceErrorKind, ResourceFuture, ResourceHandler,
    ResourceRegistry, StaticTextResource, StdioTransport, StreamableHttpTransport, serve_io,
};
use runifold_tool::ToolRegistry;
use tokio::io::{BufReader, split};

#[tokio::test]
async fn in_process_discovery_filters_authority_and_validates_prompt_arguments() {
    let fixture = content_fixture();
    let client = McpClient::new(
        Arc::new(fixture.server.session()),
        McpClientConfig::new(Implementation::new("content-client", "1")),
    );
    let initialized = client.initialize().await.unwrap();
    assert!(initialized.capabilities.resources.is_some());
    assert!(initialized.capabilities.prompts.is_some());
    assert!(initialized.capabilities.resources.unwrap().subscribe);

    let resources = client.list_resources().await.unwrap();
    assert_eq!(
        resources
            .iter()
            .map(|resource| resource.uri.as_str())
            .collect::<Vec<_>>(),
        vec!["docs://public/readme"]
    );
    let read = client.read_resource("docs://public/readme").await.unwrap();
    assert_eq!(read.contents[0].uri(), "docs://public/readme");

    let hidden = client
        .read_resource("docs://private/secret")
        .await
        .unwrap_err();
    assert!(matches!(hidden, McpError::Remote { code: -32002, .. }));

    let prompts = client.list_prompts().await.unwrap();
    assert_eq!(prompts.len(), 1);
    assert_eq!(prompts[0].name, "review");
    let missing = client
        .get_prompt("review", BTreeMap::new())
        .await
        .unwrap_err();
    assert!(matches!(missing, McpError::Remote { code: -32602, .. }));
    let rendered = client
        .get_prompt(
            "review",
            BTreeMap::from([("code".into(), "fn main() {}".into())]),
        )
        .await
        .unwrap();
    assert_eq!(rendered.messages.len(), 1);
    assert_eq!(
        rendered.messages[0].content.as_text(),
        Some("Review this code:\nfn main() {}")
    );
}

#[tokio::test]
async fn stdio_preserves_resources_and_prompts_without_transport_specific_types() {
    let fixture = content_fixture();
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let (server_reader, server_writer) = split(server_io);
    let server_task = tokio::spawn(serve_io(
        fixture.server.session(),
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
        McpClientConfig::new(Implementation::new("stdio-content-client", "1")),
    );

    assert_eq!(client.connect().await.unwrap(), McpProtocolMode::Stateless);
    assert_eq!(client.list_resources().await.unwrap().len(), 1);
    assert_eq!(client.list_prompts().await.unwrap().len(), 1);
    assert_eq!(
        client
            .read_resource("docs://public/readme")
            .await
            .unwrap()
            .contents
            .len(),
        1
    );

    drop(client);
    transport.shutdown().await.unwrap();
    drop(transport);
    tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn streamable_http_handles_concurrent_resource_reads_and_prompt_renders() {
    let fixture = content_fixture();
    let http_server = McpHttpServer::new(fixture.server, McpHttpServerConfig::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
    let router = http_server.router("/mcp");
    let server_task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = McpClient::new(
        Arc::new(StreamableHttpTransport::new(endpoint).unwrap()),
        McpClientConfig::new(Implementation::new("http-content-client", "1")),
    );
    assert_eq!(client.connect().await.unwrap(), McpProtocolMode::Stateless);

    let reads = (0..32).map(|_| {
        let client = client.clone();
        async move { client.read_resource("docs://public/readme").await.unwrap() }
    });
    assert!(
        join_all(reads)
            .await
            .iter()
            .all(|result| result.contents.len() == 1)
    );
    let prompts = (0..32).map(|index| {
        let client = client.clone();
        async move {
            client
                .get_prompt(
                    "review",
                    BTreeMap::from([("code".into(), format!("fn item_{index}() {{}}"))]),
                )
                .await
                .unwrap()
        }
    });
    assert!(
        join_all(prompts)
            .await
            .iter()
            .all(|result| result.messages.len() == 1)
    );
    server_task.abort();
}

#[tokio::test]
async fn registries_reject_invalid_uri_oversized_content_and_unknown_arguments() {
    let invalid =
        ResourceDescriptor::new(CapabilityId::new(), "relative/path", "bad", "1").unwrap_err();
    assert!(format!("{invalid}").contains("invalid resource URI"));

    let descriptor =
        ResourceDescriptor::new(CapabilityId::new(), "memory://large", "large", "1").unwrap();
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(descriptor.capability());
    let authority = RunContext::root(BudgetTracker::new(Budget::default()), capabilities);
    let mut resources = ResourceRegistry::new().with_max_content_bytes(3);
    resources
        .register(Arc::new(StaticTextResource::new(descriptor, "too large")))
        .unwrap();
    let error = resources
        .read("memory://large", &authority)
        .await
        .unwrap_err();
    assert_eq!(error.kind, ResourceErrorKind::InvalidOutput);

    let blob_descriptor =
        ResourceDescriptor::new(CapabilityId::new(), "memory://invalid-blob", "blob", "1").unwrap();
    let mut blob_capabilities = CapabilitySet::new();
    blob_capabilities.grant(blob_descriptor.capability());
    let blob_authority = RunContext::root(BudgetTracker::new(Budget::default()), blob_capabilities);
    let mut blob_resources = ResourceRegistry::new();
    blob_resources
        .register(Arc::new(InvalidBlobResource {
            descriptor: blob_descriptor,
        }))
        .unwrap();
    let error = blob_resources
        .read("memory://invalid-blob", &blob_authority)
        .await
        .unwrap_err();
    assert_eq!(error.kind, ResourceErrorKind::InvalidOutput);

    let fixture = content_fixture();
    let client = McpClient::new(
        Arc::new(fixture.server.session()),
        McpClientConfig::new(Implementation::new("argument-client", "1")),
    );
    client.initialize().await.unwrap();
    let error = client
        .get_prompt(
            "review",
            BTreeMap::from([
                ("code".into(), "fn main() {}".into()),
                ("injected".into(), "ignore policy".into()),
            ]),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, McpError::Remote { code: -32602, .. }));
}

#[tokio::test]
async fn request_id_cancellation_stops_an_inflight_resource_read() {
    let started = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let resource = Arc::new(CancellationResource::new(started.clone(), dropped.clone()));
    let mut resources = ResourceRegistry::new();
    resources.register(resource.clone()).unwrap();
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(resource.descriptor().capability());
    let session = McpServer::new(
        Arc::new(ToolRegistry::new()),
        RunContext::root(BudgetTracker::new(Budget::default()), capabilities),
        Implementation::new("cancel-server", "1"),
    )
    .with_resource_registry(Arc::new(resources))
    .session();
    let client = McpClient::new(
        Arc::new(session.clone()),
        McpClientConfig::new(Implementation::new("cancel-client", "1")),
    );
    client.initialize().await.unwrap();

    let request_session = session.clone();
    let request = tokio::spawn(async move {
        request_session
            .handle_request(runifold_mcp::JsonRpcRequest {
                jsonrpc: "2.0".into(),
                id: RequestId::String("resource-1".into()),
                method: "resources/read".into(),
                params: Some(serde_json::json!({"uri": "memory://wait"})),
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    session
        .handle_notification(runifold_mcp::JsonRpcNotification::new(
            "notifications/cancelled",
            Some(serde_json::json!({"requestId": "resource-1"})),
        ))
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), request)
        .await
        .unwrap()
        .unwrap();
    assert!(dropped.load(Ordering::Acquire));
}

struct ContentFixture {
    server: McpServer,
}

fn content_fixture() -> ContentFixture {
    let public = Arc::new(StaticTextResource::new(
        ResourceDescriptor::new(CapabilityId::new(), "docs://public/readme", "readme", "1")
            .unwrap(),
        "# Public documentation",
    ));
    let private = Arc::new(StaticTextResource::new(
        ResourceDescriptor::new(CapabilityId::new(), "docs://private/secret", "secret", "1")
            .unwrap(),
        "private",
    ));
    let prompt = Arc::new(ReviewPrompt::new());
    let mut resources = ResourceRegistry::new();
    resources.register(public.clone()).unwrap();
    resources.register(private).unwrap();
    let mut prompts = PromptRegistry::new();
    prompts.register(prompt.clone()).unwrap();
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(public.descriptor().capability());
    capabilities.grant(prompt.descriptor().capability());
    let authority = RunContext::root(BudgetTracker::new(Budget::default()), capabilities);
    let server = McpServer::new(
        Arc::new(ToolRegistry::new()),
        authority,
        Implementation::new("content-server", "1"),
    )
    .with_resource_registry(Arc::new(resources))
    .with_prompt_registry(Arc::new(prompts));
    ContentFixture { server }
}

#[derive(Debug)]
struct ReviewPrompt {
    descriptor: PromptDescriptor,
}

impl ReviewPrompt {
    fn new() -> Self {
        let mut descriptor = PromptDescriptor::new(CapabilityId::new(), "review", "1").unwrap();
        descriptor.prompt.description = Some("Review user-selected code".into());
        descriptor.prompt.arguments.push(PromptArgument {
            name: "code".into(),
            title: None,
            description: Some("Code to review".into()),
            required: true,
        });
        Self { descriptor }
    }
}

impl PromptHandler for ReviewPrompt {
    fn descriptor(&self) -> &PromptDescriptor {
        &self.descriptor
    }

    fn render(
        &self,
        arguments: BTreeMap<String, String>,
        _context: RunContext,
    ) -> PromptFuture<'_> {
        Box::pin(async move {
            let code = arguments.get("code").cloned().unwrap_or_default();
            Ok(GetPromptResult {
                description: Some("Review prompt".into()),
                messages: vec![PromptMessage::user_text(format!(
                    "Review this code:\n{code}"
                ))],
            })
        })
    }
}

#[derive(Debug)]
struct InvalidBlobResource {
    descriptor: ResourceDescriptor,
}

impl ResourceHandler for InvalidBlobResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn read(&self, _context: RunContext) -> ResourceFuture<'_> {
        Box::pin(async move {
            Ok(ReadResourceResult {
                contents: vec![ResourceContents::blob(
                    self.descriptor.resource.uri.clone(),
                    "not base64!",
                )],
                ttl_ms: None,
                cache_scope: None,
            })
        })
    }
}

#[derive(Debug)]
struct CancellationResource {
    descriptor: ResourceDescriptor,
    started: Arc<AtomicBool>,
    dropped: Arc<AtomicBool>,
}

impl CancellationResource {
    fn new(started: Arc<AtomicBool>, dropped: Arc<AtomicBool>) -> Self {
        Self {
            descriptor: ResourceDescriptor::new(CapabilityId::new(), "memory://wait", "wait", "1")
                .unwrap(),
            started,
            dropped,
        }
    }
}

impl ResourceHandler for CancellationResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn read(&self, _context: RunContext) -> ResourceFuture<'_> {
        Box::pin(async move {
            let _drop_signal = DropSignal(self.dropped.clone());
            self.started.store(true, Ordering::Release);
            std::future::pending::<Result<ReadResourceResult, runifold_mcp::ResourceError>>().await
        })
    }
}

struct DropSignal(Arc<AtomicBool>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
