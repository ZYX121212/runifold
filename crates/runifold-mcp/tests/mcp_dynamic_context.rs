//! Dynamic-context conformance for pagination, templates, completion, and subscriptions.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures_util::StreamExt;
use runifold_core::{Budget, BudgetTracker, CapabilityId, CapabilitySet, RunContext};
use runifold_mcp::{
    CompleteParams, CompleteResult, Completion, CompletionArgument, CompletionDescriptor,
    CompletionReference, CompletionRegistry, FunctionCompletion, FunctionPrompt, GetPromptResult,
    Implementation, McpClient, McpClientConfig, McpError, McpHttpServer, McpHttpServerConfig,
    McpProtocolMode, McpServer, PromptArgument, PromptDescriptor, PromptHandler, PromptMessage,
    PromptRegistry, ReadResourceResult, ResourceContents, ResourceDescriptor, ResourceFuture,
    ResourceRegistry, ResourceTemplateDescriptor, ResourceTemplateHandler, StaticTextResource,
    StdioTransport, StreamableHttpTransport, SubscriptionFilter, serve_io,
};
use runifold_tool::ToolRegistry;
use tokio::io::{BufReader, split};

#[tokio::test]
async fn automatic_pagination_is_complete_and_cursors_are_session_bound() {
    let server = paginated_server();
    let first = client(&server, 1);
    let second = client(&server, 1);
    first.initialize().await.unwrap();
    second.initialize().await.unwrap();

    let resources = first.list_resources().await.unwrap();
    assert_eq!(resources.len(), 3);
    assert_eq!(resources[0].uri, "memory://item/0");
    assert_eq!(resources[2].uri, "memory://item/2");

    let page = first.list_resources_page(None).await.unwrap();
    assert_eq!(page.resources.len(), 1);
    let foreign = second
        .list_resources_page(page.next_cursor)
        .await
        .unwrap_err();
    assert!(matches!(foreign, McpError::Remote { code: -32602, .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn stateless_http_pagination_survives_independent_requests() {
    let http_server = McpHttpServer::new(paginated_server(), McpHttpServerConfig::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
    let router = http_server.router("/mcp");
    let server_task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = McpClient::new(
        Arc::new(StreamableHttpTransport::new(endpoint).unwrap()),
        McpClientConfig::new(Implementation::new("stateless-pagination", "1")),
    );

    assert_eq!(client.connect().await.unwrap(), McpProtocolMode::Stateless);
    let resources = client.list_resources().await.unwrap();
    assert_eq!(resources.len(), 3);
    assert_eq!(resources[0].uri, "memory://item/0");
    assert_eq!(resources[2].uri, "memory://item/2");
    assert_eq!(http_server.session_count().await, 0);
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn stateless_http_listen_filters_correlates_and_uses_no_session() {
    let server = paginated_server();
    let publisher = server.clone();
    let http_server = McpHttpServer::new(server, McpHttpServerConfig::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
    let router = http_server.router("/mcp");
    let server_task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let client = McpClient::new(
        Arc::new(StreamableHttpTransport::new(endpoint).unwrap()),
        McpClientConfig::new(Implementation::new("modern-listener", "1")),
    );

    assert_eq!(client.connect().await.unwrap(), McpProtocolMode::Stateless);
    let mut subscription = client
        .listen(SubscriptionFilter {
            tools_list_changed: true,
            prompts_list_changed: true,
            resources_list_changed: false,
            resource_subscriptions: vec!["memory://item/1".into(), "memory://missing".into()],
            task_ids: Vec::new(),
        })
        .await
        .unwrap();
    assert!(subscription.accepted().tools_list_changed);
    assert!(!subscription.accepted().prompts_list_changed);
    assert_eq!(
        subscription.accepted().resource_subscriptions,
        ["memory://item/1"]
    );

    assert!(publisher.notify_resource_updated("memory://item/2"));
    assert!(publisher.notify_resource_updated("memory://item/1"));
    let update = tokio::time::timeout(Duration::from_secs(1), subscription.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(update.method, "notifications/resources/updated");
    assert_eq!(
        update
            .params
            .as_ref()
            .and_then(|params| params.get("uri"))
            .and_then(serde_json::Value::as_str),
        Some("memory://item/1")
    );
    assert_eq!(http_server.session_count().await, 0);
    drop(subscription);
    server_task.abort();
}

#[tokio::test]
async fn stateless_stdio_listen_is_multiplexed_and_drop_cancelled() {
    let server = paginated_server();
    let publisher = server.clone();
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let (server_reader, server_writer) = split(server_io);
    let server_task = tokio::spawn(serve_io(
        server.session(),
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
        McpClientConfig::new(Implementation::new("stdio-modern-listener", "1")),
    );

    assert_eq!(client.connect().await.unwrap(), McpProtocolMode::Stateless);
    let mut subscription = client
        .listen(SubscriptionFilter {
            tools_list_changed: true,
            ..SubscriptionFilter::default()
        })
        .await
        .unwrap();
    let mut second_subscription = client
        .listen(SubscriptionFilter {
            tools_list_changed: true,
            ..SubscriptionFilter::default()
        })
        .await
        .unwrap();
    assert!(publisher.notify_tools_list_changed());
    let changed = tokio::time::timeout(Duration::from_secs(1), subscription.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(changed.method, "notifications/tools/list_changed");
    let second_changed = tokio::time::timeout(Duration::from_secs(1), second_subscription.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(second_changed.method, "notifications/tools/list_changed");

    drop(subscription);
    drop(second_subscription);
    drop(client);
    transport.shutdown().await.unwrap();
    drop(transport);
    tokio::time::timeout(Duration::from_secs(1), server_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn templates_are_discoverable_readable_and_capability_gated() {
    let template = Arc::new(UserResourceTemplate::new());
    let mut resources = ResourceRegistry::new();
    resources.register_template(template.clone()).unwrap();
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(template.descriptor().capability());
    let server = server_with(capabilities, Some(Arc::new(resources)), None, None);
    let client = client(&server, 50);
    client.initialize().await.unwrap();

    let templates = client.list_resource_templates().await.unwrap();
    assert_eq!(templates.len(), 1);
    assert_eq!(templates[0].uri_template, "memory://users/{id}");
    let result = client.read_resource("memory://users/42").await.unwrap();
    assert_eq!(
        result.contents,
        vec![ResourceContents::text("memory://users/42", "user:42")]
    );

    let denied = client.read_resource("memory://other/42").await.unwrap_err();
    assert!(matches!(denied, McpError::Remote { code: -32002, .. }));
}

#[tokio::test]
async fn completion_is_reference_checked_authorized_and_bounded() {
    let server = prompt_completion_server();
    let client = client(&server, 50);
    let initialized = client.initialize().await.unwrap();
    assert!(initialized.capabilities.completions.is_some());

    let result = client
        .complete(CompleteParams {
            reference: CompletionReference::Prompt {
                name: "review".into(),
            },
            argument: CompletionArgument {
                name: "language".into(),
                value: "ru".into(),
            },
            context: None,
        })
        .await
        .unwrap();
    assert_eq!(result.completion.values, vec!["rust", "ruby"]);

    let invalid = client
        .complete(CompleteParams {
            reference: CompletionReference::Prompt {
                name: "review".into(),
            },
            argument: CompletionArgument {
                name: "secret".into(),
                value: String::new(),
            },
            context: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(
        invalid,
        McpError::Remote { code: -32601, .. } | McpError::Remote { code: -32602, .. }
    ));
}

#[tokio::test]
async fn completion_rejects_more_than_one_hundred_values() {
    let server = prompt_completion_server();
    let client = client(&server, 50);
    client.initialize().await.unwrap();
    let oversized = client
        .complete(CompleteParams {
            reference: CompletionReference::Prompt {
                name: "review".into(),
            },
            argument: CompletionArgument {
                name: "language".into(),
                value: "too-many".into(),
            },
            context: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(oversized, McpError::Remote { code: -32603, .. }));
}

fn prompt_completion_server() -> McpServer {
    let prompt_id = CapabilityId::new();
    let mut prompt_descriptor = PromptDescriptor::new(prompt_id, "review", "1").unwrap();
    prompt_descriptor.prompt.arguments.push(PromptArgument {
        name: "language".into(),
        title: Some("Language".into()),
        description: None,
        required: true,
    });
    let prompt = Arc::new(FunctionPrompt::new(
        prompt_descriptor,
        |arguments: &BTreeMap<String, String>, _: &RunContext| {
            Ok(GetPromptResult {
                description: None,
                messages: vec![PromptMessage::user_text(
                    arguments.get("language").cloned().unwrap_or_default(),
                )],
            })
        },
    ));
    let mut prompts = PromptRegistry::new();
    prompts.register(prompt.clone()).unwrap();

    let completion_descriptor = CompletionDescriptor::prompt(prompt_id, "review", "1").unwrap();
    let completion = Arc::new(FunctionCompletion::new(
        completion_descriptor,
        |params: &CompleteParams, _: &RunContext| {
            let values = if params.argument.value == "too-many" {
                (0..101).map(|index| format!("value-{index}")).collect()
            } else {
                ["rust", "ruby"]
                    .into_iter()
                    .filter(|value| value.starts_with(&params.argument.value))
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            };
            Ok(CompleteResult {
                completion: Completion {
                    total: Some(values.len() as u64),
                    has_more: Some(false),
                    values,
                },
            })
        },
    ));
    let mut completions = CompletionRegistry::new();
    completions.register(completion).unwrap();
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(prompt.descriptor().capability());
    server_with(
        capabilities,
        None,
        Some(Arc::new(prompts)),
        Some(Arc::new(completions)),
    )
}

#[tokio::test]
async fn resource_template_completion_requires_a_declared_variable() {
    let template = Arc::new(UserResourceTemplate::new());
    let template_id = template.descriptor().id;
    let mut resources = ResourceRegistry::new();
    resources.register_template(template.clone()).unwrap();
    let descriptor =
        CompletionDescriptor::resource(template_id, "memory://users/{id}", "1").unwrap();
    let completion = Arc::new(FunctionCompletion::new(
        descriptor,
        |_: &CompleteParams, _: &RunContext| {
            Ok(CompleteResult {
                completion: Completion {
                    values: vec!["42".into()],
                    total: Some(1),
                    has_more: Some(false),
                },
            })
        },
    ));
    let mut completions = CompletionRegistry::new();
    completions.register(completion).unwrap();
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(template.descriptor().capability());
    let server = server_with(
        capabilities,
        Some(Arc::new(resources)),
        None,
        Some(Arc::new(completions)),
    );
    let client = client(&server, 50);
    assert_eq!(client.connect().await.unwrap(), McpProtocolMode::Stateless);

    let result = client
        .complete(CompleteParams {
            reference: CompletionReference::Resource {
                uri: "memory://users/{id}".into(),
            },
            argument: CompletionArgument {
                name: "id".into(),
                value: "4".into(),
            },
            context: None,
        })
        .await
        .unwrap();
    assert_eq!(result.completion.values, vec!["42"]);

    let invalid = client
        .complete(CompleteParams {
            reference: CompletionReference::Resource {
                uri: "memory://users/{id}".into(),
            },
            argument: CompletionArgument {
                name: "tenant".into(),
                value: String::new(),
            },
            context: None,
        })
        .await
        .unwrap_err();
    assert!(matches!(
        invalid,
        McpError::Remote { code: -32601, .. } | McpError::Remote { code: -32602, .. }
    ));
}

#[tokio::test]
async fn resource_updates_stop_after_unsubscribe() {
    let server = paginated_server();
    let session = server.session();
    let client = McpClient::new(
        Arc::new(session.clone()),
        McpClientConfig::new(Implementation::new("subscriber", "1")),
    );
    client.initialize().await.unwrap();
    let mut notifications = client.notifications().await.unwrap();
    client.subscribe_resource("memory://item/0").await.unwrap();

    assert!(session.notify_resource_updated("memory://item/0"));
    let update = tokio::time::timeout(Duration::from_secs(1), notifications.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(update.method, "notifications/resources/updated");

    client
        .unsubscribe_resource("memory://item/0")
        .await
        .unwrap();
    assert!(!session.notify_resource_updated("memory://item/0"));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), notifications.next())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn stdio_forwards_subscribed_resource_updates() {
    let server = paginated_server();
    let session = server.session();
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let (server_reader, server_writer) = split(server_io);
    let server_task = tokio::spawn(serve_io(
        session.clone(),
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
        McpClientConfig::new(Implementation::new("stdio-subscriber", "1")),
    );
    client.initialize().await.unwrap();
    let mut notifications = client.notifications().await.unwrap();
    client.subscribe_resource("memory://item/1").await.unwrap();

    assert!(session.notify_resource_updated("memory://item/1"));
    let update = tokio::time::timeout(Duration::from_secs(1), notifications.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(update.method, "notifications/resources/updated");

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
async fn streamable_http_filters_and_replays_resource_updates() {
    let http_server = McpHttpServer::new(paginated_server(), McpHttpServerConfig::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
    let router = http_server.router("/mcp");
    let server_task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let transport = Arc::new(StreamableHttpTransport::new(endpoint).unwrap());
    let client = McpClient::new(
        transport.clone(),
        McpClientConfig::new(Implementation::new("http-subscriber", "1")),
    );
    client.initialize().await.unwrap();
    client.subscribe_resource("memory://item/2").await.unwrap();
    let session_id = transport.session_id().await.unwrap();

    assert!(
        http_server
            .send_resource_updated(&session_id, "memory://item/0")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        http_server
            .send_resource_updated(&session_id, "memory://item/2")
            .await
            .unwrap()
            .is_some()
    );
    let mut notifications = client.notifications().await.unwrap();
    let update = tokio::time::timeout(Duration::from_secs(1), notifications.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(update.method, "notifications/resources/updated");

    server_task.abort();
}

fn paginated_server() -> McpServer {
    let mut resources = ResourceRegistry::new();
    let mut capabilities = CapabilitySet::new();
    for index in 0..3 {
        let descriptor = ResourceDescriptor::new(
            CapabilityId::new(),
            format!("memory://item/{index}"),
            format!("item-{index}"),
            "1",
        )
        .unwrap();
        capabilities.grant(descriptor.capability());
        resources
            .register(Arc::new(StaticTextResource::new(
                descriptor,
                index.to_string(),
            )))
            .unwrap();
    }
    server_with(capabilities, Some(Arc::new(resources)), None, None)
}

fn server_with(
    capabilities: CapabilitySet,
    resources: Option<Arc<ResourceRegistry>>,
    prompts: Option<Arc<PromptRegistry>>,
    completions: Option<Arc<CompletionRegistry>>,
) -> McpServer {
    let mut server = McpServer::new(
        Arc::new(ToolRegistry::new()),
        RunContext::root(BudgetTracker::new(Budget::default()), capabilities),
        Implementation::new("dynamic-server", "1"),
    );
    if let Some(resources) = resources {
        server = server.with_resource_registry(resources);
    }
    if let Some(prompts) = prompts {
        server = server.with_prompt_registry(prompts);
    }
    if let Some(completions) = completions {
        server = server.with_completion_registry(completions);
    }
    server
}

fn client(server: &McpServer, page_size: usize) -> McpClient {
    let server = server.clone().with_page_size(page_size);
    McpClient::new(
        Arc::new(server.session()),
        McpClientConfig::new(Implementation::new("dynamic-client", "1")),
    )
}

#[derive(Debug)]
struct UserResourceTemplate {
    descriptor: ResourceTemplateDescriptor,
}

impl UserResourceTemplate {
    fn new() -> Self {
        Self {
            descriptor: ResourceTemplateDescriptor::new(
                CapabilityId::new(),
                "memory://users/{id}",
                "user",
                "1",
            )
            .unwrap(),
        }
    }
}

impl ResourceTemplateHandler for UserResourceTemplate {
    fn descriptor(&self) -> &ResourceTemplateDescriptor {
        &self.descriptor
    }

    fn matches_uri(&self, uri: &str) -> bool {
        uri.strip_prefix("memory://users/")
            .is_some_and(|id| !id.is_empty() && !id.contains('/'))
    }

    fn read(&self, uri: String, _context: RunContext) -> ResourceFuture<'_> {
        Box::pin(async move {
            let id = uri.trim_start_matches("memory://users/");
            Ok(ReadResourceResult {
                contents: vec![ResourceContents::text(uri.clone(), format!("user:{id}"))],
                ttl_ms: None,
                cache_scope: None,
            })
        })
    }
}
