//! MCP response-cache conformance tests.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::StreamExt;
use runifold_core::{Budget, BudgetTracker, CapabilitySet, RunContext};
use runifold_mcp::{
    CacheHint, CacheMode, CacheOperation, CacheScope, Implementation, InMemoryResponseCache,
    JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, McpClient, McpClientConfig, McpError,
    McpServer, McpTool, McpTransport, PeerRequestHandler, ResponseCacheStore,
    ServerNotificationStream, StatelessCancellation, SubscriptionFilter, TransportFuture,
};
use runifold_tool::ToolRegistry;

#[tokio::test]
async fn modern_client_reuses_refreshes_and_bypasses_tool_pages() {
    let server = cacheable_server(CacheScope::Private);
    let transport = Arc::new(CountingTransport::new(server.session()));
    let client = McpClient::new(
        transport.clone(),
        McpClientConfig::new(Implementation::new("cache-client", "1")),
    );
    client.connect().await.unwrap();

    let first = client.list_tools_page(None).await.unwrap();
    assert_eq!(first.ttl_ms, Some(60_000));
    assert_eq!(first.cache_scope, Some(CacheScope::Private));
    client.list_tools_page(None).await.unwrap();
    assert_eq!(transport.tools_list_requests(), 1);

    client
        .list_tools_page_with_cache(None, CacheMode::Refresh)
        .await
        .unwrap();
    assert_eq!(transport.tools_list_requests(), 2);

    client
        .list_tools_page_with_cache(None, CacheMode::Bypass)
        .await
        .unwrap();
    assert_eq!(transport.tools_list_requests(), 3);
}

#[tokio::test]
async fn matching_notification_invalidates_all_tool_pages() {
    let server = cacheable_server(CacheScope::Private);
    let transport = Arc::new(CountingTransport::new(server.session()));
    let client = McpClient::new(
        transport.clone(),
        McpClientConfig::new(Implementation::new("cache-client", "1")),
    );
    client.connect().await.unwrap();
    client.list_tools_page(None).await.unwrap();
    client.list_tools_page(None).await.unwrap();
    assert_eq!(transport.tools_list_requests(), 1);

    let mut subscription = client
        .listen(SubscriptionFilter {
            tools_list_changed: true,
            ..SubscriptionFilter::default()
        })
        .await
        .unwrap();
    assert!(server.notify_tools_list_changed());
    subscription.next().await.unwrap().unwrap();

    client.list_tools_page(None).await.unwrap();
    assert_eq!(transport.tools_list_requests(), 2);
}

#[tokio::test]
async fn public_entries_share_only_an_explicit_endpoint_namespace() {
    let store: Arc<dyn ResponseCacheStore> = Arc::new(InMemoryResponseCache::new(16));
    let first_transport = Arc::new(CountingTransport::new(
        cacheable_server(CacheScope::Public).session(),
    ));
    let first = McpClient::new(
        first_transport.clone(),
        cache_config(Arc::clone(&store), "alice"),
    );
    first.connect().await.unwrap();
    first.list_tools_page(None).await.unwrap();
    assert_eq!(first_transport.tools_list_requests(), 1);

    let second_transport = Arc::new(CountingTransport::new(
        cacheable_server(CacheScope::Public).session(),
    ));
    let second = McpClient::new(second_transport.clone(), cache_config(store, "bob"));
    second.connect().await.unwrap();
    second.list_tools_page(None).await.unwrap();
    assert_eq!(second_transport.tools_list_requests(), 0);
}

#[tokio::test]
async fn discovery_uses_the_same_explicit_public_cache_boundary() {
    let store: Arc<dyn ResponseCacheStore> = Arc::new(InMemoryResponseCache::new(16));
    let server = cacheable_server(CacheScope::Private).with_cache_hint(
        CacheOperation::ServerDiscover,
        CacheHint::new(60_000, CacheScope::Public),
    );
    let first_transport = Arc::new(CountingTransport::new(server.session()));
    let first = McpClient::new(
        first_transport.clone(),
        cache_config(Arc::clone(&store), "alice"),
    );
    first.connect().await.unwrap();
    assert_eq!(first_transport.discovery_requests(), 1);

    let second_transport = Arc::new(CountingTransport::new(server.session()));
    let second = McpClient::new(second_transport.clone(), cache_config(store, "bob"));
    second.connect().await.unwrap();
    assert_eq!(second_transport.discovery_requests(), 0);
}

#[tokio::test]
async fn private_entries_never_cross_authorization_partitions() {
    let store: Arc<dyn ResponseCacheStore> = Arc::new(InMemoryResponseCache::new(16));
    let first = McpClient::new(
        Arc::new(CountingTransport::new(
            cacheable_server(CacheScope::Private).session(),
        )),
        cache_config(Arc::clone(&store), "alice"),
    );
    first.connect().await.unwrap();
    first.list_tools_page(None).await.unwrap();

    let second_transport = Arc::new(CountingTransport::new(
        cacheable_server(CacheScope::Private).session(),
    ));
    let second = McpClient::new(second_transport.clone(), cache_config(store, "bob"));
    second.connect().await.unwrap();
    second.list_tools_page(None).await.unwrap();
    assert_eq!(second_transport.tools_list_requests(), 1);
}

#[tokio::test]
async fn modern_defaults_are_explicit_and_legacy_results_omit_cache_fields() {
    let server = McpServer::new(
        Arc::new(ToolRegistry::new()),
        RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new()),
        Implementation::new("default-cache-server", "1"),
    );
    let modern = McpClient::new(
        Arc::new(server.session()),
        McpClientConfig::new(Implementation::new("modern-client", "1")),
    );
    modern.connect().await.unwrap();
    let modern_page = modern.list_tools_page(None).await.unwrap();
    assert_eq!(modern_page.ttl_ms, Some(0));
    assert_eq!(modern_page.cache_scope, Some(CacheScope::Private));

    let legacy = McpClient::new(
        Arc::new(server.session()),
        McpClientConfig::new(Implementation::new("legacy-client", "1")),
    );
    legacy.initialize().await.unwrap();
    let legacy_page = legacy.list_tools_page(None).await.unwrap();
    assert_eq!(legacy_page.ttl_ms, None);
    assert_eq!(legacy_page.cache_scope, None);
}

fn cacheable_server(scope: CacheScope) -> McpServer {
    McpServer::new(
        Arc::new(ToolRegistry::new()),
        RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new()),
        Implementation::new("cache-server", "1"),
    )
    .with_cache_hint(CacheOperation::ToolsList, CacheHint::new(60_000, scope))
}

fn cache_config(store: Arc<dyn ResponseCacheStore>, principal: &str) -> McpClientConfig {
    McpClientConfig::new(Implementation::new("cache-client", "1"))
        .with_response_cache(store)
        .with_cache_namespace("https://trusted.example/mcp")
        .with_private_cache_partition(principal)
        .with_max_cache_ttl(Duration::from_secs(60))
}

#[derive(Debug)]
struct CountingTransport {
    inner: runifold_mcp::McpSession,
    tools_list_requests: AtomicUsize,
    discovery_requests: AtomicUsize,
}

impl CountingTransport {
    fn new(inner: runifold_mcp::McpSession) -> Self {
        Self {
            inner,
            tools_list_requests: AtomicUsize::new(0),
            discovery_requests: AtomicUsize::new(0),
        }
    }

    fn tools_list_requests(&self) -> usize {
        self.tools_list_requests.load(Ordering::SeqCst)
    }

    fn discovery_requests(&self) -> usize {
        self.discovery_requests.load(Ordering::SeqCst)
    }
}

impl McpTransport for CountingTransport {
    fn request(&self, request: JsonRpcRequest) -> TransportFuture<'_, JsonRpcResponse> {
        if request.method == "tools/list" {
            self.tools_list_requests.fetch_add(1, Ordering::SeqCst);
        } else if request.method == "server/discover" {
            self.discovery_requests.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.request(request)
    }

    fn notify(&self, notification: JsonRpcNotification) -> TransportFuture<'_, ()> {
        self.inner.notify(notification)
    }

    fn stateless_cancellation(&self) -> StatelessCancellation {
        self.inner.stateless_cancellation()
    }

    fn prepare_tools(&self, tools: Vec<McpTool>) -> Result<Vec<McpTool>, McpError> {
        self.inner.prepare_tools(tools)
    }

    fn subscribe(&self) -> TransportFuture<'_, ServerNotificationStream> {
        self.inner.subscribe()
    }

    fn listen(&self, request: JsonRpcRequest) -> TransportFuture<'_, ServerNotificationStream> {
        self.inner.listen(request)
    }

    fn install_peer_handler(&self, handler: Arc<dyn PeerRequestHandler>) -> Result<(), McpError> {
        self.inner.install_peer_handler(handler)
    }

    fn start_peer(&self) -> TransportFuture<'_, ()> {
        self.inner.start_peer()
    }
}
