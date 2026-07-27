//! Capability, approval, and transport conformance for basic MCP Sampling.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use runifold_core::{
    Budget, BudgetTracker, CapabilitySet, InMemoryJournal, RunContext, RunEventKind,
};
use runifold_mcp::{
    CreateMessageParams, CreateMessageResult, FixedSamplingModel, Implementation, McpClient,
    McpClientConfig, McpError, McpHttpServer, McpHttpServerConfig, McpServer,
    ModelSamplingProvider, SamplingApprover, SamplingCallContext, SamplingDecision, SamplingError,
    SamplingFuture, SamplingMessage, SamplingPolicy, SamplingProvider, SamplingService,
    StdioTransport, StreamableHttpTransport, serve_io,
};
use runifold_model::{ContentPart, FinishReason, ModelRef, ModelStreamEvent, Role};
use runifold_testkit::ScriptedModel;
use runifold_tool::ToolRegistry;
use tokio::io::{BufReader, split};

#[tokio::test]
async fn in_process_sampling_requires_capability_and_both_approval_stages() {
    let reviews = Arc::new(ReviewCounts::default());
    let sampling = sampling_service(
        ReviewMode::Approve,
        reviews.clone(),
        SamplingPolicy::default(),
    );
    let server = sampling_server();
    let session = server.session();
    let requester = session.sampling_client();
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("sampling-client", "1")).with_sampling(sampling),
    );
    client.initialize().await.unwrap();

    let result = requester.create_message(basic_request(128)).await.unwrap();
    assert_eq!(result.content.as_slice()[0].as_text(), Some("sampled"));
    assert_eq!(reviews.requests.load(Ordering::Acquire), 1);
    assert_eq!(reviews.responses.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn sampling_rejection_and_budget_exhaustion_are_explicit() {
    let server = sampling_server();
    let rejected_session = server.session();
    let rejected = rejected_session.sampling_client();
    let rejected_client = McpClient::new(
        Arc::new(rejected_session),
        McpClientConfig::new(Implementation::new("rejecting-client", "1")).with_sampling(
            sampling_service(
                ReviewMode::RejectRequest,
                Arc::new(ReviewCounts::default()),
                SamplingPolicy::default(),
            ),
        ),
    );
    rejected_client.initialize().await.unwrap();
    let error = rejected
        .create_message(basic_request(32))
        .await
        .unwrap_err();
    assert!(matches!(error, McpError::Remote { code: -1, .. }));

    let policy = SamplingPolicy {
        max_total_requests: 1,
        ..SamplingPolicy::default()
    };
    let budget_session = server.session();
    let budget = budget_session.sampling_client();
    let budget_client = McpClient::new(
        Arc::new(budget_session),
        McpClientConfig::new(Implementation::new("budget-client", "1")).with_sampling(
            sampling_service(
                ReviewMode::Approve,
                Arc::new(ReviewCounts::default()),
                policy,
            ),
        ),
    );
    budget_client.initialize().await.unwrap();
    budget.create_message(basic_request(32)).await.unwrap();
    let error = budget.create_message(basic_request(32)).await.unwrap_err();
    assert!(matches!(error, McpError::Remote { code: -32000, .. }));
}

#[tokio::test]
async fn scoped_sampling_records_redacted_lifecycle_and_review_stage() {
    let server = sampling_server();
    let session = server.session();
    let requester = session.sampling_client();
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("observed-client", "1")).with_sampling(
            sampling_service(
                ReviewMode::RejectRequest,
                Arc::new(ReviewCounts::default()),
                SamplingPolicy::default(),
            ),
        ),
    );
    client.initialize().await.unwrap();
    let journal = Arc::new(InMemoryJournal::new());
    let run = RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new())
        .with_journal(journal.clone());

    let error = requester
        .create_message_scoped(basic_request(32), &run)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        McpError::Remote {
            code: -1,
            data: Some(_),
            ..
        }
    ));
    let events = journal.events();
    let domain = events
        .iter()
        .filter_map(|event| match &event.kind {
            RunEventKind::Domain(domain) => Some(domain),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        domain
            .iter()
            .map(|event| event.name.as_str())
            .collect::<Vec<_>>(),
        ["sampling.started", "sampling.failed"]
    );
    assert_eq!(domain[1].payload["stage"], "request_review");
    assert!(
        !serde_json::to_string(&events)
            .unwrap()
            .contains("Generate a safe answer")
    );
}

#[tokio::test]
async fn response_rejection_prevents_disclosure_to_server() {
    let server = sampling_server();
    let session = server.session();
    let requester = session.sampling_client();
    let reviews = Arc::new(ReviewCounts::default());
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("response-review-client", "1")).with_sampling(
            sampling_service(
                ReviewMode::RejectResponse,
                reviews.clone(),
                SamplingPolicy::default(),
            ),
        ),
    );
    client.initialize().await.unwrap();

    let error = requester
        .create_message(basic_request(32))
        .await
        .unwrap_err();
    assert!(matches!(error, McpError::Remote { code: -1, .. }));
    assert_eq!(reviews.requests.load(Ordering::Acquire), 1);
    assert_eq!(reviews.responses.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn basic_sampling_rejects_tool_use_output() {
    let sampling = Arc::new(SamplingService::new(
        Arc::new(TestApprover {
            mode: ReviewMode::Approve,
            reviews: Arc::new(ReviewCounts::default()),
        }),
        Arc::new(ToolUseProvider),
        SamplingPolicy::default(),
    ));
    let server = sampling_server();
    let session = server.session();
    let requester = session.sampling_client();
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("tool-use-client", "1")).with_sampling(sampling),
    );
    client.initialize().await.unwrap();

    let error = requester
        .create_message(basic_request(32))
        .await
        .unwrap_err();
    assert!(matches!(error, McpError::Remote { code: -32603, .. }));
}

#[tokio::test]
async fn server_timeout_cancels_and_drops_client_model_work() {
    let dropped = Arc::new(AtomicBool::new(false));
    let sampling = Arc::new(SamplingService::new(
        Arc::new(TestApprover {
            mode: ReviewMode::Approve,
            reviews: Arc::new(ReviewCounts::default()),
        }),
        Arc::new(BlockingProvider {
            dropped: dropped.clone(),
        }),
        SamplingPolicy {
            request_timeout: Duration::from_secs(5),
            ..SamplingPolicy::default()
        },
    ));
    let server = sampling_server();
    let session = server.session();
    let requester = session
        .sampling_client()
        .with_timeout(Duration::from_millis(20));
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("cancel-client", "1")).with_sampling(sampling),
    );
    client.initialize().await.unwrap();

    let error = requester
        .create_message(basic_request(32))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), runifold_mcp::McpErrorKind::DeadlineExceeded);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !dropped.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn client_without_sampling_capability_is_rejected_before_transport() {
    let server = sampling_server();
    let session = server.session();
    let requester = session.sampling_client();
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("plain-client", "1")),
    );
    client.initialize().await.unwrap();

    let error = requester
        .create_message(basic_request(32))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), runifold_mcp::McpErrorKind::Protocol);
}

#[tokio::test]
async fn stdio_carries_server_requests_and_client_responses() {
    let server = sampling_server();
    let session = server.session();
    let requester = session.sampling_client();
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
        McpClientConfig::new(Implementation::new("stdio-sampling", "1")).with_sampling(
            sampling_service(
                ReviewMode::Approve,
                Arc::new(ReviewCounts::default()),
                SamplingPolicy::default(),
            ),
        ),
    );
    client.initialize().await.unwrap();

    let result = requester.create_message(basic_request(64)).await.unwrap();
    assert_eq!(result.model, "host-model");

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
async fn streamable_http_correlates_sampling_request_and_response() {
    let http_server = McpHttpServer::new(sampling_server(), McpHttpServerConfig::new());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/mcp", listener.local_addr().unwrap());
    let router = http_server.router("/mcp");
    let server_task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let transport = Arc::new(StreamableHttpTransport::new(endpoint).unwrap());
    let client = McpClient::new(
        transport.clone(),
        McpClientConfig::new(Implementation::new("http-sampling", "1")).with_sampling(
            sampling_service(
                ReviewMode::Approve,
                Arc::new(ReviewCounts::default()),
                SamplingPolicy::default(),
            ),
        ),
    );
    client.initialize().await.unwrap();
    let session_id = transport.session_id().await.unwrap();
    let requester = http_server.sampling_client(&session_id).await.unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        requester.create_message(basic_request(64)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.content.as_slice()[0].as_text(), Some("sampled"));
    server_task.abort();
}

#[tokio::test]
async fn canonical_model_adapter_keeps_model_selection_on_client() {
    let model = Arc::new(ScriptedModel::new());
    model.enqueue([
        ModelStreamEvent::ResponseStarted {
            id: Some("sample-1".into()),
            model: ModelRef::new("test-provider", "host-selected"),
        },
        ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::text("canonical answer"),
        },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::Stop,
            provider_metadata: BTreeMap::new(),
        },
    ]);
    let provider = Arc::new(ModelSamplingProvider::new(
        model.clone(),
        Arc::new(FixedSamplingModel::new(ModelRef::new(
            "test-provider",
            "host-selected",
        ))),
    ));
    let sampling = Arc::new(SamplingService::new(
        Arc::new(TestApprover {
            mode: ReviewMode::Approve,
            reviews: Arc::new(ReviewCounts::default()),
        }),
        provider,
        SamplingPolicy::default(),
    ));
    let server = sampling_server();
    let session = server.session();
    let requester = session.sampling_client();
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("model-adapter-client", "1"))
            .with_sampling(sampling),
    );
    client.initialize().await.unwrap();
    let mut request = basic_request(77);
    request.system_prompt = Some("Approved system instruction".into());

    let result = requester.create_message(request).await.unwrap();
    assert_eq!(
        result.content.as_slice()[0].as_text(),
        Some("canonical answer")
    );
    assert_eq!(result.model, "test-provider:host-selected");
    let recorded = model.recorded_requests();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].model.name, "host-selected");
    assert_eq!(recorded[0].messages[0].role, Role::System);
    assert_eq!(recorded[0].generation.max_output_tokens, Some(77));
}

fn sampling_server() -> McpServer {
    McpServer::new(
        Arc::new(ToolRegistry::new()),
        RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new()),
        Implementation::new("sampling-server", "1"),
    )
}

fn basic_request(max_tokens: u64) -> CreateMessageParams {
    CreateMessageParams::new(
        vec![SamplingMessage::user_text("Generate a safe answer")],
        max_tokens,
    )
}

fn sampling_service(
    mode: ReviewMode,
    reviews: Arc<ReviewCounts>,
    policy: SamplingPolicy,
) -> Arc<SamplingService> {
    Arc::new(SamplingService::new(
        Arc::new(TestApprover { mode, reviews }),
        Arc::new(EchoProvider),
        policy,
    ))
}

#[derive(Default)]
struct ReviewCounts {
    requests: AtomicUsize,
    responses: AtomicUsize,
}

#[derive(Clone, Copy)]
enum ReviewMode {
    Approve,
    RejectRequest,
    RejectResponse,
}

struct TestApprover {
    mode: ReviewMode,
    reviews: Arc<ReviewCounts>,
}

impl SamplingApprover for TestApprover {
    fn review_request(
        &self,
        request: CreateMessageParams,
        _context: SamplingCallContext,
    ) -> SamplingFuture<'_, SamplingDecision<CreateMessageParams>> {
        self.reviews.requests.fetch_add(1, Ordering::AcqRel);
        let decision = match self.mode {
            ReviewMode::Approve | ReviewMode::RejectResponse => SamplingDecision::Approve(request),
            ReviewMode::RejectRequest => SamplingDecision::Reject,
        };
        Box::pin(async move { Ok(decision) })
    }

    fn review_response(
        &self,
        response: CreateMessageResult,
        _context: SamplingCallContext,
    ) -> SamplingFuture<'_, SamplingDecision<CreateMessageResult>> {
        self.reviews.responses.fetch_add(1, Ordering::AcqRel);
        let decision = match self.mode {
            ReviewMode::RejectResponse => SamplingDecision::Reject,
            ReviewMode::Approve | ReviewMode::RejectRequest => SamplingDecision::Approve(response),
        };
        Box::pin(async move { Ok(decision) })
    }
}

struct EchoProvider;

impl SamplingProvider for EchoProvider {
    fn sample(
        &self,
        _request: CreateMessageParams,
        _context: SamplingCallContext,
    ) -> SamplingFuture<'_, CreateMessageResult> {
        Box::pin(async { Ok(CreateMessageResult::assistant_text("host-model", "sampled")) })
    }
}

struct ToolUseProvider;

impl SamplingProvider for ToolUseProvider {
    fn sample(
        &self,
        _request: CreateMessageParams,
        _context: SamplingCallContext,
    ) -> SamplingFuture<'_, CreateMessageResult> {
        Box::pin(async {
            let mut result = CreateMessageResult::assistant_text("host-model", "unsafe tool use");
            result.stop_reason = Some("toolUse".into());
            Ok(result)
        })
    }
}

struct BlockingProvider {
    dropped: Arc<AtomicBool>,
}

impl SamplingProvider for BlockingProvider {
    fn sample(
        &self,
        _request: CreateMessageParams,
        _context: SamplingCallContext,
    ) -> SamplingFuture<'_, CreateMessageResult> {
        Box::pin(async move {
            let _signal = DropSignal(self.dropped.clone());
            std::future::pending::<Result<CreateMessageResult, SamplingError>>().await
        })
    }
}

struct DropSignal(Arc<AtomicBool>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
