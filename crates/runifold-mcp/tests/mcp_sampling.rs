//! Capability, approval, and transport conformance for basic MCP Sampling.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use runifold_core::{
    Budget, BudgetTracker, CapabilitySet, InMemoryJournal, RunContext, RunEventKind,
};
use runifold_mcp::{
    ContentBlock, CreateMessageOutcome, CreateMessageParams, CreateMessageResult,
    FixedSamplingModel, Implementation, IncludeContext, McpClient, McpClientConfig, McpError,
    McpHttpServer, McpHttpServerConfig, McpSamplingTaskBackend, McpServer, McpTask,
    McpTaskBackendError, McpTaskBackendErrorKind, McpTaskFuture, McpTool, ModelSamplingProvider,
    SamplingApprover, SamplingCallContext, SamplingContent, SamplingContextProvider,
    SamplingDecision, SamplingError, SamplingFuture, SamplingMessage, SamplingModelFeature,
    SamplingModelRequirements, SamplingModelSelector, SamplingPolicy, SamplingProvider,
    SamplingRole, SamplingService, SamplingTaskCreation, SamplingTaskOutput, SamplingTaskRequest,
    SamplingTaskTerminalResult, SamplingToolChoice, SamplingToolChoiceMode, StdioTransport,
    StreamableHttpTransport, TaskMetadata, TaskStatus, serve_io,
};
use runifold_model::{
    ContentPart, FinishReason, MediaSource, ModelRef, ModelStreamEvent, ProviderData,
    ReasoningPart, Role, ToolCall, ToolChoice, ToolResult,
};
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
async fn task_augmented_sampling_returns_durable_handle_and_exact_result() {
    let backend = Arc::new(TestSamplingTaskBackend::new());
    let reviews = Arc::new(ReviewCounts::default());
    let server = sampling_server();
    let session = server.session();
    let requester = session
        .sampling_client()
        .with_timeout(Duration::from_secs(2));
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("sampling-task-client", "1"))
            .with_sampling(sampling_service(
                ReviewMode::Approve,
                reviews.clone(),
                SamplingPolicy::default(),
            ))
            .with_sampling_tasks(backend.clone()),
    );
    client.initialize().await.unwrap();
    let mut invalid = basic_request(128);
    invalid.task = Some(TaskMetadata {
        ttl: Some(8 * 24 * 60 * 60 * 1_000),
    });
    let error = requester.create_message_outcome(invalid).await.unwrap_err();
    assert!(matches!(error, McpError::Remote { code: -32000, .. }));

    let mut request = basic_request(128);
    request.task = Some(TaskMetadata { ttl: Some(10_000) });

    let task = match requester.create_message_outcome(request).await.unwrap() {
        CreateMessageOutcome::Task(task) => task,
        CreateMessageOutcome::Complete(_) => panic!("expected durable Sampling Task"),
    };
    assert_eq!(task.status, TaskStatus::Working);
    assert_eq!(backend.requested_ttl(), Some(10_000));

    let result = requester.wait_task(task).await.unwrap();
    assert_eq!(result.content.as_slice()[0].as_text(), Some("task sampled"));
    assert_eq!(
        result.meta["io.modelcontextprotocol/related-task"]["taskId"],
        "sampling-task-1"
    );
    let result = requester.task_result("sampling-task-1").await.unwrap();
    assert_eq!(result.content.as_slice()[0].as_text(), Some("task sampled"));
    assert_eq!(reviews.responses.load(Ordering::Acquire), 1);
    let error = requester.cancel_task("sampling-task-1").await.unwrap_err();
    assert!(matches!(error, McpError::Remote { code: -32602, .. }));

    let mut cancellable = basic_request(32);
    cancellable.task = Some(TaskMetadata { ttl: None });
    let task = match requester.create_message_outcome(cancellable).await.unwrap() {
        CreateMessageOutcome::Task(task) => task,
        CreateMessageOutcome::Complete(_) => panic!("expected durable Sampling Task"),
    };
    let cancelled = requester.cancel_task(task.task_id).await.unwrap();
    assert_eq!(cancelled.status, TaskStatus::Cancelled);
}

#[tokio::test]
async fn synchronous_sampling_api_rejects_task_before_remote_creation() {
    let backend = Arc::new(TestSamplingTaskBackend::new());
    let server = sampling_server();
    let session = server.session();
    let requester = session.sampling_client();
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("sampling-task-preflight", "1"))
            .with_sampling(sampling_service(
                ReviewMode::Approve,
                Arc::new(ReviewCounts::default()),
                SamplingPolicy::default(),
            ))
            .with_sampling_tasks(backend.clone()),
    );
    client.initialize().await.unwrap();
    let mut request = basic_request(32);
    request.task = Some(TaskMetadata { ttl: None });

    let error = requester.create_message(request).await.unwrap_err();

    assert_eq!(error.kind(), runifold_mcp::McpErrorKind::Protocol);
    assert_eq!(backend.create_calls.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn deterministic_task_admission_failure_rolls_back_sampling_budget() {
    let backend = Arc::new(TestSamplingTaskBackend::new());
    backend.reject_next_create.store(true, Ordering::Release);
    let server = sampling_server();
    let session = server.session();
    let requester = session.sampling_client();
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("sampling-task-budget", "1"))
            .with_sampling(sampling_service(
                ReviewMode::Approve,
                Arc::new(ReviewCounts::default()),
                SamplingPolicy {
                    max_total_requests: 1,
                    ..SamplingPolicy::default()
                },
            ))
            .with_sampling_tasks(backend),
    );
    client.initialize().await.unwrap();
    let mut first = basic_request(32);
    first.task = Some(TaskMetadata { ttl: None });
    let error = requester.create_message_outcome(first).await.unwrap_err();
    assert!(matches!(error, McpError::Remote { code: -32602, .. }));

    let mut retry = basic_request(32);
    retry.task = Some(TaskMetadata { ttl: None });
    assert!(matches!(
        requester.create_message_outcome(retry).await.unwrap(),
        CreateMessageOutcome::Task(_)
    ));
}

#[tokio::test]
async fn input_required_task_result_waits_and_terminal_error_is_exact() {
    let backend = Arc::new(TestSamplingTaskBackend::new());
    backend.input_required_once.store(true, Ordering::Release);
    let server = sampling_server();
    let session = server.session();
    let requester = session
        .sampling_client()
        .with_timeout(Duration::from_secs(2));
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("sampling-task-input", "1"))
            .with_sampling(sampling_service(
                ReviewMode::Approve,
                Arc::new(ReviewCounts::default()),
                SamplingPolicy::default(),
            ))
            .with_sampling_tasks(backend.clone()),
    );
    client.initialize().await.unwrap();
    let mut request = basic_request(32);
    request.task = Some(TaskMetadata { ttl: None });
    let task = match requester.create_message_outcome(request).await.unwrap() {
        CreateMessageOutcome::Task(task) => task,
        CreateMessageOutcome::Complete(_) => panic!("expected durable Sampling Task"),
    };
    assert_eq!(
        requester.wait_task(task).await.unwrap().content.as_slice()[0].as_text(),
        Some("task sampled")
    );

    backend
        .terminal_error
        .lock()
        .unwrap()
        .replace(runifold_mcp::JsonRpcError {
            code: -32_101,
            message: "model quota exhausted".into(),
            data: Some(serde_json::json!({"retryable": false})),
        });
    let mut failed_request = basic_request(32);
    failed_request.task = Some(TaskMetadata { ttl: None });
    let failed = match requester
        .create_message_outcome(failed_request)
        .await
        .unwrap()
    {
        CreateMessageOutcome::Task(task) => task,
        CreateMessageOutcome::Complete(_) => panic!("expected durable Sampling Task"),
    };
    let error = requester.wait_task(failed).await.unwrap_err();
    assert!(matches!(
        error,
        McpError::Remote {
            code: -32_101,
            ref message,
            data: Some(_),
        } if message == "model quota exhausted"
    ));
}

#[tokio::test]
async fn task_augmented_sampling_requires_negotiated_receiver_capability() {
    let server = sampling_server();
    let session = server.session();
    let requester = session.sampling_client();
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("sync-only-client", "1")).with_sampling(
            sampling_service(
                ReviewMode::Approve,
                Arc::new(ReviewCounts::default()),
                SamplingPolicy::default(),
            ),
        ),
    );
    client.initialize().await.unwrap();
    let mut request = basic_request(32);
    request.task = Some(TaskMetadata { ttl: Some(1_000) });

    let error = requester.create_message_outcome(request).await.unwrap_err();
    assert_eq!(error.kind(), runifold_mcp::McpErrorKind::Protocol);
}

#[tokio::test]
async fn task_augmented_sampling_cannot_bypass_either_approval_stage() {
    let server = sampling_server();
    let rejected_backend = Arc::new(TestSamplingTaskBackend::new());
    let rejected_session = server.session();
    let rejected_requester = rejected_session.sampling_client();
    let rejected_client = McpClient::new(
        Arc::new(rejected_session),
        McpClientConfig::new(Implementation::new("task-request-rejection", "1"))
            .with_sampling(sampling_service(
                ReviewMode::RejectRequest,
                Arc::new(ReviewCounts::default()),
                SamplingPolicy::default(),
            ))
            .with_sampling_tasks(rejected_backend.clone()),
    );
    rejected_client.initialize().await.unwrap();
    let mut request = basic_request(32);
    request.task = Some(TaskMetadata { ttl: Some(1_000) });

    let error = rejected_requester
        .create_message_outcome(request)
        .await
        .unwrap_err();
    assert!(matches!(error, McpError::Remote { code: -1, .. }));
    assert_eq!(rejected_backend.requested_ttl(), None);

    let response_backend = Arc::new(TestSamplingTaskBackend::new());
    let response_session = server.session();
    let response_requester = response_session
        .sampling_client()
        .with_timeout(Duration::from_secs(2));
    let response_client = McpClient::new(
        Arc::new(response_session),
        McpClientConfig::new(Implementation::new("task-response-rejection", "1"))
            .with_sampling(sampling_service(
                ReviewMode::RejectResponse,
                Arc::new(ReviewCounts::default()),
                SamplingPolicy::default(),
            ))
            .with_sampling_tasks(response_backend),
    );
    response_client.initialize().await.unwrap();
    let mut request = basic_request(32);
    request.task = Some(TaskMetadata { ttl: None });
    let task = match response_requester
        .create_message_outcome(request)
        .await
        .unwrap()
    {
        CreateMessageOutcome::Task(task) => task,
        CreateMessageOutcome::Complete(_) => panic!("expected durable Sampling Task"),
    };

    let error = response_requester.wait_task(task).await.unwrap_err();
    assert!(matches!(error, McpError::Remote { code: -1, .. }));
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

#[tokio::test]
async fn context_inclusion_is_expanded_before_approval_and_model_execution() {
    let model = Arc::new(ScriptedModel::new());
    model.enqueue([
        ModelStreamEvent::ResponseStarted {
            id: Some("sample-context".into()),
            model: ModelRef::new("test", "model"),
        },
        ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::text("context-aware answer"),
        },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::Stop,
            provider_metadata: BTreeMap::new(),
        },
    ]);
    let service = SamplingService::new(
        Arc::new(TestApprover {
            mode: ReviewMode::Approve,
            reviews: Arc::new(ReviewCounts::default()),
        }),
        Arc::new(ModelSamplingProvider::new(
            model.clone(),
            Arc::new(FixedSamplingModel::new(ModelRef::new("test", "model"))),
        )),
        SamplingPolicy::default(),
    )
    .with_context_provider(Arc::new(StaticContextProvider));
    let mut request = basic_request(32);
    request.include_context = IncludeContext::ThisServer;

    let response = service
        .execute(request, runifold_core::CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(
        response.content.as_slice()[0].as_text(),
        Some("context-aware answer")
    );
    assert!(service.capability().context.is_some());
    assert!(service.capability().tools.is_some());
    let recorded = model.recorded_requests();
    assert_eq!(recorded[0].messages.len(), 2);
    assert_eq!(
        recorded[0].messages[0].content[0],
        ContentPart::text("resolved context")
    );
}

#[tokio::test]
async fn context_resolution_failures_have_their_own_diagnostic_stage() {
    let service = SamplingService::new(
        Arc::new(TestApprover {
            mode: ReviewMode::Approve,
            reviews: Arc::new(ReviewCounts::default()),
        }),
        Arc::new(EchoProvider),
        SamplingPolicy::default(),
    )
    .with_context_provider(Arc::new(FailingContextProvider));
    let mut request = basic_request(32);
    request.include_context = IncludeContext::ThisServer;

    let error = service
        .execute(request, runifold_core::CancellationToken::new())
        .await
        .unwrap_err();

    assert_eq!(
        error.stage,
        Some(runifold_mcp::SamplingStage::ContextResolution)
    );
}

#[tokio::test]
async fn negotiated_capabilities_carry_tools_and_context_over_the_session() {
    let model = Arc::new(ScriptedModel::new());
    model.enqueue([
        ModelStreamEvent::ResponseStarted {
            id: Some("sample-negotiated".into()),
            model: ModelRef::new("test", "model"),
        },
        ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::text("negotiated"),
        },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::Stop,
            provider_metadata: BTreeMap::new(),
        },
    ]);
    let sampling = Arc::new(
        model_sampling_service(model.clone())
            .with_context_provider(Arc::new(StaticContextProvider)),
    );
    let server = sampling_server();
    let session = server.session();
    let requester = session.sampling_client();
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("negotiated-client", "1")).with_sampling(sampling),
    );
    client.initialize().await.unwrap();
    let mut request = basic_request(32);
    request.include_context = IncludeContext::AllServers;
    request.tools.push(lookup_tool());

    let response = requester.create_message(request).await.unwrap();

    assert_eq!(response.content.as_slice()[0].as_text(), Some("negotiated"));
    let recorded = model.recorded_requests();
    assert_eq!(recorded[0].tools[0].name, "lookup");
    assert_eq!(
        recorded[0].messages[0].content[0],
        ContentPart::text("resolved context")
    );
}

#[tokio::test]
async fn tool_sampling_maps_declarations_choice_and_model_tool_use() {
    let model = Arc::new(ScriptedModel::new());
    model.enqueue([
        ModelStreamEvent::ResponseStarted {
            id: Some("sample-tool".into()),
            model: ModelRef::new("test", "model"),
        },
        ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::ToolCall(ToolCall {
                id: "call-1".into(),
                name: "lookup".into(),
                arguments: serde_json::json!({"query":"runifold"}),
                raw_arguments: Some("{ \"query\": \"runifold\" }".into()),
                metadata: BTreeMap::from([("trace.id".into(), serde_json::json!("trace-1"))]),
            }),
        },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::ToolCalls,
            provider_metadata: BTreeMap::new(),
        },
    ]);
    let service = model_sampling_service(model.clone());
    let mut request = basic_request(32);
    request.tools.push(lookup_tool());
    request.tool_choice = Some(SamplingToolChoice {
        mode: SamplingToolChoiceMode::Required,
    });

    let response = service
        .execute(request, runifold_core::CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(response.stop_reason.as_deref(), Some("toolUse"));
    assert_eq!(response.content.as_slice()[0].kind, "tool_use");
    assert!(!response.content.as_slice()[0].fields.contains_key("_meta"));
    let recorded = model.recorded_requests();
    assert_eq!(recorded[0].tools[0].name, "lookup");
    assert_eq!(recorded[0].tool_choice, ToolChoice::Required);
}

#[tokio::test]
async fn tool_sampling_history_preserves_rich_result_extensions() {
    let model = Arc::new(ScriptedModel::new());
    model.enqueue([
        ModelStreamEvent::ResponseStarted {
            id: Some("sample-history".into()),
            model: ModelRef::new("test", "model"),
        },
        ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::text("done"),
        },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::Stop,
            provider_metadata: BTreeMap::new(),
        },
    ]);
    let service = model_sampling_service(model.clone());
    let tool_use = ContentBlock {
        kind: "tool_use".into(),
        fields: BTreeMap::from([
            ("id".into(), serde_json::json!("call-1")),
            ("name".into(), serde_json::json!("lookup")),
            ("input".into(), serde_json::json!({"query":"runifold"})),
        ]),
    };
    let tool_result = rich_sampling_tool_result();
    let mut request = CreateMessageParams::new(
        vec![
            SamplingMessage::user_text("search"),
            SamplingMessage {
                role: SamplingRole::Assistant,
                content: SamplingContent::One(tool_use),
                meta: BTreeMap::new(),
            },
            SamplingMessage {
                role: SamplingRole::User,
                content: SamplingContent::One(tool_result),
                meta: BTreeMap::new(),
            },
        ],
        32,
    );
    request.tools.push(lookup_tool());

    service
        .execute(request, runifold_core::CancellationToken::new())
        .await
        .unwrap();

    let recorded = model.recorded_requests();
    assert!(matches!(
        recorded[0].messages[1].content[0],
        ContentPart::ToolCall(_)
    ));
    let ContentPart::ToolResult(ToolResult {
        name,
        structured_content,
        metadata,
        content,
        is_error,
        ..
    }) = &recorded[0].messages[2].content[0]
    else {
        panic!("expected canonical Tool result");
    };
    assert_eq!(name.as_deref(), Some("lookup"));
    assert_eq!(structured_content, &Some(serde_json::json!({"hits":1})));
    assert_eq!(metadata["trace.id"], "trace-1");
    assert_eq!(metadata["cache.key"], "stable-cache-key");
    assert!(*is_error);
    assert!(matches!(content[0], ContentPart::Audio { .. }));
    assert!(matches!(content[1], ContentPart::ResourceLink { .. }));
    assert!(matches!(content[2], ContentPart::Document { .. }));
}

#[tokio::test]
async fn sampling_bridges_unknown_input_and_non_inline_model_media() {
    let model = Arc::new(ScriptedModel::new());
    model.enqueue([
        ModelStreamEvent::ResponseStarted {
            id: Some("sample-media".into()),
            model: ModelRef::new("test", "model"),
        },
        ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::Image {
                source: MediaSource::Url {
                    url: "https://example.com/result.png".into(),
                    media_type: Some("image/png".into()),
                },
            },
        },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::Stop,
            provider_metadata: BTreeMap::new(),
        },
    ]);
    let service = model_sampling_service(model.clone());
    let request = CreateMessageParams::new(
        vec![SamplingMessage {
            role: SamplingRole::User,
            content: SamplingContent::One(ContentBlock {
                kind: "future_media".into(),
                fields: BTreeMap::from([("payload".into(), serde_json::json!({"id":7}))]),
            }),
            meta: BTreeMap::new(),
        }],
        32,
    );

    let response = service
        .execute(request, runifold_core::CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(response.content.as_slice()[0].kind, "runifold/content");
    let recorded = model.recorded_requests();
    let ContentPart::Text { text } = &recorded[0].messages[0].content[0] else {
        panic!("expected unknown MCP content to use a visible envelope");
    };
    assert!(text.contains("runifold.mcp.content.v1"));
    assert!(text.contains("future_media"));
}

#[tokio::test]
async fn sampling_never_discloses_reasoning_or_provider_private_output() {
    let reasoning_only = Arc::new(ScriptedModel::new());
    reasoning_only.enqueue([
        ModelStreamEvent::ResponseStarted {
            id: Some("sample-private-reasoning".into()),
            model: ModelRef::new("test", "model"),
        },
        ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::Reasoning(ReasoningPart {
                text: Some("private chain of thought".into()),
                signature: Some("signed-token".into()),
                redacted: false,
                provider_data: Vec::new(),
            }),
        },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::Stop,
            provider_metadata: BTreeMap::new(),
        },
    ]);
    let error = model_sampling_service(reasoning_only)
        .execute(basic_request(32), runifold_core::CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind, runifold_mcp::SamplingErrorKind::InvalidOutput);

    let opaque = Arc::new(ScriptedModel::new());
    opaque.enqueue([
        ModelStreamEvent::ResponseStarted {
            id: Some("sample-private-provider-data".into()),
            model: ModelRef::new("test", "model"),
        },
        ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::ProviderOpaque(ProviderData {
                provider: "ark".into(),
                kind: "private_event".into(),
                value: serde_json::json!({"secret":"must-not-cross-mcp"}),
            }),
        },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::Stop,
            provider_metadata: BTreeMap::new(),
        },
    ]);
    let error = model_sampling_service(opaque)
        .execute(basic_request(32), runifold_core::CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind, runifold_mcp::SamplingErrorKind::InvalidOutput);
}

#[tokio::test]
async fn sampling_rejects_host_and_provider_references_in_extensions() {
    let service = model_sampling_service(Arc::new(ScriptedModel::new()));
    let request = CreateMessageParams::new(
        vec![SamplingMessage {
            role: SamplingRole::User,
            content: SamplingContent::One(ContentBlock {
                kind: "runifold/content".into(),
                fields: BTreeMap::from([(
                    "content".into(),
                    serde_json::json!({
                        "type": "image",
                        "source": {
                            "type": "provider_file",
                            "provider": "ark",
                            "file_id": "file-private"
                        }
                    }),
                )]),
            }),
            meta: BTreeMap::new(),
        }],
        32,
    );

    let error = service
        .execute(request, runifold_core::CancellationToken::new())
        .await
        .unwrap_err();

    assert_eq!(error.kind, runifold_mcp::SamplingErrorKind::InvalidRequest);
}

#[tokio::test]
async fn sampling_rejects_undeclared_tools_and_unbalanced_results_before_execution() {
    let service = model_sampling_service(Arc::new(ScriptedModel::new()));
    let undeclared_use = ContentBlock {
        kind: "tool_use".into(),
        fields: BTreeMap::from([
            ("id".into(), serde_json::json!("call-1")),
            ("name".into(), serde_json::json!("missing")),
            ("input".into(), serde_json::json!({})),
        ]),
    };
    let mut request = CreateMessageParams::new(
        vec![SamplingMessage {
            role: SamplingRole::Assistant,
            content: SamplingContent::One(undeclared_use),
            meta: BTreeMap::new(),
        }],
        32,
    );
    request.tools.push(lookup_tool());

    let error = service
        .execute(request, runifold_core::CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind, runifold_mcp::SamplingErrorKind::InvalidRequest);

    let mixed_result = ContentBlock {
        kind: "tool_result".into(),
        fields: BTreeMap::from([
            ("toolUseId".into(), serde_json::json!("call-1")),
            ("content".into(), serde_json::json!("done")),
        ]),
    };
    let mut request = CreateMessageParams::new(
        vec![SamplingMessage {
            role: SamplingRole::User,
            content: SamplingContent::Many(vec![
                mixed_result,
                ContentBlock::text("not allowed beside a result"),
            ]),
            meta: BTreeMap::new(),
        }],
        32,
    );
    request.tools.push(lookup_tool());

    let error = service
        .execute(request, runifold_core::CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind, runifold_mcp::SamplingErrorKind::InvalidRequest);
}

#[tokio::test]
async fn nested_tool_result_blocks_share_the_global_content_limit() {
    let service = SamplingService::new(
        Arc::new(TestApprover {
            mode: ReviewMode::Approve,
            reviews: Arc::new(ReviewCounts::default()),
        }),
        Arc::new(ModelSamplingProvider::new(
            Arc::new(ScriptedModel::new()),
            Arc::new(FixedSamplingModel::new(ModelRef::new("test", "model"))),
        )),
        SamplingPolicy {
            max_content_blocks: 2,
            ..SamplingPolicy::default()
        },
    );
    let mut request = CreateMessageParams::new(
        vec![SamplingMessage {
            role: SamplingRole::User,
            content: SamplingContent::One(ContentBlock {
                kind: "tool_result".into(),
                fields: BTreeMap::from([
                    ("toolUseId".into(), serde_json::json!("call-1")),
                    (
                        "content".into(),
                        serde_json::json!([
                            {"type":"text","text":"one"},
                            {"type":"text","text":"two"}
                        ]),
                    ),
                ]),
            }),
            meta: BTreeMap::new(),
        }],
        32,
    );
    request.tools.push(lookup_tool());

    let error = service
        .execute(request, runifold_core::CancellationToken::new())
        .await
        .unwrap_err();

    assert_eq!(error.kind, runifold_mcp::SamplingErrorKind::LimitExceeded);
}

#[tokio::test]
async fn failed_token_reservation_rolls_back_the_request_counter() {
    let service = SamplingService::new(
        Arc::new(TestApprover {
            mode: ReviewMode::Approve,
            reviews: Arc::new(ReviewCounts::default()),
        }),
        Arc::new(EchoProvider),
        SamplingPolicy {
            max_tokens_per_request: 10,
            max_total_requests: 2,
            max_total_requested_tokens: 10,
            ..SamplingPolicy::default()
        },
    );

    service
        .execute(basic_request(6), runifold_core::CancellationToken::new())
        .await
        .unwrap();
    let error = service
        .execute(basic_request(6), runifold_core::CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind, runifold_mcp::SamplingErrorKind::LimitExceeded);
    service
        .execute(basic_request(4), runifold_core::CancellationToken::new())
        .await
        .unwrap();
}

#[tokio::test]
async fn model_selector_receives_tool_and_media_requirements() {
    let model = Arc::new(ScriptedModel::new());
    model.enqueue([
        ModelStreamEvent::ResponseStarted {
            id: Some("sample-requirements".into()),
            model: ModelRef::new("test", "model"),
        },
        ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::text("selected"),
        },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::Stop,
            provider_metadata: BTreeMap::new(),
        },
    ]);
    let selector = Arc::new(RecordingSelector::default());
    let service = SamplingService::new(
        Arc::new(TestApprover {
            mode: ReviewMode::Approve,
            reviews: Arc::new(ReviewCounts::default()),
        }),
        Arc::new(ModelSamplingProvider::new(model, selector.clone())),
        SamplingPolicy::default(),
    );
    let mut request = CreateMessageParams::new(
        vec![SamplingMessage {
            role: SamplingRole::User,
            content: SamplingContent::Many(vec![
                ContentBlock::text("inspect"),
                ContentBlock {
                    kind: "image".into(),
                    fields: BTreeMap::from([
                        ("mimeType".into(), serde_json::json!("image/png")),
                        ("data".into(), serde_json::json!("aQ==")),
                    ]),
                },
                ContentBlock {
                    kind: "runifold/content".into(),
                    fields: BTreeMap::from([(
                        "content".into(),
                        serde_json::json!({
                            "type":"document",
                            "source": {
                                "type":"url",
                                "url":"https://example.com/guide.pdf",
                                "media_type":"application/pdf"
                            },
                            "name":"guide.pdf"
                        }),
                    )]),
                },
            ]),
            meta: BTreeMap::new(),
        }],
        32,
    );
    request.tools.push(lookup_tool());

    service
        .execute(request, runifold_core::CancellationToken::new())
        .await
        .unwrap();

    assert_eq!(
        selector.seen.lock().unwrap().as_slice(),
        &[SamplingModelRequirements::from_features([
            SamplingModelFeature::Tools,
            SamplingModelFeature::ImageInput,
            SamplingModelFeature::DocumentInput,
        ])]
    );
}

#[tokio::test]
async fn sampling_enforces_tool_choice_on_provider_output() {
    let mut required = basic_request(32);
    required.tools.push(lookup_tool());
    required.tool_choice = Some(SamplingToolChoice {
        mode: SamplingToolChoiceMode::Required,
    });
    let error = SamplingService::new(
        Arc::new(TestApprover {
            mode: ReviewMode::Approve,
            reviews: Arc::new(ReviewCounts::default()),
        }),
        Arc::new(ToolCapableEchoProvider),
        SamplingPolicy::default(),
    )
    .execute(required, runifold_core::CancellationToken::new())
    .await
    .unwrap_err();
    assert_eq!(error.kind, runifold_mcp::SamplingErrorKind::InvalidOutput);

    let model = Arc::new(ScriptedModel::new());
    model.enqueue([
        ModelStreamEvent::ResponseStarted {
            id: Some("sample-disabled-tool".into()),
            model: ModelRef::new("test", "model"),
        },
        ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::ToolCall(ToolCall {
                id: "call-disabled".into(),
                name: "lookup".into(),
                arguments: serde_json::json!({}),
                raw_arguments: None,
                metadata: BTreeMap::new(),
            }),
        },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::ToolCalls,
            provider_metadata: BTreeMap::new(),
        },
    ]);
    let mut disabled = basic_request(32);
    disabled.tools.push(lookup_tool());
    disabled.tool_choice = Some(SamplingToolChoice {
        mode: SamplingToolChoiceMode::None,
    });
    let error = model_sampling_service(model)
        .execute(disabled, runifold_core::CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind, runifold_mcp::SamplingErrorKind::InvalidOutput);
}

#[tokio::test]
async fn nested_and_extension_media_cannot_bypass_sampling_byte_limits() {
    let service = SamplingService::new(
        Arc::new(TestApprover {
            mode: ReviewMode::Approve,
            reviews: Arc::new(ReviewCounts::default()),
        }),
        Arc::new(ToolCapableEchoProvider),
        SamplingPolicy {
            max_media_bytes: 1,
            ..SamplingPolicy::default()
        },
    );
    let extension = CreateMessageParams::new(
        vec![SamplingMessage {
            role: SamplingRole::User,
            content: SamplingContent::One(ContentBlock {
                kind: "runifold/content".into(),
                fields: BTreeMap::from([(
                    "content".into(),
                    serde_json::json!({
                        "type":"document",
                        "source": {
                            "type":"base64",
                            "media_type":"application/pdf",
                            "data":"YWI="
                        },
                        "name":"report.pdf"
                    }),
                )]),
            }),
            meta: BTreeMap::new(),
        }],
        32,
    );
    let error = service
        .execute(extension, runifold_core::CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind, runifold_mcp::SamplingErrorKind::LimitExceeded);

    let tool_use = ContentBlock {
        kind: "tool_use".into(),
        fields: BTreeMap::from([
            ("id".into(), serde_json::json!("call-1")),
            ("name".into(), serde_json::json!("lookup")),
            ("input".into(), serde_json::json!({})),
        ]),
    };
    let tool_result = ContentBlock {
        kind: "tool_result".into(),
        fields: BTreeMap::from([
            ("toolUseId".into(), serde_json::json!("call-1")),
            (
                "content".into(),
                serde_json::json!([{
                    "type":"resource",
                    "resource": {
                        "uri":"memory://large.bin",
                        "mimeType":"application/octet-stream",
                        "blob":"YWI="
                    }
                }]),
            ),
        ]),
    };
    let mut request = CreateMessageParams::new(
        vec![
            SamplingMessage {
                role: SamplingRole::Assistant,
                content: SamplingContent::One(tool_use),
                meta: BTreeMap::new(),
            },
            SamplingMessage {
                role: SamplingRole::User,
                content: SamplingContent::One(tool_result),
                meta: BTreeMap::new(),
            },
        ],
        32,
    );
    request.tools.push(lookup_tool());
    let error = service
        .execute(request, runifold_core::CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind, runifold_mcp::SamplingErrorKind::LimitExceeded);
}

fn model_sampling_service(model: Arc<ScriptedModel>) -> SamplingService {
    SamplingService::new(
        Arc::new(TestApprover {
            mode: ReviewMode::Approve,
            reviews: Arc::new(ReviewCounts::default()),
        }),
        Arc::new(ModelSamplingProvider::new(
            model,
            Arc::new(FixedSamplingModel::new(ModelRef::new("test", "model"))),
        )),
        SamplingPolicy::default(),
    )
}

fn lookup_tool() -> McpTool {
    McpTool {
        name: "lookup".into(),
        title: None,
        description: Some("look up a value".into()),
        input_schema: serde_json::json!({"type":"object"}),
        output_schema: Some(serde_json::json!({"type":"object"})),
        annotations: None,
    }
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

fn rich_sampling_tool_result() -> ContentBlock {
    ContentBlock {
        kind: "tool_result".into(),
        fields: BTreeMap::from([
            ("toolUseId".into(), serde_json::json!("call-1")),
            (
                "content".into(),
                serde_json::json!([
                    {"type":"audio","mimeType":"audio/wav","data":"YQ=="},
                    {
                        "type":"resource_link",
                        "uri":"docs://runifold/guide",
                        "name":"guide",
                        "mimeType":"text/markdown",
                        "size":42
                    },
                    {
                        "type":"resource",
                        "resource": {
                            "uri":"memory://report.pdf",
                            "mimeType":"application/pdf",
                            "blob":"YQ=="
                        }
                    }
                ]),
            ),
            ("structuredContent".into(), serde_json::json!({"hits": 1})),
            ("isError".into(), serde_json::json!(true)),
            (
                "_meta".into(),
                serde_json::json!({"runifold.tool_result.v1": {
                    "name": "lookup",
                    "metadata": {"trace.id": "trace-1"}
                }, "cache.key": "stable-cache-key"}),
            ),
        ]),
    }
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

#[derive(Debug)]
struct TestSamplingTaskBackend {
    task: Mutex<McpTask>,
    requested_ttl: Mutex<Option<u64>>,
    get_calls: AtomicUsize,
    request: Mutex<Option<CreateMessageParams>>,
    create_calls: AtomicUsize,
    reject_next_create: AtomicBool,
    input_required_once: AtomicBool,
    terminal_error: Mutex<Option<runifold_mcp::JsonRpcError>>,
}

impl TestSamplingTaskBackend {
    fn new() -> Self {
        Self {
            task: Mutex::new(McpTask {
                task_id: "sampling-task-1".into(),
                status: TaskStatus::Working,
                status_message: Some("queued".into()),
                created_at: "2026-08-08T00:00:00Z".into(),
                last_updated_at: "2026-08-08T00:00:00Z".into(),
                ttl_ms: None,
                poll_interval_ms: Some(1),
                input_requests: BTreeMap::new(),
                result: None,
                error: None,
            }),
            requested_ttl: Mutex::new(None),
            get_calls: AtomicUsize::new(0),
            request: Mutex::new(None),
            create_calls: AtomicUsize::new(0),
            reject_next_create: AtomicBool::new(false),
            input_required_once: AtomicBool::new(false),
            terminal_error: Mutex::new(None),
        }
    }

    fn requested_ttl(&self) -> Option<u64> {
        *self.requested_ttl.lock().unwrap()
    }
}

impl McpSamplingTaskBackend for TestSamplingTaskBackend {
    fn create_message_task(
        &self,
        request: SamplingTaskRequest,
    ) -> McpTaskFuture<'_, SamplingTaskCreation> {
        let sequence = self.create_calls.fetch_add(1, Ordering::AcqRel) + 1;
        if self.reject_next_create.swap(false, Ordering::AcqRel) {
            return Box::pin(async {
                Err(McpTaskBackendError::new(
                    McpTaskBackendErrorKind::AdmissionDenied,
                    "task admission denied",
                ))
            });
        }
        *self.requested_ttl.lock().unwrap() =
            request.params.task.as_ref().and_then(|task| task.ttl);
        *self.request.lock().unwrap() = Some(request.params);
        self.get_calls.store(0, Ordering::Release);
        let task = {
            let mut task = self.task.lock().unwrap();
            task.task_id = format!("sampling-task-{sequence}");
            task.status = TaskStatus::Working;
            task.status_message = Some("queued".into());
            task.input_requests.clear();
            task.clone()
        };
        Box::pin(async move {
            Ok(SamplingTaskCreation {
                task,
                created: true,
            })
        })
    }

    fn get(&self, _task_id: String) -> McpTaskFuture<'_, McpTask> {
        let get_call = self.get_calls.fetch_add(1, Ordering::AcqRel);
        if self.input_required_once.swap(false, Ordering::AcqRel) && get_call == 0 {
            let mut task = self.task.lock().unwrap();
            task.status = TaskStatus::InputRequired;
            task.status_message = Some("input required".into());
            task.input_requests.insert(
                "approval".into(),
                runifold_mcp::InputRequest::new(
                    "elicitation/create",
                    Some(serde_json::json!({
                        "message": "Approve task",
                        "requestedSchema": {"type": "object"}
                    })),
                ),
            );
        } else if get_call == 0 || self.task.lock().unwrap().status == TaskStatus::InputRequired {
            let mut task = self.task.lock().unwrap();
            task.input_requests.clear();
            if self.terminal_error.lock().unwrap().is_some() {
                task.status = TaskStatus::Failed;
                task.status_message = Some("failed".into());
            } else {
                task.status = TaskStatus::Completed;
                task.status_message = Some("completed".into());
            }
            task.last_updated_at = "2026-08-08T00:00:01Z".into();
        }
        let task = self.task.lock().unwrap().clone();
        Box::pin(async move { Ok(task) })
    }

    fn result(&self, _task_id: String) -> McpTaskFuture<'_, SamplingTaskTerminalResult> {
        if let Some(error) = self.terminal_error.lock().unwrap().clone() {
            return Box::pin(async move { Ok(SamplingTaskTerminalResult::Error(error)) });
        }
        let request = self.request.lock().unwrap().clone().unwrap();
        Box::pin(async move {
            Ok(SamplingTaskTerminalResult::Success(Box::new(
                SamplingTaskOutput {
                    request,
                    result: CreateMessageResult::assistant_text("host-model", "task sampled"),
                },
            )))
        })
    }

    fn cancel(&self, _task_id: String) -> McpTaskFuture<'_, ()> {
        if self.task.lock().unwrap().status.is_terminal() {
            return Box::pin(async {
                Err(McpTaskBackendError::new(
                    McpTaskBackendErrorKind::InvalidInput,
                    "terminal Sampling Task cannot be cancelled",
                ))
            });
        }
        self.get_calls.store(1, Ordering::Release);
        let mut task = self.task.lock().unwrap();
        task.status = TaskStatus::Cancelled;
        task.status_message = Some("cancelled".into());
        task.last_updated_at = "2026-08-08T00:00:01Z".into();
        Box::pin(async { Ok(()) })
    }
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

struct ToolCapableEchoProvider;

impl SamplingProvider for ToolCapableEchoProvider {
    fn supports_tools(&self) -> bool {
        true
    }

    fn sample(
        &self,
        _request: CreateMessageParams,
        _context: SamplingCallContext,
    ) -> SamplingFuture<'_, CreateMessageResult> {
        Box::pin(async { Ok(CreateMessageResult::assistant_text("host-model", "sampled")) })
    }
}

struct StaticContextProvider;

impl SamplingContextProvider for StaticContextProvider {
    fn resolve(
        &self,
        _include: IncludeContext,
        _context: SamplingCallContext,
    ) -> SamplingFuture<'_, Vec<SamplingMessage>> {
        Box::pin(async { Ok(vec![SamplingMessage::user_text("resolved context")]) })
    }
}

struct FailingContextProvider;

impl SamplingContextProvider for FailingContextProvider {
    fn resolve(
        &self,
        _include: IncludeContext,
        _context: SamplingCallContext,
    ) -> SamplingFuture<'_, Vec<SamplingMessage>> {
        Box::pin(async {
            Err(SamplingError::new(
                runifold_mcp::SamplingErrorKind::Execution,
                "context backend unavailable",
            ))
        })
    }
}

#[derive(Default)]
struct RecordingSelector {
    seen: Mutex<Vec<SamplingModelRequirements>>,
}

impl SamplingModelSelector for RecordingSelector {
    fn select(
        &self,
        _preferences: Option<&runifold_mcp::ModelPreferences>,
    ) -> Result<ModelRef, SamplingError> {
        Ok(ModelRef::new("test", "model"))
    }

    fn select_with_requirements(
        &self,
        _preferences: Option<&runifold_mcp::ModelPreferences>,
        requirements: &SamplingModelRequirements,
    ) -> Result<ModelRef, SamplingError> {
        self.seen.lock().unwrap().push(requirements.clone());
        Ok(ModelRef::new("test", "model"))
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
