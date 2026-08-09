//! Durable workflow adapter tests for MCP Tasks.

#![cfg(feature = "workflow-tasks")]

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use runifold_core::{Budget, BudgetTracker, CapabilitySet, RunContext};
use runifold_mcp::{
    CallToolOutcome, CallToolParams, CreateMessageOutcome, CreateMessageParams,
    CreateMessageResult, Implementation, JsonRpcError, McpClient, McpClientConfig, McpError,
    McpServer, McpTaskBackend, SamplingApprover, SamplingCallContext, SamplingDecision,
    SamplingError, SamplingFuture, SamplingMessage, SamplingPolicy, SamplingProvider,
    SamplingService, SamplingTaskApprovalClaim, SamplingTaskIdempotencyNamespace,
    SamplingTaskRequest, TaskMetadata, TaskStatus, ToolTaskRequest, WorkflowSamplingTaskResult,
    WorkflowSamplingTaskRoute, WorkflowTaskAdapter, WorkflowTaskRoute,
};
use runifold_store_sqlite::SqliteWorkflowStore;
use runifold_tool::{FunctionTool, Tool, ToolRegistry};
use runifold_workflow::{
    InMemoryWorkflowStore, LeaseDuration, WorkerId, Workflow, WorkflowClock, WorkflowDefinition,
    WorkflowDisposition, WorkflowInterruptRequest, WorkflowRegistry, WorkflowSignalRetention,
    WorkflowStep, WorkflowStepFuture, WorkflowStore, WorkflowTaskStatus, WorkflowTenantId,
    WorkflowWait, WorkflowWorker, WorkflowWorkerOutcome,
};
use serde_json::{Value, json};

#[derive(Debug, Default)]
struct AdjustableClock(AtomicU64);

impl AdjustableClock {
    fn advance(&self, milliseconds: u64) {
        self.0.fetch_add(milliseconds, Ordering::SeqCst);
    }
}

impl WorkflowClock for AdjustableClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[tokio::test]
async fn sampling_approval_claim_is_cross_instance_and_fences_expired_owner() {
    let clock = Arc::new(AdjustableClock::default());
    let store = Arc::new(InMemoryWorkflowStore::with_clock(clock.clone()));
    let first = sampling_adapter(store.clone());
    let second = sampling_adapter(store.clone());
    let task = runifold_mcp::McpSamplingTaskBackend::create_message_task(
        &first,
        SamplingTaskRequest {
            params: CreateMessageParams::new(vec![SamplingMessage::user_text("approve once")], 64),
        },
    )
    .await
    .unwrap()
    .task;
    let claimed = store
        .claim(
            WorkerId::parse("sampling-approval-worker").unwrap(),
            LeaseDuration::new(Duration::from_secs(1)).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    store
        .finish(claimed.lease, WorkflowDisposition::Completed)
        .await
        .unwrap();

    let first_token = match runifold_mcp::McpSamplingTaskBackend::claim_result_approval(
        &first,
        task.task_id.clone(),
        1_000,
    )
    .await
    .unwrap()
    {
        SamplingTaskApprovalClaim::Acquired { token } => token,
        other => panic!("first adapter must acquire the claim, got {other:?}"),
    };
    assert!(matches!(
        runifold_mcp::McpSamplingTaskBackend::claim_result_approval(
            &second,
            task.task_id.clone(),
            1_000,
        )
        .await
        .unwrap(),
        SamplingTaskApprovalClaim::Busy {
            retry_after_ms: 1_000
        }
    ));

    clock.advance(1_000);
    let second_token = match runifold_mcp::McpSamplingTaskBackend::claim_result_approval(
        &second,
        task.task_id.clone(),
        1_000,
    )
    .await
    .unwrap()
    {
        SamplingTaskApprovalClaim::Acquired { token } => token,
        other => panic!("second adapter must take over the expired claim, got {other:?}"),
    };
    let approved = CreateMessageResult::assistant_text("approval-model", "approved once");
    let stale = runifold_mcp::McpSamplingTaskBackend::complete_result_approval(
        &first,
        task.task_id.clone(),
        first_token,
        approved.clone(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        stale.kind,
        runifold_mcp::McpTaskBackendErrorKind::AdmissionDenied
    );
    let stored = runifold_mcp::McpSamplingTaskBackend::complete_result_approval(
        &second,
        task.task_id.clone(),
        second_token,
        approved.clone(),
    )
    .await
    .unwrap();
    assert_eq!(stored, approved);
    assert!(matches!(
        runifold_mcp::McpSamplingTaskBackend::claim_result_approval(
            &first,
            task.task_id,
            1_000,
        )
        .await
        .unwrap(),
        SamplingTaskApprovalClaim::Completed(result) if result == approved
    ));
}

#[tokio::test]
async fn sampling_task_survives_sqlite_reopen_and_recovers_validated_result() {
    let database = TemporarySqlite::new();
    let store = Arc::new(SqliteWorkflowStore::open(&database.path).unwrap());
    let adapter = Arc::new(sampling_adapter(store.clone()));
    let server = empty_server();
    let session = server.session();
    let requester = session
        .sampling_client()
        .with_timeout(Duration::from_secs(2));
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("sampling-workflow-client", "1"))
            .with_sampling(sampling_service())
            .with_sampling_tasks(adapter),
    );
    client.initialize().await.unwrap();
    let mut request =
        CreateMessageParams::new(vec![SamplingMessage::user_text("persist this request")], 64);
    request.task = Some(TaskMetadata { ttl: Some(60_000) });

    let task = match requester.create_message_outcome(request).await.unwrap() {
        CreateMessageOutcome::Task(task) => task,
        CreateMessageOutcome::Complete(_) => panic!("expected a durable Sampling Task"),
    };
    assert_eq!(task.status, TaskStatus::Working);
    drop(client);
    drop(requester);
    drop(server);

    let workflow = Workflow::builder("sampling-flow")
        .step("sample", SamplingResultStep, CapabilitySet::new())
        .build()
        .unwrap();
    let mut definitions = WorkflowRegistry::new();
    definitions
        .register(WorkflowDefinition::new(
            Arc::new(workflow),
            Budget::default(),
            CapabilitySet::new(),
        ))
        .unwrap();
    let worker = WorkflowWorker::new(
        store.clone(),
        definitions,
        WorkerId::parse("sampling-task-worker").unwrap(),
        LeaseDuration::new(Duration::from_secs(1)).unwrap(),
        Duration::from_millis(100),
    )
    .unwrap();
    assert!(matches!(
        worker.run_once().await.unwrap(),
        WorkflowWorkerOutcome::Completed { .. }
    ));
    drop(worker);
    drop(store);

    let reopened = Arc::new(SqliteWorkflowStore::open(&database.path).unwrap());
    let recovered_adapter = Arc::new(sampling_adapter(reopened.clone()));
    let recovered_server = empty_server();
    let recovered_session = recovered_server.session();
    let recovered_requester = recovered_session
        .sampling_client()
        .with_timeout(Duration::from_secs(2));
    let recovered_client = McpClient::new(
        Arc::new(recovered_session),
        McpClientConfig::new(Implementation::new("recovered-sampling-client", "1"))
            .with_sampling(sampling_service())
            .with_sampling_tasks(recovered_adapter.clone()),
    );
    recovered_client.initialize().await.unwrap();

    let completed = recovered_requester.get_task(&task.task_id).await.unwrap();
    assert_eq!(completed.status, TaskStatus::Completed);
    let result = recovered_requester.wait_task(completed).await.unwrap();
    assert_eq!(result.model, "workflow-model");
    assert_eq!(
        result.content.as_slice()[0].as_text(),
        Some("durable answer")
    );
    assert_eq!(
        result.meta["io.modelcontextprotocol/related-task"]["taskId"],
        task.task_id
    );
    assert!(
        runifold_mcp::McpSamplingTaskBackend::approved_result(
            recovered_adapter.as_ref(),
            task.task_id.clone(),
        )
        .await
        .unwrap()
        .is_some()
    );
    compact_ordinary_signals(&reopened).await;
    drop(recovered_client);
    drop(recovered_requester);
    drop(recovered_server);
    drop(recovered_adapter);

    let final_store = Arc::new(SqliteWorkflowStore::open(&database.path).unwrap());
    let final_adapter = sampling_adapter(final_store);
    assert!(
        runifold_mcp::McpSamplingTaskBackend::approved_result(&final_adapter, task.task_id)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn sampling_task_create_retry_recovers_the_same_durable_task() {
    let database = TemporarySqlite::new();
    let store = Arc::new(SqliteWorkflowStore::open(&database.path).unwrap());
    let adapter = Arc::new(sampling_adapter(store));
    let server = empty_server();
    let session = server.session();
    let requester = session.sampling_client();
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("sampling-idempotency-client", "1"))
            .with_sampling(sampling_service())
            .with_sampling_tasks(adapter),
    );
    client.initialize().await.unwrap();
    let mut request =
        CreateMessageParams::new(vec![SamplingMessage::user_text("create exactly once")], 64)
            .with_task_idempotency_key("018f6f7e-6f1d-7f2a-9c40-7f4f8f0a3d21");
    request.task = Some(TaskMetadata { ttl: Some(60_000) });

    let first = match requester
        .create_message_outcome(request.clone())
        .await
        .unwrap()
    {
        CreateMessageOutcome::Task(task) => task,
        CreateMessageOutcome::Complete(_) => panic!("expected a durable Sampling Task"),
    };
    drop(client);
    drop(requester);
    drop(server);

    let reopened = Arc::new(SqliteWorkflowStore::open(&database.path).unwrap());
    let recovered_adapter = Arc::new(sampling_adapter(reopened));
    let recovered_server = empty_server();
    let recovered_session = recovered_server.session();
    let recovered_requester = recovered_session.sampling_client();
    let recovered_client = McpClient::new(
        Arc::new(recovered_session),
        McpClientConfig::new(Implementation::new("sampling-idempotency-recovery", "1"))
            .with_sampling(sampling_service())
            .with_sampling_tasks(recovered_adapter),
    );
    recovered_client.initialize().await.unwrap();
    let recovered = match recovered_requester
        .create_message_outcome(request)
        .await
        .unwrap()
    {
        CreateMessageOutcome::Task(task) => task,
        CreateMessageOutcome::Complete(_) => panic!("expected a durable Sampling Task"),
    };

    assert_eq!(first.task_id, recovered.task_id);
    assert_ne!(first.task_id, "018f6f7e-6f1d-7f2a-9c40-7f4f8f0a3d21");

    let mut conflicting =
        CreateMessageParams::new(vec![SamplingMessage::user_text("different request")], 64)
            .with_task_idempotency_key("018f6f7e-6f1d-7f2a-9c40-7f4f8f0a3d21");
    conflicting.task = Some(TaskMetadata { ttl: Some(60_000) });
    assert!(
        recovered_requester
            .create_message_outcome(conflicting)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn concurrent_sampling_creates_with_one_key_converge_on_one_task() {
    let store = Arc::new(SqliteWorkflowStore::open_in_memory().unwrap());
    let adapter = Arc::new(sampling_adapter(store));
    let server = empty_server();
    let session = server.session();
    let requester = session.sampling_client();
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("sampling-idempotency-race", "1"))
            .with_sampling(sampling_service())
            .with_sampling_tasks(adapter),
    );
    client.initialize().await.unwrap();
    let mut request =
        CreateMessageParams::new(vec![SamplingMessage::user_text("concurrent create")], 64)
            .with_task_idempotency_key("f84ab8cf-68ea-49bc-98fc-68d3ff88bb30");
    request.task = Some(TaskMetadata { ttl: Some(60_000) });

    let (first, second) = tokio::join!(
        requester.create_message_outcome(request.clone()),
        requester.create_message_outcome(request),
    );
    let task_id = |outcome: Result<CreateMessageOutcome, McpError>| match outcome.unwrap() {
        CreateMessageOutcome::Task(task) => task.task_id,
        CreateMessageOutcome::Complete(_) => panic!("expected a durable Sampling Task"),
    };

    assert_eq!(task_id(first), task_id(second));
}

#[tokio::test]
async fn sampling_workflow_preserves_exact_terminal_json_rpc_error() {
    let store = Arc::new(InMemoryWorkflowStore::new());
    let adapter = Arc::new(sampling_adapter(store.clone()));
    let server = empty_server();
    let session = server.session();
    let requester = session
        .sampling_client()
        .with_timeout(Duration::from_secs(2));
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("sampling-error-client", "1"))
            .with_sampling(sampling_service())
            .with_sampling_tasks(adapter),
    );
    client.initialize().await.unwrap();
    let mut request = CreateMessageParams::new(
        vec![SamplingMessage::user_text("return a durable error")],
        64,
    );
    request.task = Some(TaskMetadata { ttl: Some(60_000) });
    let task = match requester.create_message_outcome(request).await.unwrap() {
        CreateMessageOutcome::Task(task) => task,
        CreateMessageOutcome::Complete(_) => panic!("expected a durable Sampling Task"),
    };

    let workflow = Workflow::builder("sampling-flow")
        .step("sample", ExactSamplingErrorStep, CapabilitySet::new())
        .build()
        .unwrap();
    let mut definitions = WorkflowRegistry::new();
    definitions
        .register(WorkflowDefinition::new(
            Arc::new(workflow),
            Budget::default(),
            CapabilitySet::new(),
        ))
        .unwrap();
    let worker = WorkflowWorker::new(
        store,
        definitions,
        WorkerId::parse("sampling-error-worker").unwrap(),
        LeaseDuration::new(Duration::from_secs(1)).unwrap(),
        Duration::from_millis(100),
    )
    .unwrap();
    assert!(matches!(
        worker.run_once().await.unwrap(),
        WorkflowWorkerOutcome::Completed { .. }
    ));

    let error = requester.wait_task(task).await.unwrap_err();
    assert!(matches!(
        error,
        McpError::Remote {
            code: -32_077,
            ref message,
            data: Some(_),
        } if message == "durable provider rejection"
    ));
}

#[tokio::test]
async fn workflow_store_is_the_only_task_state_machine_and_survives_reconnect() {
    let store = Arc::new(InMemoryWorkflowStore::new());
    let mut adapter = WorkflowTaskAdapter::new(store.clone());
    adapter
        .register_route(
            WorkflowTaskRoute::new(
                "durable_echo",
                "durable-echo",
                1,
                WorkflowTenantId::default(),
            )
            .unwrap(),
        )
        .unwrap();

    let tool = Arc::new(FunctionTool::<Value, Value, _>::new(
        "durable_echo",
        "enqueue durable echo",
        |input, _| async move { Ok(input) },
    ));
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(tool.descriptor().capability());
    let mut tools = ToolRegistry::new();
    tools.register(tool).unwrap();
    let server = McpServer::new(
        Arc::new(tools),
        RunContext::root(BudgetTracker::new(Budget::default()), capabilities),
        Implementation::new("workflow-task-server", "1"),
    )
    .with_task_backend(Arc::new(adapter));

    let client = task_client(server.session()).await;
    let task = match client
        .call_tool_outcome(CallToolParams {
            name: "durable_echo".into(),
            arguments: Some(serde_json::Map::from_iter([("value".into(), json!(7))])),
        })
        .await
        .unwrap()
    {
        CallToolOutcome::Task(task) => task,
        CallToolOutcome::Complete(_) => panic!("workflow route must create a Task"),
    };
    assert_eq!(task.status, TaskStatus::Working);

    let workflow = Workflow::builder("durable-echo")
        .step("echo", EchoStep, CapabilitySet::new())
        .build()
        .unwrap();
    let mut definitions = WorkflowRegistry::new();
    definitions
        .register(WorkflowDefinition::new(
            Arc::new(workflow),
            Budget::default(),
            CapabilitySet::new(),
        ))
        .unwrap();
    let worker = WorkflowWorker::new(
        store,
        definitions,
        WorkerId::parse("mcp-task-worker").unwrap(),
        LeaseDuration::new(Duration::from_secs(1)).unwrap(),
        Duration::from_millis(100),
    )
    .unwrap();
    assert!(matches!(
        worker.run_once().await.unwrap(),
        WorkflowWorkerOutcome::Completed { .. }
    ));

    // A new transport/client recovers the same durable task solely from its ID.
    let recovered = task_client(server.session()).await;
    let completed = recovered.get_task(&task.task_id).await.unwrap();
    assert_eq!(completed.status, TaskStatus::Completed);
    let result = recovered.wait_task(completed).await.unwrap();
    assert_eq!(result.structured_content, Some(json!({"value": 7})));
}

#[tokio::test]
async fn workflow_interrupt_maps_to_task_input_and_update_wakes_durably() {
    let store = Arc::new(InMemoryWorkflowStore::new());
    let mut adapter = WorkflowTaskAdapter::new(store.clone());
    adapter
        .register_route(
            WorkflowTaskRoute::new("review", "review-flow", 1, WorkflowTenantId::default())
                .unwrap(),
        )
        .unwrap();
    let task = adapter
        .create_tool_task(ToolTaskRequest {
            name: "review".into(),
            arguments: serde_json::Map::new(),
            context: RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new()),
        })
        .await
        .unwrap();
    let claimed = store
        .claim(
            WorkerId::parse("review-worker").unwrap(),
            LeaseDuration::new(Duration::from_secs(1)).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let interrupt =
        WorkflowInterruptRequest::new("Approve the proposal", json!({"amount": 42})).unwrap();
    store
        .finish(
            claimed.lease,
            WorkflowDisposition::Suspend(WorkflowWait::Interrupt {
                request: interrupt.clone(),
            }),
        )
        .await
        .unwrap();

    let waiting = adapter.get(task.task_id.clone()).await.unwrap();
    assert_eq!(waiting.status, TaskStatus::InputRequired);
    let key = interrupt.interrupt_id.as_checkpoint_id().to_string();
    assert!(waiting.input_requests.contains_key(&key));
    adapter
        .update(
            task.task_id.clone(),
            std::collections::BTreeMap::from([(
                key,
                json!({
                    "action": "accept",
                    "content": {"decision": "approve"}
                }),
            )]),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .inspect(WorkflowTenantId::default(), claimed.task.checkpoint_id)
            .await
            .unwrap()
            .status,
        WorkflowTaskStatus::Queued
    );
}

async fn task_client(session: runifold_mcp::McpSession) -> McpClient {
    let client = McpClient::new(
        Arc::new(session),
        McpClientConfig::new(Implementation::new("workflow-task-client", "1")).with_tasks(),
    );
    client.connect().await.unwrap();
    client
}

#[derive(Debug)]
struct EchoStep;

impl WorkflowStep for EchoStep {
    fn execute<'a>(&'a self, input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        Box::pin(async move { Ok(input) })
    }
}

#[derive(Debug)]
struct ExactSamplingErrorStep;

impl WorkflowStep for ExactSamplingErrorStep {
    fn execute<'a>(&'a self, _input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        Box::pin(async {
            Ok(
                serde_json::to_value(WorkflowSamplingTaskResult::Error(JsonRpcError {
                    code: -32_077,
                    message: "durable provider rejection".into(),
                    data: Some(json!({"retryable": false})),
                }))
                .expect("the static Sampling error fixture is serializable"),
            )
        })
    }
}

fn sampling_adapter<S>(store: Arc<S>) -> WorkflowTaskAdapter
where
    S: WorkflowStore + 'static,
{
    let mut adapter = WorkflowTaskAdapter::new(store);
    adapter
        .register_sampling_route(
            WorkflowSamplingTaskRoute::new("sampling-flow", 1, WorkflowTenantId::default())
                .unwrap(),
        )
        .unwrap();
    adapter.with_sampling_idempotency_namespace(
        SamplingTaskIdempotencyNamespace::parse("7c442150-5f45-4f1b-a713-355fc929834d").unwrap(),
    )
}

fn empty_server() -> McpServer {
    McpServer::new(
        Arc::new(ToolRegistry::new()),
        RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new()),
        Implementation::new("sampling-workflow-server", "1"),
    )
}

fn sampling_service() -> Arc<SamplingService> {
    Arc::new(SamplingService::new(
        Arc::new(ApproveAllSampling),
        Arc::new(UnusedSamplingProvider),
        SamplingPolicy::default(),
    ))
}

async fn compact_ordinary_signals(store: &SqliteWorkflowStore) {
    tokio::time::sleep(Duration::from_millis(2)).await;
    store
        .compact_signals(
            WorkflowTenantId::default(),
            WorkflowSignalRetention::new(Duration::from_millis(1)).unwrap(),
        )
        .await
        .unwrap();
}

#[derive(Debug)]
struct SamplingResultStep;

impl WorkflowStep for SamplingResultStep {
    fn execute<'a>(&'a self, _input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        Box::pin(async {
            Ok(serde_json::to_value(CreateMessageResult::assistant_text(
                "workflow-model",
                "durable answer",
            ))
            .unwrap())
        })
    }
}

#[derive(Debug)]
struct ApproveAllSampling;

impl SamplingApprover for ApproveAllSampling {
    fn review_request(
        &self,
        request: CreateMessageParams,
        _context: SamplingCallContext,
    ) -> SamplingFuture<'_, SamplingDecision<CreateMessageParams>> {
        Box::pin(async move { Ok(SamplingDecision::Approve(request)) })
    }

    fn review_response(
        &self,
        response: CreateMessageResult,
        _context: SamplingCallContext,
    ) -> SamplingFuture<'_, SamplingDecision<CreateMessageResult>> {
        Box::pin(async move { Ok(SamplingDecision::Approve(response)) })
    }
}

#[derive(Debug)]
struct UnusedSamplingProvider;

impl SamplingProvider for UnusedSamplingProvider {
    fn sample(
        &self,
        _request: CreateMessageParams,
        _context: SamplingCallContext,
    ) -> SamplingFuture<'_, CreateMessageResult> {
        Box::pin(async {
            Err(SamplingError::new(
                runifold_mcp::SamplingErrorKind::Execution,
                "synchronous provider must not execute for a durable Sampling Task",
            ))
        })
    }
}

struct TemporarySqlite {
    path: PathBuf,
}

impl TemporarySqlite {
    fn new() -> Self {
        Self {
            path: std::env::temp_dir().join(format!(
                "runifold-mcp-sampling-{}.sqlite",
                uuid::Uuid::now_v7()
            )),
        }
    }
}

impl Drop for TemporarySqlite {
    fn drop(&mut self) {
        for path in [
            self.path.clone(),
            self.path.with_extension("sqlite-wal"),
            self.path.with_extension("sqlite-shm"),
        ] {
            let _ = std::fs::remove_file(path);
        }
    }
}
