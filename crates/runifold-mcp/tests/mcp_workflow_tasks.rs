//! Durable workflow adapter tests for MCP Tasks.

#![cfg(feature = "workflow-tasks")]

use std::{sync::Arc, time::Duration};

use runifold_core::{Budget, BudgetTracker, CapabilitySet, RunContext};
use runifold_mcp::{
    CallToolOutcome, CallToolParams, Implementation, McpClient, McpClientConfig, McpServer,
    McpTaskBackend, TaskStatus, ToolTaskRequest, WorkflowTaskAdapter, WorkflowTaskRoute,
};
use runifold_tool::{FunctionTool, Tool, ToolRegistry};
use runifold_workflow::{
    InMemoryWorkflowStore, LeaseDuration, WorkerId, Workflow, WorkflowDefinition,
    WorkflowDisposition, WorkflowInterruptRequest, WorkflowRegistry, WorkflowStep,
    WorkflowStepFuture, WorkflowStore, WorkflowTaskStatus, WorkflowTenantId, WorkflowWait,
    WorkflowWorker, WorkflowWorkerOutcome,
};
use serde_json::{Value, json};

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
