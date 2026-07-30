//! MCP Tasks extension conformance tests.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::StreamExt;
use runifold_core::{Budget, BudgetTracker, CapabilitySet, RunContext};
use runifold_mcp::{
    CallToolOutcome, CallToolParams, CallToolResult, ContentBlock, Implementation, McpClient,
    McpClientConfig, McpError, McpServer, McpTask, McpTaskBackend, McpTaskBackendError,
    McpTaskBackendErrorKind, McpTaskFuture, SubscriptionFilter, TaskStatus, ToolTaskRequest,
};
use runifold_tool::{FunctionTool, Tool, ToolRegistry};
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

#[tokio::test]
async fn task_capability_is_per_request_and_task_lifecycle_is_explicit() {
    let backend = Arc::new(FakeTaskBackend::default());
    let server = task_server(&backend);

    let unsupported = McpClient::new(
        Arc::new(server.session()),
        McpClientConfig::new(Implementation::new("plain-client", "1")),
    );
    unsupported.connect().await.unwrap();
    let error = unsupported
        .listen(SubscriptionFilter {
            task_ids: vec!["opaque-task".into()],
            ..SubscriptionFilter::default()
        })
        .await
        .unwrap_err();
    assert!(
        matches!(error, McpError::Remote { code: -32003, .. }),
        "{error:?}"
    );
    let error = unsupported
        .call_tool(CallToolParams {
            name: "slow".into(),
            arguments: None,
        })
        .await
        .unwrap_err();
    assert!(
        matches!(error, McpError::Remote { code: -32003, .. }),
        "{error:?}"
    );

    let client = task_client(
        &server,
        McpClientConfig::new(Implementation::new("task-client", "1")),
    )
    .await;
    let task = create_task(&client, Some(json!(7))).await;
    assert_eq!(task.status, TaskStatus::Working);
    assert_eq!(client.get_task(&task.task_id).await.unwrap(), task);
    let completed = complete_through_notifications(&client, &task).await;
    let result = client.wait_task(completed).await.unwrap();
    assert_eq!(result.structured_content, Some(json!({"value": 7})));

    let cancellable = create_task(&client, None).await;
    client.cancel_task(&cancellable.task_id).await.unwrap();
    assert_eq!(
        client.get_task(&cancellable.task_id).await.unwrap().status,
        TaskStatus::Cancelled
    );
}

#[tokio::test]
async fn task_retention_and_polling_floor_are_client_governed() {
    let backend = Arc::new(FakeTaskBackend::default());
    let server = task_server(&backend);
    let client = task_client(
        &server,
        McpClientConfig::new(Implementation::new("governed-client", "1"))
            .with_min_task_poll_interval(Duration::from_millis(30))
            .with_max_task_poll_interval(Duration::from_millis(30)),
    )
    .await;

    let mut expired = working_task("expired".into(), &Value::Null);
    expired.created_at = "1970-01-01T00:00:00Z".into();
    expired.last_updated_at = expired.created_at.clone();
    expired.ttl_ms = Some(1);
    assert!(matches!(
        client.wait_task(expired.clone()).await,
        Err(McpError::TaskExpired { task_id }) if task_id == "expired"
    ));

    expired.status = TaskStatus::Completed;
    expired.result = Some(tool_result(Value::Null));
    client
        .wait_task(expired)
        .await
        .expect("an already observed terminal result remains usable");

    let task = create_task(&client, None).await;
    let waiter = {
        let client = client.clone();
        let task = task.clone();
        tokio::spawn(async move { client.wait_task(task).await })
    };
    tokio::time::sleep(Duration::from_millis(65)).await;
    client
        .update_task(
            &task.task_id,
            BTreeMap::from([("complete".into(), json!({"action": "accept"}))]),
        )
        .await
        .unwrap();
    waiter.await.unwrap().unwrap();
    assert!(
        backend.get_calls.load(Ordering::Relaxed) <= 4,
        "one-millisecond server hints must not bypass the client polling floor"
    );
}

fn task_server(backend: &Arc<FakeTaskBackend>) -> McpServer {
    let tool = Arc::new(FunctionTool::<Value, Value, _>::new(
        "slow",
        "durable work",
        |input, _| async move { Ok(input) },
    ));
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(tool.descriptor().capability());
    let mut registry = ToolRegistry::new();
    registry.register(tool).unwrap();
    McpServer::new(
        Arc::new(registry),
        RunContext::root(BudgetTracker::new(Budget::default()), capabilities),
        Implementation::new("task-server", "1"),
    )
    .with_task_backend(backend.clone())
    .with_task_notification_interval(Duration::from_millis(5))
}

async fn task_client(server: &McpServer, config: McpClientConfig) -> McpClient {
    let client = McpClient::new(Arc::new(server.session()), config.with_tasks());
    client.connect().await.unwrap();
    client
}

async fn create_task(client: &McpClient, value: Option<Value>) -> McpTask {
    match client
        .call_tool_outcome(CallToolParams {
            name: "slow".into(),
            arguments: value.map(|value| serde_json::Map::from_iter([("value".into(), value)])),
        })
        .await
        .unwrap()
    {
        CallToolOutcome::Task(task) => task,
        CallToolOutcome::Complete(_) => panic!("routed Tool must become a Task"),
    }
}

async fn complete_through_notifications(client: &McpClient, task: &McpTask) -> McpTask {
    let mut notifications = client
        .listen_tasks([task.task_id.clone(), task.task_id.clone()])
        .await
        .unwrap();
    assert_eq!(
        notifications.accepted_task_ids(),
        std::slice::from_ref(&task.task_id)
    );
    let initial = tokio::time::timeout(Duration::from_millis(100), notifications.next())
        .await
        .expect("initial Task snapshot timed out")
        .expect("Task subscription closed")
        .unwrap();
    assert_eq!(&initial, task);

    client
        .update_task(
            &task.task_id,
            BTreeMap::from([("complete".into(), json!({"action": "accept"}))]),
        )
        .await
        .unwrap();
    let completed = client.get_task(&task.task_id).await.unwrap();
    assert_eq!(completed.status, TaskStatus::Completed);
    let notification = tokio::time::timeout(Duration::from_millis(100), notifications.next())
        .await
        .expect("terminal Task snapshot timed out")
        .expect("Task subscription closed")
        .unwrap();
    assert_eq!(notification, completed);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), notifications.next())
            .await
            .is_err(),
        "terminal Task must not be emitted more than once"
    );
    let mut resumed_notifications = client.listen_tasks([task.task_id.clone()]).await.unwrap();
    let recovered = tokio::time::timeout(Duration::from_millis(100), resumed_notifications.next())
        .await
        .expect("reconnected Task snapshot timed out")
        .expect("reconnected Task subscription closed")
        .unwrap();
    assert_eq!(recovered, completed);
    completed
}

#[derive(Debug, Default)]
struct FakeTaskBackend {
    tasks: Mutex<HashMap<String, McpTask>>,
    get_calls: AtomicUsize,
}

impl McpTaskBackend for FakeTaskBackend {
    fn handles_tool(&self, tool_name: &str) -> bool {
        tool_name == "slow"
    }

    fn create_tool_task(&self, request: ToolTaskRequest) -> McpTaskFuture<'_, McpTask> {
        Box::pin(async move {
            let task_id = format!("task-{}", self.tasks().len() + 1);
            let arguments = Value::Object(request.arguments);
            let task = working_task(task_id.clone(), &arguments);
            self.tasks().insert(task_id, task.clone());
            Ok(task)
        })
    }

    fn get(&self, task_id: String) -> McpTaskFuture<'_, McpTask> {
        Box::pin(async move {
            self.get_calls.fetch_add(1, Ordering::Relaxed);
            self.tasks()
                .get(&task_id)
                .cloned()
                .ok_or_else(task_not_found)
        })
    }

    fn update(
        &self,
        task_id: String,
        _input_responses: BTreeMap<String, Value>,
    ) -> McpTaskFuture<'_, ()> {
        Box::pin(async move {
            let mut tasks = self.tasks();
            let task = tasks.get_mut(&task_id).ok_or_else(task_not_found)?;
            let arguments = task
                .status_message
                .take()
                .and_then(|value| serde_json::from_str(&value).ok())
                .unwrap_or(Value::Null);
            task.status = TaskStatus::Completed;
            task.last_updated_at = now_timestamp();
            task.result = Some(tool_result(arguments));
            Ok(())
        })
    }

    fn cancel(&self, task_id: String) -> McpTaskFuture<'_, ()> {
        Box::pin(async move {
            let mut tasks = self.tasks();
            let task = tasks.get_mut(&task_id).ok_or_else(task_not_found)?;
            task.status = TaskStatus::Cancelled;
            task.status_message = None;
            task.last_updated_at = now_timestamp();
            Ok(())
        })
    }
}

impl FakeTaskBackend {
    fn tasks(&self) -> std::sync::MutexGuard<'_, HashMap<String, McpTask>> {
        self.tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn working_task(task_id: String, arguments: &Value) -> McpTask {
    let now = now_timestamp();
    McpTask {
        task_id,
        status: TaskStatus::Working,
        status_message: Some(arguments.to_string()),
        created_at: now.clone(),
        last_updated_at: now,
        ttl_ms: Some(60_000),
        poll_interval_ms: Some(10),
        input_requests: BTreeMap::new(),
        result: None,
        error: None,
    }
}

fn tool_result(arguments: Value) -> Value {
    serde_json::to_value(CallToolResult {
        content: vec![ContentBlock::text("done")],
        structured_content: Some(arguments),
        is_error: false,
    })
    .expect("Tool result is serializable")
}

fn now_timestamp() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .expect("current UTC time is RFC 3339 representable")
}

fn task_not_found() -> McpTaskBackendError {
    McpTaskBackendError::new(McpTaskBackendErrorKind::NotFound, "Task does not exist")
}
