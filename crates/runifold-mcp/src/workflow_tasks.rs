use std::{collections::BTreeMap, sync::Arc};

use runifold_core::CheckpointId;
use runifold_workflow::{
    WorkflowCancelOutcome, WorkflowCheckpointHistoryLimit, WorkflowCheckpointPhase,
    WorkflowInterruptCommand, WorkflowInterruptDecision, WorkflowOutcome, WorkflowSignalId,
    WorkflowStore, WorkflowStoreError, WorkflowStoreErrorKind, WorkflowTask, WorkflowTaskStatus,
    WorkflowTenantId,
};
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    CallToolResult, ContentBlock, InputRequest, JsonRpcError, McpTask, McpTaskBackend,
    McpTaskBackendError, McpTaskBackendErrorKind, McpTaskFuture, TaskStatus, ToolTaskRequest,
};

const INTERNAL_ERROR: i64 = -32603;

/// Durable workflow selected for one MCP Tool name.
#[derive(Clone, Debug)]
pub struct WorkflowTaskRoute {
    /// Canonical MCP Tool name.
    pub tool_name: String,
    /// Registered durable workflow definition.
    pub workflow: String,
    /// Exact durable workflow definition version.
    pub workflow_version: u32,
    /// Tenant that owns every task created through this route.
    pub tenant_id: WorkflowTenantId,
    /// Queue priority.
    pub priority: i32,
}

impl WorkflowTaskRoute {
    /// Creates a route under an explicit authorization tenant.
    ///
    /// # Errors
    ///
    /// Rejects blank names, oversized names, or version zero.
    pub fn new(
        tool_name: impl Into<String>,
        workflow: impl Into<String>,
        workflow_version: u32,
        tenant_id: WorkflowTenantId,
    ) -> Result<Self, McpTaskBackendError> {
        let route = Self {
            tool_name: tool_name.into(),
            workflow: workflow.into(),
            workflow_version,
            tenant_id,
            priority: 0,
        };
        route.validate()?;
        Ok(route)
    }

    /// Sets the durable queue priority.
    #[must_use]
    pub const fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    fn validate(&self) -> Result<(), McpTaskBackendError> {
        if self.tool_name.trim().is_empty() || self.tool_name.len() > 256 {
            return Err(invalid_input(
                "Task route Tool name must contain 1..=256 bytes",
            ));
        }
        if self.workflow.trim().is_empty()
            || self.workflow.len() > 256
            || self.workflow_version == 0
        {
            return Err(invalid_input(
                "Task route workflow must be named and versioned",
            ));
        }
        Ok(())
    }
}

/// Projects a successful workflow outcome into the exact original Tool result.
pub trait WorkflowTaskResultMapper: Send + Sync + std::fmt::Debug {
    /// Maps one terminal workflow outcome.
    ///
    /// # Errors
    ///
    /// Returns a safe projection failure when the outcome cannot be represented.
    fn map(
        &self,
        tool_name: &str,
        outcome: WorkflowOutcome,
    ) -> Result<CallToolResult, McpTaskBackendError>;
}

/// Result mapper that preserves an encoded `CallToolResult`, otherwise exposes
/// the workflow output as both text and structured content.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultWorkflowTaskResultMapper;

impl WorkflowTaskResultMapper for DefaultWorkflowTaskResultMapper {
    fn map(
        &self,
        _tool_name: &str,
        outcome: WorkflowOutcome,
    ) -> Result<CallToolResult, McpTaskBackendError> {
        if let Ok(result) = serde_json::from_value::<CallToolResult>(outcome.output.clone()) {
            return Ok(result);
        }
        let text = outcome
            .output
            .as_str()
            .map_or_else(|| outcome.output.to_string(), ToOwned::to_owned);
        Ok(CallToolResult {
            content: vec![ContentBlock::text(text)],
            structured_content: Some(outcome.output),
            is_error: false,
        })
    }
}

/// MCP Tasks backend backed directly by Runifold's durable workflow store.
///
/// The workflow store remains the sole execution state machine. This adapter
/// derives every Task view from the current tenant-scoped snapshot and
/// immutable checkpoint history.
#[derive(Clone)]
pub struct WorkflowTaskAdapter {
    store: Arc<dyn WorkflowStore>,
    routes: BTreeMap<String, WorkflowTaskRoute>,
    result_mapper: Arc<dyn WorkflowTaskResultMapper>,
    ttl_ms: Option<u64>,
    poll_interval_ms: u64,
    max_history_pages: usize,
}

impl std::fmt::Debug for WorkflowTaskAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowTaskAdapter")
            .field("routes", &self.routes)
            .field("ttl_ms", &self.ttl_ms)
            .field("poll_interval_ms", &self.poll_interval_ms)
            .field("max_history_pages", &self.max_history_pages)
            .finish_non_exhaustive()
    }
}

impl WorkflowTaskAdapter {
    /// Creates an adapter with one-second polling and 24-hour retention hints.
    pub fn new(store: Arc<dyn WorkflowStore>) -> Self {
        Self {
            store,
            routes: BTreeMap::new(),
            result_mapper: Arc::new(DefaultWorkflowTaskResultMapper),
            ttl_ms: Some(24 * 60 * 60 * 1000),
            poll_interval_ms: 1000,
            max_history_pages: 64,
        }
    }

    /// Registers a Tool-to-workflow route.
    ///
    /// # Errors
    ///
    /// Rejects invalid or duplicate Tool routes.
    pub fn register_route(&mut self, route: WorkflowTaskRoute) -> Result<(), McpTaskBackendError> {
        route.validate()?;
        if self.routes.contains_key(&route.tool_name) {
            return Err(invalid_input("duplicate MCP Task Tool route"));
        }
        if self
            .routes
            .values()
            .any(|existing| existing.tenant_id != route.tenant_id)
        {
            return Err(invalid_input(
                "one WorkflowTaskAdapter may serve only one authorization tenant",
            ));
        }
        if self.routes.values().any(|existing| {
            existing.tenant_id == route.tenant_id
                && existing.workflow == route.workflow
                && existing.workflow_version == route.workflow_version
        }) {
            return Err(invalid_input(
                "Task routes must uniquely identify tenant, workflow, and version",
            ));
        }
        self.routes.insert(route.tool_name.clone(), route);
        Ok(())
    }

    /// Replaces successful result projection.
    #[must_use]
    pub fn with_result_mapper(mut self, mapper: Arc<dyn WorkflowTaskResultMapper>) -> Self {
        self.result_mapper = mapper;
        self
    }

    /// Sets the externally advertised retention duration.
    #[must_use]
    pub const fn with_ttl_ms(mut self, ttl_ms: Option<u64>) -> Self {
        self.ttl_ms = ttl_ms;
        self
    }

    /// Sets the positive client polling hint.
    ///
    /// # Errors
    ///
    /// Rejects a zero interval.
    pub fn with_poll_interval_ms(
        mut self,
        poll_interval_ms: u64,
    ) -> Result<Self, McpTaskBackendError> {
        if poll_interval_ms == 0 {
            return Err(invalid_input("Task poll interval must be positive"));
        }
        self.poll_interval_ms = poll_interval_ms;
        Ok(self)
    }

    /// Bounds immutable history traversal when reconstructing terminal output.
    #[must_use]
    pub const fn with_max_history_pages(mut self, pages: usize) -> Self {
        self.max_history_pages = if pages == 0 { 1 } else { pages };
        self
    }

    async fn task_view(
        &self,
        route: &WorkflowTaskRoute,
        checkpoint_id: CheckpointId,
    ) -> Result<McpTask, McpTaskBackendError> {
        let snapshot = self
            .store
            .inspect(route.tenant_id.clone(), checkpoint_id)
            .await
            .map_err(map_store_error)?;
        let mut task = McpTask {
            task_id: checkpoint_id.to_string(),
            status: map_status(snapshot.status, snapshot.interrupt.is_some()),
            status_message: status_message(&snapshot),
            created_at: format_timestamp(snapshot.created_at_ms)?,
            last_updated_at: format_timestamp(snapshot.updated_at_ms)?,
            ttl_ms: self.ttl_ms,
            poll_interval_ms: Some(self.poll_interval_ms),
            input_requests: BTreeMap::new(),
            result: None,
            error: None,
        };
        if let Some(interrupt) = snapshot.interrupt {
            let key = interrupt.interrupt_id.as_checkpoint_id().to_string();
            task.input_requests.insert(
                key,
                InputRequest::new(
                    "elicitation/create",
                    Some(json!({
                        "mode": "form",
                        "message": interrupt.prompt,
                        "requestedSchema": {
                            "type": "object",
                            "properties": {
                                "decision": {
                                    "type": "string",
                                    "enum": ["approve", "edit", "reject"]
                                },
                                "value": {},
                                "reason": {"type": "string"}
                            },
                            "required": ["decision"]
                        },
                        "_meta": {
                            "io.runifold/proposal": interrupt.proposal
                        }
                    })),
                ),
            );
        }
        match snapshot.status {
            WorkflowTaskStatus::Completed => {
                let outcome = self
                    .latest_outcome(route.tenant_id.clone(), checkpoint_id)
                    .await?;
                let result = self.result_mapper.map(&route.tool_name, outcome)?;
                task.result = Some(serde_json::to_value(result).map_err(|_| {
                    invalid_state("completed workflow Tool result could not be encoded")
                })?);
            }
            WorkflowTaskStatus::Failed => {
                let message = snapshot
                    .failure_message
                    .unwrap_or_else(|| "durable workflow failed".into());
                task.error = Some(JsonRpcError {
                    code: INTERNAL_ERROR,
                    message,
                    data: None,
                });
            }
            _ => {}
        }
        task.validate()?;
        Ok(task)
    }

    async fn latest_outcome(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
    ) -> Result<WorkflowOutcome, McpTaskBackendError> {
        let limit = WorkflowCheckpointHistoryLimit::new(256)
            .map_err(|_| invalid_state("workflow history limit is invalid"))?;
        let mut after = None;
        let mut latest = None;
        for _ in 0..self.max_history_pages {
            let page = self
                .store
                .list_checkpoint_history(tenant_id.clone(), checkpoint_id, after, limit)
                .await
                .map_err(map_store_error)?;
            if page.is_empty() {
                break;
            }
            after = page.last().map(|revision| revision.revision);
            latest = page.into_iter().last();
        }
        let revision = latest.ok_or_else(|| {
            invalid_state("completed workflow has no reconstructable checkpoint history")
        })?;
        match revision.state.phase {
            WorkflowCheckpointPhase::Completed { outcome } => Ok(outcome),
            _ => Err(invalid_state(
                "completed workflow latest checkpoint is not terminal",
            )),
        }
    }

    async fn route_for_task(
        &self,
        task_id: &str,
    ) -> Result<(&WorkflowTaskRoute, CheckpointId), McpTaskBackendError> {
        let uuid = Uuid::parse_str(task_id).map_err(|_| invalid_input("invalid MCP taskId"))?;
        let checkpoint_id = CheckpointId::from_uuid(uuid);
        let tenant_id = self
            .routes
            .values()
            .next()
            .map(|route| route.tenant_id.clone())
            .ok_or_else(|| not_found("Task does not exist"))?;
        let snapshot = self
            .store
            .inspect(tenant_id, checkpoint_id)
            .await
            .map_err(map_store_error)?;
        self.routes
            .values()
            .find(|route| {
                route.workflow == snapshot.workflow
                    && route.workflow_version == snapshot.workflow_version
            })
            .map(|route| (route, checkpoint_id))
            .ok_or_else(|| not_found("Task does not exist"))
    }
}

impl McpTaskBackend for WorkflowTaskAdapter {
    fn handles_tool(&self, tool_name: &str) -> bool {
        self.routes.contains_key(tool_name)
    }

    fn create_tool_task(&self, request: ToolTaskRequest) -> McpTaskFuture<'_, McpTask> {
        Box::pin(async move {
            let route = self
                .routes
                .get(&request.name)
                .ok_or_else(|| not_found("Task route does not exist"))?;
            let task = WorkflowTask::new(
                route.workflow.clone(),
                route.workflow_version,
                Value::Object(request.arguments),
            )
            .map_err(map_store_error)?
            .with_tenant(route.tenant_id.clone())
            .with_priority(route.priority);
            let checkpoint_id = task.checkpoint_id;
            self.store.enqueue(task).await.map_err(map_store_error)?;
            self.task_view(route, checkpoint_id).await
        })
    }

    fn get(&self, task_id: String) -> McpTaskFuture<'_, McpTask> {
        Box::pin(async move {
            let (route, checkpoint_id) = self.route_for_task(&task_id).await?;
            self.task_view(route, checkpoint_id).await
        })
    }

    fn update(
        &self,
        task_id: String,
        input_responses: BTreeMap<String, Value>,
    ) -> McpTaskFuture<'_, ()> {
        Box::pin(async move {
            if input_responses.is_empty() {
                return Err(invalid_input("tasks/update inputResponses is empty"));
            }
            let (route, checkpoint_id) = self.route_for_task(&task_id).await?;
            let snapshot = self
                .store
                .inspect(route.tenant_id.clone(), checkpoint_id)
                .await
                .map_err(map_store_error)?;
            let Some(interrupt) = snapshot.interrupt else {
                return Ok(());
            };
            let key = interrupt.interrupt_id.as_checkpoint_id().to_string();
            let Some(response) = input_responses.get(&key) else {
                return Ok(());
            };
            let decision = decode_decision(response)?;
            let command = WorkflowInterruptCommand::with_id(
                WorkflowSignalId::from_checkpoint_id(interrupt.interrupt_id.as_checkpoint_id()),
                checkpoint_id,
                interrupt.interrupt_id,
                decision,
            )
            .map_err(|error| invalid_input(error.to_string()))?;
            self.store
                .decide_interrupt(route.tenant_id.clone(), command)
                .await
                .map_err(map_store_error)?;
            Ok(())
        })
    }

    fn cancel(&self, task_id: String) -> McpTaskFuture<'_, ()> {
        Box::pin(async move {
            let (route, checkpoint_id) = self.route_for_task(&task_id).await?;
            match self
                .store
                .cancel(route.tenant_id.clone(), checkpoint_id)
                .await
                .map_err(map_store_error)?
            {
                WorkflowCancelOutcome::Cancelled | WorkflowCancelOutcome::AlreadyTerminal => Ok(()),
                _ => Err(invalid_state("unsupported workflow cancellation outcome")),
            }
        })
    }
}

fn map_status(status: WorkflowTaskStatus, interrupted: bool) -> TaskStatus {
    match status {
        WorkflowTaskStatus::Waiting if interrupted => TaskStatus::InputRequired,
        WorkflowTaskStatus::Completed => TaskStatus::Completed,
        WorkflowTaskStatus::Failed => TaskStatus::Failed,
        WorkflowTaskStatus::Cancelled => TaskStatus::Cancelled,
        _ => TaskStatus::Working,
    }
}

fn status_message(snapshot: &runifold_workflow::WorkflowTaskSnapshot) -> Option<String> {
    match snapshot.status {
        WorkflowTaskStatus::Queued => Some("durable workflow queued".into()),
        WorkflowTaskStatus::Leased => Some("durable workflow executing".into()),
        WorkflowTaskStatus::Waiting if snapshot.interrupt.is_some() => {
            Some("durable workflow requires review input".into())
        }
        WorkflowTaskStatus::Waiting => Some("durable workflow is waiting".into()),
        WorkflowTaskStatus::Completed => Some("durable workflow completed".into()),
        WorkflowTaskStatus::Failed => snapshot.failure_message.clone(),
        WorkflowTaskStatus::Cancelled => Some("durable workflow cancelled".into()),
        _ => None,
    }
}

fn decode_decision(response: &Value) -> Result<WorkflowInterruptDecision, McpTaskBackendError> {
    let object = response
        .as_object()
        .ok_or_else(|| invalid_input("Task input response must be an object"))?;
    let action = object
        .get("action")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_input("Task input response omitted action"))?;
    if action == "decline" {
        return WorkflowInterruptDecision::reject("reviewer declined")
            .map_err(|error| invalid_input(error.to_string()));
    }
    if action == "cancel" {
        return WorkflowInterruptDecision::reject("reviewer cancelled")
            .map_err(|error| invalid_input(error.to_string()));
    }
    if action != "accept" {
        return Err(invalid_input("unsupported Task input response action"));
    }
    let content = object
        .get("content")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid_input("accepted Task input response omitted content"))?;
    match content.get("decision").and_then(Value::as_str) {
        Some("approve") => Ok(WorkflowInterruptDecision::Approve),
        Some("edit") => WorkflowInterruptDecision::edit(
            content
                .get("value")
                .cloned()
                .ok_or_else(|| invalid_input("edit decision omitted value"))?,
        )
        .map_err(|error| invalid_input(error.to_string())),
        Some("reject") => WorkflowInterruptDecision::reject(
            content
                .get("reason")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid_input("reject decision omitted reason"))?,
        )
        .map_err(|error| invalid_input(error.to_string())),
        _ => Err(invalid_input("unsupported workflow review decision")),
    }
}

fn format_timestamp(timestamp_ms: u64) -> Result<String, McpTaskBackendError> {
    let nanos = i128::from(timestamp_ms)
        .checked_mul(1_000_000)
        .ok_or_else(|| invalid_state("workflow timestamp overflowed"))?;
    OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .map_err(|_| invalid_state("workflow timestamp is outside RFC 3339 range"))?
        .format(&Rfc3339)
        .map_err(|_| invalid_state("workflow timestamp could not be formatted"))
}

fn map_store_error(error: WorkflowStoreError) -> McpTaskBackendError {
    let kind = match error.kind {
        WorkflowStoreErrorKind::InvalidInput => McpTaskBackendErrorKind::InvalidInput,
        WorkflowStoreErrorKind::NotFound | WorkflowStoreErrorKind::TenantMismatch => {
            McpTaskBackendErrorKind::NotFound
        }
        _ => McpTaskBackendErrorKind::Storage,
    };
    let message = if kind == McpTaskBackendErrorKind::NotFound {
        "Task does not exist".into()
    } else {
        error.message
    };
    McpTaskBackendError::new(kind, message)
}

fn invalid_input(message: impl Into<String>) -> McpTaskBackendError {
    McpTaskBackendError::new(McpTaskBackendErrorKind::InvalidInput, message)
}

fn invalid_state(message: impl Into<String>) -> McpTaskBackendError {
    McpTaskBackendError::new(McpTaskBackendErrorKind::InvalidState, message)
}

fn not_found(message: impl Into<String>) -> McpTaskBackendError {
    McpTaskBackendError::new(McpTaskBackendErrorKind::NotFound, message)
}
