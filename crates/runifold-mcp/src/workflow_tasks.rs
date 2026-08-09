use std::{collections::BTreeMap, sync::Arc};

use runifold_core::CheckpointId;
use runifold_workflow::{
    WorkflowCancelOutcome, WorkflowCheckpointHistoryLimit, WorkflowCheckpointPhase,
    WorkflowInterruptCommand, WorkflowInterruptDecision, WorkflowOutcome, WorkflowSignal,
    WorkflowSignalId, WorkflowSignalName, WorkflowSignalOutcome, WorkflowStore, WorkflowStoreError,
    WorkflowStoreErrorKind, WorkflowTask, WorkflowTaskStatus, WorkflowTenantId,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    CallToolResult, ContentBlock, CreateMessageParams, CreateMessageResult, InputRequest,
    JsonRpcError, McpSamplingTaskBackend, McpTask, McpTaskBackend, McpTaskBackendError,
    McpTaskBackendErrorKind, McpTaskFuture, SAMPLING_TASK_IDEMPOTENCY_KEY,
    SamplingTaskApprovalClaim, SamplingTaskCreation, SamplingTaskOutput, SamplingTaskRequest,
    SamplingTaskTerminalResult, TaskStatus, ToolTaskRequest,
};

const INTERNAL_ERROR: i64 = -32603;
const REQUEST_CANCELLED: i64 = -32800;
const APPROVED_RESULT_SIGNAL_NAME: &str = "runifold.mcp.sampling.approved-result.v1";
const APPROVAL_CLAIM_SIGNAL_NAME: &str = "runifold.mcp.sampling.approval-claim.v1";

/// Durable terminal output contract for a Sampling workflow.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "status", content = "value", rename_all = "snake_case")]
pub enum WorkflowSamplingTaskResult {
    /// Exact successful Sampling response.
    Success(CreateMessageResult),
    /// Exact safe JSON-RPC failure intended for the MCP requestor.
    Error(JsonRpcError),
}

/// Private UUID namespace used to derive stable server-owned Sampling Task IDs.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct SamplingTaskIdempotencyNamespace(Uuid);

impl SamplingTaskIdempotencyNamespace {
    /// Parses a deployment-stable, privately generated UUID namespace.
    ///
    /// # Errors
    ///
    /// Rejects malformed, nil, or non-random UUID values.
    pub fn parse(value: &str) -> Result<Self, McpTaskBackendError> {
        let uuid = Uuid::parse_str(value)
            .map_err(|_| invalid_input("Sampling Task idempotency namespace is invalid"))?;
        if uuid.is_nil() || uuid.get_version_num() != 4 {
            return Err(invalid_input(
                "Sampling Task idempotency namespace must be a private UUIDv4",
            ));
        }
        Ok(Self(uuid))
    }
}

impl std::fmt::Debug for SamplingTaskIdempotencyNamespace {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SamplingTaskIdempotencyNamespace([REDACTED])")
    }
}

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

/// Durable workflow selected for task-augmented Sampling.
#[derive(Clone, Debug)]
pub struct WorkflowSamplingTaskRoute {
    /// Registered durable workflow definition.
    pub workflow: String,
    /// Exact durable workflow definition version.
    pub workflow_version: u32,
    /// Tenant owning every Sampling Task created through this route.
    pub tenant_id: WorkflowTenantId,
    /// Queue priority.
    pub priority: i32,
}

impl WorkflowSamplingTaskRoute {
    /// Creates one Sampling-to-workflow route under an explicit tenant.
    ///
    /// # Errors
    ///
    /// Rejects blank or oversized names and version zero.
    pub fn new(
        workflow: impl Into<String>,
        workflow_version: u32,
        tenant_id: WorkflowTenantId,
    ) -> Result<Self, McpTaskBackendError> {
        let route = Self {
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
        if self.workflow.trim().is_empty()
            || self.workflow.len() > 256
            || self.workflow_version == 0
        {
            return Err(invalid_input(
                "Sampling Task workflow must be named and versioned",
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
            metadata: BTreeMap::new(),
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
    sampling_route: Option<WorkflowSamplingTaskRoute>,
    result_mapper: Arc<dyn WorkflowTaskResultMapper>,
    ttl_ms: Option<u64>,
    poll_interval_ms: u64,
    max_history_pages: usize,
    sampling_idempotency_namespace: Option<SamplingTaskIdempotencyNamespace>,
}

impl std::fmt::Debug for WorkflowTaskAdapter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowTaskAdapter")
            .field("routes", &self.routes)
            .field("sampling_route", &self.sampling_route)
            .field("ttl_ms", &self.ttl_ms)
            .field("poll_interval_ms", &self.poll_interval_ms)
            .field("max_history_pages", &self.max_history_pages)
            .field(
                "sampling_idempotency_namespace",
                &self.sampling_idempotency_namespace,
            )
            .finish_non_exhaustive()
    }
}

impl WorkflowTaskAdapter {
    /// Creates an adapter with one-second polling and 24-hour retention hints.
    pub fn new(store: Arc<dyn WorkflowStore>) -> Self {
        Self {
            store,
            routes: BTreeMap::new(),
            sampling_route: None,
            result_mapper: Arc::new(DefaultWorkflowTaskResultMapper),
            ttl_ms: Some(24 * 60 * 60 * 1000),
            poll_interval_ms: 1000,
            max_history_pages: 64,
            sampling_idempotency_namespace: None,
        }
    }

    /// Configures the private, deployment-stable namespace used for Sampling
    /// Task create idempotency across process restarts.
    #[must_use]
    pub const fn with_sampling_idempotency_namespace(
        mut self,
        namespace: SamplingTaskIdempotencyNamespace,
    ) -> Self {
        self.sampling_idempotency_namespace = Some(namespace);
        self
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
            || self
                .sampling_route
                .as_ref()
                .is_some_and(|existing| existing.tenant_id != route.tenant_id)
        {
            return Err(invalid_input(
                "one WorkflowTaskAdapter may serve only one authorization tenant",
            ));
        }
        if self.routes.values().any(|existing| {
            existing.tenant_id == route.tenant_id
                && existing.workflow == route.workflow
                && existing.workflow_version == route.workflow_version
        }) || self.sampling_route.as_ref().is_some_and(|existing| {
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

    /// Registers the single task-augmented Sampling workflow route.
    ///
    /// # Errors
    ///
    /// Rejects duplicate registration, tenant mixing, and workflow identities
    /// already assigned to a Tool route.
    pub fn register_sampling_route(
        &mut self,
        route: WorkflowSamplingTaskRoute,
    ) -> Result<(), McpTaskBackendError> {
        route.validate()?;
        if self.sampling_route.is_some() {
            return Err(invalid_input("duplicate MCP Sampling Task route"));
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
        self.sampling_route = Some(route);
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

    async fn initial_sampling_request(
        &self,
        route: &WorkflowSamplingTaskRoute,
        checkpoint_id: CheckpointId,
    ) -> Result<CreateMessageParams, McpTaskBackendError> {
        let limit = WorkflowCheckpointHistoryLimit::new(1)
            .map_err(|_| invalid_state("workflow history limit is invalid"))?;
        let revision = self
            .store
            .list_checkpoint_history(route.tenant_id.clone(), checkpoint_id, None, limit)
            .await
            .map_err(map_store_error)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                invalid_state("Sampling workflow has no reconstructable initial checkpoint")
            })?;
        serde_json::from_value(revision.state.value)
            .map_err(|_| invalid_state("Sampling workflow initial request is invalid"))
    }

    async fn sampling_task_view(
        &self,
        route: &WorkflowSamplingTaskRoute,
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
        if snapshot.status == WorkflowTaskStatus::Failed {
            task.error = Some(JsonRpcError {
                code: INTERNAL_ERROR,
                message: snapshot
                    .failure_message
                    .unwrap_or_else(|| "durable Sampling workflow failed".into()),
                data: None,
            });
        }
        task.validate_metadata()?;
        Ok(task)
    }

    async fn sampling_route_for_task(
        &self,
        task_id: &str,
    ) -> Result<(&WorkflowSamplingTaskRoute, CheckpointId), McpTaskBackendError> {
        let route = self
            .sampling_route
            .as_ref()
            .ok_or_else(|| not_found("Task does not exist"))?;
        let uuid = Uuid::parse_str(task_id).map_err(|_| invalid_input("invalid MCP taskId"))?;
        let checkpoint_id = CheckpointId::from_uuid(uuid);
        let snapshot = self
            .store
            .inspect(route.tenant_id.clone(), checkpoint_id)
            .await
            .map_err(map_store_error)?;
        if snapshot.workflow != route.workflow
            || snapshot.workflow_version != route.workflow_version
        {
            return Err(not_found("Task does not exist"));
        }
        Ok((route, checkpoint_id))
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

impl McpSamplingTaskBackend for WorkflowTaskAdapter {
    fn create_message_task(
        &self,
        request: SamplingTaskRequest,
    ) -> McpTaskFuture<'_, SamplingTaskCreation> {
        Box::pin(async move {
            let route = self
                .sampling_route
                .as_ref()
                .ok_or_else(|| not_found("Sampling Task route does not exist"))?;
            let idempotency_id = sampling_idempotency_id(
                &request.params,
                route,
                self.sampling_idempotency_namespace,
            )?;
            let input = serde_json::to_value(&request.params)
                .map_err(|_| invalid_input("Sampling request could not be encoded"))?;
            let mut task = WorkflowTask::new(route.workflow.clone(), route.workflow_version, input)
                .map_err(map_store_error)?
                .with_tenant(route.tenant_id.clone())
                .with_priority(route.priority);
            if let Some(checkpoint_id) = idempotency_id {
                task = task.with_checkpoint_id(checkpoint_id);
            }
            let checkpoint_id = task.checkpoint_id;
            let created = match self.store.enqueue(task).await {
                Ok(()) => true,
                Err(error)
                    if error.kind == WorkflowStoreErrorKind::Conflict
                        && idempotency_id.is_some() =>
                {
                    self.sampling_route_for_task(&checkpoint_id.to_string())
                        .await?;
                    let existing = self
                        .store
                        .load_task_input(route.tenant_id.clone(), checkpoint_id)
                        .await
                        .map_err(map_store_error)
                        .and_then(|value| {
                            serde_json::from_value::<CreateMessageParams>(value).map_err(|_| {
                                invalid_state("Sampling workflow initial request is invalid")
                            })
                        })?;
                    if existing != request.params {
                        return Err(invalid_input(
                            "Sampling Task idempotency key was reused with a different request",
                        ));
                    }
                    false
                }
                Err(error) => return Err(map_store_error(error)),
            };
            Ok(SamplingTaskCreation {
                task: self.sampling_task_view(route, checkpoint_id).await?,
                created,
            })
        })
    }

    fn get(&self, task_id: String) -> McpTaskFuture<'_, McpTask> {
        Box::pin(async move {
            let (route, checkpoint_id) = self.sampling_route_for_task(&task_id).await?;
            self.sampling_task_view(route, checkpoint_id).await
        })
    }

    fn result(&self, task_id: String) -> McpTaskFuture<'_, SamplingTaskTerminalResult> {
        Box::pin(async move {
            let (route, checkpoint_id) = self.sampling_route_for_task(&task_id).await?;
            let snapshot = self
                .store
                .inspect(route.tenant_id.clone(), checkpoint_id)
                .await
                .map_err(map_store_error)?;
            match snapshot.status {
                WorkflowTaskStatus::Failed => {
                    return Ok(SamplingTaskTerminalResult::Error(JsonRpcError {
                        code: INTERNAL_ERROR,
                        message: snapshot
                            .failure_message
                            .unwrap_or_else(|| "durable Sampling workflow failed".into()),
                        data: None,
                    }));
                }
                WorkflowTaskStatus::Cancelled => {
                    return Ok(SamplingTaskTerminalResult::Error(JsonRpcError {
                        code: REQUEST_CANCELLED,
                        message: "Sampling Task was cancelled".into(),
                        data: None,
                    }));
                }
                WorkflowTaskStatus::Completed => {}
                _ => return Err(invalid_state("Sampling Task is not terminal")),
            }
            let request = self.initial_sampling_request(route, checkpoint_id).await?;
            let outcome = self
                .latest_outcome(route.tenant_id.clone(), checkpoint_id)
                .await?;
            if let Ok(terminal) =
                serde_json::from_value::<WorkflowSamplingTaskResult>(outcome.output.clone())
            {
                return match terminal {
                    WorkflowSamplingTaskResult::Success(result) => {
                        Ok(SamplingTaskTerminalResult::Success(Box::new(
                            SamplingTaskOutput { request, result },
                        )))
                    }
                    WorkflowSamplingTaskResult::Error(error) => {
                        Ok(SamplingTaskTerminalResult::Error(error))
                    }
                };
            }
            let result = serde_json::from_value::<CreateMessageResult>(outcome.output)
                .map_err(|_| invalid_state("Sampling workflow result is invalid"))?;
            Ok(SamplingTaskTerminalResult::Success(Box::new(
                SamplingTaskOutput { request, result },
            )))
        })
    }

    fn approved_result(&self, task_id: String) -> McpTaskFuture<'_, Option<CreateMessageResult>> {
        Box::pin(async move {
            let (route, checkpoint_id) = self.sampling_route_for_task(&task_id).await?;
            let signal_id = approved_result_signal_id(checkpoint_id);
            match self
                .store
                .load_signal_payload(route.tenant_id.clone(), signal_id)
                .await
            {
                Ok(value) => serde_json::from_value(value)
                    .map(Some)
                    .map_err(|_| invalid_state("approved Sampling result is invalid")),
                Err(error) if error.kind == WorkflowStoreErrorKind::NotFound => Ok(None),
                Err(error) => Err(map_store_error(error)),
            }
        })
    }

    fn claim_result_approval(
        &self,
        task_id: String,
        lease_ms: u64,
    ) -> McpTaskFuture<'_, SamplingTaskApprovalClaim> {
        Box::pin(async move {
            if lease_ms == 0 {
                return Err(invalid_input("Sampling approval lease must be positive"));
            }
            if let Some(result) = self.approved_result(task_id.clone()).await? {
                return Ok(SamplingTaskApprovalClaim::Completed(result));
            }
            let (route, checkpoint_id) = self.sampling_route_for_task(&task_id).await?;
            let task = self
                .store
                .inspect(route.tenant_id.clone(), checkpoint_id)
                .await
                .map_err(map_store_error)?;
            if task.status != WorkflowTaskStatus::Completed {
                return Err(invalid_state(
                    "Sampling result approval requires a completed Task",
                ));
            }
            let now = self
                .store
                .current_time_ms()
                .await
                .map_err(map_store_error)?;
            let generation = now / lease_ms;
            if generation > 0 {
                let previous_signal_id = approval_claim_signal_id(checkpoint_id, generation - 1);
                match self
                    .store
                    .inspect_signal(route.tenant_id.clone(), previous_signal_id)
                    .await
                {
                    Ok(previous) => {
                        let previous_expires_at_ms =
                            previous.accepted_at_ms.saturating_add(lease_ms);
                        if previous_expires_at_ms > now {
                            return Ok(SamplingTaskApprovalClaim::Busy {
                                retry_after_ms: previous_expires_at_ms - now,
                            });
                        }
                    }
                    Err(error) if error.kind == WorkflowStoreErrorKind::NotFound => {}
                    Err(error) => return Err(map_store_error(error)),
                }
            }
            let signal_id = approval_claim_signal_id(checkpoint_id, generation);
            let name = WorkflowSignalName::parse(APPROVAL_CLAIM_SIGNAL_NAME)
                .map_err(|_| invalid_state("approval-claim signal name is invalid"))?;
            let signal = WorkflowSignal::with_id(
                signal_id,
                checkpoint_id,
                name,
                json!({"generation": generation}),
            )
            .map_err(|_| invalid_state("Sampling approval claim could not be encoded"))?;
            let outcome = self
                .store
                .publish_control_signal(route.tenant_id.clone(), signal)
                .await
                .map_err(map_store_error)?;
            let claim = self
                .store
                .inspect_signal(route.tenant_id.clone(), signal_id)
                .await
                .map_err(map_store_error)?;
            let expires_at_ms = claim.accepted_at_ms.saturating_add(lease_ms);
            if outcome == WorkflowSignalOutcome::Duplicate {
                if let Some(result) = self.approved_result(task_id).await? {
                    return Ok(SamplingTaskApprovalClaim::Completed(result));
                }
                return Ok(SamplingTaskApprovalClaim::Busy {
                    retry_after_ms: expires_at_ms.saturating_sub(now).max(1),
                });
            }
            Ok(SamplingTaskApprovalClaim::Acquired {
                token: expires_at_ms.to_string(),
            })
        })
    }

    fn store_approved_result(
        &self,
        task_id: String,
        result: CreateMessageResult,
    ) -> McpTaskFuture<'_, CreateMessageResult> {
        Box::pin(async move {
            let (route, checkpoint_id) = self.sampling_route_for_task(&task_id).await?;
            let signal_id = approved_result_signal_id(checkpoint_id);
            let name = WorkflowSignalName::parse(APPROVED_RESULT_SIGNAL_NAME)
                .map_err(|_| invalid_state("approved-result signal name is invalid"))?;
            let payload = serde_json::to_value(&result)
                .map_err(|_| invalid_state("approved Sampling result could not be encoded"))?;
            let signal = WorkflowSignal::with_id(signal_id, checkpoint_id, name, payload)
                .map_err(|_| invalid_state("approved Sampling result exceeds durable limits"))?;
            self.store
                .publish_control_signal(route.tenant_id.clone(), signal)
                .await
                .map_err(map_store_error)?;
            let stored = self
                .store
                .load_signal_payload(route.tenant_id.clone(), signal_id)
                .await
                .map_err(map_store_error)?;
            serde_json::from_value(stored)
                .map_err(|_| invalid_state("approved Sampling result is invalid"))
        })
    }

    fn complete_result_approval(
        &self,
        task_id: String,
        token: String,
        result: CreateMessageResult,
    ) -> McpTaskFuture<'_, CreateMessageResult> {
        Box::pin(async move {
            let expires_at_ms = token
                .parse::<u64>()
                .map_err(|_| invalid_input("Sampling approval fencing token is invalid"))?;
            let now = self
                .store
                .current_time_ms()
                .await
                .map_err(map_store_error)?;
            if now >= expires_at_ms {
                return Err(McpTaskBackendError::new(
                    McpTaskBackendErrorKind::AdmissionDenied,
                    "Sampling approval lease expired and was fenced",
                ));
            }
            self.store_approved_result(task_id, result).await
        })
    }

    fn cancel(&self, task_id: String) -> McpTaskFuture<'_, ()> {
        Box::pin(async move {
            let (route, checkpoint_id) = self.sampling_route_for_task(&task_id).await?;
            let snapshot = self
                .store
                .inspect(route.tenant_id.clone(), checkpoint_id)
                .await
                .map_err(map_store_error)?;
            if matches!(
                snapshot.status,
                WorkflowTaskStatus::Completed
                    | WorkflowTaskStatus::Failed
                    | WorkflowTaskStatus::Cancelled
            ) {
                return Err(invalid_input("terminal Sampling Task cannot be cancelled"));
            }
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

fn sampling_idempotency_id(
    params: &CreateMessageParams,
    route: &WorkflowSamplingTaskRoute,
    namespace: Option<SamplingTaskIdempotencyNamespace>,
) -> Result<Option<CheckpointId>, McpTaskBackendError> {
    let Some(value) = params.meta.get(SAMPLING_TASK_IDEMPOTENCY_KEY) else {
        return Ok(None);
    };
    let value = value
        .as_str()
        .ok_or_else(|| invalid_input("Sampling Task idempotency key must be a UUID string"))?;
    let uuid = Uuid::parse_str(value)
        .map_err(|_| invalid_input("Sampling Task idempotency key must be a valid UUID"))?;
    if !matches!(uuid.get_version_num(), 4 | 7) {
        return Err(invalid_input(
            "Sampling Task idempotency key must be UUIDv4 or UUIDv7",
        ));
    }
    let namespace = namespace.ok_or_else(|| {
        invalid_input("Sampling Task idempotency requires a configured private namespace")
    })?;
    let scope = serde_json::to_vec(&(
        route.tenant_id.as_str(),
        route.workflow.as_str(),
        route.workflow_version,
        uuid,
    ))
    .map_err(|_| invalid_state("Sampling Task idempotency scope could not be encoded"))?;
    Ok(Some(CheckpointId::from_uuid(Uuid::new_v5(
        &namespace.0,
        &scope,
    ))))
}

fn approved_result_signal_id(checkpoint_id: CheckpointId) -> WorkflowSignalId {
    WorkflowSignalId::from_checkpoint_id(CheckpointId::from_uuid(Uuid::new_v5(
        &checkpoint_id.as_uuid(),
        APPROVED_RESULT_SIGNAL_NAME.as_bytes(),
    )))
}

fn approval_claim_signal_id(checkpoint_id: CheckpointId, generation: u64) -> WorkflowSignalId {
    WorkflowSignalId::from_checkpoint_id(CheckpointId::from_uuid(Uuid::new_v5(
        &checkpoint_id.as_uuid(),
        format!("{APPROVAL_CLAIM_SIGNAL_NAME}:{generation}").as_bytes(),
    )))
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
        WorkflowStoreErrorKind::AdmissionDenied | WorkflowStoreErrorKind::Conflict => {
            McpTaskBackendErrorKind::AdmissionDenied
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
