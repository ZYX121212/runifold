use std::{collections::BTreeMap, fmt, future::Future, pin::Pin};

use runifold_core::RunContext;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{CallToolResult, InputRequest, JsonRpcError, McpResultType};

/// Official identifier of the MCP Tasks extension.
pub const TASKS_EXTENSION_ID: &str = "io.modelcontextprotocol/tasks";

/// Current durable task state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// Work is queued or executing.
    Working,
    /// Work is durably waiting for client input.
    InputRequired,
    /// The original protocol request completed successfully.
    Completed,
    /// The original protocol request failed with a JSON-RPC error.
    Failed,
    /// Cancellation was durably accepted by the execution backend.
    Cancelled,
}

impl TaskStatus {
    /// Returns whether no further state transition is expected.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Complete externally visible state of one MCP Task.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpTask {
    /// Stable, unguessable server-generated identity.
    pub task_id: String,
    /// Current durable state.
    pub status: TaskStatus,
    /// Optional safe user-facing status detail.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
    /// RFC 3339 last-transition timestamp.
    pub last_updated_at: String,
    /// Retention duration from creation, or unlimited when null.
    pub ttl_ms: Option<u64>,
    /// Server-suggested client polling delay.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub poll_interval_ms: Option<u64>,
    /// Outstanding server-to-client requests while input is required.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub input_requests: BTreeMap<String, InputRequest>,
    /// Exact result shape of the original request after successful completion.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    /// JSON-RPC execution error after protocol-level failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

impl McpTask {
    /// Returns the absolute retention deadline in Unix milliseconds.
    ///
    /// `ttlMs` is a protocol-record retention bound, not a workflow execution
    /// timeout. `None` means that the server advertised no retention limit.
    ///
    /// # Errors
    ///
    /// Returns [`McpTaskTimeError`] when `createdAt` is malformed, predates the
    /// Unix epoch, or cannot be combined with `ttlMs`.
    pub fn retention_expires_at_ms(&self) -> Result<Option<u64>, McpTaskTimeError> {
        self.ttl_ms
            .map(|ttl_ms| {
                timestamp_ms(&self.created_at, "createdAt")?
                    .checked_add(ttl_ms)
                    .ok_or(McpTaskTimeError::RetentionOverflow)
            })
            .transpose()
    }

    /// Returns whether this Task handle has exceeded its advertised retention.
    ///
    /// # Errors
    ///
    /// Returns [`McpTaskTimeError`] for invalid Task time metadata.
    pub fn is_retention_expired_at(&self, now_ms: u64) -> Result<bool, McpTaskTimeError> {
        Ok(self
            .retention_expires_at_ms()?
            .is_some_and(|expires_at_ms| now_ms >= expires_at_ms))
    }

    pub(crate) fn validate(&self) -> Result<(), McpTaskBackendError> {
        if self.task_id.is_empty()
            || self.created_at.is_empty()
            || self.last_updated_at.is_empty()
            || self.poll_interval_ms == Some(0)
        {
            return Err(McpTaskBackendError::invalid_state(
                "task metadata violates the MCP Tasks contract",
            ));
        }
        let created_at_ms = timestamp_ms(&self.created_at, "createdAt")
            .map_err(|error| McpTaskBackendError::invalid_state(error.to_string()))?;
        let last_updated_at_ms = timestamp_ms(&self.last_updated_at, "lastUpdatedAt")
            .map_err(|error| McpTaskBackendError::invalid_state(error.to_string()))?;
        if last_updated_at_ms < created_at_ms {
            return Err(McpTaskBackendError::invalid_state(
                "task lastUpdatedAt predates createdAt",
            ));
        }
        self.retention_expires_at_ms()
            .map_err(|error| McpTaskBackendError::invalid_state(error.to_string()))?;
        let payload_is_valid = match self.status {
            TaskStatus::Working | TaskStatus::Cancelled => {
                self.input_requests.is_empty() && self.result.is_none() && self.error.is_none()
            }
            TaskStatus::InputRequired => {
                !self.input_requests.is_empty() && self.result.is_none() && self.error.is_none()
            }
            TaskStatus::Completed => {
                self.input_requests.is_empty() && self.result.is_some() && self.error.is_none()
            }
            TaskStatus::Failed => {
                self.input_requests.is_empty() && self.result.is_none() && self.error.is_some()
            }
        };
        if !payload_is_valid {
            return Err(McpTaskBackendError::invalid_state(
                "task status payload violates the MCP Tasks contract",
            ));
        }
        for (key, request) in &self.input_requests {
            if key.is_empty() {
                return Err(McpTaskBackendError::invalid_state(
                    "task input request key is empty",
                ));
            }
            request
                .validate()
                .map_err(|_| McpTaskBackendError::invalid_state("task input request is invalid"))?;
        }
        Ok(())
    }
}

/// Invalid MCP Task time or retention metadata.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum McpTaskTimeError {
    /// An RFC 3339 Task timestamp was malformed or predates the Unix epoch.
    #[error("task {field} is not a non-negative RFC 3339 timestamp")]
    InvalidTimestamp {
        /// MCP field carrying the invalid timestamp.
        field: &'static str,
    },
    /// `createdAt + ttlMs` exceeded the Unix millisecond range.
    #[error("task retention deadline overflowed")]
    RetentionOverflow,
}

fn timestamp_ms(value: &str, field: &'static str) -> Result<u64, McpTaskTimeError> {
    let timestamp = OffsetDateTime::parse(value, &Rfc3339)
        .map_err(|_| McpTaskTimeError::InvalidTimestamp { field })?;
    let milliseconds = timestamp.unix_timestamp_nanos() / 1_000_000;
    u64::try_from(milliseconds).map_err(|_| McpTaskTimeError::InvalidTimestamp { field })
}

/// Task handle returned instead of a synchronous Tool result.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTaskResult {
    /// Polymorphic result discriminator.
    pub result_type: McpResultType,
    /// Initial durable task state.
    #[serde(flatten)]
    pub task: McpTask,
}

/// Polymorphic outcome of a Task-capable `tools/call`.
#[derive(Clone, Debug, PartialEq)]
pub enum CallToolOutcome {
    /// The server completed the Tool call synchronously.
    Complete(CallToolResult),
    /// The server durably materialized the call for asynchronous execution.
    Task(McpTask),
}

/// Result of `tasks/get`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetTaskResult {
    /// Ordinary completed-request discriminator.
    pub result_type: McpResultType,
    /// Current durable task state.
    #[serde(flatten)]
    pub task: McpTask,
}

/// Parameters identifying one task.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskIdParams {
    /// Stable task identity.
    pub task_id: String,
}

/// Parameters for `tasks/update`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateTaskParams {
    /// Stable task identity.
    pub task_id: String,
    /// Responses keyed by outstanding `inputRequests`.
    pub input_responses: BTreeMap<String, Value>,
}

/// One Tool request selected for durable task execution.
#[derive(Clone, Debug)]
pub struct ToolTaskRequest {
    /// Canonical Tool name.
    pub name: String,
    /// Validated Tool arguments.
    pub arguments: Map<String, Value>,
    /// Capability-attenuated execution context.
    pub context: RunContext,
}

/// Stable backend failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpTaskBackendErrorKind {
    /// Task identity or client update is invalid.
    InvalidInput,
    /// The task does not exist in this authorization context.
    NotFound,
    /// The backend's stored state violates the adapter contract.
    InvalidState,
    /// Durable storage or execution control failed.
    Storage,
}

/// Safe failure returned by a Task backend.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{kind:?}: {message}")]
pub struct McpTaskBackendError {
    /// Stable failure category.
    pub kind: McpTaskBackendErrorKind,
    /// Safe protocol-facing explanation.
    pub message: String,
}

impl McpTaskBackendError {
    /// Creates a normalized backend failure.
    pub fn new(kind: McpTaskBackendErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub(crate) fn invalid_state(message: impl Into<String>) -> Self {
        Self::new(McpTaskBackendErrorKind::InvalidState, message)
    }
}

/// Future returned by durable Task backend operations.
pub type McpTaskFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, McpTaskBackendError>> + Send + 'a>>;

/// Durable execution boundary behind the MCP Tasks extension.
///
/// Implementations own authorization partitioning, persistence, execution
/// state and exact result reconstruction. The MCP server never maintains a
/// second in-memory task state machine.
pub trait McpTaskBackend: Send + Sync + fmt::Debug {
    /// Returns whether this Tool must be represented as a durable Task.
    fn handles_tool(&self, tool_name: &str) -> bool;

    /// Durably creates and makes a Task inspectable before returning.
    fn create_tool_task(&self, request: ToolTaskRequest) -> McpTaskFuture<'_, McpTask>;

    /// Loads the current complete Task view.
    fn get(&self, task_id: String) -> McpTaskFuture<'_, McpTask>;

    /// Applies responses to currently outstanding input requests.
    fn update(
        &self,
        task_id: String,
        input_responses: BTreeMap<String, Value>,
    ) -> McpTaskFuture<'_, ()>;

    /// Cooperatively requests durable cancellation.
    fn cancel(&self, task_id: String) -> McpTaskFuture<'_, ()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_time_metadata_is_validated_and_retention_is_absolute() {
        let mut task = working_task();
        assert_eq!(task.retention_expires_at_ms().unwrap(), Some(1_000_001));
        assert!(!task.is_retention_expired_at(1_000_000).unwrap());
        assert!(task.is_retention_expired_at(1_000_001).unwrap());

        task.last_updated_at = "1969-12-31T23:59:59Z".into();
        assert!(task.validate().is_err());
        task.last_updated_at = task.created_at.clone();
        task.created_at = "not-a-timestamp".into();
        assert_eq!(
            task.retention_expires_at_ms(),
            Err(McpTaskTimeError::InvalidTimestamp { field: "createdAt" })
        );
    }

    #[test]
    fn task_retention_overflow_is_rejected() {
        let mut task = working_task();
        task.created_at = "9999-12-31T23:59:59Z".into();
        task.last_updated_at = task.created_at.clone();
        task.ttl_ms = Some(u64::MAX);
        assert_eq!(
            task.retention_expires_at_ms(),
            Err(McpTaskTimeError::RetentionOverflow)
        );
        assert!(task.validate().is_err());
    }

    fn working_task() -> McpTask {
        McpTask {
            task_id: "task-1".into(),
            status: TaskStatus::Working,
            status_message: None,
            created_at: "1970-01-01T00:16:40Z".into(),
            last_updated_at: "1970-01-01T00:16:40Z".into(),
            ttl_ms: Some(1),
            poll_interval_ms: Some(100),
            input_requests: BTreeMap::new(),
            result: None,
            error: None,
        }
    }
}
