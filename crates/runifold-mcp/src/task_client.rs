use std::{
    collections::BTreeMap,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::Stream;
use runifold_core::CancellationToken;
use serde_json::Value;

use crate::{
    CacheMode, CallToolOutcome, CallToolParams, CallToolResult, CreateTaskResult, GetTaskResult,
    InputRequiredResult, McpClient, McpError, McpResultType, McpSubscription, McpTask,
    SubscriptionFilter, TaskIdParams, TaskStatus, UpdateTaskParams,
};

/// Typed stream of complete `notifications/tasks` state snapshots.
pub struct McpTaskSubscription {
    inner: McpSubscription,
}

impl McpTaskSubscription {
    fn new(inner: McpSubscription) -> Self {
        Self { inner }
    }

    /// Returns the Task IDs accepted by the server.
    pub fn accepted_task_ids(&self) -> &[String] {
        &self.inner.accepted().task_ids
    }
}

impl Stream for McpTaskSubscription {
    type Item = Result<McpTask, McpError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inner = &mut self.get_mut().inner;
        match Pin::new(inner).poll_next(context) {
            Poll::Ready(Some(Ok(notification))) => {
                if notification.method != "notifications/tasks" {
                    return Poll::Ready(Some(Err(McpError::protocol(
                        "Task subscription received a non-Task notification",
                    ))));
                }
                let task = notification
                    .params
                    .ok_or_else(|| McpError::protocol("Task notification omitted parameters"))
                    .and_then(|params| {
                        serde_json::from_value::<McpTask>(params).map_err(Into::into)
                    })
                    .and_then(|task| {
                        task.validate()
                            .map_err(|error| McpError::protocol(error.to_string()))?;
                        Ok(task)
                    });
                Poll::Ready(Some(task))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl std::fmt::Debug for McpTaskSubscription {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpTaskSubscription")
            .field("accepted_task_ids", &self.accepted_task_ids())
            .finish_non_exhaustive()
    }
}

impl McpClient {
    /// Opens a typed status stream for specific durable Task IDs.
    ///
    /// The server derives notifications from durable current state, sends an
    /// initial snapshot, suppresses unchanged snapshots, and stops observing a
    /// Task after its first terminal snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when Tasks were not negotiated or the modern
    /// subscription cannot be opened.
    pub async fn listen_tasks<I, S>(&self, task_ids: I) -> Result<McpTaskSubscription, McpError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.require_tasks().await?;
        let subscription = self
            .listen(SubscriptionFilter {
                task_ids: task_ids.into_iter().map(Into::into).collect(),
                ..SubscriptionFilter::default()
            })
            .await?;
        Ok(McpTaskSubscription::new(subscription))
    }

    /// Calls a Tool while preserving a server-created durable Task handle.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when Tasks were not negotiated, the request fails,
    /// or the peer returns an invalid polymorphic result.
    pub async fn call_tool_outcome(
        &self,
        params: CallToolParams,
    ) -> Result<CallToolOutcome, McpError> {
        self.require_tasks().await?;
        self.call_tool_outcome_scoped(params, self.request_timeout(), CancellationToken::new())
            .await
    }

    /// Loads one exact durable Task state.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for lifecycle, capability, transport, or task failures.
    pub async fn get_task(&self, task_id: impl Into<String>) -> Result<McpTask, McpError> {
        self.require_tasks().await?;
        self.get_task_scoped(
            task_id.into(),
            self.request_timeout(),
            CancellationToken::new(),
        )
        .await
    }

    /// Applies responses to outstanding Task input requests.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when the Task or response keys are invalid.
    pub async fn update_task(
        &self,
        task_id: impl Into<String>,
        input_responses: BTreeMap<String, Value>,
    ) -> Result<(), McpError> {
        self.require_tasks().await?;
        self.update_task_scoped(
            task_id.into(),
            input_responses,
            self.request_timeout(),
            CancellationToken::new(),
        )
        .await
    }

    /// Cooperatively requests cancellation of one durable Task.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when the Task is unknown or the request fails.
    pub async fn cancel_task(&self, task_id: impl Into<String>) -> Result<(), McpError> {
        self.require_tasks().await?;
        let _: Value = self
            .request_typed_mrtr(
                "tasks/cancel",
                &TaskIdParams {
                    task_id: task_id.into(),
                },
                self.request_timeout(),
                CancellationToken::new(),
                CacheMode::Bypass,
            )
            .await?;
        Ok(())
    }

    /// Polls a Task to a final Tool result while honoring server intervals.
    ///
    /// Task input requests are resolved through the configured
    /// [`crate::MrtrInputHandler`] under the same deadline and cancellation.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for timeout, cancellation, input, transport, or
    /// terminal JSON-RPC execution failures.
    pub async fn wait_task(&self, task: McpTask) -> Result<CallToolResult, McpError> {
        self.require_tasks().await?;
        task.validate()
            .map_err(|error| McpError::protocol(error.to_string()))?;
        let deadline = tokio::time::Instant::now()
            .checked_add(self.request_timeout())
            .ok_or_else(|| McpError::protocol("Task timeout is outside platform limits"))?;
        self.wait_task_scoped(task, deadline, CancellationToken::new())
            .await
    }

    pub(crate) async fn call_tool_outcome_scoped(
        &self,
        params: CallToolParams,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<CallToolOutcome, McpError> {
        let result: Value = self
            .request_typed_mrtr(
                "tools/call",
                &params,
                timeout,
                cancellation,
                CacheMode::Bypass,
            )
            .await?;
        match result.get("resultType").and_then(Value::as_str) {
            Some("task") => {
                let task = serde_json::from_value::<CreateTaskResult>(result)?.task;
                task.validate()
                    .map_err(|error| McpError::protocol(error.to_string()))?;
                Ok(CallToolOutcome::Task(task))
            }
            None | Some("complete") => serde_json::from_value(result)
                .map(CallToolOutcome::Complete)
                .map_err(Into::into),
            Some(other) => Err(McpError::protocol(format!(
                "unsupported tools/call resultType `{other}`"
            ))),
        }
    }

    pub(crate) async fn wait_task_scoped(
        &self,
        mut task: McpTask,
        deadline: tokio::time::Instant,
        cancellation: CancellationToken,
    ) -> Result<CallToolResult, McpError> {
        loop {
            ensure_retained(&task)?;
            match task.status {
                TaskStatus::Completed => {
                    return serde_json::from_value(
                        task.result
                            .take()
                            .ok_or_else(|| McpError::protocol("completed Task omitted result"))?,
                    )
                    .map_err(Into::into);
                }
                TaskStatus::Failed => {
                    let error = task
                        .error
                        .take()
                        .ok_or_else(|| McpError::protocol("failed Task omitted JSON-RPC error"))?;
                    return Err(McpError::Remote {
                        code: error.code,
                        message: error.message,
                        data: error.data,
                    });
                }
                TaskStatus::Cancelled => return Err(McpError::Cancelled),
                TaskStatus::InputRequired => {
                    let incomplete = InputRequiredResult {
                        result_type: McpResultType::InputRequired,
                        input_requests: task.input_requests.clone(),
                        request_state: None,
                    };
                    incomplete.validate(self.max_task_inputs())?;
                    let responses = self
                        .resolve_mrtr_inputs(&incomplete, deadline, &cancellation)
                        .await?;
                    self.update_task_scoped(
                        task.task_id.clone(),
                        responses,
                        remaining(deadline)?,
                        cancellation.clone(),
                    )
                    .await?;
                }
                TaskStatus::Working => {
                    let maximum = self.max_task_poll_interval().max(Duration::from_millis(1));
                    let minimum = self.min_task_poll_interval().min(maximum);
                    let delay = Duration::from_millis(task.poll_interval_ms.unwrap_or(1000))
                        .max(minimum)
                        .min(maximum)
                        .min(retention_remaining(&task)?)
                        .min(remaining(deadline)?);
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        () = cancellation.cancelled() => return Err(McpError::Cancelled),
                    }
                }
            }
            ensure_retained(&task)?;
            task = self
                .get_task_scoped(task.task_id, remaining(deadline)?, cancellation.clone())
                .await?;
        }
    }

    async fn get_task_scoped(
        &self,
        task_id: String,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<McpTask, McpError> {
        let result: GetTaskResult = self
            .request_typed_mrtr(
                "tasks/get",
                &TaskIdParams { task_id },
                timeout,
                cancellation,
                CacheMode::Bypass,
            )
            .await?;
        if result.result_type != McpResultType::Complete {
            return Err(McpError::protocol(
                "tasks/get returned a non-complete result",
            ));
        }
        result
            .task
            .validate()
            .map_err(|error| McpError::protocol(error.to_string()))?;
        Ok(result.task)
    }

    async fn update_task_scoped(
        &self,
        task_id: String,
        input_responses: BTreeMap<String, Value>,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<(), McpError> {
        let _: Value = self
            .request_typed_mrtr(
                "tasks/update",
                &UpdateTaskParams {
                    task_id,
                    input_responses,
                },
                timeout,
                cancellation,
                CacheMode::Bypass,
            )
            .await?;
        Ok(())
    }
}

fn remaining(deadline: tokio::time::Instant) -> Result<Duration, McpError> {
    deadline
        .checked_duration_since(tokio::time::Instant::now())
        .ok_or(McpError::DeadlineExceeded)
}

fn ensure_retained(task: &McpTask) -> Result<(), McpError> {
    if !task.status.is_terminal()
        && task
            .is_retention_expired_at(unix_now_ms())
            .map_err(|error| McpError::protocol(error.to_string()))?
    {
        return Err(McpError::TaskExpired {
            task_id: task.task_id.clone(),
        });
    }
    Ok(())
}

fn retention_remaining(task: &McpTask) -> Result<Duration, McpError> {
    task.retention_expires_at_ms()
        .map_err(|error| McpError::protocol(error.to_string()))
        .map(|expires_at_ms| {
            expires_at_ms.map_or(Duration::MAX, |expires_at_ms| {
                Duration::from_millis(expires_at_ms.saturating_sub(unix_now_ms()))
            })
        })
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
