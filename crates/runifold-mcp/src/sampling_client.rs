use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use runifold_core::{CancellationToken, DomainEvent, RunContext, RunEventKind};
use serde::Serialize;
use serde_json::{Value, json};

use crate::{
    CoreTaskWire, CreateMessageOutcome, CreateMessageParams, CreateMessageResult,
    JsonRpcNotification, JsonRpcRequest, McpError, McpSession, McpTask, RequestId,
    SamplingTaskResult, TaskIdParams, TaskStatus,
};

/// Server-side handle for requesting host-controlled client Sampling.
#[derive(Clone, Debug)]
pub struct McpSamplingClient {
    session: McpSession,
    next_id: Arc<AtomicU64>,
    timeout: Duration,
}

impl McpSamplingClient {
    pub(crate) fn new(session: McpSession) -> Self {
        Self {
            session,
            next_id: Arc::new(AtomicU64::new(1)),
            timeout: Duration::from_secs(60),
        }
    }

    /// Replaces the maximum server wait for approval and model execution.
    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Requests one client-controlled model generation.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for lifecycle, capability, rejection, timeout,
    /// transport, or malformed-response failures.
    pub async fn create_message(
        &self,
        params: CreateMessageParams,
    ) -> Result<CreateMessageResult, McpError> {
        reject_task_params_for_synchronous_api(&params, "create_message_outcome")?;
        match self
            .request_outcome(params, CancellationToken::new(), self.timeout, None)
            .await?
        {
            CreateMessageOutcome::Complete(result) => Ok(result),
            CreateMessageOutcome::Task(_) => Err(McpError::protocol(
                "task-augmented Sampling requires create_message_outcome",
            )),
        }
    }

    /// Requests Sampling and preserves either a synchronous result or durable Task handle.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for lifecycle, capability, transport, policy, or
    /// malformed-response failures.
    pub async fn create_message_outcome(
        &self,
        params: CreateMessageParams,
    ) -> Result<CreateMessageOutcome, McpError> {
        self.request_outcome(params, CancellationToken::new(), self.timeout, None)
            .await
    }

    /// Requests Sampling scoped to a Runifold run's cancellation and deadline.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for lifecycle, capability, rejection, cancellation,
    /// timeout, transport, or malformed-response failures.
    pub async fn create_message_scoped(
        &self,
        params: CreateMessageParams,
        context: &RunContext,
    ) -> Result<CreateMessageResult, McpError> {
        reject_task_params_for_synchronous_api(&params, "create_message_outcome_scoped")?;
        match self.create_message_outcome_scoped(params, context).await? {
            CreateMessageOutcome::Complete(result) => Ok(result),
            CreateMessageOutcome::Task(_) => Err(McpError::protocol(
                "task-augmented Sampling requires create_message_outcome_scoped",
            )),
        }
    }

    /// Requests task-capable Sampling under a Runifold cancellation and deadline scope.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for lifecycle, capability, rejection, cancellation,
    /// timeout, transport, or malformed-response failures.
    pub async fn create_message_outcome_scoped(
        &self,
        params: CreateMessageParams,
        context: &RunContext,
    ) -> Result<CreateMessageOutcome, McpError> {
        let timeout = context.deadline().map_or(self.timeout, |deadline| {
            deadline
                .saturating_duration_since(std::time::Instant::now())
                .min(self.timeout)
        });
        self.request_outcome(
            params,
            context.cancellation().clone(),
            timeout,
            Some(context),
        )
        .await
    }

    /// Loads one task-augmented Sampling state from the client receiver.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for transport, task, or malformed-state failures.
    pub async fn get_task(&self, task_id: impl Into<String>) -> Result<McpTask, McpError> {
        let result: CoreTaskWire = self
            .request_peer_typed(
                "tasks/get",
                &TaskIdParams {
                    task_id: task_id.into(),
                },
                CancellationToken::new(),
                self.timeout,
            )
            .await?;
        let task: McpTask = result.into();
        task.validate_metadata()
            .map_err(|error| McpError::protocol(error.to_string()))?;
        Ok(task)
    }

    /// Fetches the exact completed Sampling result for a durable Task.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] until the backend can reconstruct a successful result.
    pub async fn task_result(
        &self,
        task_id: impl Into<String>,
    ) -> Result<CreateMessageResult, McpError> {
        self.request_peer_typed(
            "tasks/result",
            &TaskIdParams {
                task_id: task_id.into(),
            },
            CancellationToken::new(),
            self.timeout,
        )
        .await
    }

    /// Cooperatively requests cancellation and returns the resulting task state.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when cancellation is rejected or state is malformed.
    pub async fn cancel_task(&self, task_id: impl Into<String>) -> Result<McpTask, McpError> {
        self.session.ensure_sampling_task_cancel_supported()?;
        let result: CoreTaskWire = self
            .request_peer_typed(
                "tasks/cancel",
                &TaskIdParams {
                    task_id: task_id.into(),
                },
                CancellationToken::new(),
                self.timeout,
            )
            .await?;
        let task: McpTask = result.into();
        task.validate_metadata()
            .map_err(|error| McpError::protocol(error.to_string()))?;
        Ok(task)
    }

    /// Polls a Sampling Task and retrieves its exact terminal result.
    ///
    /// A timeout or local cancellation does not cancel durable remote work.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for retention expiry, timeout, cancellation,
    /// terminal failure, or malformed state.
    pub async fn wait_task(&self, task: McpTask) -> Result<CreateMessageResult, McpError> {
        self.wait_task_with_cancellation(task, CancellationToken::new(), self.timeout)
            .await
    }

    /// Polls a Sampling Task under a Runifold cancellation and deadline scope.
    ///
    /// Local cancellation stops waiting but does not cancel durable remote work.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for retention expiry, timeout, cancellation,
    /// terminal failure, or malformed state.
    pub async fn wait_task_scoped(
        &self,
        task: McpTask,
        context: &RunContext,
    ) -> Result<CreateMessageResult, McpError> {
        let timeout = context.deadline().map_or(self.timeout, |deadline| {
            deadline
                .saturating_duration_since(std::time::Instant::now())
                .min(self.timeout)
        });
        self.wait_task_with_cancellation(task, context.cancellation().clone(), timeout)
            .await
    }

    async fn wait_task_with_cancellation(
        &self,
        mut task: McpTask,
        cancellation: CancellationToken,
        timeout: Duration,
    ) -> Result<CreateMessageResult, McpError> {
        task.validate_metadata()
            .map_err(|error| McpError::protocol(error.to_string()))?;
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| McpError::protocol("Task timeout is outside platform limits"))?;
        loop {
            ensure_sampling_task_retained(&task)?;
            match task.status {
                TaskStatus::Completed
                | TaskStatus::Failed
                | TaskStatus::Cancelled
                | TaskStatus::InputRequired => {
                    return self
                        .request_peer_typed(
                            "tasks/result",
                            &TaskIdParams {
                                task_id: task.task_id,
                            },
                            cancellation,
                            remaining_sampling_task_time(deadline)?,
                        )
                        .await;
                }
                TaskStatus::Working => {
                    let remaining = remaining_sampling_task_time(deadline)?;
                    let delay = Duration::from_millis(task.poll_interval_ms.unwrap_or(1000))
                        .clamp(Duration::from_millis(100), Duration::from_secs(30))
                        .min(remaining)
                        .min(sampling_retention_remaining(&task)?);
                    tokio::select! {
                        () = tokio::time::sleep(delay) => {}
                        () = cancellation.cancelled() => return Err(McpError::Cancelled),
                    }
                    let result: CoreTaskWire = self
                        .request_peer_typed(
                            "tasks/get",
                            &TaskIdParams {
                                task_id: task.task_id,
                            },
                            cancellation.clone(),
                            remaining_sampling_task_time(deadline)?,
                        )
                        .await?;
                    let recovered: McpTask = result.into();
                    recovered
                        .validate_metadata()
                        .map_err(|error| McpError::protocol(error.to_string()))?;
                    task = recovered;
                }
            }
        }
    }

    async fn request_outcome(
        &self,
        params: CreateMessageParams,
        cancellation: CancellationToken,
        timeout: Duration,
        run: Option<&RunContext>,
    ) -> Result<CreateMessageOutcome, McpError> {
        self.session.await_active(timeout).await?;
        self.session.ensure_sampling_supported(&params)?;
        let message_count = params.messages.len();
        let max_tokens = params.max_tokens;
        let id = RequestId::String(format!(
            "runifold-server-{}",
            self.next_id.fetch_add(1, Ordering::Relaxed)
        ));
        if let Some(run) = run {
            record_sampling_started(run, &id, message_count, max_tokens)?;
        }
        let request = JsonRpcRequest::new(
            id.clone(),
            "sampling/createMessage",
            Some(serde_json::to_value(params)?),
        );
        let result = tokio::select! {
            response = self.session.request_peer(request) => {
                match response {
                    Err(error) => Err(error),
                    Ok(response) if response.id() != &id => Err(McpError::protocol(
                            "Sampling response id does not match request id",
                        )),
                    Ok(response) => response.into_result().and_then(decode_sampling_outcome),
                }
            }
            () = cancellation.cancelled() => {
                self.cancel(&id, "Sampling request cancelled").await;
                Err(McpError::Cancelled)
            }
            () = tokio::time::sleep(timeout) => {
                self.cancel(&id, "Sampling request deadline exceeded").await;
                Err(McpError::DeadlineExceeded)
            }
        };
        if let Some(run) = run {
            record_sampling_terminal(run, &id, &result)?;
        }
        result
    }

    async fn request_peer_typed<P, R>(
        &self,
        method: &str,
        params: &P,
        cancellation: CancellationToken,
        timeout: Duration,
    ) -> Result<R, McpError>
    where
        P: Serialize + ?Sized,
        R: serde::de::DeserializeOwned,
    {
        let id = RequestId::String(format!(
            "runifold-server-{}",
            self.next_id.fetch_add(1, Ordering::Relaxed)
        ));
        let request = JsonRpcRequest::new(id.clone(), method, Some(serde_json::to_value(params)?));
        tokio::select! {
            response = self.session.request_peer(request) => {
                let response = response?;
                if response.id() != &id {
                    return Err(McpError::protocol("peer response id does not match request id"));
                }
                serde_json::from_value(response.into_result()?).map_err(Into::into)
            }
            () = cancellation.cancelled() => {
                self.cancel(&id, "peer request cancelled").await;
                Err(McpError::Cancelled)
            }
            () = tokio::time::sleep(timeout) => {
                self.cancel(&id, "peer request deadline exceeded").await;
                Err(McpError::DeadlineExceeded)
            }
        }
    }

    async fn cancel(&self, id: &RequestId, reason: &str) {
        let _ = self
            .session
            .notify_peer(JsonRpcNotification::new(
                "notifications/cancelled",
                Some(json!({"requestId": id, "reason": reason})),
            ))
            .await;
    }
}

fn decode_sampling_outcome(value: Value) -> Result<CreateMessageOutcome, McpError> {
    if value.get("task").is_some() {
        let task = serde_json::from_value::<SamplingTaskResult>(value)?.task;
        task.validate_metadata()
            .map_err(|error| McpError::protocol(error.to_string()))?;
        return Ok(CreateMessageOutcome::Task(task));
    }
    serde_json::from_value(value)
        .map(CreateMessageOutcome::Complete)
        .map_err(Into::into)
}

fn reject_task_params_for_synchronous_api(
    params: &CreateMessageParams,
    outcome_api: &str,
) -> Result<(), McpError> {
    if params.task.is_some() {
        return Err(McpError::protocol(format!(
            "task-augmented Sampling requires {outcome_api}"
        )));
    }
    Ok(())
}

fn ensure_sampling_task_retained(task: &McpTask) -> Result<(), McpError> {
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

fn sampling_retention_remaining(task: &McpTask) -> Result<Duration, McpError> {
    let Some(expires_at) = task
        .retention_expires_at_ms()
        .map_err(|error| McpError::protocol(error.to_string()))?
    else {
        return Ok(Duration::MAX);
    };
    Ok(Duration::from_millis(
        expires_at.saturating_sub(unix_now_ms()),
    ))
}

fn remaining_sampling_task_time(deadline: tokio::time::Instant) -> Result<Duration, McpError> {
    deadline
        .checked_duration_since(tokio::time::Instant::now())
        .ok_or(McpError::DeadlineExceeded)
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn record_sampling_started(
    run: &RunContext,
    id: &RequestId,
    message_count: usize,
    max_tokens: u64,
) -> Result<(), McpError> {
    run.record(
        RunEventKind::Domain(DomainEvent {
            namespace: "runifold.mcp".into(),
            name: "sampling.started".into(),
            payload: json!({
                "call_id": request_id_label(id),
                "message_count": message_count,
                "max_tokens": max_tokens,
            }),
        }),
        run.caused_by(),
    )?;
    Ok(())
}

fn record_sampling_terminal(
    run: &RunContext,
    id: &RequestId,
    result: &Result<CreateMessageOutcome, McpError>,
) -> Result<(), McpError> {
    let (name, payload) = match result {
        Ok(CreateMessageOutcome::Complete(response)) => (
            "sampling.completed",
            json!({
                "call_id": request_id_label(id),
                "model": response.model,
                "stop_reason": response.stop_reason,
            }),
        ),
        Ok(CreateMessageOutcome::Task(task)) => (
            "sampling.task_created",
            json!({
                "call_id": request_id_label(id),
                "task_id": task.task_id,
            }),
        ),
        Err(error) => (
            "sampling.failed",
            json!({
                "call_id": request_id_label(id),
                "error_type": sampling_error_type(error),
                "stage": sampling_error_stage(error),
            }),
        ),
    };
    run.record(
        RunEventKind::Domain(DomainEvent {
            namespace: "runifold.mcp".into(),
            name: name.into(),
            payload,
        }),
        run.caused_by(),
    )?;
    Ok(())
}

fn sampling_error_type(error: &McpError) -> &'static str {
    match error.kind() {
        crate::McpErrorKind::Transport => "transport",
        crate::McpErrorKind::Protocol => "protocol",
        crate::McpErrorKind::Remote => "remote",
        crate::McpErrorKind::DeadlineExceeded => "timeout",
        crate::McpErrorKind::Cancelled => "cancelled",
        crate::McpErrorKind::TaskExpired => "task_expired",
        crate::McpErrorKind::Lifecycle => "lifecycle",
        crate::McpErrorKind::UnsupportedVersion => "unsupported_version",
        crate::McpErrorKind::Authentication => "authentication",
        crate::McpErrorKind::SessionExpired => "session_expired",
        crate::McpErrorKind::Observability => "observability",
    }
}

fn sampling_error_stage(error: &McpError) -> Option<&str> {
    let McpError::Remote {
        data: Some(data), ..
    } = error
    else {
        return None;
    };
    data.get("stage").and_then(serde_json::Value::as_str)
}

fn request_id_label(id: &RequestId) -> String {
    match id {
        RequestId::Null => "null".into(),
        RequestId::Number(number) => number.to_string(),
        RequestId::String(value) => value.clone(),
    }
}
