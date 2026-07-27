use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use runifold_core::{CancellationToken, DomainEvent, RunContext, RunEventKind};
use serde_json::json;

use crate::{
    CreateMessageParams, CreateMessageResult, JsonRpcNotification, JsonRpcRequest, McpError,
    McpSession, RequestId,
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
        self.request(params, CancellationToken::new(), self.timeout, None)
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
        let timeout = context.deadline().map_or(self.timeout, |deadline| {
            deadline
                .saturating_duration_since(std::time::Instant::now())
                .min(self.timeout)
        });
        self.request(
            params,
            context.cancellation().clone(),
            timeout,
            Some(context),
        )
        .await
    }

    async fn request(
        &self,
        params: CreateMessageParams,
        cancellation: CancellationToken,
        timeout: Duration,
        run: Option<&RunContext>,
    ) -> Result<CreateMessageResult, McpError> {
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
                    Ok(response) => response.into_result().and_then(|value| {
                        serde_json::from_value(value).map_err(Into::into)
                    }),
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
    result: &Result<CreateMessageResult, McpError>,
) -> Result<(), McpError> {
    let (name, payload) = match result {
        Ok(response) => (
            "sampling.completed",
            json!({
                "call_id": request_id_label(id),
                "model": response.model,
                "stop_reason": response.stop_reason,
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
