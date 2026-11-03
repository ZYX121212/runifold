//! Runtime route selection, retries, circuit permits, and stream commitment.

use std::sync::Arc;

use futures_util::{
    StreamExt,
    future::{Either, select},
};
use runifold_core::RetrySafety;
use serde_json::{Value, json};

use crate::{
    ModelRequestOutcome, ModelRouteAttemptReceipt, ModelRouteReceipt, ModelStreamAccumulator,
    ModelUsage, ResponseMode,
};

use super::{
    BreakerPermit, CircuitBreakerConfig, ModelCallContext, ModelError, ModelErrorKind,
    ModelEventStream, ModelFallbackPolicy, ModelRef, ModelRequest, ModelRetryPolicy, ModelRoute,
    ModelRouter, ModelStreamEvent, ProviderEvent, RoutePermit, RouterClock, RouterSleeper,
};

pub(super) struct RoutingRuntime {
    pub(super) circuit_breaker: Option<CircuitBreakerConfig>,
    pub(super) clock: Arc<dyn RouterClock>,
    pub(super) retry_policy: Option<ModelRetryPolicy>,
    pub(super) sleeper: Arc<dyn RouterSleeper>,
}

pub(super) fn routed_stream(
    routes: Vec<ModelRoute>,
    fallback: ModelFallbackPolicy,
    runtime: RoutingRuntime,
    request: ModelRequest,
    context: ModelCallContext,
) -> ModelEventStream {
    Box::pin(async_stream::stream! {
        let mut failures = Vec::new();
        'routes: for (index, route) in routes.iter().enumerate() {
            let max_attempts = runtime
                .retry_policy
                .as_ref()
                .map_or(1, ModelRetryPolicy::max_attempts);
            for route_attempt in 1..=max_attempts {
                if context.cancellation().is_cancelled() {
                    let error = cancelled_retry();
                    yield Err(annotate_error(error, &request.model, failures));
                    return;
                }
                match start_route_attempt(route, &request, &context, &runtime, &failures, route_attempt).await {
                    RouteAttempt::CircuitOpen => {
                        failures.push(circuit_open_summary(route, route_attempt));
                        snapshot_failures(&context, &request.model, &failures);
                        continue 'routes;
                    }
                    RouteAttempt::Committed {
                        first,
                        stream,
                        permit,
                        attempt_id,
                    } => {
                        let terminal = is_terminal(&first);
                        yield Ok(*first);
                        if terminal {
                            succeed_permit(permit);
                            return;
                        }
                        let circuit_probe =
                            permit.as_ref().is_some_and(BreakerPermit::is_probe);
                        let mut committed = committed_stream(
                            CommittedRoute {
                                route: route.clone(),
                                index,
                                route_attempt,
                                attempt_id,
                                circuit_probe,
                            },
                            stream,
                            permit,
                            failures,
                            request.model.clone(),
                            context.clone(),
                        );
                        while let Some(item) = committed.next().await {
                            yield item;
                        }
                        return;
                    }
                    RouteAttempt::Failed(error) => {
                        failures.push(failure_summary(route, route_attempt, &error));
                        snapshot_failures(&context, &request.model, &failures);
                        let retry = runtime.retry_policy.as_ref().is_some_and(|policy| {
                            route_attempt < max_attempts && policy.permits(&error)
                        });
                        if retry {
                            match wait_before_retry(
                                runtime.retry_policy.as_ref().expect("retry policy exists"),
                                &error,
                                route,
                                route_attempt,
                                &context,
                                &runtime.sleeper,
                            )
                            .await
                            {
                                RetryWait::Ready => continue,
                                RetryWait::Stop(stop) => {
                                    yield Err(annotate_error(
                                        stop,
                                        &request.model,
                                        failures,
                                    ));
                                    return;
                                }
                            }
                        }
                        if index + 1 < routes.len() && fallback.permits(&error) {
                            continue 'routes;
                        }
                        yield Err(annotate_error(error, &request.model, failures));
                        return;
                    }
                }
            }
        }
        let mut error = ModelError::local(
            ModelErrorKind::Provider,
            "all physical model routes are unavailable",
        );
        error.retry_safety = RetrySafety::Safe;
        yield Err(annotate_error(error, &request.model, failures));
    })
}

enum RouteAttempt {
    CircuitOpen,
    Failed(ModelError),
    Committed {
        first: Box<ModelStreamEvent>,
        stream: ModelEventStream,
        permit: Option<BreakerPermit>,
        attempt_id: String,
    },
}

enum RetryWait {
    Ready,
    Stop(ModelError),
}

async fn wait_before_retry(
    policy: &ModelRetryPolicy,
    error: &ModelError,
    route: &ModelRoute,
    failed_attempt: u32,
    context: &ModelCallContext,
    sleeper: &Arc<dyn RouterSleeper>,
) -> RetryWait {
    let entropy = retry_entropy(
        &context.invocation_id().to_string(),
        &route.name,
        failed_attempt,
    );
    let backoff = policy.delay(failed_attempt, entropy);
    let delay = retry_after(error).map_or(backoff, |server| server.max(backoff));
    if let Some(remaining) = context.remaining()
        && delay >= remaining
    {
        return RetryWait::Stop(retry_deadline(delay, remaining));
    }
    if context.cancellation().is_cancelled() {
        return RetryWait::Stop(cancelled_retry());
    }
    if delay.is_zero() {
        return RetryWait::Ready;
    }
    let cancellation = context.cancellation().clone();
    match select(
        Box::pin(cancellation.cancelled()),
        Box::pin(sleeper.sleep(delay)),
    )
    .await
    {
        Either::Left(_) => RetryWait::Stop(cancelled_retry()),
        Either::Right(_) => RetryWait::Ready,
    }
}

fn retry_after(error: &ModelError) -> Option<std::time::Duration> {
    error
        .metadata
        .get("retry.after_ms")
        .and_then(Value::as_u64)
        .map(std::time::Duration::from_millis)
}

fn retry_entropy(invocation: &str, route: &str, attempt: u32) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in invocation
        .bytes()
        .chain(route.bytes())
        .chain(attempt.to_le_bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn cancelled_retry() -> ModelError {
    ModelError::local(
        ModelErrorKind::Cancelled,
        "logical model invocation was cancelled during retry",
    )
}

fn retry_deadline(delay: std::time::Duration, remaining: std::time::Duration) -> ModelError {
    let mut error = ModelError::local(
        ModelErrorKind::DeadlineExceeded,
        "retry backoff would exceed the model invocation deadline",
    );
    error.metadata.insert(
        "retry.delay_ms".into(),
        Value::from(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX)),
    );
    error.metadata.insert(
        "retry.remaining_ms".into(),
        Value::from(u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX)),
    );
    error
}

async fn start_route_attempt(
    route: &ModelRoute,
    request: &ModelRequest,
    context: &ModelCallContext,
    runtime: &RoutingRuntime,
    failures: &[Value],
    route_attempt: u32,
) -> RouteAttempt {
    if context.cancellation().is_cancelled()
        || context
            .deadline()
            .is_some_and(|deadline| deadline <= runtime.clock.now())
    {
        let mut error = ModelError::local(
            if context.cancellation().is_cancelled() {
                ModelErrorKind::Cancelled
            } else {
                ModelErrorKind::DeadlineExceeded
            },
            "model route scope ended before provider invocation",
        );
        error.retry_safety = RetrySafety::Safe;
        error
            .metadata
            .insert("runifold.request.provider_invoked".into(), false.into());
        error.metadata.insert(
            "runifold.request.remote_outcome".into(),
            "not_started".into(),
        );
        return RouteAttempt::Failed(error);
    }
    let permit = match crate::circuit::acquire(
        &route.health,
        runtime.circuit_breaker.as_ref(),
        &runtime.clock,
    ) {
        RoutePermit::Disabled => None,
        RoutePermit::Acquired(permit) => Some(permit),
        RoutePermit::Rejected => return RouteAttempt::CircuitOpen,
    };
    let mut routed_request = request.clone();
    routed_request.model.clone_from(&route.target);
    let response_mode = routed_request.selected_response_mode();
    let attempt = context.child_attempt();
    let attempt_id = attempt.invocation_id().to_string();
    let mut pending = completed_receipt(
        route,
        route_attempt,
        &attempt_id,
        &request.model,
        failures,
        None,
    );
    pending
        .attempts
        .last_mut()
        .expect("pending receipt includes this attempt")
        .outcome = ModelRequestOutcome::Unknown;
    context.set_route_receipt(pending);
    let opened = route.model.stream(routed_request, attempt).await;
    let mut stream = match opened {
        Ok(stream) => stream,
        Err(error) => {
            fail_permit(permit, &error);
            return RouteAttempt::Failed(attempt_error(error, &attempt_id));
        }
    };
    if response_mode == ResponseMode::Complete {
        return validate_complete_attempt(stream, permit, attempt_id, context).await;
    }
    match stream.next().await {
        Some(Ok(first)) => RouteAttempt::Committed {
            first: Box::new(first),
            stream,
            permit,
            attempt_id,
        },
        Some(Err(error)) => {
            fail_permit(permit, &error);
            RouteAttempt::Failed(attempt_error(error, &attempt_id))
        }
        None => {
            let error = ModelError::local(
                ModelErrorKind::Protocol,
                "candidate model stream ended before its first event",
            );
            fail_permit(permit, &error);
            RouteAttempt::Failed(attempt_error(error, &attempt_id))
        }
    }
}

async fn validate_complete_attempt(
    mut stream: ModelEventStream,
    permit: Option<BreakerPermit>,
    attempt_id: String,
    context: &ModelCallContext,
) -> RouteAttempt {
    let mut accumulator = ModelStreamAccumulator::new();
    let mut events = Vec::new();
    let mut completed = false;
    let mut usage = None;
    while let Some(item) = stream.next().await {
        let event = match item {
            Ok(event) => event,
            Err(error) => {
                fail_permit(permit, &error);
                return RouteAttempt::Failed(attempt_error_with_usage(error, &attempt_id, usage));
            }
        };
        if let ModelStreamEvent::UsageUpdated { usage: reported } = &event {
            usage = Some(*reported);
            context.observe_route_usage(*reported);
        }
        match accumulator.push(event.clone()) {
            Ok(response) => completed |= response.is_some(),
            Err(error) => {
                fail_permit(permit, &error);
                return RouteAttempt::Failed(attempt_error_with_usage(error, &attempt_id, usage));
            }
        }
        events.push(event);
    }
    if !completed {
        let error = ModelError::local(
            ModelErrorKind::Protocol,
            "complete model stream ended before a terminal response event",
        );
        fail_permit(permit, &error);
        return RouteAttempt::Failed(attempt_error_with_usage(error, &attempt_id, usage));
    }
    let mut events = events.into_iter();
    let Some(first) = events.next() else {
        let error = ModelError::local(
            ModelErrorKind::Protocol,
            "validated complete model stream contained no events",
        );
        fail_permit(permit, &error);
        return RouteAttempt::Failed(attempt_error_with_usage(error, &attempt_id, usage));
    };
    RouteAttempt::Committed {
        first: Box::new(first),
        stream: Box::pin(futures_util::stream::iter(events.map(Ok))),
        permit,
        attempt_id,
    }
}

struct CommittedRoute {
    route: ModelRoute,
    index: usize,
    route_attempt: u32,
    attempt_id: String,
    circuit_probe: bool,
}

fn committed_stream(
    committed: CommittedRoute,
    mut stream: ModelEventStream,
    permit: Option<BreakerPermit>,
    mut failures: Vec<Value>,
    logical: ModelRef,
    context: ModelCallContext,
) -> ModelEventStream {
    Box::pin(async_stream::stream! {
        let mut usage = None;
        yield Ok(selected_event(
            &committed.route,
            committed.index,
            committed.route_attempt,
            &committed.attempt_id,
            committed.circuit_probe,
            &failures,
        ));
        while let Some(item) = stream.next().await {
            match item {
                Ok(mut event) => {
                    if let ModelStreamEvent::UsageUpdated { usage: reported } = &event {
                        usage = Some(*reported);
                        context.observe_route_usage(*reported);
                    }
                    let terminal = is_terminal(&event);
                    if let ModelStreamEvent::ResponseCompleted { provider_metadata, .. } = &mut event {
                        let receipt = completed_receipt(&committed.route, committed.route_attempt, &committed.attempt_id, &logical, &failures, usage);
                        provider_metadata.insert("runifold.router.receipt".into(), serde_json::json!(receipt));
                        context.set_route_receipt(receipt);
                    }
                    yield Ok(event);
                    if terminal {
                        succeed_permit(permit);
                        return;
                    }
                }
                Err(mut error) => {
                    error.retry_safety = RetrySafety::UnsafeAfterVisibleOutput;
                    fail_permit(permit, &error);
                    error = attempt_error(error, &committed.attempt_id);
                    if let Some(usage) = usage { error.metadata.insert("runifold.request.usage".into(), serde_json::json!(usage)); }
                    failures.push(failure_summary(
                        &committed.route,
                        committed.route_attempt,
                        &error,
                    ));
                    snapshot_failures(&context, &logical, &failures);
                    yield Err(annotate_error(error, &logical, failures));
                    return;
                }
            }
        }
        let mut error = ModelError::local(
            ModelErrorKind::Protocol,
            "selected model stream ended without a terminal event",
        );
        error.retry_safety = RetrySafety::UnsafeAfterVisibleOutput;
        fail_permit(permit, &error);
        error = attempt_error(error, &committed.attempt_id);
        if let Some(usage) = usage { error.metadata.insert("runifold.request.usage".into(), serde_json::json!(usage)); }
        failures.push(failure_summary(
            &committed.route,
            committed.route_attempt,
            &error,
        ));
        snapshot_failures(&context, &logical, &failures);
        yield Err(annotate_error(error, &logical, failures));
    })
}

fn fail_permit(permit: Option<BreakerPermit>, error: &ModelError) {
    if let Some(permit) = permit {
        permit.failure(error);
    }
}

fn succeed_permit(permit: Option<BreakerPermit>) {
    if let Some(permit) = permit {
        permit.success();
    }
}

impl std::fmt::Debug for ModelRouter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ModelRouter")
            .field("logical", &self.logical)
            .field("routes", &self.routes)
            .field("policy", &self.policy)
            .field("circuit_breaker", &self.circuit_breaker)
            .field("retry_policy", &self.retry_policy)
            .finish_non_exhaustive()
    }
}

fn selected_event(
    route: &ModelRoute,
    index: usize,
    route_attempt: u32,
    attempt_id: &str,
    circuit_probe: bool,
    failures: &[Value],
) -> ModelStreamEvent {
    ModelStreamEvent::Provider {
        event: ProviderEvent {
            provider: "runifold.router".into(),
            name: "route.selected".into(),
            payload: json!({
                "route": route.name,
                "target": route.target,
                "index": index,
                "route_attempt": route_attempt,
                "attempt_id": attempt_id,
                "circuit_probe": circuit_probe,
                "prior_failures": failures,
            }),
        },
    }
}

fn failure_summary(route: &ModelRoute, route_attempt: u32, error: &ModelError) -> Value {
    json!({
        "route": route.name,
        "target": route.target,
        "route_attempt": route_attempt,
        "kind": &error.kind,
        "retry_safety": error.retry_safety,
        "diagnostic_code": error.diagnostic_code(),
        "diagnostics": error.diagnostic_metadata(),
        "attempt_id": error.metadata.get("runifold.router.attempt_id"),
        "usage": error.metadata.get("runifold.request.usage").and_then(|value| serde_json::from_value::<ModelUsage>(value.clone()).ok()),
    })
}

fn circuit_open_summary(route: &ModelRoute, route_attempt: u32) -> Value {
    json!({
        "route": route.name,
        "target": route.target,
        "route_attempt": route_attempt,
        "kind": "circuit_open",
    })
}

const fn is_terminal(event: &ModelStreamEvent) -> bool {
    matches!(event, ModelStreamEvent::ResponseCompleted { .. })
}

fn annotate_error(mut error: ModelError, logical: &ModelRef, failures: Vec<Value>) -> ModelError {
    error.metadata.insert(
        "runifold.router.logical_model".into(),
        serde_json::to_value(logical).expect("ModelRef is serializable"),
    );
    let receipt = ModelRouteReceipt {
        schema_version: 1,
        logical_model: logical.clone(),
        attempts: failure_receipts(&failures),
    };
    error
        .metadata
        .insert("runifold.router.receipt".into(), serde_json::json!(receipt));
    error
        .metadata
        .insert("runifold.router.failures".into(), Value::Array(failures));
    error
}

fn attempt_error(mut error: ModelError, attempt_id: &str) -> ModelError {
    error
        .metadata
        .insert("runifold.router.attempt_id".into(), attempt_id.into());
    error
}

fn failure_receipts(failures: &[Value]) -> Vec<ModelRouteAttemptReceipt> {
    failures
        .iter()
        .map(|failure| {
            let diagnostics: std::collections::BTreeMap<String, Value> = failure
                .get("diagnostics")
                .and_then(|value| serde_json::from_value(value.clone()).ok())
                .unwrap_or_default();
            let not_started = failure["kind"] == "circuit_open"
                || (diagnostics.get("runifold.request.provider_invoked")
                    == Some(&Value::Bool(false))
                    && diagnostics
                        .get("runifold.request.remote_outcome")
                        .and_then(Value::as_str)
                        == Some("not_started"));
            ModelRouteAttemptReceipt {
                route: failure["route"].as_str().unwrap_or_default().into(),
                target: serde_json::from_value(failure["target"].clone())
                    .expect("runtime route target is serializable"),
                route_attempt: serde_json::from_value(failure["route_attempt"].clone())
                    .expect("runtime attempt number is serializable"),
                attempt_id: failure["attempt_id"].as_str().map(str::to_owned),
                outcome: if not_started {
                    ModelRequestOutcome::NotStarted
                } else {
                    ModelRequestOutcome::Unknown
                },
                retry_safety: serde_json::from_value(failure["retry_safety"].clone())
                    .unwrap_or(RetrySafety::Unknown),
                usage: serde_json::from_value(failure["usage"].clone()).ok(),
                error_kind: serde_json::from_value(failure["kind"].clone()).ok(),
                diagnostics,
            }
        })
        .collect()
}

fn completed_receipt(
    route: &ModelRoute,
    route_attempt: u32,
    attempt_id: &str,
    logical: &ModelRef,
    failures: &[Value],
    usage: Option<ModelUsage>,
) -> ModelRouteReceipt {
    let mut attempts = failure_receipts(failures);
    attempts.push(ModelRouteAttemptReceipt {
        route: route.name.clone(),
        target: route.target.clone(),
        route_attempt,
        attempt_id: Some(attempt_id.into()),
        outcome: ModelRequestOutcome::Completed,
        retry_safety: RetrySafety::Unknown,
        usage,
        error_kind: None,
        diagnostics: std::collections::BTreeMap::new(),
    });
    ModelRouteReceipt {
        schema_version: 1,
        logical_model: logical.clone(),
        attempts,
    }
}

fn attempt_error_with_usage(
    mut error: ModelError,
    attempt_id: &str,
    usage: Option<ModelUsage>,
) -> ModelError {
    if let Some(usage) = usage {
        error
            .metadata
            .insert("runifold.request.usage".into(), serde_json::json!(usage));
    }
    attempt_error(error, attempt_id)
}

fn snapshot_failures(context: &ModelCallContext, logical: &ModelRef, failures: &[Value]) {
    context.set_route_receipt(ModelRouteReceipt {
        schema_version: 1,
        logical_model: logical.clone(),
        attempts: failure_receipts(failures),
    });
}
