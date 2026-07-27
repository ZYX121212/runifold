//! Runtime route selection, retries, circuit permits, and stream commitment.

use std::sync::Arc;

use futures_util::{
    StreamExt,
    future::{Either, select},
};
use runifold_core::RetrySafety;
use serde_json::{Value, json};

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
                match start_route_attempt(route, &request, &context, &runtime).await {
                    RouteAttempt::CircuitOpen => {
                        failures.push(circuit_open_summary(route, route_attempt));
                        continue 'routes;
                    }
                    RouteAttempt::Committed {
                        first,
                        stream,
                        permit,
                        attempt_id,
                    } => {
                        let terminal = is_terminal(&first);
                        yield Ok(first);
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
                        );
                        while let Some(item) = committed.next().await {
                            yield item;
                        }
                        return;
                    }
                    RouteAttempt::Failed(error) => {
                        failures.push(failure_summary(route, route_attempt, &error));
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
        first: ModelStreamEvent,
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
    if let Some(remaining) = context.remaining() {
        if delay >= remaining {
            return RetryWait::Stop(retry_deadline(delay, remaining));
        }
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
) -> RouteAttempt {
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
    let attempt = context.child_attempt();
    let attempt_id = attempt.invocation_id().to_string();
    let opened = route.model.stream(routed_request, attempt).await;
    let mut stream = match opened {
        Ok(stream) => stream,
        Err(error) => {
            fail_permit(permit, &error);
            return RouteAttempt::Failed(error);
        }
    };
    match stream.next().await {
        Some(Ok(first)) => RouteAttempt::Committed {
            first,
            stream,
            permit,
            attempt_id,
        },
        Some(Err(error)) => {
            fail_permit(permit, &error);
            RouteAttempt::Failed(error)
        }
        None => {
            let error = ModelError::local(
                ModelErrorKind::Protocol,
                "candidate model stream ended before its first event",
            );
            fail_permit(permit, &error);
            RouteAttempt::Failed(error)
        }
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
) -> ModelEventStream {
    Box::pin(async_stream::stream! {
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
                Ok(event) => {
                    let terminal = is_terminal(&event);
                    yield Ok(event);
                    if terminal {
                        succeed_permit(permit);
                        return;
                    }
                }
                Err(mut error) => {
                    error.retry_safety = RetrySafety::UnsafeAfterVisibleOutput;
                    fail_permit(permit, &error);
                    failures.push(failure_summary(
                        &committed.route,
                        committed.route_attempt,
                        &error,
                    ));
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
        failures.push(failure_summary(
            &committed.route,
            committed.route_attempt,
            &error,
        ));
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
    error
        .metadata
        .insert("runifold.router.failures".into(), Value::Array(failures));
    error
}
