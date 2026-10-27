use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_core::Stream;
use futures_util::future::{Either, select};
use runifold_core::{Instant, RetrySafety};

use super::state::{Permit, lifecycle_error};
use super::{ModelCapacityPoolId, ModelScheduler, ModelTokenEstimator, ModelTokenSettlement};
use crate::{
    Model, ModelCallContext, ModelCapabilities, ModelError, ModelErrorKind, ModelEventStream,
    ModelFuture, ModelRef, ModelRequest, ModelStreamEvent, ModelUsage, ProviderModel,
    ProviderRuntimeProfile,
};

/// A physical Model sharing explicit pools, with permits held through stream termination.
///
/// Place below routing/retry. Dropping an unpolled returned stream releases its
/// permit. The inner Model must stop local transport when its future/stream is
/// dropped; remote processing after disconnect cannot be confirmed here.
/// Poll or drop returned streams to observe cancellation and deadlines;
/// retaining an unpolled stream holds its capacity permit.
#[derive(Clone)]
pub struct ScheduledModel<M> {
    inner: M,
    scheduler: ModelScheduler,
    pools: Vec<ModelCapacityPoolId>,
    estimator: Option<Arc<dyn ModelTokenEstimator>>,
    settlement: ModelTokenSettlement,
}

impl<M> ScheduledModel<M> {
    pub(super) fn new(
        inner: M,
        scheduler: ModelScheduler,
        pools: Vec<ModelCapacityPoolId>,
    ) -> Self {
        Self {
            inner,
            scheduler,
            pools,
            estimator: None,
            settlement: ModelTokenSettlement::default(),
        }
    }
    /// Sets trusted per-attempt token reservation policy, required for token limits.
    #[must_use]
    pub fn token_estimator(mut self, estimator: Arc<dyn ModelTokenEstimator>) -> Self {
        self.estimator = Some(estimator);
        self
    }
    /// Sets explicit trust in final reported billable usage.
    ///
    /// The conservative default retains at least the reservation. Select
    /// `ReportedUsage` only for adapters verified to report complete final usage.
    #[must_use]
    pub fn token_settlement(mut self, policy: ModelTokenSettlement) -> Self {
        self.settlement = policy;
        self
    }
    /// Returns the shared scheduler for payload-free inspection.
    pub const fn scheduler(&self) -> &ModelScheduler {
        &self.scheduler
    }
}

impl<M: Model> Model for ScheduledModel<M> {
    fn capabilities<'a>(
        &'a self,
        model: &'a ModelRef,
    ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
        self.inner.capabilities(model)
    }
    fn stream(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
        Box::pin(async move {
            let mut diagnostics = AttemptDiagnostics::new();
            if let Some(error) =
                lifecycle_error(&context, self.scheduler.shared.clock.now(), false, false)
            {
                return Err(diagnostics.failure(error, self.scheduler.shared.clock.now(), false));
            }
            let mut context = context;
            if !context.enter_scheduler() {
                let error = self
                    .scheduler
                    .reject(super::ModelSchedulingError::NestedAdmission);
                return Err(diagnostics.failure(error, self.scheduler.shared.clock.now(), false));
            }
            let tokens = self
                .estimator
                .as_ref()
                .map_or(Ok(0), |estimator| estimator.estimate(&request))
                .map_err(|error| {
                    diagnostics.failure(error, self.scheduler.shared.clock.now(), false)
                })?;
            // Keep the same attempt identity while giving this wrapper a child
            // cancellation root it can cancel without cancelling its caller.
            let parent = context.cancellation().clone();
            let context = context.with_cancellation(&parent);
            diagnostics.queued_at = Some(self.scheduler.shared.clock.now());
            let permit = self
                .scheduler
                .acquire(&self.pools, tokens, context.clone())
                .await
                .map_err(|error| {
                    diagnostics.failure(error, self.scheduler.shared.clock.now(), false)
                })?;
            diagnostics.admitted_at = Some(self.scheduler.shared.clock.now());
            if let Some(error) =
                lifecycle_error(&context, self.scheduler.shared.clock.now(), false, false)
            {
                drop(permit);
                return Err(diagnostics.failure(error, self.scheduler.shared.clock.now(), false));
            }
            diagnostics.dispatched_at = Some(self.scheduler.shared.clock.now());
            diagnostics.phase = RequestPhase::Opening;
            // The selected future is dropped before permit release on failure.
            let opened = {
                let opening = Box::pin(OpenAttempt {
                    inner: Some(self.inner.stream(request, context.clone())),
                    cancellation: context.cancellation().clone(),
                    completed: false,
                });
                match select(self.stop(context.clone()), opening).await {
                    Either::Left((error, pending)) => {
                        // Abandoning the opening future cancels our child token.
                        // Preserve the stop reason selected before that cleanup.
                        drop(pending);
                        drop(permit);
                        return Err(diagnostics.failure(
                            error,
                            self.scheduler.shared.clock.now(),
                            false,
                        ));
                    }
                    Either::Right((result, pending)) => {
                        drop(pending);
                        result
                    }
                }
            };
            if let Some(error) =
                lifecycle_error(&context, self.scheduler.shared.clock.now(), true, false)
            {
                context.cancellation().cancel();
                drop(opened);
                drop(permit);
                return Err(diagnostics.failure(error, self.scheduler.shared.clock.now(), false));
            }
            let opened = match opened {
                Ok(stream) => stream,
                Err(error) => {
                    drop(permit);
                    return Err(diagnostics.failure(
                        error,
                        self.scheduler.shared.clock.now(),
                        false,
                    ));
                }
            };
            diagnostics.phase = RequestPhase::WaitingFirstEvent;
            let cancellation = context.cancellation().clone();
            let deadline = context.deadline().map(|at| self.scheduler.timer(at));
            Ok(Box::pin(ScheduledStream {
                inner: Some(opened),
                permit: Some(permit),
                scheduler: self.scheduler.clone(),
                context,
                _parent: parent,
                cancelled: Box::pin(async move { cancellation.cancelled().await }),
                deadline,
                visible: false,
                usage: None,
                settlement: self.settlement,
                diagnostics,
            }) as ModelEventStream)
        })
    }
}

impl<M> ScheduledModel<M> {
    fn stop(&self, context: ModelCallContext) -> ModelFuture<'static, ModelError> {
        let scheduler = self.scheduler.clone();
        Box::pin(async move {
            let deadline: ModelFuture<'static, ()> = match context.deadline() {
                Some(at) => scheduler.timer(at),
                None => Box::pin(std::future::pending()),
            };
            let _ = select(Box::pin(context.cancellation().cancelled()), deadline).await;
            lifecycle_error(&context, scheduler.shared.clock.now(), true, false).unwrap_or_else(
                || {
                    ModelError::local(
                        ModelErrorKind::DeadlineExceeded,
                        "scheduled model call exceeded its deadline",
                    )
                },
            )
        })
    }
}

impl<M: ProviderModel> ProviderModel for ScheduledModel<M> {
    fn provider(&self) -> &str {
        self.inner.provider()
    }
    fn runtime_profile(&self) -> ProviderRuntimeProfile {
        self.inner.runtime_profile()
    }
}

impl<M> std::fmt::Debug for ScheduledModel<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScheduledModel")
            .field("pools", &self.pools)
            .finish_non_exhaustive()
    }
}

struct ScheduledStream {
    inner: Option<ModelEventStream>,
    permit: Option<Permit>,
    scheduler: ModelScheduler,
    context: ModelCallContext,
    // Retain the weak-linked cancellation bridge after the opening future exits.
    _parent: runifold_core::CancellationToken,
    cancelled: ModelFuture<'static, ()>,
    deadline: Option<ModelFuture<'static, ()>>,
    visible: bool,
    usage: Option<ModelUsage>,
    settlement: ModelTokenSettlement,
    diagnostics: AttemptDiagnostics,
}

impl ScheduledStream {
    fn finish(&mut self, authoritative: bool) {
        self.context.cancellation().cancel();
        // Drop the transport before making the released slot visible.
        self.inner.take();
        if let Some(mut permit) = self.permit.take() {
            permit.finish(authoritative);
        }
    }
    fn fail(
        &mut self,
        mut error: ModelError,
    ) -> Poll<Option<Result<ModelStreamEvent, ModelError>>> {
        if self.visible {
            error.retry_safety = RetrySafety::UnsafeAfterVisibleOutput;
        }
        self.finish(false);
        let error =
            self.diagnostics
                .failure(error, self.scheduler.shared.clock.now(), self.visible);
        Poll::Ready(Some(Err(error)))
    }
}

impl Stream for ScheduledStream {
    type Item = Result<ModelStreamEvent, ModelError>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.inner.is_none() {
            return Poll::Ready(None);
        }
        let _ = this.cancelled.as_mut().poll(cx);
        let deadline_ready = this
            .deadline
            .as_mut()
            .is_some_and(|timer| timer.as_mut().poll(cx).is_ready());
        if let Some(error) = lifecycle_error(
            &this.context,
            this.scheduler.shared.clock.now(),
            true,
            this.visible,
        ) {
            return this.fail(error);
        }
        if deadline_ready {
            return this.fail(ModelError::local(
                ModelErrorKind::DeadlineExceeded,
                "scheduled model call exceeded its deadline",
            ));
        }
        let item = this
            .inner
            .as_mut()
            .expect("stream was checked above")
            .as_mut()
            .poll_next(cx);
        if let Poll::Ready(Some(Ok(ModelStreamEvent::UsageUpdated { usage }))) = &item {
            // Account cumulative provider usage before handling a lifecycle race;
            // discarded output must not discard already-reported billable debt.
            let previous = this.usage.get_or_insert_with(ModelUsage::default);
            previous.input_tokens = previous.input_tokens.max(usage.input_tokens);
            previous.output_tokens = previous.output_tokens.max(usage.output_tokens);
            if let Some(permit) = &mut this.permit {
                permit.observe(
                    u128::from(previous.input_tokens) + u128::from(previous.output_tokens),
                );
            }
        }
        if let Some(error) = lifecycle_error(
            &this.context,
            this.scheduler.shared.clock.now(),
            true,
            this.visible,
        ) {
            return this.fail(error);
        }
        let event = match item {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(Err(error))) => return this.fail(error),
            Poll::Ready(None) => {
                return this.fail(ModelError::local(
                    ModelErrorKind::Protocol,
                    "scheduled model stream ended before a terminal response event",
                ));
            }
            Poll::Ready(Some(Ok(event))) => event,
        };
        if matches!(event, ModelStreamEvent::ResponseCompleted { .. }) {
            this.finish(this.settlement == ModelTokenSettlement::ReportedUsage);
        }
        this.visible = true;
        this.diagnostics.phase = RequestPhase::Streaming;
        Poll::Ready(Some(Ok(event)))
    }
}

impl Drop for ScheduledStream {
    fn drop(&mut self) {
        self.finish(false);
    }
}

// Cancel before dropping an abandoned opening future, including when the outer
// caller drops it without polling cancellation again.
struct OpenAttempt<'a> {
    inner: Option<ModelFuture<'a, Result<ModelEventStream, ModelError>>>,
    cancellation: runifold_core::CancellationToken,
    completed: bool,
}
impl Future for OpenAttempt<'_> {
    type Output = Result<ModelEventStream, ModelError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = this
            .inner
            .as_mut()
            .expect("opening future exists")
            .as_mut()
            .poll(cx);
        this.completed = result.is_ready();
        result
    }
}
impl Drop for OpenAttempt<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.cancellation.cancel();
        }
        self.inner.take();
    }
}

#[derive(Clone, Copy)]
enum RequestPhase {
    Admission,
    Opening,
    WaitingFirstEvent,
    Streaming,
}

struct AttemptDiagnostics {
    phase: RequestPhase,
    queued_at: Option<Instant>,
    admitted_at: Option<Instant>,
    dispatched_at: Option<Instant>,
}

impl AttemptDiagnostics {
    fn new() -> Self {
        Self {
            phase: RequestPhase::Admission,
            queued_at: None,
            admitted_at: None,
            dispatched_at: None,
        }
    }

    // Called only after the inner transport and any capacity permit are dropped.
    // Invoking the provider is not proof of upstream acceptance or termination.
    fn failure(&self, mut error: ModelError, now: Instant, output_observed: bool) -> ModelError {
        let phase = match self.phase {
            RequestPhase::Admission => "admission",
            RequestPhase::Opening => "opening",
            RequestPhase::WaitingFirstEvent => "waiting_first_event",
            RequestPhase::Streaming => "streaming",
        };
        let invoked = self.dispatched_at.is_some();
        let queue_wait = self.queued_at.map_or(std::time::Duration::ZERO, |at| {
            self.admitted_at
                .unwrap_or(now)
                .saturating_duration_since(at)
        });
        let execution = self.dispatched_at.map_or(std::time::Duration::ZERO, |at| {
            now.saturating_duration_since(at)
        });
        for (key, value) in [
            ("runifold.request.phase", serde_json::Value::from(phase)),
            ("runifold.request.provider_invoked", invoked.into()),
            ("runifold.request.local_capacity_released", true.into()),
            ("runifold.request.output_observed", output_observed.into()),
            (
                "runifold.request.remote_outcome",
                if invoked { "unknown" } else { "not_started" }.into(),
            ),
            (
                "runifold.request.queue_wait_ms",
                u64::try_from(queue_wait.as_millis())
                    .unwrap_or(u64::MAX)
                    .into(),
            ),
            (
                "runifold.request.execution_ms",
                u64::try_from(execution.as_millis())
                    .unwrap_or(u64::MAX)
                    .into(),
            ),
        ] {
            error.metadata.insert(key.into(), value);
        }
        error
    }
}
