use std::{
    collections::VecDeque,
    future::Future,
    num::{NonZeroU64, NonZeroUsize},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures_core::Stream;
use futures_util::{StreamExt, task::noop_waker};
use runifold_core::{CancellationToken, Instant, RetrySafety};

use super::*;
use crate::{
    FinishReason, Message, ModelCallContext, ModelCapabilities, ModelEventStream, ModelFuture,
    ModelRef, ModelRetryPolicy, ModelRouter, ModelStreamEvent, ModelUsage, ProviderModel,
    RouterSleepFuture,
};

#[derive(Clone)]
enum Script {
    Hold,
    OpenHold,
    Events(Vec<Result<ModelStreamEvent, ModelError>>),
    Error(ModelError),
}

#[derive(Clone)]
struct Probe {
    scripts: Arc<Mutex<VecDeque<Script>>>,
    calls: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    cancelled_drops: Arc<AtomicUsize>,
}
impl Probe {
    fn new(scripts: impl IntoIterator<Item = Script>) -> Self {
        Self {
            scripts: Arc::new(Mutex::new(scripts.into_iter().collect())),
            calls: Arc::new(AtomicUsize::new(0)),
            active: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
            drops: Arc::new(AtomicUsize::new(0)),
            cancelled_drops: Arc::new(AtomicUsize::new(0)),
        }
    }
}
struct Transport {
    probe: Probe,
    cancellation: CancellationToken,
    events: VecDeque<Result<ModelStreamEvent, ModelError>>,
    hold: bool,
}
impl Stream for Transport {
    type Item = Result<ModelStreamEvent, ModelError>;
    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(event) = self.events.pop_front() {
            Poll::Ready(Some(event))
        } else if self.hold {
            Poll::Pending
        } else {
            Poll::Ready(None)
        }
    }
}
impl Drop for Transport {
    fn drop(&mut self) {
        self.probe.active.fetch_sub(1, Ordering::SeqCst);
        self.probe.drops.fetch_add(1, Ordering::SeqCst);
        if self.cancellation.is_cancelled() {
            self.probe.cancelled_drops.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl Model for Probe {
    fn capabilities<'a>(
        &'a self,
        _: &'a ModelRef,
    ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
        Box::pin(async { Ok(ModelCapabilities::default()) })
    }
    fn stream(
        &self,
        _: ModelRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            let script = self
                .scripts
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Script::Hold);
            let mut transport = Transport {
                probe: self.clone(),
                cancellation: context.cancellation().clone(),
                events: VecDeque::new(),
                hold: false,
            };
            match script {
                Script::Hold => transport.hold = true,
                Script::OpenHold => std::future::pending::<()>().await,
                Script::Events(events) => transport.events = events.into(),
                Script::Error(error) => return Err(error),
            }
            Ok(Box::pin(transport) as ModelEventStream)
        })
    }
}
impl ProviderModel for Probe {
    fn provider(&self) -> &'static str {
        "probe"
    }
}

#[derive(Clone)]
struct Time {
    now: Arc<Mutex<Instant>>,
    timers: Arc<Mutex<Vec<Waker>>>,
}
impl Time {
    fn new() -> Self {
        Self {
            now: Arc::new(Mutex::new(Instant::now())),
            timers: Arc::default(),
        }
    }
    fn advance(&self, duration: Duration) {
        *self.now.lock().unwrap() += duration;
        let wakers = std::mem::take(&mut *self.timers.lock().unwrap());
        for waker in wakers {
            waker.wake();
        }
    }
}
impl RouterClock for Time {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap()
    }
}
impl RouterSleeper for Time {
    fn sleep(&self, duration: Duration) -> RouterSleepFuture<'_> {
        let at = self.now() + duration;
        Box::pin(futures_util::future::poll_fn(move |cx| {
            if self.now() >= at {
                Poll::Ready(())
            } else {
                self.timers.lock().unwrap().push(cx.waker().clone());
                Poll::Pending
            }
        }))
    }
}
struct Estimate(u64);
impl ModelTokenEstimator for Estimate {
    fn estimate(&self, _: &ModelRequest) -> Result<u64, ModelError> {
        Ok(self.0)
    }
}
fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
fn pool(name: &str) -> ModelCapacityPoolId {
    ModelCapacityPoolId::new(name).unwrap()
}
fn rate(value: u64) -> ModelRateLimit {
    ModelRateLimit::new(NonZeroU64::new(value).unwrap(), Duration::from_secs(60)).unwrap()
}
fn scheduler(limits: ModelCapacityLimits, queue: usize) -> (ModelScheduler, Time) {
    let time = Time::new();
    let config = ModelSchedulerConfig::new(nz(queue))
        .pool(pool("account"), limits)
        .unwrap();
    (
        ModelScheduler::with_time(config, Arc::new(time.clone()), Arc::new(time.clone())).unwrap(),
        time,
    )
}
fn request() -> ModelRequest {
    ModelRequest::new(
        ModelRef::new("probe", "test"),
        Message::user("private prompt"),
    )
}
fn complete() -> ModelStreamEvent {
    ModelStreamEvent::ResponseCompleted {
        finish_reason: FinishReason::Stop,
        provider_metadata: BTreeMap::default(),
    }
}
fn usage(input: u64, output: u64) -> ModelStreamEvent {
    ModelStreamEvent::UsageUpdated {
        usage: ModelUsage {
            input_tokens: input,
            output_tokens: output,
            ..ModelUsage::default()
        },
    }
}
fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(&noop_waker()))
}
fn next(stream: &mut ModelEventStream) -> Poll<Option<Result<ModelStreamEvent, ModelError>>> {
    stream
        .as_mut()
        .poll_next(&mut Context::from_waker(&noop_waker()))
}
fn open(model: &impl Model) -> ModelEventStream {
    futures_executor::block_on(model.stream(request(), ModelCallContext::new())).unwrap()
}
fn drain(stream: &mut ModelEventStream) {
    while let Poll::Ready(Some(event)) = next(stream) {
        event.unwrap();
    }
}

#[test]
fn configuration_and_bindings_fail_closed() {
    assert!(ModelCapacityPoolId::new("secret with spaces").is_err());
    assert!(ModelRateLimit::new(NonZeroU64::MIN, Duration::ZERO).is_err());
    assert!(ModelRateLimit::new(NonZeroU64::MIN, Duration::MAX).is_err());
    assert!(ModelScheduler::new(ModelSchedulerConfig::new(nz(1))).is_err());
    let (scheduler, _) = scheduler(ModelCapacityLimits::new(nz(1)), 2);
    let probe = Probe::new([]);
    assert!(scheduler.bind(probe.clone(), []).is_err());
    assert!(scheduler.bind(probe.clone(), [pool("missing")]).is_err());
    assert!(
        scheduler
            .bind(probe.clone(), [pool("account"), pool("account")])
            .is_err()
    );
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn shared_clones_bound_many_requests_and_complete_immediately_refills() {
    let (scheduler, _) = scheduler(ModelCapacityLimits::new(nz(2)), 200);
    let probe = Probe::new([Script::Hold, Script::Events(vec![Ok(complete())])]);
    let a = scheduler.bind(probe.clone(), [pool("account")]).unwrap();
    let b = scheduler
        .clone()
        .bind(probe.clone(), [pool("account")])
        .unwrap();
    let long = open(&a);
    let mut short = open(&b);
    let mut waiting: Vec<_> = (0..100)
        .map(|_| b.stream(request(), ModelCallContext::new()))
        .collect();
    for future in &mut waiting {
        assert!(poll(future.as_mut()).is_pending());
    }
    assert_eq!(scheduler.snapshot().queued, 100);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 2);
    assert!(matches!(next(&mut short), Poll::Ready(Some(Ok(_)))));
    assert_eq!(scheduler.snapshot().pools[0].active, 1);
    let Poll::Ready(Ok(replacement)) = poll(waiting[0].as_mut()) else {
        panic!("slot was not refilled")
    };
    assert_eq!(probe.peak.load(Ordering::SeqCst), 2);
    assert!(next(&mut short).is_ready()); // Retaining terminal stream does not pin capacity.
    drop(replacement);
    drop(long);
    drop(waiting);
    assert_eq!(scheduler.snapshot().queued, 0);
    assert_eq!(scheduler.snapshot().pools[0].active, 0);
    assert_eq!(probe.active.load(Ordering::SeqCst), 0);
}

#[test]
fn queue_overflow_drop_cancellation_and_deadline_do_no_provider_work() {
    let (scheduler, time) = scheduler(ModelCapacityLimits::new(nz(1)), 1);
    let probe = Probe::new([]);
    let model = scheduler.bind(probe.clone(), [pool("account")]).unwrap();
    let held = open(&model);
    let cancellation = CancellationToken::new();
    let mut waiting = model.stream(
        request(),
        ModelCallContext::new().with_cancellation(&cancellation),
    );
    assert!(poll(waiting.as_mut()).is_pending());
    let error = futures_executor::block_on(model.stream(request(), ModelCallContext::new()))
        .err()
        .unwrap();
    assert_eq!(error.kind, ModelErrorKind::AdmissionDenied);
    assert_eq!(error.retry_safety, RetrySafety::Safe);
    assert_eq!(error.metadata["runifold.scheduler.reason"], "queue_full");
    assert!(!error.to_string().contains("private"));
    cancellation.cancel();
    assert!(matches!(
        poll(waiting.as_mut()),
        Poll::Ready(Err(ModelError {
            kind: ModelErrorKind::Cancelled,
            ..
        }))
    ));
    assert_eq!(scheduler.snapshot().queued, 0);
    let mut expired = model.stream(
        request(),
        ModelCallContext::new().with_deadline(time.now() + Duration::from_secs(1)),
    );
    assert!(poll(expired.as_mut()).is_pending());
    time.advance(Duration::from_secs(1));
    assert!(matches!(
        poll(expired.as_mut()),
        Poll::Ready(Err(ModelError {
            kind: ModelErrorKind::DeadlineExceeded,
            ..
        }))
    ));
    let mut abandoned = model.stream(request(), ModelCallContext::new());
    assert!(poll(abandoned.as_mut()).is_pending());
    drop(abandoned);
    assert_eq!(scheduler.snapshot().queued, 0);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    assert_eq!(scheduler.snapshot().cancelled, 1);
    assert_eq!(scheduler.snapshot().expired, 1);
    drop(held);
}

#[test]
fn multi_pool_admission_is_atomic_and_disjoint_pools_progress() {
    let config = ModelSchedulerConfig::new(nz(10))
        .pool(pool("a"), ModelCapacityLimits::new(nz(1)))
        .unwrap()
        .pool(pool("b"), ModelCapacityLimits::new(nz(1)))
        .unwrap()
        .pool(pool("c"), ModelCapacityLimits::new(nz(1)))
        .unwrap();
    let scheduler = ModelScheduler::new(config).unwrap();
    let probe = Probe::new([]);
    let a = scheduler.bind(probe.clone(), [pool("a")]).unwrap();
    let ab = scheduler
        .bind(probe.clone(), [pool("a"), pool("b")])
        .unwrap();
    let c = scheduler.bind(probe.clone(), [pool("c")]).unwrap();
    let held = open(&a);
    let mut blocked = ab.stream(request(), ModelCallContext::new());
    assert!(poll(blocked.as_mut()).is_pending());
    assert_eq!(scheduler.snapshot().pools[1].active, 0);
    let independent = open(&c);
    drop(held);
    let Poll::Ready(Ok(both)) = poll(blocked.as_mut()) else {
        panic!("joint admission failed")
    };
    let snapshot = scheduler.snapshot();
    assert_eq!(
        snapshot.pools.iter().map(|p| p.active).collect::<Vec<_>>(),
        [1, 1, 1]
    );
    drop(both);
    drop(independent);
}

#[test]
fn request_window_expires_exactly_and_failed_attempts_are_not_refunded() {
    let (scheduler, time) = scheduler(ModelCapacityLimits::new(nz(2)).requests(rate(1)), 10);
    let probe = Probe::new([Script::Error(ModelError::local(
        ModelErrorKind::Transport,
        "lost",
    ))]);
    let model = scheduler.bind(probe.clone(), [pool("account")]).unwrap();
    assert!(futures_executor::block_on(model.stream(request(), ModelCallContext::new())).is_err());
    let mut waiting = model.stream(request(), ModelCallContext::new());
    assert!(poll(waiting.as_mut()).is_pending());
    time.advance(Duration::from_secs(59));
    assert!(poll(waiting.as_mut()).is_pending());
    time.advance(Duration::from_secs(1));
    assert!(matches!(poll(waiting.as_mut()), Poll::Ready(Ok(_))));
    assert_eq!(probe.calls.load(Ordering::SeqCst), 2);
}

#[test]
fn token_reservations_settle_cumulative_usage_and_live_debt() {
    let (scheduler, time) = scheduler(ModelCapacityLimits::new(nz(3)).tokens(rate(100)), 10);
    let probe = Probe::new([Script::Events(vec![
        Ok(usage(30, 10)),
        Ok(usage(30, 20)),
        Ok(complete()),
    ])]);
    let model = scheduler
        .bind(probe.clone(), [pool("account")])
        .unwrap()
        .token_estimator(Arc::new(Estimate(80)))
        .token_settlement(ModelTokenSettlement::ReportedUsage);
    let mut stream = open(&model);
    assert_eq!(scheduler.snapshot().pools[0].reserved_tokens, 80);
    drain(&mut stream);
    assert_eq!(scheduler.snapshot().pools[0].settled_tokens, 50);
    let smaller = scheduler
        .bind(probe.clone(), [pool("account")])
        .unwrap()
        .token_estimator(Arc::new(Estimate(50)));
    let held = open(&smaller);
    time.advance(Duration::from_secs(60));
    let snapshot = scheduler.snapshot();
    assert_eq!(snapshot.pools[0].settled_tokens, 0);
    assert_eq!(snapshot.pools[0].reserved_tokens, 50); // Active reservation never expires.
    drop(held);
    assert_eq!(scheduler.snapshot().pools[0].settled_tokens, 50);
    assert_eq!(scheduler.snapshot().unknown_usage, 1);

    let over = Probe::new([Script::Events(vec![
        Ok(usage(u64::MAX, u64::MAX)),
        Ok(complete()),
    ])]);
    let over = scheduler
        .bind(over, [pool("account")])
        .unwrap()
        .token_estimator(Arc::new(Estimate(1)));
    let mut stream = open(&over);
    assert!(matches!(next(&mut stream), Poll::Ready(Some(Ok(_)))));
    let debt = u128::from(u64::MAX) * 2;
    assert_eq!(scheduler.snapshot().pools[0].reserved_tokens, debt);
    assert!(matches!(next(&mut stream), Poll::Ready(Some(Ok(_)))));
    assert_eq!(scheduler.snapshot().pools[0].settled_tokens, debt + 50);
    assert_eq!(scheduler.snapshot().token_overruns, 1);
    let mut waiting = smaller.stream(request(), ModelCallContext::new());
    assert!(poll(waiting.as_mut()).is_pending());
    time.advance(Duration::from_secs(60));
    assert!(matches!(poll(waiting.as_mut()), Poll::Ready(Ok(_))));
}

#[test]
fn missing_impossible_and_zero_token_estimates_fail_before_dispatch() {
    let (scheduler, _) = scheduler(ModelCapacityLimits::new(nz(1)).tokens(rate(100)), 2);
    let probe = Probe::new([]);
    for estimate in [None, Some(0), Some(101)] {
        let mut model = scheduler.bind(probe.clone(), [pool("account")]).unwrap();
        if let Some(value) = estimate {
            model = model.token_estimator(Arc::new(Estimate(value)));
        }
        let error = futures_executor::block_on(model.stream(request(), ModelCallContext::new()))
            .err()
            .unwrap();
        assert_eq!(error.kind, ModelErrorKind::AdmissionDenied);
    }
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
    assert_eq!(scheduler.snapshot().rejected, 3);
}

#[test]
fn aged_large_waiter_blocks_younger_shared_work_until_it_fits() {
    let (scheduler, time) = scheduler(ModelCapacityLimits::new(nz(3)).tokens(rate(100)), 10);
    let probe = Probe::new([]);
    let small = scheduler
        .bind(probe.clone(), [pool("account")])
        .unwrap()
        .token_estimator(Arc::new(Estimate(20)));
    let big = scheduler
        .bind(probe, [pool("account")])
        .unwrap()
        .token_estimator(Arc::new(Estimate(100)));
    drop(open(&small));
    let mut older = big.stream(request(), ModelCallContext::new());
    assert!(poll(older.as_mut()).is_pending());
    drop(open(&small)); // Feasible younger work can bypass before aging.
    time.advance(Duration::from_secs(30));
    let mut younger = small.stream(request(), ModelCallContext::new());
    assert!(poll(younger.as_mut()).is_pending());
    assert_eq!(scheduler.snapshot().pools[0].active, 0); // Intentional fairness barrier.
    time.advance(Duration::from_secs(30));
    let Poll::Ready(Ok(large)) = poll(older.as_mut()) else {
        panic!("old request starved")
    };
    assert!(poll(younger.as_mut()).is_pending());
    drop(large);
}

#[test]
fn retry_reacquires_per_attempt_and_admission_failures_are_not_retried() {
    let (scheduler, _) = scheduler(ModelCapacityLimits::new(nz(1)), 2);
    let mut safe = ModelError::local(ModelErrorKind::Transport, "safe before dispatch");
    safe.retry_safety = RetrySafety::Safe;
    let probe = Probe::new([Script::Error(safe), Script::Events(vec![Ok(complete())])]);
    let physical = scheduler.bind(probe.clone(), [pool("account")]).unwrap();
    let router = ModelRouter::builder(ModelRef::new("logical", "test"))
        .route("one", Arc::new(physical), ModelRef::new("probe", "test"))
        .retry_policy(ModelRetryPolicy::exponential(2, Duration::ZERO, Duration::ZERO, 1).unwrap())
        .build()
        .unwrap();
    futures_executor::block_on(async {
        let mut routed_request = request();
        routed_request.model = ModelRef::new("logical", "test");
        let mut stream = router
            .stream(routed_request, ModelCallContext::new())
            .await
            .unwrap();
        while let Some(item) = stream.next().await {
            item.unwrap();
        }
    });
    assert_eq!(probe.calls.load(Ordering::SeqCst), 2);
    assert_eq!(scheduler.snapshot().admitted, 2);
    assert_eq!(probe.peak.load(Ordering::SeqCst), 1);
    let policy = ModelRetryPolicy::default().allow_unknown(ModelErrorKind::AdmissionDenied);
    assert!(!policy.permits(&ModelSchedulingError::QueueFull.into()));
}

#[test]
fn real_parallel_callers_wake_without_exceeding_the_shared_ceiling() {
    let (scheduler, _) = scheduler(ModelCapacityLimits::new(nz(4)), 100);
    let probe = Probe::new([]);
    let model = scheduler.bind(probe.clone(), [pool("account")]).unwrap();
    std::thread::scope(|scope| {
        for _ in 0..32 {
            let model = model.clone();
            scope.spawn(move || {
                let stream = open(&model);
                // Independent test executors overlap transports deliberately.
                std::thread::sleep(Duration::from_millis(2));
                drop(stream);
            });
        }
    });
    assert_eq!(probe.calls.load(Ordering::SeqCst), 32);
    assert!(probe.peak.load(Ordering::SeqCst) <= 4);
    assert_eq!(scheduler.snapshot().queued, 0);
    assert_eq!(scheduler.snapshot().pools[0].active, 0);
}

#[test]
fn backoff_holds_no_slot_and_fallback_reacquires_capacity() {
    let (scheduler, time) = scheduler(ModelCapacityLimits::new(nz(1)), 10);
    let mut safe = ModelError::local(ModelErrorKind::Transport, "safe failure");
    safe.retry_safety = RetrySafety::Safe;
    let probe = Probe::new([
        Script::Error(safe.clone()),
        Script::Events(vec![Ok(complete())]),
        Script::Events(vec![Ok(complete())]),
    ]);
    let physical = scheduler.bind(probe.clone(), [pool("account")]).unwrap();
    let router = ModelRouter::builder(ModelRef::new("probe", "test"))
        .route(
            "primary",
            Arc::new(physical.clone()),
            ModelRef::new("probe", "test"),
        )
        .retry_policy(
            ModelRetryPolicy::exponential(2, Duration::from_secs(10), Duration::from_secs(10), 1)
                .unwrap()
                .jitter(crate::RetryJitter::None),
        )
        .sleeper(Arc::new(time.clone()))
        .build()
        .unwrap();
    let mut response_stream = open(&router);
    assert!(next(&mut response_stream).is_pending());
    assert_eq!(scheduler.snapshot().pools[0].active, 0);
    let mut other = open(&physical);
    drain(&mut other);
    time.advance(Duration::from_secs(10));
    drain(&mut response_stream);
    assert_eq!(scheduler.snapshot().admitted, 3);
    assert_eq!(probe.peak.load(Ordering::SeqCst), 1);

    let first = Probe::new([Script::Error(safe)]);
    let second = Probe::new([Script::Events(vec![Ok(complete())])]);
    let router = ModelRouter::builder(ModelRef::new("probe", "test"))
        .route(
            "first",
            Arc::new(scheduler.bind(first, [pool("account")]).unwrap()),
            ModelRef::new("probe", "test"),
        )
        .route(
            "second",
            Arc::new(scheduler.bind(second.clone(), [pool("account")]).unwrap()),
            ModelRef::new("probe", "test"),
        )
        .build()
        .unwrap();
    drain(&mut open(&router));
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    assert_eq!(scheduler.snapshot().pools[0].active, 0);
}

#[test]
fn terminal_usage_missing_usage_and_partial_usage_settle_conservatively() {
    let (scheduler, _) = scheduler(ModelCapacityLimits::new(nz(1)).tokens(rate(100)), 5);
    for (events, expected, unknown) in [
        (vec![Ok(usage(0, 0)), Ok(complete())], 0, 0),
        (vec![Ok(complete())], 30, 1),
        (
            vec![
                Ok(usage(1, 1)),
                Err(ModelError::local(ModelErrorKind::Transport, "lost")),
            ],
            30,
            1,
        ),
    ] {
        let before = scheduler.snapshot();
        let model = scheduler
            .bind(Probe::new([Script::Events(events)]), [pool("account")])
            .unwrap()
            .token_estimator(Arc::new(Estimate(30)))
            .token_settlement(ModelTokenSettlement::ReportedUsage);
        let mut stream = open(&model);
        while let Poll::Ready(Some(_)) = next(&mut stream) {}
        let after = scheduler.snapshot();
        assert_eq!(
            after.pools[0].settled_tokens - before.pools[0].settled_tokens,
            expected
        );
        assert_eq!(after.unknown_usage - before.unknown_usage, unknown);
    }
}

#[test]
fn rate_timer_wakes_without_another_request_or_manual_polling() {
    let config = ModelSchedulerConfig::new(nz(2))
        .pool(
            pool("account"),
            ModelCapacityLimits::new(nz(1))
                .requests(ModelRateLimit::new(NonZeroU64::MIN, Duration::from_millis(5)).unwrap()),
        )
        .unwrap();
    let scheduler = ModelScheduler::new(config).unwrap();
    let model = scheduler.bind(Probe::new([]), [pool("account")]).unwrap();
    drop(open(&model));
    // block_on only wakes from scheduler timers/notifications, not periodic polling.
    drop(open(&model));
    assert_eq!(scheduler.snapshot().admitted, 2);
}

#[test]
fn nested_admission_fails_without_deadlock_or_provider_work() {
    let (scheduler, _) = scheduler(ModelCapacityLimits::new(nz(1)), 2);
    let (independent, _) = self::scheduler(ModelCapacityLimits::new(nz(1)), 2);
    for outer_scheduler in [scheduler.clone(), independent] {
        let probe = Probe::new([]);
        let inner = scheduler.bind(probe.clone(), [pool("account")]).unwrap();
        let outer = outer_scheduler.bind(inner, [pool("account")]).unwrap();
        let error = futures_executor::block_on(outer.stream(request(), ModelCallContext::new()))
            .err()
            .unwrap();
        assert_eq!(error.kind, ModelErrorKind::AdmissionDenied);
        assert_eq!(
            error.metadata["runifold.scheduler.reason"],
            "nested_admission"
        );
        assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
        assert_eq!(scheduler.snapshot().pools[0].active, 0);
        assert_eq!(outer_scheduler.snapshot().pools[0].active, 0);
        assert_eq!(scheduler.snapshot().queued, 0);
    }
}

#[test]
fn admission_denial_cannot_be_bypassed_by_router_fallback() {
    let (scheduler, _) = scheduler(ModelCapacityLimits::new(nz(1)).tokens(rate(100)), 2);
    let first = Probe::new([]);
    let second = Probe::new([Script::Events(vec![Ok(complete())])]);
    let router = ModelRouter::builder(ModelRef::new("probe", "test"))
        .route(
            "limited",
            Arc::new(scheduler.bind(first.clone(), [pool("account")]).unwrap()),
            ModelRef::new("probe", "test"),
        )
        .route(
            "unlimited",
            Arc::new(second.clone()),
            ModelRef::new("probe", "test"),
        )
        .retry_policy(ModelRetryPolicy::default())
        .build()
        .unwrap();
    let mut stream = open(&router);
    assert!(matches!(
        next(&mut stream),
        Poll::Ready(Some(Err(ModelError {
            kind: ModelErrorKind::AdmissionDenied,
            ..
        })))
    ));
    assert_eq!(first.calls.load(Ordering::SeqCst), 0);
    assert_eq!(second.calls.load(Ordering::SeqCst), 0);
    assert_eq!(scheduler.snapshot().rejected, 1);
}

#[test]
fn partial_terminal_usage_cannot_refund_a_reservation_without_explicit_trust() {
    for policy in [
        ModelTokenSettlement::Conservative,
        ModelTokenSettlement::ReportedUsage,
    ] {
        let (scheduler, _) = scheduler(ModelCapacityLimits::new(nz(1)).tokens(rate(100)), 2);
        let probe = Probe::new([Script::Events(vec![Ok(usage(10, 0)), Ok(complete())])]);
        let mut model = scheduler
            .bind(probe, [pool("account")])
            .unwrap()
            .token_estimator(Arc::new(Estimate(100)));
        // Exercise the default, not just an explicit Conservative setter.
        if policy == ModelTokenSettlement::ReportedUsage {
            model = model.token_settlement(policy);
        }
        drain(&mut open(&model));
        let snapshot = scheduler.snapshot();
        assert_eq!(
            snapshot.pools[0].settled_tokens,
            if policy == ModelTokenSettlement::ReportedUsage {
                10
            } else {
                100
            }
        );
        assert_eq!(
            snapshot.unknown_usage,
            u64::from(policy == ModelTokenSettlement::Conservative)
        );
        let mut pending = model.stream(request(), ModelCallContext::new());
        assert!(poll(pending.as_mut()).is_pending());
    }
}

struct ReentrantWake {
    scheduler: ModelScheduler,
    locked_callbacks: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}
impl ReentrantWake {
    fn inspect(&self) {
        let available = self.scheduler.shared.state.try_lock().is_ok();
        if available {
            let _ = self.scheduler.snapshot();
        } else {
            self.locked_callbacks.fetch_add(1, Ordering::SeqCst);
        }
    }
}
impl futures_util::task::ArcWake for ReentrantWake {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.inspect();
    }
}
impl Drop for ReentrantWake {
    fn drop(&mut self) {
        self.inspect();
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn waker_replacement_pruning_admission_and_drop_callbacks_run_without_state_lock() {
    for action in ["replace", "prune", "admit", "drop"] {
        let (scheduler, time) = scheduler(ModelCapacityLimits::new(nz(1)), 4);
        let model = scheduler.bind(Probe::new([]), [pool("account")]).unwrap();
        let held = open(&model);
        let token = CancellationToken::new();
        let mut pending =
            model.stream(request(), ModelCallContext::new().with_cancellation(&token));
        let locked = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let waker = futures_util::task::waker(Arc::new(ReentrantWake {
            scheduler: scheduler.clone(),
            locked_callbacks: locked.clone(),
            dropped: dropped.clone(),
        }));
        assert!(poll(pending.as_mut()).is_pending());
        // Isolate the queue's wake object from cancellation/timer references so
        // replacing or removing it really executes its final destructor.
        let previous = {
            let mut state = scheduler.shared.state.lock().unwrap();
            state
                .waiters
                .values_mut()
                .next()
                .unwrap()
                .waker
                .replace(Arc::new(waker))
        };
        drop(previous);
        match action {
            "replace" => {
                assert!(poll(pending.as_mut()).is_pending());
            }
            "prune" => {
                token.cancel();
                let mut other = model.stream(request(), ModelCallContext::new());
                assert!(poll(other.as_mut()).is_pending());
                drop(other);
            }
            "admit" => {
                drop(held);
                assert!(matches!(poll(pending.as_mut()), Poll::Ready(Ok(_))));
            }
            "drop" => {}
            _ => unreachable!(),
        }
        // Release every fixture reference before asserting final destruction.
        drop(pending);
        drop(token);
        time.advance(Duration::ZERO);
        assert_eq!(locked.load(Ordering::SeqCst), 0, "{action}");
        assert_eq!(dropped.load(Ordering::SeqCst), 1, "{action}");
    }
}

#[path = "tests_lifecycle.rs"]
mod lifecycle;
