//! Lifecycle regressions, including cancellation/deadline changes inside a physical poll.

use super::*;

#[test]
fn cancellation_deadlines_and_abandonment_stop_uncooperative_transports() {
    for script in [Script::Hold, Script::OpenHold] {
        for deadline in [false, true] {
            let (scheduler, time) = scheduler(ModelCapacityLimits::new(nz(1)), 2);
            let probe = Probe::new([script.clone()]);
            let model = scheduler.bind(probe.clone(), [pool("account")]).unwrap();
            let cancellation = CancellationToken::new();
            let mut context = ModelCallContext::new().with_cancellation(&cancellation);
            if deadline {
                context = context.with_deadline(time.now() + Duration::from_secs(1));
            }
            let mut opening = model.stream(request(), context);
            let mut stream = match poll(opening.as_mut()) {
                Poll::Ready(Ok(s)) => Some(s),
                Poll::Pending => None,
                Poll::Ready(_) => panic!("unexpected open result"),
            };
            if let Some(stream) = &mut stream {
                assert!(next(stream).is_pending());
            }
            if deadline {
                time.advance(Duration::from_secs(1));
            } else {
                cancellation.cancel();
            }
            let error = if let Some(stream) = &mut stream {
                match next(stream) {
                    Poll::Ready(Some(Err(e))) => e,
                    _ => panic!("stream did not stop"),
                }
            } else {
                match poll(opening.as_mut()) {
                    Poll::Ready(Err(e)) => e,
                    _ => panic!("opening did not stop"),
                }
            };
            assert_eq!(
                error.kind,
                if deadline {
                    ModelErrorKind::DeadlineExceeded
                } else {
                    ModelErrorKind::Cancelled
                }
            );
            assert_eq!(scheduler.snapshot().pools[0].active, 0);
            assert_eq!(
                error.metadata["runifold.request.local_capacity_released"],
                true
            );
            assert_eq!(error.metadata["runifold.request.provider_invoked"], true);
            assert_eq!(error.metadata["runifold.request.remote_outcome"], "unknown");
            assert_eq!(
                error.metadata["runifold.request.phase"],
                if stream.is_some() {
                    "waiting_first_event"
                } else {
                    "opening"
                }
            );
            assert_eq!(probe.cancelled_drops.load(Ordering::SeqCst), 1);
            assert!(!cancellation.is_cancelled() || !deadline);
        }
        let (scheduler, _) = scheduler(ModelCapacityLimits::new(nz(1)), 2);
        let probe = Probe::new([script]);
        let model = scheduler.bind(probe.clone(), [pool("account")]).unwrap();
        let mut opening = model.stream(request(), ModelCallContext::new());
        match poll(opening.as_mut()) {
            Poll::Ready(Ok(stream)) => drop(stream),
            Poll::Pending => {}
            Poll::Ready(_) => panic!("open failed"),
        }
        drop(opening);
        assert_eq!(scheduler.snapshot().pools[0].active, 0);
        assert_eq!(probe.cancelled_drops.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn diagnostics_separate_queue_wait_from_execution_and_preserve_unknown_cost() {
    let (scheduler, time) = scheduler(ModelCapacityLimits::new(nz(1)), 2);
    let model = scheduler.bind(Probe::new([]), [pool("account")]).unwrap();
    let held = open(&model);
    let cancellation = CancellationToken::new();
    let mut waiting = model.stream(
        request(),
        ModelCallContext::new().with_cancellation(&cancellation),
    );
    assert!(poll(waiting.as_mut()).is_pending());
    time.advance(Duration::from_millis(2_500));
    drop(held);
    let Poll::Ready(Ok(mut stream)) = poll(waiting.as_mut()) else {
        panic!("released capacity should admit the queued request");
    };
    assert!(next(&mut stream).is_pending());
    time.advance(Duration::from_millis(1_500));
    cancellation.cancel();
    let Poll::Ready(Some(Err(error))) = next(&mut stream) else {
        panic!("cancelled stream should terminate");
    };
    assert_eq!(error.metadata["runifold.request.queue_wait_ms"], 2_500);
    assert_eq!(error.metadata["runifold.request.execution_ms"], 1_500);
    assert_eq!(error.metadata["runifold.request.remote_outcome"], "unknown");
    assert_eq!(error.retry_safety, RetrySafety::Unknown);
    assert_eq!(scheduler.snapshot().pools[0].active, 0);
}

#[test]
fn cancelled_queued_request_reports_no_provider_invocation() {
    let (scheduler, time) = scheduler(ModelCapacityLimits::new(nz(1)), 2);
    let probe = Probe::new([]);
    let model = scheduler.bind(probe.clone(), [pool("account")]).unwrap();
    let held = open(&model);
    let cancellation = CancellationToken::new();
    let mut waiting = model.stream(
        request(),
        ModelCallContext::new().with_cancellation(&cancellation),
    );
    assert!(poll(waiting.as_mut()).is_pending());
    time.advance(Duration::from_millis(750));
    cancellation.cancel();
    let Poll::Ready(Err(error)) = poll(waiting.as_mut()) else {
        panic!("queued cancellation should terminate");
    };
    assert_eq!(error.metadata["runifold.request.phase"], "admission");
    assert_eq!(error.metadata["runifold.request.provider_invoked"], false);
    assert_eq!(
        error.metadata["runifold.request.remote_outcome"],
        "not_started"
    );
    assert_eq!(error.metadata["runifold.request.queue_wait_ms"], 750);
    assert_eq!(error.metadata["runifold.request.execution_ms"], 0);
    assert_eq!(error.retry_safety, RetrySafety::Safe);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    drop(held);
}

#[test]
fn failed_preflight_does_not_report_estimation_time_as_queue_wait() {
    struct RejectingEstimator(Time);
    impl ModelTokenEstimator for RejectingEstimator {
        fn estimate(&self, _: &ModelRequest) -> Result<u64, ModelError> {
            self.0.advance(Duration::from_secs(5));
            Err(ModelError::local(
                ModelErrorKind::InvalidRequest,
                "reservation unavailable",
            ))
        }
    }
    let (scheduler, time) = scheduler(ModelCapacityLimits::new(nz(1)), 2);
    let probe = Probe::new([]);
    let model = scheduler
        .bind(probe.clone(), [pool("account")])
        .unwrap()
        .token_estimator(Arc::new(RejectingEstimator(time)));
    let error = futures_executor::block_on(model.stream(request(), ModelCallContext::new()))
        .err()
        .unwrap();
    assert_eq!(error.metadata["runifold.request.queue_wait_ms"], 0);
    assert_eq!(error.metadata["runifold.request.execution_ms"], 0);
    assert_eq!(error.metadata["runifold.request.provider_invoked"], false);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
}

#[test]
fn eof_errors_and_visible_output_preserve_retry_safety() {
    for events in [
        vec![],
        vec![Err(ModelError::local(ModelErrorKind::Transport, "lost"))],
        vec![
            Ok(ModelStreamEvent::Heartbeat),
            Err(ModelError::local(ModelErrorKind::Transport, "lost")),
        ],
    ] {
        let (scheduler, _) = scheduler(ModelCapacityLimits::new(nz(1)).tokens(rate(100)), 2);
        let probe = Probe::new([Script::Events(events.clone())]);
        let model = scheduler
            .bind(probe, [pool("account")])
            .unwrap()
            .token_estimator(Arc::new(Estimate(40)));
        let mut stream = open(&model);
        let mut error = None;
        while let Poll::Ready(Some(item)) = next(&mut stream) {
            if let Err(e) = item {
                error = Some(e);
            }
        }
        let error = error.unwrap();
        if events.len() > 1 {
            assert_eq!(error.retry_safety, RetrySafety::UnsafeAfterVisibleOutput);
            assert_eq!(error.metadata["runifold.request.phase"], "streaming");
        } else {
            assert_eq!(
                error.metadata["runifold.request.phase"],
                "waiting_first_event"
            );
        }
        assert_eq!(scheduler.snapshot().pools[0].active, 0);
        assert_eq!(scheduler.snapshot().pools[0].settled_tokens, 40);
        drop(stream);
        assert_eq!(scheduler.snapshot().unknown_usage, 1);
    }
}

#[derive(Clone, Copy)]
enum BoundaryStage {
    Opening,
    OpeningError,
    PendingPoll,
    UsagePoll,
}
struct BoundaryModel {
    stage: BoundaryStage,
    time: Time,
    expire: bool,
    stopped_drops: Arc<AtomicUsize>,
}
struct BoundaryStream {
    stage: BoundaryStage,
    time: Time,
    expire: bool,
    context: ModelCallContext,
    stopped_drops: Arc<AtomicUsize>,
}
impl BoundaryStream {
    fn trigger(&self) {
        if self.expire {
            self.time.advance(Duration::from_secs(2));
        } else {
            self.context.cancellation().cancel();
        }
    }
}
impl Model for BoundaryModel {
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
            let stream = BoundaryStream {
                stage: self.stage,
                time: self.time.clone(),
                expire: self.expire,
                context,
                stopped_drops: self.stopped_drops.clone(),
            };
            if matches!(
                self.stage,
                BoundaryStage::Opening | BoundaryStage::OpeningError
            ) {
                stream.trigger();
            }
            if matches!(self.stage, BoundaryStage::OpeningError) {
                let mut error = ModelError::local(ModelErrorKind::Transport, "safe open error");
                error.retry_safety = RetrySafety::Safe;
                return Err(error);
            }
            Ok(Box::pin(stream) as ModelEventStream)
        })
    }
}
impl Stream for BoundaryStream {
    type Item = Result<ModelStreamEvent, ModelError>;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.trigger();
        if matches!(self.stage, BoundaryStage::UsagePoll) {
            Poll::Ready(Some(Ok(usage(64, 64))))
        } else {
            Poll::Pending
        }
    }
}
impl Drop for BoundaryStream {
    fn drop(&mut self) {
        if self.context.cancellation().is_cancelled() {
            self.stopped_drops.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[test]
fn lifecycle_changes_inside_provider_operations_stop_before_returning_results() {
    for stage in [
        BoundaryStage::Opening,
        BoundaryStage::OpeningError,
        BoundaryStage::PendingPoll,
        BoundaryStage::UsagePoll,
    ] {
        for expire in [false, true] {
            let (scheduler, time) = scheduler(ModelCapacityLimits::new(nz(1)).tokens(rate(100)), 2);
            let stopped_drops = Arc::new(AtomicUsize::new(0));
            let model = scheduler
                .bind(
                    BoundaryModel {
                        stage,
                        time: time.clone(),
                        expire,
                        stopped_drops: stopped_drops.clone(),
                    },
                    [pool("account")],
                )
                .unwrap()
                .token_estimator(Arc::new(Estimate(20)));
            let context =
                ModelCallContext::new().with_deadline(time.now() + Duration::from_secs(1));
            let error = match futures_executor::block_on(model.stream(request(), context)) {
                Err(error) => error,
                Ok(mut stream) => {
                    let Poll::Ready(Some(Err(error))) = next(&mut stream) else {
                        panic!("lifecycle race escaped the physical poll")
                    };
                    error
                }
            };
            assert_eq!(
                error.kind,
                if expire {
                    ModelErrorKind::DeadlineExceeded
                } else {
                    ModelErrorKind::Cancelled
                }
            );
            assert_eq!(error.retry_safety, RetrySafety::Unknown);
            let snapshot = scheduler.snapshot();
            assert_eq!(snapshot.pools[0].active, 0);
            assert_eq!(
                snapshot.pools[0].settled_tokens,
                if matches!(stage, BoundaryStage::UsagePoll) {
                    128
                } else {
                    20
                }
            );
            assert_eq!(
                stopped_drops.load(Ordering::SeqCst),
                usize::from(!(expire && matches!(stage, BoundaryStage::OpeningError)))
            );
        }
    }
}
