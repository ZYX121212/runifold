//! Bounded repeated request lifecycle/resource checks in an isolated test process.
#![cfg(all(feature = "runtime", any(target_os = "linux", target_os = "macos")))]
use futures_util::{StreamExt, stream};
use runifold::{
    Model, ModelCallContext, ModelCapacityLimits, ModelCapacityPoolId, ModelRequest,
    ModelScheduler, ModelSchedulerConfig,
    core::{CancellationToken, RetrySafety},
    model::{
        FinishReason, Message, ModelCapabilities, ModelError, ModelErrorKind, ModelEventStream,
        ModelFuture, ModelRef, ModelStreamEvent, ModelUsage,
    },
};
use std::{
    collections::VecDeque,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

#[derive(Default)]
struct Physical {
    calls: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
}
struct Active(Arc<Physical>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}
struct YieldOnce(bool);
impl Future for YieldOnce {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
struct PhysicalModel(Arc<Physical>);
impl Model for PhysicalModel {
    fn capabilities<'a>(
        &'a self,
        _: &'a ModelRef,
    ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
        Box::pin(async { Ok(ModelCapabilities::default()) })
    }
    fn stream(
        &self,
        _: ModelRequest,
        _: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
        Box::pin(async move {
            let index = self.0.calls.fetch_add(1, Ordering::SeqCst);
            let active = self.0.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.0.peak.fetch_max(active, Ordering::SeqCst);
            let guard = Active(self.0.clone());
            YieldOnce(false).await;
            if index.is_multiple_of(11) {
                return Err(ModelError::local(
                    ModelErrorKind::Transport,
                    "ambiguous connection loss",
                ));
            }
            let mut events = VecDeque::from([ModelStreamEvent::Heartbeat]);
            if !index.is_multiple_of(13) {
                events.push_back(ModelStreamEvent::UsageUpdated {
                    usage: ModelUsage {
                        input_tokens: 11,
                        output_tokens: 7,
                        ..ModelUsage::default()
                    },
                });
                events.push_back(ModelStreamEvent::ResponseCompleted {
                    finish_reason: FinishReason::Stop,
                    provider_metadata: std::collections::BTreeMap::new(),
                });
            }
            Ok(Box::pin(stream::unfold(
                (guard, events),
                |(guard, mut events)| async move {
                    YieldOnce(false).await;
                    events.pop_front().map(|event| (Ok(event), (guard, events)))
                },
            )) as ModelEventStream)
        })
    }
}

fn resident_kib() -> u64 {
    #[cfg(target_os = "linux")]
    {
        let status =
            std::fs::read_to_string("/proc/self/status").expect("process memory is readable");
        status
            .lines()
            .find_map(|line| {
                line.strip_prefix("VmRSS:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
            .expect("kernel reports resident memory")
    }
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .expect("process memory inspector runs");
        assert!(output.status.success());
        String::from_utf8(output.stdout)
            .expect("numeric RSS is UTF-8")
            .trim()
            .parse()
            .expect("RSS is numeric KiB")
    }
}

async fn exercise(model: &impl Model, round: usize) {
    stream::iter(0_usize..512)
        .map(|index| async move {
            let cancellation = CancellationToken::new();
            if index.is_multiple_of(7) {
                cancellation.cancel();
            }
            let request = ModelRequest::new(
                ModelRef::new("test", "stability"),
                Message::user("fixed non-private fixture"),
            );
            let context = ModelCallContext::new().with_cancellation(&cancellation);
            let mut response = match model.stream(request, context).await {
                Ok(response) => response,
                Err(error) => {
                    assert_eq!(
                        error.metadata["runifold.request.local_capacity_released"],
                        true
                    );
                    if index.is_multiple_of(7) {
                        assert_eq!(error.retry_safety, RetrySafety::Safe);
                        assert_eq!(error.metadata["runifold.request.provider_invoked"], false);
                    } else {
                        assert_eq!(error.retry_safety, RetrySafety::Unknown);
                        assert_eq!(error.metadata["runifold.request.remote_outcome"], "unknown");
                    }
                    return;
                }
            };
            let mut terminal = false;
            while let Some(item) = response.next().await {
                match item {
                    Ok(ModelStreamEvent::Heartbeat) if (index + round).is_multiple_of(5) => {
                        cancellation.cancel();
                    }
                    Ok(ModelStreamEvent::ResponseCompleted { .. }) => terminal = true,
                    Err(error) => {
                        assert_eq!(error.retry_safety, RetrySafety::UnsafeAfterVisibleOutput);
                        assert_eq!(
                            error.metadata["runifold.request.local_capacity_released"],
                            true
                        );
                        assert_eq!(error.metadata["runifold.request.remote_outcome"], "unknown");
                        terminal = true;
                    }
                    _ => {}
                }
            }
            assert!(
                terminal,
                "partial output must not become successful completion"
            );
        })
        .buffer_unordered(32)
        .for_each(|()| async {})
        .await;
}

#[test]
fn repeated_failures_cancellations_and_eof_release_capacity_without_unbounded_growth() {
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        sender
            .send(std::panic::catch_unwind(run_stability_fixture))
            .expect("test observer remains alive");
    });
    match receiver.recv_timeout(std::time::Duration::from_secs(30)) {
        Ok(Ok(())) => {}
        Ok(Err(failure)) => std::panic::resume_unwind(failure),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("request fixture exceeded its 30-second deadlock/recovery bound")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("request fixture worker exited without a result")
        }
    }
}

fn run_stability_fixture() {
    let scheduler = ModelScheduler::new(
        ModelSchedulerConfig::new(NonZeroUsize::new(32).unwrap())
            .pool(
                ModelCapacityPoolId::new("stability").unwrap(),
                ModelCapacityLimits::new(NonZeroUsize::new(4).unwrap()),
            )
            .unwrap(),
    )
    .unwrap();
    let physical = Arc::new(Physical::default());
    let model = scheduler
        .bind(
            PhysicalModel(physical.clone()),
            [ModelCapacityPoolId::new("stability").unwrap()],
        )
        .unwrap();
    futures_executor::block_on(exercise(&model, 0));
    let baseline = resident_kib();
    let mut peak = baseline;
    for round in 1..=32 {
        futures_executor::block_on(exercise(&model, round));
        let snapshot = scheduler.snapshot();
        assert_eq!(snapshot.queued, 0);
        assert_eq!(snapshot.pools[0].active, 0);
        assert_eq!(snapshot.pools[0].reserved_tokens, 0);
        assert_eq!(snapshot.pools[0].settled_tokens, 0);
        assert_eq!(physical.active.load(Ordering::SeqCst), 0);
        assert_eq!(
            snapshot.admitted,
            u64::try_from(physical.calls.load(Ordering::SeqCst)).unwrap()
        );
        peak = peak.max(resident_kib());
        assert!(
            peak <= baseline + 8 * 1024,
            "resident growth exceeded the fixed 8 MiB acceptance bound"
        );
    }
    assert_eq!(physical.peak.load(Ordering::SeqCst), 4);
    assert_eq!(
        physical.calls.load(Ordering::SeqCst),
        33 * 438,
        "ambiguous attempts must not be silently repeated"
    );
    println!(
        "{}",
        serde_json::json!({"suite":"request_stability", "result":"passed",
        "requests": 33 * 512, "physical_attempts":physical.calls.load(Ordering::SeqCst),
        "peak_concurrency":4, "resident_baseline_kib":baseline, "resident_peak_kib":peak,
        "maximum_resident_growth_kib":8*1024})
    );
}
