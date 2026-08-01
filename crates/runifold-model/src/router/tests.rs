use std::collections::VecDeque;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use futures_util::stream;
use runifold_core::RetrySafety;

use crate::{
    CircuitBreakerConfig, CircuitState, ContentPart, FeatureSupport, FinishReason, Message, Model,
    ModelCallContext, ModelCapabilities, ModelError, ModelErrorKind, ModelEventStream, ModelFuture,
    ModelRef, ModelRequest, ModelRetryPolicy, ModelStreamEvent, RetryJitter, RouterClock,
    RouterSleepFuture, RouterSleeper, SupportLevel,
};

use super::{ModelFallbackPolicy, ModelRouter, ModelRouterBuildError};

#[derive(Clone)]
enum Script {
    OpenError(ModelError),
    Events(Vec<Result<ModelStreamEvent, ModelError>>),
}

struct TestModel {
    script: Script,
    capabilities: ModelCapabilities,
    calls: AtomicUsize,
    contexts: Mutex<Vec<ModelCallContext>>,
}

struct ManualClock {
    now: Mutex<Instant>,
}

struct SequenceModel {
    scripts: Mutex<VecDeque<Script>>,
    calls: AtomicUsize,
}

impl SequenceModel {
    fn new(scripts: impl IntoIterator<Item = Script>) -> Self {
        Self {
            scripts: Mutex::new(scripts.into_iter().collect()),
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl Model for SequenceModel {
    fn capabilities<'a>(
        &'a self,
        _model: &'a ModelRef,
    ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
        Box::pin(async { Ok(ModelCapabilities::default()) })
    }

    fn stream(
        &self,
        _request: ModelRequest,
        _context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let script = self
            .scripts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
            .expect("test sequence exhausted");
        Box::pin(async move {
            match script {
                Script::OpenError(error) => Err(error),
                Script::Events(events) => Ok(Box::pin(stream::iter(events)) as ModelEventStream),
            }
        })
    }
}

#[derive(Default)]
struct RecordingSleeper {
    delays: Mutex<Vec<Duration>>,
}

impl RecordingSleeper {
    fn delays(&self) -> Vec<Duration> {
        self.delays
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl RouterSleeper for RecordingSleeper {
    fn sleep(&self, duration: Duration) -> RouterSleepFuture<'_> {
        self.delays
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(duration);
        Box::pin(async {})
    }
}

impl ManualClock {
    fn new() -> Self {
        Self {
            now: Mutex::new(Instant::now()),
        }
    }
}

impl RouterClock for ManualClock {
    fn now(&self) -> Instant {
        *self
            .now
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl TestModel {
    fn new(script: Script) -> Self {
        Self {
            script,
            capabilities: ModelCapabilities::default(),
            calls: AtomicUsize::new(0),
            contexts: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn invocation_ids(&self) -> Vec<runifold_core::InvocationId> {
        self.contexts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(ModelCallContext::invocation_id)
            .collect()
    }
}

impl Model for TestModel {
    fn capabilities<'a>(
        &'a self,
        _model: &'a ModelRef,
    ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
        let capabilities = self.capabilities.clone();
        Box::pin(async move { Ok(capabilities) })
    }

    fn stream(
        &self,
        _request: ModelRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.contexts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(context);
        let script = self.script.clone();
        Box::pin(async move {
            match script {
                Script::OpenError(error) => Err(error),
                Script::Events(events) => Ok(Box::pin(stream::iter(events)) as ModelEventStream),
            }
        })
    }
}

fn logical() -> ModelRef {
    ModelRef::new("router", "fast")
}

fn request() -> ModelRequest {
    ModelRequest::new(logical(), Message::user("hello"))
}

fn completed(model: ModelRef, text: &str) -> Vec<Result<ModelStreamEvent, ModelError>> {
    vec![
        Ok(ModelStreamEvent::ResponseStarted {
            id: Some("response".into()),
            model,
        }),
        Ok(ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::text(text),
        }),
        Ok(ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::Stop,
            provider_metadata: std::collections::BTreeMap::new(),
        }),
    ]
}

fn error(kind: ModelErrorKind, retry_safety: RetrySafety) -> ModelError {
    let mut error = ModelError::local(kind, "safe test failure");
    error.retry_safety = retry_safety;
    error
}

fn router(
    primary: Arc<TestModel>,
    backup: Arc<TestModel>,
    policy: ModelFallbackPolicy,
) -> ModelRouter {
    ModelRouter::builder(logical())
        .route("primary", primary, ModelRef::new("primary", "model"))
        .route("backup", backup, ModelRef::new("backup", "model"))
        .fallback_policy(policy)
        .build()
        .unwrap()
}

#[test]
fn safe_open_failure_falls_back_and_records_the_selected_route() {
    let primary = Arc::new(TestModel::new(Script::OpenError(error(
        ModelErrorKind::Transport,
        RetrySafety::Safe,
    ))));
    let backup = Arc::new(TestModel::new(Script::Events(completed(
        ModelRef::new("backup", "model"),
        "ok",
    ))));
    let router = router(
        primary.clone(),
        backup.clone(),
        ModelFallbackPolicy::default(),
    );
    let context = ModelCallContext::new();
    let logical_invocation = context.invocation_id();

    let response = futures_executor::block_on(router.invoke(request(), context)).expect("fallback");

    assert_eq!(primary.calls(), 1);
    assert_eq!(backup.calls(), 1);
    assert_eq!(response.model, ModelRef::new("backup", "model"));
    assert_eq!(response.provider_events.len(), 1);
    assert_eq!(response.provider_events[0].provider, "runifold.router");
    assert_eq!(response.provider_events[0].kind, "route.selected");
    assert_eq!(
        response.provider_events[0].value["prior_failures"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let primary_id = primary.invocation_ids()[0];
    let backup_id = backup.invocation_ids()[0];
    assert_ne!(primary_id, logical_invocation);
    assert_ne!(backup_id, logical_invocation);
    assert_ne!(primary_id, backup_id);
}

#[test]
fn unknown_retry_safety_requires_explicit_authority() {
    let primary = Arc::new(TestModel::new(Script::OpenError(error(
        ModelErrorKind::Transport,
        RetrySafety::Unknown,
    ))));
    let backup = Arc::new(TestModel::new(Script::Events(completed(
        ModelRef::new("backup", "model"),
        "ok",
    ))));
    let router = router(primary, backup.clone(), ModelFallbackPolicy::default());

    let error =
        futures_executor::block_on(router.invoke(request(), ModelCallContext::new())).unwrap_err();

    assert_eq!(backup.calls(), 0);
    assert_eq!(
        error.metadata["runifold.router.failures"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn policy_can_authorize_unknown_transport_fallback() {
    let primary = Arc::new(TestModel::new(Script::OpenError(error(
        ModelErrorKind::Transport,
        RetrySafety::Unknown,
    ))));
    let backup = Arc::new(TestModel::new(Script::Events(completed(
        ModelRef::new("backup", "model"),
        "ok",
    ))));
    let policy = ModelFallbackPolicy::safe_only().allow_unknown(ModelErrorKind::Transport);
    let router = router(primary, backup.clone(), policy);

    futures_executor::block_on(router.invoke(request(), ModelCallContext::new())).unwrap();

    assert_eq!(backup.calls(), 1);
}

#[test]
fn failure_after_the_first_event_never_falls_back() {
    let primary = Arc::new(TestModel::new(Script::Events(vec![
        Ok(ModelStreamEvent::ResponseStarted {
            id: Some("started".into()),
            model: ModelRef::new("primary", "model"),
        }),
        Err(error(ModelErrorKind::Transport, RetrySafety::Safe)),
    ])));
    let backup = Arc::new(TestModel::new(Script::Events(completed(
        ModelRef::new("backup", "model"),
        "duplicate",
    ))));
    let router = router(primary, backup.clone(), ModelFallbackPolicy::default());

    let error =
        futures_executor::block_on(router.invoke(request(), ModelCallContext::new())).unwrap_err();

    assert_eq!(error.retry_safety, RetrySafety::UnsafeAfterVisibleOutput);
    assert_eq!(backup.calls(), 0);
}

#[test]
fn cancellation_never_falls_back_even_if_marked_safe() {
    let primary = Arc::new(TestModel::new(Script::OpenError(error(
        ModelErrorKind::Cancelled,
        RetrySafety::Safe,
    ))));
    let backup = Arc::new(TestModel::new(Script::Events(completed(
        ModelRef::new("backup", "model"),
        "late",
    ))));
    let router = router(primary, backup.clone(), ModelFallbackPolicy::default());

    let error =
        futures_executor::block_on(router.invoke(request(), ModelCallContext::new())).unwrap_err();

    assert_eq!(error.kind, ModelErrorKind::Cancelled);
    assert_eq!(backup.calls(), 0);
}

#[test]
fn preexisting_cancellation_stops_before_any_route() {
    let primary = Arc::new(TestModel::new(Script::Events(completed(
        ModelRef::new("primary", "model"),
        "late",
    ))));
    let backup = Arc::new(TestModel::new(Script::Events(completed(
        ModelRef::new("backup", "model"),
        "later",
    ))));
    let router = router(
        primary.clone(),
        backup.clone(),
        ModelFallbackPolicy::default(),
    );
    let context = ModelCallContext::new();
    context.cancellation().cancel();

    let error = futures_executor::block_on(router.invoke(request(), context)).unwrap_err();

    assert_eq!(error.kind, ModelErrorKind::Cancelled);
    assert_eq!(primary.calls(), 0);
    assert_eq!(backup.calls(), 0);
}

#[test]
fn capabilities_are_the_safe_intersection_of_all_routes() {
    let native = ModelCapabilities {
        streaming: FeatureSupport::new(SupportLevel::Native),
        max_context_tokens: Some(128_000),
        ..ModelCapabilities::default()
    };
    let emulated = ModelCapabilities {
        streaming: FeatureSupport::new(SupportLevel::Emulated),
        max_context_tokens: Some(32_000),
        ..native.clone()
    };
    let primary = Arc::new(TestModel {
        capabilities: native,
        ..TestModel::new(Script::Events(Vec::new()))
    });
    let backup = Arc::new(TestModel {
        capabilities: emulated,
        ..TestModel::new(Script::Events(Vec::new()))
    });
    let router = router(primary, backup, ModelFallbackPolicy::default());

    let capabilities = futures_executor::block_on(router.capabilities(&logical())).unwrap();

    assert_eq!(capabilities.streaming.level, SupportLevel::Emulated);
    assert_eq!(capabilities.max_context_tokens, Some(32_000));
}

#[test]
fn incompatible_capability_constraints_degrade_to_unknown() {
    let mut first_support = FeatureSupport::new(SupportLevel::Native);
    first_support
        .constraints
        .insert("mode".into(), serde_json::json!("a"));
    let first = ModelCapabilities {
        tools: first_support,
        ..ModelCapabilities::default()
    };
    let mut second_support = FeatureSupport::new(SupportLevel::Native);
    second_support
        .constraints
        .insert("mode".into(), serde_json::json!("b"));
    let second = ModelCapabilities {
        tools: second_support,
        ..ModelCapabilities::default()
    };
    let primary = Arc::new(TestModel {
        capabilities: first,
        ..TestModel::new(Script::Events(Vec::new()))
    });
    let backup = Arc::new(TestModel {
        capabilities: second,
        ..TestModel::new(Script::Events(Vec::new()))
    });
    let router = router(primary, backup, ModelFallbackPolicy::default());

    let capabilities = futures_executor::block_on(router.capabilities(&logical())).unwrap();

    assert_eq!(capabilities.tools.level, SupportLevel::Unknown);
    assert!(capabilities.tools.constraints.is_empty());
}

#[test]
fn open_circuit_skips_the_unhealthy_route_without_calling_it() {
    let primary = Arc::new(TestModel::new(Script::OpenError(error(
        ModelErrorKind::Transport,
        RetrySafety::Safe,
    ))));
    let backup = Arc::new(TestModel::new(Script::Events(completed(
        ModelRef::new("backup", "model"),
        "ok",
    ))));
    let router = ModelRouter::builder(logical())
        .route(
            "primary",
            primary.clone(),
            ModelRef::new("primary", "model"),
        )
        .route("backup", backup.clone(), ModelRef::new("backup", "model"))
        .circuit_breaker(CircuitBreakerConfig::new(1, Duration::from_secs(30)).unwrap())
        .clock(Arc::new(ManualClock::new()))
        .build()
        .unwrap();

    futures_executor::block_on(router.invoke(request(), ModelCallContext::new())).unwrap();
    let second =
        futures_executor::block_on(router.invoke(request(), ModelCallContext::new())).unwrap();

    assert_eq!(primary.calls(), 1);
    assert_eq!(backup.calls(), 2);
    assert_eq!(router.route_health()[0].state, CircuitState::Open);
    assert_eq!(
        second.provider_events[0].value["prior_failures"][0]["kind"],
        "circuit_open"
    );
}

#[test]
fn cloned_router_shares_circuit_state() {
    let primary = Arc::new(TestModel::new(Script::OpenError(error(
        ModelErrorKind::Transport,
        RetrySafety::Safe,
    ))));
    let backup = Arc::new(TestModel::new(Script::Events(completed(
        ModelRef::new("backup", "model"),
        "ok",
    ))));
    let router = ModelRouter::builder(logical())
        .route(
            "primary",
            primary.clone(),
            ModelRef::new("primary", "model"),
        )
        .route("backup", backup.clone(), ModelRef::new("backup", "model"))
        .circuit_breaker(CircuitBreakerConfig::new(1, Duration::from_secs(30)).unwrap())
        .clock(Arc::new(ManualClock::new()))
        .build()
        .unwrap();
    let shared = router.clone();

    futures_executor::block_on(router.invoke(request(), ModelCallContext::new())).unwrap();
    futures_executor::block_on(shared.invoke(request(), ModelCallContext::new())).unwrap();

    assert_eq!(primary.calls(), 1);
    assert_eq!(backup.calls(), 2);
    assert_eq!(shared.route_health()[0].state, CircuitState::Open);
}

#[test]
fn safe_failure_retries_the_same_route_before_fallback() {
    let model = Arc::new(SequenceModel::new([
        Script::OpenError(error(ModelErrorKind::Transport, RetrySafety::Safe)),
        Script::Events(completed(ModelRef::new("primary", "model"), "ok")),
    ]));
    let retry = ModelRetryPolicy::exponential(2, Duration::ZERO, Duration::ZERO, 1).unwrap();
    let router = ModelRouter::builder(logical())
        .route("primary", model.clone(), ModelRef::new("primary", "model"))
        .retry_policy(retry)
        .build()
        .unwrap();

    let response =
        futures_executor::block_on(router.invoke(request(), ModelCallContext::new())).unwrap();

    assert_eq!(model.calls(), 2);
    assert_eq!(response.provider_events[0].value["route_attempt"], 2);
    assert_eq!(
        response.provider_events[0].value["prior_failures"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn retry_after_overrides_shorter_local_backoff() {
    let mut rate_limit = error(ModelErrorKind::Provider, RetrySafety::Safe);
    rate_limit
        .metadata
        .insert("retry.after_ms".into(), serde_json::json!(250));
    let model = Arc::new(SequenceModel::new([
        Script::OpenError(rate_limit),
        Script::Events(completed(ModelRef::new("primary", "model"), "ok")),
    ]));
    let sleeper = Arc::new(RecordingSleeper::default());
    let retry =
        ModelRetryPolicy::exponential(2, Duration::from_millis(100), Duration::from_millis(100), 1)
            .unwrap()
            .jitter(RetryJitter::None);
    let router = ModelRouter::builder(logical())
        .route("primary", model, ModelRef::new("primary", "model"))
        .retry_policy(retry)
        .sleeper(sleeper.clone())
        .build()
        .unwrap();

    futures_executor::block_on(router.invoke(request(), ModelCallContext::new())).unwrap();

    assert_eq!(sleeper.delays(), vec![Duration::from_millis(250)]);
}

#[test]
fn retry_is_truncated_before_it_can_cross_the_deadline() {
    let model = Arc::new(SequenceModel::new([
        Script::OpenError(error(ModelErrorKind::Transport, RetrySafety::Safe)),
        Script::Events(completed(ModelRef::new("primary", "model"), "must not run")),
    ]));
    let sleeper = Arc::new(RecordingSleeper::default());
    let retry = ModelRetryPolicy::exponential(2, Duration::from_secs(1), Duration::from_secs(1), 1)
        .unwrap()
        .jitter(RetryJitter::None);
    let router = ModelRouter::builder(logical())
        .route("primary", model.clone(), ModelRef::new("primary", "model"))
        .retry_policy(retry)
        .sleeper(sleeper.clone())
        .build()
        .unwrap();
    let context =
        ModelCallContext::new().with_deadline(Instant::now() + Duration::from_millis(100));

    let error = futures_executor::block_on(router.invoke(request(), context)).unwrap_err();

    assert_eq!(error.kind, ModelErrorKind::DeadlineExceeded);
    assert_eq!(model.calls(), 1);
    assert!(sleeper.delays().is_empty());
}

#[test]
fn retry_never_occurs_after_the_stream_commit_point() {
    let model = Arc::new(SequenceModel::new([
        Script::Events(vec![
            Ok(ModelStreamEvent::ResponseStarted {
                id: Some("started".into()),
                model: ModelRef::new("primary", "model"),
            }),
            Err(error(ModelErrorKind::Transport, RetrySafety::Safe)),
        ]),
        Script::Events(completed(ModelRef::new("primary", "model"), "duplicate")),
    ]));
    let retry = ModelRetryPolicy::exponential(2, Duration::ZERO, Duration::ZERO, 1).unwrap();
    let router = ModelRouter::builder(logical())
        .route("primary", model.clone(), ModelRef::new("primary", "model"))
        .retry_policy(retry)
        .build()
        .unwrap();

    let error =
        futures_executor::block_on(router.invoke(request(), ModelCallContext::new())).unwrap_err();

    assert_eq!(error.retry_safety, RetrySafety::UnsafeAfterVisibleOutput);
    assert_eq!(model.calls(), 1);
}

#[test]
fn builder_rejects_empty_and_duplicate_routes() {
    let no_routes = ModelRouter::builder(logical()).build().unwrap_err();
    assert_eq!(no_routes, ModelRouterBuildError::NoRoutes);

    let model = Arc::new(TestModel::new(Script::Events(Vec::new())));
    let duplicate = ModelRouter::builder(logical())
        .route("same", model.clone(), ModelRef::new("a", "one"))
        .route("same", model, ModelRef::new("b", "two"))
        .build()
        .unwrap_err();
    assert_eq!(
        duplicate,
        ModelRouterBuildError::DuplicateRoute("same".into())
    );
}
