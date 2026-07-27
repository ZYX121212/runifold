//! Deterministic helpers for testing Runifold runtime semantics.

mod evaluation;
mod evaluation_scorers;
mod evaluation_store;

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use runifold_core::{
    Budget, BudgetTracker, CapabilitySet, EventFactory, InMemoryJournal, LifecycleEvent,
    RunContext, RunEvent, RunEventKind,
};
use runifold_model::{
    Model, ModelCallContext, ModelCapabilities, ModelError, ModelErrorKind, ModelEventStream,
    ModelFuture, ModelRef, ModelRequest, ModelResponse, ModelStreamAccumulator, ModelStreamEvent,
};
use serde_json::Value;

pub use evaluation::{
    EvaluationCase, EvaluationCaseId, EvaluationCaseResult, EvaluationDataset, EvaluationError,
    EvaluationFailure, EvaluationFailureStage, EvaluationFuture, EvaluationMetrics,
    EvaluationOutput, EvaluationReport, EvaluationRunner, EvaluationScore, EvaluationScoreSummary,
    EvaluationScorer, EvaluationTarget, FnScorer, JsonExactMatchScorer, MetricRegression,
    RegressionComparison, RegressionPolicy, ScoreValue,
};
pub use evaluation_scorers::{
    JsonRule, JsonRuleScorer, JudgeRubric, ModelJudgeScorer, TokenOverlapScorer, WeightedJsonRule,
};
pub use evaluation_store::{EvaluationRepository, EvaluationStoreError, FileEvaluationRepository};

type ModelScript = Result<Vec<ModelStreamEvent>, ModelError>;
type ModelScriptQueue = Arc<Mutex<VecDeque<ModelScript>>>;

/// An isolated root run with deterministic event collection.
#[derive(Debug)]
pub struct RunScenario {
    context: RunContext,
    events: EventFactory,
    journal: InMemoryJournal,
}

impl RunScenario {
    /// Creates a scenario with an empty capability set.
    pub fn new(budget: Budget) -> Self {
        let context = RunContext::root(BudgetTracker::new(budget), CapabilitySet::new());
        let events = EventFactory::new(context.run_id(), context.parent_run_id());
        Self {
            context,
            events,
            journal: InMemoryJournal::new(),
        }
    }

    /// Returns the scenario's run context.
    pub const fn context(&self) -> &RunContext {
        &self.context
    }

    /// Records a run-start event.
    pub fn start(&self) -> RunEvent {
        let event = self
            .events
            .emit(RunEventKind::Lifecycle(LifecycleEvent::Started), None);
        self.journal.push(event.clone());
        event
    }

    /// Records a successful terminal event caused by a prior event.
    pub fn complete(&self, output: Value, caused_by: &RunEvent) -> RunEvent {
        let event = self.events.emit(
            RunEventKind::Lifecycle(LifecycleEvent::Completed { output }),
            Some(caused_by.meta.event_id),
        );
        self.journal.push(event.clone());
        event
    }

    /// Returns a snapshot of recorded events.
    pub fn recorded_events(&self) -> Vec<RunEvent> {
        self.journal.events()
    }
}

/// Accumulates a scripted provider event sequence without network access.
#[derive(Clone, Debug, Default)]
pub struct ModelScenario {
    events: Vec<ModelStreamEvent>,
}

impl ModelScenario {
    /// Creates an empty model scenario.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a provider-neutral stream event.
    #[must_use]
    pub fn then(mut self, event: ModelStreamEvent) -> Self {
        self.events.push(event);
        self
    }

    /// Reconstructs the scenario's canonical response.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError`] if the scripted stream violates lifecycle or
    /// content-block rules, or if it never produces a terminal response.
    pub fn response(self) -> Result<ModelResponse, ModelError> {
        let mut accumulator = ModelStreamAccumulator::new();
        for event in self.events {
            if let Some(response) = accumulator.push(event)? {
                return Ok(response);
            }
        }
        Err(ModelError::local(
            runifold_model::ModelErrorKind::StreamState,
            "scripted model stream did not complete",
        ))
    }
}

/// A deterministic, queue-backed model adapter.
///
/// Each invocation consumes exactly one queued script. This makes retry,
/// fallback, and multi-turn behavior explicit in tests.
#[derive(Clone, Debug, Default)]
pub struct ScriptedModel {
    capabilities: ModelCapabilities,
    scripts: ModelScriptQueue,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
    contexts: Arc<Mutex<Vec<ModelCallContext>>>,
}

impl ScriptedModel {
    /// Creates a scripted model with unknown capabilities.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the capabilities returned by discovery calls.
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: ModelCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Queues one successful canonical event stream.
    pub fn enqueue(&self, events: impl IntoIterator<Item = ModelStreamEvent>) {
        self.scripts()
            .push_back(Ok(events.into_iter().collect::<Vec<_>>()));
    }

    /// Queues an error returned while opening the next stream.
    pub fn enqueue_error(&self, error: ModelError) {
        self.scripts().push_back(Err(error));
    }

    /// Returns canonical requests observed by this model.
    pub fn recorded_requests(&self) -> Vec<ModelRequest> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Returns invocation contexts observed by this model.
    pub fn recorded_contexts(&self) -> Vec<ModelCallContext> {
        self.contexts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn scripts(&self) -> std::sync::MutexGuard<'_, VecDeque<ModelScript>> {
        self.scripts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Model for ScriptedModel {
    fn capabilities<'a>(
        &'a self,
        _model: &'a ModelRef,
    ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
        let capabilities = self.capabilities.clone();
        Box::pin(async move { Ok(capabilities) })
    }

    fn stream(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(request);
        self.contexts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(context);
        let script = self.scripts().pop_front().unwrap_or_else(|| {
            Err(ModelError::local(
                ModelErrorKind::Protocol,
                "scripted model has no queued invocation",
            ))
        });
        Box::pin(async move {
            let events = script?;
            Ok(
                Box::pin(futures_util::stream::iter(events.into_iter().map(Ok)))
                    as ModelEventStream,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use runifold_core::Budget;
    use runifold_model::{
        ContentBlockKind, ContentPart, FinishReason, Message, Model, ModelCallContext,
        ModelErrorKind, ModelRef, ModelRequest, ModelStreamEvent,
    };

    use super::{ModelScenario, RunScenario, ScriptedModel};

    #[test]
    fn scenario_records_a_causal_lifecycle() {
        let scenario = RunScenario::new(Budget::default());
        let started = scenario.start();
        let completed = scenario.complete(serde_json::json!({"ok": true}), &started);
        let events = scenario.recorded_events();

        assert_eq!(events, vec![started.clone(), completed.clone()]);
        assert_eq!(completed.meta.caused_by, Some(started.meta.event_id));
    }

    #[test]
    fn model_scenario_accumulates_a_response() {
        let response = ModelScenario::new()
            .then(ModelStreamEvent::ResponseStarted {
                id: Some("test-response".into()),
                model: ModelRef::new("test", "scripted"),
            })
            .then(ModelStreamEvent::ContentBlockStarted {
                index: 0,
                kind: ContentBlockKind::Text,
            })
            .then(ModelStreamEvent::TextDelta {
                index: 0,
                text: "hello".into(),
            })
            .then(ModelStreamEvent::ContentBlockCompleted { index: 0 })
            .then(ModelStreamEvent::ResponseCompleted {
                finish_reason: FinishReason::Stop,
                provider_metadata: BTreeMap::new(),
            })
            .response()
            .unwrap();

        assert_eq!(response.content, vec![ContentPart::text("hello")]);
    }

    #[test]
    fn scripted_model_uses_the_same_stream_accumulator_as_real_adapters() {
        let model = ScriptedModel::new();
        model.enqueue([
            ModelStreamEvent::ResponseStarted {
                id: Some("test-response".into()),
                model: ModelRef::new("test", "scripted"),
            },
            ModelStreamEvent::ContentPartCompleted {
                index: 0,
                part: ContentPart::text("hello"),
            },
            ModelStreamEvent::ResponseCompleted {
                finish_reason: FinishReason::Stop,
                provider_metadata: BTreeMap::new(),
            },
        ]);
        let request = ModelRequest::new(ModelRef::new("test", "scripted"), Message::user("hello"));

        let response = futures_executor::block_on(model.invoke(request, ModelCallContext::new()))
            .expect("script should complete");

        assert_eq!(response.content, vec![ContentPart::text("hello")]);
    }

    #[test]
    fn scripted_model_invocation_observes_preexisting_cancellation() {
        let model = ScriptedModel::new();
        model.enqueue([]);
        let request = ModelRequest::new(ModelRef::new("test", "scripted"), Message::user("hello"));
        let context = ModelCallContext::new();
        context.cancellation().cancel();

        let error = futures_executor::block_on(model.invoke(request, context))
            .expect_err("cancelled invocation must fail");

        assert_eq!(error.kind, ModelErrorKind::Cancelled);
    }
}
