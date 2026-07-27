use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
};

use opentelemetry::{
    Context, KeyValue, global,
    global::BoxedTracer,
    metrics::Meter,
    trace::{SpanKind, Status, TraceContextExt, Tracer},
};
use runifold_core::{
    ChildEvent, DomainEvent, Journal, JournalError, LifecycleEvent, RunEvent, RunEventKind, RunId,
};

use crate::{
    CorrelationRegistry,
    journal_metrics::{ActiveOperation, JournalInstruments, run_error_type},
};

const SCOPE: &str = "runifold.observability.otel";

/// A [`Journal`] decorator that derives agent, tool, delegation, and workflow
/// spans from durable runtime events.
///
/// The wrapped journal always receives an event before telemetry is updated, so
/// observability cannot make an accepted event appear durable.
pub struct OtelJournal<J> {
    inner: J,
    tracer: Arc<BoxedTracer>,
    state: Mutex<JournalState>,
    correlation: Arc<CorrelationRegistry>,
}

impl<J> OtelJournal<J> {
    /// Wraps a journal using the globally configured tracer provider.
    pub fn new(inner: J) -> Self {
        Self::from_tracer(inner, global::tracer(SCOPE))
    }

    /// Wraps a journal with an explicitly injected tracer.
    pub fn from_tracer(inner: J, tracer: BoxedTracer) -> Self {
        let meter = global::meter(SCOPE);
        Self {
            inner,
            tracer: Arc::new(tracer),
            state: Mutex::new(JournalState::new(&meter)),
            correlation: Arc::new(CorrelationRegistry::default()),
        }
    }

    pub(crate) fn from_shared_parts(
        inner: J,
        tracer: Arc<BoxedTracer>,
        meter: &Meter,
        correlation: Arc<CorrelationRegistry>,
    ) -> Self {
        Self {
            inner,
            tracer,
            state: Mutex::new(JournalState::new(meter)),
            correlation,
        }
    }

    /// Returns the wrapped journal.
    pub fn into_inner(self) -> J {
        self.inner
    }

    fn lock_state(&self) -> MutexGuard<'_, JournalState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl<J: fmt::Debug> fmt::Debug for OtelJournal<J> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OtelJournal")
            .field("inner", &self.inner)
            .field("tracer", &self.tracer)
            .field("causal_correlation", &true)
            .finish_non_exhaustive()
    }
}

impl<J: Journal> Journal for OtelJournal<J> {
    fn record(&self, event: &RunEvent) -> Result<(), JournalError> {
        self.inner.record(event)?;
        self.lock_state()
            .observe(self.tracer.as_ref(), &self.correlation, event);
        Ok(())
    }
}

struct JournalState {
    operations: HashMap<OperationKey, ActiveOperation>,
    active_turns: HashMap<RunId, OperationKey>,
    delegation_parents: HashMap<RunId, Context>,
    agent_started: HashMap<RunId, std::time::Instant>,
    instruments: JournalInstruments,
}

impl JournalState {
    fn new(meter: &Meter) -> Self {
        Self {
            operations: HashMap::new(),
            active_turns: HashMap::new(),
            delegation_parents: HashMap::new(),
            agent_started: HashMap::new(),
            instruments: JournalInstruments::new(meter),
        }
    }

    fn observe(
        &mut self,
        tracer: &BoxedTracer,
        correlation: &CorrelationRegistry,
        event: &RunEvent,
    ) {
        match &event.kind {
            RunEventKind::Domain(domain) => {
                self.observe_domain(tracer, correlation, event, domain);
            }
            RunEventKind::Lifecycle(lifecycle) => {
                self.observe_lifecycle(correlation, event.meta.run_id, lifecycle);
            }
            RunEventKind::Child(child) => {
                self.observe_child(correlation, event.meta.run_id, child);
            }
            _ => {}
        }
    }

    fn observe_domain(
        &mut self,
        tracer: &BoxedTracer,
        correlation: &CorrelationRegistry,
        event: &RunEvent,
        domain: &DomainEvent,
    ) {
        match (domain.namespace.as_str(), domain.name.as_str()) {
            ("runifold.agent", "turn.started") => {
                if let Some(agent) = string_field(domain, "agent") {
                    Self::ensure_root(tracer, correlation, event, "invoke_agent", "agent", agent);
                    self.agent_started
                        .entry(event.meta.run_id)
                        .or_insert_with(std::time::Instant::now);
                    self.start_agent_turn(tracer, correlation, event, domain, agent);
                }
            }
            ("runifold.agent" | "runifold.mcp", "tool.started") => {
                self.start_callable(tracer, correlation, event, domain, CallableKind::Tool);
            }
            ("runifold.agent" | "runifold.mcp", "tool.completed") => {
                self.finish_callable(
                    correlation,
                    event.meta.run_id,
                    domain,
                    CallableKind::Tool,
                    false,
                );
            }
            ("runifold.agent" | "runifold.mcp", "tool.failed") => {
                self.finish_callable(
                    correlation,
                    event.meta.run_id,
                    domain,
                    CallableKind::Tool,
                    true,
                );
            }
            ("runifold.agent", "delegation.started") => {
                self.start_callable(tracer, correlation, event, domain, CallableKind::Delegation);
            }
            ("runifold.agent", "delegation.completed") => {
                self.finish_callable(
                    correlation,
                    event.meta.run_id,
                    domain,
                    CallableKind::Delegation,
                    false,
                );
            }
            ("runifold.agent", "delegation.failed") => {
                self.finish_callable(
                    correlation,
                    event.meta.run_id,
                    domain,
                    CallableKind::Delegation,
                    true,
                );
            }
            ("runifold.mcp", "sampling.started") => {
                self.start_sampling(tracer, correlation, event, domain);
            }
            ("runifold.mcp", "sampling.completed") => {
                self.finish_sampling(correlation, event.meta.run_id, domain, false);
            }
            ("runifold.mcp", "sampling.failed") => {
                self.finish_sampling(correlation, event.meta.run_id, domain, true);
            }
            ("runifold.workflow", "step.started") => {
                self.start_workflow_step(tracer, correlation, event, domain);
            }
            ("runifold.workflow", "step.completed") => {
                self.finish_workflow_step(correlation, event.meta.run_id, domain, false);
            }
            ("runifold.workflow", "step.failed") => {
                self.finish_workflow_step(correlation, event.meta.run_id, domain, true);
            }
            _ => {}
        }
    }

    fn start_sampling(
        &mut self,
        tracer: &BoxedTracer,
        correlation: &CorrelationRegistry,
        event: &RunEvent,
        domain: &DomainEvent,
    ) {
        let Some(call_id) = string_field(domain, "call_id") else {
            return;
        };
        let run_id = event.meta.run_id;
        let key = OperationKey::new(run_id, CallableKind::Sampling, call_id);
        if self.operations.contains_key(&key) {
            return;
        }
        let mut attributes = vec![
            KeyValue::new("runifold.operation.name", "mcp.sampling.create_message"),
            KeyValue::new("runifold.mcp.sampling.request.id", call_id.to_owned()),
            KeyValue::new("runifold.run.id", run_id.to_string()),
        ];
        if let Some(count) = domain
            .payload
            .get("message_count")
            .and_then(serde_json::Value::as_u64)
        {
            attributes.push(KeyValue::new(
                "runifold.mcp.sampling.message.count",
                i64::try_from(count).unwrap_or(i64::MAX),
            ));
        }
        if let Some(tokens) = domain
            .payload
            .get("max_tokens")
            .and_then(serde_json::Value::as_u64)
        {
            attributes.push(KeyValue::new(
                "gen_ai.request.max_tokens",
                i64::try_from(tokens).unwrap_or(i64::MAX),
            ));
        }
        let parent = correlation.current(run_id).unwrap_or_default();
        let span = tracer
            .span_builder("mcp.sampling.create_message")
            .with_kind(SpanKind::Internal)
            .with_attributes(attributes)
            .start_with_context(tracer, &parent);
        self.operations
            .insert(key, ActiveOperation::new(Context::current_with_span(span)));
        self.instruments.sampling_started();
    }

    fn finish_sampling(
        &mut self,
        correlation: &CorrelationRegistry,
        run_id: RunId,
        domain: &DomainEvent,
        failed: bool,
    ) {
        let Some(call_id) = string_field(domain, "call_id") else {
            return;
        };
        let key = OperationKey::new(run_id, CallableKind::Sampling, call_id);
        if let Some(operation) = self.operations.get_mut(&key) {
            for (payload_key, attribute_name) in [
                ("model", "gen_ai.response.model"),
                ("stop_reason", "gen_ai.response.finish_reason"),
                ("error_type", "error.type"),
                ("stage", "runifold.mcp.sampling.stage"),
            ] {
                if let Some(value) = string_field(domain, payload_key) {
                    operation
                        .context
                        .span()
                        .set_attribute(KeyValue::new(attribute_name, value.to_owned()));
                }
            }
            self.instruments.record_sampling(
                operation.started,
                failed,
                string_field(domain, "error_type"),
                string_field(domain, "stage"),
            );
            operation.metric_recorded = true;
        }
        self.finish_operation(correlation, &key, failed);
    }

    fn ensure_root(
        tracer: &BoxedTracer,
        correlation: &CorrelationRegistry,
        event: &RunEvent,
        operation: &'static str,
        entity_key: &'static str,
        entity: &str,
    ) {
        let run_id = event.meta.run_id;
        let parent = correlation
            .take_child_parent(run_id)
            .or_else(|| {
                event
                    .meta
                    .parent_run_id
                    .and_then(|parent_run_id| correlation.root(parent_run_id))
            })
            .unwrap_or_default();
        correlation.get_or_insert_with(run_id, || {
            let builder = tracer
                .span_builder(format!("{operation} {entity}"))
                .with_kind(SpanKind::Internal)
                .with_attributes([
                    KeyValue::new("gen_ai.operation.name", operation),
                    KeyValue::new(format!("gen_ai.{entity_key}.name"), entity.to_owned()),
                    KeyValue::new("runifold.run.id", run_id.to_string()),
                ]);
            let span = if event.meta.parent_run_id.is_some() {
                builder.start_with_context(tracer, &parent)
            } else {
                builder.start(tracer)
            };
            Context::current_with_span(span)
        });
    }

    fn start_agent_turn(
        &mut self,
        tracer: &BoxedTracer,
        correlation: &CorrelationRegistry,
        event: &RunEvent,
        domain: &DomainEvent,
        agent: &str,
    ) {
        let run_id = event.meta.run_id;
        if let Some(previous) = self.active_turns.remove(&run_id) {
            self.finish_operation(correlation, &previous, false);
        }
        let Some(turn) = domain
            .payload
            .get("turn")
            .and_then(serde_json::Value::as_u64)
        else {
            return;
        };
        let key = OperationKey::new(run_id, CallableKind::AgentTurn, &turn.to_string());
        if self.operations.contains_key(&key) {
            return;
        }
        let parent = correlation.current(run_id).unwrap_or_default();
        let span = tracer
            .span_builder(format!("agent.turn {turn}"))
            .with_kind(SpanKind::Internal)
            .with_attributes([
                KeyValue::new("runifold.operation.name", "agent.turn"),
                KeyValue::new("gen_ai.agent.name", agent.to_owned()),
                KeyValue::new(
                    "runifold.agent.turn",
                    i64::try_from(turn).unwrap_or(i64::MAX),
                ),
                KeyValue::new("runifold.run.id", run_id.to_string()),
            ])
            .start_with_context(tracer, &parent);
        let context = Context::current_with_span(span);
        self.operations
            .insert(key.clone(), ActiveOperation::new(context.clone()));
        self.active_turns.insert(run_id, key);
        correlation.set_active(run_id, context);
    }

    fn start_callable(
        &mut self,
        tracer: &BoxedTracer,
        correlation: &CorrelationRegistry,
        event: &RunEvent,
        domain: &DomainEvent,
        kind: CallableKind,
    ) {
        let run_id = event.meta.run_id;
        let Some(call_id) = string_field(domain, "call_id") else {
            return;
        };
        let Some(name) = string_field(domain, kind.payload_key()) else {
            return;
        };
        if let Some(agent) = string_field(domain, "agent") {
            Self::ensure_root(tracer, correlation, event, "invoke_agent", "agent", agent);
            self.agent_started
                .entry(run_id)
                .or_insert_with(std::time::Instant::now);
        }
        let key = OperationKey::new(run_id, kind, call_id);
        if self.operations.contains_key(&key) {
            return;
        }
        let parent = correlation.current(run_id).unwrap_or_default();
        let mut attributes = vec![
            KeyValue::new("gen_ai.operation.name", kind.operation_name()),
            KeyValue::new("gen_ai.tool.call.id", call_id.to_owned()),
            KeyValue::new("runifold.run.id", run_id.to_string()),
        ];
        attributes.push(KeyValue::new(kind.attribute_name(), name.to_owned()));
        if let Some(agent) = string_field(domain, "agent") {
            let agent_attribute = if matches!(kind, CallableKind::Delegation) {
                "runifold.parent_agent.name"
            } else {
                "gen_ai.agent.name"
            };
            attributes.push(KeyValue::new(agent_attribute, agent.to_owned()));
        }
        let span = tracer
            .span_builder(format!("{} {name}", kind.operation_name()))
            .with_kind(SpanKind::Internal)
            .with_attributes(attributes)
            .start_with_context(tracer, &parent);
        let context = Context::current_with_span(span);
        if matches!(kind, CallableKind::Delegation) {
            self.delegation_parents.insert(run_id, context.clone());
        }
        self.operations.insert(key, ActiveOperation::new(context));
    }

    fn finish_callable(
        &mut self,
        correlation: &CorrelationRegistry,
        run_id: RunId,
        domain: &DomainEvent,
        kind: CallableKind,
        failed: bool,
    ) {
        let Some(call_id) = string_field(domain, "call_id") else {
            return;
        };
        self.finish_operation(
            correlation,
            &OperationKey::new(run_id, kind, call_id),
            failed,
        );
    }

    fn start_workflow_step(
        &mut self,
        tracer: &BoxedTracer,
        correlation: &CorrelationRegistry,
        event: &RunEvent,
        domain: &DomainEvent,
    ) {
        let run_id = event.meta.run_id;
        let (Some(workflow), Some(step)) = (
            string_field(domain, "workflow"),
            string_field(domain, "step"),
        ) else {
            return;
        };
        Self::ensure_root(
            tracer,
            correlation,
            event,
            "invoke_workflow",
            "workflow",
            workflow,
        );
        let key = OperationKey::new(run_id, CallableKind::WorkflowStep, step);
        if self.operations.contains_key(&key) {
            return;
        }
        let parent = correlation.current(run_id).unwrap_or_default();
        let span = tracer
            .span_builder(format!("workflow.step {step}"))
            .with_kind(SpanKind::Internal)
            .with_attributes([
                KeyValue::new("runifold.operation.name", "workflow.step"),
                KeyValue::new("gen_ai.workflow.name", workflow.to_owned()),
                KeyValue::new("runifold.workflow.step.name", step.to_owned()),
                KeyValue::new("runifold.run.id", run_id.to_string()),
            ])
            .start_with_context(tracer, &parent);
        self.operations
            .insert(key, ActiveOperation::new(Context::current_with_span(span)));
    }

    fn finish_workflow_step(
        &mut self,
        correlation: &CorrelationRegistry,
        run_id: RunId,
        domain: &DomainEvent,
        failed: bool,
    ) {
        let Some(step) = string_field(domain, "step") else {
            return;
        };
        self.finish_operation(
            correlation,
            &OperationKey::new(run_id, CallableKind::WorkflowStep, step),
            failed,
        );
    }

    fn finish_operation(
        &mut self,
        correlation: &CorrelationRegistry,
        key: &OperationKey,
        failed: bool,
    ) {
        let Some(operation) = self.operations.remove(key) else {
            return;
        };
        match key.kind {
            CallableKind::AgentTurn => {
                self.active_turns.remove(&key.run_id);
                correlation.clear_active(key.run_id);
            }
            CallableKind::Delegation => {
                self.delegation_parents.remove(&key.run_id);
            }
            CallableKind::Tool | CallableKind::WorkflowStep | CallableKind::Sampling => {}
        }
        match key.kind {
            CallableKind::AgentTurn => {
                self.instruments.record_turn(operation.started, failed);
            }
            CallableKind::Sampling if !operation.metric_recorded => {
                self.instruments.record_sampling(
                    operation.started,
                    true,
                    Some("operation_abandoned"),
                    None,
                );
            }
            CallableKind::Tool
            | CallableKind::Delegation
            | CallableKind::WorkflowStep
            | CallableKind::Sampling => {}
        }
        if failed {
            operation
                .context
                .span()
                .set_attribute(KeyValue::new("error.type", "operation_failed"));
            operation
                .context
                .span()
                .set_status(Status::error("operation_failed"));
        }
        operation.context.span().end();
    }

    fn observe_lifecycle(
        &mut self,
        correlation: &CorrelationRegistry,
        run_id: RunId,
        lifecycle: &LifecycleEvent,
    ) {
        if matches!(lifecycle, LifecycleEvent::Started) {
            return;
        }
        let failed = matches!(
            lifecycle,
            LifecycleEvent::Failed { .. } | LifecycleEvent::Cancelled
        );
        let operation_keys = self
            .operations
            .keys()
            .filter(|key| key.run_id == run_id)
            .cloned()
            .collect::<Vec<_>>();
        for key in operation_keys {
            let operation_failed = failed || !matches!(key.kind, CallableKind::AgentTurn);
            self.finish_operation(correlation, &key, operation_failed);
        }
        if let Some(started) = self.agent_started.remove(&run_id) {
            self.instruments.record_agent(started, lifecycle);
        }
        let Some(context) = correlation.remove(run_id) else {
            return;
        };
        if failed {
            let error_type = match lifecycle {
                LifecycleEvent::Cancelled => "cancelled",
                LifecycleEvent::Failed { error } => run_error_type(error),
                _ => "operation_failed",
            };
            context
                .span()
                .set_attribute(KeyValue::new("error.type", error_type));
            context.span().set_status(Status::error(error_type));
        }
        context.span().end();
    }

    fn observe_child(
        &mut self,
        correlation: &CorrelationRegistry,
        parent_run_id: RunId,
        child: &ChildEvent,
    ) {
        let ChildEvent::Started { child_run_id } = child else {
            return;
        };
        if let Some(parent) = self.delegation_parents.get(&parent_run_id).cloned() {
            correlation.bind_child(*child_run_id, parent);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum CallableKind {
    AgentTurn,
    Sampling,
    Tool,
    Delegation,
    WorkflowStep,
}

impl CallableKind {
    const fn operation_name(self) -> &'static str {
        match self {
            Self::AgentTurn => "agent.turn",
            Self::Sampling => "mcp.sampling.create_message",
            Self::Tool => "execute_tool",
            Self::Delegation => "invoke_agent",
            Self::WorkflowStep => "workflow.step",
        }
    }

    const fn payload_key(self) -> &'static str {
        match self {
            Self::AgentTurn => "turn",
            Self::Sampling => "sampling",
            Self::Tool => "tool",
            Self::Delegation => "delegation",
            Self::WorkflowStep => "step",
        }
    }

    const fn attribute_name(self) -> &'static str {
        match self {
            Self::AgentTurn => "runifold.agent.turn",
            Self::Sampling => "runifold.mcp.sampling.request.id",
            Self::Tool => "gen_ai.tool.name",
            Self::Delegation => "gen_ai.agent.name",
            Self::WorkflowStep => "runifold.workflow.step.name",
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct OperationKey {
    run_id: RunId,
    kind: CallableKind,
    id: String,
}

impl OperationKey {
    fn new(run_id: RunId, kind: CallableKind, id: &str) -> Self {
        Self {
            run_id,
            kind,
            id: id.to_owned(),
        }
    }
}

fn string_field<'a>(domain: &'a DomainEvent, key: &str) -> Option<&'a str> {
    domain.payload.get(key)?.as_str()
}

#[cfg(test)]
mod tests {
    use opentelemetry::trace::{SpanId, Status};
    use opentelemetry_sdk::trace::SpanData;
    use runifold_core::{
        ChildEvent, DomainEvent, EventFactory, InMemoryJournal, Journal, JournalError,
        LifecycleEvent, RunEvent, RunEventKind, RunId,
    };

    use super::OtelJournal;
    use crate::test_support::TraceFixture;

    #[test]
    fn agent_and_tool_events_create_parented_gen_ai_spans() {
        let fixture = TraceFixture::new();
        let journal = OtelJournal::from_tracer(InMemoryJournal::new(), fixture.tracer);
        let factory = EventFactory::new(RunId::new(), None);

        record(
            &journal,
            &factory,
            RunEventKind::Lifecycle(LifecycleEvent::Started),
        );
        record(
            &journal,
            &factory,
            domain(
                "runifold.agent",
                "turn.started",
                serde_json::json!({"agent": "planner", "turn": 1}),
            ),
        );
        record_callable(
            &journal,
            &factory,
            "tool.started",
            "call-1",
            "tool",
            "search",
        );
        record_callable(
            &journal,
            &factory,
            "delegation.started",
            "call-2",
            "delegation",
            "researcher",
        );
        record_callable(
            &journal,
            &factory,
            "delegation.completed",
            "call-2",
            "delegation",
            "researcher",
        );
        record_callable(
            &journal,
            &factory,
            "tool.completed",
            "call-1",
            "tool",
            "search",
        );
        record(
            &journal,
            &factory,
            RunEventKind::Lifecycle(LifecycleEvent::Completed {
                output: serde_json::json!({"ok": true}),
            }),
        );

        let spans = fixture.exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 4);
        let root = named(&spans, "invoke_agent planner");
        let turn = named(&spans, "agent.turn 1");
        let tool = named(&spans, "execute_tool search");
        let delegation = named(&spans, "invoke_agent researcher");
        assert_eq!(root.parent_span_id, SpanId::INVALID);
        assert_eq!(turn.parent_span_id, root.span_context.span_id());
        assert_eq!(tool.parent_span_id, turn.span_context.span_id());
        assert_eq!(delegation.parent_span_id, turn.span_context.span_id());
        assert_eq!(
            attribute(tool, "gen_ai.operation.name"),
            Some("execute_tool".into())
        );
        assert_eq!(
            attribute(tool, "gen_ai.tool.call.id"),
            Some("call-1".into())
        );
        assert_eq!(
            attribute(delegation, "gen_ai.agent.name"),
            Some("researcher".into())
        );
        assert_eq!(
            attribute(delegation, "runifold.parent_agent.name"),
            Some("planner".into())
        );
    }

    #[test]
    fn delegated_child_agent_is_parented_to_delegation_span() {
        let fixture = TraceFixture::new();
        let journal = OtelJournal::from_tracer(InMemoryJournal::new(), fixture.tracer);
        let parent_run_id = RunId::new();
        let child_run_id = RunId::new();
        let parent = EventFactory::new(parent_run_id, None);
        let child = EventFactory::new(child_run_id, Some(parent_run_id));

        record(
            &journal,
            &parent,
            domain(
                "runifold.agent",
                "turn.started",
                serde_json::json!({"agent": "planner", "turn": 1}),
            ),
        );
        record(
            &journal,
            &parent,
            domain(
                "runifold.agent",
                "delegation.started",
                serde_json::json!({
                    "agent": "planner",
                    "call_id": "delegate-1",
                    "delegation": "researcher"
                }),
            ),
        );
        record(
            &journal,
            &parent,
            RunEventKind::Child(ChildEvent::Started { child_run_id }),
        );
        record(
            &journal,
            &child,
            domain(
                "runifold.agent",
                "turn.started",
                serde_json::json!({"agent": "researcher", "turn": 1}),
            ),
        );
        record(
            &journal,
            &child,
            RunEventKind::Lifecycle(LifecycleEvent::Completed {
                output: serde_json::json!({"ok": true}),
            }),
        );
        record(
            &journal,
            &parent,
            domain(
                "runifold.agent",
                "delegation.completed",
                serde_json::json!({
                    "agent": "planner",
                    "call_id": "delegate-1",
                    "delegation": "researcher"
                }),
            ),
        );
        record(
            &journal,
            &parent,
            RunEventKind::Lifecycle(LifecycleEvent::Completed {
                output: serde_json::json!({"ok": true}),
            }),
        );

        let spans = fixture.exporter.get_finished_spans().unwrap();
        let delegation = spans
            .iter()
            .find(|span| {
                span.name == "invoke_agent researcher"
                    && attribute(span, "runifold.run.id") == Some(parent_run_id.to_string())
            })
            .unwrap();
        let child_root = spans
            .iter()
            .find(|span| {
                span.name == "invoke_agent researcher"
                    && attribute(span, "runifold.run.id") == Some(child_run_id.to_string())
            })
            .unwrap();
        assert_ne!(
            delegation.span_context.span_id(),
            child_root.span_context.span_id()
        );
        assert_eq!(child_root.parent_span_id, delegation.span_context.span_id());
    }

    #[test]
    fn failed_workflow_step_and_run_are_closed_as_errors() {
        let fixture = TraceFixture::new();
        let journal = OtelJournal::from_tracer(InMemoryJournal::new(), fixture.tracer);
        let factory = EventFactory::new(RunId::new(), None);

        record(
            &journal,
            &factory,
            domain(
                "runifold.workflow",
                "step.started",
                serde_json::json!({"workflow": "release", "step": "deploy"}),
            ),
        );
        record(
            &journal,
            &factory,
            domain(
                "runifold.workflow",
                "step.failed",
                serde_json::json!({"workflow": "release", "step": "deploy"}),
            ),
        );
        record(
            &journal,
            &factory,
            RunEventKind::Lifecycle(LifecycleEvent::Cancelled),
        );

        let spans = fixture.exporter.get_finished_spans().unwrap();
        assert!(matches!(
            named(&spans, "workflow.step deploy").status,
            Status::Error { .. }
        ));
        let root = named(&spans, "invoke_workflow release");
        assert!(matches!(root.status, Status::Error { .. }));
        assert_eq!(attribute(root, "error.type"), Some("cancelled".into()));
    }

    #[test]
    fn mcp_tool_events_create_gen_ai_tool_span() {
        let fixture = TraceFixture::new();
        let journal = OtelJournal::from_tracer(InMemoryJournal::new(), fixture.tracer);
        let factory = EventFactory::new(RunId::new(), None);
        let payload = serde_json::json!({
            "call_id": "mcp-7",
            "tool": "filesystem.read",
            "protocol_version": "2025-11-25"
        });

        record(
            &journal,
            &factory,
            domain("runifold.mcp", "tool.started", payload.clone()),
        );
        record(
            &journal,
            &factory,
            domain("runifold.mcp", "tool.completed", payload),
        );

        let spans = fixture.exporter.get_finished_spans().unwrap();
        let tool = named(&spans, "execute_tool filesystem.read");
        assert_eq!(
            attribute(tool, "gen_ai.operation.name"),
            Some("execute_tool".into())
        );
        assert_eq!(attribute(tool, "gen_ai.tool.call.id"), Some("mcp-7".into()));
        assert_eq!(
            attribute(tool, "gen_ai.tool.name"),
            Some("filesystem.read".into())
        );
    }

    #[test]
    fn mcp_sampling_span_is_parented_to_turn_and_keeps_review_stage() {
        let fixture = TraceFixture::new();
        let journal = OtelJournal::from_tracer(InMemoryJournal::new(), fixture.tracer);
        let factory = EventFactory::new(RunId::new(), None);

        record(
            &journal,
            &factory,
            domain(
                "runifold.agent",
                "turn.started",
                serde_json::json!({"agent": "planner", "turn": 1}),
            ),
        );
        record(
            &journal,
            &factory,
            domain(
                "runifold.mcp",
                "sampling.started",
                serde_json::json!({
                    "call_id": "sampling-1",
                    "message_count": 2,
                    "max_tokens": 128
                }),
            ),
        );
        record(
            &journal,
            &factory,
            domain(
                "runifold.mcp",
                "sampling.failed",
                serde_json::json!({
                    "call_id": "sampling-1",
                    "error_type": "remote",
                    "stage": "response_review"
                }),
            ),
        );
        record(
            &journal,
            &factory,
            RunEventKind::Lifecycle(LifecycleEvent::Failed {
                error: runifold_core::RunError {
                    kind: runifold_core::RunErrorKind::Invocation,
                    message: "safe failure".into(),
                    retry_safety: runifold_core::RetrySafety::Unknown,
                    metadata: std::collections::BTreeMap::new(),
                },
            }),
        );

        let spans = fixture.exporter.get_finished_spans().unwrap();
        let turn = named(&spans, "agent.turn 1");
        let sampling = named(&spans, "mcp.sampling.create_message");
        assert_eq!(sampling.parent_span_id, turn.span_context.span_id());
        assert_eq!(
            attribute(sampling, "runifold.mcp.sampling.stage"),
            Some("response_review".into())
        );
        assert_eq!(attribute(sampling, "error.type"), Some("remote".into()));
        assert!(matches!(sampling.status, Status::Error { .. }));
    }

    #[test]
    fn telemetry_is_not_updated_when_durable_recording_fails() {
        let fixture = TraceFixture::new();
        let journal = OtelJournal::from_tracer(FailingJournal, fixture.tracer);
        let factory = EventFactory::new(RunId::new(), None);
        let event = factory.emit(
            domain(
                "runifold.agent",
                "turn.started",
                serde_json::json!({"agent": "planner"}),
            ),
            None,
        );

        assert!(journal.record(&event).is_err());
        assert!(fixture.exporter.get_finished_spans().unwrap().is_empty());
    }

    #[derive(Debug)]
    struct FailingJournal;

    impl Journal for FailingJournal {
        fn record(&self, _event: &RunEvent) -> Result<(), JournalError> {
            Err(JournalError {
                message: "storage unavailable".into(),
            })
        }
    }

    fn record<J: Journal>(journal: &J, factory: &EventFactory, kind: RunEventKind) {
        journal.record(&factory.emit(kind, None)).unwrap();
    }

    fn record_callable<J: Journal>(
        journal: &J,
        factory: &EventFactory,
        event: &str,
        call_id: &str,
        kind: &str,
        name: &str,
    ) {
        let mut payload = serde_json::json!({"agent": "planner", "call_id": call_id});
        payload
            .as_object_mut()
            .unwrap()
            .insert(kind.into(), name.into());
        record(journal, factory, domain("runifold.agent", event, payload));
    }

    fn domain(namespace: &str, name: &str, payload: serde_json::Value) -> RunEventKind {
        RunEventKind::Domain(DomainEvent {
            namespace: namespace.into(),
            name: name.into(),
            payload,
        })
    }

    fn named<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
        spans.iter().find(|span| span.name == name).unwrap()
    }

    fn attribute(span: &SpanData, key: &str) -> Option<String> {
        span.attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == key)
            .map(|attribute| attribute.value.as_str().into_owned())
    }
}
