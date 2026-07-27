use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use futures_util::StreamExt;
use runifold_core::{
    Budget, BudgetResource, BudgetTracker, CapabilityId, CapabilitySet, Checkpoint,
    CheckpointError, CheckpointErrorKind, CheckpointId, CheckpointStore, ChildEvent, EffectClass,
    InMemoryCheckpointStore, InMemoryJournal, Journal, JournalError, LifecycleEvent, RiskLevel,
    RunContext, RunEvent, RunEventKind,
};
use runifold_model::{
    ContentBlockKind, ContentPart, FinishReason, ModelError, ModelErrorKind, ModelRef,
    ModelStreamEvent, Role, ToolCall,
};
use runifold_testkit::ScriptedModel;
use runifold_tool::{
    Tool, ToolContext, ToolDescriptor, ToolError, ToolErrorKind, ToolFuture, ToolOutput,
    ToolRegistry,
};
use serde_json::{Value, json};

use crate::{AgentStreamEvent, CallableKind};

use crate::{
    Agent, AgentCheckpoint, AgentCheckpointPhase, AgentConfig, AgentDescriptor, AgentError,
    AgentGateway, AgentRoute, GatewayErrorKind, ResumePolicy, ToolErrorPolicy,
};

#[derive(Debug)]
struct EchoTool {
    descriptor: ToolDescriptor,
}

struct FailingJournal;

struct FailRevisionOnceStore {
    inner: InMemoryCheckpointStore,
    revision: u64,
    failed: AtomicBool,
}

impl FailRevisionOnceStore {
    fn new(revision: u64) -> Self {
        Self {
            inner: InMemoryCheckpointStore::new(),
            revision,
            failed: AtomicBool::new(false),
        }
    }
}

impl CheckpointStore for FailRevisionOnceStore {
    fn load(&self, id: CheckpointId) -> Result<Checkpoint, CheckpointError> {
        self.inner.load(id)
    }

    fn compare_and_swap(
        &self,
        checkpoint: &Checkpoint,
        expected_revision: Option<u64>,
    ) -> Result<(), CheckpointError> {
        if checkpoint.revision == self.revision && !self.failed.swap(true, Ordering::SeqCst) {
            return Err(CheckpointError::new(
                CheckpointErrorKind::Storage,
                "injected checkpoint interruption",
            ));
        }
        self.inner.compare_and_swap(checkpoint, expected_revision)
    }
}

impl Journal for FailingJournal {
    fn record(&self, _event: &RunEvent) -> Result<(), JournalError> {
        Err(JournalError {
            message: "storage unavailable".into(),
        })
    }
}

impl EchoTool {
    fn new() -> Self {
        Self {
            descriptor: ToolDescriptor {
                id: CapabilityId::new(),
                name: "echo".into(),
                version: "1".into(),
                description: "Echo input".into(),
                input_schema: json!({"type": "object"}),
                output_schema: json!({"type": "object"}),
                effect: EffectClass::Pure,
                risk: RiskLevel::Low,
                metadata: BTreeMap::new(),
            },
        }
    }
}

impl Tool for EchoTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        input: Value,
        _context: ToolContext,
    ) -> ToolFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async move { Ok(ToolOutput::model_visible(input)) })
    }
}

struct CountingTool {
    descriptor: ToolDescriptor,
    calls: Arc<AtomicUsize>,
}

impl CountingTool {
    fn new(calls: Arc<AtomicUsize>) -> Self {
        Self {
            descriptor: EchoTool::new().descriptor,
            calls,
        }
    }
}

impl Tool for CountingTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        input: Value,
        _context: ToolContext,
    ) -> ToolFuture<'_, Result<ToolOutput, ToolError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(ToolOutput::model_visible(input)) })
    }
}

#[test]
fn completes_a_model_tool_model_loop() {
    let model = ScriptedModel::new();
    model.enqueue(response_events(
        "one",
        vec![tool_call("call_1", "echo", json!({"value": 7}))],
        FinishReason::ToolCalls,
    ));
    model.enqueue(response_events(
        "two",
        vec![ContentPart::text("done")],
        FinishReason::Stop,
    ));
    let tool = Arc::new(EchoTool::new());
    let mut registry = ToolRegistry::new();
    registry.register(tool.clone()).unwrap();
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(tool.descriptor().capability());
    let run = RunContext::root(
        BudgetTracker::new(Budget {
            turns: Some(4),
            tool_calls: Some(2),
            ..Budget::default()
        }),
        capabilities,
    );
    let agent = Agent::new(
        "worker",
        Arc::new(model.clone()),
        ModelRef::new("test", "scripted"),
    )
    .system("Be concise")
    .tools(registry);

    let outcome = futures_executor::block_on(agent.run("start", &run)).unwrap();

    assert_eq!(outcome.turns, 2);
    assert_eq!(outcome.tool_calls, 1);
    assert_eq!(outcome.response.content, vec![ContentPart::text("done")]);
    let requests = model.recorded_requests();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].messages.iter().any(|message| {
        message.role == Role::Tool
            && matches!(
                message.content.first(),
                Some(ContentPart::ToolResult(result)) if !result.is_error
            )
    }));
}

#[test]
fn successful_agent_run_records_lifecycle_model_tool_and_budget_events() {
    let model = ScriptedModel::new();
    model.enqueue(response_events(
        "one",
        vec![tool_call("call_1", "echo", json!({"value": 7}))],
        FinishReason::ToolCalls,
    ));
    model.enqueue(response_events(
        "two",
        vec![ContentPart::text("done")],
        FinishReason::Stop,
    ));
    let tool = Arc::new(EchoTool::new());
    let mut registry = ToolRegistry::new();
    registry.register(tool.clone()).unwrap();
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(tool.descriptor().capability());
    let journal = InMemoryJournal::new();
    let run = RunContext::root(BudgetTracker::new(Budget::default()), capabilities)
        .with_journal(Arc::new(journal.clone()));
    let agent =
        Agent::new("worker", Arc::new(model), ModelRef::new("test", "scripted")).tools(registry);

    futures_executor::block_on(agent.run("start", &run)).unwrap();

    let events = journal.events();
    assert!(matches!(
        events.first().map(|event| &event.kind),
        Some(RunEventKind::Lifecycle(LifecycleEvent::Started))
    ));
    assert!(matches!(
        events.last().map(|event| &event.kind),
        Some(RunEventKind::Lifecycle(LifecycleEvent::Completed { .. }))
    ));
    let domain_names = events
        .iter()
        .filter_map(|event| match &event.kind {
            RunEventKind::Domain(event) => Some(event.name.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(domain_names.contains(&"model.started"));
    assert!(domain_names.contains(&"model.completed"));
    assert!(domain_names.contains(&"tool.started"));
    assert!(domain_names.contains(&"tool.completed"));
    assert!(
        events
            .iter()
            .any(|event| matches!(event.kind, RunEventKind::Budget(_)))
    );
    let started_id = events[0].meta.event_id;
    assert_eq!(events.last().unwrap().meta.caused_by, Some(started_id));
}

#[test]
fn model_failure_records_domain_and_terminal_failure_events() {
    let model = ScriptedModel::new();
    model.enqueue_error(ModelError::local(
        ModelErrorKind::Provider,
        "provider failed",
    ));
    let journal = InMemoryJournal::new();
    let run = root_run(Budget::default()).with_journal(Arc::new(journal.clone()));
    let agent = Agent::new("worker", Arc::new(model), ModelRef::new("test", "scripted"));

    futures_executor::block_on(agent.run("start", &run)).unwrap_err();

    let events = journal.events();
    assert!(events.iter().any(|event| {
        matches!(
            &event.kind,
            RunEventKind::Domain(event) if event.name == "model.failed"
        )
    }));
    assert!(matches!(
        events.last().map(|event| &event.kind),
        Some(RunEventKind::Lifecycle(LifecycleEvent::Failed { .. }))
    ));
}

#[test]
fn journal_failure_is_fail_closed_before_model_execution() {
    let model = ScriptedModel::new();
    let run = root_run(Budget::default()).with_journal(Arc::new(FailingJournal));
    let agent = Agent::new(
        "worker",
        Arc::new(model.clone()),
        ModelRef::new("test", "scripted"),
    );

    let error = futures_executor::block_on(agent.run("start", &run)).unwrap_err();

    assert!(matches!(error, AgentError::Journal(_)));
    assert!(model.recorded_requests().is_empty());
}

#[test]
fn completed_checkpoint_resumes_idempotently_without_model_execution() {
    let model = ScriptedModel::new();
    model.enqueue(response_events(
        "done",
        vec![ContentPart::text("answer")],
        FinishReason::Stop,
    ));
    let checkpoint = AgentCheckpoint::new(Arc::new(InMemoryCheckpointStore::new()));
    let run = root_run(Budget::default());
    let agent = Agent::new(
        "worker",
        Arc::new(model.clone()),
        ModelRef::new("test", "scripted"),
    );

    let first =
        futures_executor::block_on(agent.run_checkpointed("start", &run, &checkpoint)).unwrap();
    let (_, state) = checkpoint.load().unwrap();
    assert!(matches!(
        state.phase,
        AgentCheckpointPhase::Completed { .. }
    ));

    let resumed =
        futures_executor::block_on(agent.resume(&checkpoint, &run, ResumePolicy::RejectAmbiguous))
            .unwrap();

    assert_eq!(resumed, first);
    assert_eq!(model.recorded_requests().len(), 1);
}

#[test]
fn in_flight_checkpoint_requires_explicit_retry_authority() {
    let model = ScriptedModel::new();
    model.enqueue_error(ModelError::local(
        ModelErrorKind::Provider,
        "connection lost after request",
    ));
    model.enqueue(response_events(
        "retry",
        vec![ContentPart::text("recovered")],
        FinishReason::Stop,
    ));
    let checkpoint = AgentCheckpoint::new(Arc::new(InMemoryCheckpointStore::new()));
    let run = root_run(Budget::default());
    let agent = Agent::new(
        "worker",
        Arc::new(model.clone()),
        ModelRef::new("test", "scripted"),
    );

    futures_executor::block_on(agent.run_checkpointed("start", &run, &checkpoint)).unwrap_err();
    let (_, state) = checkpoint.load().unwrap();
    assert!(matches!(
        state.phase,
        AgentCheckpointPhase::TurnInFlight { turn: 1 }
    ));

    let rejected =
        futures_executor::block_on(agent.resume(&checkpoint, &run, ResumePolicy::RejectAmbiguous))
            .unwrap_err();
    assert!(matches!(
        rejected,
        AgentError::AmbiguousCheckpoint { turn: 1 }
    ));

    let outcome = futures_executor::block_on(agent.resume(
        &checkpoint,
        &run,
        ResumePolicy::RetryInterruptedTurn,
    ))
    .unwrap();

    assert_eq!(
        outcome.response.content,
        vec![ContentPart::text("recovered")]
    );
    assert_eq!(outcome.usage.turns, 2);
    assert_eq!(model.recorded_requests().len(), 2);
}

#[test]
fn checkpoint_retry_replays_completed_tool_effect_without_reexecution() {
    let model = ScriptedModel::new();
    let repeated_call = || {
        response_events(
            "tool-turn",
            vec![tool_call("call_1", "echo", json!({"value": 7}))],
            FinishReason::ToolCalls,
        )
    };
    model.enqueue(repeated_call());
    model.enqueue(repeated_call());
    model.enqueue(response_events(
        "done",
        vec![ContentPart::text("finished")],
        FinishReason::Stop,
    ));
    let calls = Arc::new(AtomicUsize::new(0));
    let tool = Arc::new(CountingTool::new(calls.clone()));
    let mut tools = ToolRegistry::new();
    tools.register(tool.clone()).unwrap();
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(tool.descriptor().capability());
    let run = RunContext::root(BudgetTracker::new(Budget::default()), capabilities);
    let checkpoint = AgentCheckpoint::new(Arc::new(FailRevisionOnceStore::new(2)));
    let agent = Agent::new(
        "worker",
        Arc::new(model.clone()),
        ModelRef::new("test", "scripted"),
    )
    .tools(tools);

    futures_executor::block_on(agent.run_checkpointed("start", &run, &checkpoint)).unwrap_err();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let outcome = futures_executor::block_on(agent.resume(
        &checkpoint,
        &run,
        ResumePolicy::RetryInterruptedTurn,
    ))
    .unwrap();

    assert_eq!(
        outcome.response.content,
        vec![ContentPart::text("finished")]
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(model.recorded_requests().len(), 3);
    assert_eq!(outcome.usage.tool_calls, 2);
}

#[test]
fn checkpoint_retry_replays_completed_delegation_without_child_reexecution() {
    let child_model = ScriptedModel::new();
    child_model.enqueue(response_events(
        "child",
        vec![ContentPart::text("child result")],
        FinishReason::Stop,
    ));
    let child = Arc::new(Agent::new(
        "child",
        Arc::new(child_model.clone()),
        ModelRef::new("test", "child"),
    ));
    let descriptor = AgentDescriptor::new("ask_child", "Delegate work");
    let mut gateway = AgentGateway::new();
    gateway
        .register(AgentRoute::new(descriptor.clone(), child))
        .unwrap();

    let parent_model = ScriptedModel::new();
    let repeated_call = || {
        response_events(
            "delegate-turn",
            vec![tool_call(
                "delegate_1",
                "ask_child",
                json!({"input": "work"}),
            )],
            FinishReason::ToolCalls,
        )
    };
    parent_model.enqueue(repeated_call());
    parent_model.enqueue(repeated_call());
    parent_model.enqueue(response_events(
        "done",
        vec![ContentPart::text("finished")],
        FinishReason::Stop,
    ));
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(descriptor.capability());
    let run = RunContext::root(BudgetTracker::new(Budget::default()), capabilities);
    let checkpoint = AgentCheckpoint::new(Arc::new(FailRevisionOnceStore::new(2)));
    let parent = Agent::new(
        "parent",
        Arc::new(parent_model),
        ModelRef::new("test", "parent"),
    )
    .agents(gateway);

    futures_executor::block_on(parent.run_checkpointed("start", &run, &checkpoint)).unwrap_err();
    assert_eq!(child_model.recorded_requests().len(), 1);

    let outcome = futures_executor::block_on(parent.resume(
        &checkpoint,
        &run,
        ResumePolicy::RetryInterruptedTurn,
    ))
    .unwrap();

    assert_eq!(
        outcome.response.content,
        vec![ContentPart::text("finished")]
    );
    assert_eq!(child_model.recorded_requests().len(), 1);
    assert_eq!(outcome.usage.delegations, 1);
}

#[test]
fn recoverable_tool_errors_are_returned_to_the_model() {
    let model = ScriptedModel::new();
    model.enqueue(response_events(
        "one",
        vec![tool_call("missing_1", "missing", json!({}))],
        FinishReason::ToolCalls,
    ));
    model.enqueue(response_events(
        "two",
        vec![ContentPart::text("recovered")],
        FinishReason::Stop,
    ));
    let run = root_run(Budget::default());
    let agent = Agent::new(
        "worker",
        Arc::new(model.clone()),
        ModelRef::new("test", "scripted"),
    );

    let outcome = futures_executor::block_on(agent.run("start", &run)).unwrap();

    assert_eq!(
        outcome.response.content,
        vec![ContentPart::text("recovered")]
    );
    let requests = model.recorded_requests();
    assert!(matches!(
        requests[1].messages.last().unwrap().content.first(),
        Some(ContentPart::ToolResult(result)) if result.is_error
    ));
}

#[test]
fn capability_denials_cannot_be_downgraded_to_model_visible_errors() {
    let model = ScriptedModel::new();
    model.enqueue(response_events(
        "one",
        vec![tool_call("call_1", "echo", json!({}))],
        FinishReason::ToolCalls,
    ));
    let mut registry = ToolRegistry::new();
    registry.register(Arc::new(EchoTool::new())).unwrap();
    let run = root_run(Budget::default());
    let agent =
        Agent::new("worker", Arc::new(model), ModelRef::new("test", "scripted")).tools(registry);

    let error = futures_executor::block_on(agent.run("start", &run)).unwrap_err();

    assert!(matches!(
        error,
        AgentError::Tool(ToolError {
            kind: ToolErrorKind::CapabilityDenied,
            ..
        })
    ));
}

#[test]
fn shared_tool_call_budget_stops_execution_before_the_effect() {
    let model = ScriptedModel::new();
    model.enqueue(response_events(
        "one",
        vec![tool_call("call_1", "echo", json!({}))],
        FinishReason::ToolCalls,
    ));
    let run = root_run(Budget {
        tool_calls: Some(0),
        ..Budget::default()
    });
    let agent = Agent::new("worker", Arc::new(model), ModelRef::new("test", "scripted"));

    let error = futures_executor::block_on(agent.run("start", &run)).unwrap_err();

    assert!(matches!(
        error,
        AgentError::Budget(ref exceeded)
            if exceeded.resource == BudgetResource::ToolCalls
    ));
}

#[test]
fn local_max_turns_is_enforced_independently_of_shared_budget() {
    let model = ScriptedModel::new();
    model.enqueue(response_events(
        "one",
        vec![tool_call("missing_1", "missing", json!({}))],
        FinishReason::ToolCalls,
    ));
    let run = root_run(Budget::default());
    let agent = Agent::new("worker", Arc::new(model), ModelRef::new("test", "scripted"))
        .with_config(AgentConfig {
            max_turns: 1,
            tool_error_policy: ToolErrorPolicy::ReturnToModel,
            ..AgentConfig::default()
        });

    let error = futures_executor::block_on(agent.run("start", &run)).unwrap_err();

    assert!(matches!(error, AgentError::MaxTurns { max_turns: 1 }));
}

#[test]
fn delegates_to_a_child_agent_through_the_canonical_model_loop() {
    let child_model = ScriptedModel::new();
    child_model.enqueue(response_events(
        "child",
        vec![ContentPart::text("child answer")],
        FinishReason::Stop,
    ));
    let child = Arc::new(Agent::new(
        "researcher",
        Arc::new(child_model.clone()),
        ModelRef::new("test", "child"),
    ));
    let descriptor = AgentDescriptor::new("ask_researcher", "Delegate research");
    let mut gateway = AgentGateway::new();
    gateway
        .register(AgentRoute::new(descriptor.clone(), child))
        .unwrap();

    let parent_model = ScriptedModel::new();
    parent_model.enqueue(response_events(
        "parent-one",
        vec![tool_call(
            "delegate_1",
            "ask_researcher",
            json!({"input": "find evidence"}),
        )],
        FinishReason::ToolCalls,
    ));
    parent_model.enqueue(response_events(
        "parent-two",
        vec![ContentPart::text("parent answer")],
        FinishReason::Stop,
    ));
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(descriptor.capability());
    let journal = InMemoryJournal::new();
    let run = RunContext::root(
        BudgetTracker::new(Budget {
            delegations: Some(1),
            turns: Some(4),
            ..Budget::default()
        }),
        capabilities,
    )
    .with_journal(Arc::new(journal.clone()));
    let parent = Agent::new(
        "coordinator",
        Arc::new(parent_model.clone()),
        ModelRef::new("test", "parent"),
    )
    .agents(gateway);

    let outcome = futures_executor::block_on(parent.run("solve", &run)).unwrap();

    assert_eq!(outcome.delegations, 1);
    assert_eq!(outcome.tool_calls, 0);
    assert_eq!(outcome.usage.delegations, 1);
    assert_eq!(child_model.recorded_requests().len(), 1);
    let child_run_id = child_model.recorded_contexts()[0]
        .run_id()
        .expect("delegated model invocation must be scoped to a child run");
    assert_ne!(child_run_id, run.run_id());
    let requests = parent_model.recorded_requests();
    assert_eq!(requests[0].tools[0].name, "ask_researcher");
    assert!(matches!(
        requests[1].messages.last().unwrap().content.first(),
        Some(ContentPart::ToolResult(result))
            if !result.is_error
                && matches!(
                    result.content.first(),
                    Some(ContentPart::Text { text }) if text.contains("child answer")
                )
    ));
    let events = journal.events();
    let (child_event_id, recorded_child_id) = events
        .iter()
        .find_map(|event| match event.kind {
            RunEventKind::Child(ChildEvent::Started { child_run_id }) => {
                Some((event.meta.event_id, child_run_id))
            }
            _ => None,
        })
        .expect("parent must record child creation");
    assert_eq!(recorded_child_id, child_run_id);
    assert!(events.iter().any(|event| {
        event.meta.run_id == child_run_id
            && event.meta.caused_by == Some(child_event_id)
            && matches!(event.kind, RunEventKind::Lifecycle(LifecycleEvent::Started))
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event.kind,
            RunEventKind::Child(ChildEvent::Completed { child_run_id: completed })
                if completed == child_run_id
        ) && event.meta.caused_by == Some(child_event_id)
    }));
}

#[test]
fn agent_capability_denial_is_a_hard_gateway_failure() {
    let child_model = ScriptedModel::new();
    let child = Arc::new(Agent::new(
        "child",
        Arc::new(child_model),
        ModelRef::new("test", "child"),
    ));
    let descriptor = AgentDescriptor::new("ask_child", "Delegate work");
    let mut gateway = AgentGateway::new();
    gateway
        .register(AgentRoute::new(descriptor, child))
        .unwrap();
    let parent_model = ScriptedModel::new();
    parent_model.enqueue(response_events(
        "parent",
        vec![tool_call(
            "delegate_1",
            "ask_child",
            json!({"input": "work"}),
        )],
        FinishReason::ToolCalls,
    ));
    let parent = Agent::new(
        "parent",
        Arc::new(parent_model),
        ModelRef::new("test", "parent"),
    )
    .agents(gateway);

    let error =
        futures_executor::block_on(parent.run("start", &root_run(Budget::default()))).unwrap_err();

    assert!(matches!(
        error,
        AgentError::Gateway(ref error)
            if error.kind == GatewayErrorKind::CapabilityDenied
    ));
}

#[test]
fn gateway_rejects_child_authority_amplification_before_execution() {
    let child_model = ScriptedModel::new();
    let child = Arc::new(Agent::new(
        "child",
        Arc::new(child_model.clone()),
        ModelRef::new("test", "child"),
    ));
    let descriptor = AgentDescriptor::new("ask_child", "Delegate work");
    let hidden_tool = EchoTool::new();
    let mut child_capabilities = CapabilitySet::new();
    child_capabilities.grant(hidden_tool.descriptor().capability());
    let mut gateway = AgentGateway::new();
    gateway
        .register(AgentRoute::new(descriptor.clone(), child).with_capabilities(child_capabilities))
        .unwrap();
    let parent_model = ScriptedModel::new();
    parent_model.enqueue(response_events(
        "parent",
        vec![tool_call(
            "delegate_1",
            "ask_child",
            json!({"input": "work"}),
        )],
        FinishReason::ToolCalls,
    ));
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(descriptor.capability());
    let run = RunContext::root(BudgetTracker::new(Budget::default()), capabilities);
    let parent = Agent::new(
        "parent",
        Arc::new(parent_model),
        ModelRef::new("test", "parent"),
    )
    .agents(gateway);

    let error = futures_executor::block_on(parent.run("start", &run)).unwrap_err();

    assert!(matches!(
        error,
        AgentError::Gateway(ref error)
            if error.kind == GatewayErrorKind::AuthorityEscalation
    ));
    assert_eq!(run.budget().usage().delegations, 0);
    assert!(child_model.recorded_requests().is_empty());
}

#[test]
fn delegation_budget_stops_before_the_child_model_runs() {
    let child_model = ScriptedModel::new();
    let child = Arc::new(Agent::new(
        "child",
        Arc::new(child_model.clone()),
        ModelRef::new("test", "child"),
    ));
    let descriptor = AgentDescriptor::new("ask_child", "Delegate work");
    let mut gateway = AgentGateway::new();
    gateway
        .register(AgentRoute::new(descriptor.clone(), child))
        .unwrap();
    let parent_model = ScriptedModel::new();
    parent_model.enqueue(response_events(
        "parent",
        vec![tool_call(
            "delegate_1",
            "ask_child",
            json!({"input": "work"}),
        )],
        FinishReason::ToolCalls,
    ));
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(descriptor.capability());
    let run = RunContext::root(
        BudgetTracker::new(Budget {
            delegations: Some(0),
            ..Budget::default()
        }),
        capabilities,
    );
    let parent = Agent::new(
        "parent",
        Arc::new(parent_model),
        ModelRef::new("test", "parent"),
    )
    .agents(gateway);

    let error = futures_executor::block_on(parent.run("start", &run)).unwrap_err();

    assert!(matches!(
        error,
        AgentError::Gateway(ref error)
            if error.kind == GatewayErrorKind::BudgetExceeded
    ));
    assert!(child_model.recorded_requests().is_empty());
}

#[test]
fn streaming_drives_the_canonical_model_tool_loop_in_order() {
    let model = ScriptedModel::new();
    model.enqueue(response_events(
        "tool-turn",
        vec![tool_call("call_1", "echo", json!({"value": 7}))],
        FinishReason::ToolCalls,
    ));
    model.enqueue([
        ModelStreamEvent::ResponseStarted {
            id: Some("terminal-turn".into()),
            model: ModelRef::new("test", "scripted"),
        },
        ModelStreamEvent::ContentBlockStarted {
            index: 0,
            kind: ContentBlockKind::Text,
        },
        ModelStreamEvent::TextDelta {
            index: 0,
            text: "hel".into(),
        },
        ModelStreamEvent::TextDelta {
            index: 0,
            text: "lo".into(),
        },
        ModelStreamEvent::ContentBlockCompleted { index: 0 },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::Stop,
            provider_metadata: BTreeMap::new(),
        },
    ]);
    let tool = Arc::new(EchoTool::new());
    let mut tools = ToolRegistry::new();
    tools.register(tool.clone()).unwrap();
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(tool.descriptor().capability());
    let run = RunContext::root(BudgetTracker::new(Budget::default()), capabilities);
    let agent = Agent::new(
        "streamer",
        Arc::new(model),
        ModelRef::new("test", "scripted"),
    )
    .tools(tools);

    let events = futures_executor::block_on(agent.stream("start", &run).collect::<Vec<_>>())
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(matches!(
        events.first(),
        Some(AgentStreamEvent::Started { agent }) if agent == "streamer"
    ));
    assert!(events.iter().any(|event| matches!(
        event,
        AgentStreamEvent::Model {
            turn: 2,
            event: ModelStreamEvent::TextDelta { text, .. },
        } if text == "hel"
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        AgentStreamEvent::CallableStarted {
            turn: 1,
            kind: CallableKind::Tool,
            call,
        } if call.name == "echo"
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        AgentStreamEvent::CallableCompleted {
            turn: 1,
            kind: CallableKind::Tool,
            success: true,
            ..
        }
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        AgentStreamEvent::UsageUpdated { usage }
            if usage.turns == 2 && usage.tool_calls == 1
    )));
    assert!(matches!(
        events.last(),
        Some(AgentStreamEvent::Completed { outcome })
            if outcome.response.content == vec![ContentPart::text("hello")]
    ));
}

#[test]
fn streaming_applies_backpressure_at_each_visible_event() {
    let model = ScriptedModel::new();
    model.enqueue(response_events(
        "terminal",
        vec![ContentPart::text("done")],
        FinishReason::Stop,
    ));
    let run = root_run(Budget::default());
    let agent = Agent::new(
        "streamer",
        Arc::new(model.clone()),
        ModelRef::new("test", "scripted"),
    );
    let mut stream = agent.stream("start", &run);

    let first = futures_executor::block_on(stream.next()).unwrap().unwrap();

    assert!(matches!(first, AgentStreamEvent::Started { .. }));
    assert!(
        model.recorded_requests().is_empty(),
        "the model advanced before the consumer requested the next event"
    );

    let remaining = futures_executor::block_on(stream.collect::<Vec<_>>());
    assert!(remaining.into_iter().all(|event| event.is_ok()));
    assert_eq!(model.recorded_requests().len(), 1);
}

fn response_events(
    id: &str,
    content: Vec<ContentPart>,
    finish_reason: FinishReason,
) -> Vec<ModelStreamEvent> {
    let mut events = vec![ModelStreamEvent::ResponseStarted {
        id: Some(id.into()),
        model: ModelRef::new("test", "scripted"),
    }];
    events.extend(content.into_iter().enumerate().map(|(index, part)| {
        ModelStreamEvent::ContentPartCompleted {
            index: u32::try_from(index).unwrap(),
            part,
        }
    }));
    events.push(ModelStreamEvent::ResponseCompleted {
        finish_reason,
        provider_metadata: BTreeMap::new(),
    });
    events
}

fn tool_call(id: &str, name: &str, arguments: Value) -> ContentPart {
    ContentPart::ToolCall(ToolCall {
        id: id.into(),
        name: name.into(),
        raw_arguments: Some(arguments.to_string()),
        arguments,
        metadata: BTreeMap::new(),
    })
}

fn root_run(budget: Budget) -> RunContext {
    RunContext::root(BudgetTracker::new(budget), CapabilitySet::new())
}
