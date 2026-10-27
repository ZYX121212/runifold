//! Behavioral checks at the canonical execution and recovery boundaries.

use super::context::SUMMARY_THROUGH;
use super::tests::{EchoTool, response_events};
use crate::{
    Agent, AgentCheckpoint, AgentError, AgentStreamEvent, ContextBudget,
    ContextComponentDescriptor, ContextError, ContextGroup, ContextGroupKind, ContextManager,
    ContextSelectionStrategy, ContextTokenCounter, ConversationContextPolicy, ConversationId,
    ConversationStore, ConversationWindow, InMemoryConversationStore, MemoryNamespace,
    ResumePolicy, SerializedBytesCounter,
};
use futures_util::StreamExt;
use runifold_core::{
    Budget, BudgetTracker, CapabilitySet, InMemoryCheckpointStore, InMemoryJournal, RunContext,
};
use runifold_model::{
    ContentPart, FinishReason, Message, ModelError, ModelErrorKind, ModelRef, ModelRequest, Role,
    ToolCall, ToolResult,
};
use runifold_testkit::ScriptedModel;
use std::collections::BTreeMap;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct CharacterCounter;
impl ContextTokenCounter for CharacterCounter {
    fn descriptor(&self) -> ContextComponentDescriptor {
        ContextComponentDescriptor::new("test.characters", 1).unwrap()
    }
    fn is_estimate(&self) -> bool {
        false
    }
    fn count_overhead(&self, request: &ModelRequest) -> Result<u64, ContextError> {
        Ok(5 + request.tools.len() as u64 * 20)
    }
    fn count_message(&self, message: &Message) -> Result<u64, ContextError> {
        Ok(message
            .content
            .iter()
            .map(|part| match part {
                ContentPart::Text { text } => text.len() as u64,
                _ => 5,
            })
            .sum())
    }
}

fn manager(input: u64) -> ContextManager {
    ContextManager::new(ContextBudget::new(input + 10, 10, 0).unwrap())
        .with_counter(Arc::new(CharacterCounter))
}
fn run() -> RunContext {
    RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new())
}
fn model() -> ScriptedModel {
    let model = ScriptedModel::new();
    for index in 0..8 {
        model.enqueue(response_events(
            &format!("response-{index}"),
            vec![ContentPart::text("answer")],
            FinishReason::Stop,
        ));
    }
    model
}
fn agent(model: ScriptedModel, manager: ContextManager) -> Agent {
    Agent::builder("analysis", Arc::new(model), ModelRef::new("test", "script"))
        .context_manager(manager)
        .build()
        .unwrap()
}
fn message(role: Role, part: ContentPart) -> Message {
    Message::new(role, vec![part]).unwrap()
}
fn tool_pair(id: &str) -> [Message; 2] {
    [
        message(
            Role::Assistant,
            ContentPart::ToolCall(ToolCall {
                id: id.into(),
                name: "echo".into(),
                arguments: serde_json::json!({}),
                raw_arguments: None,
                metadata: BTreeMap::new(),
            }),
        ),
        message(
            Role::Tool,
            ContentPart::ToolResult(ToolResult {
                call_id: id.into(),
                name: Some("echo".into()),
                content: vec![ContentPart::text("result")],
                structured_content: None,
                is_error: false,
                metadata: BTreeMap::new(),
            }),
        ),
    ]
}

#[test]
fn context_budget_reserves_output_and_rejects_invalid_deserialized_values() {
    assert!(ContextBudget::new(10, 10, 0).is_err());
    assert!(ContextBudget::new(u64::MAX, 1, u64::MAX).is_err());
    let budget: ContextBudget =
        serde_json::from_str(r#"{"window":1,"output_reserve":2,"safety_margin":0}"#).unwrap();
    assert!(ContextManager::new(budget).validate().is_err());
    assert_eq!(ContextBudget::new(100, 20, 5).unwrap().input_limit(), 75);
}

#[test]
fn context_projects_whole_history_and_preserves_system_summary_and_current_turn() {
    let mut summary = Message::user("summary");
    summary.metadata.insert(SUMMARY_THROUGH.into(), 12.into());
    let mut request = ModelRequest::new(ModelRef::new("test", "script"), Message::system("rules"));
    request.messages.push(summary);
    request.messages.push(Message::user("older question"));
    request.messages.extend(tool_pair("old"));
    request
        .messages
        .push(message(Role::Assistant, ContentPart::text("older answer")));
    request.messages.push(Message::user("new"));
    request.messages.extend(tool_pair("current"));
    let plan = manager(32).plan(&request, 6, 2).unwrap();
    let history = plan
        .groups
        .iter()
        .find(|group| group.kind == ContextGroupKind::History)
        .unwrap();
    assert_eq!(history.message_indices, vec![2, 3, 4, 5]);
    assert!(!history.selected);
    assert!(
        plan.groups
            .iter()
            .filter(|group| group.required)
            .all(|group| group.selected)
    );
    assert_eq!(request.messages.len(), 9);
}

#[test]
fn context_repeated_tool_ids_do_not_hide_split_older_exchanges() {
    let mut request = ModelRequest::new(ModelRef::new("test", "script"), Message::user("old"));
    let [call, result] = tool_pair("reused");
    request.messages.push(call);
    request.messages.push(Message::user("interleaved"));
    request.messages.push(result);
    request.messages.push(Message::user("new"));
    request.messages.extend(tool_pair("reused"));
    assert_eq!(
        manager(34).plan(&request, 4, 1).unwrap_err(),
        ContextError::SplitToolExchange
    );
}

#[test]
fn context_counts_callable_overhead_and_refuses_current_tool_overflow() {
    let model = model();
    let agent = Agent::builder(
        "worker",
        Arc::new(model.clone()),
        ModelRef::new("test", "script"),
    )
    .tool(EchoTool::new())
    .context_manager(manager(10))
    .build()
    .unwrap();
    let error = futures_executor::block_on(agent.prompt("input")).unwrap_err();
    assert!(matches!(
        error,
        AgentError::Context(ContextError::Overflow { .. })
    ));
    assert!(model.recorded_requests().is_empty());
}

#[test]
fn context_sets_output_limit_and_rejects_oversized_output_before_dispatch() {
    let model = model();
    let outcome =
        futures_executor::block_on(agent(model.clone(), manager(40)).prompt("input")).unwrap();
    assert_eq!(
        model.recorded_requests()[0].generation.max_output_tokens,
        Some(10)
    );
    assert_eq!(outcome.context_plans().unwrap()[0].input_tokens, 10);
    assert!(
        !model.recorded_requests()[0].messages[0]
            .metadata
            .contains_key(super::context::INPUT_MARKER)
    );
    let model = self::model();
    let bad = Agent::builder(
        "analysis",
        Arc::new(model.clone()),
        ModelRef::new("test", "script"),
    )
    .context_manager(manager(40))
    .max_output_tokens(11)
    .build()
    .unwrap();
    assert!(matches!(
        futures_executor::block_on(bad.prompt("input")),
        Err(AgentError::Context(ContextError::OutputReserveExceeded))
    ));
    assert!(model.recorded_requests().is_empty());
}

#[test]
fn context_text_estimator_rejects_nested_media_and_accounts_schemas() {
    let mut request = ModelRequest::new(ModelRef::new("test", "script"), Message::user("text"));
    let before = SerializedBytesCounter.count_overhead(&request).unwrap();
    request.output_format = runifold_model::OutputFormat::typed::<String>("string");
    assert!(SerializedBytesCounter.count_overhead(&request).unwrap() > before);
    let [_, mut result] = tool_pair("media");
    if let ContentPart::ToolResult(result) = &mut result.content[0] {
        result.content = vec![ContentPart::Image {
            source: runifold_model::MediaSource::Url {
                url: "https://example.invalid/image".into(),
                media_type: None,
            },
        }];
    }
    assert_eq!(
        SerializedBytesCounter.count_message(&result).unwrap_err(),
        ContextError::UnsupportedContent
    );
}

struct InvalidStrategy;
impl ContextSelectionStrategy for InvalidStrategy {
    fn descriptor(&self) -> ContextComponentDescriptor {
        ContextComponentDescriptor::new("test.invalid", 1).unwrap()
    }
    fn eviction_order(&self, _: &[ContextGroup]) -> Vec<usize> {
        vec![0, 0]
    }
}
#[test]
fn context_validates_strategies_even_when_input_fits() {
    let request = ModelRequest::new(ModelRef::new("test", "script"), Message::user("input"));
    assert_eq!(
        manager(100)
            .with_strategy(Arc::new(InvalidStrategy))
            .plan(&request, 0, 1)
            .unwrap_err(),
        ContextError::InvalidSelection
    );
}

#[test]
fn context_conversation_keeps_full_history_and_strips_runtime_metadata() {
    let model = model();
    let agent = agent(model.clone(), manager(15));
    let store = InMemoryConversationStore::new();
    let id = ConversationId::new();
    let namespace = MemoryNamespace::parse("test.user").unwrap();
    let policy = ConversationContextPolicy::new(ConversationWindow::new(20).unwrap());
    for input in ["first", "second", "third"] {
        futures_executor::block_on(agent.run_conversation(
            input,
            &run(),
            &store,
            id,
            namespace.clone(),
            policy,
        ))
        .unwrap();
    }
    assert_eq!(model.recorded_requests()[2].messages.len(), 1);
    let transcript = futures_executor::block_on(store.list_transcript(
        id,
        namespace,
        None,
        ConversationWindow::new(20).unwrap(),
    ))
    .unwrap();
    assert_eq!(transcript.len(), 6);
    assert!(
        transcript
            .iter()
            .all(|entry| !entry.message.metadata.contains_key(super::context::PLANS))
    );
}

#[test]
fn context_stream_and_journal_report_the_same_persisted_decisions_without_payloads() {
    let model = model();
    let agent = agent(model, manager(50));
    let journal = Arc::new(InMemoryJournal::new());
    let run = run().with_journal(journal.clone());
    let (plan, outcome) = futures_executor::block_on(async {
        let mut stream = agent.stream("private-input", &run);
        let mut plan = None;
        let mut outcome = None;
        while let Some(event) = stream.next().await {
            match event.unwrap() {
                AgentStreamEvent::ContextPrepared { plan: value } => plan = Some(value),
                AgentStreamEvent::Completed { outcome: value } => outcome = Some(value),
                _ => {}
            }
        }
        (plan.unwrap(), outcome.unwrap())
    });
    assert_eq!(outcome.context_plans().unwrap(), vec![plan.clone()]);
    assert!(
        !serde_json::to_string(&plan)
            .unwrap()
            .contains("private-input")
    );
    assert!(journal.events().iter().any(|event| matches!(&event.kind, runifold_core::RunEventKind::Domain(domain) if domain.name == "context.prepared")));
}

struct OnceStrategy(AtomicUsize);
impl ContextSelectionStrategy for OnceStrategy {
    fn descriptor(&self) -> ContextComponentDescriptor {
        ContextComponentDescriptor::new("test.once", 1).unwrap()
    }
    fn eviction_order(&self, _: &[ContextGroup]) -> Vec<usize> {
        assert_eq!(
            self.0.fetch_add(1, Ordering::SeqCst),
            0,
            "recovery must reuse persisted selection"
        );
        Vec::new()
    }
}
#[test]
fn context_recovery_reuses_exact_plan_and_detects_changed_policy() {
    let model = ScriptedModel::new();
    model.enqueue_error(ModelError::local(ModelErrorKind::Provider, "interrupted"));
    model.enqueue(response_events(
        "recovered",
        vec![ContentPart::text("done")],
        FinishReason::Stop,
    ));
    let strategy = Arc::new(OnceStrategy(AtomicUsize::new(0)));
    let agent = agent(model.clone(), manager(50).with_strategy(strategy.clone()));
    let checkpoint = AgentCheckpoint::new(Arc::new(InMemoryCheckpointStore::new()));
    let run = run();
    futures_executor::block_on(agent.run_checkpointed("question", &run, &checkpoint)).unwrap_err();
    let changed = self::agent(model.clone(), manager(51));
    assert!(
        futures_executor::block_on(changed.resume(
            &checkpoint,
            &run,
            ResumePolicy::RetryInterruptedTurn
        ))
        .is_err()
    );
    assert!(matches!(
        futures_executor::block_on(agent.resume(&checkpoint, &run, ResumePolicy::RejectAmbiguous)),
        Err(AgentError::AmbiguousCheckpoint { .. })
    ));
    let outcome = futures_executor::block_on(agent.resume(
        &checkpoint,
        &run,
        ResumePolicy::RetryInterruptedTurn,
    ))
    .unwrap();
    assert_eq!(strategy.0.load(Ordering::SeqCst), 1);
    assert_eq!(model.recorded_requests()[0], model.recorded_requests()[1]);
    assert_eq!(outcome.context_plans().unwrap().len(), 1);
}

#[test]
fn context_excludes_optional_documents_individually_and_records_their_ids() {
    let model = model();
    let agent = Agent::builder(
        "analysis",
        Arc::new(model.clone()),
        ModelRef::new("test", "script"),
    )
    .context("A large optional reference document.")
    .context("A different optional reference document.")
    .context_manager(manager(10))
    .build()
    .unwrap();
    let outcome = futures_executor::block_on(agent.prompt("input")).unwrap();
    let plan = outcome.context_plans().unwrap().remove(0);
    let references = plan
        .groups
        .iter()
        .filter(|group| group.kind == ContextGroupKind::Reference)
        .collect::<Vec<_>>();
    assert_eq!(references.len(), 2);
    assert!(
        references
            .iter()
            .all(|group| !group.selected && group.source_ids.len() == 1)
    );
    assert_eq!(
        model.recorded_requests()[0].messages,
        vec![Message::user("input")]
    );
    assert_eq!(outcome.transcript.len(), 4); // Two references, original input, answer.
}

#[test]
fn context_overflow_after_a_tool_never_discards_the_current_exchange_or_retries_it() {
    let model = ScriptedModel::new();
    let mut pair = tool_pair("current");
    if let ContentPart::ToolCall(call) = &mut pair[0].content[0] {
        call.arguments = serde_json::json!({"value": 7});
    }
    model.enqueue(response_events(
        "call",
        pair[0].content.clone(),
        FinishReason::ToolCalls,
    ));
    let agent = Agent::builder(
        "worker",
        Arc::new(model.clone()),
        ModelRef::new("test", "script"),
    )
    .tool(EchoTool::new())
    .context_manager(manager(35))
    .build()
    .unwrap();
    let run = agent.default_run_context();
    let error = futures_executor::block_on(agent.run("input", &run)).unwrap_err();
    assert!(matches!(
        error,
        AgentError::Context(ContextError::Overflow { .. })
    ));
    assert_eq!(error.retry_safety(), runifold_core::RetrySafety::Unknown);
    assert_eq!(model.recorded_requests().len(), 1);
    assert_eq!(run.budget().usage().tool_calls, 1);
}

#[test]
fn context_summary_overflow_is_explicit_and_coverage_is_reported() {
    let mut summary = Message::user("summary");
    summary.metadata.insert(SUMMARY_THROUGH.into(), 17.into());
    let mut request = ModelRequest::new(ModelRef::new("test", "script"), summary);
    request.messages.push(Message::user("input"));
    let plan = manager(17).plan(&request, 1, 1).unwrap();
    assert_eq!(plan.groups[0].summary_through, Some(17));
    assert!(plan.groups[0].required);
    assert!(matches!(
        manager(16).plan(&request, 1, 1),
        Err(ContextError::Overflow { .. })
    ));
}

#[test]
fn context_recovered_plan_cannot_omit_required_input_or_change_its_budget() {
    let request = ModelRequest::new(ModelRef::new("test", "script"), Message::user("input"));
    let manager = manager(50);
    let mut plan = manager.plan(&request, 0, 1).unwrap();
    plan.groups[0].selected = false;
    assert_eq!(
        manager
            .validate_recovered_plan(&request, 0, &plan)
            .unwrap_err(),
        ContextError::RecoveryMismatch
    );
    let plan = manager.plan(&request, 0, 1).unwrap();
    assert_eq!(
        self::manager(51)
            .validate_recovered_plan(&request, 0, &plan)
            .unwrap_err(),
        ContextError::RecoveryMismatch
    );
}

#[test]
fn context_disabled_recovery_contract_remains_compatible_with_legacy_json() {
    let agent = Agent::new("legacy", Arc::new(model()), ModelRef::new("test", "script"));
    let json = serde_json::to_value(agent.recovery_contract()).unwrap();
    assert!(json.get("context_manager").is_none());
    let decoded: crate::AgentRecoveryContract = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, agent.recovery_contract());
    assert!(ContextComponentDescriptor::new("private text cannot be a component name", 1).is_err());
}

#[test]
fn context_disabled_static_recovery_contract_has_no_new_reference_metadata() {
    let agent = Agent::builder("legacy", Arc::new(model()), ModelRef::new("test", "script"))
        .context("legacy static data")
        .build()
        .unwrap();
    let json = serde_json::to_value(agent.recovery_contract()).unwrap();
    assert_eq!(json["context"][0]["metadata"].as_object().unwrap().len(), 1);
}

#[test]
fn context_recovery_rejects_a_changed_original_request_before_dispatch() {
    use runifold_core::CheckpointStore;
    let model = ScriptedModel::new();
    model.enqueue_error(ModelError::local(ModelErrorKind::Provider, "interrupted"));
    let agent = agent(model.clone(), manager(50));
    let store = Arc::new(InMemoryCheckpointStore::new());
    let checkpoint = AgentCheckpoint::new(store.clone());
    let run = run();
    futures_executor::block_on(agent.run_checkpointed("original", &run, &checkpoint)).unwrap_err();
    let (envelope, mut state) = checkpoint.load().unwrap();
    state.transcript[0].content = vec![ContentPart::text("changed input")];
    let next = envelope.next(serde_json::to_value(state).unwrap()).unwrap();
    store
        .compare_and_swap(&next, Some(envelope.revision))
        .unwrap();
    let error = futures_executor::block_on(agent.resume(
        &checkpoint,
        &run,
        ResumePolicy::RetryInterruptedTurn,
    ))
    .unwrap_err();
    assert!(matches!(
        error,
        AgentError::Context(ContextError::RecoveryMismatch)
    ));
    assert_eq!(model.recorded_requests().len(), 1);
}

#[test]
fn context_history_stays_atomic_across_interleaved_reference_messages() {
    let mut reference = Message::user("ref");
    reference.metadata.insert(
        crate::conversation::TRANSIENT_CONTEXT_METADATA.into(),
        true.into(),
    );
    let mut request = ModelRequest::new(ModelRef::new("test", "script"), Message::user("old"));
    request.messages.extend([
        reference,
        message(Role::Assistant, ContentPart::text("answer")),
        Message::user("new"),
    ]);
    let manager = manager(18);
    let plan = manager.plan(&request, 3, 1).unwrap();
    let history = &plan.groups[0];
    assert_eq!(history.message_indices, vec![0, 2]);
    assert!(!history.selected);
    assert!(plan.groups[1].selected);
    assert_eq!(plan.input_tokens, 11);
    manager.validate_recovered_plan(&request, 3, &plan).unwrap();
}

#[test]
fn context_recovery_rejects_split_history_and_changed_source_provenance() {
    let mut request = ModelRequest::new(ModelRef::new("test", "script"), Message::user("old"));
    request.messages.extend([
        message(Role::Assistant, ContentPart::text("answer")),
        Message::user("new"),
    ]);
    let manager = manager(50);
    let mut plan = manager.plan(&request, 2, 1).unwrap();
    let mut split = plan.groups[0].clone();
    plan.groups[0].message_indices = vec![0];
    plan.groups[0].tokens = 3;
    split.message_indices = vec![1];
    split.tokens = 6;
    plan.groups.insert(1, split);
    assert_eq!(
        manager.validate_recovered_plan(&request, 2, &plan),
        Err(ContextError::RecoveryMismatch)
    );
    let mut plan = manager.plan(&request, 2, 1).unwrap();
    plan.groups[0].source_ids.push("different-source".into());
    assert_eq!(
        manager.validate_recovered_plan(&request, 2, &plan),
        Err(ContextError::RecoveryMismatch)
    );
}

#[test]
fn context_streaming_accounting_and_digest_preserve_canonical_json_bytes() {
    use sha2::{Digest, Sha256};
    let mut request = ModelRequest::new(
        ModelRef::new("test", "script"),
        Message::user("中文\\\"\n🙂".repeat(200_000)),
    );
    request.output_format = runifold_model::OutputFormat::typed::<String>("answer");
    request.provider_options.insert(
        "test".into(),
        serde_json::json!({"text":"中文\\\"\n🙂", "list":[1, true, null]}),
    );
    request
        .metadata
        .insert("test.flags".into(), serde_json::json!(["one", "two"]));
    let expected: [u8; 32] = Sha256::digest(serde_json::to_vec(&request).unwrap()).into();
    assert_eq!(
        super::context_runtime::serialized_digest(&request).unwrap(),
        expected
    );
    let overhead = SerializedBytesCounter.count_overhead(&request).unwrap();
    let full_message = &request.messages[0];
    assert_eq!(
        SerializedBytesCounter.count_message(full_message).unwrap(),
        serde_json::to_vec(full_message).unwrap().len() as u64 + 16
    );
    request.messages.clear();
    assert_eq!(
        overhead,
        serde_json::to_vec(&request).unwrap().len() as u64 + 256
    );
}

#[test]
fn context_snapshot_rejects_changed_accounting_without_reopening_the_model() {
    use runifold_core::CheckpointStore;
    let model = ScriptedModel::new();
    model.enqueue_error(ModelError::local(ModelErrorKind::Provider, "interrupted"));
    let agent = agent(model.clone(), manager(50));
    let store = Arc::new(InMemoryCheckpointStore::new());
    let checkpoint = AgentCheckpoint::new(store.clone());
    let run = run();
    futures_executor::block_on(agent.run_checkpointed("original", &run, &checkpoint)).unwrap_err();
    let (envelope, mut state) = checkpoint.load().unwrap();
    let plan = &mut state.transcript[0]
        .metadata
        .get_mut(super::context::PLANS)
        .unwrap()["1"]["plan"];
    plan["groups"][0]["tokens"] = 1.into();
    plan["input_tokens"] = 6.into();
    let next = envelope.next(serde_json::to_value(state).unwrap()).unwrap();
    store
        .compare_and_swap(&next, Some(envelope.revision))
        .unwrap();
    let error = futures_executor::block_on(agent.resume(
        &checkpoint,
        &run,
        ResumePolicy::RetryInterruptedTurn,
    ))
    .unwrap_err();
    assert!(matches!(
        error,
        AgentError::Context(ContextError::RecoveryMismatch)
    ));
    assert_eq!(model.recorded_requests().len(), 1);
}

#[test]
fn context_legacy_snapshot_revalidates_accounting_and_upgrades_without_reselection() {
    use runifold_core::CheckpointStore;
    let model = ScriptedModel::new();
    model.enqueue_error(ModelError::local(ModelErrorKind::Provider, "interrupted"));
    model.enqueue(response_events(
        "retry",
        vec![ContentPart::text("done")],
        FinishReason::Stop,
    ));
    let strategy = Arc::new(OnceStrategy(AtomicUsize::new(0)));
    let agent = agent(model.clone(), manager(50).with_strategy(strategy.clone()));
    let store = Arc::new(InMemoryCheckpointStore::new());
    let checkpoint = AgentCheckpoint::new(store.clone());
    let run = run();
    futures_executor::block_on(agent.run_checkpointed("original", &run, &checkpoint)).unwrap_err();
    let (envelope, mut state) = checkpoint.load().unwrap();
    state.transcript[0]
        .metadata
        .get_mut(super::context::PLANS)
        .unwrap()["1"]
        .as_object_mut()
        .unwrap()
        .remove("plan_digest");
    let next = envelope.next(serde_json::to_value(state).unwrap()).unwrap();
    store
        .compare_and_swap(&next, Some(envelope.revision))
        .unwrap();
    let outcome = futures_executor::block_on(agent.resume(
        &checkpoint,
        &run,
        ResumePolicy::RetryInterruptedTurn,
    ))
    .unwrap();
    assert_eq!(strategy.0.load(Ordering::SeqCst), 1);
    assert_eq!(model.recorded_requests()[0], model.recorded_requests()[1]);
    assert!(outcome.transcript[0].metadata[super::context::PLANS]["1"]["plan_digest"].is_array());
    assert_eq!(outcome.context_plans().unwrap().len(), 1);
}

#[test]
fn context_legacy_snapshot_cannot_lower_its_stored_token_counts() {
    let request = ModelRequest::new(ModelRef::new("test", "script"), Message::user("input"));
    let manager = manager(50);
    let mut plan = manager.plan(&request, 0, 1).unwrap();
    plan.groups[0].tokens = 0;
    plan.input_tokens = plan.overhead_tokens;
    assert_eq!(
        manager.validate_legacy_accounting(&request, &plan),
        Err(ContextError::RecoveryMismatch)
    );
}

struct OnceCounter {
    overhead: AtomicUsize,
    messages: AtomicUsize,
}
impl ContextTokenCounter for OnceCounter {
    fn descriptor(&self) -> ContextComponentDescriptor {
        ContextComponentDescriptor::new("test.once-counter", 1).unwrap()
    }
    fn is_estimate(&self) -> bool {
        false
    }
    fn count_overhead(&self, _: &ModelRequest) -> Result<u64, ContextError> {
        assert_eq!(
            self.overhead.fetch_add(1, Ordering::SeqCst),
            0,
            "recovery must reuse valid accounting"
        );
        Ok(5)
    }
    fn count_message(&self, _: &Message) -> Result<u64, ContextError> {
        assert_eq!(
            self.messages.fetch_add(1, Ordering::SeqCst),
            0,
            "recovery must reuse valid accounting"
        );
        Ok(5)
    }
}

#[test]
fn context_checksummed_recovery_reuses_counting_as_well_as_selection() {
    let counter = Arc::new(OnceCounter {
        overhead: AtomicUsize::new(0),
        messages: AtomicUsize::new(0),
    });
    let model = ScriptedModel::new();
    model.enqueue_error(ModelError::local(ModelErrorKind::Provider, "interrupted"));
    model.enqueue(response_events(
        "retry",
        vec![ContentPart::text("done")],
        FinishReason::Stop,
    ));
    let agent = agent(model.clone(), manager(50).with_counter(counter.clone()));
    let checkpoint = AgentCheckpoint::new(Arc::new(InMemoryCheckpointStore::new()));
    let run = run();
    futures_executor::block_on(agent.run_checkpointed("input", &run, &checkpoint)).unwrap_err();
    let result = futures_executor::block_on(agent.resume(
        &checkpoint,
        &run,
        ResumePolicy::RetryInterruptedTurn,
    ))
    .unwrap();
    assert_eq!(counter.overhead.load(Ordering::SeqCst), 1);
    assert_eq!(counter.messages.load(Ordering::SeqCst), 1);
    assert_eq!(model.recorded_requests()[0], model.recorded_requests()[1]);
    assert_eq!(result.context_plans().unwrap().len(), 1);
}

#[test]
fn context_all_history_window_cuts_preserve_selected_tool_exchanges() {
    let mut archive = Vec::new();
    for turn in 0..6 {
        archive.push(Message::user(format!("question-{turn}")));
        archive.extend(tool_pair("reused-per-turn"));
        archive.push(message(Role::Assistant, ContentPart::text("answer")));
    }
    for start in 0..=archive.len() {
        let mut request = ModelRequest::new(ModelRef::new("test", "script"), Message::user("now"));
        request.messages = archive[start..].to_vec();
        let current = request.messages.len();
        request.messages.push(Message::user("now"));
        for input in [8, 30, 1_000] {
            let manager = manager(input);
            let plan = manager.plan(&request, current, 1).unwrap();
            assert!(plan.input_tokens <= input);
            assert!(
                plan.groups
                    .iter()
                    .filter(|group| group.kind == ContextGroupKind::IncompleteHistory)
                    .all(|group| !group.selected)
            );
            manager
                .validate_recovered_plan(&request, current, &plan)
                .unwrap();
            // Reprojected input is valid independently of excluded prefix data.
            let selected: std::collections::BTreeSet<usize> = plan
                .groups
                .iter()
                .filter(|group| group.selected)
                .flat_map(|group| group.message_indices.iter().copied())
                .collect();
            let mut pending = 0;
            for (index, message) in request.messages.iter().enumerate() {
                if !selected.contains(&index) {
                    continue;
                }
                for part in &message.content {
                    match part {
                        ContentPart::ToolCall(_) => {
                            assert_eq!(pending, 0);
                            pending += 1;
                        }
                        ContentPart::ToolResult(_) => {
                            assert_eq!(pending, 1);
                            pending -= 1;
                        }
                        _ => {}
                    }
                }
            }
            assert_eq!(pending, 0);
        }
    }
}

#[test]
fn context_recovery_cannot_reinclude_a_window_truncated_prefix() {
    let request = ModelRequest::new(
        ModelRef::new("test", "script"),
        message(Role::Assistant, ContentPart::text("orphan")),
    )
    .message(Message::user("now"));
    let manager = manager(100);
    let mut plan = manager.plan(&request, 1, 1).unwrap();
    assert_eq!(plan.groups[0].kind, ContextGroupKind::IncompleteHistory);
    assert!(!plan.groups[0].selected);
    plan.groups[0].selected = true;
    plan.input_tokens += plan.groups[0].tokens;
    assert_eq!(
        manager.validate_recovered_plan(&request, 1, &plan),
        Err(ContextError::RecoveryMismatch)
    );
}

struct EveryOptional;
impl ContextSelectionStrategy for EveryOptional {
    fn descriptor(&self) -> ContextComponentDescriptor {
        ContextComponentDescriptor::new("test.every-optional", 1).unwrap()
    }
    fn eviction_order(&self, groups: &[ContextGroup]) -> Vec<usize> {
        groups
            .iter()
            .enumerate()
            .filter(|(_, group)| !group.required)
            .map(|(index, _)| index)
            .collect()
    }
}

#[test]
fn context_custom_eviction_does_not_subtract_an_already_excluded_prefix_twice() {
    let request = ModelRequest::new(
        ModelRef::new("test", "script"),
        message(Role::Assistant, ContentPart::text("large orphan prefix")),
    )
    .message(Message::user("a"))
    .message(message(Role::Assistant, ContentPart::text("b")))
    .message(Message::user("now"));
    let plan = manager(8)
        .with_strategy(Arc::new(EveryOptional))
        .plan(&request, 3, 1)
        .unwrap();
    assert_eq!(plan.input_tokens, 8);
    assert!(plan.groups[..2].iter().all(|group| !group.selected));
}

#[test]
fn context_clean_copy_preserves_provider_metadata_and_canonical_history() {
    let mut original = Message::user("input");
    original
        .metadata
        .insert(super::context::INPUT_MARKER.into(), "execution".into());
    original.metadata.insert(
        super::context::PLANS.into(),
        serde_json::json!({"1":{"private":"runtime"}}),
    );
    original.metadata.insert(
        "openai.message_blocks".into(),
        serde_json::json!([{"id":"original","phase":"final_answer"}]),
    );
    let copy = super::context::clean_message(&original);
    assert_eq!(copy.content, original.content);
    assert_eq!(
        copy.metadata["openai.message_blocks"],
        original.metadata["openai.message_blocks"]
    );
    assert_eq!(copy.metadata.len(), 1);
    assert_eq!(original.metadata.len(), 3);
}

#[test]
fn context_zero_output_limit_fails_before_model_dispatch() {
    let model = model();
    let agent = Agent::builder(
        "worker",
        Arc::new(model.clone()),
        ModelRef::new("test", "script"),
    )
    .context_manager(manager(50))
    .max_output_tokens(0)
    .build()
    .unwrap();
    assert!(matches!(
        futures_executor::block_on(agent.prompt("input")),
        Err(AgentError::Context(ContextError::InvalidConfiguration))
    ));
    assert!(model.recorded_requests().is_empty());
}

#[test]
fn context_automatic_summary_finishes_a_partial_tool_turn_before_analysis() {
    use crate::{
        AutomaticConversationSummary, ConversationAppend, ConversationSummaryPassLimit,
        ConversationVersion,
    };
    let store = InMemoryConversationStore::new();
    let id = ConversationId::new();
    let namespace = MemoryNamespace::parse("context.summary-tail").unwrap();
    let mut history = vec![Message::user("old")];
    history.extend(tool_pair("old-tool"));
    history.push(message(Role::Assistant, ContentPart::text("old answer")));
    futures_executor::block_on(store.create(id, namespace.clone())).unwrap();
    futures_executor::block_on(store.append(
        namespace.clone(),
        ConversationAppend {
            conversation_id: id,
            expected_version: ConversationVersion::new(0),
            messages: history,
        },
    ))
    .unwrap();
    let model = model();
    let summaries = self::model();
    let analyst = agent(model.clone(), manager(10_000));
    let summarizer = agent(summaries.clone(), manager(10_000));
    let policy = ConversationContextPolicy::new(ConversationWindow::new(2).unwrap());
    let summary = AutomaticConversationSummary::new(policy, &summarizer)
        .with_pass_limit(ConversationSummaryPassLimit::new(4).unwrap());
    let result = futures_executor::block_on(analyst.run_conversation_with_summary(
        "new",
        &run(),
        &store,
        id,
        namespace,
        summary,
    ))
    .unwrap();
    assert_eq!(summaries.recorded_requests().len(), 2);
    assert_eq!(model.recorded_requests()[0].messages.len(), 2);
    let plans = result.outcome.context_plans().unwrap();
    assert_eq!(plans[0].groups[0].summary_through, Some(4));
    assert!(
        !plans[0]
            .groups
            .iter()
            .any(|group| group.kind == ContextGroupKind::IncompleteHistory)
    );
}

#[test]
fn context_partial_tool_turn_compaction_respects_the_summary_pass_limit() {
    use crate::{
        AgentConversationError, AutomaticConversationSummary, ConversationAppend,
        ConversationSummaryPassLimit, ConversationVersion,
    };
    let store = InMemoryConversationStore::new();
    let id = ConversationId::new();
    let namespace = MemoryNamespace::parse("context.summary-limit").unwrap();
    let mut history = vec![Message::user("old")];
    history.extend(tool_pair("old-tool"));
    history.push(message(Role::Assistant, ContentPart::text("old answer")));
    futures_executor::block_on(store.create(id, namespace.clone())).unwrap();
    futures_executor::block_on(store.append(
        namespace.clone(),
        ConversationAppend {
            conversation_id: id,
            expected_version: ConversationVersion::new(0),
            messages: history,
        },
    ))
    .unwrap();
    let model = model();
    let summaries = self::model();
    let analyst = agent(model.clone(), manager(10_000));
    let summarizer = agent(summaries.clone(), manager(10_000));
    let policy = ConversationContextPolicy::new(ConversationWindow::new(2).unwrap());
    let summary = AutomaticConversationSummary::new(policy, &summarizer)
        .with_pass_limit(ConversationSummaryPassLimit::new(1).unwrap());
    let error = futures_executor::block_on(analyst.run_conversation_with_summary(
        "new",
        &run(),
        &store,
        id,
        namespace,
        summary,
    ))
    .unwrap_err();
    assert!(matches!(
        error,
        AgentConversationError::SummaryPassLimitExceeded {
            remaining_entries: 2,
            ..
        }
    ));
    assert_eq!(summaries.recorded_requests().len(), 1);
    assert!(model.recorded_requests().is_empty());
}
