//! Fault boundaries for checkpointed session compaction.

use futures_util::StreamExt;
use runifold_agent::{
    Agent, AgentConversationError, AgentSession, AgentSessionError, AgentStreamEvent,
    ConversationAppend, ConversationContextPolicy, ConversationId, ConversationStore,
    ConversationStoreErrorKind, ConversationSummaryBatch, ConversationSummaryPassLimit,
    ConversationVersion, ConversationWindow, MemoryNamespace, ResumePolicy,
};
use runifold_core::{
    Budget, BudgetTracker, CapabilitySet, CheckpointId, CheckpointStore, RunContext, Usage,
};
use runifold_model::{ContentPart, FinishReason, Message, ModelRef, ModelStreamEvent, ModelUsage};
use runifold_store_sqlite::SqliteStore;
use runifold_testkit::ScriptedModel;
use std::{collections::BTreeMap, sync::Arc};

struct Fixture {
    store: Arc<SqliteStore>,
    id: ConversationId,
    namespace: MemoryNamespace,
    summary: ScriptedModel,
    main: ScriptedModel,
}

impl Fixture {
    async fn new(store: Arc<SqliteStore>, entries: usize) -> Self {
        let id = ConversationId::new();
        let namespace = MemoryNamespace::parse("session.summary").expect("namespace");
        store
            .create(id, namespace.clone())
            .await
            .expect("conversation");
        store
            .append(
                namespace.clone(),
                ConversationAppend {
                    conversation_id: id,
                    expected_version: ConversationVersion::new(0),
                    messages: (0..entries)
                        .map(|index| Message::user(format!("entry {index}")))
                        .collect(),
                },
            )
            .await
            .expect("seed transcript");
        Self {
            store,
            id,
            namespace,
            summary: ScriptedModel::new(),
            main: ScriptedModel::new(),
        }
    }

    fn session(&self, passes: u16) -> AgentSession {
        AgentSession::new(
            Agent::new(
                "main",
                Arc::new(self.main.clone()),
                ModelRef::new("test", "script"),
            ),
            self.store.clone(),
            self.id,
            self.namespace.clone(),
            ConversationContextPolicy::new(ConversationWindow::new(2).expect("window"))
                .with_summary_batch(ConversationSummaryBatch::new(2).expect("batch")),
        )
        .with_summary_agent(
            Agent::new(
                "summary",
                Arc::new(self.summary.clone()),
                ModelRef::new("test", "script"),
            ),
            ConversationSummaryPassLimit::new(passes).expect("pass limit"),
        )
    }

    fn revision(&self) -> u64 {
        CheckpointStore::load(self.store.as_ref(), self.id.as_checkpoint_id())
            .expect("admission")
            .revision
    }
}

fn run(usage: Usage) -> RunContext {
    RunContext::root(
        BudgetTracker::restore(Budget::default(), usage).expect("usage fits"),
        CapabilitySet::new(),
    )
}

fn response(text: &str) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ResponseStarted {
            id: Some(text.into()),
            model: ModelRef::new("test", "script"),
        },
        ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::text(text),
        },
        ModelStreamEvent::UsageUpdated {
            usage: ModelUsage {
                input_tokens: 5,
                output_tokens: 3,
                ..ModelUsage::default()
            },
        },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::Stop,
            provider_metadata: BTreeMap::new(),
        },
    ]
}

#[tokio::test]
async fn session_stream_compacts_batches_and_replay_skips_all_model_work() {
    let fixture = Fixture::new(Arc::new(SqliteStore::open_in_memory().expect("store")), 6).await;
    fixture.summary.enqueue(response("first summary"));
    fixture.summary.enqueue(response("second summary"));
    fixture.main.enqueue(response("answer"));
    let session = fixture.session(2);
    let request = CheckpointId::new();
    let context = run(Usage::default());
    let events: Vec<_> = session.stream(request, "next", &context).collect().await;
    let events: Vec<_> = events
        .into_iter()
        .collect::<Result<_, _>>()
        .expect("stream");
    let summaries: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            AgentStreamEvent::ConversationSummaryCommitted {
                through_sequence, ..
            } => Some(through_sequence.get()),
            _ => None,
        })
        .collect();
    assert_eq!(summaries, [2, 4]);
    assert!(matches!(
        events.last(),
        Some(AgentStreamEvent::ConversationCommitted { .. })
    ));
    assert_eq!(context.budget().usage().tokens, 24);
    assert_eq!(context.budget().usage().turns, 3);
    let main_request = &fixture.main.recorded_requests()[0];
    let rendered = serde_json::to_string(&main_request.messages).expect("messages serialize");
    assert!(rendered.contains("second summary"));
    assert!(!rendered.contains("entry 0"));
    let replay = session
        .run(request, "next", &run(Usage::default()))
        .await
        .expect("replay");
    assert_eq!(replay.outcome.text(), "answer");
    assert_eq!(fixture.summary.recorded_requests().len(), 2);
    assert_eq!(fixture.main.recorded_requests().len(), 1);
    let transcript = fixture
        .store
        .list_transcript(
            fixture.id,
            fixture.namespace.clone(),
            None,
            ConversationWindow::new(16).expect("window"),
        )
        .await
        .expect("transcript");
    assert_eq!(
        transcript.len(),
        8,
        "summaries do not replace immutable history"
    );
}

#[tokio::test]
async fn dropped_stream_between_summary_batches_restores_usage_without_regenerating() {
    let fixture = Fixture::new(Arc::new(SqliteStore::open_in_memory().expect("store")), 6).await;
    fixture.summary.enqueue(response("first summary"));
    fixture.summary.enqueue(response("second summary"));
    fixture.main.enqueue(response("answer"));
    let session = fixture.session(2);
    let request = CheckpointId::new();
    let context = run(Usage::default());
    let mut stream = session.stream(request, "next", &context);
    loop {
        if matches!(
            stream.next().await.expect("event").expect("success"),
            AgentStreamEvent::ConversationSummaryCommitted { .. }
        ) {
            break;
        }
    }
    drop(stream);
    let usage = session.recovery_usage(request).expect("persisted usage");
    assert_eq!(usage.tokens, 8);
    let changed = fixture.session(3);
    assert!(matches!(
        changed
            .recover_after_owner_exit(
                request,
                "next",
                &run(usage),
                fixture.revision(),
                ResumePolicy::RejectAmbiguous
            )
            .await,
        Err(AgentSessionError::ConfigurationMismatch)
    ));
    assert!(matches!(
        session
            .recover_after_owner_exit(
                request,
                "next",
                &run(Usage::default()),
                fixture.revision(),
                ResumePolicy::RejectAmbiguous
            )
            .await,
        Err(AgentSessionError::UsageMismatch)
    ));
    let result = session
        .recover_after_owner_exit(
            request,
            "next",
            &run(usage),
            fixture.revision(),
            ResumePolicy::RejectAmbiguous,
        )
        .await
        .expect("resume summary and main");
    assert_eq!(result.outcome.usage.tokens, 24);
    assert_eq!(fixture.summary.recorded_requests().len(), 2);
}

#[tokio::test]
async fn summary_commit_conflict_retains_completed_output_without_overwriting_history() {
    let fixture = Fixture::new(Arc::new(SqliteStore::open_in_memory().expect("store")), 4).await;
    fixture.summary.enqueue(response("stale summary"));
    let session = fixture.session(2);
    let request = CheckpointId::new();
    let context = run(Usage::default());
    let mut stream = session.stream(request, "next", &context);
    assert!(matches!(
        stream.next().await,
        Some(Ok(AgentStreamEvent::ConversationSummaryStarted { .. }))
    ));
    fixture
        .store
        .append(
            fixture.namespace.clone(),
            ConversationAppend {
                conversation_id: fixture.id,
                expected_version: ConversationVersion::new(1),
                messages: vec![Message::user("external writer")],
            },
        )
        .await
        .expect("inject concurrent transcript change");
    let events: Vec<_> = stream.collect().await;
    assert!(
        matches!(events.last(), Some(Err(AgentSessionError::Conversation(error))) if matches!(error.as_ref(), AgentConversationError::Store(error) if error.kind == ConversationStoreErrorKind::Conflict))
    );
    assert!(!events.iter().any(|event| matches!(
        event,
        Ok(AgentStreamEvent::ConversationCommitted { .. }
            | AgentStreamEvent::ConversationSummaryCommitted { .. })
    )));
    let usage = session
        .recovery_usage(request)
        .expect("summary checkpoint usage");
    assert_eq!(usage.tokens, 8);
    let mut incorrect_usage = usage;
    incorrect_usage.tokens += 1;
    assert!(
        session
            .recover_after_owner_exit(
                request,
                "next",
                &run(incorrect_usage),
                fixture.revision(),
                ResumePolicy::RejectAmbiguous
            )
            .await
            .is_err()
    );
    assert_eq!(
        session.recovery_usage(request).expect("unchanged usage"),
        usage,
        "a rejected restore must not poison persisted accounting"
    );
    assert!(
        session
            .recover_after_owner_exit(
                request,
                "next",
                &run(usage),
                fixture.revision(),
                ResumePolicy::RejectAmbiguous
            )
            .await
            .is_err()
    );
    assert_eq!(
        fixture.summary.recorded_requests().len(),
        1,
        "completed summary is reused on retry"
    );
    assert!(fixture.main.recorded_requests().is_empty());
}

#[tokio::test]
async fn summary_pass_limit_persists_across_recovery() {
    let fixture = Fixture::new(Arc::new(SqliteStore::open_in_memory().expect("store")), 6).await;
    fixture.summary.enqueue(response("one pass"));
    let session = fixture.session(1);
    let request = CheckpointId::new();
    let error = session
        .run(request, "next", &run(Usage::default()))
        .await
        .expect_err("limit");
    assert!(
        matches!(error, AgentSessionError::Conversation(error) if matches!(error.as_ref(), AgentConversationError::SummaryPassLimitExceeded { remaining_entries: 2, .. }))
    );
    let usage = session.recovery_usage(request).expect("usage");
    assert!(
        session
            .recover_after_owner_exit(
                request,
                "next",
                &run(usage),
                fixture.revision(),
                ResumePolicy::RejectAmbiguous
            )
            .await
            .is_err()
    );
    assert_eq!(fixture.summary.recorded_requests().len(), 1);
    assert!(fixture.main.recorded_requests().is_empty());
}

#[tokio::test]
async fn committed_summary_survives_failed_progress_ack_and_reopen() {
    let path =
        std::env::temp_dir().join(format!("runifold-summary-{}.sqlite", CheckpointId::new()));
    let fixture = Fixture::new(Arc::new(SqliteStore::open(&path).expect("store")), 4).await;
    fixture.summary.enqueue(response("durable summary"));
    fixture.main.enqueue(response("answer"));
    let session = fixture.session(1);
    let connection = rusqlite::Connection::open(&path).expect("fault injection connection");
    connection
        .execute_batch(
            "CREATE TRIGGER fail_summary_progress BEFORE UPDATE ON runifold_checkpoints
        WHEN json_extract(NEW.record_json, '$.kind') = 'runifold.conversation.admission'
        AND json_extract(NEW.record_json, '$.payload.active.summary.completed') = 1
        BEGIN SELECT RAISE(ABORT, 'injected failure after summary commit'); END;",
        )
        .expect("fault trigger");
    let request = CheckpointId::new();
    assert!(
        session
            .run(request, "next", &run(Usage::default()))
            .await
            .is_err()
    );
    assert!(fixture.main.recorded_requests().is_empty());
    let view = fixture
        .store
        .load_view(
            fixture.id,
            fixture.namespace.clone(),
            ConversationWindow::new(2).expect("window"),
            ConversationSummaryBatch::new(2).expect("batch"),
        )
        .await
        .expect("view");
    let summary_id = view.summary.expect("summary already committed").summary_id;
    connection
        .execute_batch("DROP TRIGGER fail_summary_progress;")
        .expect("clear injected fault");
    let reopened = Fixture {
        store: Arc::new(SqliteStore::open(&path).expect("reopen")),
        id: fixture.id,
        namespace: fixture.namespace.clone(),
        summary: fixture.summary.clone(),
        main: fixture.main.clone(),
    };
    let session = reopened.session(1);
    let usage = session
        .recovery_usage(request)
        .expect("persisted summary usage");
    session
        .recover_after_owner_exit(
            request,
            "next",
            &run(usage),
            reopened.revision(),
            ResumePolicy::RejectAmbiguous,
        )
        .await
        .expect("recognize existing summary commit");
    assert_eq!(fixture.summary.recorded_requests().len(), 1);
    let view = reopened
        .store
        .load_view(
            reopened.id,
            reopened.namespace.clone(),
            ConversationWindow::new(2).expect("window"),
            ConversationSummaryBatch::new(2).expect("batch"),
        )
        .await
        .expect("view");
    assert_eq!(
        view.summary.expect("summary").summary_id,
        summary_id,
        "ack loss must not replace the summary"
    );
    std::fs::remove_file(path).expect("remove temporary database");
}

#[tokio::test]
async fn interrupted_summary_requires_explicit_retry_and_keeps_spent_turns() {
    let fixture = Fixture::new(Arc::new(SqliteStore::open_in_memory().expect("store")), 4).await;
    fixture
        .summary
        .enqueue_error(runifold_model::ModelError::local(
            runifold_model::ModelErrorKind::Transport,
            "injected disconnect",
        ));
    fixture.summary.enqueue(response("recovered summary"));
    fixture.main.enqueue(response("answer"));
    let session = fixture.session(1);
    let request = CheckpointId::new();
    assert!(
        session
            .run(request, "next", &run(Usage::default()))
            .await
            .is_err()
    );
    let usage = session.recovery_usage(request).expect("usage floor");
    assert_eq!(usage.turns, 1);
    assert!(matches!(
        session
            .recover_after_owner_exit(
                request,
                "next",
                &run(Usage::default()),
                fixture.revision(),
                ResumePolicy::RetryInterruptedTurn
            )
            .await,
        Err(AgentSessionError::UsageMismatch)
    ));
    let error = session
        .recover_after_owner_exit(
            request,
            "next",
            &run(usage),
            fixture.revision(),
            ResumePolicy::RejectAmbiguous,
        )
        .await
        .expect_err("ambiguous summary must not retry implicitly");
    assert!(
        matches!(error, AgentSessionError::Conversation(error) if matches!(error.as_ref(), AgentConversationError::Run(runifold_agent::AgentError::AmbiguousCheckpoint { .. })))
    );
    assert_eq!(fixture.summary.recorded_requests().len(), 1);
    let outcome = session
        .recover_after_owner_exit(
            request,
            "next",
            &run(usage),
            fixture.revision(),
            ResumePolicy::RetryInterruptedTurn,
        )
        .await
        .expect("explicit retry");
    assert_eq!(outcome.outcome.usage.turns, 3);
    assert_eq!(fixture.summary.recorded_requests().len(), 2);
}

#[tokio::test]
async fn summary_and_main_agent_share_one_hard_budget() {
    let fixture = Fixture::new(Arc::new(SqliteStore::open_in_memory().expect("store")), 4).await;
    fixture
        .summary
        .enqueue(response("summary uses the available turn"));
    let session = fixture.session(1);
    let request = CheckpointId::new();
    let context = RunContext::root(
        BudgetTracker::new(Budget {
            turns: Some(1),
            ..Budget::default()
        }),
        CapabilitySet::new(),
    );
    assert!(session.run(request, "next", &context).await.is_err());
    assert_eq!(fixture.summary.recorded_requests().len(), 1);
    assert!(fixture.main.recorded_requests().is_empty());
    assert_eq!(context.budget().usage().turns, 1);
    assert_eq!(
        session
            .recovery_usage(request)
            .expect("main checkpoint usage")
            .turns,
        1
    );
}
