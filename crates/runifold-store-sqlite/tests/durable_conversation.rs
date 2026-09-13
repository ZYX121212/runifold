//! `SQLite` durable-conversation atomicity and recovery tests.

use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use runifold_agent::{
    Agent, AgentCheckpoint, AgentCheckpointPhase, ConversationAppend, ConversationContextPolicy,
    ConversationId, ConversationSequence, ConversationStore, ConversationSummaryBatch,
    ConversationSummaryCommit, ConversationVersion, ConversationWindow, DurableConversationCommit,
    DurableConversationRequest, DurableConversationStore, MemoryNamespace, SemanticMemoryId,
    SemanticMemoryQuery, SemanticMemorySource, SemanticMemoryUpsert,
};
use runifold_core::{
    Budget, BudgetTracker, CapabilitySet, Checkpoint, CheckpointId, CheckpointStore, RunContext,
    RunId,
};
use runifold_model::{ContentPart, FinishReason, Message, ModelRef, ModelStreamEvent};
use runifold_store_sqlite::SqliteStore;
use runifold_testkit::ScriptedModel;
use uuid::Uuid;

const CRASH_CHILD_ENV: &str = "RUNIFOLD_DURABLE_CONVERSATION_CHILD";
const DATABASE_ENV: &str = "RUNIFOLD_DURABLE_CONVERSATION_DATABASE";
const CHECKPOINT_ENV: &str = "RUNIFOLD_DURABLE_CONVERSATION_CHECKPOINT";
const CONVERSATION_ENV: &str = "RUNIFOLD_DURABLE_CONVERSATION_ID";
const CRASH_TEST_NAME: &str = "committed_turn_recovers_after_process_exit_before_ack";
const CRASH_EXIT_CODE: i32 = 87;

#[tokio::test]
async fn durable_turn_atomically_survives_reopen_and_resume() {
    let path = database_path();
    let store = Arc::new(SqliteStore::open(&path).expect("SQLite store opens"));
    let model = ScriptedModel::new();
    model.enqueue(response_events("terminal", "persisted"));
    let agent = Agent::new(
        "durable-assistant",
        Arc::new(model.clone()),
        ModelRef::new("test", "durable-script"),
    );
    let conversation_id = ConversationId::new();
    let checkpoint_id = CheckpointId::new();
    let namespace = MemoryNamespace::parse("tenant.user").expect("namespace is valid");
    let policy = ConversationContextPolicy::new(
        ConversationWindow::new(8).expect("window is in the documented range"),
    );

    let first = agent
        .run_durable_conversation(
            "remember this",
            &root_run(),
            store.clone(),
            DurableConversationRequest {
                checkpoint_id,
                conversation_id,
                namespace: namespace.clone(),
                policy,
            },
        )
        .await
        .expect("durable turn commits");
    assert_eq!(first.conversation_version.get(), 1);
    assert_eq!(model.recorded_requests().len(), 1);
    drop(store);

    let reopened = Arc::new(SqliteStore::open(&path).expect("SQLite store reopens"));
    let transcript = reopened
        .list_transcript(
            conversation_id,
            namespace,
            None,
            ConversationWindow::new(8).expect("window is in the documented range"),
        )
        .await
        .expect("transcript survives reopen");
    assert_eq!(transcript.len(), 2);
    let checkpoint = AgentCheckpoint::existing(checkpoint_id, reopened.clone());
    let (_, state) = checkpoint.load().expect("checkpoint survives reopen");
    assert!(matches!(
        state.phase,
        AgentCheckpointPhase::Completed { .. }
    ));

    let resumed = agent
        .resume_durable_conversation(
            reopened,
            checkpoint_id,
            &root_run(),
            runifold_agent::ResumePolicy::RejectAmbiguous,
        )
        .await
        .expect("committed durable turn resumes without execution");
    assert_eq!(resumed.conversation_version.get(), 1);
    assert_eq!(model.recorded_requests().len(), 1);

    fs::remove_file(path).expect("temporary SQLite database is removable");
}

#[tokio::test]
async fn checkpoint_conflict_rolls_back_transcript_append() {
    let store = SqliteStore::open_in_memory().expect("SQLite store opens");
    let conversation_id = ConversationId::new();
    let namespace = MemoryNamespace::parse("tenant.rollback").expect("namespace is valid");
    store
        .create(conversation_id, namespace.clone())
        .await
        .expect("conversation is created");
    let checkpoint_id = CheckpointId::new();
    let initial = Checkpoint::initial(
        checkpoint_id,
        RunId::new(),
        "test.durable",
        1,
        serde_json::json!({"phase": "initial"}),
    );
    CheckpointStore::compare_and_swap(&store, &initial, None)
        .expect("initial checkpoint is created");
    let completed = initial
        .next(serde_json::json!({"phase": "completed"}))
        .expect("checkpoint revision advances");

    let error = store
        .commit_durable_turn(DurableConversationCommit {
            namespace: namespace.clone(),
            append: ConversationAppend {
                conversation_id,
                expected_version: ConversationVersion::new(0),
                messages: vec![Message::user("must roll back")],
            },
            checkpoint: completed,
            expected_checkpoint_revision: 7,
        })
        .await
        .expect_err("stale checkpoint precondition conflicts");
    assert_eq!(
        error.kind,
        runifold_agent::ConversationStoreErrorKind::Conflict
    );
    let transcript = store
        .list_transcript(
            conversation_id,
            namespace,
            None,
            ConversationWindow::new(8).expect("window is in the documented range"),
        )
        .await
        .expect("conversation remains readable");
    assert!(transcript.is_empty());
    assert_eq!(
        CheckpointStore::load(&store, checkpoint_id)
            .expect("checkpoint remains readable")
            .revision,
        0
    );
}

#[tokio::test]
async fn committed_turn_recovers_after_process_exit_before_ack() {
    if env::var_os(CRASH_CHILD_ENV).is_some() {
        run_crash_child().await;
        panic!("child must exit after the durable commit");
    }

    let directory = env::temp_dir().join(format!("runifold-durable-crash-{}", CheckpointId::new()));
    fs::create_dir(&directory).expect("temporary directory is created");
    let database = directory.join("runifold.sqlite3");
    let checkpoint_id = CheckpointId::new();
    let conversation_id = ConversationId::new();
    let status = Command::new(env::current_exe().expect("test executable is available"))
        .arg("--exact")
        .arg(CRASH_TEST_NAME)
        .arg("--nocapture")
        .env(CRASH_CHILD_ENV, "1")
        .env(DATABASE_ENV, &database)
        .env(CHECKPOINT_ENV, checkpoint_id.to_string())
        .env(
            CONVERSATION_ENV,
            conversation_id.as_checkpoint_id().to_string(),
        )
        .status()
        .expect("crash child starts");
    assert_eq!(status.code(), Some(CRASH_EXIT_CODE));

    let store = Arc::new(SqliteStore::open(&database).expect("committed database reopens"));
    let model = ScriptedModel::new();
    let agent = durable_agent(model.clone());
    let outcome = agent
        .resume_durable_conversation(
            store.clone(),
            checkpoint_id,
            &root_run(),
            runifold_agent::ResumePolicy::RejectAmbiguous,
        )
        .await
        .expect("committed turn resumes after response loss");
    assert_eq!(outcome.conversation_version.get(), 1);
    assert!(model.recorded_requests().is_empty());
    let transcript = store
        .list_transcript(
            conversation_id,
            MemoryNamespace::parse("tenant.crash").expect("namespace is valid"),
            None,
            ConversationWindow::new(8).expect("window is in the documented range"),
        )
        .await
        .expect("committed transcript survives child exit");
    assert_eq!(transcript.len(), 2);
    drop(store);
    fs::remove_dir_all(directory).expect("temporary directory is removable");
}

#[tokio::test]
async fn complete_conversation_state_survives_reopen() {
    let path = database_path();
    let store = SqliteStore::open(&path).expect("SQLite store opens");
    let conversation_id = ConversationId::new();
    let namespace = MemoryNamespace::parse("tenant.memory").expect("namespace is valid");
    store
        .create(conversation_id, namespace.clone())
        .await
        .expect("conversation is created");
    store
        .append(
            namespace.clone(),
            ConversationAppend {
                conversation_id,
                expected_version: ConversationVersion::new(0),
                messages: vec![
                    Message::user("I prefer Rust"),
                    Message::new(
                        runifold_model::Role::Assistant,
                        vec![ContentPart::text("Preference recorded")],
                    )
                    .expect("assistant message is valid"),
                    Message::user("Continue"),
                    Message::new(
                        runifold_model::Role::Assistant,
                        vec![ContentPart::text("Continuing")],
                    )
                    .expect("assistant message is valid"),
                ],
            },
        )
        .await
        .expect("transcript is appended");
    store
        .commit_summary(
            namespace.clone(),
            ConversationSummaryCommit {
                conversation_id,
                expected_version: ConversationVersion::new(1),
                through_sequence: ConversationSequence::new(2)
                    .expect("summary sequence is positive"),
                content: "The user prefers Rust.".into(),
            },
        )
        .await
        .expect("summary is committed");
    let memory_id = SemanticMemoryId::new();
    store
        .upsert_memory(SemanticMemoryUpsert {
            memory_id,
            namespace: namespace.clone(),
            content: "User prefers Rust for systems work".into(),
            sources: vec![SemanticMemorySource {
                conversation_id,
                from_sequence: ConversationSequence::new(1).expect("source sequence is positive"),
                through_sequence: ConversationSequence::new(2)
                    .expect("source sequence is positive"),
            }],
            metadata: BTreeMap::new(),
            expected_revision: None,
        })
        .await
        .expect("semantic memory is persisted");
    drop(store);

    let reopened = SqliteStore::open(&path).expect("SQLite store reopens");
    let view = reopened
        .load_view(
            conversation_id,
            namespace.clone(),
            ConversationWindow::new(2).expect("window is in the documented range"),
            ConversationSummaryBatch::new(2).expect("batch is in the documented range"),
        )
        .await
        .expect("conversation view survives reopen");
    assert_eq!(
        view.summary.expect("summary survives reopen").content,
        "The user prefers Rust."
    );
    let memories = reopened
        .search_memory(
            SemanticMemoryQuery::new(namespace, "Rust preference", 4)
                .expect("memory query is valid"),
        )
        .await
        .expect("memory search survives reopen");
    assert_eq!(memories.len(), 1);
    assert_eq!(memories[0].memory_id, memory_id);
    drop(reopened);
    fs::remove_file(path).expect("temporary SQLite database is removable");
}

async fn run_crash_child() {
    let database = Path::new(&required_env(DATABASE_ENV)).to_path_buf();
    let checkpoint_id = parse_checkpoint_id(&required_env(CHECKPOINT_ENV));
    let conversation_id =
        ConversationId::from_checkpoint_id(parse_checkpoint_id(&required_env(CONVERSATION_ENV)));
    let store = Arc::new(SqliteStore::open(database).expect("child SQLite store opens"));
    let model = ScriptedModel::new();
    model.enqueue(response_events("child-terminal", "committed before exit"));
    durable_agent(model)
        .run_durable_conversation(
            "persist before acknowledgement",
            &root_run(),
            store,
            DurableConversationRequest {
                checkpoint_id,
                conversation_id,
                namespace: MemoryNamespace::parse("tenant.crash").expect("namespace is valid"),
                policy: ConversationContextPolicy::new(
                    ConversationWindow::new(8).expect("window is in the documented range"),
                ),
            },
        )
        .await
        .expect("child durable turn commits");
    std::process::exit(CRASH_EXIT_CODE);
}

fn durable_agent(model: ScriptedModel) -> Agent {
    Agent::new(
        "durable-assistant",
        Arc::new(model),
        ModelRef::new("test", "durable-script"),
    )
}

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("missing child environment variable `{name}`"))
}

fn parse_checkpoint_id(value: &str) -> CheckpointId {
    CheckpointId::from_uuid(Uuid::parse_str(value).expect("checkpoint UUID is valid"))
}

fn root_run() -> RunContext {
    RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new())
}

fn response_events(id: &str, text: &str) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ResponseStarted {
            id: Some(id.into()),
            model: ModelRef::new("test", "durable-script"),
        },
        ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::text(text),
        },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::Stop,
            provider_metadata: BTreeMap::new(),
        },
    ]
}

fn database_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "runifold-durable-conversation-{}.sqlite3",
        CheckpointId::new()
    ))
}

#[tokio::test]
async fn durable_stream_reports_commit_and_replays_without_model_work() {
    use futures_util::StreamExt;
    use runifold_agent::{AgentStreamEvent, ResumePolicy};
    let store = Arc::new(SqliteStore::open_in_memory().expect("store opens"));
    let model = ScriptedModel::new();
    model.enqueue(response_events("stream", "saved"));
    let agent = Agent::new(
        "stream",
        Arc::new(model.clone()),
        ModelRef::new("test", "script"),
    );
    let id = CheckpointId::new();
    let run = root_run();
    let mut stream = agent.stream_durable_conversation(
        "hello",
        &run,
        store.clone(),
        DurableConversationRequest {
            checkpoint_id: id,
            conversation_id: ConversationId::new(),
            namespace: MemoryNamespace::parse("stream").expect("namespace"),
            policy: ConversationContextPolicy::new(ConversationWindow::new(8).expect("window")),
        },
    );
    let mut committed = 0;
    while let Some(event) = stream.next().await {
        let event = event.expect("stream succeeds");
        assert!(!matches!(event, AgentStreamEvent::Completed { .. }));
        if let AgentStreamEvent::ConversationCommitted {
            conversation_version,
            ..
        } = event
        {
            committed += 1;
            assert_eq!(conversation_version.get(), 1);
            let (_, state) = AgentCheckpoint::existing(id, store.clone())
                .load()
                .expect("checkpoint");
            assert!(matches!(
                state.phase,
                AgentCheckpointPhase::Completed { .. }
            ));
        }
    }
    assert_eq!(committed, 1);
    assert!(stream.next().await.is_none());
    let run = root_run();
    let events: Vec<_> = agent
        .stream_resume_durable_conversation(store, id, &run, ResumePolicy::RejectAmbiguous)
        .collect()
        .await;
    assert!(matches!(
        events.as_slice(),
        [Ok(AgentStreamEvent::ConversationCommitted { .. })]
    ));
    assert_eq!(model.recorded_requests().len(), 1);
}

#[tokio::test]
async fn durable_stream_does_not_report_success_when_commit_conflicts() {
    use futures_util::StreamExt;
    use runifold_agent::{AgentConversationError, AgentStreamEvent};
    let store = Arc::new(SqliteStore::open_in_memory().expect("store opens"));
    let model = ScriptedModel::new();
    model.enqueue(response_events("stream", "lost commit"));
    let agent = Agent::new("stream", Arc::new(model), ModelRef::new("test", "script"));
    let conversation_id = ConversationId::new();
    let namespace = MemoryNamespace::parse("conflict").expect("namespace");
    let run = root_run();
    let mut stream = agent.stream_durable_conversation(
        "hello",
        &run,
        store.clone(),
        DurableConversationRequest {
            checkpoint_id: CheckpointId::new(),
            conversation_id,
            namespace: namespace.clone(),
            policy: ConversationContextPolicy::new(ConversationWindow::new(8).expect("window")),
        },
    );
    assert!(matches!(
        stream.next().await,
        Some(Ok(AgentStreamEvent::Started { .. }))
    ));
    store
        .append(
            namespace,
            ConversationAppend {
                conversation_id,
                expected_version: ConversationVersion::new(0),
                messages: vec![Message::user("competing writer")],
            },
        )
        .await
        .expect("competing commit succeeds");
    let events: Vec<_> = stream.collect().await;
    assert!(!events.iter().any(|event| matches!(
        event,
        Ok(AgentStreamEvent::Completed { .. } | AgentStreamEvent::ConversationCommitted { .. })
    )));
    assert!(matches!(
        events.last(),
        Some(Err(AgentConversationError::Commit { .. }))
    ));
}

#[tokio::test]
async fn session_rejects_other_connection_before_work_and_recovers_dropped_stream() {
    use futures_util::StreamExt;
    use runifold_agent::{AgentSession, AgentSessionError, AgentStreamEvent, ResumePolicy};
    let path = database_path();
    let first = Arc::new(SqliteStore::open(&path).expect("first connection"));
    let second = Arc::new(SqliteStore::open(&path).expect("second connection"));
    let model = ScriptedModel::new();
    model.enqueue(response_events("recovered", "done"));
    let agent = Agent::new(
        "session",
        Arc::new(model.clone()),
        ModelRef::new("test", "script"),
    );
    let conversation_id = ConversationId::new();
    let namespace = MemoryNamespace::parse("session").expect("namespace");
    let policy = ConversationContextPolicy::new(ConversationWindow::new(8).expect("window"));
    let session = AgentSession::new(
        agent.clone(),
        first,
        conversation_id,
        namespace.clone(),
        policy,
    );
    let other = AgentSession::new(agent, second, conversation_id, namespace, policy);
    let request_id = CheckpointId::new();
    let run = root_run();
    let mut stream = session.stream(request_id, "hello", &run);
    assert!(matches!(
        stream.next().await,
        Some(Ok(AgentStreamEvent::Started { .. }))
    ));
    let error = other
        .run(CheckpointId::new(), "other", &root_run())
        .await
        .expect_err("occupied");
    let AgentSessionError::Busy {
        request_id: active,
        revision,
    } = error
    else {
        panic!("expected busy")
    };
    assert_eq!(active, request_id);
    assert!(model.recorded_requests().is_empty());
    assert!(matches!(
        other.run(request_id, "changed", &root_run()).await,
        Err(AgentSessionError::RequestMismatch)
    ));
    drop(stream); // The prior owner has now stopped polling and is gone.
    let result = other
        .recover_after_owner_exit(
            request_id,
            "hello",
            &root_run(),
            revision,
            ResumePolicy::RejectAmbiguous,
        )
        .await
        .expect("safe recovery");
    assert_eq!(result.outcome.text(), "done");
    assert_eq!(
        other
            .run(request_id, "hello", &root_run())
            .await
            .expect("idempotent retry")
            .conversation_version
            .get(),
        1
    );
    assert_eq!(model.recorded_requests().len(), 1);
    assert!(matches!(
        other.run(request_id, "changed", &root_run()).await,
        Err(AgentSessionError::RequestMismatch)
    ));
    fs::remove_file(path).expect("temporary database removable");
}

#[tokio::test]
async fn resume_rejects_changed_contract_and_legacy_unbound_checkpoint() {
    let store = Arc::new(SqliteStore::open_in_memory().expect("store opens"));
    let model = ScriptedModel::new();
    model.enqueue(response_events("terminal", "done"));
    let agent = Agent::new(
        "contract",
        Arc::new(model.clone()),
        ModelRef::new("test", "script"),
    );
    let checkpoint = AgentCheckpoint::new(store.clone());
    agent
        .run_checkpointed("hello", &root_run(), &checkpoint)
        .await
        .expect("checkpoint committed");
    let changed = agent.clone().system("new policy");
    assert!(
        changed
            .resume(
                &checkpoint,
                &root_run(),
                runifold_agent::ResumePolicy::RejectAmbiguous
            )
            .await
            .is_err()
    );
    let (envelope, _) = checkpoint.load().expect("checkpoint");
    let mut payload = envelope.payload.clone();
    payload
        .as_object_mut()
        .expect("object")
        .remove("recovery_contract");
    let legacy = envelope.next(payload).expect("next revision");
    CheckpointStore::compare_and_swap(store.as_ref(), &legacy, Some(envelope.revision))
        .expect("legacy fixture");
    assert!(
        agent
            .resume(
                &checkpoint,
                &root_run(),
                runifold_agent::ResumePolicy::RejectAmbiguous
            )
            .await
            .is_err()
    );
    assert_eq!(model.recorded_requests().len(), 1);
}

#[tokio::test]
async fn admission_binds_agent_before_first_execution_checkpoint() {
    use runifold_agent::{AgentSession, AgentSessionError, ResumePolicy};
    let path = database_path();
    let store = Arc::new(SqliteStore::open(&path).expect("store"));
    let connection = rusqlite::Connection::open(&path).expect("fault connection");
    connection
        .execute_batch(
            "CREATE TRIGGER fail_first_agent BEFORE INSERT ON runifold_checkpoints
         WHEN json_extract(NEW.record_json, '$.kind') != 'runifold.conversation.admission'
         BEGIN SELECT RAISE(ABORT, 'before first Agent checkpoint'); END;",
        )
        .expect("inject checkpoint failure");
    let model = ScriptedModel::new();
    model.enqueue(response_events("recovery", "done"));
    let agent = durable_agent(model.clone());
    let conversation = ConversationId::new();
    let namespace = MemoryNamespace::parse("admission.definition").expect("namespace");
    let policy = ConversationContextPolicy::new(ConversationWindow::new(8).expect("window"));
    let session = AgentSession::new(
        agent.clone(),
        store.clone(),
        conversation,
        namespace.clone(),
        policy,
    );
    let request = CheckpointId::new();
    assert!(session.run(request, "hello", &root_run()).await.is_err());
    assert!(model.recorded_requests().is_empty());
    assert!(
        AgentCheckpoint::existing(request, store.clone())
            .load()
            .is_err()
    );
    connection
        .execute_batch("DROP TRIGGER fail_first_agent;")
        .expect("remove fault");
    let revision = CheckpointStore::load(store.as_ref(), conversation.as_checkpoint_id())
        .expect("admission")
        .revision;
    let changed = AgentSession::new(
        agent.system("changed execution"),
        store.clone(),
        conversation,
        namespace,
        policy,
    );
    assert!(matches!(
        changed
            .recover_after_owner_exit(
                request,
                "hello",
                &root_run(),
                revision,
                ResumePolicy::RejectAmbiguous
            )
            .await,
        Err(AgentSessionError::ConfigurationMismatch)
    ));
    assert!(
        model.recorded_requests().is_empty(),
        "rejected recovery must not invoke the model"
    );
    let result = session
        .recover_after_owner_exit(
            request,
            "hello",
            &root_run(),
            revision,
            ResumePolicy::RejectAmbiguous,
        )
        .await
        .expect("same definition recovers");
    assert_eq!(result.outcome.text(), "done");
    assert_eq!(model.recorded_requests().len(), 1);
    drop((changed, session, store, connection));
    fs::remove_file(path).expect("remove database");
}
