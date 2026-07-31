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
