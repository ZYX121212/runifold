//! Real `PostgreSQL` contracts for checkpoints, effects, and atomic conversation commits.

mod support;

use runifold_agent::{
    ConversationAppend, ConversationId, ConversationStore, ConversationStoreErrorKind,
    ConversationSummaryBatch, ConversationVersion, ConversationWindow, DurableConversationCommit,
    DurableConversationStore, MemoryNamespace,
};
use runifold_core::{
    CapabilityId, Checkpoint, CheckpointStore, EffectClass, EffectId, EffectKind, EffectRequest,
    InvocationId, RunId,
};
use runifold_effect::{EffectExecutorErrorKind, EffectRecord, EffectStatus, EffectStore};
use runifold_model::{ContentPart, Message, Role};
use runifold_store_postgres::PostgresConversationStore;
use serde_json::json;
use tokio_postgres::NoTls;
use uuid::Uuid;

use support::PostgresTestContext;

struct SessionUpgradeModel(std::sync::Arc<std::sync::atomic::AtomicUsize>);
impl runifold_model::Model for SessionUpgradeModel {
    fn capabilities<'a>(
        &'a self,
        _: &'a runifold_model::ModelRef,
    ) -> runifold_model::ModelFuture<
        'a,
        Result<runifold_model::ModelCapabilities, runifold_model::ModelError>,
    > {
        Box::pin(async { Ok(runifold_model::ModelCapabilities::default()) })
    }
    fn stream(
        &self,
        request: runifold_model::ModelRequest,
        _: runifold_model::ModelCallContext,
    ) -> runifold_model::ModelFuture<
        '_,
        Result<runifold_model::ModelEventStream, runifold_model::ModelError>,
    > {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            use runifold_model::{FinishReason, ModelStreamEvent};
            Ok(Box::pin(futures_util::stream::iter(
                [
                    ModelStreamEvent::ResponseStarted {
                        id: Some("session-upgrade".into()),
                        model: request.model,
                    },
                    ModelStreamEvent::ContentPartCompleted {
                        index: 0,
                        part: ContentPart::text("done"),
                    },
                    ModelStreamEvent::ResponseCompleted {
                        finish_reason: FinishReason::Stop,
                        provider_metadata: std::collections::BTreeMap::new(),
                    },
                ]
                .map(Ok),
            )) as runifold_model::ModelEventStream)
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn session_schema_rejection_and_current_replay_survive_reconnection() {
    use runifold_agent::{Agent, AgentSession, AgentSessionError, ConversationContextPolicy};
    use runifold_core::{Budget, BudgetTracker, CapabilitySet, CheckpointId, RunContext};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let database = PostgresTestContext::isolated().await;
    let table = format!("session_upgrade_{}", Uuid::now_v7().simple());
    let store = Arc::new(
        PostgresConversationStore::connect(database.connection_url(), &table)
            .await
            .unwrap(),
    );
    store.ensure_schema().await.unwrap();
    let namespace = MemoryNamespace::parse("session.upgrade").unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let agent = Agent::new(
        "session-upgrade",
        Arc::new(SessionUpgradeModel(calls.clone())),
        runifold_model::ModelRef::new("test", "session"),
    );
    let run = || RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new());
    let policy = ConversationContextPolicy::new(ConversationWindow::new(8).unwrap());
    for schema in [0, 1, 2, 4, u32::MAX] {
        let conversation = ConversationId::new();
        store.create(conversation, namespace.clone()).await.unwrap();
        let old = Checkpoint::initial(
            conversation.as_checkpoint_id(),
            RunId::new(),
            "runifold.conversation.admission",
            schema,
            json!({"legacy":"opaque-private-state"}),
        );
        CheckpointStore::compare_and_swap(store.as_ref(), &old, None).unwrap();
        let reopened = Arc::new(
            PostgresConversationStore::connect(database.connection_url(), &table)
                .await
                .unwrap(),
        );
        let session = AgentSession::new(
            agent.clone(),
            reopened.clone(),
            conversation,
            namespace.clone(),
            policy,
        );
        assert!(matches!(
            session.run(CheckpointId::new(), "new work", &run()).await,
            Err(AgentSessionError::InvalidAdmission)
        ));
        assert_eq!(
            CheckpointStore::load(reopened.as_ref(), conversation.as_checkpoint_id()).unwrap(),
            old
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    let conversation = ConversationId::new();
    let request = CheckpointId::new();
    let session = AgentSession::new(
        agent.clone(),
        store,
        conversation,
        namespace.clone(),
        policy,
    );
    assert_eq!(
        session
            .run(request, "hello", &run())
            .await
            .unwrap()
            .outcome
            .text(),
        "done"
    );
    drop(session);
    let reopened = Arc::new(
        PostgresConversationStore::connect(database.connection_url(), &table)
            .await
            .unwrap(),
    );
    let session = AgentSession::new(agent, reopened, conversation, namespace, policy);
    assert_eq!(
        session
            .run(request, "hello", &run())
            .await
            .unwrap()
            .outcome
            .text(),
        "done"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "replay must not call the Provider again"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn checkpoint_effect_and_atomic_conversation_survive_reconnect() {
    let database = PostgresTestContext::start("RUNIFOLD_TEST_POSTGRES_URL").await;
    let connection_url = database.connection_url().to_owned();
    let table = format!("runifold_ds_{}", Uuid::now_v7().simple());
    let store = PostgresConversationStore::connect(&connection_url, &table)
        .await
        .unwrap();
    store.ensure_schema().await.unwrap();

    let conversation_id = ConversationId::new();
    let namespace = MemoryNamespace::parse("tenant.durable-postgres").unwrap();
    store
        .create(conversation_id, namespace.clone())
        .await
        .unwrap();
    let initial = Checkpoint::initial(
        runifold_core::CheckpointId::new(),
        RunId::new(),
        "runifold.agent.durable-conversation",
        1,
        json!({"phase": "running"}),
    );
    CheckpointStore::compare_and_swap(&store, &initial, None).unwrap();
    let completed = initial.next(json!({"phase": "completed"})).unwrap();
    let version = store
        .commit_durable_turn(DurableConversationCommit {
            namespace: namespace.clone(),
            append: ConversationAppend {
                conversation_id,
                expected_version: ConversationVersion::default(),
                messages: vec![
                    Message::user("question"),
                    Message::new(Role::Assistant, vec![ContentPart::text("answer")]).unwrap(),
                ],
            },
            checkpoint: completed.clone(),
            expected_checkpoint_revision: initial.revision,
        })
        .await
        .unwrap();
    assert_eq!(version.get(), 1);

    let request = effect_request(CapabilityId::new(), "stable-postgres-effect");
    let prepared = EffectRecord::prepared(request.clone());
    EffectStore::compare_and_swap(&store, &prepared, None).unwrap();
    let completed_effect = EffectRecord {
        revision: 1,
        request: request.clone(),
        status: EffectStatus::Completed {
            output: json!({"remote_id": "operation-1"}),
        },
    };
    EffectStore::compare_and_swap(&store, &completed_effect, Some(0)).unwrap();

    let reopened = PostgresConversationStore::connect(&connection_url, &table)
        .await
        .unwrap();
    assert_eq!(
        CheckpointStore::load(&reopened, initial.id).unwrap(),
        completed
    );
    assert_eq!(
        EffectStore::find_by_idempotency(
            &reopened,
            request.capability_id,
            "stable-postgres-effect"
        )
        .unwrap(),
        Some(completed_effect)
    );
    let view = reopened
        .load_view(
            conversation_id,
            namespace.clone(),
            ConversationWindow::new(8).unwrap(),
            ConversationSummaryBatch::new(8).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(view.version.get(), 1);
    assert_eq!(view.window.len(), 2);

    atomic_commit_rolls_back_transcript_when_checkpoint_is_stale(
        &reopened,
        conversation_id,
        namespace,
        &completed,
    )
    .await;
    idempotency_conflict_is_preserved(&reopened, request.capability_id);
    drop_tables(&connection_url, &table).await;
}

async fn atomic_commit_rolls_back_transcript_when_checkpoint_is_stale(
    store: &PostgresConversationStore,
    conversation_id: ConversationId,
    namespace: MemoryNamespace,
    completed: &Checkpoint,
) {
    let externally_advanced = completed.next(json!({"phase": "advanced"})).unwrap();
    CheckpointStore::compare_and_swap(store, &externally_advanced, Some(completed.revision))
        .unwrap();
    let stale_candidate = completed.next(json!({"phase": "stale"})).unwrap();
    let error = store
        .commit_durable_turn(DurableConversationCommit {
            namespace: namespace.clone(),
            append: ConversationAppend {
                conversation_id,
                expected_version: ConversationVersion::new(1),
                messages: vec![Message::user("must roll back")],
            },
            checkpoint: stale_candidate,
            expected_checkpoint_revision: completed.revision,
        })
        .await
        .unwrap_err();
    assert_eq!(error.kind, ConversationStoreErrorKind::Conflict);
    let view = store
        .load_view(
            conversation_id,
            namespace,
            ConversationWindow::new(8).unwrap(),
            ConversationSummaryBatch::new(8).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(view.version.get(), 1);
    assert_eq!(view.window.len(), 2);
}

fn idempotency_conflict_is_preserved(store: &PostgresConversationStore, capability: CapabilityId) {
    let conflicting = EffectRecord::prepared(effect_request(capability, "stable-postgres-effect"));
    let error = EffectStore::compare_and_swap(store, &conflicting, None).unwrap_err();
    assert_eq!(error.kind, EffectExecutorErrorKind::IdempotencyConflict);
}

fn effect_request(capability_id: CapabilityId, key: &str) -> EffectRequest {
    EffectRequest {
        effect_id: EffectId::new(),
        invocation_id: InvocationId::new(),
        kind: EffectKind::Tool,
        capability_id,
        input: json!({"value": 1}),
        effect_class: EffectClass::IdempotentWrite,
        idempotency_key: Some(key.into()),
    }
}

async fn drop_tables(connection_url: &str, table: &str) {
    let (client, connection) = tokio_postgres::connect(connection_url, NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "DROP TABLE {table}_effects, {table}_checkpoints, \
             {table}_memory, {table}_transcript, {table}"
        ))
        .await
        .unwrap();
}
