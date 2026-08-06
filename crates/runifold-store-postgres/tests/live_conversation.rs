//! Disposable `PostgreSQL` conversation-store integration tests.

mod support;

use std::collections::BTreeMap;

use runifold_agent::{
    ConversationAppend, ConversationId, ConversationSequence, ConversationStore,
    ConversationStoreErrorKind, ConversationSummaryBatch, ConversationSummaryCommit,
    ConversationVersion, ConversationWindow, MemoryNamespace, SemanticMemoryId,
    SemanticMemoryQuery, SemanticMemorySource, SemanticMemoryUpsert,
};
use runifold_model::{ArtifactError, ArtifactScope, ArtifactStore, ArtifactWrite, Message};
use runifold_store_postgres::PostgresConversationStore;
use serde_json::json;
use tokio_postgres::NoTls;
use uuid::Uuid;

pub use support::PostgresTestContext;

#[tokio::test]
async fn transcript_summary_memory_and_concurrent_cas_survive_reconnect() {
    let database = PostgresTestContext::start("RUNIFOLD_TEST_POSTGRES_URL").await;
    let connection_url = database.connection_url().to_owned();
    let suffix = Uuid::now_v7().simple().to_string();
    let table = format!("runifold_cv_{suffix}");
    let first = PostgresConversationStore::connect(&connection_url, &table)
        .await
        .unwrap();
    let second = PostgresConversationStore::connect(&connection_url, &table)
        .await
        .unwrap();
    first.ensure_schema().await.unwrap();
    let conversation_id = ConversationId::new();
    let namespace = MemoryNamespace::parse("tenant.postgres").unwrap();
    first
        .create(conversation_id, namespace.clone())
        .await
        .unwrap();

    let first_append = ConversationAppend {
        conversation_id,
        expected_version: ConversationVersion::default(),
        messages: vec![Message::user(
            "Rust is the preferred implementation language",
        )],
    };
    let second_append = ConversationAppend {
        conversation_id,
        expected_version: ConversationVersion::default(),
        messages: vec![Message::user("this concurrent writer must lose")],
    };
    let (left, right) = tokio::join!(
        first.append(namespace.clone(), first_append),
        second.append(namespace.clone(), second_append),
    );
    assert_eq!(
        [left.as_ref(), right.as_ref()]
            .into_iter()
            .flatten()
            .count(),
        1
    );
    let conflict = [left, right]
        .into_iter()
        .find_map(Result::err)
        .expect("one concurrent writer must conflict");
    assert_eq!(conflict.kind, ConversationStoreErrorKind::Conflict);

    let reopened = PostgresConversationStore::connect(&connection_url, &table)
        .await
        .unwrap();
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
    assert_eq!(view.window.len(), 1);
    let bounded_version =
        append_and_assert_bounded(&reopened, conversation_id, namespace.clone(), view.version)
            .await;
    reopened
        .commit_summary(
            namespace.clone(),
            ConversationSummaryCommit {
                conversation_id,
                expected_version: bounded_version,
                through_sequence: ConversationSequence::new(3).unwrap(),
                content: "The user stated a preferred implementation language.".into(),
            },
        )
        .await
        .unwrap();
    let memory = reopened
        .upsert_memory(SemanticMemoryUpsert {
            memory_id: SemanticMemoryId::new(),
            namespace: namespace.clone(),
            content: "The user prefers Rust for implementation work.".into(),
            sources: vec![SemanticMemorySource {
                conversation_id,
                from_sequence: ConversationSequence::new(1).unwrap(),
                through_sequence: ConversationSequence::new(1).unwrap(),
            }],
            metadata: BTreeMap::from([("kind".into(), json!("preference"))]),
            expected_revision: None,
        })
        .await
        .unwrap();
    let found = reopened
        .search_memory(SemanticMemoryQuery::new(namespace, "Rust implementation", 4).unwrap())
        .await
        .unwrap();
    assert_eq!(found, vec![memory]);

    assert_artifacts(&reopened, &second).await;

    drop_tables(&connection_url, &table).await;
}

async fn assert_artifacts(
    store: &PostgresConversationStore,
    concurrent: &PostgresConversationStore,
) {
    let scope = ArtifactScope::parse("tenant.postgres").unwrap();
    let other_scope = ArtifactScope::parse("tenant.postgres.other").unwrap();
    let png = b"\x89PNG\r\n\x1a\npostgres-artifact";
    let write = ArtifactWrite::new(
        scope.clone(),
        "conversation:render",
        "image/png",
        png.to_vec(),
    )
    .unwrap();
    let artifact = store.put(write.clone()).await.unwrap();
    assert_eq!(store.put(write).await.unwrap(), artifact);
    assert_eq!(store.get(&artifact).await.unwrap().bytes, png);
    let changed_replay = ArtifactWrite::new(
        scope.clone(),
        "conversation:render",
        "image/png",
        png.to_vec(),
    )
    .unwrap()
    .with_expires_at_unix_ms(i64::MAX as u64)
    .unwrap();
    assert!(matches!(
        store.put(changed_replay).await,
        Err(ArtifactError::IdempotencyConflict(_))
    ));
    let changed_alias = ArtifactWrite::new(
        scope.clone(),
        "conversation:alias",
        "image/png",
        png.to_vec(),
    )
    .unwrap()
    .with_name("different")
    .unwrap();
    assert!(matches!(
        store.put(changed_alias).await,
        Err(ArtifactError::MetadataConflict(_))
    ));
    let conflict = ArtifactWrite::new(
        scope.clone(),
        "conversation:render",
        "image/png",
        b"\x89PNG\r\n\x1a\nconflict".to_vec(),
    )
    .unwrap();
    assert!(matches!(
        store.put(conflict).await,
        Err(ArtifactError::IdempotencyConflict(_))
    ));
    let isolated = store
        .put(
            ArtifactWrite::new(
                other_scope.clone(),
                "conversation:render",
                "image/png",
                png.to_vec(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let expired = store
        .put(
            ArtifactWrite::new(
                scope.clone(),
                "conversation:expired",
                "text/plain",
                b"expired".to_vec(),
            )
            .unwrap()
            .with_expires_at_unix_ms(1)
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(matches!(
        store.get(&expired).await,
        Err(ArtifactError::Expired(_))
    ));
    assert_eq!(
        store
            .purge_expired(&scope, i64::MAX as u64, 10)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store.list(&scope, None, 10).await.unwrap().items.as_slice(),
        std::slice::from_ref(&artifact)
    );
    assert!(store.delete(&scope, &artifact.artifact_id).await.unwrap());
    assert!(!store.delete(&scope, &artifact.artifact_id).await.unwrap());
    assert_eq!(store.get(&isolated).await.unwrap().bytes, png);

    assert_concurrent_artifact_idempotency(store, concurrent).await;
}

async fn assert_concurrent_artifact_idempotency(
    first: &PostgresConversationStore,
    second: &PostgresConversationStore,
) {
    let shared_scope = ArtifactScope::parse("tenant.postgres.concurrent.same").unwrap();
    let shared = ArtifactWrite::new(
        shared_scope,
        "same-key",
        "text/plain",
        b"same-content".to_vec(),
    )
    .unwrap();
    let (left, right) = tokio::join!(first.put(shared.clone()), second.put(shared));
    assert_eq!(left.unwrap(), right.unwrap());

    let conflict_scope = ArtifactScope::parse("tenant.postgres.concurrent.conflict").unwrap();
    let left_write = ArtifactWrite::new(
        conflict_scope.clone(),
        "conflicting-key",
        "text/plain",
        b"left".to_vec(),
    )
    .unwrap();
    let right_write = ArtifactWrite::new(
        conflict_scope.clone(),
        "conflicting-key",
        "text/plain",
        b"right".to_vec(),
    )
    .unwrap();
    let (left, right) = tokio::join!(first.put(left_write), second.put(right_write));
    assert_eq!(
        [left.is_ok(), right.is_ok()]
            .into_iter()
            .filter(|ok| *ok)
            .count(),
        1
    );
    assert!(
        [left, right]
            .into_iter()
            .filter_map(Result::err)
            .all(|error| matches!(error, ArtifactError::IdempotencyConflict(_)))
    );
    assert_eq!(
        first
            .list(&conflict_scope, None, 10)
            .await
            .unwrap()
            .items
            .len(),
        1
    );
}

async fn append_and_assert_bounded(
    store: &PostgresConversationStore,
    conversation_id: ConversationId,
    namespace: MemoryNamespace,
    expected_version: ConversationVersion,
) -> ConversationVersion {
    store
        .append(
            namespace.clone(),
            ConversationAppend {
                conversation_id,
                expected_version,
                messages: (2..=10)
                    .map(|sequence| Message::user(format!("message-{sequence}")))
                    .collect(),
            },
        )
        .await
        .unwrap();
    let bounded = store
        .load_view(
            conversation_id,
            namespace,
            ConversationWindow::new(2).unwrap(),
            ConversationSummaryBatch::new(3).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bounded.summary_buffer.len(), 3);
    assert_eq!(bounded.summary_backlog, 5);
    assert_eq!(bounded.window.len(), 2);
    bounded.version
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
            "DROP TABLE {table}_artifact_idempotency, {table}_artifacts, \
             {table}_effects, {table}_checkpoints, \
             {table}_memory, {table}_transcript, {table}"
        ))
        .await
        .unwrap();
}
