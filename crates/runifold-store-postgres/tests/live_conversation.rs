//! Disposable `PostgreSQL` conversation-store integration tests.

mod support;

use std::collections::BTreeMap;

use runifold_agent::{
    ConversationAppend, ConversationId, ConversationSequence, ConversationStore,
    ConversationStoreErrorKind, ConversationSummaryBatch, ConversationSummaryCommit,
    ConversationVersion, ConversationWindow, MemoryNamespace, SemanticMemoryId,
    SemanticMemoryQuery, SemanticMemorySource, SemanticMemoryUpsert,
};
use runifold_model::Message;
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

    drop_tables(&connection_url, &table).await;
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
            "DROP TABLE {table}_memory, {table}_transcript, {table}"
        ))
        .await
        .unwrap();
}
