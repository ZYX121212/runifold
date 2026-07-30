//! Database-stop fault injection and reconnect recovery coverage.

mod support;

use std::time::Duration;

use runifold_agent::{
    ConversationAppend, ConversationId, ConversationStore, ConversationStoreErrorKind,
    ConversationSummaryBatch, ConversationVersion, ConversationWindow, MemoryNamespace,
};
use runifold_model::Message;
use runifold_store_postgres::PostgresConversationStore;
use tokio::time::timeout;
use uuid::Uuid;

pub use support::PostgresTestContext;

#[tokio::test]
async fn database_restart_surfaces_storage_failure_and_preserves_committed_transcript() {
    let database = PostgresTestContext::isolated().await;
    let connection_url = database.connection_url();
    let table = format!("runifold_rc_{}", Uuid::now_v7().simple());
    let store = PostgresConversationStore::connect(connection_url, &table)
        .await
        .unwrap();
    store.ensure_schema().await.unwrap();
    let conversation_id = ConversationId::new();
    let namespace = MemoryNamespace::parse("tenant.recovery").unwrap();
    store
        .create(conversation_id, namespace.clone())
        .await
        .unwrap();
    store
        .append(
            namespace.clone(),
            ConversationAppend {
                conversation_id,
                expected_version: ConversationVersion::default(),
                messages: vec![Message::user("committed before database restart")],
            },
        )
        .await
        .unwrap();

    database.stop().await;
    let outage = timeout(
        Duration::from_secs(5),
        store.load_view(
            conversation_id,
            namespace.clone(),
            ConversationWindow::new(8).unwrap(),
            ConversationSummaryBatch::new(8).unwrap(),
        ),
    )
    .await
    .expect("database outage must surface within the bounded test deadline")
    .unwrap_err();
    assert_eq!(outage.kind, ConversationStoreErrorKind::Storage);

    let recovered_url = database.restart().await;
    let recovered = PostgresConversationStore::connect(&recovered_url, &table)
        .await
        .unwrap()
        .load_view(
            conversation_id,
            namespace,
            ConversationWindow::new(8).unwrap(),
            ConversationSummaryBatch::new(8).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recovered.version.get(), 1);
    assert_eq!(recovered.window.len(), 1);
    assert_eq!(
        recovered.window[0].message,
        Message::user("committed before database restart")
    );
}
