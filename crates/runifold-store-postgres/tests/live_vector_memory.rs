//! Disposable `PostgreSQL`/pgvector semantic-memory integration test.

mod support;

use std::{collections::BTreeMap, num::NonZeroU32, sync::Arc};

use runifold_agent::{
    ConversationAppend, ConversationId, ConversationSequence, ConversationStore,
    ConversationVersion, MemoryNamespace, SemanticMemoryId, SemanticMemoryQuery,
    SemanticMemorySource, SemanticMemoryUpsert,
};
use runifold_core::Usage;
use runifold_model::Message;
use runifold_retrieval::{
    Embedding, EmbeddingBatch, EmbeddingFuture, EmbeddingModel, EmbeddingRequest, RetrievalContext,
    RetrievalError,
};
use runifold_store_postgres::PostgresConversationStore;
use tokio_postgres::NoTls;
use uuid::Uuid;

pub use support::PostgresTestContext;

struct KeywordEmbedder;

impl EmbeddingModel for KeywordEmbedder {
    fn embed(
        &self,
        request: EmbeddingRequest,
        _context: RetrievalContext,
    ) -> EmbeddingFuture<'_, Result<EmbeddingBatch, RetrievalError>> {
        Box::pin(async move {
            let embeddings = request
                .inputs()
                .iter()
                .map(|input| {
                    let normalized = input.to_lowercase();
                    let values = if normalized.contains("rust") {
                        vec![1.0, 0.0, 0.0]
                    } else if normalized.contains("garden") {
                        vec![0.0, 1.0, 0.0]
                    } else {
                        vec![0.0, 0.0, 1.0]
                    };
                    Embedding::new(values)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(EmbeddingBatch {
                usage: Usage {
                    tokens: u64::try_from(embeddings.len()).unwrap_or(u64::MAX),
                    ..Usage::default()
                },
                embeddings,
            })
        })
    }
}

#[tokio::test]
async fn vector_memory_is_atomic_scoped_and_semantically_ranked() {
    let database = PostgresTestContext::start("RUNIFOLD_TEST_PGVECTOR_URL").await;
    let connection_url = database.connection_url().to_owned();
    let suffix = Uuid::now_v7().simple().to_string();
    let table = format!("runifold_vm_{suffix}");
    let store = PostgresConversationStore::connect(&connection_url, &table)
        .await
        .unwrap()
        .with_semantic_memory_embedder(Arc::new(KeywordEmbedder));
    store.ensure_schema().await.unwrap();
    store
        .ensure_semantic_memory_vector_schema(NonZeroU32::new(3).unwrap())
        .await
        .unwrap();
    let conversation_id = ConversationId::new();
    let namespace = MemoryNamespace::parse("tenant.vector").unwrap();
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
                messages: vec![Message::user("Remember two durable preferences")],
            },
        )
        .await
        .unwrap();
    let rust = upsert(
        &store,
        namespace.clone(),
        conversation_id,
        "The user prefers Rust for systems work",
    )
    .await;
    upsert(
        &store,
        namespace.clone(),
        conversation_id,
        "The user enjoys garden design",
    )
    .await;

    let found = store
        .search_memory_scoped(
            SemanticMemoryQuery::new(namespace.clone(), "Rust implementation language", 2).unwrap(),
            RetrievalContext::new(),
        )
        .await
        .unwrap();

    assert_eq!(found.memories.first(), Some(&rust));
    assert_eq!(found.usage.tokens, 1);
    let compatibility = store
        .search_memory(
            SemanticMemoryQuery::new(namespace, "Rust implementation language", 2).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(compatibility.first(), Some(&rust));
    drop_tables(&connection_url, &table).await;
}

async fn upsert(
    store: &PostgresConversationStore,
    namespace: MemoryNamespace,
    conversation_id: ConversationId,
    content: &str,
) -> runifold_agent::SemanticMemory {
    let outcome = store
        .upsert_memory_scoped(
            SemanticMemoryUpsert {
                memory_id: SemanticMemoryId::new(),
                namespace,
                content: content.into(),
                sources: vec![SemanticMemorySource {
                    conversation_id,
                    from_sequence: ConversationSequence::new(1).unwrap(),
                    through_sequence: ConversationSequence::new(1).unwrap(),
                }],
                metadata: BTreeMap::new(),
                expected_revision: None,
            },
            RetrievalContext::new(),
        )
        .await
        .unwrap();
    assert_eq!(outcome.usage.tokens, 1);
    outcome.memory
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
