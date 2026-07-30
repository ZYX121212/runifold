//! Environment-gated real PostgreSQL/pgvector integration.

use std::num::NonZeroU32;

use runifold_retrieval::{Document, Embedding, RetrievalContext, VectorRecord, VectorStore};
use runifold_retrieval_pgvector::PgVectorStore;

#[tokio::test]
async fn upsert_and_cosine_search_round_trip() {
    let Ok(connection) = std::env::var("RUNIFOLD_TEST_POSTGRES_URL") else {
        return;
    };
    let table = format!("runifold_vector_test_{}", std::process::id());
    let store = PgVectorStore::connect(&connection, &table).await.unwrap();
    store
        .ensure_schema(NonZeroU32::new(2).unwrap())
        .await
        .unwrap();
    store
        .upsert(
            vec![
                VectorRecord {
                    document: Document::new("rust", "Rust ownership").unwrap(),
                    embedding: Embedding::new(vec![1.0, 0.0]).unwrap(),
                },
                VectorRecord {
                    document: Document::new("python", "Python typing").unwrap(),
                    embedding: Embedding::new(vec![0.0, 1.0]).unwrap(),
                },
            ],
            RetrievalContext::new(),
        )
        .await
        .unwrap();

    let response = store
        .search(
            Embedding::new(vec![1.0, 0.0]).unwrap(),
            1,
            RetrievalContext::new(),
        )
        .await
        .unwrap();

    assert_eq!(response.results[0].document.id.as_str(), "rust");

    let (client, connection_task) = tokio_postgres::connect(&connection, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection_task.await;
    });
    client
        .batch_execute(&format!("DROP TABLE {table}"))
        .await
        .unwrap();
}
