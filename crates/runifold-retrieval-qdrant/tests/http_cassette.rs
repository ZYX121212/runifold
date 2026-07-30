//! Qdrant adapter tests over a real loopback HTTP socket.

use runifold_provider_testkit::{CassetteServer, HttpExchange, ScriptedResponse};
use runifold_retrieval::{Document, Embedding, RetrievalContext, VectorRecord, VectorStore};
use runifold_retrieval_qdrant::{QdrantConfig, QdrantVectorStore};
use serde_json::json;

#[tokio::test]
async fn upserts_and_queries_documents_with_redacted_authentication() {
    let upsert = ScriptedResponse::json(200, &json!({"result":{"status":"completed"}})).unwrap();
    let query = ScriptedResponse::json(
        200,
        &json!({
            "result": {
                "points": [{
                    "id": "9fc3bca3-42ce-53cb-a610-05544c2b2644",
                    "score": 0.91,
                    "payload": {
                        "_runifold_id": "doc-1",
                        "_runifold_text": "Rust ownership",
                        "_runifold_metadata": {"source": "guide"}
                    }
                }]
            }
        }),
    )
    .unwrap();
    let server = CassetteServer::start(vec![
        HttpExchange::new(
            "PUT",
            "/collections/product%20docs/points?wait=true",
            upsert,
        ),
        HttpExchange::new("POST", "/collections/product%20docs/points/query", query)
            .with_json_body(json!({
                "query": [1.0, 0.0],
                "limit": 2,
                "with_payload": true,
                "with_vector": false
            })),
    ])
    .unwrap();
    let config = QdrantConfig::new(&server.base_url())
        .unwrap()
        .with_api_key("qdrant-secret")
        .unwrap();
    let store = QdrantVectorStore::new(config, "product docs").unwrap();
    let mut document = Document::new("doc-1", "Rust ownership").unwrap();
    document.metadata.insert("source".into(), json!("guide"));

    store
        .upsert(
            vec![VectorRecord {
                document,
                embedding: Embedding::new(vec![1.0, 0.0]).unwrap(),
            }],
            RetrievalContext::new(),
        )
        .await
        .unwrap();
    let response = store
        .search(
            Embedding::new(vec![1.0, 0.0]).unwrap(),
            2,
            RetrievalContext::new(),
        )
        .await
        .unwrap();

    assert_eq!(response.results[0].document.id.as_str(), "doc-1");
    assert!((response.results[0].score - 0.91).abs() < f64::EPSILON);
    server.assert_finished().unwrap();
    let observed = server.observed_requests();
    assert_eq!(observed[0].headers["api-key"], "[REDACTED]");
    let upsert_body = observed[0].json_body().unwrap();
    assert_eq!(upsert_body["points"][0]["payload"]["_runifold_id"], "doc-1");
    assert!(upsert_body["points"][0]["id"].as_str().is_some());
}
