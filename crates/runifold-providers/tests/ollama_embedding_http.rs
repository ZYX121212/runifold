//! Ollama embeddings over a real loopback HTTP socket.
#![cfg(feature = "ollama")]

use runifold_provider_testkit::{CassetteServer, HttpExchange, ScriptedResponse};
use runifold_providers::ollama::{OllamaClient, OllamaConfig};
use runifold_retrieval::{EmbeddingModel, EmbeddingRequest, EmbeddingTask, RetrievalContext};
use serde_json::json;

fn client(server: &CassetteServer) -> OllamaClient {
    let config = OllamaConfig::new(&server.base_url())
        .unwrap()
        .with_bearer_token("ollama-secret")
        .unwrap();
    OllamaClient::new(config)
}

#[tokio::test]
async fn embeds_a_native_batch_with_provider_usage_and_redaction() {
    let response = ScriptedResponse::json(
        200,
        &json!({
            "model": "embeddinggemma",
            "embeddings": [[1.0, 0.0], [0.0, 1.0]],
            "total_duration": 42000,
            "load_duration": 1000,
            "prompt_eval_count": 6
        }),
    )
    .unwrap();
    let server = CassetteServer::start(vec![
        HttpExchange::new("POST", "/api/embed", response).with_json_body(json!({
            "model": "embeddinggemma",
            "input": ["rust", "agent"],
            "truncate": false
        })),
    ])
    .unwrap();
    let model = client(&server).embedding_model("embeddinggemma").unwrap();

    let batch = model
        .embed(
            EmbeddingRequest::new(
                vec!["rust".into(), "agent".into()],
                EmbeddingTask::RetrievalQuery,
            )
            .unwrap(),
            RetrievalContext::new(),
        )
        .await
        .unwrap();

    assert_eq!(batch.embeddings.len(), 2);
    assert_eq!(batch.usage.tokens, 6);
    assert_eq!(batch.usage.duration_micros, 42);
    server.assert_finished().unwrap();
    assert_eq!(
        server.observed_requests()[0].headers["authorization"],
        "[REDACTED]"
    );
}

#[tokio::test]
async fn empty_batches_do_not_open_the_transport() {
    let server = CassetteServer::start(Vec::new()).unwrap();
    let model = client(&server).embedding_model("embeddinggemma").unwrap();

    let batch = model
        .embed(
            EmbeddingRequest::new(Vec::new(), EmbeddingTask::Unspecified).unwrap(),
            RetrievalContext::new(),
        )
        .await
        .unwrap();

    assert!(batch.embeddings.is_empty());
    assert!(server.observed_requests().is_empty());
}
