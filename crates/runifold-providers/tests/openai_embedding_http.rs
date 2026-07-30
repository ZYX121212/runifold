//! OpenAI-compatible embeddings over a real loopback HTTP socket.
#![cfg(feature = "openai")]

use std::time::{Duration, Instant};

use runifold_provider_testkit::{CassetteServer, HttpExchange, ResponseChunk, ScriptedResponse};
use runifold_providers::openai::{OpenAiClient, OpenAiConfig};
use runifold_retrieval::{
    EmbeddingModel, EmbeddingRequest, EmbeddingTask, RetrievalContext, RetrievalError,
};
use serde_json::json;

fn client(server: &CassetteServer) -> OpenAiClient {
    let config = OpenAiConfig::new("embedding-secret")
        .unwrap()
        .with_base_url(&format!("{}v1/", server.base_url()))
        .unwrap();
    OpenAiClient::new(config)
}

#[tokio::test]
async fn embeds_a_batch_in_input_order_and_redacts_credentials() {
    let response = ScriptedResponse::json(
        200,
        &json!({
            "data": [
                {"embedding": [0.0, 1.0], "index": 1},
                {"embedding": [1.0, 0.0], "index": 0}
            ],
            "model": "text-embedding-test",
            "usage": {"prompt_tokens": 7, "total_tokens": 7}
        }),
    )
    .unwrap();
    let server = CassetteServer::start(vec![
        HttpExchange::new("POST", "/v1/embeddings", response).with_json_body(json!({
            "input": ["rust", "agent"],
            "model": "text-embedding-test",
            "encoding_format": "float"
        })),
    ])
    .unwrap();
    let model = client(&server)
        .embedding_model("text-embedding-test")
        .unwrap();

    let batch = model
        .embed(
            EmbeddingRequest::new(
                vec!["rust".into(), "agent".into()],
                EmbeddingTask::RetrievalDocument,
            )
            .unwrap(),
            RetrievalContext::new(),
        )
        .await
        .unwrap();

    assert_eq!(batch.embeddings[0].values(), &[1.0, 0.0]);
    assert_eq!(batch.embeddings[1].values(), &[0.0, 1.0]);
    assert_eq!(batch.usage.tokens, 7);
    server.assert_finished().unwrap();
    assert_eq!(
        server.observed_requests()[0].headers["authorization"],
        "[REDACTED]"
    );
}

#[tokio::test]
async fn embedding_body_timeout_retains_deadline_classification() {
    let response = ScriptedResponse::ok(vec![
        ResponseChunk::text(
            r#"{"data":[{"embedding":[1.0],"index":0}],"usage":{"prompt_tokens":1}}"#,
        )
        .after(Duration::from_millis(100)),
    ])
    .with_header("content-type", "application/json");
    let server =
        CassetteServer::start(vec![HttpExchange::new("POST", "/v1/embeddings", response)]).unwrap();
    let model = client(&server).embedding_model("embedding-test").unwrap();
    let context = RetrievalContext::new().with_deadline(Instant::now() + Duration::from_millis(20));

    let error = model
        .embed(
            EmbeddingRequest::new(vec!["rust".into()], EmbeddingTask::RetrievalQuery).unwrap(),
            context,
        )
        .await
        .unwrap_err();

    assert_eq!(error, RetrievalError::DeadlineExceeded);
}
