//! Gemini embeddings over a real loopback HTTP socket.
#![cfg(feature = "gemini")]

use std::num::NonZeroU32;

use runifold_provider_testkit::{CassetteServer, HttpExchange, ScriptedResponse};
use runifold_providers::gemini::{GeminiClient, GeminiConfig};
use runifold_retrieval::{EmbeddingModel, EmbeddingRequest, EmbeddingTask, RetrievalContext};
use serde_json::json;

fn client(server: &CassetteServer) -> GeminiClient {
    let config = GeminiConfig::new("gemini-embedding-secret")
        .unwrap()
        .with_base_url(&format!("{}v1beta/", server.base_url()))
        .unwrap();
    GeminiClient::new(config)
}

#[tokio::test]
async fn maps_retrieval_task_dimensions_usage_and_credentials() {
    let response = ScriptedResponse::json(
        200,
        &json!({
            "embeddings": [
                {"values": [1.0, 0.0]},
                {"values": [0.0, 1.0]}
            ],
            "usageMetadata": {"promptTokenCount": 9}
        }),
    )
    .unwrap();
    let server = CassetteServer::start(vec![
        HttpExchange::new(
            "POST",
            "/v1beta/models/gemini-embedding-001:batchEmbedContents",
            response,
        )
        .with_json_body(json!({
            "requests": [
                {
                    "model": "models/gemini-embedding-001",
                    "content": {"parts": [{"text": "first"}]},
                    "embedContentConfig": {
                        "taskType": "RETRIEVAL_DOCUMENT",
                        "autoTruncate": false,
                        "outputDimensionality": 2
                    }
                },
                {
                    "model": "models/gemini-embedding-001",
                    "content": {"parts": [{"text": "second"}]},
                    "embedContentConfig": {
                        "taskType": "RETRIEVAL_DOCUMENT",
                        "autoTruncate": false,
                        "outputDimensionality": 2
                    }
                }
            ]
        })),
    ])
    .unwrap();
    let model = client(&server)
        .embedding_model("models/gemini-embedding-001")
        .unwrap()
        .with_dimensions(NonZeroU32::new(2).unwrap());

    let batch = model
        .embed(
            EmbeddingRequest::new(
                vec!["first".into(), "second".into()],
                EmbeddingTask::RetrievalDocument,
            )
            .unwrap(),
            RetrievalContext::new(),
        )
        .await
        .unwrap();

    assert_eq!(batch.embeddings.len(), 2);
    assert_eq!(batch.usage.tokens, 9);
    server.assert_finished().unwrap();
    assert_eq!(
        server.observed_requests()[0].headers["x-goog-api-key"],
        "[REDACTED]"
    );
}

#[tokio::test]
async fn maps_structured_embedding_errors_without_input_disclosure() {
    let response = ScriptedResponse::json(
        400,
        &json!({"error":{"message":"unsupported embedding task"}}),
    )
    .unwrap();
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1beta/models/gemini-embedding-001:batchEmbedContents",
        response,
    )])
    .unwrap();
    let model = client(&server)
        .embedding_model("gemini-embedding-001")
        .unwrap();

    let error = model
        .embed(
            EmbeddingRequest::new(vec!["private input".into()], EmbeddingTask::RetrievalQuery)
                .unwrap(),
            RetrievalContext::new(),
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("unsupported embedding task"));
    assert!(!error.to_string().contains("private input"));
}
