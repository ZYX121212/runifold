//! Untrusted upstream failures must not enter ordinary error Display output.
#![cfg(all(
    feature = "openai",
    feature = "anthropic",
    feature = "gemini",
    feature = "ollama"
))]
use runifold_core::RetrySafety;
use runifold_model::{Message, Model, ModelCallContext, ModelErrorKind, ModelRef, ModelRequest};
use runifold_provider_testkit::{CassetteServer, HttpExchange, ResponseChunk, ScriptedResponse};
use serde_json::json;

const PRIVATE: &str = "private-upstream-prompt-and-token";
const CASES: [(&str, &str, &str); 4] = [
    ("openai", "gpt-test", "/v1/responses"),
    ("anthropic", "claude-test", "/v1/messages"),
    (
        "gemini",
        "gemini-test",
        "/v1beta/models/gemini-test:streamGenerateContent?alt=sse",
    ),
    ("ollama", "qwen3", "/api/chat"),
];

fn client(provider: &str, server: &CassetteServer) -> Box<dyn Model> {
    match provider {
        "openai" => Box::new(runifold_providers::openai::OpenAiClient::new(
            runifold_providers::openai::OpenAiConfig::new("fixture-auth")
                .unwrap()
                .with_base_url(&format!("{}v1/", server.base_url()))
                .unwrap(),
        )),
        "anthropic" => Box::new(runifold_providers::anthropic::AnthropicClient::new(
            runifold_providers::anthropic::AnthropicConfig::new("fixture-auth")
                .unwrap()
                .with_base_url(&format!("{}v1/", server.base_url()))
                .unwrap(),
        )),
        "gemini" => Box::new(runifold_providers::gemini::GeminiClient::new(
            runifold_providers::gemini::GeminiConfig::new("fixture-auth")
                .unwrap()
                .with_base_url(&format!("{}v1beta/", server.base_url()))
                .unwrap(),
        )),
        "ollama" => Box::new(runifold_providers::ollama::OllamaClient::new(
            runifold_providers::ollama::OllamaConfig::new(&server.base_url()).unwrap(),
        )),
        _ => panic!("unknown test provider"),
    }
}

#[tokio::test]
async fn http_error_bodies_are_not_reflected_in_display_and_retry_safety_is_preserved() {
    for (provider, model, path) in CASES {
        for status in [429, 503] {
            let response = ScriptedResponse::json(status, &json!({"error":{
                "message":PRIVATE, "type":"server_error", "code":"provider_unavailable", "status":"UNAVAILABLE"
            }})).unwrap();
            let server =
                CassetteServer::start(vec![HttpExchange::new("POST", path, response)]).unwrap();
            let error = client(provider, &server)
                .invoke(
                    ModelRequest::new(ModelRef::new(provider, model), Message::user(PRIVATE)),
                    ModelCallContext::new(),
                )
                .await
                .unwrap_err();
            assert_eq!(error.kind, ModelErrorKind::Provider);
            assert_eq!(error.metadata["http.status"], status);
            assert_eq!(
                error.retry_safety,
                if status == 429 {
                    RetrySafety::Safe
                } else {
                    RetrySafety::Unknown
                }
            );
            assert!(!error.to_string().contains(PRIVATE));
            assert!(error.to_string().contains(&status.to_string()));
            assert!(
                !serde_json::to_string(&error.diagnostic_metadata())
                    .unwrap()
                    .contains(PRIVATE)
            );
        }
    }
}

#[tokio::test]
async fn streamed_provider_error_messages_are_not_reflected_in_display() {
    for (provider, model, path) in CASES {
        let body = match provider {
            "openai" => format!(
                "data: {}\n\n",
                json!({"type":"error","sequence_number":0,"error":{"message":PRIVATE,"type":"server_error"}})
            ),
            "anthropic" => format!(
                "event: error\ndata: {}\n\n",
                json!({"type":"error","error":{"message":PRIVATE,"type":"api_error"}})
            ),
            "gemini" => format!(
                "data: {}\n\n",
                json!({"error":{"message":PRIVATE,"status":"UNAVAILABLE"}})
            ),
            "ollama" => format!("{}\n", json!({"error":PRIVATE})),
            _ => unreachable!("fixed fixture"),
        };
        let mime = if provider == "ollama" {
            "application/x-ndjson"
        } else {
            "text/event-stream"
        };
        let server = CassetteServer::start(vec![HttpExchange::new(
            "POST",
            path,
            ScriptedResponse::ok(vec![ResponseChunk::text(body)]).with_header("content-type", mime),
        )])
        .unwrap();
        let error = client(provider, &server)
            .invoke(
                ModelRequest::new(ModelRef::new(provider, model), Message::user(PRIVATE)),
                ModelCallContext::new(),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, ModelErrorKind::Provider);
        assert!(!error.to_string().contains(PRIVATE));
        assert_eq!(error.retry_safety, RetrySafety::Unknown);
    }
}
