//! Provider-specific reasoning survives serialization and a subsequent HTTP turn.
#![cfg(feature = "openai")]

use runifold_model::{Message, Model, ModelCallContext, ModelRef, ModelRequest, ResponseMode};
use runifold_provider_testkit::{CassetteServer, HttpExchange, ResponseChunk, ScriptedResponse};
use runifold_providers::openai::{OpenAiClient, OpenAiCompatibleProfile, OpenAiConfig};
use serde_json::json;

#[tokio::test]
async fn reasoning_survives_complete_and_streaming_history_replay() {
    for (profile, provider) in [
        (OpenAiCompatibleProfile::DeepSeek, "deepseek"),
        (OpenAiCompatibleProfile::QwenInternational, "qwen"),
        (OpenAiCompatibleProfile::OpenRouter, "openrouter"),
        (OpenAiCompatibleProfile::Zhipu, "zhipu"),
        (OpenAiCompatibleProfile::MiniMaxInternational, "minimax"),
    ] {
        for mode in [ResponseMode::Complete, ResponseMode::Streaming] {
            let reasoning = if provider == "openrouter" {
                json!({"reasoning_details":[{"index":0,"type":"reasoning.encrypted","data":"private","id":"r1","format":"test"}]})
            } else {
                json!({"reasoning_content":"private reasoning"})
            };
            let mut message = reasoning.clone();
            message["role"] = json!("assistant");
            message["content"] = json!("answer");
            let first = if mode == ResponseMode::Complete {
                ScriptedResponse::json(200, &json!({
                    "id":"r1","model":"test","choices":[{"index":0,"message":message,"finish_reason":"stop"}]
                })).unwrap()
            } else {
                let mut a = reasoning.clone();
                a["role"] = json!("assistant");
                let a = json!({"id":"r1","model":"test","choices":[{"index":0,"delta":a,"finish_reason":null}]});
                let b = json!({"id":"r1","model":"test","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":"stop"}]});
                ScriptedResponse::ok(vec![ResponseChunk::text(format!(
                    "data: {a}\n\ndata: {b}\n\ndata: [DONE]\n\n"
                ))])
                .with_header("content-type", "text/event-stream")
            };
            let final_response = ScriptedResponse::json(200, &json!({
                "id":"r2","model":"test","choices":[{"index":0,"message":{"role":"assistant","content":"done"},"finish_reason":"stop"}]
            })).unwrap();
            let server = CassetteServer::start(vec![
                HttpExchange::new("POST", "/v1/chat/completions", first),
                HttpExchange::new("POST", "/v1/chat/completions", final_response),
            ])
            .unwrap();
            let config = OpenAiConfig::from_profile(profile, "test-key")
                .unwrap()
                .with_base_url(&format!("{}v1/", server.base_url()))
                .unwrap();
            let client = OpenAiClient::new(config);
            let response = client
                .invoke(
                    ModelRequest::new(ModelRef::new(provider, "test"), Message::user("hello"))
                        .response_mode(mode),
                    ModelCallContext::new(),
                )
                .await
                .unwrap();
            let stored = serde_json::to_vec(&response.assistant_message().unwrap()).unwrap();
            let restored: Message = serde_json::from_slice(&stored).unwrap();
            client
                .invoke(
                    ModelRequest::new(ModelRef::new(provider, "test"), Message::user("hello"))
                        .message(restored)
                        .message(Message::user("continue"))
                        .response_mode(ResponseMode::Complete),
                    ModelCallContext::new(),
                )
                .await
                .unwrap();
            server.assert_finished().unwrap();
            let body = server.observed_requests()[1].json_body().unwrap();
            for (key, value) in reasoning.as_object().unwrap() {
                assert_eq!(&body["messages"][1][key], value, "{provider} {mode:?}");
            }
        }
    }
}
