//! Ollama protocol tests over a real loopback HTTP socket.
#![cfg(feature = "ollama")]

use runifold_model::{
    ContentPart, FinishReason, Message, Model, ModelCallContext, ModelErrorKind, ModelRef,
    ModelRequest, ModelUsage,
};
use runifold_provider_testkit::{
    CassetteServer, HttpExchange, ResponseChunk, ScriptedResponse, SuccessContract, verify_success,
};
use runifold_providers::ollama::{OllamaClient, OllamaConfig};
use serde_json::json;

fn client(server: &CassetteServer) -> OllamaClient {
    OllamaClient::new(OllamaConfig::new(&server.base_url()).unwrap())
}

fn request() -> ModelRequest {
    ModelRequest::new(ModelRef::new("ollama", "qwen3"), Message::user("hello"))
}

#[tokio::test]
async fn frames_native_ndjson_across_http_chunks() {
    let response = ScriptedResponse::ok(vec![
        ResponseChunk::text(
            "{\"model\":\"qwen3\",\"message\":{\"role\":\"assistant\",\"thinking\":\"hmm\",\"content\":\"hel\"},\"done\":false}\n{\"model\":\"qwen3\",\"message\":{\"role\":\"assistant\",\"content\":\"",
        ),
        ResponseChunk::text(
            "lo\"},\"done\":false}\n{\"model\":\"qwen3\",\"message\":{\"role\":\"assistant\",\"content\":\"\"},\"done\":true,\"done_reason\":\"stop\",\"prompt_eval_count\":2,\"eval_count\":3,\"total_duration\":50}\n",
        ),
    ])
    .with_header("content-type", "application/x-ndjson");
    let server =
        CassetteServer::start(vec![HttpExchange::new("POST", "/api/chat", response)]).unwrap();

    let report = verify_success(
        &client(&server),
        request(),
        ModelCallContext::new(),
        &SuccessContract::new("ollama")
            .visible_text("hello")
            .reasoning("hmm")
            .usage(ModelUsage {
                input_tokens: 2,
                output_tokens: 3,
                ..ModelUsage::default()
            })
            .provider_events(),
    )
    .await
    .unwrap();

    assert_eq!(report.checks().len(), 5);
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn decodes_native_tool_calls() {
    let response = ScriptedResponse::ok(vec![ResponseChunk::text(
        "{\"model\":\"qwen3\",\"message\":{\"role\":\"assistant\",\"content\":\"\",\"tool_calls\":[{\"id\":\"tc1\",\"function\":{\"name\":\"weather\",\"arguments\":{\"city\":\"Shanghai\"}}}]},\"done\":false}\n{\"model\":\"qwen3\",\"message\":{\"role\":\"assistant\",\"content\":\"\"},\"done\":true,\"done_reason\":\"tool_calls\"}\n",
    )])
    .with_header("content-type", "application/x-ndjson");
    let server =
        CassetteServer::start(vec![HttpExchange::new("POST", "/api/chat", response)]).unwrap();

    let response = client(&server)
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap();

    assert_eq!(response.finish_reason, FinishReason::ToolCalls);
    let ContentPart::ToolCall(call) = &response.content[0] else {
        panic!("expected a tool call");
    };
    assert_eq!(call.id, "tc1");
    assert_eq!(call.arguments, json!({"city":"Shanghai"}));
}

#[tokio::test]
async fn rejects_midstream_provider_errors() {
    let response = ScriptedResponse::ok(vec![ResponseChunk::text(
        "{\"model\":\"qwen3\",\"message\":{\"content\":\"partial\"},\"done\":false}\n{\"error\":\"model runner crashed\"}\n",
    )])
    .with_header("content-type", "application/x-ndjson");
    let server =
        CassetteServer::start(vec![HttpExchange::new("POST", "/api/chat", response)]).unwrap();

    let error = client(&server)
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap_err();

    assert_eq!(error.kind, ModelErrorKind::Provider);
    assert_eq!(error.message, "model runner crashed");
}
