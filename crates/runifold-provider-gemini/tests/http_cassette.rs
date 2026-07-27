//! Gemini protocol tests over a real loopback HTTP socket.

use runifold_model::{
    ContentPart, FinishReason, Message, Model, ModelCallContext, ModelErrorKind, ModelRef,
    ModelRequest,
};
use runifold_provider_gemini::{GeminiClient, GeminiConfig};
use runifold_provider_testkit::{CassetteServer, HttpExchange, ResponseChunk, ScriptedResponse};
use serde_json::json;

fn client(server: &CassetteServer) -> GeminiClient {
    let config = GeminiConfig::new("secret")
        .unwrap()
        .with_base_url(&format!("{}v1beta/", server.base_url()))
        .unwrap();
    GeminiClient::new(config)
}

fn request() -> ModelRequest {
    ModelRequest::new(
        ModelRef::new("gemini", "gemini-test"),
        Message::user("hello"),
    )
}

#[tokio::test]
async fn streams_text_and_usage_over_native_sse() {
    let response = ScriptedResponse::ok(vec![
        ResponseChunk::text(
            "data: {\"responseId\":\"r1\",\"modelVersion\":\"gemini-test-v1\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hel\"}]}}],\"usageMetadata\":{\"promptTokenCount\":2}}\n\n",
        ),
        ResponseChunk::text(
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"lo\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3}}\n\n",
        ),
    ])
    .with_header("content-type", "text/event-stream");
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1beta/models/gemini-test:streamGenerateContent?alt=sse",
        response,
    )])
    .unwrap();

    let response = client(&server)
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap();

    assert_eq!(
        response.content,
        vec![ContentPart::Text {
            text: "hello".into()
        }]
    );
    assert_eq!(response.finish_reason, FinishReason::Stop);
    assert_eq!(response.usage.total_tokens(), 5);
    server.assert_finished().unwrap();
    assert_eq!(
        server.observed_requests()[0].headers["x-goog-api-key"],
        "[REDACTED]"
    );
}

#[tokio::test]
async fn reconstructs_a_native_function_call() {
    let response = ScriptedResponse::ok(vec![ResponseChunk::text(
        "data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"id\":\"fc1\",\"name\":\"weather\",\"args\":{\"city\":\"Shanghai\"}}}]},\"finishReason\":\"STOP\"}]}\n\n",
    )])
    .with_header("content-type", "text/event-stream");
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1beta/models/gemini-test:streamGenerateContent?alt=sse",
        response,
    )])
    .unwrap();

    let response = client(&server)
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap();

    let ContentPart::ToolCall(call) = &response.content[0] else {
        panic!("expected a function call");
    };
    assert_eq!(call.id, "fc1");
    assert_eq!(call.name, "weather");
    assert_eq!(call.arguments, json!({"city":"Shanghai"}));
}

#[tokio::test]
async fn maps_structured_http_errors() {
    let response = ScriptedResponse::json(
        400,
        &json!({"error":{"code":400,"message":"bad request","status":"INVALID_ARGUMENT"}}),
    )
    .unwrap();
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1beta/models/gemini-test:streamGenerateContent?alt=sse",
        response,
    )])
    .unwrap();

    let error = client(&server)
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap_err();

    assert_eq!(error.kind, ModelErrorKind::Provider);
    assert_eq!(error.metadata["gemini.error.status"], "INVALID_ARGUMENT");
}
