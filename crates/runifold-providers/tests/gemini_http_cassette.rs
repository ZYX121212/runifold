//! Gemini protocol tests over a real loopback HTTP socket.
#![cfg(feature = "gemini")]

use std::collections::BTreeMap;

use runifold_model::{
    ContentPart, Message, Model, ModelCallContext, ModelErrorKind, ModelRef, ModelRequest,
    ModelUsage,
};
use runifold_provider_testkit::{
    CassetteServer, HttpExchange, ResponseChunk, ScriptedResponse, SuccessContract, verify_success,
};
use runifold_providers::gemini::{GeminiClient, GeminiConfig};
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
async fn remote_tool_media_and_structured_error_are_preserved_over_http() {
    let event = json!({"candidates":[{"content":{"parts":[{"text":"done"}]},
        "finishReason":"STOP"}]});
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1beta/models/gemini-test:streamGenerateContent?alt=sse",
        ScriptedResponse::ok(vec![ResponseChunk::text(format!("data: {event}\n\n"))])
            .with_header("content-type", "text/event-stream"),
    )])
    .unwrap();
    let tool_result = runifold_model::ToolResult {
        call_id: "call-remote".into(),
        name: Some("inspect".into()),
        content: vec![ContentPart::Image {
            source: runifold_model::MediaSource::Url {
                url: "https://example.com/image.png".into(),
                media_type: Some("image/png".into()),
            },
        }],
        structured_content: Some(json!({"code":"missing"})),
        is_error: true,
        metadata: BTreeMap::new(),
    };
    let request = request().message(
        Message::new(
            runifold_model::Role::Tool,
            vec![ContentPart::ToolResult(tool_result)],
        )
        .unwrap(),
    );
    client(&server)
        .invoke(request, ModelCallContext::new())
        .await
        .unwrap();

    server.assert_finished().unwrap();
    let body = server.observed_requests()[0].json_body().unwrap();
    let response = &body["contents"][1]["parts"][0]["functionResponse"];
    assert!(response.get("parts").is_none());
    assert_eq!(response["response"]["isError"], true);
    assert_eq!(response["response"]["structuredContent"]["code"], "missing");
    assert!(
        response["response"]["content"][0]
            .as_str()
            .unwrap()
            .contains("https://example.com/image.png")
    );
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

    let report = verify_success(
        &client(&server),
        request(),
        ModelCallContext::new(),
        &SuccessContract::new("gemini")
            .visible_text("hello")
            .usage(ModelUsage {
                input_tokens: 2,
                output_tokens: 3,
                ..ModelUsage::default()
            })
            .provider_events(),
    )
    .await
    .unwrap();

    assert_eq!(report.checks().len(), 4);
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

#[tokio::test]
async fn signed_function_without_native_id_replays_exactly_with_its_result() {
    let part = json!({"functionCall":{"name":"lookup","args":{"key":1}},"thoughtSignature":"private-signature"});
    let first = json!({"candidates":[{"content":{"parts":[part.clone()]},"finishReason":"STOP"}],
        "usageMetadata":{"promptTokenCount":10,"toolUsePromptTokenCount":2,"candidatesTokenCount":3,"thoughtsTokenCount":7}});
    let second =
        json!({"candidates":[{"content":{"parts":[{"text":"done"}]},"finishReason":"STOP"}]});
    let response = |value: &serde_json::Value| {
        ScriptedResponse::ok(vec![ResponseChunk::text(format!("data: {value}\n\n"))])
            .with_header("content-type", "text/event-stream")
    };
    let path = "/v1beta/models/gemini-test:streamGenerateContent?alt=sse";
    let server = CassetteServer::start(vec![
        HttpExchange::new("POST", path, response(&first)),
        HttpExchange::new("POST", path, response(&second)),
    ])
    .unwrap();
    let client = client(&server);
    let first = client
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap();
    assert_eq!(first.usage.total_tokens(), 22);
    let call = first
        .content
        .iter()
        .find_map(|part| {
            if let ContentPart::ToolCall(call) = part {
                Some(call)
            } else {
                None
            }
        })
        .unwrap();
    let result = runifold_model::ToolResult {
        call_id: call.id.clone(),
        name: Some(call.name.clone()),
        content: vec![ContentPart::text("ok")],
        structured_content: None,
        is_error: false,
        metadata: BTreeMap::new(),
    };
    let stored = serde_json::to_vec(&first.assistant_message().unwrap()).unwrap();
    let restored: Message = serde_json::from_slice(&stored).unwrap();
    let mut next = request().message(restored).message(
        Message::new(
            runifold_model::Role::Tool,
            vec![ContentPart::ToolResult(result)],
        )
        .unwrap(),
    );
    next.generation.seed = Some(42);
    next.provider_options.insert(
        "gemini".into(),
        json!({"generationConfig":{"thinkingConfig":{"thinkingBudget":128}}}),
    );
    client.invoke(next, ModelCallContext::new()).await.unwrap();
    server.assert_finished().unwrap();
    let body = server.observed_requests()[1].json_body().unwrap();
    assert_eq!(body["contents"][1]["parts"][0], part);
    assert!(
        body["contents"][2]["parts"][0]["functionResponse"]
            .get("id")
            .is_none()
    );
    assert_eq!(body["generationConfig"]["seed"], 42);
    assert_eq!(
        body["generationConfig"]["thinkingConfig"]["thinkingBudget"],
        128
    );
}
