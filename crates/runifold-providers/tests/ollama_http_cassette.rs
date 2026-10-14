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

#[tokio::test]
async fn parallel_same_name_tool_ids_survive_serialized_http_replay() {
    let calls = json!([{"id":"call1","function":{"name":"lookup","arguments":{"key":1}}},
        {"id":"call2","function":{"name":"lookup","arguments":{"key":2}}}]);
    let first = json!({"model":"qwen3","message":{"role":"assistant","content":"","tool_calls":calls},"done":true,"done_reason":"tool_calls"});
    let second = json!({"model":"qwen3","message":{"role":"assistant","content":"done"},"done":true,"done_reason":"stop"});
    let response = |value: &serde_json::Value| {
        ScriptedResponse::ok(vec![ResponseChunk::text(format!("{value}\n"))])
            .with_header("content-type", "application/x-ndjson")
    };
    let server = CassetteServer::start(vec![
        HttpExchange::new("POST", "/api/chat", response(&first)),
        HttpExchange::new("POST", "/api/chat", response(&second)),
    ])
    .unwrap();
    let client = client(&server);
    let first = client
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap();
    let stored = serde_json::to_vec(&first.assistant_message().unwrap()).unwrap();
    let restored: Message = serde_json::from_slice(&stored).unwrap();
    let mut next = request().message(restored);
    for id in ["call1", "call2"] {
        next = next.message(
            Message::new(
                runifold_model::Role::Tool,
                vec![ContentPart::ToolResult(runifold_model::ToolResult {
                    call_id: id.into(),
                    name: Some("lookup".into()),
                    content: vec![ContentPart::text(id)],
                    structured_content: None,
                    is_error: false,
                    metadata: std::collections::BTreeMap::default(),
                })],
            )
            .unwrap(),
        );
    }
    client.invoke(next, ModelCallContext::new()).await.unwrap();
    server.assert_finished().unwrap();
    let body = server.observed_requests()[1].json_body().unwrap();
    assert_eq!(body["messages"][1]["tool_calls"][0]["id"], "call1");
    assert_eq!(body["messages"][1]["tool_calls"][1]["id"], "call2");
    assert_eq!(body["messages"][2]["tool_call_id"], "call1");
    assert_eq!(body["messages"][3]["tool_call_id"], "call2");
}
