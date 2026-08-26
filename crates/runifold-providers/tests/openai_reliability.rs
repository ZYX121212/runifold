//! Real-HTTP concurrency and timeout tests for the `OpenAI` provider.
#![cfg(feature = "openai")]

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use futures_util::future::join_all;
use runifold_model::{
    ContentPart, Message, Model, ModelCallContext, ModelErrorKind, ModelRef, ModelRequest,
    ModelUsage, OutputFormat, ResponseMode, Role, ToolResult,
};
use runifold_provider_testkit::{
    CassetteServer, HttpExchange, ResponseChunk, ScriptedResponse, SuccessContract, verify_success,
};
use runifold_providers::openai::{
    OpenAiClient, OpenAiCompatibleProfile, OpenAiConfig, OpenAiWireProtocol,
};
use schemars::JsonSchema;

#[derive(JsonSchema)]
struct TypedWireAnswer {
    value: u32,
    note: Option<String>,
}

fn request() -> ModelRequest {
    ModelRequest::new(ModelRef::new("openai", "gpt-test"), Message::user("stress"))
}

fn client(server: &CassetteServer) -> OpenAiClient {
    let config = OpenAiConfig::new("secret")
        .unwrap()
        .with_base_url(&format!("{}v1/", server.base_url()))
        .unwrap();
    OpenAiClient::new(config)
}

fn response(delay: Duration) -> ScriptedResponse {
    ScriptedResponse::ok(vec![
        ResponseChunk::text(
            "data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp\",\"model\":\"gpt-test\"}}\n\ndata: {\"type\":\"response.content_part.added\",\"sequence_number\":1,\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"sequence_number\":2,\"output_index\":0,\"content_index\":0,\"delta\":\"ok\"}\n\ndata: {\"type\":\"response.content_part.done\",\"sequence_number\":3,\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\"}}\n\ndata: {\"type\":\"response.completed\",\"sequence_number\":4,\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n",
        )
        .after(delay),
    ])
    .with_header("content-type", "text/event-stream")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_client_isolates_16_concurrent_responses_streams() {
    let server = CassetteServer::start_repeating(
        HttpExchange::new("POST", "/v1/responses", response(Duration::from_millis(20))),
        16,
    )
    .unwrap();
    let client = client(&server);

    let results =
        join_all((0..16).map(|_| client.invoke(request(), ModelCallContext::new()))).await;

    assert!(results.iter().all(Result::is_ok));
    server.assert_finished().unwrap();
    assert_eq!(server.stats().completed, 16);
    assert!(server.stats().max_in_flight > 1);
}

#[tokio::test]
async fn typed_strict_schema_reaches_the_http_transport_in_wire_form() {
    let example = TypedWireAnswer {
        value: 7,
        note: None,
    };
    assert_eq!(example.value, 7);
    assert!(example.note.is_none());
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1/responses",
        response(Duration::ZERO),
    )])
    .unwrap();
    let request = request().output_format(OutputFormat::typed::<TypedWireAnswer>("typed_answer"));

    client(&server)
        .invoke(request, ModelCallContext::new())
        .await
        .unwrap();

    server.assert_finished().unwrap();
    let body = server.observed_requests()[0].json_body().unwrap();
    let schema = &body["text"]["format"]["schema"];
    assert!(schema.get("$schema").is_none());
    assert_eq!(schema["additionalProperties"], false);
    let required = schema["required"].as_array().unwrap();
    assert_eq!(required.len(), 2);
    for name in ["value", "note"] {
        assert!(required.iter().any(|value| value.as_str() == Some(name)));
    }
}

#[tokio::test]
async fn delayed_sse_body_is_deadline_exceeded() {
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1/responses",
        response(Duration::from_millis(200)),
    )])
    .unwrap();
    let context = ModelCallContext::new().with_deadline(Instant::now() + Duration::from_millis(40));

    let error = client(&server)
        .invoke(request(), context)
        .await
        .unwrap_err();

    assert_eq!(error.kind, ModelErrorKind::DeadlineExceeded);
}

#[tokio::test]
async fn oversized_error_body_fails_before_unbounded_buffering() {
    let mut response = ScriptedResponse::ok(vec![ResponseChunk::text("x".repeat(1024 * 1024 + 1))]);
    response.status = 500;
    let server =
        CassetteServer::start(vec![HttpExchange::new("POST", "/v1/responses", response)]).unwrap();

    let error = client(&server)
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap_err();

    assert_eq!(error.kind, ModelErrorKind::Provider);
    assert_eq!(error.metadata["http.status"], 500);
    assert_eq!(error.metadata["http.error_body_truncated"], true);
}

#[tokio::test]
async fn public_openai_chat_dialect_reaches_the_http_request() {
    let response = ScriptedResponse::ok(vec![ResponseChunk::text(concat!(
        "data: {\"id\":\"chat_1\",\"model\":\"chat-model\",",
        "\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},",
        "\"finish_reason\":\"stop\"}]}\n\n",
        "data: {\"id\":\"chat_1\",\"model\":\"chat-model\",",
        "\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\n",
        "data: [DONE]\n\n"
    ))])
    .with_header("content-type", "text/event-stream");
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1/chat/completions",
        response,
    )])
    .unwrap();
    let config = OpenAiConfig::new("secret")
        .unwrap()
        .with_base_url(&format!("{}v1/", server.base_url()))
        .unwrap()
        .with_wire_protocol(OpenAiWireProtocol::ChatCompletions);
    let client = OpenAiClient::new(config);
    let mut request = ModelRequest::new(
        ModelRef::new("openai", "chat-model"),
        Message::user("hello"),
    );
    request.generation.max_output_tokens = Some(64);

    let result = client
        .invoke(request, ModelCallContext::new())
        .await
        .unwrap();

    assert_eq!(result.text(), "ok");
    assert_eq!(result.usage.output_tokens, 1);
    let body: serde_json::Value =
        serde_json::from_slice(&server.observed_requests()[0].body).unwrap();
    assert_eq!(body["max_completion_tokens"], 64);
    assert!(body.get("max_tokens").is_none());
    assert_eq!(body["stream_options"]["include_usage"], true);
}

#[tokio::test]
async fn complete_chat_response_uses_the_chat_decoder() {
    let response = ScriptedResponse::ok(vec![ResponseChunk::text(
        serde_json::json!({
            "id":"chat-complete",
            "object":"chat.completion",
            "model":"chat-model",
            "choices":[{
                "index":0,
                "message":{"role":"assistant","content":"complete ok"},
                "finish_reason":"stop"
            }],
            "usage":{"prompt_tokens":2,"completion_tokens":2}
        })
        .to_string(),
    )])
    .with_header("content-type", "application/json");
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1/chat/completions",
        response,
    )])
    .unwrap();
    let config = OpenAiConfig::new("secret")
        .unwrap()
        .with_base_url(&format!("{}v1/", server.base_url()))
        .unwrap()
        .with_wire_protocol(OpenAiWireProtocol::ChatCompletions);
    let client = OpenAiClient::new(config);
    let request = ModelRequest::new(
        ModelRef::new("openai", "chat-model"),
        Message::user("hello"),
    )
    .response_mode(ResponseMode::Complete);

    let result = client
        .invoke(request, ModelCallContext::new())
        .await
        .unwrap();

    assert_eq!(result.text(), "complete ok");
    assert_eq!(result.usage.output_tokens, 2);
    let body: serde_json::Value =
        serde_json::from_slice(&server.observed_requests()[0].body).unwrap();
    assert_eq!(body["stream"], false);
}

#[tokio::test]
async fn official_streamed_image_item_reaches_canonical_media() {
    let response = ScriptedResponse::ok(vec![ResponseChunk::text(concat!(
        "data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_image\",\"model\":\"gpt-test\"}}\n\n",
        "data: {\"type\":\"response.image_generation_call.completed\",\"sequence_number\":1,\"output_index\":0,\"item_id\":\"image_1\"}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"sequence_number\":2,\"output_index\":0,\"item\":{\"id\":\"image_1\",\"type\":\"image_generation_call\",\"status\":\"completed\",\"result\":\"UklGRiQAAABXRUJQ\"}}\n\n",
        "data: {\"type\":\"response.completed\",\"sequence_number\":3,\"response\":{\"status\":\"completed\",\"usage\":{}}}\n\n"
    ))])
    .with_header("content-type", "text/event-stream");
    let server =
        CassetteServer::start(vec![HttpExchange::new("POST", "/v1/responses", response)]).unwrap();

    let result = client(&server)
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap();

    assert!(matches!(
        &result.content[0],
        ContentPart::Image {
            source: runifold_model::MediaSource::Base64 { media_type, data }
        } if media_type == "image/webp" && data == "UklGRiQAAABXRUJQ"
    ));
    assert!(
        result
            .provider_events
            .iter()
            .all(|event| !event.value.to_string().contains("UklGRiQAAABXRUJQ"))
    );
}

#[tokio::test]
async fn complete_responses_body_uses_the_canonical_model_path() {
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1/responses",
        ScriptedResponse::json(
            200,
            &serde_json::json!({
                "id": "resp_complete",
                "model": "gpt-test",
                "status": "completed",
                "output": [{
                    "type": "message",
                    "status": "completed",
                    "content": [{"type": "output_text", "text": "complete"}]
                }],
                "usage": {"input_tokens": 2, "output_tokens": 1}
            }),
        )
        .unwrap(),
    )])
    .unwrap();
    let request = request().response_mode(ResponseMode::Complete);

    let response = client(&server)
        .invoke(request, ModelCallContext::new())
        .await
        .unwrap();

    assert_eq!(response.text(), "complete");
    assert_eq!(response.usage.input_tokens, 2);
    let observed = server.observed_requests();
    let body: serde_json::Value = serde_json::from_slice(&observed[0].body).unwrap();
    assert_eq!(body["stream"], false);
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn complete_function_call_round_trip_replays_required_status() {
    let first = ScriptedResponse::json(
        200,
        &serde_json::json!({
            "id": "resp_tool",
            "model": "gpt-test",
            "status": "completed",
            "output": [{
                "type": "function_call",
                "id": "fc_tool",
                "call_id": "call_tool",
                "name": "lookup",
                "arguments": "{\"value\":7}",
                "status": "completed"
            }],
            "usage": {}
        }),
    )
    .unwrap();
    let second = ScriptedResponse::json(
        200,
        &serde_json::json!({
            "id": "resp_done",
            "model": "gpt-test",
            "status": "completed",
            "output": [{
                "type": "message",
                "status": "completed",
                "content": [{"type": "output_text", "text": "done"}]
            }],
            "usage": {}
        }),
    )
    .unwrap();
    let server = CassetteServer::start(vec![
        HttpExchange::new("POST", "/v1/responses", first),
        HttpExchange::new("POST", "/v1/responses", second),
    ])
    .unwrap();
    let client = client(&server);
    let first_response = client
        .invoke(
            request().response_mode(ResponseMode::Complete),
            ModelCallContext::new(),
        )
        .await
        .unwrap();
    let tool_call = first_response.content[0].clone();
    let tool_result = ContentPart::ToolResult(ToolResult {
        call_id: "call_tool".into(),
        name: Some("lookup".into()),
        content: vec![ContentPart::text("7")],
        structured_content: None,
        is_error: false,
        metadata: BTreeMap::default(),
    });
    let second_request = request()
        .response_mode(ResponseMode::Complete)
        .message(Message::new(Role::Assistant, vec![tool_call]).unwrap())
        .message(Message::new(Role::Tool, vec![tool_result]).unwrap());

    let response = client
        .invoke(second_request, ModelCallContext::new())
        .await
        .unwrap();

    assert_eq!(response.text(), "done");
    let observed = server.observed_requests();
    let body: serde_json::Value = serde_json::from_slice(&observed[1].body).unwrap();
    assert_eq!(body["input"][1]["type"], "function_call");
    assert_eq!(body["input"][1]["id"], "fc_tool");
    assert_eq!(body["input"][1]["call_id"], "call_tool");
    assert_eq!(body["input"][1]["status"], "completed");
    assert_eq!(body["input"][2]["type"], "function_call_output");
    assert_eq!(body["input"][2]["call_id"], "call_tool");
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn streaming_function_call_round_trip_replays_required_status() {
    let first = ScriptedResponse::ok(vec![ResponseChunk::text(concat!(
        "data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_tool\",\"model\":\"gpt-test\"}}\n\n",
        "data: {\"type\":\"response.output_item.added\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc_stream\",\"call_id\":\"call_stream\",\"name\":\"lookup\",\"arguments\":\"\",\"status\":\"in_progress\"}}\n\n",
        "data: {\"type\":\"response.function_call_arguments.done\",\"sequence_number\":2,\"item_id\":\"fc_stream\",\"output_index\":0,\"arguments\":\"{\\\"value\\\":8}\"}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"sequence_number\":3,\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc_stream\",\"call_id\":\"call_stream\",\"name\":\"lookup\",\"arguments\":\"{\\\"value\\\":8}\",\"status\":\"completed\"}}\n\n",
        "data: {\"type\":\"response.completed\",\"sequence_number\":4,\"response\":{\"status\":\"completed\",\"usage\":{}}}\n\n"
    ))])
    .with_header("content-type", "text/event-stream");
    let second = ScriptedResponse::json(
        200,
        &serde_json::json!({
            "id": "resp_done",
            "model": "gpt-test",
            "status": "completed",
            "output": [{
                "type": "message",
                "status": "completed",
                "content": [{"type": "output_text", "text": "done"}]
            }],
            "usage": {}
        }),
    )
    .unwrap();
    let server = CassetteServer::start(vec![
        HttpExchange::new("POST", "/v1/responses", first),
        HttpExchange::new("POST", "/v1/responses", second),
    ])
    .unwrap();
    let client = client(&server);
    let first_response = client
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap();
    let second_request = request()
        .response_mode(ResponseMode::Complete)
        .message(Message::new(Role::Assistant, first_response.content).unwrap())
        .message(
            Message::new(
                Role::Tool,
                vec![ContentPart::ToolResult(ToolResult {
                    call_id: "call_stream".into(),
                    name: Some("lookup".into()),
                    content: vec![ContentPart::text("8")],
                    structured_content: None,
                    is_error: false,
                    metadata: BTreeMap::default(),
                })],
            )
            .unwrap(),
        );

    client
        .invoke(second_request, ModelCallContext::new())
        .await
        .unwrap();

    let observed = server.observed_requests();
    let body: serde_json::Value = serde_json::from_slice(&observed[1].body).unwrap();
    assert_eq!(body["input"][1]["id"], "fc_stream");
    assert_eq!(body["input"][1]["call_id"], "call_stream");
    assert_eq!(body["input"][1]["arguments"], "{\"value\":8}");
    assert_eq!(body["input"][1]["status"], "completed");
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn mixed_native_and_function_output_items_are_both_replayed() {
    let first = ScriptedResponse::json(
        200,
        &serde_json::json!({
            "id": "resp_mixed",
            "model": "gpt-test",
            "status": "completed",
            "output": [
                {
                    "type": "web_search_call",
                    "id": "ws_mixed",
                    "status": "completed",
                    "action": {"type": "search", "query": "Runifold"}
                },
                {
                    "type": "function_call",
                    "id": "fc_mixed",
                    "call_id": "call_mixed",
                    "name": "lookup",
                    "arguments": "{}",
                    "status": "completed"
                }
            ],
            "usage": {}
        }),
    )
    .unwrap();
    let second = ScriptedResponse::json(
        200,
        &serde_json::json!({
            "id": "resp_done",
            "model": "gpt-test",
            "status": "completed",
            "output": [{
                "type": "message",
                "status": "completed",
                "content": [{"type": "output_text", "text": "done"}]
            }],
            "usage": {}
        }),
    )
    .unwrap();
    let server = CassetteServer::start(vec![
        HttpExchange::new("POST", "/v1/responses", first),
        HttpExchange::new("POST", "/v1/responses", second),
    ])
    .unwrap();
    let client = client(&server);
    let first_response = client
        .invoke(
            request().response_mode(ResponseMode::Complete),
            ModelCallContext::new(),
        )
        .await
        .unwrap();
    let second_request = request()
        .response_mode(ResponseMode::Complete)
        .message(Message::new(Role::Assistant, first_response.content).unwrap())
        .message(
            Message::new(
                Role::Tool,
                vec![ContentPart::ToolResult(ToolResult {
                    call_id: "call_mixed".into(),
                    name: Some("lookup".into()),
                    content: vec![ContentPart::text("ok")],
                    structured_content: None,
                    is_error: false,
                    metadata: BTreeMap::default(),
                })],
            )
            .unwrap(),
        );

    client
        .invoke(second_request, ModelCallContext::new())
        .await
        .unwrap();

    let observed = server.observed_requests();
    let body: serde_json::Value = serde_json::from_slice(&observed[1].body).unwrap();
    assert_eq!(body["input"][1]["type"], "web_search_call");
    assert_eq!(body["input"][1]["id"], "ws_mixed");
    assert_eq!(body["input"][1]["status"], "completed");
    assert_eq!(body["input"][2]["type"], "function_call");
    assert_eq!(body["input"][2]["status"], "completed");
    assert_eq!(body["input"][3]["type"], "function_call_output");
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn compatible_profile_preserves_identity_reasoning_usage_and_attribution() {
    let response = ScriptedResponse::ok(vec![ResponseChunk::text(concat!(
        "data: {\"id\":\"chat_1\",\"model\":\"deepseek-reasoner\",",
        "\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\"step\"},",
        "\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"chat_1\",\"model\":\"deepseek-reasoner\",",
        "\"choices\":[{\"index\":0,\"delta\":{\"content\":\"answer\"},",
        "\"finish_reason\":\"stop\"}],",
        "\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2,",
        "\"completion_tokens_details\":{\"reasoning_tokens\":1}}}\n\n",
        "data: [DONE]\n\n"
    ))])
    .with_header("content-type", "text/event-stream");
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1/chat/completions",
        response,
    )])
    .unwrap();
    let config = OpenAiConfig::from_profile(OpenAiCompatibleProfile::OpenRouter, "secret")
        .unwrap()
        .with_base_url(&format!("{}v1/", server.base_url()))
        .unwrap()
        .with_openrouter_attribution("https://example.com/runifold", "Runifold test")
        .unwrap();
    let client = OpenAiClient::new(config);
    let request = ModelRequest::new(
        ModelRef::new("openrouter", "deepseek/deepseek-reasoner"),
        Message::user("reason"),
    );

    let report = verify_success(
        &client,
        request,
        ModelCallContext::new(),
        &SuccessContract::new("openrouter")
            .visible_text("answer")
            .reasoning("step")
            .usage(ModelUsage {
                input_tokens: 3,
                output_tokens: 2,
                reasoning_tokens: 1,
                ..ModelUsage::default()
            })
            .provider_events(),
    )
    .await
    .unwrap();

    assert_eq!(report.provider(), "openrouter");
    assert_eq!(report.checks().len(), 5);
    let observed = server.observed_requests();
    assert_eq!(
        observed[0].headers["http-referer"],
        "https://example.com/runifold"
    );
    assert_eq!(observed[0].headers["x-openrouter-title"], "Runifold test");
    assert_eq!(observed[0].headers["authorization"], "[REDACTED]");
    server.assert_finished().unwrap();
}
