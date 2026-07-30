//! End-to-end Anthropic protocol tests over a real loopback HTTP socket.
#![cfg(feature = "anthropic")]

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    time::{Duration, Instant},
};

use futures_util::StreamExt;
use runifold_core::RetrySafety;
use runifold_model::{
    ContentPart, FinishReason, Message, Model, ModelCallContext, ModelErrorKind, ModelRef,
    ModelRequest, ModelUsage, ToolCall,
};
use runifold_provider_testkit::{
    CassetteServer, ErrorContract, HttpExchange, ResponseChunk, ScriptedResponse, SuccessContract,
    verify_error, verify_success,
};
use runifold_providers::anthropic::{AnthropicClient, AnthropicConfig};
use serde_json::json;

fn sse_response(events: &[serde_json::Value]) -> ScriptedResponse {
    let body = events.iter().fold(String::new(), |mut body, event| {
        write!(body, "event: message\ndata: {event}\n\n").expect("writing to a String cannot fail");
        body
    });
    ScriptedResponse::ok(vec![ResponseChunk::text(body)])
        .with_header("content-type", "text/event-stream")
}

fn request() -> ModelRequest {
    ModelRequest::new(
        ModelRef::new("anthropic", "claude-test"),
        Message::user("hello"),
    )
}

fn client(server: &CassetteServer) -> AnthropicClient {
    let config = AnthropicConfig::new("test-secret")
        .unwrap()
        .with_base_url(&format!("{}v1/", server.base_url()))
        .unwrap();
    AnthropicClient::new(config)
}

#[tokio::test]
async fn invokes_text_over_real_http_and_redacts_the_key() {
    let events = vec![
        json!({
            "type":"message_start",
            "message":{
                "id":"msg_1",
                "model":"claude-test",
                "usage":{"input_tokens":5,"output_tokens":1}
            }
        }),
        json!({
            "type":"content_block_start",
            "index":0,
            "content_block":{"type":"text","text":""}
        }),
        json!({
            "type":"content_block_delta",
            "index":0,
            "delta":{"type":"text_delta","text":"Hello"}
        }),
        json!({"type":"content_block_stop","index":0}),
        json!({
            "type":"message_delta",
            "delta":{"stop_reason":"end_turn","stop_sequence":null},
            "usage":{"output_tokens":3}
        }),
        json!({"type":"message_stop"}),
    ];
    let server = CassetteServer::start(vec![
        HttpExchange::new("POST", "/v1/messages", sse_response(&events)).with_json_body(json!({
            "model":"claude-test",
            "messages":[{
                "role":"user",
                "content":[{"type":"text","text":"hello"}]
            }],
            "stream":true,
            "max_tokens":1024
        })),
    ])
    .unwrap();

    let report = verify_success(
        &client(&server),
        request(),
        ModelCallContext::new(),
        &SuccessContract::new("anthropic")
            .visible_text("Hello")
            .usage(ModelUsage {
                input_tokens: 5,
                output_tokens: 3,
                ..ModelUsage::default()
            })
            .provider_events(),
    )
    .await
    .unwrap();

    assert_eq!(report.checks().len(), 4);
    server.assert_finished().unwrap();
    let observed = server.observed_requests();
    assert_eq!(observed[0].headers["x-api-key"], "[REDACTED]");
    assert_eq!(observed[0].headers["anthropic-version"], "2023-06-01");
}

#[tokio::test]
async fn preserves_structured_rate_limit_metadata() {
    let response = ScriptedResponse::json(
        429,
        &json!({
            "type":"error",
            "error":{"type":"rate_limit_error","message":"slow down"}
        }),
    )
    .unwrap()
    .with_header("request-id", "req_limit")
    .with_header("retry-after", "2");
    let server =
        CassetteServer::start_repeating(HttpExchange::new("POST", "/v1/messages", response), 2)
            .unwrap();

    let report = verify_error(
        &client(&server),
        request(),
        ModelCallContext::new(),
        &ErrorContract::new("anthropic", ModelErrorKind::Provider, RetrySafety::Safe),
    )
    .await
    .unwrap();
    assert_eq!(report.checks().len(), 1);

    let error = client(&server)
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap_err();

    assert_eq!(error.kind, ModelErrorKind::Provider);
    assert_eq!(error.metadata["http.status"], 429);
    assert_eq!(error.metadata["anthropic.request_id"], "req_limit");
    assert_eq!(error.metadata["retry.after_ms"], 2_000);
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn reconstructs_fragmented_tool_arguments() {
    let events = vec![
        json!({
            "type":"message_start",
            "message":{"id":"msg_tool","model":"claude-test","usage":{"input_tokens":7}}
        }),
        json!({
            "type":"content_block_start",
            "index":0,
            "content_block":{"type":"tool_use","id":"tool_1","name":"weather","input":{}}
        }),
        json!({
            "type":"content_block_delta",
            "index":0,
            "delta":{"type":"input_json_delta","partial_json":"{\"city\":\"Shang"}
        }),
        json!({
            "type":"content_block_delta",
            "index":0,
            "delta":{"type":"input_json_delta","partial_json":"hai\"}"}
        }),
        json!({"type":"content_block_stop","index":0}),
        json!({
            "type":"message_delta",
            "delta":{"stop_reason":"tool_use"},
            "usage":{"output_tokens":11}
        }),
        json!({"type":"message_stop"}),
    ];
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1/messages",
        sse_response(&events),
    )])
    .unwrap();

    let response = client(&server)
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap();

    assert_eq!(response.finish_reason, FinishReason::ToolCalls);
    assert_eq!(
        response.content,
        vec![ContentPart::ToolCall(ToolCall {
            id: "tool_1".into(),
            name: "weather".into(),
            arguments: json!({"city":"Shanghai"}),
            raw_arguments: Some("{\"city\":\"Shanghai\"}".into()),
            metadata: BTreeMap::default(),
        })]
    );
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn rejects_a_stream_that_disconnects_before_message_stop() {
    let events = vec![
        json!({
            "type":"message_start",
            "message":{"id":"msg_broken","model":"claude-test","usage":{}}
        }),
        json!({
            "type":"content_block_start",
            "index":0,
            "content_block":{"type":"text","text":""}
        }),
        json!({
            "type":"content_block_delta",
            "index":0,
            "delta":{"type":"text_delta","text":"partial"}
        }),
    ];
    let response = sse_response(&events).disconnect_after_chunks();
    let server =
        CassetteServer::start(vec![HttpExchange::new("POST", "/v1/messages", response)]).unwrap();

    let error = client(&server)
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap_err();

    assert_eq!(error.kind, ModelErrorKind::Transport);
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn cancellation_interrupts_a_delayed_stream_chunk() {
    let response = ScriptedResponse::ok(vec![
        ResponseChunk::text(
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg\",\"model\":\"claude-test\",\"usage\":{}}}\n\n",
        )
        .after(Duration::from_millis(300)),
    ])
    .with_header("content-type", "text/event-stream");
    let server =
        CassetteServer::start(vec![HttpExchange::new("POST", "/v1/messages", response)]).unwrap();
    let context = ModelCallContext::new();
    let cancellation = context.cancellation().clone();
    let mut stream = client(&server).stream(request(), context).await.unwrap();

    cancellation.cancel();
    let error = stream.next().await.unwrap().unwrap_err();

    assert_eq!(error.kind, ModelErrorKind::Cancelled);
}

#[tokio::test]
async fn an_elapsed_deadline_fails_before_opening_transport() {
    let server = CassetteServer::start(Vec::new()).unwrap();
    let context = ModelCallContext::new().with_deadline(Instant::now());

    let Err(error) = client(&server).stream(request(), context).await else {
        panic!("elapsed deadline unexpectedly opened a stream");
    };

    assert_eq!(error.kind, ModelErrorKind::DeadlineExceeded);
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn delayed_sse_body_preserves_deadline_exceeded() {
    let response = ScriptedResponse::ok(vec![
        ResponseChunk::text(
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg\",\"model\":\"claude-test\",\"usage\":{}}}\n\n",
        )
        .after(Duration::from_millis(200)),
    ])
    .with_header("content-type", "text/event-stream");
    let server =
        CassetteServer::start(vec![HttpExchange::new("POST", "/v1/messages", response)]).unwrap();
    let context = ModelCallContext::new().with_deadline(Instant::now() + Duration::from_millis(40));

    let error = client(&server)
        .invoke(request(), context)
        .await
        .unwrap_err();

    assert_eq!(error.kind, ModelErrorKind::DeadlineExceeded);
}
