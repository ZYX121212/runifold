//! Amazon Bedrock Converse Stream tests over a real loopback HTTP socket.
#![cfg(feature = "bedrock")]

use std::{
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

use aws_sdk_bedrockruntime::{
    Config,
    config::{BehaviorVersion, Credentials, Region},
};
use aws_smithy_eventstream::frame::write_message_to;
use aws_smithy_types::event_stream::{Header, HeaderValue, Message as EventMessage};
use runifold_model::{
    Message, Model, ModelCallContext, ModelErrorKind, ModelRef, ModelRequest, ModelUsage,
};
use runifold_provider_testkit::{
    BenchmarkPlan, CassetteServer, HttpExchange, ModelBenchmarkTarget, ResponseChunk,
    ScriptedResponse, benchmark,
};
use runifold_providers::bedrock::BedrockClient;
use serde_json::{Value, json};

const MODEL: &str = "model-test";
const ACCESS_KEY: &str = "test-access-key";
const SECRET_KEY: &str = "test-secret-key";
const SESSION_TOKEN: &str = "test-session-token";

fn request() -> ModelRequest {
    ModelRequest::new(ModelRef::new("bedrock", MODEL), Message::user("hello"))
}

fn client(server: &CassetteServer) -> BedrockClient {
    let credentials = Credentials::new(
        ACCESS_KEY,
        SECRET_KEY,
        Some(SESSION_TOKEN.into()),
        None,
        "runifold-bedrock-cassette",
    );
    let config = Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(credentials)
        .endpoint_url(server.base_url())
        .build();
    BedrockClient::new(&config)
}

fn event(event_type: &str, payload: &Value) -> Vec<u8> {
    let headers = vec![
        Header::new(":message-type", HeaderValue::String("event".into())),
        Header::new(
            ":event-type",
            HeaderValue::String(event_type.to_owned().into()),
        ),
        Header::new(
            ":content-type",
            HeaderValue::String("application/json".into()),
        ),
    ];
    let message = EventMessage::new_from_parts(
        headers,
        serde_json::to_vec(payload).expect("test event payload must serialize"),
    );
    let mut bytes = Vec::new();
    write_message_to(&message, &mut bytes).expect("test event frame must serialize");
    bytes
}

fn successful_body(text: &str) -> Vec<u8> {
    [
        event("messageStart", &json!({"role":"assistant"})),
        event("contentBlockStart", &json!({"contentBlockIndex":0})),
        event(
            "contentBlockDelta",
            &json!({"contentBlockIndex":0,"delta":{"text":text}}),
        ),
        event("contentBlockStop", &json!({"contentBlockIndex":0})),
        event("messageStop", &json!({"stopReason":"end_turn"})),
        event(
            "metadata",
            &json!({
                "usage":{
                    "inputTokens":5,
                    "outputTokens":3,
                    "totalTokens":8,
                    "cacheReadInputTokens":2,
                    "cacheWriteInputTokens":1
                },
                "metrics":{"latencyMs":17}
            }),
        ),
    ]
    .concat()
}

fn event_stream_response(body: &[u8]) -> ScriptedResponse {
    let chunks = body
        .chunks(11)
        .map(|fragment| ResponseChunk {
            body: fragment.to_vec(),
            delay: Duration::ZERO,
        })
        .collect();
    ScriptedResponse::ok(chunks)
        .with_header("content-type", "application/vnd.amazon.eventstream")
        .with_header("x-amzn-requestid", "bedrock-cassette-request")
}

fn exchange(response: ScriptedResponse) -> HttpExchange {
    HttpExchange::new("POST", format!("/model/{MODEL}/converse-stream"), response).with_json_body(
        json!({
            "inferenceConfig":{},
            "messages":[{
                "role":"user",
                "content":[{"text":"hello"}]
            }]
        }),
    )
}

#[tokio::test]
async fn invokes_binary_event_stream_with_sigv4_and_redacted_temporary_credentials() {
    let server = CassetteServer::start(vec![exchange(event_stream_response(&successful_body(
        "Hello",
    )))])
    .unwrap();

    let response = client(&server)
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap();

    assert_eq!(response.text(), "Hello");
    assert_eq!(
        response.usage,
        ModelUsage {
            input_tokens: 5,
            output_tokens: 3,
            cached_input_tokens: 2,
            cache_write_tokens: 1,
            ..ModelUsage::default()
        }
    );
    assert!(!response.provider_events.is_empty());
    server.assert_finished().unwrap();

    let observed = server.observed_requests();
    assert_eq!(observed[0].headers["authorization"], "[REDACTED]");
    assert_eq!(observed[0].headers["x-amz-security-token"], "[REDACTED]");
    assert!(observed[0].headers.contains_key("x-amz-date"));
    let body = String::from_utf8_lossy(&observed[0].body);
    assert!(!body.contains(ACCESS_KEY));
    assert!(!body.contains(SECRET_KEY));
    assert!(!body.contains(SESSION_TOKEN));
}

#[tokio::test]
async fn truncated_binary_stream_never_becomes_partial_success() {
    let complete = successful_body("partial");
    let truncated = complete[..complete.len() - 7].to_vec();
    let response = event_stream_response(&truncated).disconnect_after_chunks();
    let server = CassetteServer::start(vec![exchange(response)]).unwrap();

    let error = client(&server)
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap_err();

    assert!(matches!(
        error.kind,
        ModelErrorKind::Transport | ModelErrorKind::Protocol
    ));
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn delayed_binary_frame_preserves_deadline_exceeded() {
    let response = ScriptedResponse::ok(vec![ResponseChunk {
        body: successful_body("late"),
        delay: Duration::from_millis(200),
    }])
    .with_header("content-type", "application/vnd.amazon.eventstream");
    let server = CassetteServer::start(vec![exchange(response)]).unwrap();
    let context = ModelCallContext::new().with_deadline(Instant::now() + Duration::from_millis(40));

    let error = client(&server)
        .invoke(request(), context)
        .await
        .unwrap_err();

    assert_eq!(error.kind, ModelErrorKind::DeadlineExceeded);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_sdk_client_isolates_sixteen_concurrent_binary_streams() {
    let response = ScriptedResponse::ok(vec![ResponseChunk {
        body: successful_body("parallel"),
        delay: Duration::from_millis(40),
    }])
    .with_header("content-type", "application/vnd.amazon.eventstream");
    let server = CassetteServer::start_repeating(exchange(response), 16).unwrap();
    let target = Arc::new(ModelBenchmarkTarget::new(
        Arc::new(client(&server)),
        request(),
    ));
    let plan = BenchmarkPlan::new(NonZeroUsize::new(16).unwrap())
        .with_concurrency(NonZeroUsize::new(16).unwrap());

    let report = benchmark("runifold-bedrock", target, plan).await.unwrap();

    assert_eq!(report.measured_runs, 16);
    assert_eq!(report.successes, 16);
    assert_eq!(report.failures, 0);
    assert!(report.ttft.is_some());
    assert!(report.total_latency.is_some());
    assert!(report.throughput_per_second.is_finite());
    server.assert_finished().unwrap();
    assert!(server.stats().max_in_flight > 1);
}
