//! Concurrency, timeout, offline, and truncation tests for Gemini transport.
#![cfg(feature = "gemini")]

use std::{
    net::TcpListener,
    time::{Duration, Instant},
};

use futures_util::future::join_all;
use runifold_model::{Message, Model, ModelCallContext, ModelErrorKind, ModelRef, ModelRequest};
use runifold_provider_testkit::{CassetteServer, HttpExchange, ResponseChunk, ScriptedResponse};
use runifold_providers::gemini::{GeminiClient, GeminiConfig};

const PATH: &str = "/v1beta/models/gemini-test:streamGenerateContent?alt=sse";

fn request() -> ModelRequest {
    ModelRequest::new(
        ModelRef::new("gemini", "gemini-test"),
        Message::user("stress"),
    )
}

fn client(base_url: &str) -> GeminiClient {
    GeminiClient::new(
        GeminiConfig::new("secret")
            .unwrap()
            .with_base_url(base_url)
            .unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_client_isolates_32_concurrent_invocations() {
    let response = ScriptedResponse::ok(vec![
        ResponseChunk::text(
            "data: {\"responseId\":\"shared\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"ok\"}]},\"finishReason\":\"STOP\"}]}\n\n",
        )
        .after(Duration::from_millis(20)),
    ])
    .with_header("content-type", "text/event-stream");
    let server =
        CassetteServer::start_repeating(HttpExchange::new("POST", PATH, response), 32).unwrap();
    let client = client(&format!("{}v1beta/", server.base_url()));

    let results =
        join_all((0..32).map(|_| client.invoke(request(), ModelCallContext::new()))).await;

    assert!(results.iter().all(Result::is_ok));
    server.assert_finished().unwrap();
    let stats = server.stats();
    assert_eq!(stats.accepted, 32);
    assert_eq!(stats.completed, 32);
    assert!(stats.max_in_flight > 1);
}

#[tokio::test]
async fn body_timeout_retains_deadline_exceeded_classification() {
    let response = ScriptedResponse::ok(vec![
        ResponseChunk::text(
            "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"late\"}]},\"finishReason\":\"STOP\"}]}\n\n",
        )
        .after(Duration::from_millis(200)),
    ])
    .with_header("content-type", "text/event-stream");
    let server = CassetteServer::start(vec![HttpExchange::new("POST", PATH, response)]).unwrap();
    let context = ModelCallContext::new().with_deadline(Instant::now() + Duration::from_millis(40));

    let error = client(&format!("{}v1beta/", server.base_url()))
        .invoke(request(), context)
        .await
        .unwrap_err();

    assert_eq!(error.kind, ModelErrorKind::DeadlineExceeded);
}

#[tokio::test]
async fn unavailable_loopback_endpoint_is_a_transport_error() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);

    let error = client(&format!("http://{address}/v1beta/"))
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap_err();

    assert_eq!(error.kind, ModelErrorKind::Transport);
}

#[tokio::test]
async fn truncated_sse_never_becomes_a_partial_success() {
    let response = ScriptedResponse::ok(vec![ResponseChunk::text(
        "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"partial\"}]}}]}\n\n",
    )])
    .with_header("content-type", "text/event-stream")
    .disconnect_after_chunks();
    let server = CassetteServer::start(vec![HttpExchange::new("POST", PATH, response)]).unwrap();

    let error = client(&format!("{}v1beta/", server.base_url()))
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap_err();

    assert!(matches!(
        error.kind,
        ModelErrorKind::Transport | ModelErrorKind::Protocol
    ));
}
