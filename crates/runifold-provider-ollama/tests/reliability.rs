//! Concurrency, timeout, and truncation tests for Ollama transport.

use std::time::{Duration, Instant};

use futures_util::future::join_all;
use runifold_model::{Message, Model, ModelCallContext, ModelErrorKind, ModelRef, ModelRequest};
use runifold_provider_ollama::{OllamaClient, OllamaConfig};
use runifold_provider_testkit::{CassetteServer, HttpExchange, ResponseChunk, ScriptedResponse};

fn request() -> ModelRequest {
    ModelRequest::new(ModelRef::new("ollama", "qwen3"), Message::user("stress"))
}

fn client(server: &CassetteServer) -> OllamaClient {
    OllamaClient::new(OllamaConfig::new(&server.base_url()).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_client_isolates_32_concurrent_ndjson_streams() {
    let response = ScriptedResponse::ok(vec![
        ResponseChunk::text(
            "{\"model\":\"qwen3\",\"message\":{\"content\":\"ok\"},\"done\":true,\"done_reason\":\"stop\"}\n",
        )
        .after(Duration::from_millis(20)),
    ])
    .with_header("content-type", "application/x-ndjson");
    let server =
        CassetteServer::start_repeating(HttpExchange::new("POST", "/api/chat", response), 32)
            .unwrap();
    let client = client(&server);

    let results =
        join_all((0..32).map(|_| client.invoke(request(), ModelCallContext::new()))).await;

    assert!(results.iter().all(Result::is_ok));
    server.assert_finished().unwrap();
    assert_eq!(server.stats().completed, 32);
    assert!(server.stats().max_in_flight > 1);
}

#[tokio::test]
async fn ndjson_body_timeout_is_deadline_exceeded() {
    let response = ScriptedResponse::ok(vec![
        ResponseChunk::text(
            "{\"model\":\"qwen3\",\"message\":{\"content\":\"late\"},\"done\":true}\n",
        )
        .after(Duration::from_millis(200)),
    ])
    .with_header("content-type", "application/x-ndjson");
    let server =
        CassetteServer::start(vec![HttpExchange::new("POST", "/api/chat", response)]).unwrap();
    let context = ModelCallContext::new().with_deadline(Instant::now() + Duration::from_millis(40));

    let error = client(&server)
        .invoke(request(), context)
        .await
        .unwrap_err();

    assert_eq!(error.kind, ModelErrorKind::DeadlineExceeded);
}

#[tokio::test]
async fn truncated_ndjson_never_becomes_a_partial_success() {
    let response = ScriptedResponse::ok(vec![ResponseChunk::text(
        "{\"model\":\"qwen3\",\"message\":{\"content\":\"partial\"},\"done\":false}\n",
    )])
    .with_header("content-type", "application/x-ndjson")
    .disconnect_after_chunks();
    let server =
        CassetteServer::start(vec![HttpExchange::new("POST", "/api/chat", response)]).unwrap();

    let error = client(&server)
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap_err();

    assert!(matches!(
        error.kind,
        ModelErrorKind::Transport | ModelErrorKind::Protocol
    ));
}
