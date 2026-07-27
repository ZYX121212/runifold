//! Real-HTTP concurrency and timeout tests for the `OpenAI` provider.

use std::time::{Duration, Instant};

use futures_util::future::join_all;
use runifold_model::{Message, Model, ModelCallContext, ModelErrorKind, ModelRef, ModelRequest};
use runifold_provider_openai::{OpenAiClient, OpenAiConfig};
use runifold_provider_testkit::{CassetteServer, HttpExchange, ResponseChunk, ScriptedResponse};

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
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp\",\"model\":\"gpt-test\"}}\n\ndata: {\"type\":\"response.content_part.added\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"ok\"}\n\ndata: {\"type\":\"response.content_part.done\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n",
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
