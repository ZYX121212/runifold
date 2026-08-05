//! Real-HTTP concurrency and timeout tests for the `OpenAI` provider.
#![cfg(feature = "openai")]

use std::time::{Duration, Instant};

use futures_util::future::join_all;
use runifold_model::{
    Message, Model, ModelCallContext, ModelErrorKind, ModelRef, ModelRequest, ModelUsage,
    ResponseMode,
};
use runifold_provider_testkit::{
    CassetteServer, HttpExchange, ResponseChunk, ScriptedResponse, SuccessContract, verify_success,
};
use runifold_providers::openai::{OpenAiClient, OpenAiCompatibleProfile, OpenAiConfig};

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
