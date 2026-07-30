//! Azure `OpenAI` v1 authentication and Responses conformance over loopback HTTP.
#![cfg(feature = "openai")]

use runifold_model::{Message, ModelCallContext, ModelRef, ModelRequest, ModelUsage};
use runifold_provider_testkit::{
    CassetteServer, HttpExchange, ResponseChunk, ScriptedResponse, SuccessContract, verify_success,
};
use runifold_providers::openai::{AzureOpenAiApiVersion, OpenAiClient, OpenAiConfig};

fn response() -> ScriptedResponse {
    ScriptedResponse::ok(vec![ResponseChunk::text(concat!(
        "data: {\"type\":\"response.created\",\"response\":",
        "{\"id\":\"resp_azure\",\"model\":\"deployment-test\"}}\n\n",
        "data: {\"type\":\"response.content_part.added\",\"output_index\":0,",
        "\"content_index\":0,\"part\":{\"type\":\"output_text\"}}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,",
        "\"content_index\":0,\"delta\":\"azure\"}\n\n",
        "data: {\"type\":\"response.content_part.done\",\"output_index\":0,",
        "\"content_index\":0,\"part\":{\"type\":\"output_text\"}}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":",
        "{\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}}\n\n"
    ))])
    .with_header("content-type", "text/event-stream")
}

fn request() -> ModelRequest {
    ModelRequest::new(
        ModelRef::new("azure-openai", "deployment-test"),
        Message::user("hello"),
    )
}

#[tokio::test]
async fn api_key_preview_path_passes_canonical_conformance() {
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/openai/v1/responses?api-version=preview",
        response(),
    )])
    .unwrap();
    let config = OpenAiConfig::azure_api_key(&server.base_url(), "azure-secret")
        .unwrap()
        .with_azure_api_version(AzureOpenAiApiVersion::Preview);
    let client = OpenAiClient::new(config);

    let report = verify_success(
        &client,
        request(),
        ModelCallContext::new(),
        &SuccessContract::new("azure-openai")
            .visible_text("azure")
            .usage(ModelUsage {
                input_tokens: 2,
                output_tokens: 1,
                ..ModelUsage::default()
            })
            .provider_events(),
    )
    .await
    .unwrap();

    assert_eq!(report.checks().len(), 4);
    let observed = server.observed_requests();
    assert_eq!(observed[0].headers["api-key"], "[REDACTED]");
    assert!(!observed[0].headers.contains_key("authorization"));
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn entra_token_uses_bearer_auth_without_api_key_header() {
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/openai/v1/responses",
        response(),
    )])
    .unwrap();
    let client = OpenAiClient::new(
        OpenAiConfig::azure_bearer_token(&server.base_url(), "entra-token").unwrap(),
    );

    verify_success(
        &client,
        request(),
        ModelCallContext::new(),
        &SuccessContract::new("azure-openai").visible_text("azure"),
    )
    .await
    .unwrap();

    let observed = server.observed_requests();
    assert_eq!(observed[0].headers["authorization"], "[REDACTED]");
    assert!(!observed[0].headers.contains_key("api-key"));
    server.assert_finished().unwrap();
}
