//! Opt-in live Ark Responses verification for strict structured output,
//! hosted web search, mixed tools, and both delivery modes.
#![cfg(feature = "openai")]

use std::{collections::BTreeMap, env, fs, path::Path, time::Duration};

use runifold_model::{
    FeaturePolicy, Message, Model, ModelCallContext, ModelRef, ModelRequest, OutputFormat,
    ResponseMode, ToolSpec,
};
use runifold_providers::openai::{ArkWebSearchTool, OpenAiClient, OpenAiConfig};
use serde::Serialize;
use serde_json::json;

#[derive(Serialize)]
struct ArkLiveEvidence {
    schema_version: u32,
    suite: &'static str,
    result: &'static str,
    revision: Option<String>,
    provider: &'static str,
    model: String,
    requests: u32,
    passed_checks: [&'static str; 6],
    credential_material: &'static str,
    response_content: &'static str,
}

#[test]
fn evidence_schema_cannot_store_credentials_or_response_content() {
    let evidence = evidence("doubao-test".into());
    let encoded = serde_json::to_string(&evidence).expect("Ark evidence schema must serialize");

    assert!(!encoded.contains("api_key"));
    assert!(!encoded.contains("response_text"));
    assert!(encoded.contains("excluded"));
}

#[tokio::test]
#[ignore = "requires ARK_API_KEY, a compatible Ark model, and explicit live API authority"]
async fn strict_schema_web_search_and_delivery_modes_work_against_ark() {
    let api_key = env::var("ARK_API_KEY")
        .expect("ARK_API_KEY is required when the ignored Ark canary is selected");
    let model = env::var("RUNIFOLD_LIVE_ARK_MODEL")
        .expect("RUNIFOLD_LIVE_ARK_MODEL must select an Ark Responses model");
    let client = OpenAiClient::new(OpenAiConfig::ark(api_key).expect("Ark config must be valid"));

    let complete = client
        .invoke(
            request(&model, ResponseMode::Complete, true),
            deadline_context(),
        )
        .await
        .expect("Ark complete strict structured response must succeed");
    let complete_json: serde_json::Value = complete
        .structured()
        .expect("Ark complete response must satisfy the requested JSON schema");
    assert!(complete_json["answer"].is_string());

    let streamed = client
        .invoke(
            request(&model, ResponseMode::Streaming, false),
            deadline_context(),
        )
        .await
        .expect("Ark streamed strict structured response must succeed");
    let streamed_json: serde_json::Value = streamed
        .structured()
        .expect("Ark streamed response must satisfy the requested JSON schema");
    assert!(streamed_json["answer"].is_string());

    let path = env::var("RUNIFOLD_LIVE_EVIDENCE_PATH")
        .expect("RUNIFOLD_LIVE_EVIDENCE_PATH must be configured");
    write_evidence(Path::new(&path), &evidence(model));
}

fn request(model: &str, mode: ResponseMode, include_function: bool) -> ModelRequest {
    let mut request = ModelRequest::new(
        ModelRef::new("ark", model),
        Message::user(
            "Use web search when useful. Return a short JSON answer about Runifold's public repository.",
        ),
    )
    .feature_policy(FeaturePolicy::Strict)
    .response_mode(mode)
    .provider_tool(ArkWebSearchTool::new().limit(3).max_keyword(2).into())
    .output_format(OutputFormat::JsonSchema {
        name: "ark_canary_answer".into(),
        schema: json!({
            "type": "object",
            "properties": {"answer": {"type": "string"}},
            "required": ["answer"],
            "additionalProperties": false
        }),
        strict: true,
    });
    request.generation.max_output_tokens = Some(512);
    if include_function {
        request.tools.push(ToolSpec {
            name: "local_canary".into(),
            description: "A local function included only to validate mixed Tool encoding".into(),
            input_schema: json!({"type": "object", "additionalProperties": false}),
            output_schema: None,
            metadata: BTreeMap::new(),
        });
    }
    request
}

fn deadline_context() -> ModelCallContext {
    ModelCallContext::new().with_deadline(runifold_core::Instant::now() + Duration::from_secs(120))
}

fn evidence(model: String) -> ArkLiveEvidence {
    ArkLiveEvidence {
        schema_version: 1,
        suite: "runifold.ark-responses-live",
        result: "passed",
        revision: env::var("GITHUB_SHA").ok(),
        provider: "ark",
        model,
        requests: 2,
        passed_checks: [
            "strict-capability-preflight",
            "strict-json-schema",
            "native-web-search",
            "mixed-native-and-function-tools",
            "complete-response",
            "streamed-response",
        ],
        credential_material: "excluded",
        response_content: "excluded",
    }
}

fn write_evidence(path: &Path, evidence: &ArkLiveEvidence) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("Ark evidence directory must be creatable");
    }
    let encoded = serde_json::to_vec_pretty(evidence).expect("Ark evidence must serialize");
    fs::write(path, encoded).expect("Ark evidence must be writable");
}
