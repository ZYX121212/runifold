//! Opt-in live verification for `OpenAI` Realtime client-secret rotation.
#![cfg(feature = "openai")]

use std::{
    env, fs,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use runifold_model::ModelCallContext;
use runifold_providers::openai::{OpenAiClient, OpenAiRealtimeClientSecretRequest};
use secrecy::ExposeSecret;
use serde::Serialize;

const DEFAULT_MODEL: &str = "gpt-realtime-2.1";
const DEFAULT_EXPIRATION_SECONDS: u32 = 60;
const SAFETY_IDENTIFIER: &str = "runifold-live-realtime-canary";

#[derive(Debug, Serialize)]
struct LiveRealtimeEvidence {
    schema_version: u32,
    suite: &'static str,
    result: &'static str,
    revision: Option<String>,
    provider: &'static str,
    model: String,
    requests: u32,
    expiration_seconds: u32,
    passed_checks: [&'static str; 4],
    safety_identifier: &'static str,
    secret_material: &'static str,
}

#[test]
fn evidence_schema_has_no_field_for_credential_values() {
    let evidence = LiveRealtimeEvidence {
        schema_version: 1,
        suite: "runifold.openai-realtime-client-secret-live",
        result: "passed",
        revision: Some("0123456789abcdef".into()),
        provider: "openai",
        model: DEFAULT_MODEL.into(),
        requests: 2,
        expiration_seconds: DEFAULT_EXPIRATION_SECONDS,
        passed_checks: [
            "documented-prefix",
            "distinct-secret-values",
            "distinct-session-identities",
            "bounded-expiration",
        ],
        safety_identifier: "supplied",
        secret_material: "excluded",
    };
    let encoded = serde_json::to_string(&evidence).expect("live evidence schema must serialize");

    assert!(!encoded.contains("ek_"));
    assert!(!encoded.contains("sk-"));
    assert!(!encoded.contains("session_id"));
}

#[tokio::test]
#[ignore = "requires OPENAI_API_KEY and explicit live OpenAI API authority"]
async fn client_secrets_are_short_lived_distinct_and_never_persisted() {
    let api_key = env::var("OPENAI_API_KEY")
        .expect("OPENAI_API_KEY is required when the ignored live canary is selected");
    let model =
        env::var("RUNIFOLD_LIVE_OPENAI_REALTIME_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.into());
    let expiration_seconds = env::var("RUNIFOLD_LIVE_OPENAI_REALTIME_TTL_SECONDS").map_or(
        DEFAULT_EXPIRATION_SECONDS,
        |value| {
            value
                .parse::<u32>()
                .expect("RUNIFOLD_LIVE_OPENAI_REALTIME_TTL_SECONDS must be an integer")
        },
    );
    let request = OpenAiRealtimeClientSecretRequest::new(model.clone())
        .expect("the configured live Realtime model must be valid")
        .with_expiration_seconds(expiration_seconds)
        .expect("the configured live Realtime TTL must be between 10 and 7200 seconds")
        .with_safety_identifier(SAFETY_IDENTIFIER)
        .expect("the fixed non-user canary safety identifier is valid");
    let control = OpenAiClient::from_api_key(api_key)
        .expect("OPENAI_API_KEY must construct the official OpenAI client")
        .control_plane();
    let before = unix_time();

    let first = control
        .create_realtime_client_secret(
            request.clone(),
            ModelCallContext::new()
                .with_deadline(runifold_core::Instant::now() + Duration::from_secs(30)),
        )
        .await
        .expect("the first live Realtime client-secret request must succeed");
    let second = control
        .create_realtime_client_secret(
            request,
            ModelCallContext::new()
                .with_deadline(runifold_core::Instant::now() + Duration::from_secs(30)),
        )
        .await
        .expect("the second live Realtime client-secret request must succeed");
    let after = unix_time();

    let first_value = first.secret().expose_secret();
    let second_value = second.secret().expose_secret();
    let prefix_valid = first_value.starts_with("ek_") && second_value.starts_with("ek_");
    assert!(
        prefix_valid,
        "OpenAI Realtime client secrets must use the documented redacted prefix"
    );
    assert!(
        first_value != second_value,
        "two live client-secret requests must never reuse credential material"
    );
    validate_expiration(first.expires_at, before, after, expiration_seconds);
    validate_expiration(second.expires_at, before, after, expiration_seconds);

    let first_session_id = session_string(&first.session, "id");
    let second_session_id = session_string(&second.session, "id");
    assert!(
        first_session_id != second_session_id,
        "each live client secret must own a distinct effective session"
    );
    assert_eq!(
        session_string(&first.session, "model"),
        model,
        "the first effective session must retain the requested model"
    );
    assert_eq!(
        session_string(&second.session, "model"),
        model,
        "the second effective session must retain the requested model"
    );

    if let Some(path) = env::var_os("RUNIFOLD_LIVE_EVIDENCE_PATH") {
        write_evidence(
            Path::new(&path),
            &LiveRealtimeEvidence {
                schema_version: 1,
                suite: "runifold.openai-realtime-client-secret-live",
                result: "passed",
                revision: env::var("GITHUB_SHA").ok(),
                provider: "openai",
                model,
                requests: 2,
                expiration_seconds,
                passed_checks: [
                    "documented-prefix",
                    "distinct-secret-values",
                    "distinct-session-identities",
                    "bounded-expiration",
                ],
                safety_identifier: "supplied",
                secret_material: "excluded",
            },
        );
    }
}

fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the live canary runner clock must be after the Unix epoch")
        .as_secs()
}

fn validate_expiration(expires_at: u64, before: u64, after: u64, requested: u32) {
    assert!(
        expires_at >= before.saturating_add(10),
        "live client secret must preserve the documented minimum lifetime"
    );
    assert!(
        expires_at
            <= after
                .saturating_add(u64::from(requested))
                .saturating_add(30),
        "live client secret expiration must remain close to the requested TTL"
    );
}

fn session_string<'a>(session: &'a serde_json::Value, field: &str) -> &'a str {
    session
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| panic!("live effective session must contain a non-empty {field}"))
}

fn write_evidence(path: &Path, evidence: &LiveRealtimeEvidence) {
    let parent = path
        .parent()
        .expect("live evidence path must have a parent directory");
    fs::create_dir_all(parent).expect("live evidence directory must be creatable");
    let body = serde_json::to_vec_pretty(&evidence).expect("live evidence must serialize as JSON");
    fs::write(path, body).expect("live evidence must be writable");
}
