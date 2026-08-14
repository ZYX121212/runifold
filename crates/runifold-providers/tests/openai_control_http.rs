//! OpenAI-compatible model, file, and Batch control-plane HTTP tests.
#![cfg(feature = "openai")]

use std::time::{Duration, Instant};

use runifold_model::ModelCallContext;
use runifold_provider_testkit::{CassetteServer, HttpExchange, ResponseChunk, ScriptedResponse};
use runifold_providers::openai::{
    OpenAiBatchEndpoint, OpenAiBatchRequest, OpenAiBatchStatus, OpenAiClient, OpenAiConfig,
    OpenAiControlError, OpenAiFilePurpose, OpenAiFileStatus, OpenAiFileUpload,
    OpenAiFileWaitPolicy, OpenAiWireProtocol,
};
#[cfg(feature = "openai-realtime")]
use runifold_providers::openai::{
    OpenAiRealtimeCallRequest, OpenAiRealtimeClientSecretRequest, OpenAiRealtimeModality,
    OpenAiRealtimeSdpOffer,
};
#[cfg(feature = "openai-realtime")]
use secrecy::ExposeSecret;
use serde_json::json;

fn client(server: &CassetteServer) -> OpenAiClient {
    let config = OpenAiConfig::custom(
        "control-test",
        &format!("{}v1/", server.base_url()),
        OpenAiWireProtocol::Responses,
    )
    .unwrap();
    OpenAiClient::new(config)
}

fn batch(status: &str) -> serde_json::Value {
    json!({
        "id": "batch_test",
        "input_file_id": "file_test",
        "endpoint": "/v1/responses",
        "status": status,
        "output_file_id": null,
        "error_file_id": null,
        "metadata": {"tenant": "test"}
    })
}

fn file(status: &str) -> serde_json::Value {
    json!({
        "id":"file_test", "filename":"batch.jsonl", "purpose":"batch",
        "bytes":12, "created_at":7, "status":status
    })
}

#[tokio::test]
async fn file_lifecycle_is_complete_and_credential_free() {
    let server = CassetteServer::start(vec![
        HttpExchange::new(
            "POST",
            "/v1/files",
            ScriptedResponse::json(200, &file("processing")).unwrap(),
        ),
        HttpExchange::new(
            "GET",
            "/v1/files/file_test",
            ScriptedResponse::json(200, &file("processing")).unwrap(),
        ),
        HttpExchange::new(
            "GET",
            "/v1/files",
            ScriptedResponse::json(200, &json!({"data":[file("processing")]})).unwrap(),
        ),
        HttpExchange::new(
            "GET",
            "/v1/files/file_test",
            ScriptedResponse::json(200, &file("active")).unwrap(),
        ),
        HttpExchange::new(
            "DELETE",
            "/v1/files/file_test",
            ScriptedResponse::json(200, &json!({"id":"file_test", "deleted":true})).unwrap(),
        ),
    ])
    .unwrap();
    let control = client(&server).control_plane();

    let upload = OpenAiFileUpload::new(
        "batch.jsonl",
        OpenAiFilePurpose::batch(),
        b"{\"test\":1}\n".to_vec(),
    )
    .unwrap();
    let file = control
        .upload_file(upload, ModelCallContext::new())
        .await
        .unwrap();
    assert_eq!(file.id, "file_test");
    assert_eq!(file.lifecycle_status(), OpenAiFileStatus::Processing);
    let inspected = control
        .get_file("file_test", ModelCallContext::new())
        .await
        .unwrap();
    assert_eq!(inspected.lifecycle_status(), OpenAiFileStatus::Processing);
    let files = control.list_files(ModelCallContext::new()).await.unwrap();
    assert_eq!(files.len(), 1);
    let active = control
        .wait_file_active(
            "file_test",
            OpenAiFileWaitPolicy::new(Duration::from_millis(1), Duration::from_secs(1)).unwrap(),
            ModelCallContext::new(),
        )
        .await
        .unwrap();
    assert_eq!(active.lifecycle_status(), OpenAiFileStatus::Active);
    let deletion = control
        .delete_file("file_test", ModelCallContext::new())
        .await
        .unwrap();
    assert!(deletion.deleted);

    server.assert_finished().unwrap();
    let observed = server.observed_requests();
    assert!(
        observed
            .iter()
            .all(|request| !request.headers.contains_key("authorization"))
    );
    let upload_request = &observed[0];
    let multipart = String::from_utf8_lossy(&upload_request.body);
    assert!(multipart.contains("name=\"purpose\""));
    assert!(multipart.contains("batch"));
    assert!(multipart.contains("filename=\"batch.jsonl\""));
    assert!(multipart.contains("{\"test\":1}"));
}

#[tokio::test]
async fn model_and_batch_lifecycle_is_typed() {
    let server = CassetteServer::start(vec![
        HttpExchange::new(
            "GET",
            "/v1/models",
            ScriptedResponse::json(
                200,
                &json!({"data":[{"id":"gpt-test","created":7,"owned_by":"test"}]}),
            )
            .unwrap(),
        ),
        HttpExchange::new(
            "POST",
            "/v1/batches",
            ScriptedResponse::json(200, &batch("validating")).unwrap(),
        )
        .with_json_body(json!({
            "input_file_id":"file_test", "endpoint":"/v1/responses",
            "completion_window":"24h", "metadata":{"tenant":"test"}
        })),
        HttpExchange::new(
            "GET",
            "/v1/batches/batch_test",
            ScriptedResponse::json(200, &batch("completed")).unwrap(),
        ),
        HttpExchange::new(
            "POST",
            "/v1/batches/batch_test/cancel",
            ScriptedResponse::json(200, &batch("cancelling")).unwrap(),
        ),
    ])
    .unwrap();
    let control = client(&server).control_plane();

    let models = control.list_models(ModelCallContext::new()).await.unwrap();
    assert_eq!(models[0].id, "gpt-test");
    let request = OpenAiBatchRequest::new("file_test", OpenAiBatchEndpoint::Responses)
        .unwrap()
        .with_metadata("tenant", "test")
        .unwrap();
    let created = control
        .create_batch(request, ModelCallContext::new())
        .await
        .unwrap();
    assert_eq!(created.status, OpenAiBatchStatus::Validating);
    let completed = control
        .get_batch("batch_test", ModelCallContext::new())
        .await
        .unwrap();
    assert_eq!(completed.status, OpenAiBatchStatus::Completed);
    let cancelled = control
        .cancel_batch("batch_test", ModelCallContext::new())
        .await
        .unwrap();
    assert_eq!(cancelled.status, OpenAiBatchStatus::Cancelling);
    server.assert_finished().unwrap();
}

#[tokio::test]
#[cfg(feature = "openai-realtime")]
async fn realtime_client_secret_is_typed_and_redacted() {
    let server = CassetteServer::start(vec![
        HttpExchange::new(
            "POST",
            "/v1/realtime/client_secrets",
            ScriptedResponse::json(
                200,
                &json!({
                    "value":"ek_test_secret",
                    "expires_at":1_800_000_000_u64,
                    "session":{
                        "type":"realtime",
                        "id":"sess_secret",
                        "model":"gpt-realtime"
                    }
                }),
            )
            .unwrap(),
        )
        .with_json_body(json!({
            "expires_after":{"anchor":"created_at","seconds":300},
            "session":{
                "type":"realtime",
                "model":"gpt-realtime",
                "instructions":"Be concise",
                "output_modalities":["audio"]
            }
        })),
    ])
    .unwrap();
    let request = OpenAiRealtimeClientSecretRequest::new("gpt-realtime")
        .unwrap()
        .with_instructions("Be concise")
        .unwrap()
        .with_modality(OpenAiRealtimeModality::Audio)
        .with_expiration_seconds(300)
        .unwrap()
        .with_safety_identifier("hashed-user")
        .unwrap();
    let secret = client(&server)
        .control_plane()
        .create_realtime_client_secret(request, ModelCallContext::new())
        .await
        .unwrap();

    assert_eq!(secret.secret().expose_secret(), "ek_test_secret");
    assert_eq!(secret.session["id"], "sess_secret");
    assert!(!format!("{secret:?}").contains("ek_test_secret"));
    assert!(!format!("{secret:?}").contains("sess_secret"));
    assert_eq!(
        server.observed_requests()[0].headers["openai-safety-identifier"],
        "hashed-user"
    );
    server.assert_finished().unwrap();
}

#[tokio::test]
#[cfg(feature = "openai-realtime")]
async fn unified_realtime_call_sends_multipart_sdp_session_and_safety_identity() {
    let answer = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n";
    let response = ScriptedResponse::ok(vec![ResponseChunk::text(answer)])
        .with_header("content-type", "application/sdp")
        .with_header("location", "/v1/realtime/calls/call_test");
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1/realtime/calls",
        response,
    )])
    .unwrap();
    let offer = OpenAiRealtimeSdpOffer::new("v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n").unwrap();
    let request = OpenAiRealtimeCallRequest::new(offer, "gpt-realtime")
        .unwrap()
        .with_instructions("Be concise")
        .unwrap()
        .with_modality(OpenAiRealtimeModality::Audio)
        .with_safety_identifier("hashed-user")
        .unwrap();
    let call = client(&server)
        .control_plane()
        .create_realtime_call(request, ModelCallContext::new())
        .await
        .unwrap();

    assert_eq!(call.answer_sdp(), answer);
    assert_eq!(
        call.location.as_deref(),
        Some("/v1/realtime/calls/call_test")
    );
    let observed = server.observed_requests();
    assert_eq!(
        observed[0].headers["openai-safety-identifier"],
        "hashed-user"
    );
    let multipart = String::from_utf8_lossy(&observed[0].body);
    assert!(multipart.contains("name=\"sdp\""));
    assert!(multipart.contains("application/sdp"));
    assert!(multipart.contains("name=\"session\""));
    assert!(multipart.contains("\"model\":\"gpt-realtime\""));
    assert!(multipart.contains("\"output_modalities\":[\"audio\"]"));
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn control_plane_preserves_deadline_and_provider_diagnostics() {
    let delayed = ScriptedResponse::ok(vec![
        ResponseChunk::text(r#"{"data":[]}"#).after(Duration::from_millis(100)),
    ])
    .with_header("content-type", "application/json");
    let rejected = ScriptedResponse::json(
        429,
        &json!({"error":{"message":"control plane rate limited"}}),
    )
    .unwrap()
    .with_header("x-request-id", "req_control");
    let server = CassetteServer::start(vec![
        HttpExchange::new("GET", "/v1/models", delayed),
        HttpExchange::new("GET", "/v1/models", rejected),
    ])
    .unwrap();
    let control = client(&server).control_plane();

    let deadline = control
        .list_models(
            ModelCallContext::new().with_deadline(Instant::now() + Duration::from_millis(20)),
        )
        .await
        .unwrap_err();
    assert!(matches!(deadline, OpenAiControlError::DeadlineExceeded));

    let rejected = control
        .list_models(ModelCallContext::new())
        .await
        .unwrap_err();
    assert!(
        matches!(
            &rejected,
            OpenAiControlError::Provider {
                status: 429,
                request_id,
                ..
            } if request_id.as_deref() == Some("req_control")
        ),
        "unexpected Provider rejection: {rejected:?}"
    );
    server.assert_finished().unwrap();
}
