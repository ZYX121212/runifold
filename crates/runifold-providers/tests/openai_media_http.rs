//! `OpenAI` media task HTTP contract tests.
#![cfg(feature = "openai")]

use std::time::Duration;

use runifold_core::{CancellationToken, Instant};
use runifold_model::{
    ImageFormat, ImageGenerationModel, ImageGenerationRequest, MediaSource, ModelCallContext,
    ModelRef, SpeechFormat, SpeechModel, SpeechRequest, TranscriptionModel, TranscriptionRequest,
};
use runifold_provider_testkit::{CassetteServer, HttpExchange, ResponseChunk, ScriptedResponse};
use runifold_providers::openai::{
    OpenAiClient, OpenAiConfig, OpenAiImageWireProfile, OpenAiMediaCapabilityCatalog,
    OpenAiSpeechWireProfile, OpenAiTranscriptionWireProfile, OpenAiWireProtocol,
};
use serde_json::json;

fn client(server: &CassetteServer) -> OpenAiClient {
    let config = OpenAiConfig::custom(
        "media-test",
        &format!("{}v1/", server.base_url()),
        OpenAiWireProtocol::Responses,
    )
    .unwrap();
    let mut media = OpenAiMediaCapabilityCatalog::new();
    media.insert_image_profile(
        ModelRef::new("media-test", "gpt-image-test"),
        OpenAiImageWireProfile::GptImage,
    );
    media.insert_image_profile(
        ModelRef::new("media-test", "dall-e-3"),
        OpenAiImageWireProfile::DallE3,
    );
    media.insert_speech_profile(
        ModelRef::new("media-test", "tts-test"),
        OpenAiSpeechWireProfile::Instructional,
    );
    media.insert_transcription_profile(
        ModelRef::new("media-test", "transcribe-test"),
        OpenAiTranscriptionWireProfile::Prompted,
    );
    OpenAiClient::new(config).with_media_capability_catalog(media)
}

#[tokio::test]
async fn image_speech_and_transcription_use_complete_typed_contracts() {
    let image_response = json!({
        "data": [{"b64_json": "aW1hZ2U=", "revised_prompt": "revised"}]
    });
    let server = CassetteServer::start(vec![
        HttpExchange::new(
            "POST",
            "/v1/images/generations",
            ScriptedResponse::json(200, &image_response).unwrap(),
        )
        .with_json_body(json!({
            "model": "gpt-image-test",
            "prompt": "draw a safe test card",
            "n": 1,
            "output_format": "png"
        })),
        HttpExchange::new(
            "POST",
            "/v1/audio/speech",
            ScriptedResponse::ok(vec![ResponseChunk {
                body: b"audio".to_vec(),
                delay: std::time::Duration::ZERO,
            }]),
        ),
        HttpExchange::new(
            "POST",
            "/v1/audio/transcriptions",
            ScriptedResponse::json(
                200,
                &json!({"text":"hello", "language":"en", "duration":1.25}),
            )
            .unwrap(),
        ),
    ])
    .unwrap();
    let client = client(&server);

    let image = client
        .generate_image(
            ImageGenerationRequest {
                model: ModelRef::new("media-test", "gpt-image-test"),
                prompt: "draw a safe test card".into(),
                count: 1,
                size: None,
                quality: None,
                format: ImageFormat::Png,
                transparent: false,
            },
            ModelCallContext::new(),
        )
        .await
        .unwrap();
    assert!(matches!(
        &image.images[0].source,
        MediaSource::Base64 { data, .. } if data == "aW1hZ2U="
    ));

    let speech = client
        .synthesize_speech(
            SpeechRequest {
                model: ModelRef::new("media-test", "tts-test"),
                input: "hello".into(),
                voice: "alloy".into(),
                instructions: None,
                format: SpeechFormat::Mp3,
                speed: None,
            },
            ModelCallContext::new(),
        )
        .await
        .unwrap();
    assert_eq!(speech.bytes, b"audio");
    assert_eq!(speech.media_type, "audio/mpeg");

    let transcription = client
        .transcribe(
            TranscriptionRequest {
                model: ModelRef::new("media-test", "transcribe-test"),
                file_name: "sample.wav".into(),
                media_type: "audio/wav".into(),
                bytes: b"wave".to_vec(),
                language: Some("en".into()),
                prompt: None,
            },
            ModelCallContext::new(),
        )
        .await
        .unwrap();
    assert_eq!(transcription.text, "hello");
    assert_eq!(transcription.language.as_deref(), Some("en"));

    server.assert_finished().unwrap();
    let observed = server.observed_requests();
    assert!(String::from_utf8_lossy(&observed[1].body).contains("tts-test"));
    let multipart = String::from_utf8_lossy(&observed[2].body);
    assert!(multipart.contains("filename=\"sample.wav\""));
    assert!(multipart.contains("transcribe-test"));
    assert!(multipart.contains("name=\"response_format\""));
    assert!(multipart.contains("json"));
    assert!(!multipart.contains("verbose_json"));
}

#[tokio::test]
async fn dall_e_uses_legacy_safe_parameters_and_accepts_url_outputs() {
    let server = CassetteServer::start(vec![
        HttpExchange::new(
            "POST",
            "/v1/images/generations",
            ScriptedResponse::json(
                200,
                &json!({"data": [{"url": "https://media.example/image.png"}]}),
            )
            .unwrap(),
        )
        .with_json_body(json!({
            "model": "dall-e-3",
            "prompt": "draw a compatible test card",
            "n": 1
        })),
    ])
    .unwrap();

    let image = client(&server)
        .generate_image(
            ImageGenerationRequest {
                model: ModelRef::new("media-test", "dall-e-3"),
                prompt: "draw a compatible test card".into(),
                count: 1,
                size: None,
                quality: None,
                format: ImageFormat::Png,
                transparent: false,
            },
            ModelCallContext::new(),
        )
        .await
        .unwrap();

    assert!(matches!(
        &image.images[0].source,
        MediaSource::Url { url, media_type }
            if url == "https://media.example/image.png"
                && media_type.as_deref() == Some("image/png")
    ));
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn media_validation_happens_before_transport() {
    let server = CassetteServer::start(Vec::new()).unwrap();
    let client = client(&server);
    let error = client
        .synthesize_speech(
            SpeechRequest {
                model: ModelRef::new("media-test", "tts-test"),
                input: String::new(),
                voice: "alloy".into(),
                instructions: None,
                format: SpeechFormat::Mp3,
                speed: None,
            },
            ModelCallContext::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, runifold_model::ModelErrorKind::InvalidRequest);
    let error = client
        .synthesize_speech(
            SpeechRequest {
                model: ModelRef::new("media-test", "undeclared-tts"),
                input: "hello".into(),
                voice: "alloy".into(),
                instructions: Some("whisper".into()),
                format: SpeechFormat::Mp3,
                speed: None,
            },
            ModelCallContext::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, runifold_model::ModelErrorKind::InvalidRequest);
    let error = client
        .transcribe(
            TranscriptionRequest {
                model: ModelRef::new("media-test", "undeclared-transcription"),
                file_name: "sample.wav".into(),
                media_type: "audio/wav".into(),
                bytes: b"wave".to_vec(),
                language: None,
                prompt: Some("Runifold".into()),
            },
            ModelCallContext::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, runifold_model::ModelErrorKind::InvalidRequest);
    let error = client
        .generate_image(
            ImageGenerationRequest {
                model: ModelRef::new("media-test", "undeclared-image-model"),
                prompt: "draw a test card".into(),
                count: 1,
                size: None,
                quality: None,
                format: ImageFormat::Webp,
                transparent: false,
            },
            ModelCallContext::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind, runifold_model::ModelErrorKind::InvalidRequest);
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn media_disconnect_never_returns_partial_speech() {
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1/audio/speech",
        ScriptedResponse::ok(vec![ResponseChunk::text("partial-audio")]).disconnect_after_chunks(),
    )])
    .unwrap();

    let error = client(&server)
        .synthesize_speech(speech_request(), ModelCallContext::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind, runifold_model::ModelErrorKind::Transport);
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn media_body_observes_cancellation_and_deadline() {
    let delayed = || {
        HttpExchange::new(
            "POST",
            "/v1/audio/speech",
            ScriptedResponse::ok(vec![
                ResponseChunk::text("audio").after(Duration::from_millis(200)),
            ]),
        )
    };
    let cancellation_server = CassetteServer::start(vec![delayed()]).unwrap();
    let cancellation = CancellationToken::new();
    let context = ModelCallContext::new().with_cancellation(&cancellation);
    let canceller = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancellation.cancel();
    });
    let error = client(&cancellation_server)
        .synthesize_speech(speech_request(), context)
        .await
        .unwrap_err();
    canceller.await.unwrap();
    assert_eq!(error.kind, runifold_model::ModelErrorKind::Cancelled);
    cancellation_server.assert_finished().unwrap();

    let deadline_server = CassetteServer::start(vec![delayed()]).unwrap();
    let context = ModelCallContext::new().with_deadline(Instant::now() + Duration::from_millis(20));
    let error = client(&deadline_server)
        .synthesize_speech(speech_request(), context)
        .await
        .unwrap_err();
    assert_eq!(error.kind, runifold_model::ModelErrorKind::DeadlineExceeded);
    deadline_server.assert_finished().unwrap();
}

#[tokio::test]
async fn declared_oversize_media_is_rejected_before_body_buffering() {
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1/audio/speech",
        ScriptedResponse::ok(Vec::new()).with_header("content-length", "33554433"),
    )])
    .unwrap();

    let error = client(&server)
        .synthesize_speech(speech_request(), ModelCallContext::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind, runifold_model::ModelErrorKind::Protocol);
    server.assert_finished().unwrap();
}

fn speech_request() -> SpeechRequest {
    SpeechRequest {
        model: ModelRef::new("media-test", "tts-test"),
        input: "hello".into(),
        voice: "alloy".into(),
        instructions: None,
        format: SpeechFormat::Mp3,
        speed: None,
    }
}
