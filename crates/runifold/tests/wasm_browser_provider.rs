#![allow(missing_docs)]
#![cfg(target_arch = "wasm32")]

//! Headless-browser verification for first-party Fetch transports.

use std::{cell::RefCell, rc::Rc, time::Duration};

use futures_timer::Delay;
use runifold::Message;
use runifold::{
    CancellationToken, Model, ModelCallContext, ModelRef, ModelRequest, ProviderModelExt,
    anthropic::{AnthropicClient, AnthropicConfig},
    core::{Instant, RetrySafety},
    gemini::{GeminiClient, GeminiConfig},
    model::{ModelErrorKind, RetryJitter},
    ollama::{OllamaClient, OllamaConfig},
    openai::{
        OpenAiBatchEndpoint, OpenAiBatchRequest, OpenAiBatchStatus, OpenAiClient, OpenAiConfig,
        OpenAiFilePurpose, OpenAiFileUpload, OpenAiRealtimeAudioChunk, OpenAiRealtimeAudioFormat,
        OpenAiRealtimeClientSecretRequest, OpenAiRealtimeCommand, OpenAiRealtimeError,
        OpenAiRealtimeEvent, OpenAiRealtimeIceServer, OpenAiRealtimeIceTransportPolicy,
        OpenAiRealtimeModality, OpenAiRealtimeReconnectController, OpenAiRealtimeReconnectEvent,
        OpenAiRealtimeReconnectFailureKind, OpenAiRealtimeReconnectPolicy,
        OpenAiRealtimeSessionUpdate, OpenAiRealtimeState, OpenAiRealtimeWebRtcConnectionState,
        OpenAiRealtimeWebRtcIceState, OpenAiRealtimeWebRtcOptions, OpenAiRealtimeWebRtcSession,
        OpenAiWireProtocol, RealtimeReconnectDisposition,
    },
    retrieval::{EmbeddingModel, EmbeddingRequest, EmbeddingTask, RetrievalContext},
};
use wasm_bindgen::{JsCast, closure::Closure};
use wasm_bindgen_futures::{JsFuture, spawn_local};
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};
use web_sys::{
    MessageEvent, RtcConfiguration, RtcDataChannel, RtcDataChannelEvent, RtcIceGatheringState,
    RtcIceServer, RtcIceTransportPolicy, RtcPeerConnection, RtcSdpType, RtcSessionDescriptionInit,
};

wasm_bindgen_test_configure!(run_in_browser);

const ENDPOINT: &str = "http://127.0.0.1:38087/";
const BROWSER_DEADLINE_TEST_WINDOW: Duration = Duration::from_millis(150);
const TURN_URL: Option<&str> = option_env!("RUNIFOLD_BROWSER_TURN_URL");
const TURN_USERNAME: Option<&str> = option_env!("RUNIFOLD_BROWSER_TURN_USERNAME");
const TURN_CREDENTIAL: Option<&str> = option_env!("RUNIFOLD_BROWSER_TURN_CREDENTIAL");
const TURN_FAULT_ENDPOINT: Option<&str> = option_env!("RUNIFOLD_BROWSER_TURN_FAULT_ENDPOINT");
const TURN_RECOVER_ENDPOINT: Option<&str> = option_env!("RUNIFOLD_BROWSER_TURN_RECOVER_ENDPOINT");

#[wasm_bindgen_test]
async fn browser_realtime_reconnect_rotates_factory_and_emits_lifecycle() {
    let policy = OpenAiRealtimeReconnectPolicy::exponential(2, Duration::ZERO, Duration::ZERO, 1)
        .expect("the zero-delay browser policy is valid")
        .jitter(RetryJitter::None);
    let mut controller = OpenAiRealtimeReconnectController::new(policy);
    let attempts = Rc::new(RefCell::new(Vec::new()));
    let observed_attempts = attempts.clone();
    let events = Rc::new(RefCell::new(Vec::new()));
    let observed_events = events.clone();

    let connected_on = controller
        .reconnect(
            RealtimeReconnectDisposition::SafeWhenIdle,
            ModelCallContext::new(),
            move |attempt, _| {
                observed_attempts
                    .borrow_mut()
                    .push((attempt.ordinal(), attempt.requires_fresh_credential()));
                std::future::ready(if attempt.ordinal() == 1 {
                    Err(OpenAiRealtimeError::Transport)
                } else {
                    Ok(attempt.ordinal())
                })
            },
            move |event| observed_events.borrow_mut().push(event),
        )
        .await
        .expect("the second browser replacement attempt must connect");

    assert_eq!(connected_on, 2);
    assert_eq!(attempts.borrow().as_slice(), [(1, true), (2, true)]);
    assert_eq!(
        events.borrow().last(),
        Some(&OpenAiRealtimeReconnectEvent::Connected { attempt: 2 })
    );
}

#[wasm_bindgen_test]
async fn browser_realtime_gateway_rebuild_retries_only_transient_status() {
    let policy = OpenAiRealtimeReconnectPolicy::exponential(2, Duration::ZERO, Duration::ZERO, 1)
        .expect("the zero-delay Gateway policy is valid")
        .jitter(RetryJitter::None);
    let mut controller = OpenAiRealtimeReconnectController::new(policy);
    let events = Rc::new(RefCell::new(Vec::new()));
    let observed_events = events.clone();
    let realtime = client("v1/")
        .realtime("gpt-realtime-browser")
        .expect("the fixed browser Realtime model is valid");

    let error = realtime
        .reconnect_webrtc_with_gateway(
            &mut controller,
            RealtimeReconnectDisposition::SafeBeforeSession,
            OpenAiRealtimeWebRtcOptions::new()
                .with_microphone(false)
                .with_playback(false),
            format!("{ENDPOINT}realtime-gateway-retry"),
            ModelCallContext::new().with_deadline(Instant::now() + Duration::from_secs(5)),
            move |event| observed_events.borrow_mut().push(event),
        )
        .await
        .expect_err("the cassette's permanent second status must stop replacement");

    assert!(matches!(
        error,
        runifold::openai::OpenAiRealtimeReconnectError::Permanent {
            source,
        } if matches!(
            *source,
            OpenAiRealtimeError::SdpExchange {
                status: 400,
                retryable: false
            }
        )
    ));
    assert!(
        events
            .borrow()
            .contains(&OpenAiRealtimeReconnectEvent::AttemptFailed {
                attempt: 1,
                kind: OpenAiRealtimeReconnectFailureKind::SdpExchange,
                retryable: true,
            })
    );
}

fn client(path: &str) -> OpenAiClient {
    let base_url = format!("{ENDPOINT}{path}");
    let config = OpenAiConfig::custom("browser-test", &base_url, OpenAiWireProtocol::Responses)
        .expect("the fixed browser cassette endpoint is valid");
    OpenAiClient::new(config)
}

fn request() -> ModelRequest {
    ModelRequest::new(
        ModelRef::new("browser-test", "gpt-browser"),
        Message::user("stream from a browser"),
    )
}

fn embedding_request() -> EmbeddingRequest {
    EmbeddingRequest::new(
        vec!["rust".into(), "agent".into()],
        EmbeddingTask::RetrievalDocument,
    )
    .expect("the fixed browser embedding inputs are valid")
}

#[wasm_bindgen_test]
async fn agent_streams_through_browser_fetch_without_a_bundled_secret() {
    let answer = client("v1/")
        .agent("browser-agent", "gpt-browser")
        .system("Return the streamed cassette.")
        .prompt_text("stream from a browser")
        .await
        .expect("the browser Agent invocation must succeed");

    assert_eq!(answer, "browser-stream");
}

#[wasm_bindgen_test]
async fn browser_fetch_is_cancelled_while_the_response_body_is_pending() {
    let cancellation = CancellationToken::new();
    let canceller = cancellation.clone();
    spawn_local(async move {
        Delay::new(Duration::from_millis(40)).await;
        canceller.cancel();
    });

    let error = client("slow/")
        .invoke(
            request(),
            ModelCallContext::new().with_cancellation(&cancellation),
        )
        .await
        .expect_err("in-flight browser cancellation must fail the invocation");

    assert_eq!(error.kind, ModelErrorKind::Cancelled);
}

#[wasm_bindgen_test]
async fn browser_fetch_deadlines_and_rate_limits_keep_typed_classification() {
    let deadline_error = client("slow/")
        .invoke(
            request(),
            ModelCallContext::new().with_deadline(Instant::now() + BROWSER_DEADLINE_TEST_WINDOW),
        )
        .await
        .expect_err("the browser Fetch deadline must abort a slow body");
    assert_eq!(deadline_error.kind, ModelErrorKind::DeadlineExceeded);

    let rate_error = client("rate/")
        .invoke(request(), ModelCallContext::new())
        .await
        .expect_err("the cassette must return a rate-limit error");
    assert_eq!(rate_error.kind, ModelErrorKind::Provider);
    assert_eq!(rate_error.retry_safety, RetrySafety::Safe);
    assert_eq!(rate_error.metadata["http.status"], 429);
    assert_eq!(rate_error.metadata["retry.after_ms"], 1000);
}

#[wasm_bindgen_test]
async fn native_provider_agents_stream_without_upstream_credentials() {
    let anthropic = AnthropicClient::new(
        AnthropicConfig::gateway(&format!("{ENDPOINT}anthropic/v1/"))
            .expect("the fixed Anthropic gateway endpoint is valid"),
    );
    let gemini = GeminiClient::new(
        GeminiConfig::gateway(&format!("{ENDPOINT}gemini/v1beta/"))
            .expect("the fixed Gemini gateway endpoint is valid"),
    );
    let ollama = OllamaClient::new(
        OllamaConfig::new(&format!("{ENDPOINT}ollama/"))
            .expect("the fixed Ollama gateway endpoint is valid"),
    );

    let anthropic_answer = anthropic
        .agent("anthropic-browser", "claude-browser")
        .prompt_text("stream from a browser")
        .await
        .expect("Anthropic must stream through the credential-free gateway");
    let gemini_answer = gemini
        .agent("gemini-browser", "gemini-browser")
        .prompt_text("stream from a browser")
        .await
        .expect("Gemini must stream through the credential-free gateway");
    let ollama_answer = ollama
        .agent("ollama-browser", "qwen-browser")
        .prompt_text("stream from a browser")
        .await
        .expect("Ollama must stream through the credential-free gateway");

    assert_eq!(anthropic_answer, "anthropic-browser");
    assert_eq!(gemini_answer, "gemini-browser");
    assert_eq!(ollama_answer, "ollama-browser");
}

#[wasm_bindgen_test]
async fn native_provider_deadlines_keep_the_same_typed_classification() {
    let anthropic = AnthropicClient::new(
        AnthropicConfig::gateway(&format!("{ENDPOINT}anthropic-slow/v1/"))
            .expect("the fixed Anthropic slow endpoint is valid"),
    );
    let gemini = GeminiClient::new(
        GeminiConfig::gateway(&format!("{ENDPOINT}gemini-slow/v1beta/"))
            .expect("the fixed Gemini slow endpoint is valid"),
    );
    let ollama = OllamaClient::new(
        OllamaConfig::new(&format!("{ENDPOINT}ollama-slow/"))
            .expect("the fixed Ollama slow endpoint is valid"),
    );

    let cases: Vec<(&str, ModelRef, &dyn Model)> = vec![
        (
            "Anthropic",
            ModelRef::new("anthropic", "claude-browser"),
            &anthropic,
        ),
        ("Gemini", ModelRef::new("gemini", "gemini-browser"), &gemini),
        ("Ollama", ModelRef::new("ollama", "qwen-browser"), &ollama),
    ];

    for (provider, model, client) in cases {
        let error = client
            .invoke(
                ModelRequest::new(model, Message::user("timeout")),
                ModelCallContext::new()
                    .with_deadline(Instant::now() + BROWSER_DEADLINE_TEST_WINDOW),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.kind,
            ModelErrorKind::DeadlineExceeded,
            "{provider} must preserve the canonical deadline classification",
        );
    }
}

#[wasm_bindgen_test]
async fn browser_embeddings_are_ordered_portable_and_credential_free() {
    let openai = client("v1/")
        .embedding_model("embedding-browser")
        .expect("the fixed OpenAI embedding model is valid");
    let gemini = GeminiClient::new(
        GeminiConfig::gateway(&format!("{ENDPOINT}gemini/v1beta/"))
            .expect("the fixed Gemini gateway endpoint is valid"),
    )
    .embedding_model("gemini-embedding")
    .expect("the fixed Gemini embedding model is valid");
    let ollama = OllamaClient::new(
        OllamaConfig::new(&format!("{ENDPOINT}ollama/"))
            .expect("the fixed Ollama gateway endpoint is valid"),
    )
    .embedding_model("ollama-embedding")
    .expect("the fixed Ollama embedding model is valid");

    let openai_batch = openai
        .embed(embedding_request(), RetrievalContext::new())
        .await
        .expect("OpenAI-compatible embeddings must work in the browser");
    let gemini_batch = gemini
        .embed(embedding_request(), RetrievalContext::new())
        .await
        .expect("Gemini embeddings must work in the browser");
    let ollama_batch = ollama
        .embed(embedding_request(), RetrievalContext::new())
        .await
        .expect("Ollama embeddings must work in the browser");

    for batch in [&openai_batch, &gemini_batch, &ollama_batch] {
        assert_eq!(batch.embeddings[0].values(), &[1.0, 0.0]);
        assert_eq!(batch.embeddings[1].values(), &[0.0, 1.0]);
        assert_eq!(batch.usage.tokens, 7);
    }
}

#[wasm_bindgen_test]
async fn browser_control_plane_completes_model_file_and_batch_lifecycle() {
    let control = client("v1/").control_plane();
    let models = control
        .list_models(ModelCallContext::new())
        .await
        .expect("browser model discovery must succeed");
    assert_eq!(models[0].id, "gpt-browser");

    let secret = control
        .create_realtime_client_secret(
            OpenAiRealtimeClientSecretRequest::new("gpt-realtime")
                .expect("the fixed Realtime model is valid")
                .with_expiration_seconds(300)
                .expect("the fixed secret lifetime is valid"),
            ModelCallContext::new(),
        )
        .await
        .expect("browser gateway client-secret creation must succeed");
    assert_eq!(secret.expires_at, 1_800_000_000);
    assert_eq!(secret.session["id"], "sess_browser_secret");
    assert!(!format!("{secret:?}").contains("ek_browser_secret"));

    let upload = OpenAiFileUpload::new(
        "browser.jsonl",
        OpenAiFilePurpose::batch(),
        b"{\"custom_id\":\"browser\"}\n".to_vec(),
    )
    .expect("the fixed browser upload is valid");
    let file = control
        .upload_file(upload, ModelCallContext::new())
        .await
        .expect("browser multipart upload must succeed");
    assert_eq!(file.id, "file_browser");

    let request = OpenAiBatchRequest::new(file.id, OpenAiBatchEndpoint::Responses)
        .expect("the fixed browser Batch request is valid")
        .with_metadata("runtime", "browser")
        .expect("the fixed browser Batch metadata is valid");
    let created = control
        .create_batch(request, ModelCallContext::new())
        .await
        .expect("browser Batch creation must succeed");
    assert_eq!(created.status, OpenAiBatchStatus::Validating);

    let completed = control
        .get_batch("batch_browser", ModelCallContext::new())
        .await
        .expect("browser Batch inspection must succeed");
    assert_eq!(completed.status, OpenAiBatchStatus::Completed);

    let cancelling = control
        .cancel_batch("batch_browser", ModelCallContext::new())
        .await
        .expect("browser Batch cancellation must succeed");
    assert_eq!(cancelling.status, OpenAiBatchStatus::Cancelling);
}

#[wasm_bindgen_test]
async fn browser_realtime_websocket_is_bounded_typed_and_credential_free() {
    let mut connection = client("v1/")
        .realtime("gpt-realtime-browser")
        .expect("the fixed browser Realtime model is valid")
        .connect(ModelCallContext::new())
        .await
        .expect("the credential-free browser WebSocket must connect");

    assert!(matches!(
        connection
            .next_event(ModelCallContext::new())
            .await
            .expect("the server must create a session"),
        OpenAiRealtimeEvent::SessionCreated { .. }
    ));
    assert!(matches!(
        connection.state(),
        OpenAiRealtimeState::Ready { .. }
    ));

    let update = OpenAiRealtimeSessionUpdate::new()
        .with_instructions("Answer with the cassette text")
        .expect("the browser Realtime instructions are valid")
        .with_modality(OpenAiRealtimeModality::Text);
    connection
        .send(
            &OpenAiRealtimeCommand::update_session(update),
            ModelCallContext::new(),
        )
        .await
        .expect("the session update must be sent");
    connection
        .send(
            &OpenAiRealtimeCommand::user_text("hello").expect("the browser user text is valid"),
            ModelCallContext::new(),
        )
        .await
        .expect("the user item must be sent");
    connection
        .send(
            &OpenAiRealtimeCommand::create_response(),
            ModelCallContext::new(),
        )
        .await
        .expect("response generation must start");

    assert!(matches!(
        connection
            .next_event(ModelCallContext::new())
            .await
            .expect("the response must start"),
        OpenAiRealtimeEvent::ResponseCreated { .. }
    ));
    assert!(matches!(
        connection
            .next_event(ModelCallContext::new())
            .await
            .expect("the response must stream text"),
        OpenAiRealtimeEvent::OutputTextDelta { delta, .. }
            if delta == "realtime-browser"
    ));
    assert!(matches!(
        connection
            .next_event(ModelCallContext::new())
            .await
            .expect("the response must complete"),
        OpenAiRealtimeEvent::ResponseDone { status, .. }
            if status == "completed"
    ));
    assert!(matches!(
        connection.state(),
        OpenAiRealtimeState::Ready { .. }
    ));
    connection
        .close(ModelCallContext::new())
        .await
        .expect("the browser Realtime connection must close cleanly");
}

#[wasm_bindgen_test]
async fn browser_realtime_audio_data_plane_is_typed_and_bounded() {
    let mut connection = client("v1/")
        .realtime("gpt-realtime-audio-browser")
        .expect("the fixed browser audio model is valid")
        .connect(ModelCallContext::new())
        .await
        .expect("the browser audio WebSocket must connect");
    connection
        .next_event(ModelCallContext::new())
        .await
        .expect("the server must create an audio session");

    let session = OpenAiRealtimeSessionUpdate::new()
        .with_modality(OpenAiRealtimeModality::Audio)
        .with_audio_formats(
            OpenAiRealtimeAudioFormat::Pcm24Khz,
            OpenAiRealtimeAudioFormat::Pcm24Khz,
        );
    for command in [
        OpenAiRealtimeCommand::update_session(session),
        OpenAiRealtimeCommand::append_input_audio(
            OpenAiRealtimeAudioChunk::new(vec![0, 1, 2, 255])
                .expect("the fixed browser audio is bounded"),
        ),
        OpenAiRealtimeCommand::commit_input_audio(),
        OpenAiRealtimeCommand::create_response(),
    ] {
        connection
            .send(&command, ModelCallContext::new())
            .await
            .expect("the browser audio command must send");
    }

    assert!(matches!(
        connection.next_event(ModelCallContext::new()).await.unwrap(),
        OpenAiRealtimeEvent::InputAudioCommitted { item_id, .. }
            if item_id == "item_browser_audio"
    ));
    connection
        .next_event(ModelCallContext::new())
        .await
        .unwrap();
    assert!(matches!(
        connection.next_event(ModelCallContext::new()).await.unwrap(),
        OpenAiRealtimeEvent::OutputAudioDelta { audio, .. }
            if audio.as_bytes() == [9, 8, 7]
    ));
    assert!(matches!(
        connection.next_event(ModelCallContext::new()).await.unwrap(),
        OpenAiRealtimeEvent::OutputAudioTranscriptDelta { delta, .. }
            if delta == "browser audio"
    ));
    connection
        .next_event(ModelCallContext::new())
        .await
        .unwrap();
    connection
        .next_event(ModelCallContext::new())
        .await
        .unwrap();
    connection
        .next_event(ModelCallContext::new())
        .await
        .unwrap();
    assert!(matches!(
        connection.state(),
        OpenAiRealtimeState::Ready { .. }
    ));
}

#[wasm_bindgen_test]
async fn browser_realtime_webrtc_reuses_the_typed_event_state_machine() {
    let pending = client("v1/")
        .realtime("gpt-realtime-browser")
        .expect("the fixed browser Realtime model is valid")
        .prepare_webrtc(
            OpenAiRealtimeWebRtcOptions::new()
                .with_microphone(false)
                .with_playback(false),
            ModelCallContext::new(),
        )
        .await
        .expect("Chrome must create a WebRTC offer and oai-events channel");
    assert!(pending.offer().as_str().contains("m=application"));

    let remote = RtcPeerConnection::new().expect("Chrome must create the cassette peer");
    let remote_channel = Rc::new(RefCell::new(None::<RtcDataChannel>));
    let on_data_channel = {
        let remote_channel = Rc::clone(&remote_channel);
        Closure::wrap(Box::new(move |event: RtcDataChannelEvent| {
            remote_channel.replace(Some(event.channel()));
        }) as Box<dyn FnMut(RtcDataChannelEvent)>)
    };
    remote.set_ondatachannel(Some(on_data_channel.as_ref().unchecked_ref()));

    let offer = RtcSessionDescriptionInit::new(RtcSdpType::Offer);
    offer.set_sdp(pending.offer().as_str());
    JsFuture::from(remote.set_remote_description(&offer))
        .await
        .expect("the cassette peer must accept the local offer");
    let answer = JsFuture::from(remote.create_answer())
        .await
        .expect("the cassette peer must create an answer")
        .unchecked_into::<RtcSessionDescriptionInit>();
    JsFuture::from(remote.set_local_description(&answer))
        .await
        .expect("the cassette peer must install its answer");
    for _ in 0..500 {
        if remote.ice_gathering_state() == RtcIceGatheringState::Complete {
            break;
        }
        Delay::new(Duration::from_millis(10)).await;
    }
    assert_eq!(remote.ice_gathering_state(), RtcIceGatheringState::Complete);
    let answer_sdp = remote
        .local_description()
        .expect("the cassette peer must retain its answer")
        .sdp();
    assert!(
        pending.offer().as_str().contains("a=candidate:"),
        "the local offer must contain gathered ICE candidates: {}",
        pending.offer().as_str()
    );
    assert!(
        answer_sdp.contains("a=candidate:"),
        "the remote answer must contain gathered ICE candidates: {answer_sdp}"
    );
    let mut session = pending
        .complete(
            answer_sdp,
            ModelCallContext::new().with_deadline(Instant::now() + Duration::from_secs(5)),
        )
        .await
        .expect("the two browser peers must establish the data channel");
    assert_eq!(
        session.connection_state(),
        OpenAiRealtimeWebRtcConnectionState::Connected
    );
    assert!(matches!(
        session.ice_connection_state(),
        OpenAiRealtimeWebRtcIceState::Connected | OpenAiRealtimeWebRtcIceState::Completed
    ));
    assert_eq!(
        session.reconnect_disposition(),
        RealtimeReconnectDisposition::SafeBeforeSession
    );

    let channel = loop {
        if let Some(channel) = remote_channel.borrow().clone() {
            break channel;
        }
        Delay::new(Duration::from_millis(10)).await;
    };
    channel
        .send_with_str(
            &serde_json::json!({
                "type":"session.created",
                "session":{"id":"sess_webrtc_browser"}
            })
            .to_string(),
        )
        .expect("the cassette peer must create the session");
    assert!(matches!(
        session.next_event(ModelCallContext::new()).await.unwrap(),
        OpenAiRealtimeEvent::SessionCreated { session_id, .. }
            if session_id == "sess_webrtc_browser"
    ));

    let response_channel = channel.clone();
    let on_message = Closure::wrap(Box::new(move |event: MessageEvent| {
        let Some(text) = event.data().as_string() else {
            return;
        };
        let value: serde_json::Value =
            serde_json::from_str(&text).expect("typed commands must be JSON");
        if value["type"] == "response.create" {
            for payload in [
                serde_json::json!({
                    "type":"response.created",
                    "response":{"id":"resp_webrtc_browser"}
                }),
                serde_json::json!({
                    "type":"response.output_text.delta",
                    "response_id":"resp_webrtc_browser",
                    "delta":"webrtc-browser"
                }),
                serde_json::json!({
                    "type":"response.done",
                    "response":{"id":"resp_webrtc_browser","status":"completed"}
                }),
            ] {
                response_channel
                    .send_with_str(&payload.to_string())
                    .expect("the cassette peer must send typed response events");
            }
        }
    }) as Box<dyn FnMut(MessageEvent)>);
    channel.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
    session
        .send(
            &OpenAiRealtimeCommand::create_response(),
            ModelCallContext::new(),
        )
        .await
        .unwrap();
    assert!(matches!(
        session.next_event(ModelCallContext::new()).await.unwrap(),
        OpenAiRealtimeEvent::ResponseCreated { .. }
    ));
    assert_eq!(
        session.reconnect_disposition(),
        RealtimeReconnectDisposition::AmbiguousResponseInFlight
    );
    assert!(matches!(
        session.next_event(ModelCallContext::new()).await.unwrap(),
        OpenAiRealtimeEvent::OutputTextDelta { delta, .. } if delta == "webrtc-browser"
    ));
    session.next_event(ModelCallContext::new()).await.unwrap();
    assert!(matches!(session.state(), OpenAiRealtimeState::Ready { .. }));
    assert_eq!(
        session.reconnect_disposition(),
        RealtimeReconnectDisposition::SafeWhenIdle
    );

    for sequence in 0..64 {
        channel
            .send_with_str(
                &serde_json::json!({
                    "type":"future.event",
                    "sequence":sequence
                })
                .to_string(),
            )
            .expect("the cassette peer must flood the bounded data channel");
    }
    Delay::new(Duration::from_millis(20)).await;
    let mut observed_overflow = false;
    for _ in 0..35 {
        match session.next_event(ModelCallContext::new()).await {
            Err(OpenAiRealtimeError::Closed {
                code: 1009,
                disposition: RealtimeReconnectDisposition::SafeWhenIdle,
                ..
            }) => {
                observed_overflow = true;
                break;
            }
            Ok(_) => {}
            Err(error) => panic!("unexpected WebRTC overflow result: {error}"),
        }
    }
    assert!(
        observed_overflow,
        "the bounded oai-events queue must fail closed under event flood"
    );
    let _ = session.close(ModelCallContext::new()).await;
    remote.close();
    drop(on_message);
    drop(on_data_channel);
}

#[wasm_bindgen_test]
async fn browser_realtime_webrtc_prepares_microphone_and_remote_playback() {
    let turn = OpenAiRealtimeIceServer::turn(
        "turn:127.0.0.1:9?transport=udp",
        "browser-user",
        "browser-turn-secret",
    )
    .expect("the local TURN fixture must be valid");
    let turn_debug = format!("{turn:?}");
    assert!(turn_debug.contains("[REDACTED]"));
    assert!(!turn_debug.contains("browser-turn-secret"));
    assert!(!turn_debug.contains("browser-user"));
    let options = OpenAiRealtimeWebRtcOptions::new()
        .with_ice_server(
            OpenAiRealtimeIceServer::stun("stun:127.0.0.1:38088")
                .expect("the local STUN fixture must be valid"),
        )
        .unwrap()
        .with_ice_transport_policy(OpenAiRealtimeIceTransportPolicy::All);
    let pending = client("v1/")
        .realtime("gpt-realtime-browser")
        .expect("the fixed browser Realtime model is valid")
        .prepare_webrtc(
            options,
            ModelCallContext::new().with_deadline(Instant::now() + Duration::from_secs(5)),
        )
        .await
        .expect("Chrome must capture fake microphone media");

    assert!(pending.has_microphone());
    assert!(pending.offer().as_str().contains("m=audio"));
    assert!(pending.offer().as_str().contains("typ srflx"));
    assert!(
        pending
            .audio_element()
            .is_some_and(|audio| audio.autoplay())
    );
    pending.abort();
}

struct RelayFixture {
    session: OpenAiRealtimeWebRtcSession,
    remote: RtcPeerConnection,
    channel: RtcDataChannel,
    _on_data_channel: Closure<dyn FnMut(RtcDataChannelEvent)>,
}

impl RelayFixture {
    async fn close(mut self) {
        let _ = self.session.close(ModelCallContext::new()).await;
        self.remote.close();
    }
}

async fn connect_relay_fixture(
    url: &str,
    username: &str,
    credential: &str,
    session_id: String,
    context: ModelCallContext,
) -> Result<RelayFixture, OpenAiRealtimeError> {
    let options = OpenAiRealtimeWebRtcOptions::new()
        .with_microphone(false)
        .with_playback(false)
        .with_ice_server(OpenAiRealtimeIceServer::turn(url, username, credential)?)?
        .with_ice_transport_policy(OpenAiRealtimeIceTransportPolicy::Relay);
    let pending = client("v1/")
        .realtime("gpt-realtime-browser")?
        .prepare_webrtc(options, context.child_attempt())
        .await?;
    if !pending.offer().as_str().contains("typ relay") {
        pending.abort();
        return Err(OpenAiRealtimeError::BrowserWebRtc(
            "TURN allocation produced no relay candidate".into(),
        ));
    }

    let remote = turn_peer(url, username, credential)?;
    let remote_channel = Rc::new(RefCell::new(None::<RtcDataChannel>));
    let on_data_channel = {
        let remote_channel = Rc::clone(&remote_channel);
        Closure::wrap(Box::new(move |event: RtcDataChannelEvent| {
            remote_channel.replace(Some(event.channel()));
        }) as Box<dyn FnMut(RtcDataChannelEvent)>)
    };
    remote.set_ondatachannel(Some(on_data_channel.as_ref().unchecked_ref()));
    let offer = RtcSessionDescriptionInit::new(RtcSdpType::Offer);
    offer.set_sdp(pending.offer().as_str());
    JsFuture::from(remote.set_remote_description(&offer))
        .await
        .map_err(|_| relay_fixture_error("remote peer rejected the relay offer"))?;
    let answer = JsFuture::from(remote.create_answer())
        .await
        .map_err(|_| relay_fixture_error("remote peer could not create a relay answer"))?
        .unchecked_into::<RtcSessionDescriptionInit>();
    JsFuture::from(remote.set_local_description(&answer))
        .await
        .map_err(|_| relay_fixture_error("remote peer rejected its relay answer"))?;
    wait_for_browser_ice(&remote).await?;
    let answer_sdp = remote
        .local_description()
        .ok_or_else(|| relay_fixture_error("remote relay answer was not retained"))?
        .sdp();
    if !answer_sdp.contains("typ relay") {
        pending.abort();
        remote.close();
        return Err(relay_fixture_error(
            "remote TURN allocation produced no relay candidate",
        ));
    }

    let mut session = pending
        .complete(answer_sdp, context.child_attempt())
        .await?;
    if session.connection_state() != OpenAiRealtimeWebRtcConnectionState::Connected {
        let _ = session.close(context.child_attempt()).await;
        remote.close();
        return Err(relay_fixture_error(
            "relay-only peers did not reach connected state",
        ));
    }
    let channel = wait_for_relay_channel(&remote_channel).await?;
    channel
        .send_with_str(
            &serde_json::json!({
                "type":"session.created",
                "session":{"id":session_id}
            })
            .to_string(),
        )
        .map_err(|_| relay_fixture_error("relay peer could not create the typed session"))?;
    match session.next_event(context.child_attempt()).await? {
        OpenAiRealtimeEvent::SessionCreated {
            session_id: observed,
            ..
        } if observed == session_id => {}
        _ => {
            let _ = session.close(context.child_attempt()).await;
            remote.close();
            return Err(OpenAiRealtimeError::Protocol(
                "replacement relay session identity did not match".into(),
            ));
        }
    }
    Ok(RelayFixture {
        session,
        remote,
        channel,
        _on_data_channel: on_data_channel,
    })
}

async fn wait_for_relay_channel(
    channel: &Rc<RefCell<Option<RtcDataChannel>>>,
) -> Result<RtcDataChannel, OpenAiRealtimeError> {
    for _ in 0..1_000 {
        if let Some(channel) = channel.borrow().clone() {
            return Ok(channel);
        }
        Delay::new(Duration::from_millis(10)).await;
    }
    Err(relay_fixture_error(
        "relay data channel did not open within ten seconds",
    ))
}

fn relay_fixture_error(message: &str) -> OpenAiRealtimeError {
    OpenAiRealtimeError::BrowserWebRtc(message.into())
}

#[wasm_bindgen_test]
async fn browser_realtime_relay_only_recovers_after_real_coturn_partition() {
    let (Some(url), Some(username), Some(credential), Some(fault_endpoint), Some(recover_endpoint)) = (
        TURN_URL,
        TURN_USERNAME,
        TURN_CREDENTIAL,
        TURN_FAULT_ENDPOINT,
        TURN_RECOVER_ENDPOINT,
    ) else {
        return;
    };
    let initial_context =
        ModelCallContext::new().with_deadline(Instant::now() + Duration::from_secs(10));
    let fixture = connect_relay_fixture(
        url,
        username,
        credential,
        "sess_turn_before_partition".into(),
        initial_context,
    )
    .await
    .expect("the initial relay-only peers must connect through coturn");
    assert_eq!(
        fixture.session.connection_state(),
        OpenAiRealtimeWebRtcConnectionState::Connected
    );

    let window = web_sys::window().expect("the browser test must have a Window");
    JsFuture::from(window.fetch_with_str(fault_endpoint))
        .await
        .expect("the TURN fault request must reach the local test control plane");
    let mut observed_partition = false;
    for _ in 0..3_000 {
        if matches!(
            fixture.session.ice_connection_state(),
            OpenAiRealtimeWebRtcIceState::Disconnected | OpenAiRealtimeWebRtcIceState::Failed
        ) {
            observed_partition = true;
            break;
        }
        Delay::new(Duration::from_millis(10)).await;
    }
    assert!(
        observed_partition,
        "stopping coturn must make relay-only ICE disconnected or failed"
    );
    let disposition = fixture.session.reconnect_disposition();
    assert_eq!(disposition, RealtimeReconnectDisposition::SafeWhenIdle);

    let _ = fixture.channel.send_with_str(
        &serde_json::json!({
            "type":"session.updated",
            "session":{"id":"stale_session_must_not_cross_transport"}
        })
        .to_string(),
    );
    fixture.close().await;
    JsFuture::from(window.fetch_with_str(recover_endpoint))
        .await
        .expect("the TURN recovery request must reach the local test control plane");

    let policy = OpenAiRealtimeReconnectPolicy::exponential(
        4,
        Duration::from_millis(100),
        Duration::from_millis(800),
        2,
    )
    .expect("the bounded relay recovery policy is valid")
    .jitter(RetryJitter::None);
    let mut controller = OpenAiRealtimeReconnectController::new(policy);
    let attempts = Rc::new(RefCell::new(Vec::new()));
    let observed_attempts = attempts.clone();
    let events = Rc::new(RefCell::new(Vec::new()));
    let observed_events = events.clone();
    let recovery_context =
        ModelCallContext::new().with_deadline(Instant::now() + Duration::from_secs(40));

    let mut recovered = controller
        .reconnect(
            disposition,
            recovery_context,
            move |attempt, attempt_context| {
                observed_attempts
                    .borrow_mut()
                    .push((attempt.ordinal(), attempt.requires_fresh_credential()));
                let session_id = format!("sess_turn_recovered_{}", attempt.ordinal());
                async move {
                    connect_relay_fixture(url, username, credential, session_id, attempt_context)
                        .await
                }
            },
            move |event| observed_events.borrow_mut().push(event),
        )
        .await
        .expect("the controller must rebuild both relay-only peers after coturn restarts");
    assert_eq!(
        recovered.session.connection_state(),
        OpenAiRealtimeWebRtcConnectionState::Connected
    );
    assert!(
        attempts.borrow().iter().all(|(_, fresh)| *fresh),
        "every recovery attempt must require fresh credentials and SDP"
    );
    assert!(matches!(
        events.borrow().last(),
        Some(OpenAiRealtimeReconnectEvent::Connected { .. })
    ));

    let stale = recovered
        .session
        .next_event(
            ModelCallContext::new().with_deadline(Instant::now() + Duration::from_millis(100)),
        )
        .await
        .expect_err("old-session events must never enter the replacement connection");
    assert!(matches!(stale, OpenAiRealtimeError::DeadlineExceeded));
    recovered.close().await;
}

#[wasm_bindgen_test]
async fn browser_realtime_ice_configuration_fails_closed_before_peer_creation() {
    assert!(OpenAiRealtimeIceServer::stun("https://not-stun.example").is_err());
    assert!(OpenAiRealtimeIceServer::stun("stun:user@host.example").is_err());
    assert!(OpenAiRealtimeIceServer::turn("turn:host.example", "", "secret").is_err());

    let mut options = OpenAiRealtimeWebRtcOptions::new();
    for port in 1..=8 {
        options = options
            .with_ice_server(
                OpenAiRealtimeIceServer::stun(format!("stun:127.0.0.1:{port}"))
                    .expect("bounded local STUN fixture must be valid"),
            )
            .expect("the first eight ICE servers must be accepted");
    }
    assert!(
        options
            .with_ice_server(
                OpenAiRealtimeIceServer::stun("stun:127.0.0.1:9")
                    .expect("the ninth local STUN fixture must be valid")
            )
            .is_err()
    );

    let error = client("v1/")
        .realtime("gpt-realtime-browser")
        .expect("the fixed browser Realtime model is valid")
        .prepare_webrtc(
            OpenAiRealtimeWebRtcOptions::new()
                .with_microphone(false)
                .with_playback(false)
                .with_ice_transport_policy(OpenAiRealtimeIceTransportPolicy::Relay),
            ModelCallContext::new(),
        )
        .await
        .expect_err("relay-only policy without TURN must fail before peer creation");
    assert!(matches!(error, OpenAiRealtimeError::InvalidRequest(_)));
}

fn turn_peer(
    url: &str,
    username: &str,
    credential: &str,
) -> Result<RtcPeerConnection, OpenAiRealtimeError> {
    let server = RtcIceServer::new();
    server.set_urls_str(url);
    server.set_username(username);
    server.set_credential(credential);
    let servers = js_sys::Array::new();
    servers.push(&server);
    let configuration = RtcConfiguration::new();
    configuration.set_ice_servers(&servers);
    configuration.set_ice_transport_policy(RtcIceTransportPolicy::Relay);
    RtcPeerConnection::new_with_configuration(&configuration)
        .map_err(|_| relay_fixture_error("Chrome could not create the relay-only cassette peer"))
}

async fn wait_for_browser_ice(peer: &RtcPeerConnection) -> Result<(), OpenAiRealtimeError> {
    for _ in 0..1_000 {
        if peer.ice_gathering_state() == RtcIceGatheringState::Complete {
            return Ok(());
        }
        Delay::new(Duration::from_millis(10)).await;
    }
    Err(relay_fixture_error(
        "browser ICE gathering did not finish within ten seconds",
    ))
}

#[wasm_bindgen_test]
async fn browser_realtime_closes_instead_of_growing_an_unbounded_event_queue() {
    let mut connection = client("v1/")
        .realtime("gpt-realtime-overflow")
        .expect("the fixed overflow model is valid")
        .connect(ModelCallContext::new())
        .await
        .expect("the overflow cassette must complete its handshake");

    let mut observed_overflow = false;
    for _ in 0..66 {
        match connection.next_event(ModelCallContext::new()).await {
            Err(OpenAiRealtimeError::Transport | OpenAiRealtimeError::Closed { .. }) => {
                observed_overflow = true;
                break;
            }
            Ok(_) => {}
            Err(error) => panic!("unexpected Realtime overflow result: {error}"),
        }
    }
    assert!(
        observed_overflow,
        "the bounded browser queue must fail closed under event flood"
    );
}
