//! `OpenAI` Realtime WebSocket lifecycle, audio, deadline, and ambiguity tests.

#![cfg(feature = "openai")]
#![allow(missing_docs)]
#![allow(clippy::result_large_err, clippy::too_many_lines)]

use std::{sync::Arc, time::Duration};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use runifold_model::ModelCallContext;
use runifold_providers::openai::{
    OpenAiClient, OpenAiConfig, OpenAiRealtimeAudioChunk, OpenAiRealtimeAudioFormat,
    OpenAiRealtimeCommand, OpenAiRealtimeEvent, OpenAiRealtimeModality,
    OpenAiRealtimeSessionUpdate, OpenAiRealtimeState, OpenAiWireProtocol,
    RealtimeReconnectDisposition,
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::Mutex};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{Request, Response},
    },
};

#[tokio::test]
async fn realtime_websocket_is_typed_ordered_and_authenticated() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::<Value>::new()));
    let server_seen = Arc::clone(&seen);

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let socket = accept_hdr_async(stream, |request: &Request, response: Response| {
            assert_eq!(request.uri().path(), "/v1/realtime");
            assert_eq!(request.uri().query(), Some("model=gpt-realtime-test"));
            assert_eq!(request.headers()["authorization"], "Bearer realtime-secret");
            Ok(response)
        })
        .await
        .unwrap();
        let (mut writer, mut reader) = socket.split();
        writer
            .send(Message::text(
                json!({
                    "type": "session.created",
                    "session": {"id": "sess_test"}
                })
                .to_string(),
            ))
            .await
            .unwrap();

        for _ in 0..3 {
            let Message::Text(text) = reader.next().await.unwrap().unwrap() else {
                panic!("client commands must be text frames");
            };
            server_seen
                .lock()
                .await
                .push(serde_json::from_str(&text).unwrap());
        }

        for event in [
            json!({
                "type": "response.created",
                "response": {"id": "resp_test"}
            }),
            json!({
                "type": "response.output_text.delta",
                "response_id": "resp_test",
                "delta": "hello"
            }),
            json!({
                "type": "response.done",
                "response": {
                    "id": "resp_test",
                    "status": "completed",
                    "usage": {"input_tokens": 2, "output_tokens": 1}
                }
            }),
        ] {
            writer.send(Message::text(event.to_string())).await.unwrap();
        }
    });

    let config = OpenAiConfig::compatible(
        "openai",
        "realtime-secret",
        &format!("http://{address}/v1/"),
        OpenAiWireProtocol::default(),
    )
    .unwrap();
    let mut connection = OpenAiClient::new(config)
        .realtime("gpt-realtime-test")
        .unwrap()
        .connect(ModelCallContext::new())
        .await
        .unwrap();

    assert!(matches!(
        connection
            .next_event(ModelCallContext::new())
            .await
            .unwrap(),
        OpenAiRealtimeEvent::SessionCreated { .. }
    ));
    assert_eq!(
        connection.state(),
        &OpenAiRealtimeState::Ready {
            session_id: "sess_test".into()
        }
    );

    let update = OpenAiRealtimeSessionUpdate::new()
        .with_instructions("Answer briefly")
        .unwrap()
        .with_modality(OpenAiRealtimeModality::Text);
    connection
        .send(
            &OpenAiRealtimeCommand::update_session(update),
            ModelCallContext::new(),
        )
        .await
        .unwrap();
    connection
        .send(
            &OpenAiRealtimeCommand::user_text("hello").unwrap(),
            ModelCallContext::new(),
        )
        .await
        .unwrap();
    connection
        .send(
            &OpenAiRealtimeCommand::create_response(),
            ModelCallContext::new(),
        )
        .await
        .unwrap();

    assert!(matches!(
        connection
            .next_event(ModelCallContext::new())
            .await
            .unwrap(),
        OpenAiRealtimeEvent::ResponseCreated { .. }
    ));
    assert!(matches!(
        connection.next_event(ModelCallContext::new()).await.unwrap(),
        OpenAiRealtimeEvent::OutputTextDelta { delta, .. } if delta == "hello"
    ));
    assert!(matches!(
        connection.next_event(ModelCallContext::new()).await.unwrap(),
        OpenAiRealtimeEvent::ResponseDone { status, .. } if status == "completed"
    ));
    assert!(matches!(
        connection.state(),
        OpenAiRealtimeState::Ready { .. }
    ));

    server.await.unwrap();
    let commands = seen.lock().await;
    assert_eq!(commands[0]["type"], "session.update");
    assert_eq!(commands[0]["session"]["type"], "realtime");
    assert_eq!(commands[1]["type"], "conversation.item.create");
    assert_eq!(commands[2]["type"], "response.create");
    assert!(
        commands
            .iter()
            .all(|command| command.get("event_id").is_none())
    );
}

#[tokio::test]
async fn realtime_audio_is_bounded_encoded_and_decoded() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_hdr_async(stream, |_request: &Request, response: Response| {
            Ok(response)
        })
        .await
        .unwrap();
        socket
            .send(Message::text(
                json!({"type":"session.created","session":{"id":"sess_audio"}}).to_string(),
            ))
            .await
            .unwrap();

        let mut commands = Vec::new();
        for _ in 0..4 {
            let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                panic!("audio commands must be text frames");
            };
            commands.push(serde_json::from_str::<Value>(&text).unwrap());
        }
        assert_eq!(
            commands[0]["session"]["audio"]["input"]["format"],
            json!({"type":"audio/pcm","rate":24000})
        );
        assert_eq!(
            STANDARD
                .decode(commands[1]["audio"].as_str().unwrap())
                .unwrap(),
            vec![0, 1, 2, 255]
        );
        assert_eq!(commands[2]["type"], "input_audio_buffer.commit");
        assert_eq!(commands[3]["type"], "response.create");

        for event in [
            json!({
                "type":"input_audio_buffer.committed",
                "previous_item_id":null,
                "item_id":"item_audio"
            }),
            json!({"type":"response.created","response":{"id":"resp_audio"}}),
            json!({
                "type":"response.output_audio.delta",
                "response_id":"resp_audio",
                "item_id":"item_output",
                "output_index":0,
                "content_index":0,
                "delta":STANDARD.encode([9, 8, 7])
            }),
            json!({
                "type":"response.output_audio_transcript.delta",
                "response_id":"resp_audio",
                "item_id":"item_output",
                "output_index":0,
                "content_index":0,
                "delta":"hello"
            }),
            json!({
                "type":"response.output_audio.done",
                "response_id":"resp_audio",
                "item_id":"item_output",
                "output_index":0,
                "content_index":0
            }),
            json!({
                "type":"response.output_audio_transcript.done",
                "response_id":"resp_audio",
                "item_id":"item_output",
                "output_index":0,
                "content_index":0,
                "transcript":"hello"
            }),
            json!({
                "type":"response.done",
                "response":{"id":"resp_audio","status":"completed"}
            }),
        ] {
            socket.send(Message::text(event.to_string())).await.unwrap();
        }
    });

    let config = OpenAiConfig::custom(
        "gateway",
        &format!("http://{address}/v1/"),
        OpenAiWireProtocol::default(),
    )
    .unwrap();
    let mut connection = OpenAiClient::new(config)
        .realtime("gpt-realtime-test")
        .unwrap()
        .connect(ModelCallContext::new())
        .await
        .unwrap();
    connection
        .next_event(ModelCallContext::new())
        .await
        .unwrap();

    let session = OpenAiRealtimeSessionUpdate::new()
        .with_modality(OpenAiRealtimeModality::Audio)
        .with_audio_formats(
            OpenAiRealtimeAudioFormat::Pcm24Khz,
            OpenAiRealtimeAudioFormat::Pcm24Khz,
        )
        .with_voice("alloy")
        .unwrap();
    for command in [
        OpenAiRealtimeCommand::update_session(session),
        OpenAiRealtimeCommand::append_input_audio(
            OpenAiRealtimeAudioChunk::new(vec![0, 1, 2, 255]).unwrap(),
        ),
        OpenAiRealtimeCommand::commit_input_audio(),
        OpenAiRealtimeCommand::create_response(),
    ] {
        connection
            .send(&command, ModelCallContext::new())
            .await
            .unwrap();
    }

    assert!(matches!(
        connection.next_event(ModelCallContext::new()).await.unwrap(),
        OpenAiRealtimeEvent::InputAudioCommitted { item_id, .. } if item_id == "item_audio"
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
        OpenAiRealtimeEvent::OutputAudioTranscriptDelta { delta, .. } if delta == "hello"
    ));
    assert!(matches!(
        connection
            .next_event(ModelCallContext::new())
            .await
            .unwrap(),
        OpenAiRealtimeEvent::OutputAudioDone { .. }
    ));
    assert!(matches!(
        connection.next_event(ModelCallContext::new()).await.unwrap(),
        OpenAiRealtimeEvent::OutputAudioTranscriptDone { transcript, .. }
            if transcript == "hello"
    ));
    connection
        .next_event(ModelCallContext::new())
        .await
        .unwrap();
    assert!(matches!(
        connection.state(),
        OpenAiRealtimeState::Ready { .. }
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn realtime_receive_obeys_the_call_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _socket = accept_hdr_async(stream, |_request: &Request, response: Response| {
            Ok(response)
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    });

    let config = OpenAiConfig::custom(
        "gateway",
        &format!("http://{address}/v1/"),
        OpenAiWireProtocol::default(),
    )
    .unwrap();
    let mut connection = OpenAiClient::new(config)
        .realtime("gpt-realtime-test")
        .unwrap()
        .connect(ModelCallContext::new())
        .await
        .unwrap();
    let error = connection
        .next_event(
            ModelCallContext::new()
                .with_deadline(runifold_core::Instant::now() + Duration::from_millis(30)),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        runifold_providers::openai::OpenAiRealtimeError::DeadlineExceeded
    ));

    server.abort();
}

#[tokio::test]
async fn disconnect_during_a_response_is_explicitly_ambiguous() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_hdr_async(stream, |_request: &Request, response: Response| {
            Ok(response)
        })
        .await
        .unwrap();
        socket
            .send(Message::text(
                json!({"type":"session.created","session":{"id":"sess_ambiguous"}}).to_string(),
            ))
            .await
            .unwrap();
        socket
            .send(Message::text(
                json!({"type":"response.created","response":{"id":"resp_ambiguous"}}).to_string(),
            ))
            .await
            .unwrap();
        socket.close(None).await.unwrap();
    });

    let config = OpenAiConfig::custom(
        "gateway",
        &format!("http://{address}/v1/"),
        OpenAiWireProtocol::default(),
    )
    .unwrap();
    let mut connection = OpenAiClient::new(config)
        .realtime("gpt-realtime-test")
        .unwrap()
        .connect(ModelCallContext::new())
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
    let error = connection
        .next_event(ModelCallContext::new())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        runifold_providers::openai::OpenAiRealtimeError::Closed {
            disposition: RealtimeReconnectDisposition::AmbiguousResponseInFlight,
            ..
        }
    ));
    server.await.unwrap();
}
