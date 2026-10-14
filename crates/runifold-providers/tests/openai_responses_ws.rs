//! Responses sockets must preserve independent lanes and conservative recovery.
#![allow(clippy::result_large_err)] // tungstenite owns the handshake callback error type.
#![cfg(all(feature = "openai-realtime", not(target_arch = "wasm32")))]

use futures_util::{SinkExt, StreamExt};
use runifold_model::{
    Message, Model, ModelCallContext, ModelRef, ModelRequest, ModelStreamAccumulator,
};
use runifold_providers::openai::{OpenAiClient, OpenAiConfig};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        Message as Frame,
        handshake::server::{Request, Response},
    },
};

fn request() -> ModelRequest {
    ModelRequest::new(ModelRef::new("openai", "gpt-test"), Message::user("hello"))
}
fn client(address: std::net::SocketAddr) -> OpenAiClient {
    OpenAiClient::new(
        OpenAiConfig::new("ws-secret")
            .unwrap()
            .with_base_url(&format!("http://{address}/v1/"))
            .unwrap(),
    )
}
fn events(id: &str, text: &str) -> Vec<Value> {
    vec![
        json!({"type":"response.created","sequence_number":0,"response":{"id":id,"model":"gpt-test"}}),
        json!({"type":"response.content_part.added","sequence_number":1,"output_index":0,"content_index":0,"part":{"type":"output_text","text":""}}),
        json!({"type":"response.output_text.delta","sequence_number":2,"output_index":0,"content_index":0,"delta":text}),
        json!({"type":"response.content_part.done","sequence_number":3,"output_index":0,"content_index":0,"part":{"type":"output_text","text":text}}),
        json!({"type":"response.completed","sequence_number":4,"response":{"id":id,"model":"gpt-test","status":"completed","usage":{}}}),
    ]
}

#[tokio::test]
async fn interleaved_lanes_decode_independently_and_steering_is_only_acknowledged() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_hdr_async(stream, |request: &Request, response: Response| {
            assert_eq!(request.uri().path(), "/v1/responses");
            assert_eq!(request.headers()["authorization"], "Bearer ws-secret");
            Ok(response)
        })
        .await
        .unwrap();
        for lane in ["a", "b"] {
            let command: Value =
                serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap())
                    .unwrap();
            assert_eq!(command["type"], "response.create");
            assert_eq!(command["stream_id"], lane);
            assert!(command.get("stream").is_none());
        }
        let mut a = events("resp_a", "alpha");
        let mut b = events("resp_b", "beta");
        for event in &mut a[1..] {
            let sequence = event["sequence_number"].as_u64().unwrap();
            event["sequence_number"] = json!(sequence + 1);
        }
        for index in 0..a.len() {
            for (lane, event) in [("a", &mut a[index]), ("b", &mut b[index])] {
                event["stream_id"] = json!(lane);
                socket.send(Frame::text(event.to_string())).await.unwrap();
            }
            if index == 0 {
                let steer: Value =
                    serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap())
                        .unwrap();
                assert_eq!(steer["type"], "response.steer");
                assert_eq!(steer["previous_response_id"], "resp_a");
                socket.send(Frame::text(json!({"type":"response.steer.accepted","stream_id":"a","sequence_number":1,"steer":{"id":"steer_a","previous_response_id":"resp_a"}}).to_string())).await.unwrap();
            }
        }
        // Do not equate the steering acknowledgement with an applied continuation.
    });
    let mut connection = client(address)
        .connect_responses_websocket(ModelCallContext::new())
        .await
        .unwrap();
    for lane in ["a", "b"] {
        connection
            .create_response(request(), Some(lane), None, ModelCallContext::new())
            .await
            .unwrap();
    }
    let mut accumulators = [ModelStreamAccumulator::new(), ModelStreamAccumulator::new()];
    let mut results = Vec::new();
    let mut acknowledged = false;
    for index in 0..11 {
        let event = connection
            .next_event(ModelCallContext::new())
            .await
            .unwrap();
        if index == 0 {
            connection
                .steer("resp_a", "keep it brief", ModelCallContext::new())
                .await
                .unwrap();
        }
        if event.payload["type"] == "response.steer.accepted" {
            acknowledged = true;
            assert_eq!(event.canonical, [] as [runifold_model::ModelStreamEvent; 0]);
        }
        let lane = usize::from(event.stream_id.as_deref() != Some("a"));
        for canonical in event.canonical {
            if let Some(response) = accumulators[lane].push(canonical).unwrap() {
                results.push(response.text());
            }
        }
    }
    assert!(acknowledged);
    assert_eq!(results, ["alpha", "beta"]);
    server.await.unwrap();
}

#[tokio::test]
async fn model_adapter_reuses_one_socket_and_never_reconnects_during_an_ambiguous_call() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        for id in ["resp_one", "resp_two"] {
            let command: Value =
                serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap())
                    .unwrap();
            assert_eq!(command["type"], "response.create");
            for event in events(id, "done") {
                socket.send(Frame::text(event.to_string())).await.unwrap();
            }
        }
        let _third = socket.next().await.unwrap().unwrap();
        socket.close(None).await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let model = client(address).responses_websocket_model();
    for _ in 0..2 {
        assert_eq!(
            model
                .invoke(request(), ModelCallContext::new())
                .await
                .unwrap()
                .text(),
            "done"
        );
    }
    let error = model
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap_err();
    assert_eq!(error.retry_safety, runifold_core::RetrySafety::Unknown);
    server.await.unwrap();
}

#[tokio::test]
async fn atomic_websocket_delivery_validates_tool_arguments_before_exposing_text() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let _request = socket.next().await.unwrap().unwrap();
        let mut response_events = events("resp_invalid", "provisional");
        response_events.pop();
        response_events.extend([
            json!({"type":"response.output_item.added","sequence_number":4,"output_index":1,"item":{"type":"function_call","id":"fc_bad","call_id":"call_bad","name":"lookup","arguments":"","status":"in_progress"}}),
            json!({"type":"response.function_call_arguments.done","sequence_number":5,"output_index":1,"item_id":"fc_bad","arguments":"{invalid"}),
            json!({"type":"response.completed","sequence_number":6,"response":{"id":"resp_invalid","status":"completed","usage":{}}}),
        ]);
        for event in response_events {
            socket.send(Frame::text(event.to_string())).await.unwrap();
        }
    });
    let model = client(address).responses_websocket_model();
    let mut stream = model
        .stream(
            request().response_mode(runifold_model::ResponseMode::Complete),
            ModelCallContext::new(),
        )
        .await
        .unwrap();
    let error = stream.next().await.unwrap().unwrap_err();
    assert_eq!(
        error.kind,
        runifold_model::ModelErrorKind::MalformedToolArguments
    );
    assert_eq!(error.retry_safety, runifold_core::RetrySafety::Unknown);
    server.await.unwrap();
}

#[tokio::test]
async fn atomic_terminal_overflow_discards_the_socket_before_the_next_call() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut first = tokio_tungstenite::accept_async(stream).await.unwrap();
        let _create = first.next().await.unwrap().unwrap();
        first
            .send(Frame::text(events("resp_large", "")[0].to_string()))
            .await
            .unwrap();
        let text = "x".repeat(925_000);
        let mut sequence = 1;
        // Each wire frame fits the 1 MiB transport limit; canonical content
        // stays below 32 MiB until the final response adds its metadata.
        // Text deltas and their original provider events are both retained.
        for output_index in 0..12 {
            for mut event in [
                json!({"type":"response.content_part.added","output_index":output_index,"content_index":0,"part":{"type":"output_text","text":""}}),
                json!({"type":"response.output_text.delta","output_index":output_index,"content_index":0,"delta":text}),
                json!({"type":"response.content_part.done","output_index":output_index,"content_index":0,"part":{"type":"output_text","text":text}}),
            ] {
                event["sequence_number"] = json!(sequence);
                sequence += 1;
                first.send(Frame::text(event.to_string())).await.unwrap();
            }
        }
        first.send(Frame::text(json!({"type":"response.completed","sequence_number":sequence,"response":{"id":"resp_large","model":"gpt-test","status":"completed","usage":{},"prompt_cache_diagnostics":{"detail":"m".repeat(500_000)}}}).to_string())).await.unwrap();
        // Keep the first socket open: a broken adapter would reuse it here.
        let (stream, _) =
            tokio::time::timeout(std::time::Duration::from_secs(3), listener.accept())
                .await
                .unwrap()
                .unwrap();
        let mut second = tokio_tungstenite::accept_async(stream).await.unwrap();
        let _create = second.next().await.unwrap().unwrap();
        for event in events("resp_fresh", "fresh connection") {
            second.send(Frame::text(event.to_string())).await.unwrap();
        }
        drop(first);
    });
    let model = client(address).responses_websocket_model();
    let mut stream = model
        .stream(
            request().response_mode(runifold_model::ResponseMode::Complete),
            ModelCallContext::new(),
        )
        .await
        .unwrap();
    let error = stream.next().await.unwrap().unwrap_err();
    assert_eq!(error.kind, runifold_model::ModelErrorKind::Protocol);
    assert!(error.message.contains("atomic WebSocket response exceeds"));
    assert_eq!(error.retry_safety, runifold_core::RetrySafety::Unknown);
    drop(stream);
    let result = model
        .invoke(
            request(),
            ModelCallContext::new()
                .with_deadline(runifold_core::Instant::now() + std::time::Duration::from_secs(2)),
        )
        .await
        .unwrap();
    assert_eq!(result.text(), "fresh connection");
    server.await.unwrap();
}

#[tokio::test]
async fn failed_response_releases_its_lane_without_interrupting_other_lanes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        for _ in 0..2 {
            let _command = socket.next().await.unwrap().unwrap();
        }
        let mut start = events("resp_failed", "").remove(0);
        start["stream_id"] = json!("a");
        socket.send(Frame::text(start.to_string())).await.unwrap();
        socket.send(Frame::text(json!({"type":"response.failed","stream_id":"a","sequence_number":1,"response":{"id":"resp_failed","status":"failed","error":{"code":"server_error","message":"temporary failure"}}}).to_string())).await.unwrap();
        for mut event in events("resp_other", "other lane finished") {
            event["stream_id"] = json!("b");
            socket.send(Frame::text(event.to_string())).await.unwrap();
        }
        let command: Value =
            serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(command["stream_id"], "a");
        for mut event in events("resp_recovered", "recovered") {
            event["stream_id"] = json!("a");
            socket.send(Frame::text(event.to_string())).await.unwrap();
        }
    });
    let mut connection = client(address)
        .connect_responses_websocket(ModelCallContext::new())
        .await
        .unwrap();
    for lane in ["a", "b"] {
        connection
            .create_response(request(), Some(lane), None, ModelCallContext::new())
            .await
            .unwrap();
    }
    connection
        .next_event(ModelCallContext::new())
        .await
        .unwrap();
    let failure = connection
        .next_event(ModelCallContext::new())
        .await
        .unwrap();
    assert_eq!(failure.payload["type"], "response.failed");
    assert_eq!(
        failure.canonical,
        [] as [runifold_model::ModelStreamEvent; 0]
    );
    for _ in 0..5 {
        assert_eq!(
            connection
                .next_event(ModelCallContext::new())
                .await
                .unwrap()
                .stream_id
                .as_deref(),
            Some("b")
        );
    }
    connection
        .create_response(request(), Some("a"), None, ModelCallContext::new())
        .await
        .unwrap();
    let mut accumulator = ModelStreamAccumulator::new();
    let mut result = None;
    for _ in 0..5 {
        for event in connection
            .next_event(ModelCallContext::new())
            .await
            .unwrap()
            .canonical
        {
            result = accumulator.push(event).unwrap().or(result);
        }
    }
    assert_eq!(result.unwrap().text(), "recovered");
    server.await.unwrap();
}

#[tokio::test]
async fn steering_automatically_starts_a_successor_without_another_submission() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let _create = socket.next().await.unwrap().unwrap();
        socket
            .send(Frame::text(events("resp_parent", "")[0].to_string()))
            .await
            .unwrap();
        let _steer = socket.next().await.unwrap().unwrap();
        for event in [
            json!({"type":"response.steer.accepted","sequence_number":1,"steer":{"id":"steer_1","previous_response_id":"resp_parent"}}),
            json!({"type":"response.incomplete","sequence_number":2,"response":{"id":"resp_parent","status":"incomplete","incomplete_details":{"reason":"steered"},"usage":{}}}),
        ] {
            socket.send(Frame::text(event.to_string())).await.unwrap();
        }
        for event in events("resp_successor", "updated plan") {
            socket.send(Frame::text(event.to_string())).await.unwrap();
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), socket.next())
                .await
                .is_err()
        );
    });
    let mut connection = client(address)
        .connect_responses_websocket(ModelCallContext::new())
        .await
        .unwrap();
    connection
        .create_response(request(), None, None, ModelCallContext::new())
        .await
        .unwrap();
    connection
        .next_event(ModelCallContext::new())
        .await
        .unwrap();
    connection
        .steer("resp_parent", "change the plan", ModelCallContext::new())
        .await
        .unwrap();
    assert_eq!(
        connection
            .next_event(ModelCallContext::new())
            .await
            .unwrap()
            .payload["type"],
        "response.steer.accepted"
    );
    assert_eq!(
        connection
            .next_event(ModelCallContext::new())
            .await
            .unwrap()
            .payload["type"],
        "response.incomplete"
    );
    let mut accumulator = ModelStreamAccumulator::new();
    let mut result = None;
    for _ in 0..5 {
        for event in connection
            .next_event(ModelCallContext::new())
            .await
            .unwrap()
            .canonical
        {
            result = accumulator.push(event).unwrap().or(result);
        }
    }
    assert_eq!(result.unwrap().text(), "updated plan");
    server.await.unwrap();
}

#[tokio::test]
async fn model_adapter_returns_provider_failure_with_ambiguous_retry_safety() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let _create = socket.next().await.unwrap().unwrap();
        for event in [
            events("resp_failed", "").remove(0),
            json!({"type":"response.failed","sequence_number":1,"response":{"id":"resp_failed","status":"failed","error":{"code":"server_error","message":"temporary failure"}}}),
        ] {
            socket.send(Frame::text(event.to_string())).await.unwrap();
        }
    });
    let error = client(address)
        .responses_websocket_model()
        .invoke(request(), ModelCallContext::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind, runifold_model::ModelErrorKind::Provider);
    assert_eq!(error.metadata["openai.error.code"], "server_error");
    assert_eq!(error.retry_safety, runifold_core::RetrySafety::Unknown);
    server.await.unwrap();
}

#[tokio::test]
async fn request_error_without_a_sequence_is_visible_and_connection_expiry_is_final() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let _create = socket.next().await.unwrap().unwrap();
        socket.send(Frame::text(json!({"type":"error","status":400,"stream_id":"a","error":{"type":"invalid_request_error","code":"previous_response_not_found","message":"not cached"}}).to_string())).await.unwrap();
        let _reconciled_create = socket.next().await.unwrap().unwrap();
        for mut event in events("resp_reconciled", "reconciled") {
            event["stream_id"] = json!("a");
            socket.send(Frame::text(event.to_string())).await.unwrap();
        }
        socket.send(Frame::text(json!({"type":"error","status":400,"error":{"type":"invalid_request_error","code":"websocket_connection_limit_reached","message":"connection expired"}}).to_string())).await.unwrap();
    });
    let mut connection = client(address)
        .connect_responses_websocket(ModelCallContext::new())
        .await
        .unwrap();
    connection
        .create_response(
            request(),
            Some("a"),
            Some("resp_uncached"),
            ModelCallContext::new(),
        )
        .await
        .unwrap();
    let rejected = connection
        .next_event(ModelCallContext::new())
        .await
        .unwrap();
    assert_eq!(
        rejected.payload["error"]["code"],
        "previous_response_not_found"
    );
    assert_eq!(
        rejected.canonical,
        [] as [runifold_model::ModelStreamEvent; 0]
    );
    connection
        .create_response(request(), Some("a"), None, ModelCallContext::new())
        .await
        .unwrap();
    for _ in 0..5 {
        connection
            .next_event(ModelCallContext::new())
            .await
            .unwrap();
    }
    assert_eq!(
        connection
            .next_event(ModelCallContext::new())
            .await
            .unwrap()
            .payload["error"]["code"],
        "websocket_connection_limit_reached"
    );
    let error = connection
        .create_response(request(), Some("a"), None, ModelCallContext::new())
        .await
        .unwrap_err();
    assert_eq!(error.kind, runifold_model::ModelErrorKind::Transport);
    assert_eq!(error.retry_safety, runifold_core::RetrySafety::Unknown);
    server.await.unwrap();
}

#[tokio::test]
async fn websocket_model_maps_self_namespaced_calls_to_the_advertised_flat_tool() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        for _ in 0..2 {
            let _create = socket.next().await.unwrap().unwrap();
            let item = json!({"type":"function_call","id":"fc_test","call_id":"call_test","name":"lookup","namespace":"lookup","arguments":"{}","status":"completed"});
            let mut added = item.clone();
            added["arguments"] = json!("");
            added["status"] = json!("in_progress");
            for event in [
                events("resp_test", "").remove(0),
                json!({"type":"response.output_item.added","sequence_number":1,"output_index":0,"item":added}),
                json!({"type":"response.function_call_arguments.done","sequence_number":2,"output_index":0,"arguments":"{}"}),
                json!({"type":"response.output_item.done","sequence_number":3,"output_index":0,"item":item}),
                json!({"type":"response.completed","sequence_number":4,"response":{"id":"resp_test","model":"gpt-test","status":"completed","usage":{}}}),
            ] {
                socket.send(Frame::text(event.to_string())).await.unwrap();
            }
        }
    });
    let model = client(address).responses_websocket_model();
    let spec: runifold_model::ToolSpec = serde_json::from_value(json!({"name":"lookup","description":"lookup","input_schema":{"type":"object"},"metadata":{"openai.defer_loading":true}})).unwrap();
    for mode in [
        runifold_model::ResponseMode::Streaming,
        runifold_model::ResponseMode::Complete,
    ] {
        let response = model
            .invoke(
                request().tool(spec.clone()).response_mode(mode),
                ModelCallContext::new(),
            )
            .await
            .unwrap();
        let runifold_model::ContentPart::ToolCall(call) = &response.content[0] else {
            panic!("expected call");
        };
        assert_eq!(call.name, "lookup");
        assert_eq!(call.metadata["openai.namespace"], "lookup");
    }
    server.await.unwrap();
}
