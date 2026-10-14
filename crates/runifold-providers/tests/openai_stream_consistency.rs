//! Cross-event consistency and partial-response parity at the public boundary.
#![cfg(feature = "openai")]

use runifold_model::{
    ContentPart, FinishReason, Message, Model, ModelCallContext, ModelRef, ModelRequest,
    ModelStreamAccumulator, ResponseMode,
};
use runifold_provider_testkit::{CassetteServer, HttpExchange, ResponseChunk, ScriptedResponse};
use runifold_providers::openai::{OpenAiClient, OpenAiConfig, OpenAiEventDecoder};
use serde_json::{Value, json};

fn request() -> ModelRequest {
    ModelRequest::new(
        ModelRef::new("openai", "gpt-test"),
        Message::user("inspect"),
    )
}
fn client(server: &CassetteServer) -> OpenAiClient {
    OpenAiClient::new(
        OpenAiConfig::new("test-secret")
            .unwrap()
            .with_base_url(&format!("{}v1/", server.base_url()))
            .unwrap(),
    )
}
fn started() -> OpenAiEventDecoder {
    let mut decoder = OpenAiEventDecoder::new();
    decoder
        .decode(json!({"type":"response.created","response":{"id":"resp_test","model":"gpt-test"}}))
        .unwrap();
    decoder
}
fn tool(kind: &str, id: &str, call: &str) -> Value {
    json!({"type":"response.output_item.added","output_index":0,"item":{"type":kind,"id":id,"call_id":call,"name":"lookup","arguments":"","input":"","status":"in_progress"}})
}

#[test]
fn conflicting_function_arguments_and_cross_type_events_fail_closed() {
    let added = tool("function_call", "fc_test", "call_test");
    for events in [
        vec![
            json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":"{\"a\":1"}),
            json!({"type":"response.function_call_arguments.done","output_index":0,"arguments":"{\"a\":2}"}),
        ],
        vec![
            json!({"type":"response.function_call_arguments.done","output_index":0,"arguments":"{\"a\":1}"}),
            json!({"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","id":"fc_test","call_id":"call_test","name":"lookup","status":"completed","arguments":"{\"a\":2}"}}),
        ],
        vec![
            json!({"type":"response.function_call_arguments.done","output_index":0,"arguments":"{}"}),
            json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":" "}),
        ],
        vec![json!({"type":"response.custom_tool_call_input.done","output_index":0,"input":"{}"})],
    ] {
        let mut decoder = started();
        decoder.decode(added.clone()).unwrap();
        for event in &events[..events.len() - 1] {
            decoder.decode(event.clone()).unwrap();
        }
        assert!(decoder.decode(events.last().unwrap().clone()).is_err());
    }
    let mut decoder = started();
    decoder
        .decode(tool("custom_tool_call", "ctc_test", "call_test"))
        .unwrap();
    assert!(decoder.decode(json!({"type":"response.function_call_arguments.done","output_index":0,"arguments":"{}"})).is_err());
}

#[test]
fn duplicate_call_ids_indices_and_response_identity_drift_are_rejected() {
    for (index, id, call) in [(0, "fc_other", "call_other"), (1, "fc_other", "call_test")] {
        let mut decoder = started();
        decoder
            .decode(tool("function_call", "fc_test", "call_test"))
            .unwrap();
        let mut duplicate = tool("function_call", id, call);
        duplicate["output_index"] = json!(index);
        assert!(decoder.decode(duplicate).is_err());
    }
    let mut decoder = started();
    decoder
        .decode(tool("function_call", "fc_test", "call_test"))
        .unwrap();
    assert!(decoder.decode(json!({"type":"response.function_call_arguments.delta","response_id":"resp_other","output_index":0,"delta":"{}"})).is_err());
}

#[tokio::test]
async fn complete_responses_reject_reused_tool_call_ids() {
    let server = CassetteServer::start(vec![HttpExchange::new("POST", "/v1/responses", ScriptedResponse::json(200, &json!({"id":"resp_test","model":"gpt-test","status":"completed","output":[
        {"type":"function_call","id":"fc_one","call_id":"call_same","name":"one","arguments":"{}","status":"completed"},
        {"type":"custom_tool_call","id":"ctc_two","call_id":"call_same","name":"two","input":"run","status":"completed"}
    ],"usage":{}})).unwrap())]).unwrap();
    assert!(
        client(&server)
            .invoke(
                request().response_mode(ResponseMode::Complete),
                ModelCallContext::new()
            )
            .await
            .is_err()
    );
    server.assert_finished().unwrap();
}

#[test]
fn incomplete_streamed_messages_preserve_partial_output_and_reject_false_success() {
    for incomplete in [true, false] {
        let mut decoder = OpenAiEventDecoder::new();
        let mut accumulator = ModelStreamAccumulator::new();
        let events = [
            json!({"type":"response.created","response":{"id":"resp_test","model":"gpt-test"}}),
            json!({"type":"response.content_part.added","output_index":0,"content_index":0,"part":{"type":"output_text","text":""}}),
            json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"partial"}),
            json!({"type":"response.content_part.done","output_index":0,"content_index":0,"part":{"type":"output_text","text":"partial"}}),
            json!({"type":"response.output_item.done","output_index":0,"item":{"type":"message","status":"incomplete","role":"assistant","content":[{"type":"output_text","text":"partial"}]}}),
        ];
        for event in events {
            for canonical in decoder.decode(event).unwrap() {
                accumulator.push(canonical).unwrap();
            }
        }
        let terminal = json!({"type":if incomplete {"response.incomplete"} else {"response.completed"},"response":{"id":"resp_test","status":if incomplete {"incomplete"} else {"completed"},"incomplete_details":{"reason":"max_output_tokens"},"usage":{}}});
        let result = decoder.decode(terminal);
        if incomplete {
            let mut response = None;
            for event in result.unwrap() {
                response = accumulator.push(event).unwrap().or(response);
            }
            let response = response.unwrap();
            assert_eq!(response.text(), "partial");
            assert_eq!(response.finish_reason, FinishReason::Length);
        } else {
            assert!(result.is_err());
        }
    }
}

#[test]
fn final_message_content_must_match_the_closed_delta_blocks() {
    let mut decoder = started();
    for event in [
        json!({"type":"response.content_part.added","output_index":0,"content_index":0,"part":{"type":"output_text","text":""}}),
        json!({"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"original"}),
        json!({"type":"response.content_part.done","output_index":0,"content_index":0,"part":{"type":"output_text","text":"original"}}),
    ] {
        decoder.decode(event).unwrap();
    }
    assert!(decoder.decode(json!({"type":"response.output_item.done","output_index":0,"item":{"type":"message","status":"completed","content":[{"type":"output_text","text":"different"}]}})).is_err());
}

#[tokio::test]
async fn resumed_background_delta_cannot_target_another_response() {
    use futures_util::StreamExt;
    let event = json!({"type":"response.output_text.delta","sequence_number":9,"response_id":"resp_other","output_index":0,"content_index":0,"delta":"wrong task"});
    let server = CassetteServer::start(vec![HttpExchange::new(
        "GET",
        "/v1/responses/resp_test?stream=true&starting_after=8",
        ScriptedResponse::ok(vec![ResponseChunk::text(format!("data: {event}\n\n"))])
            .with_header("content-type", "text/event-stream"),
    )])
    .unwrap();
    let client = client(&server);
    let mut stream = client
        .control_plane()
        .stream_response("resp_test", Some(8), ModelCallContext::new())
        .await
        .unwrap();
    assert!(stream.next().await.unwrap().is_err());
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn compaction_preserves_its_documented_options_and_rejects_unrelated_fields() {
    let server = CassetteServer::start(vec![HttpExchange::new("POST", "/v1/responses/compact", ScriptedResponse::json(200, &json!({"id":"cmp_test","object":"response.compaction","output":[{"type":"compaction","encrypted_content":"opaque"}]})).unwrap())]).unwrap();
    let client = client(&server);
    let control = client.control_plane();
    for options in [json!({"instructions":true}), json!({"store":false})] {
        assert!(
            control
                .compact_response(
                    request().provider_option("openai", options),
                    ModelCallContext::new()
                )
                .await
                .is_err()
        );
    }
    control.compact_response(request().provider_option("openai", json!({"instructions":"keep tool identities","prompt_cache_key":"session","service_tier":"default"})), ModelCallContext::new()).await.unwrap();
    let body = server.observed_requests()[0].json_body().unwrap();
    assert_eq!(body["instructions"], "keep tool identities");
    assert_eq!(body["prompt_cache_key"], "session");
    server.assert_finished().unwrap();
}

#[test]
fn unknown_tool_result_kinds_do_not_silently_become_function_outputs() {
    let result: runifold_model::ToolResult = serde_json::from_value(json!({"call_id":"call_test","content":[{"type":"text","text":"done"}],"is_error":false,"metadata":{"openai.tool_type":"future_tool"}})).unwrap();
    let request = request().message(
        Message::new(
            runifold_model::Role::Tool,
            vec![ContentPart::ToolResult(result)],
        )
        .unwrap(),
    );
    assert!(runifold_providers::openai::encode_request(&request).is_err());
}

#[tokio::test]
async fn image_responses_cannot_silently_change_the_requested_output_format() {
    use runifold_model::{ImageFormat, ImageGenerationModel, ImageGenerationRequest};
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1/images/generations",
        ScriptedResponse::json(
            200,
            &json!({"output_format":"png","data":[{"b64_json":"aW1hZ2U="}]}),
        )
        .unwrap(),
    )])
    .unwrap();
    let error = client(&server)
        .generate_image(
            ImageGenerationRequest {
                model: ModelRef::new("openai", "gpt-image-2.5-sunburst"),
                prompt: "test image".into(),
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
    assert!(error.message.contains("format disagrees"));
    server.assert_finished().unwrap();
}

#[test]
fn async_correlation_normalizes_legitimate_compatible_metadata_for_each_provider() {
    use runifold_providers::openai::OpenAiAsyncToolState;
    for provider in ["openai", "openai-compatible", "custom-provider"] {
        let mut state = OpenAiAsyncToolState::new(provider, "session").unwrap();
        let call: runifold_model::ToolCall = serde_json::from_value(
            json!({"id":"call_test","name":"code","arguments":"run","metadata":{
                format!("{provider}.async"):true,
                "openai-compatible.tool_type":"custom_tool_call",
                "openai-compatible.caller":{"type":"program","caller_id":"program_original"}
            }}),
        )
        .unwrap();
        state.register("session", call).unwrap();
        let result: runifold_model::ToolResult = serde_json::from_value(json!({"call_id":"call_test","content":[{"type":"text","text":"done"}],"is_error":false,"metadata":{}})).unwrap();
        let message = state.resolve("session", result).unwrap();
        let ContentPart::ToolResult(result) = &message.content[0] else {
            panic!("expected result");
        };
        assert_eq!(
            result.metadata[&format!("{provider}.tool_type")],
            "custom_tool_call"
        );
        assert_eq!(
            result.metadata[&format!("{provider}.caller")]["caller_id"],
            "program_original"
        );
        state.validate("session").unwrap();
        let restored: OpenAiAsyncToolState =
            serde_json::from_value(serde_json::to_value(state).unwrap()).unwrap();
        restored.validate("session").unwrap();
    }
}

#[test]
fn decoder_debug_does_not_expose_text_or_tool_arguments() {
    let mut decoder = started();
    decoder.decode(json!({"type":"response.content_part.added","output_index":0,"content_index":0,"part":{"type":"output_text","text":"private_output_sentinel"}})).unwrap();
    let mut call = tool("function_call", "fc_test", "call_test");
    call["output_index"] = json!(1);
    call["item"]["arguments"] = json!("{\"secret\":\"private_argument_sentinel\"}");
    decoder.decode(call).unwrap();
    let debug = format!("{decoder:?}");
    assert!(!debug.contains("private_output_sentinel"));
    assert!(!debug.contains("private_argument_sentinel"));
    assert!(debug.contains("open_tool_calls"));
}

#[tokio::test]
async fn self_namespaced_discovered_tools_use_the_advertised_registry_name_and_replay_wire_identity()
 {
    for mode in [ResponseMode::Complete, ResponseMode::Streaming] {
        for explicit_namespace in [false, true] {
            let item = json!({"type":"function_call","id":"fc_test","call_id":"call_test","name":"lookup","namespace":"lookup","arguments":"{}","status":"completed"});
            let terminal = json!({"id":"resp_test","model":"gpt-test","status":"completed","output":[item],"usage":{}});
            let response = if mode == ResponseMode::Complete {
                ScriptedResponse::json(200, &terminal).unwrap()
            } else {
                let mut added = item.clone();
                added["arguments"] = json!("");
                added["status"] = json!("in_progress");
                let events = [
                    json!({"type":"response.created","response":{"id":"resp_test","model":"gpt-test"}}),
                    json!({"type":"response.output_item.added","output_index":0,"item":added}),
                    json!({"type":"response.function_call_arguments.done","output_index":0,"arguments":"{}"}),
                    json!({"type":"response.output_item.done","output_index":0,"item":item}),
                    json!({"type":"response.completed","response":terminal}),
                ];
                ScriptedResponse::ok(
                    events
                        .into_iter()
                        .enumerate()
                        .map(|(sequence, mut event)| {
                            event["sequence_number"] = json!(sequence);
                            ResponseChunk::text(format!("data: {event}\n\n"))
                        })
                        .collect(),
                )
                .with_header("content-type", "text/event-stream")
            };
            let server =
                CassetteServer::start(vec![HttpExchange::new("POST", "/v1/responses", response)])
                    .unwrap();
            let name = if explicit_namespace {
                "lookup.lookup"
            } else {
                "lookup"
            };
            let spec: runifold_model::ToolSpec = serde_json::from_value(json!({"name":name,"description":"lookup","input_schema":{"type":"object","properties":{}},"metadata":if explicit_namespace {json!({"openai.namespace":"lookup"})} else {json!({"openai.defer_loading":true})}})).unwrap();
            let request = request().tool(spec).response_mode(mode);
            let response = client(&server)
                .invoke(request.clone(), ModelCallContext::new())
                .await
                .unwrap();
            let ContentPart::ToolCall(call) = &response.content[0] else {
                panic!("expected tool");
            };
            assert_eq!(call.name, name);
            let body = runifold_providers::openai::encode_request(
                &request.message(response.assistant_message().unwrap()),
            )
            .unwrap();
            assert_eq!(body["input"][1]["name"], "lookup");
            assert_eq!(body["input"][1]["namespace"], "lookup");
            server.assert_finished().unwrap();
        }
    }
}

#[test]
fn ambiguous_flat_and_explicit_self_namespaced_definitions_fail_before_transport() {
    let flat: runifold_model::ToolSpec = serde_json::from_value(json!({"name":"lookup","description":"flat","input_schema":{"type":"object"},"metadata":{}})).unwrap();
    let mut explicit = flat.clone();
    explicit.name = "lookup.lookup".into();
    explicit
        .metadata
        .insert("openai.namespace".into(), json!("lookup"));
    assert!(
        runifold_providers::openai::encode_request(&request().tool(flat).tool(explicit)).is_err()
    );
}

#[test]
fn chat_history_cannot_silently_discard_responses_tool_or_phase_semantics() {
    use runifold_providers::openai::encode_chat_request;
    for field in ["tool_type", "namespace", "caller", "async"] {
        let metadata =
            std::collections::BTreeMap::from([(format!("openai.{field}"), json!("native"))]);
        let call = runifold_model::ToolCall {
            id: "call_test".into(),
            name: "lookup".into(),
            arguments: json!({}),
            raw_arguments: None,
            metadata: metadata.clone(),
        };
        let request_call = request().message(
            Message::new(
                runifold_model::Role::Assistant,
                vec![ContentPart::ToolCall(call)],
            )
            .unwrap(),
        );
        assert!(encode_chat_request(&request_call, "openai").is_err());
        let result = runifold_model::ToolResult {
            call_id: "call_test".into(),
            name: None,
            content: vec![ContentPart::text("done")],
            structured_content: None,
            is_error: false,
            metadata,
        };
        let request_result = request().message(
            Message::new(
                runifold_model::Role::Tool,
                vec![ContentPart::ToolResult(result)],
            )
            .unwrap(),
        );
        assert!(encode_chat_request(&request_result, "openai").is_err());
    }
    let mut message = Message::new(
        runifold_model::Role::Assistant,
        vec![ContentPart::text("commentary")],
    )
    .unwrap();
    message.metadata.insert(
        "openai.message_blocks".into(),
        json!([{"phase":"commentary"}]),
    );
    assert!(encode_chat_request(&request().message(message), "openai").is_err());
}
