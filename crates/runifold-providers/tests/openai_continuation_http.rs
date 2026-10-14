//! Continuation state must survive transport, serialization and replay.
#![cfg(feature = "openai")]

use runifold_model::{
    ContentPart, Message, Model, ModelCallContext, ModelRef, ModelRequest, ResponseMode, ToolSpec,
};
use runifold_provider_testkit::{CassetteServer, HttpExchange, ResponseChunk, ScriptedResponse};
use runifold_providers::openai::{
    OpenAiClient, OpenAiConfig, OpenAiResponseStatus, encode_request,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn client(server: &CassetteServer) -> OpenAiClient {
    OpenAiClient::new(
        OpenAiConfig::new("test-secret")
            .unwrap()
            .with_base_url(&format!("{}v1/", server.base_url()))
            .unwrap(),
    )
}
fn request() -> ModelRequest {
    ModelRequest::new(
        ModelRef::new("openai", "gpt-test"),
        Message::user("inspect"),
    )
}
fn message(id: &str, phase: &str, text: &str) -> Value {
    json!({"id":id,"type":"message","role":"assistant","status":"completed", "phase":phase,"content":[{"type":"output_text","text":text}]})
}
fn complete(output: Value) -> Value {
    let mut response = json!({"id":"resp_test","model":"gpt-test","status":"completed","usage":{}});
    response["output"] = output;
    response
}

#[tokio::test]
async fn complete_and_streamed_message_phases_survive_serialized_history() {
    let messages = [
        message("msg_comment", "commentary", "checking"),
        message("msg_final", "final_answer", "done"),
    ];
    for mode in [ResponseMode::Complete, ResponseMode::Streaming] {
        let response = if mode == ResponseMode::Complete {
            ScriptedResponse::json(200, &complete(json!(messages))).unwrap()
        } else {
            let mut events = vec![
                json!({"type":"response.created","response":{"id":"resp_test","model":"gpt-test"}}),
            ];
            for (index, item) in messages.iter().enumerate() {
                events.extend([
                    json!({"type":"response.content_part.added","output_index":index,"content_index":0,"part":{"type":"output_text","text":""}}),
                    json!({"type":"response.output_text.delta","output_index":index,"content_index":0,"delta":item["content"][0]["text"]}),
                    json!({"type":"response.content_part.done","output_index":index,"content_index":0,"part":item["content"][0]}),
                    json!({"type":"response.output_item.done","output_index":index,"item":item}),
                ]);
            }
            events.push(json!({"type":"response.completed","response":complete(json!(messages))}));
            let chunks = events
                .into_iter()
                .enumerate()
                .map(|(sequence, mut event)| {
                    event["sequence_number"] = json!(sequence);
                    ResponseChunk::text(format!("data: {event}\n\n"))
                })
                .collect();
            ScriptedResponse::ok(chunks).with_header("content-type", "text/event-stream")
        };
        let server =
            CassetteServer::start(vec![HttpExchange::new("POST", "/v1/responses", response)])
                .unwrap();
        let response = client(&server)
            .invoke(request().response_mode(mode), ModelCallContext::new())
            .await
            .unwrap();
        assert_eq!(response.text(), "checkingdone");
        let history = response.assistant_message().unwrap();
        let history: Message =
            serde_json::from_value(serde_json::to_value(history).unwrap()).unwrap();
        let replay = encode_request(&request().message(history)).unwrap();
        assert_eq!(replay["input"][1], messages[0]);
        assert_eq!(replay["input"][2], messages[1]);
        server.assert_finished().unwrap();
    }
}

#[tokio::test]
async fn namespaced_calls_are_qualified_and_replay_the_wire_name() {
    let call = json!({"type":"function_call","id":"fc_test","call_id":"call_test","namespace":"crm","name":"lookup","arguments":"{}","status":"completed","async":true});
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1/responses",
        ScriptedResponse::json(200, &complete(json!([call]))).unwrap(),
    )])
    .unwrap();
    let response = client(&server)
        .invoke(
            request().response_mode(ResponseMode::Complete),
            ModelCallContext::new(),
        )
        .await
        .unwrap();
    let ContentPart::ToolCall(tool) = &response.content[0] else {
        panic!("expected a tool call");
    };
    assert_eq!(tool.name, "crm.lookup");
    assert_eq!(tool.metadata["openai.async"], true);
    let replay = encode_request(&request().message(response.assistant_message().unwrap())).unwrap();
    assert_eq!(replay["input"][1], call);
    server.assert_finished().unwrap();
}

#[test]
fn namespaced_tool_definitions_keep_dispatch_and_discovery_consistent() {
    let mut request = request();
    request.tools.push(ToolSpec {
        name: "crm.lookup".into(),
        description: "lookup".into(),
        input_schema: json!({"type":"object","properties":{}}),
        output_schema: None,
        metadata: BTreeMap::from([
            ("openai.namespace".into(), json!("crm")),
            ("openai.defer_loading".into(), json!(true)),
            ("openai.allowed_callers".into(), json!(["programmatic"])),
        ]),
    });
    let body = encode_request(&request).unwrap();
    assert_eq!(body["tools"][0]["type"], "namespace");
    assert_eq!(body["tools"][0]["name"], "crm");
    assert_eq!(body["tools"][0]["tools"][0]["name"], "lookup");
    assert_eq!(body["tools"][0]["tools"][0]["defer_loading"], true);
    request.tool_choice = runifold_model::ToolChoice::Named {
        name: "crm.lookup".into(),
    };
    assert_eq!(
        encode_request(&request).unwrap_err().kind,
        runifold_model::ModelErrorKind::UnsupportedFeature
    );
}

#[tokio::test]
async fn background_lifecycle_retrieves_the_same_result_and_compaction_preserves_every_item() {
    let window = json!([{"role":"user","type":"message","content":[{"type":"input_text","text":"retained"}]},{"type":"compaction","id":"cmp_item","encrypted_content":"private-window"}]);
    let server = CassetteServer::start(vec![
        HttpExchange::new(
            "POST",
            "/v1/responses",
            ScriptedResponse::json(200, &json!({"id":"resp_test","status":"queued"})).unwrap(),
        ),
        HttpExchange::new(
            "GET",
            "/v1/responses/resp_test",
            ScriptedResponse::json(
                200,
                &complete(json!([message("msg_final", "final_answer", "done")])),
            )
            .unwrap(),
        ),
        HttpExchange::new(
            "POST",
            "/v1/responses/resp_test/cancel",
            ScriptedResponse::json(200, &json!({"id":"resp_test","status":"cancelled"})).unwrap(),
        ),
        HttpExchange::new(
            "DELETE",
            "/v1/responses/resp_test",
            ScriptedResponse::json(200, &json!({"id":"resp_test","deleted":true})).unwrap(),
        ),
        HttpExchange::new(
            "POST",
            "/v1/responses/compact",
            ScriptedResponse::json(
                200,
                &json!({"id":"cmp_test","object":"response.compaction","output":window}),
            )
            .unwrap(),
        ),
    ])
    .unwrap();
    let client = client(&server);
    let control = client.control_plane();
    let pending = control
        .create_background_response(request(), ModelCallContext::new())
        .await
        .unwrap();
    assert_eq!(pending.status(), &OpenAiResponseStatus::Queued);
    assert!(pending.model_response().is_err());
    let completed = control
        .get_response(pending.id(), ModelCallContext::new())
        .await
        .unwrap();
    assert_eq!(completed.model_response().unwrap().text(), "done");
    assert_eq!(
        control
            .cancel_response(pending.id(), ModelCallContext::new())
            .await
            .unwrap()
            .status(),
        &OpenAiResponseStatus::Cancelled
    );
    control
        .delete_response(pending.id(), ModelCallContext::new())
        .await
        .unwrap();
    let compacted = control
        .compact_response(request(), ModelCallContext::new())
        .await
        .unwrap();
    assert!(!format!("{compacted:?}").contains("private-window"));
    let next = ModelRequest::new(
        ModelRef::new("openai", "gpt-test"),
        compacted.input_message().unwrap(),
    );
    assert_eq!(encode_request(&next).unwrap()["input"], window);
    let observed = server.observed_requests();
    assert_eq!(observed[0].json_body().unwrap()["background"], true);
    assert_eq!(observed[0].json_body().unwrap()["stream"], false);
    server.assert_finished().unwrap();
}

#[tokio::test]
async fn resumed_background_events_use_the_cursor_without_a_new_post() {
    use futures_util::StreamExt;
    let payload = json!({"type":"response.completed","sequence_number":9,"response":complete(json!([message("msg_done","final_answer","done")]))});
    let server = CassetteServer::start(vec![HttpExchange::new(
        "GET",
        "/v1/responses/resp_test?stream=true&starting_after=8",
        ScriptedResponse::ok(vec![ResponseChunk::text(format!("data: {payload}\n\n"))])
            .with_header("content-type", "text/event-stream"),
    )])
    .unwrap();
    let mut stream = client(&server)
        .control_plane()
        .stream_response("resp_test", Some(8), ModelCallContext::new())
        .await
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap()["sequence_number"], 9);
    assert!(stream.next().await.is_none());
    server.assert_finished().unwrap();
    assert_eq!(server.observed_requests().len(), 1);
}

#[tokio::test]
async fn new_models_reject_incompatible_parameters_before_transport() {
    use runifold_providers::openai::encode_chat_request;
    let server = CassetteServer::start(Vec::new()).unwrap();
    let client = client(&server);
    for (model, effort) in [
        ("gpt-6-astra", "none"),
        ("gpt-6.1-sol", "minimal"),
        ("gpt-6-sol", "medium"),
    ] {
        let mut request = ModelRequest::new(ModelRef::new("openai", model), Message::user("hello"));
        request
            .provider_options
            .insert("openai".into(), json!({"reasoning":{"effort":effort}}));
        if model == "gpt-6-sol" {
            request.generation.temperature = Some(0.2);
        }
        let error = client
            .invoke(request, ModelCallContext::new())
            .await
            .unwrap_err();
        assert_eq!(error.kind, runifold_model::ModelErrorKind::InvalidRequest);
    }
    let mut request = ModelRequest::new(
        ModelRef::new("openai", "gpt-6-luna"),
        Message::user("hello"),
    );
    request
        .provider_options
        .insert("openai".into(), json!({"reasoning_effort":"none"}));
    request.generation.temperature = Some(0.2);
    assert!(encode_chat_request(&request, "openai").is_ok());
    assert_eq!(
        server.observed_requests(),
        [] as [runifold_provider_testkit::ObservedRequest; 0]
    );
}

#[tokio::test]
async fn client_search_and_custom_calls_are_actionable_and_keep_their_output_types() {
    for (call, expected_name, expected_arguments, result, expected_output) in [
        (
            json!({"type":"custom_tool_call","id":"ctc_test","call_id":"call_test","name":"code","input":"print(1)","status":"completed","caller":{"type":"direct"}}),
            "code",
            json!("print(1)"),
            json!("1"),
            "custom_tool_call_output",
        ),
        (
            json!({"type":"tool_search_call","execution":"client","call_id":"call_test","arguments":{"goal":"lookup"},"status":"completed"}),
            "tool_search",
            json!({"goal":"lookup"}),
            json!([]),
            "tool_search_output",
        ),
    ] {
        let server = CassetteServer::start(vec![HttpExchange::new(
            "POST",
            "/v1/responses",
            ScriptedResponse::json(200, &complete(json!([call]))).unwrap(),
        )])
        .unwrap();
        let response = client(&server)
            .invoke(
                request().response_mode(ResponseMode::Complete),
                ModelCallContext::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.finish_reason,
            runifold_model::FinishReason::ToolCalls
        );
        let ContentPart::ToolCall(call) = &response.content[0] else {
            panic!("expected actionable local call");
        };
        assert_eq!(call.name, expected_name);
        assert_eq!(call.arguments, expected_arguments);
        let output = runifold_model::ToolResult {
            call_id: call.id.clone(),
            name: Some(call.name.clone()),
            content: Vec::new(),
            structured_content: Some(result),
            is_error: false,
            metadata: call.metadata.clone(),
        };
        let replay = encode_request(
            &request()
                .message(response.assistant_message().unwrap())
                .message(
                    Message::new(
                        runifold_model::Role::Tool,
                        vec![ContentPart::ToolResult(output)],
                    )
                    .unwrap(),
                ),
        )
        .unwrap();
        assert_eq!(replay["input"][2]["type"], expected_output);
        server.assert_finished().unwrap();
    }
}

#[tokio::test]
async fn gpt_image_25_edits_use_json_and_preserve_output_format() {
    use runifold_model::{ImageFormat, ImageGenerationRequest, MediaSource};
    use runifold_providers::openai::OpenAiImageEditRequest;
    let server = CassetteServer::start(vec![HttpExchange::new(
        "POST",
        "/v1/images/edits",
        ScriptedResponse::json(200, &json!({"data":[{"b64_json":"aW1hZ2U="}]})).unwrap(),
    )])
    .unwrap();
    let request = OpenAiImageEditRequest {
        generation: ImageGenerationRequest {
            model: ModelRef::new("openai", "gpt-image-2.5-sunburst-2026-09-08"),
            prompt: "preserve details".into(),
            count: 1,
            size: Some("1024x1024".into()),
            quality: Some("xhigh".into()),
            format: ImageFormat::Webp,
            transparent: true,
        },
        images: vec![MediaSource::Url {
            url: "https://example.com/reference.png".into(),
            media_type: Some("image/png".into()),
        }],
        mask: Some(MediaSource::Url {
            url: "https://example.com/mask.png".into(),
            media_type: Some("image/png".into()),
        }),
    };
    let result = client(&server)
        .edit_image(request, ModelCallContext::new())
        .await
        .unwrap();
    assert_eq!(result.images.len(), 1);
    let body = server.observed_requests()[0].json_body().unwrap();
    assert_eq!(
        body["images"][0]["image_url"],
        "https://example.com/reference.png"
    );
    assert_eq!(body["output_format"], "webp");
    assert_eq!(body["quality"], "xhigh");
    assert_eq!(body["background"], "transparent");
    assert_eq!(body["model"], "gpt-image-2.5-sunburst-2026-09-08");
    assert_eq!(
        body["mask"],
        json!({"image_url":"https://example.com/mask.png"})
    );
    server.assert_finished().unwrap();
}

#[test]
fn client_tool_search_rejects_function_only_metadata_before_transport() {
    // Use valid function metadata so this exercises tool-kind validation rather
    // than being rejected earlier for an unrelated metadata type error.
    for (field, value) in [
        ("namespace", json!("catalog")),
        ("defer_loading", json!(true)),
        ("allowed_callers", json!(["direct"])),
        ("async", json!(true)),
    ] {
        let tool = ToolSpec {
            name: "tool_search".into(),
            description: "find tools".into(),
            input_schema: json!({"type":"object","properties":{}}),
            output_schema: None,
            metadata: BTreeMap::from([
                ("openai.tool_type".into(), json!("tool_search")),
                (format!("openai.{field}"), value),
            ]),
        };
        let error = encode_request(&request().tool(tool)).unwrap_err();
        assert!(
            error
                .message
                .contains("client tool search does not support")
        );
        assert!(error.message.contains(field));
    }
}

#[test]
fn async_result_routing_is_owned_by_the_original_call_and_checked_on_restore() {
    use runifold_providers::openai::OpenAiAsyncToolState;
    let mut state = OpenAiAsyncToolState::new("openai", "session").unwrap();
    state
        .register(
            "session",
            runifold_model::ToolCall {
                id: "call_async".into(),
                name: "lookup".into(),
                arguments: json!({}),
                raw_arguments: None,
                metadata: BTreeMap::from([("openai.async".into(), json!(true))]),
            },
        )
        .unwrap();
    let message = state
        .resolve(
            "session",
            runifold_model::ToolResult {
                call_id: "call_async".into(),
                name: None,
                content: vec![ContentPart::text("done")],
                structured_content: None,
                is_error: false,
                metadata: BTreeMap::from([
                    (
                        "openai.caller".into(),
                        json!({"type":"program","caller_id":"unrelated_program"}),
                    ),
                    ("openai.tool_type".into(), json!("custom_tool_call")),
                    (
                        "openai-compatible.tool_type".into(),
                        json!("custom_tool_call"),
                    ),
                    (
                        "openai-compatible.caller".into(),
                        json!({"type":"program","caller_id":"unrelated_program"}),
                    ),
                ]),
            },
        )
        .unwrap();
    let body = encode_request(&request().message(message)).unwrap();
    assert_eq!(body["input"][1]["type"], "function_call_output");
    assert!(body["input"][1].get("caller").is_none());
    let snapshot = serde_json::to_value(&state).unwrap();
    for (field, value) in [
        (
            "openai.caller",
            json!({"type":"program","caller_id":"unrelated_program"}),
        ),
        ("openai.tool_type", json!("custom_tool_call")),
        ("openai-compatible.tool_type", json!("custom_tool_call")),
        (
            "openai-compatible.caller",
            json!({"type":"program","caller_id":"unrelated_program"}),
        ),
    ] {
        let mut changed = snapshot.clone();
        changed["calls"]["call_async"]["result"]["metadata"][field] = value;
        let restored: OpenAiAsyncToolState = serde_json::from_value(changed).unwrap();
        assert!(restored.validate("session").is_err());
    }
}

#[test]
fn async_jobs_keep_scope_and_original_call_identity_across_restart() {
    use runifold_providers::openai::OpenAiAsyncToolState;
    let call = runifold_model::ToolCall {
        id: "call_async".into(),
        name: "lookup".into(),
        arguments: json!({"id":7}),
        raw_arguments: None,
        metadata: BTreeMap::from([("openai.async".into(), json!(true))]),
    };
    let mut state = OpenAiAsyncToolState::new("openai", "tenant/session").unwrap();
    assert!(state.register("tenant/session", call.clone()).unwrap());
    assert!(!state.register("tenant/session", call.clone()).unwrap());
    let mut changed = call;
    changed.arguments = json!({"id":8});
    assert!(state.register("tenant/session", changed).is_err());
    let mut restored: OpenAiAsyncToolState =
        serde_json::from_value(serde_json::to_value(state).unwrap()).unwrap();
    assert!(restored.validate("other-tenant/session").is_err());
    assert_eq!(restored.pending().count(), 1);
    let result = runifold_model::ToolResult {
        call_id: "call_async".into(),
        name: None,
        content: vec![ContentPart::text("done")],
        structured_content: None,
        is_error: false,
        metadata: BTreeMap::new(),
    };
    let message = restored.resolve("tenant/session", result.clone()).unwrap();
    assert_eq!(
        restored.resolve("tenant/session", result.clone()).unwrap(),
        message
    );
    let mut conflicting = result;
    conflicting.content = vec![ContentPart::text("different")];
    assert!(restored.resolve("tenant/session", conflicting).is_err());
    assert_eq!(restored.pending().count(), 0);
}

#[tokio::test]
async fn custom_streams_preserve_text_input_and_reject_conflicting_final_input() {
    for final_input in ["print(\"你好\")", "different"] {
        let item = json!({"type":"custom_tool_call","id":"ctc_stream","call_id":"call_stream","name":"code","input":final_input,"status":"completed"});
        let events = [
            json!({"type":"response.created","response":{"id":"resp_test","model":"gpt-test"}}),
            json!({"type":"response.output_item.added","output_index":0,"item":{"type":"custom_tool_call","id":"ctc_stream","call_id":"call_stream","name":"code","input":"","status":"in_progress"}}),
            json!({"type":"response.custom_tool_call_input.delta","output_index":0,"item_id":"ctc_stream","delta":"print("}),
            json!({"type":"response.custom_tool_call_input.done","output_index":0,"item_id":"ctc_stream","input":"print(\"你好\")"}),
            json!({"type":"response.output_item.done","output_index":0,"item":item}),
            json!({"type":"response.completed","response":complete(json!([item]))}),
        ];
        let chunks = events
            .into_iter()
            .enumerate()
            .map(|(sequence, mut event)| {
                event["sequence_number"] = json!(sequence);
                let chunk = ResponseChunk::text(format!("data: {event}\n\n"));
                if final_input == "different" && sequence == 5 {
                    chunk.after(std::time::Duration::from_millis(100))
                } else {
                    chunk
                }
            })
            .collect();
        let server = CassetteServer::start(vec![HttpExchange::new(
            "POST",
            "/v1/responses",
            ScriptedResponse::ok(chunks).with_header("content-type", "text/event-stream"),
        )])
        .unwrap();
        let result = client(&server)
            .invoke(
                request().response_mode(ResponseMode::Streaming),
                ModelCallContext::new(),
            )
            .await;
        if final_input == "different" {
            assert_eq!(
                result.unwrap_err().kind,
                runifold_model::ModelErrorKind::Protocol
            );
            // Rejecting item.done closes the response before the final chunk.
            // The request must match; a successful trailing write is not required.
            server.assert_finished().unwrap();
        } else {
            let response = result.unwrap();
            let ContentPart::ToolCall(call) = &response.content[0] else {
                panic!("expected custom tool call");
            };
            assert_eq!(call.arguments, json!(final_input));
            assert_eq!(call.metadata["openai.tool_type"], "custom_tool_call");
            server
                .wait_until_finished(std::time::Duration::from_secs(5))
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn cursor_and_remote_response_identity_drift_are_rejected() {
    use futures_util::StreamExt;
    for (sequence, id) in [(8, "resp_test"), (9, "resp_other")] {
        let payload =
            json!({"type":"response.completed","sequence_number":sequence,"response":{"id":id}});
        let server = CassetteServer::start(vec![HttpExchange::new(
            "GET",
            "/v1/responses/resp_test?stream=true&starting_after=8",
            ScriptedResponse::ok(vec![ResponseChunk::text(format!("data: {payload}\n\n"))])
                .with_header("content-type", "text/event-stream"),
        )])
        .unwrap();
        let mut stream = client(&server)
            .control_plane()
            .stream_response("resp_test", Some(8), ModelCallContext::new())
            .await
            .unwrap();
        assert!(stream.next().await.unwrap().is_err());
        server
            .wait_until_finished(std::time::Duration::from_secs(5))
            .await
            .unwrap();
    }
}

#[test]
fn streamed_tool_identity_cannot_switch_namespace_at_completion() {
    use runifold_providers::openai::OpenAiEventDecoder;
    let mut decoder = OpenAiEventDecoder::new();
    decoder
        .decode(json!({"type":"response.created","response":{"id":"resp_test","model":"gpt-test"}}))
        .unwrap();
    decoder.decode(json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_test","call_id":"call_test","namespace":"crm","name":"lookup","arguments":"","status":"in_progress"}})).unwrap();
    let error = decoder.decode(json!({"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","id":"fc_test","call_id":"call_test","namespace":"payments","name":"lookup","arguments":"{}","status":"completed"}})).unwrap_err();
    assert_eq!(error.kind, runifold_model::ModelErrorKind::Protocol);
}

#[tokio::test]
async fn background_stream_creation_and_reconnect_preserve_storage_policy_and_identity() {
    use futures_util::StreamExt;
    let created = json!({"type":"response.created","sequence_number":0,"response":{"id":"resp_test","model":"gpt-test","status":"queued"}});
    let completed = json!({"type":"response.completed","sequence_number":5,"response":complete(json!([message("msg_done","final_answer","done")]))});
    let server = CassetteServer::start(vec![
        HttpExchange::new(
            "POST",
            "/v1/responses",
            ScriptedResponse::ok(vec![ResponseChunk::text(format!("data: {created}\n\n"))])
                .with_header("content-type", "text/event-stream"),
        ),
        HttpExchange::new(
            "GET",
            "/v1/responses/resp_test?stream=true&starting_after=0",
            ScriptedResponse::ok(vec![ResponseChunk::text(format!("data: {completed}\n\n"))])
                .with_header("content-type", "text/event-stream"),
        ),
    ])
    .unwrap();
    let client = client(&server);
    let control = client.control_plane();
    let accepted = control
        .create_background_stream(
            request().provider_option("openai", json!({"store":false})),
            ModelCallContext::new(),
        )
        .await
        .unwrap();
    let id = accepted.id().to_owned();
    let mut events = accepted.into_events();
    let first = events.next().await.unwrap().unwrap();
    let cursor = first["sequence_number"].as_u64().unwrap();
    drop(events);
    let mut resumed = control
        .stream_response(&id, Some(cursor), ModelCallContext::new())
        .await
        .unwrap();
    assert_eq!(
        resumed.next().await.unwrap().unwrap()["type"],
        "response.completed"
    );
    let body = server.observed_requests()[0].json_body().unwrap();
    assert_eq!(body["background"], true);
    assert_eq!(body["stream"], true);
    assert_eq!(body["store"], false);
    assert_eq!(server.observed_requests().len(), 2);
    server.assert_finished().unwrap();
}
