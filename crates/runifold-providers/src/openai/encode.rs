//! `OpenAI` Responses request encoding.

use serde_json::{Map, Value, json};

use crate::content_projection::{
    encode_content_envelope, validate_inline_media, validate_media_url,
    validate_optional_media_type,
};

use runifold_model::{
    ContentPart, MediaSource, ModelError, ModelErrorKind, ModelRequest, OutputFormat, ResponseMode,
    Role, ToolChoice, ToolResult,
};

/// Encodes a canonical request as an `OpenAI` Responses API request.
///
/// # Errors
///
/// Returns [`ModelError`] when a canonical feature has no lossless Responses
/// API representation, or when provider options try to replace fields owned by
/// the adapter.
pub fn encode_request(request: &ModelRequest) -> Result<Value, ModelError> {
    encode_request_for(request, "openai")
}

pub(crate) fn encode_request_for(
    request: &ModelRequest,
    provider: &str,
) -> Result<Value, ModelError> {
    if request.messages.is_empty() {
        return Err(invalid("a model request must contain at least one message"));
    }
    if request.generation.seed.is_some() {
        return Err(unsupported(
            "Responses API adapter does not support deterministic seed",
        ));
    }
    if !request.generation.stop.is_empty() {
        return Err(unsupported(
            "Responses API adapter does not support stop sequences",
        ));
    }

    let mut body = Map::new();
    body.insert("model".into(), Value::String(request.model.name.clone()));
    body.insert("input".into(), Value::Array(encode_input(request)?));
    body.insert(
        "stream".into(),
        Value::Bool(matches!(
            request.selected_response_mode(),
            ResponseMode::Streaming
        )),
    );

    insert_optional(
        &mut body,
        "temperature",
        request.generation.temperature.map(Value::from),
    );
    insert_optional(
        &mut body,
        "top_p",
        request.generation.top_p.map(Value::from),
    );
    insert_optional(
        &mut body,
        "max_output_tokens",
        request.generation.max_output_tokens.map(Value::from),
    );

    let provider_tools = encode_provider_tools(request, provider)?;
    if !request.tools.is_empty() || !provider_tools.is_empty() {
        let mut tools = request
            .tools
            .iter()
            .map(|tool| {
                json!({
                    "type": "function",
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.input_schema,
                    "strict": true
                })
            })
            .collect::<Vec<_>>();
        tools.extend(provider_tools);
        body.insert("tools".into(), Value::Array(tools));
    }
    body.insert(
        "tool_choice".into(),
        encode_tool_choice(&request.tool_choice)?,
    );
    body.insert("text".into(), encode_output_format(&request.output_format)?);
    merge_provider_options(&mut body, request, provider)?;

    Ok(Value::Object(body))
}

fn encode_provider_tools(request: &ModelRequest, provider: &str) -> Result<Vec<Value>, ModelError> {
    request
        .provider_tools()
        .into_iter()
        .map(|tool| {
            if tool.provider != provider && tool.provider != "openai-compatible" {
                return Err(invalid(format!(
                    "provider-native tool for `{}` cannot be sent to `{provider}`",
                    tool.provider
                )));
            }
            if tool.options.contains_key("type") {
                return Err(invalid(
                    "provider-native tool option `type` conflicts with its declared tool type",
                ));
            }
            let mut encoded =
                Map::from_iter([("type".into(), Value::String(tool.tool_type.clone()))]);
            encoded.extend(tool.options.clone());
            Ok(Value::Object(encoded))
        })
        .collect()
}

fn encode_input(request: &ModelRequest) -> Result<Vec<Value>, ModelError> {
    let mut input = Vec::new();
    for message in &request.messages {
        let mut message_content = Vec::new();
        for part in &message.content {
            match part {
                ContentPart::Text { text } => {
                    let part_type = if message.role == Role::Assistant {
                        "output_text"
                    } else {
                        "input_text"
                    };
                    message_content.push(json!({"type": part_type, "text": text}));
                }
                ContentPart::Image { source } => {
                    require_input_role(message.role, "image")?;
                    message_content.push(encode_image(source, &request.model.provider)?);
                }
                ContentPart::Document { source, name } => {
                    require_input_role(message.role, "document")?;
                    message_content.push(encode_document(
                        source,
                        name.as_deref(),
                        &request.model.provider,
                    )?);
                }
                ContentPart::ToolCall(call) => {
                    flush_message(&mut input, message.role, &mut message_content)?;
                    let arguments = call
                        .raw_arguments
                        .clone()
                        .unwrap_or_else(|| call.arguments.to_string());
                    input.push(encode_function_call(
                        call,
                        arguments,
                        &request.model.provider,
                    )?);
                }
                ContentPart::ToolResult(result) => {
                    flush_message(&mut input, message.role, &mut message_content)?;
                    input.push(encode_tool_result(result, &request.model.provider)?);
                }
                ContentPart::ProviderOpaque(data)
                    if (data.provider == request.model.provider
                        || data.provider == "openai-compatible")
                        && data.kind == "input_item" =>
                {
                    flush_message(&mut input, message.role, &mut message_content)?;
                    input.push(data.value.clone());
                }
                ContentPart::Audio { .. } | ContentPart::ResourceLink { .. } => {
                    require_input_role(message.role, "projected rich content")?;
                    message_content.push(json!({
                        "type": "input_text",
                        "text": encode_content_envelope(part)?
                    }));
                }
                ContentPart::Reasoning(_) => {
                    return Err(unsupported(
                        "reasoning round trips require an OpenAI input_item escape hatch",
                    ));
                }
                ContentPart::Refusal { .. } => {
                    return Err(unsupported(
                        "refusal round trips are not accepted as generic message input",
                    ));
                }
                ContentPart::Citation(_) => {
                    return Err(unsupported("citations cannot be sent as message input"));
                }
                ContentPart::ProviderOpaque(_) => {
                    return Err(unsupported(
                        "opaque content belongs to another provider or unsupported OpenAI kind",
                    ));
                }
                _ => {
                    return Err(unsupported(
                        "content variant is newer than this OpenAI adapter",
                    ));
                }
            }
        }
        flush_message(&mut input, message.role, &mut message_content)?;
    }
    Ok(input)
}

fn encode_function_call(
    call: &runifold_model::ToolCall,
    arguments: String,
    provider: &str,
) -> Result<Value, ModelError> {
    let mut item = Map::from_iter([
        ("type".into(), Value::String("function_call".into())),
        ("call_id".into(), Value::String(call.id.clone())),
        ("name".into(), Value::String(call.name.clone())),
        ("arguments".into(), Value::String(arguments)),
        ("status".into(), Value::String("completed".into())),
    ]);
    if let Some(id) = provider_metadata_string(&call.metadata, provider, "id")? {
        item.insert("id".into(), Value::String(id));
    }
    if let Some(status) = provider_metadata_string(&call.metadata, provider, "status")?
        && status != "completed"
    {
        return Err(invalid(format!(
            "completed tool call `{}` cannot replay provider status `{status}`",
            call.id
        )));
    }
    Ok(Value::Object(item))
}

fn provider_metadata_string(
    metadata: &runifold_model::ExtensionMap,
    provider: &str,
    field: &str,
) -> Result<Option<String>, ModelError> {
    let compatible_key = format!("openai-compatible.{field}");
    let provider_key = format!("{provider}.{field}");
    let value = metadata
        .get(&provider_key)
        .or_else(|| metadata.get(&compatible_key));
    value
        .map(|value| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                invalid(format!(
                    "tool-call metadata `{provider_key}` must be a string"
                ))
            })
        })
        .transpose()
}

fn flush_message(
    input: &mut Vec<Value>,
    role: Role,
    content: &mut Vec<Value>,
) -> Result<(), ModelError> {
    if content.is_empty() {
        return Ok(());
    }
    let mut message = json!({
        "type": "message",
        "role": role_name(role)?,
        "content": std::mem::take(content)
    });
    if role == Role::Assistant {
        message["status"] = Value::String("completed".into());
    }
    input.push(message);
    Ok(())
}

fn encode_image(source: &MediaSource, provider: &str) -> Result<Value, ModelError> {
    if let MediaSource::ProviderFile {
        provider: owner,
        file_id,
    } = source
    {
        require_provider_file_owner(owner, provider)?;
        return Ok(json!({"type": "input_image", "file_id": file_id}));
    }
    let image_url = match source {
        MediaSource::Url { url, media_type } => {
            validate_media_url(url, &["http", "https"])?;
            validate_optional_media_type(media_type.as_deref())?;
            url.clone()
        }
        MediaSource::Base64 { media_type, data } => {
            validate_inline_media(media_type, data)?;
            format!("data:{media_type};base64,{data}")
        }
        MediaSource::Artifact { .. } => {
            return Err(unsupported(
                "artifact images must be resolved before provider invocation",
            ));
        }
        _ => {
            return Err(unsupported(
                "image source is newer than this OpenAI adapter",
            ));
        }
    };
    Ok(json!({"type": "input_image", "image_url": image_url}))
}

fn encode_document(
    source: &MediaSource,
    name: Option<&str>,
    provider: &str,
) -> Result<Value, ModelError> {
    match source {
        MediaSource::Url { url, media_type } => {
            validate_media_url(url, &["http", "https"])?;
            validate_optional_media_type(media_type.as_deref())?;
            Ok(json!({
                "type": "input_file",
                "file_url": url
            }))
        }
        MediaSource::Base64 { media_type, data } => {
            validate_inline_media(media_type, data)?;
            Ok(json!({
                "type": "input_file",
                "filename": name.unwrap_or("document"),
                "file_data": format!("data:{media_type};base64,{data}")
            }))
        }
        MediaSource::Artifact { .. } => Err(unsupported(
            "artifact documents must be resolved before provider invocation",
        )),
        MediaSource::ProviderFile {
            provider: owner,
            file_id,
        } => {
            require_provider_file_owner(owner, provider)?;
            Ok(json!({"type": "input_file", "file_id": file_id}))
        }
        _ => Err(unsupported(
            "document source is newer than this OpenAI adapter",
        )),
    }
}

fn require_provider_file_owner(owner: &str, provider: &str) -> Result<(), ModelError> {
    if owner == provider || owner == "openai-compatible" {
        Ok(())
    } else {
        Err(invalid(format!(
            "provider file owned by `{owner}` cannot be sent to `{provider}`"
        )))
    }
}

fn encode_tool_result(result: &ToolResult, provider: &str) -> Result<Value, ModelError> {
    let mut text = Vec::new();
    let mut rich = Vec::new();
    for part in &result.content {
        match part {
            ContentPart::Text { text: value } => text.push(value.clone()),
            ContentPart::Image { source } => match encode_image(source, provider) {
                Ok(image) => rich.push(image),
                Err(error) if error.kind == ModelErrorKind::UnsupportedFeature => {
                    text.push(encode_content_envelope(part)?);
                }
                Err(error) => return Err(error),
            },
            ContentPart::Document { source, name } => {
                match encode_document(source, name.as_deref(), provider) {
                    Ok(document) => rich.push(document),
                    Err(error) if error.kind == ModelErrorKind::UnsupportedFeature => {
                        text.push(encode_content_envelope(part)?);
                    }
                    Err(error) => return Err(error),
                }
            }
            ContentPart::ResourceLink {
                uri,
                name,
                media_type,
                ..
            } => {
                text.push(encode_content_envelope(part)?);
                let source = MediaSource::Url {
                    url: uri.clone(),
                    media_type: media_type.clone(),
                };
                if media_type
                    .as_deref()
                    .is_some_and(|kind| kind.starts_with("image/"))
                {
                    if let Ok(image) = encode_image(&source, provider) {
                        rich.push(image);
                    }
                } else if let Ok(document) = encode_document(&source, Some(name), provider) {
                    rich.push(document);
                }
            }
            _ => text.push(encode_content_envelope(part)?),
        }
    }
    if let Some(structured) = &result.structured_content {
        let encoded = structured.to_string();
        if !text.iter().any(|item| item == &encoded) {
            text.push(encoded);
        }
    }
    if result.is_error {
        text.insert(0, "Tool execution reported an application error.".into());
    }
    let output = if rich.is_empty() {
        Value::String(text.join("\n"))
    } else {
        let mut content = text
            .into_iter()
            .map(|text| json!({"type":"input_text","text":text}))
            .collect::<Vec<_>>();
        content.extend(rich);
        Value::Array(content)
    };
    Ok(json!({
        "type": "function_call_output",
        "call_id": result.call_id,
        "output": output
    }))
}

fn encode_tool_choice(choice: &ToolChoice) -> Result<Value, ModelError> {
    Ok(match choice {
        ToolChoice::Auto => Value::String("auto".into()),
        ToolChoice::None => Value::String("none".into()),
        ToolChoice::Required => Value::String("required".into()),
        ToolChoice::Named { name } => json!({"type": "function", "name": name}),
        _ => {
            return Err(unsupported("tool choice is newer than this OpenAI adapter"));
        }
    })
}

fn encode_output_format(format: &OutputFormat) -> Result<Value, ModelError> {
    let format = match format {
        OutputFormat::Text => json!({"type": "text"}),
        OutputFormat::Json => json!({"type": "json_object"}),
        OutputFormat::JsonSchema {
            name,
            schema,
            strict,
        } => json!({
            "type": "json_schema",
            "name": name,
            "schema": schema,
            "strict": strict
        }),
        _ => {
            return Err(unsupported(
                "output format is newer than this OpenAI adapter",
            ));
        }
    };
    Ok(json!({"format": format}))
}

fn merge_provider_options(
    body: &mut Map<String, Value>,
    request: &ModelRequest,
    provider: &str,
) -> Result<(), ModelError> {
    for namespace in ["openai-compatible", provider] {
        let Some(options) = request.provider_options.get(namespace) else {
            continue;
        };
        let options = options
            .as_object()
            .ok_or_else(|| invalid(format!("provider_options.{namespace} must be an object")))?;
        for (key, value) in options {
            if body.contains_key(key) {
                return Err(invalid(format!(
                    "provider option `{key}` conflicts with an adapter-owned field"
                )));
            }
            body.insert(key.clone(), value.clone());
        }
    }
    Ok(())
}

fn require_input_role(role: Role, content: &str) -> Result<(), ModelError> {
    if matches!(role, Role::User | Role::System) {
        Ok(())
    } else {
        Err(unsupported(format!(
            "{content} content is only supported on input roles"
        )))
    }
}

fn role_name(role: Role) -> Result<&'static str, ModelError> {
    Ok(match role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => {
            return Err(unsupported(
                "tool messages must contain canonical tool-result parts",
            ));
        }
        _ => return Err(unsupported("role is newer than this OpenAI adapter")),
    })
}

fn insert_optional(body: &mut Map<String, Value>, name: &str, value: Option<Value>) {
    if let Some(value) = value {
        body.insert(name.into(), value);
    }
}

fn invalid(message: impl Into<String>) -> ModelError {
    ModelError::local(ModelErrorKind::InvalidRequest, message)
}

fn unsupported(message: impl Into<String>) -> ModelError {
    ModelError::local(ModelErrorKind::UnsupportedFeature, message)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use runifold_model::{
        ContentPart, MediaSource, Message, ModelErrorKind, ModelRef, ModelRequest, OutputFormat,
        ProviderToolSpec, ResponseMode, Role, ToolCall, ToolResult, ToolSpec,
    };

    use super::encode_request;

    use crate::content_projection::decode_content_envelope;

    #[test]
    fn encodes_text_tools_and_structured_output() {
        let request = ModelRequest::new(
            ModelRef::new("openai", "test-model"),
            Message::user("weather?"),
        )
        .tool(ToolSpec {
            name: "weather".into(),
            description: "Get weather".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {"city": {"type": "string"}},
                "required": ["city"],
                "additionalProperties": false
            }),
            output_schema: None,
            metadata: BTreeMap::new(),
        })
        .output_format(OutputFormat::JsonSchema {
            name: "answer".into(),
            schema: serde_json::json!({"type": "object"}),
            strict: true,
        });

        let encoded = encode_request(&request).unwrap();

        assert_eq!(encoded["model"], "test-model");
        assert_eq!(encoded["stream"], true);
        assert_eq!(encoded["tools"][0]["name"], "weather");
        assert_eq!(encoded["text"]["format"]["type"], "json_schema");
        assert_eq!(encoded["input"][0]["content"][0]["type"], "input_text");
    }

    #[test]
    fn preserves_tool_call_identity_and_raw_arguments() {
        let message = Message::new(
            Role::Assistant,
            vec![ContentPart::ToolCall(ToolCall {
                id: "call-7".into(),
                name: "lookup".into(),
                arguments: serde_json::json!({"value": 7}),
                raw_arguments: Some("{ \"value\": 7 }".into()),
                metadata: BTreeMap::from([
                    ("openai.id".into(), serde_json::json!("fc_7")),
                    ("openai.status".into(), serde_json::json!("completed")),
                ]),
            })],
        )
        .unwrap();
        let request = ModelRequest::new(ModelRef::new("openai", "model"), message);

        let encoded = encode_request(&request).unwrap();

        assert_eq!(encoded["input"][0]["call_id"], "call-7");
        assert_eq!(encoded["input"][0]["id"], "fc_7");
        assert_eq!(encoded["input"][0]["status"], "completed");
        assert_eq!(encoded["input"][0]["arguments"], "{ \"value\": 7 }");
    }

    #[test]
    fn legacy_tool_calls_replay_as_completed() {
        let message = Message::new(
            Role::Assistant,
            vec![ContentPart::ToolCall(ToolCall {
                id: "call-legacy".into(),
                name: "lookup".into(),
                arguments: serde_json::json!({}),
                raw_arguments: Some("{}".into()),
                metadata: BTreeMap::new(),
            })],
        )
        .unwrap();

        let encoded =
            encode_request(&ModelRequest::new(ModelRef::new("ark", "doubao"), message)).unwrap();

        assert_eq!(encoded["input"][0]["status"], "completed");
        assert!(encoded["input"][0].get("id").is_none());
    }

    #[test]
    fn assistant_messages_replay_with_required_completion_status() {
        let request = ModelRequest::new(
            ModelRef::new("openai", "model"),
            Message::new(Role::Assistant, vec![ContentPart::text("previous answer")]).unwrap(),
        );

        let encoded = encode_request(&request).unwrap();

        assert_eq!(encoded["input"][0]["type"], "message");
        assert_eq!(encoded["input"][0]["role"], "assistant");
        assert_eq!(encoded["input"][0]["status"], "completed");
    }

    #[test]
    fn rejects_replaying_an_incomplete_canonical_tool_call() {
        let message = Message::new(
            Role::Assistant,
            vec![ContentPart::ToolCall(ToolCall {
                id: "call-incomplete".into(),
                name: "lookup".into(),
                arguments: serde_json::json!({}),
                raw_arguments: Some("{}".into()),
                metadata: BTreeMap::from([("ark.status".into(), serde_json::json!("incomplete"))]),
            })],
        )
        .unwrap();

        let error = encode_request(&ModelRequest::new(ModelRef::new("ark", "doubao"), message))
            .unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::InvalidRequest);
    }

    #[test]
    fn function_outputs_preserve_images_files_and_structured_content() {
        let message = Message::new(
            Role::Tool,
            vec![ContentPart::ToolResult(ToolResult {
                call_id: "call-rich".into(),
                name: Some("render".into()),
                content: vec![
                    ContentPart::Image {
                        source: MediaSource::Base64 {
                            media_type: "image/png".into(),
                            data: "cG5n".into(),
                        },
                    },
                    ContentPart::Document {
                        source: MediaSource::Url {
                            url: "https://example.com/report.pdf".into(),
                            media_type: Some("application/pdf".into()),
                        },
                        name: Some("report.pdf".into()),
                    },
                ],
                structured_content: Some(serde_json::json!({"count":2})),
                is_error: false,
                metadata: BTreeMap::new(),
            })],
        )
        .unwrap();
        let body = encode_request(&ModelRequest::new(
            ModelRef::new("openai", "vision"),
            message,
        ))
        .unwrap();

        assert_eq!(body["input"][0]["type"], "function_call_output");
        assert_eq!(body["input"][0]["output"][0]["type"], "input_text");
        assert_eq!(body["input"][0]["output"][1]["type"], "input_image");
        assert_eq!(body["input"][0]["output"][2]["type"], "input_file");
    }

    #[test]
    fn function_outputs_bridge_audio_without_rejecting_the_request() {
        let message = Message::new(
            Role::Tool,
            vec![ContentPart::ToolResult(ToolResult {
                call_id: "call-audio".into(),
                name: Some("listen".into()),
                content: vec![ContentPart::Audio {
                    source: MediaSource::Base64 {
                        media_type: "audio/wav".into(),
                        data: "UklGRg==".into(),
                    },
                }],
                structured_content: None,
                is_error: false,
                metadata: BTreeMap::new(),
            })],
        )
        .unwrap();

        let body = encode_request(&ModelRequest::new(
            ModelRef::new("openai", "model"),
            message,
        ))
        .unwrap();
        let envelope = body["input"][0]["output"].as_str().unwrap();

        assert!(envelope.contains("runifold.content.v1"));
        assert!(envelope.contains("audio/wav"));
        assert!(envelope.contains("UklGRg=="));
    }

    #[test]
    fn ordinary_audio_input_uses_the_bounded_projection() {
        let message = Message::new(
            Role::User,
            vec![ContentPart::Audio {
                source: MediaSource::Base64 {
                    media_type: "audio/wav".into(),
                    data: "YXVkaW8=".into(),
                },
            }],
        )
        .unwrap();

        let body = encode_request(&ModelRequest::new(
            ModelRef::new("openai", "model"),
            message,
        ))
        .unwrap();
        let envelope = body["input"][0]["content"][0]["text"].as_str().unwrap();

        assert!(decode_content_envelope(envelope).unwrap().is_some());
    }

    #[test]
    fn native_image_input_rejects_invalid_base64() {
        let message = Message::new(
            Role::User,
            vec![ContentPart::Image {
                source: MediaSource::Base64 {
                    media_type: "image/png".into(),
                    data: "not base64".into(),
                },
            }],
        )
        .unwrap();

        let error = encode_request(&ModelRequest::new(
            ModelRef::new("openai", "vision"),
            message,
        ))
        .unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::InvalidRequest);
    }

    #[test]
    fn native_document_input_rejects_credentialed_url() {
        let message = Message::new(
            Role::User,
            vec![ContentPart::Document {
                source: MediaSource::Url {
                    url: "https://user:secret@example.com/report.pdf".into(),
                    media_type: Some("application/pdf".into()),
                },
                name: Some("report.pdf".into()),
            }],
        )
        .unwrap();

        let error = encode_request(&ModelRequest::new(
            ModelRef::new("openai", "model"),
            message,
        ))
        .unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::InvalidRequest);
    }

    #[test]
    fn invalid_provider_file_ownership_is_not_downgraded_to_text() {
        let message = Message::new(
            Role::Tool,
            vec![ContentPart::ToolResult(ToolResult {
                call_id: "call-file".into(),
                name: Some("inspect".into()),
                content: vec![ContentPart::Image {
                    source: MediaSource::provider_file("anthropic", "file-secret"),
                }],
                structured_content: None,
                is_error: false,
                metadata: BTreeMap::new(),
            })],
        )
        .unwrap();

        let error = encode_request(&ModelRequest::new(
            ModelRef::new("openai", "model"),
            message,
        ))
        .unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::InvalidRequest);
    }

    #[test]
    fn never_silently_ignores_unsupported_options() {
        let mut request =
            ModelRequest::new(ModelRef::new("openai", "model"), Message::user("hello"));
        request.generation.stop.push("STOP".into());

        let error = encode_request(&request).unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::UnsupportedFeature);
    }

    #[test]
    fn adapter_owned_fields_cannot_be_overridden() {
        let mut request =
            ModelRequest::new(ModelRef::new("openai", "model"), Message::user("hello"));
        request
            .provider_options
            .insert("openai".into(), serde_json::json!({"model": "other"}));

        let error = encode_request(&request).unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::InvalidRequest);
    }

    #[test]
    fn compatible_parallel_tool_policy_is_forwarded_explicitly() {
        let request = ModelRequest::new(ModelRef::new("openai", "model"), Message::user("hello"))
            .provider_option(
                "openai-compatible",
                serde_json::json!({"parallel_tool_calls": false}),
            );

        let encoded = encode_request(&request).unwrap();

        assert_eq!(encoded["parallel_tool_calls"], false);
    }

    #[test]
    fn ark_native_and_function_tools_share_one_wire_array() {
        let request = ModelRequest::new(ModelRef::new("ark", "doubao"), Message::user("research"))
            .tool(ToolSpec {
                name: "lookup".into(),
                description: "lookup".into(),
                input_schema: serde_json::json!({"type": "object"}),
                output_schema: None,
                metadata: BTreeMap::new(),
            })
            .provider_tool(
                ProviderToolSpec::new("ark", "web_search")
                    .unwrap()
                    .option("limit", 8_u32)
                    .option("max_keyword", 5_u32),
            );

        let encoded = super::encode_request_for(&request, "ark").unwrap();

        assert_eq!(encoded["tools"][0]["type"], "function");
        assert_eq!(encoded["tools"][1]["type"], "web_search");
        assert_eq!(encoded["tools"][1]["limit"], 8);
    }

    #[test]
    fn complete_mode_and_provider_file_ids_are_encoded() {
        let message = Message::new(
            Role::User,
            vec![
                ContentPart::Image {
                    source: MediaSource::provider_file("ark", "file_image"),
                },
                ContentPart::Document {
                    source: MediaSource::provider_file("ark", "file_document"),
                    name: Some("report.pdf".into()),
                },
            ],
        )
        .unwrap();
        let request = ModelRequest::new(ModelRef::new("ark", "doubao"), message)
            .response_mode(ResponseMode::Complete);

        let encoded = super::encode_request_for(&request, "ark").unwrap();

        assert_eq!(encoded["stream"], false);
        assert_eq!(encoded["input"][0]["content"][0]["file_id"], "file_image");
        assert_eq!(
            encoded["input"][0]["content"][1]["file_id"],
            "file_document"
        );
    }
}
