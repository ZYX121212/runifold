//! `OpenAI` Responses request encoding.

use serde_json::{Map, Value, json};

use runifold_model::{
    ContentPart, MediaSource, ModelError, ModelErrorKind, ModelRequest, OutputFormat, Role,
    ToolChoice, ToolResult,
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
    body.insert("stream".into(), Value::Bool(true));

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

    if !request.tools.is_empty() {
        body.insert(
            "tools".into(),
            Value::Array(
                request
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
                    .collect(),
            ),
        );
    }
    body.insert(
        "tool_choice".into(),
        encode_tool_choice(&request.tool_choice)?,
    );
    body.insert("text".into(), encode_output_format(&request.output_format)?);
    merge_provider_options(&mut body, request, provider)?;

    Ok(Value::Object(body))
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
                    message_content.push(encode_image(source)?);
                }
                ContentPart::Document { source, name } => {
                    require_input_role(message.role, "document")?;
                    message_content.push(encode_document(source, name.as_deref())?);
                }
                ContentPart::ToolCall(call) => {
                    flush_message(&mut input, message.role, &mut message_content)?;
                    let arguments = call
                        .raw_arguments
                        .clone()
                        .unwrap_or_else(|| call.arguments.to_string());
                    input.push(json!({
                        "type": "function_call",
                        "call_id": call.id,
                        "name": call.name,
                        "arguments": arguments
                    }));
                }
                ContentPart::ToolResult(result) => {
                    flush_message(&mut input, message.role, &mut message_content)?;
                    input.push(encode_tool_result(result)?);
                }
                ContentPart::ProviderOpaque(data)
                    if data.provider == "openai" && data.kind == "input_item" =>
                {
                    flush_message(&mut input, message.role, &mut message_content)?;
                    input.push(data.value.clone());
                }
                ContentPart::Audio { .. } => {
                    return Err(unsupported(
                        "audio input is not yet implemented by the OpenAI adapter",
                    ));
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

fn flush_message(
    input: &mut Vec<Value>,
    role: Role,
    content: &mut Vec<Value>,
) -> Result<(), ModelError> {
    if content.is_empty() {
        return Ok(());
    }
    input.push(json!({
        "type": "message",
        "role": role_name(role)?,
        "content": std::mem::take(content)
    }));
    Ok(())
}

fn encode_image(source: &MediaSource) -> Result<Value, ModelError> {
    let image_url = match source {
        MediaSource::Url { url, .. } => url.clone(),
        MediaSource::Base64 { media_type, data } => {
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

fn encode_document(source: &MediaSource, name: Option<&str>) -> Result<Value, ModelError> {
    match source {
        MediaSource::Url { url, .. } => Ok(json!({
            "type": "input_file",
            "file_url": url
        })),
        MediaSource::Base64 { media_type, data } => Ok(json!({
            "type": "input_file",
            "filename": name.unwrap_or("document"),
            "file_data": format!("data:{media_type};base64,{data}")
        })),
        MediaSource::Artifact { .. } => Err(unsupported(
            "artifact documents must be resolved before provider invocation",
        )),
        _ => Err(unsupported(
            "document source is newer than this OpenAI adapter",
        )),
    }
}

fn encode_tool_result(result: &ToolResult) -> Result<Value, ModelError> {
    let text = result
        .content
        .iter()
        .map(|part| match part {
            ContentPart::Text { text } => Ok(text.clone()),
            _ => serde_json::to_string(part).map_err(|error| {
                invalid(format!(
                    "failed to encode rich tool result content: {error}"
                ))
            }),
        })
        .collect::<Result<Vec<_>, _>>()?
        .join("\n");
    let output = if result.is_error {
        json!({"error": text}).to_string()
    } else {
        text
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
        ContentPart, Message, ModelErrorKind, ModelRef, ModelRequest, OutputFormat, Role, ToolCall,
        ToolSpec,
    };

    use super::encode_request;

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
                metadata: BTreeMap::new(),
            })],
        )
        .unwrap();
        let request = ModelRequest::new(ModelRef::new("openai", "model"), message);

        let encoded = encode_request(&request).unwrap();

        assert_eq!(encoded["input"][0]["call_id"], "call-7");
        assert_eq!(encoded["input"][0]["arguments"], "{ \"value\": 7 }");
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
}
