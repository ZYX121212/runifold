//! Anthropic Messages request encoding.

use serde_json::{Map, Value, json};

use crate::content_projection::encode_content_envelope;

use runifold_model::{
    ContentPart, MediaSource, ModelError, ModelErrorKind, ModelRequest, OutputFormat, Role,
    ToolChoice, ToolResult,
};

const PROVIDER: &str = "anthropic";

/// Encodes a canonical request as an Anthropic Messages API request.
///
/// `default_max_tokens` is used because Anthropic requires this field while
/// Runifold's provider-neutral request leaves output limits optional.
///
/// # Errors
///
/// Returns [`ModelError`] when the request cannot be represented losslessly or
/// provider options replace adapter-owned protocol fields.
pub fn encode_request(
    request: &ModelRequest,
    default_max_tokens: u64,
) -> Result<Value, ModelError> {
    if request.messages.is_empty() {
        return Err(invalid("a model request must contain at least one message"));
    }
    if request.generation.seed.is_some() {
        return Err(unsupported(
            "Anthropic Messages does not support deterministic seed",
        ));
    }
    if !matches!(request.output_format, OutputFormat::Text) {
        return Err(unsupported(
            "structured output requires a provider-specific strategy and is not enabled",
        ));
    }

    let (system, messages) = encode_messages(request)?;
    let mut body = Map::new();
    body.insert("model".into(), Value::String(request.model.name.clone()));
    body.insert("messages".into(), Value::Array(messages));
    body.insert("stream".into(), Value::Bool(true));
    body.insert(
        "max_tokens".into(),
        Value::from(
            request
                .generation
                .max_output_tokens
                .unwrap_or(default_max_tokens),
        ),
    );
    if !system.is_empty() {
        body.insert("system".into(), Value::Array(system));
    }
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
    if !request.generation.stop.is_empty() {
        body.insert(
            "stop_sequences".into(),
            Value::Array(
                request
                    .generation
                    .stop
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
    }
    if !request.tools.is_empty() && !matches!(request.tool_choice, ToolChoice::None) {
        body.insert(
            "tools".into(),
            Value::Array(
                request
                    .tools
                    .iter()
                    .map(|tool| {
                        json!({
                            "name": tool.name,
                            "description": tool.description,
                            "input_schema": tool.input_schema
                        })
                    })
                    .collect(),
            ),
        );
        body.insert(
            "tool_choice".into(),
            encode_tool_choice(&request.tool_choice)?,
        );
    } else if !matches!(request.tool_choice, ToolChoice::Auto | ToolChoice::None) {
        return Err(invalid("tool choice requires at least one tool"));
    }
    merge_provider_options(&mut body, request)?;
    Ok(Value::Object(body))
}

fn encode_messages(request: &ModelRequest) -> Result<(Vec<Value>, Vec<Value>), ModelError> {
    let mut system = Vec::new();
    let mut messages = Vec::new();
    for message in &request.messages {
        let target = if message.role == Role::System {
            &mut system
        } else {
            &mut messages
        };
        let mut content = Vec::new();
        for part in &message.content {
            content.push(encode_part(part, message.role)?);
        }
        if message.role == Role::System {
            target.extend(content);
        } else {
            target.push(json!({
                "role": message_role(message.role)?,
                "content": content
            }));
        }
    }
    if messages.is_empty() {
        return Err(invalid(
            "Anthropic requests require at least one user or assistant message",
        ));
    }
    Ok((system, messages))
}

fn encode_part(part: &ContentPart, role: Role) -> Result<Value, ModelError> {
    match part {
        ContentPart::Text { text } => Ok(json!({"type": "text", "text": text})),
        ContentPart::Image { source } => {
            require_role(role, Role::User, "image")?;
            Ok(json!({"type": "image", "source": encode_media(source)?}))
        }
        ContentPart::ToolCall(call) => {
            require_role(role, Role::Assistant, "tool call")?;
            Ok(json!({
                "type": "tool_use",
                "id": call.id,
                "name": call.name,
                "input": call.arguments
            }))
        }
        ContentPart::ToolResult(result) => {
            if !matches!(role, Role::User | Role::Tool) {
                return Err(invalid("tool results must use the user or tool role"));
            }
            encode_tool_result(result)
        }
        ContentPart::Reasoning(reasoning) => {
            require_role(role, Role::Assistant, "reasoning")?;
            let signature = reasoning
                .signature
                .as_ref()
                .ok_or_else(|| unsupported("Anthropic thinking round trips require a signature"))?;
            let thinking = reasoning.text.as_ref().ok_or_else(|| {
                unsupported("Anthropic thinking round trips require visible thinking text")
            })?;
            Ok(json!({
                "type": "thinking",
                "thinking": thinking,
                "signature": signature
            }))
        }
        ContentPart::ProviderOpaque(data)
            if data.provider == PROVIDER && data.kind == "content_block" =>
        {
            Ok(data.value.clone())
        }
        ContentPart::Audio { .. }
        | ContentPart::Document { .. }
        | ContentPart::ResourceLink { .. } => {
            require_role(role, Role::User, "projected rich content")?;
            Ok(json!({"type":"text","text":encode_content_envelope(part)?}))
        }
        ContentPart::Refusal { .. } => Err(unsupported(
            "refusals cannot be sent as generic Anthropic message input",
        )),
        ContentPart::Citation(_) => Err(unsupported(
            "citations cannot be sent as generic Anthropic message input",
        )),
        ContentPart::ProviderOpaque(_) => Err(unsupported(
            "opaque content belongs to another provider or unsupported Anthropic kind",
        )),
        _ => Err(unsupported(
            "content variant is newer than this Anthropic adapter",
        )),
    }
}

fn encode_media(source: &MediaSource) -> Result<Value, ModelError> {
    match source {
        MediaSource::Base64 { media_type, data } => Ok(json!({
            "type": "base64",
            "media_type": media_type,
            "data": data
        })),
        MediaSource::Url { url, .. } => Ok(json!({"type": "url", "url": url})),
        MediaSource::Artifact { .. } => Err(unsupported(
            "artifact images must be resolved before provider invocation",
        )),
        _ => Err(unsupported(
            "image source is newer than this Anthropic adapter",
        )),
    }
}

fn encode_tool_result(result: &ToolResult) -> Result<Value, ModelError> {
    let mut content = result
        .content
        .iter()
        .map(|part| match part {
            ContentPart::Text { text } => Ok(json!({"type": "text", "text": text})),
            ContentPart::Image { source } => match encode_media(source) {
                Ok(source) => Ok(json!({"type": "image", "source": source})),
                Err(error) if error.kind == ModelErrorKind::UnsupportedFeature => Ok(json!({
                    "type": "text",
                    "text": encode_content_envelope(part)?
                })),
                Err(error) => Err(error),
            },
            _ => Ok(json!({
                "type": "text",
                "text": encode_content_envelope(part)?
            })),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(structured) = &result.structured_content {
        let encoded = structured.to_string();
        let already_present = result
            .content
            .iter()
            .any(|part| matches!(part, ContentPart::Text { text } if text == &encoded));
        if !already_present {
            content.push(json!({"type":"text","text":encoded}));
        }
    }
    Ok(json!({
        "type": "tool_result",
        "tool_use_id": result.call_id,
        "content": content,
        "is_error": result.is_error
    }))
}

fn encode_tool_choice(choice: &ToolChoice) -> Result<Value, ModelError> {
    match choice {
        ToolChoice::Auto => Ok(json!({"type": "auto"})),
        ToolChoice::Required => Ok(json!({"type": "any"})),
        ToolChoice::Named { name } => Ok(json!({"type": "tool", "name": name})),
        ToolChoice::None => Err(invalid("tool choice `none` must omit Anthropic tools")),
        _ => Err(unsupported(
            "tool choice is newer than this Anthropic adapter",
        )),
    }
}

fn message_role(role: Role) -> Result<&'static str, ModelError> {
    match role {
        Role::User | Role::Tool => Ok("user"),
        Role::Assistant => Ok("assistant"),
        Role::System => Err(invalid(
            "system messages must use the top-level system field",
        )),
        _ => Err(unsupported(
            "message role is newer than this Anthropic adapter",
        )),
    }
}

fn require_role(actual: Role, expected: Role, label: &str) -> Result<(), ModelError> {
    if actual == expected {
        Ok(())
    } else {
        Err(invalid(format!(
            "{label} content requires the {expected:?} role"
        )))
    }
}

fn merge_provider_options(
    body: &mut Map<String, Value>,
    request: &ModelRequest,
) -> Result<(), ModelError> {
    let Some(options) = request.provider_options.get(PROVIDER) else {
        return Ok(());
    };
    let options = options
        .as_object()
        .ok_or_else(|| invalid("`provider_options.anthropic` must be a JSON object"))?;
    for (key, value) in options {
        if body.contains_key(key)
            || matches!(
                key.as_str(),
                "model" | "messages" | "system" | "stream" | "max_tokens" | "tools" | "tool_choice"
            )
        {
            return Err(invalid(format!(
                "provider option `{key}` conflicts with an adapter-owned field"
            )));
        }
        body.insert(key.clone(), value.clone());
    }
    Ok(())
}

fn insert_optional(body: &mut Map<String, Value>, key: &str, value: Option<Value>) {
    if let Some(value) = value {
        body.insert(key.into(), value);
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
    use std::collections::{BTreeMap, btree_map::Entry};

    use runifold_model::{
        ContentPart, MediaSource, Message, ModelRef, ModelRequest, Role, ToolCall, ToolChoice,
        ToolResult, ToolSpec,
    };
    use serde_json::json;

    use super::encode_request;

    use crate::content_projection::decode_content_envelope;

    #[test]
    fn encodes_system_messages_and_required_tools_natively() {
        let mut request = ModelRequest::new(
            ModelRef::new("anthropic", "claude-test"),
            Message::system("be concise"),
        )
        .message(Message::user("weather?"));
        request.tools.push(ToolSpec {
            name: "weather".into(),
            description: "Get weather".into(),
            input_schema: json!({"type": "object"}),
            output_schema: None,
            metadata: BTreeMap::new(),
        });
        request.tool_choice = ToolChoice::Required;

        let body = encode_request(&request, 1_024).unwrap();

        assert_eq!(
            body["system"][0],
            json!({"type":"text","text":"be concise"})
        );
        assert_eq!(body["tool_choice"], json!({"type":"any"}));
        assert_eq!(body["max_tokens"], 1_024);
    }

    #[test]
    fn assistant_tool_calls_round_trip_as_tool_use() {
        let call = ToolCall {
            id: "call_1".into(),
            name: "lookup".into(),
            arguments: json!({"id": 7}),
            raw_arguments: None,
            metadata: BTreeMap::new(),
        };
        let assistant = Message::new(Role::Assistant, vec![ContentPart::ToolCall(call)]).unwrap();
        let request = ModelRequest::new(
            ModelRef::new("anthropic", "claude-test"),
            Message::user("lookup"),
        )
        .message(assistant);

        let body = encode_request(&request, 100).unwrap();

        assert_eq!(body["messages"][1]["content"][0]["type"], "tool_use");
        assert_eq!(body["messages"][1]["content"][0]["input"]["id"], 7);
    }

    #[test]
    fn tool_results_bridge_audio_and_documents() {
        let result = ToolResult {
            call_id: "call-rich".into(),
            name: Some("inspect".into()),
            content: vec![
                ContentPart::Audio {
                    source: MediaSource::Base64 {
                        media_type: "audio/wav".into(),
                        data: "YXVkaW8=".into(),
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
            structured_content: None,
            is_error: false,
            metadata: BTreeMap::new(),
        };
        let message = Message::new(Role::Tool, vec![ContentPart::ToolResult(result)]).unwrap();

        let body = encode_request(
            &ModelRequest::new(ModelRef::new("anthropic", "claude-test"), message),
            100,
        )
        .unwrap();

        assert_eq!(body["messages"][0]["content"].as_array().unwrap().len(), 1);
        let blocks = body["messages"][0]["content"][0]["content"]
            .as_array()
            .unwrap();
        assert!(blocks.iter().all(|block| block["type"] == "text"));
        assert!(blocks[0]["text"].as_str().unwrap().contains("audio/wav"));
        assert!(blocks[1]["text"].as_str().unwrap().contains("report.pdf"));
    }

    #[test]
    fn ordinary_audio_and_document_inputs_use_safe_projection_blocks() {
        let message = Message::new(
            Role::User,
            vec![
                ContentPart::Audio {
                    source: MediaSource::Base64 {
                        media_type: "audio/wav".into(),
                        data: "YXVkaW8=".into(),
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
        )
        .unwrap();

        let body = encode_request(
            &ModelRequest::new(ModelRef::new("anthropic", "claude-test"), message),
            100,
        )
        .unwrap();
        let blocks = body["messages"][0]["content"].as_array().unwrap();

        assert!(blocks.iter().all(|block| {
            decode_content_envelope(block["text"].as_str().unwrap())
                .unwrap()
                .is_some()
        }));
    }

    #[test]
    fn adapter_owned_options_cannot_be_replaced() {
        let mut request = ModelRequest::new(
            ModelRef::new("anthropic", "claude-test"),
            Message::user("hi"),
        );
        if let Entry::Vacant(entry) = request.provider_options.entry("anthropic".into()) {
            entry.insert(json!({"model": "other"}));
        }

        let error = encode_request(&request, 100).unwrap_err();

        assert_eq!(error.kind, runifold_model::ModelErrorKind::InvalidRequest);
    }
}
