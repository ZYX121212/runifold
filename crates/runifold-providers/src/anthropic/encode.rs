//! Anthropic Messages request encoding.

use serde_json::{Map, Value, json};

use crate::content_projection::{
    encode_content_envelope, validate_inline_media, validate_media_url,
    validate_optional_media_type,
};

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
                    .map(encode_tool)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        );
        body.insert(
            "tool_choice".into(),
            encode_tool_choice(&request.tool_choice)?,
        );
    } else if !matches!(request.tool_choice, ToolChoice::Auto | ToolChoice::None) {
        return Err(invalid("tool choice requires at least one tool"));
    }
    if let OutputFormat::JsonSchema { schema, .. } = &request.output_format {
        if !schema.is_object() {
            return Err(invalid(
                "Anthropic structured output requires a JSON Schema object",
            ));
        }
        body.insert(
            "output_config".into(),
            json!({"format":{"type":"json_schema","schema":schema}}),
        );
    } else if !matches!(request.output_format, OutputFormat::Text) {
        return Err(unsupported(
            "Anthropic JSON output requires an explicit JSON Schema",
        ));
    }
    merge_provider_options(&mut body, request)?;
    Ok(Value::Object(body))
}

fn pdf_source(source: &MediaSource) -> bool {
    match source {
        MediaSource::Base64 { media_type, .. } => media_type == "application/pdf",
        MediaSource::Url { media_type, .. } => media_type.as_deref() == Some("application/pdf"),
        _ => false,
    }
}

fn encode_tool(tool: &runifold_model::ToolSpec) -> Result<Value, ModelError> {
    let mut value =
        json!({"name":tool.name,"description":tool.description,"input_schema":tool.input_schema});
    for field in [
        "strict",
        "defer_loading",
        "eager_input_streaming",
        "cache_control",
        "allowed_callers",
        "input_examples",
    ] {
        if let Some(option) = tool.metadata.get(&format!("anthropic.{field}")) {
            let valid = match field {
                "strict" | "defer_loading" | "eager_input_streaming" => option.is_boolean(),
                "cache_control" => option.is_object(),
                _ => option.is_array(),
            };
            if !valid {
                return Err(invalid(format!(
                    "invalid Anthropic tool metadata `{field}`"
                )));
            }
            value[field] = option.clone();
        }
    }
    Ok(value)
}

fn encode_messages(request: &ModelRequest) -> Result<(Vec<Value>, Vec<Value>), ModelError> {
    let mut system = Vec::new();
    let mut messages: Vec<Value> = Vec::new();
    for message in &request.messages {
        let mut content = Vec::new();
        for part in &message.content {
            content.push(encode_part(part, message.role)?);
        }
        if message.role == Role::System {
            system.extend(content);
        } else {
            let role = message_role(message.role)?;
            if let Some(previous) = messages.last_mut()
                && previous["role"] == role
            {
                let blocks = previous["content"]
                    .as_array_mut()
                    .ok_or_else(|| invalid("encoded Anthropic content must be an array"))?;
                blocks.extend(content);
            } else {
                messages.push(json!({"role": role, "content": content}));
            }
        }
    }
    for message in &mut messages {
        if message["role"] == "user" {
            let blocks = message["content"]
                .as_array_mut()
                .ok_or_else(|| invalid("encoded Anthropic content must be an array"))?;
            blocks.sort_by_key(|block| block["type"] != "tool_result");
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
            let mut block =
                json!({"type":"tool_use","id":call.id,"name":call.name,"input":call.arguments});
            if let Some(caller) = call.metadata.get("anthropic.caller") {
                if !caller.is_object() {
                    return Err(invalid("Anthropic tool caller must be an object"));
                }
                block["caller"] = caller.clone();
            }
            Ok(block)
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
            if reasoning.redacted || !reasoning.provider_data.is_empty() {
                return Err(unsupported(
                    "private Anthropic thinking must use its preserved content block",
                ));
            }
            let thinking = reasoning.text.as_deref().unwrap_or("");
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
        ContentPart::Document { source, name } if pdf_source(source) => {
            require_role(role, Role::User, "document")?;
            let mut block = json!({"type":"document","source":encode_media(source)?});
            if let Some(name) = name {
                block["title"] = Value::String(name.clone());
            }
            Ok(block)
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
        MediaSource::Base64 { media_type, data } => {
            validate_inline_media(media_type, data)?;
            Ok(json!({
                "type": "base64",
                "media_type": media_type,
                "data": data
            }))
        }
        MediaSource::Url { url, media_type } => {
            validate_media_url(url, &["http", "https"])?;
            validate_optional_media_type(media_type.as_deref())?;
            Ok(json!({"type": "url", "url": url}))
        }
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
        if key == "output_config" && body.contains_key(key) {
            let extra = value
                .as_object()
                .ok_or_else(|| invalid("Anthropic output_config must be an object"))?;
            let target = body
                .get_mut(key)
                .and_then(Value::as_object_mut)
                .ok_or_else(|| invalid("Anthropic output_config must be an object"))?;
            for (field, value) in extra {
                if target.contains_key(field) {
                    return Err(invalid(format!(
                        "Anthropic output_config `{field}` is adapter-owned"
                    )));
                }
                target.insert(field.clone(), value.clone());
            }
            continue;
        }
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
    fn strict_tool_metadata_and_output_effort_are_preserved_without_override() {
        let mut request =
            ModelRequest::new(ModelRef::new("anthropic", "test"), Message::user("hello"));
        request.tools.push(ToolSpec {
            name: "lookup".into(),
            description: "lookup".into(),
            input_schema: json!({"type":"object","additionalProperties":false}),
            output_schema: None,
            metadata: BTreeMap::from([
                ("anthropic.strict".into(), json!(true)),
                ("anthropic.allowed_callers".into(), json!(["direct"])),
            ]),
        });
        request.output_format = runifold_model::OutputFormat::JsonSchema {
            name: "answer".into(),
            schema: json!({"type":"string"}),
            strict: true,
        };
        request.provider_options.insert(
            "anthropic".into(),
            json!({"output_config":{"effort":"high"}}),
        );
        let body = encode_request(&request, 100).unwrap();
        assert_eq!(body["tools"][0]["strict"], true);
        assert_eq!(body["tools"][0]["allowed_callers"], json!(["direct"]));
        assert_eq!(body["output_config"]["effort"], "high");
        assert_eq!(
            body["output_config"]["format"]["schema"],
            json!({"type":"string"})
        );
        request
            .provider_options
            .insert("anthropic".into(), json!({"output_config":{"format":{}}}));
        assert!(encode_request(&request, 100).is_err());
        request.tools[0]
            .metadata
            .insert("anthropic.strict".into(), json!("true"));
        assert!(encode_request(&request, 100).is_err());
    }

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
    fn groups_parallel_tool_results_before_user_text() {
        let result = |id: &str| {
            Message::new(
                Role::Tool,
                vec![ContentPart::ToolResult(ToolResult {
                    call_id: id.into(),
                    name: Some("lookup".into()),
                    content: vec![ContentPart::text("ok")],
                    structured_content: None,
                    is_error: false,
                    metadata: BTreeMap::new(),
                })],
            )
            .unwrap()
        };
        let request = ModelRequest::new(
            ModelRef::new("anthropic", "claude-test"),
            Message::user("lookup"),
        )
        .message(
            Message::new(
                Role::Assistant,
                ["call-1", "call-2"]
                    .into_iter()
                    .map(|id| {
                        ContentPart::ToolCall(ToolCall {
                            id: id.into(),
                            name: "lookup".into(),
                            arguments: json!({}),
                            raw_arguments: None,
                            metadata: BTreeMap::new(),
                        })
                    })
                    .collect(),
            )
            .unwrap(),
        )
        .message(Message::user("continue after results"))
        .message(result("call-1"))
        .message(result("call-2"));

        let body = encode_request(&request, 100).unwrap();
        let messages = body["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[2]["content"][0]["tool_use_id"], "call-1");
        assert_eq!(messages[2]["content"][1]["tool_use_id"], "call-2");
        assert_eq!(messages[2]["content"][2]["text"], "continue after results");
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
    fn audio_is_projected_and_pdf_uses_native_document_blocks() {
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

        assert!(
            decode_content_envelope(blocks[0]["text"].as_str().unwrap())
                .unwrap()
                .is_some()
        );
        assert_eq!(blocks[1]["type"], "document");
        assert_eq!(
            blocks[1]["source"],
            json!({"type":"url","url":"https://example.com/report.pdf"})
        );
        assert_eq!(blocks[1]["title"], "report.pdf");
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

        let error = encode_request(
            &ModelRequest::new(ModelRef::new("anthropic", "claude-test"), message),
            100,
        )
        .unwrap_err();

        assert_eq!(error.kind, runifold_model::ModelErrorKind::InvalidRequest);
    }

    #[test]
    fn native_image_input_rejects_unsafe_url() {
        let message = Message::new(
            Role::User,
            vec![ContentPart::Image {
                source: MediaSource::Url {
                    url: "file:///etc/passwd".into(),
                    media_type: Some("image/png".into()),
                },
            }],
        )
        .unwrap();

        let error = encode_request(
            &ModelRequest::new(ModelRef::new("anthropic", "claude-test"), message),
            100,
        )
        .unwrap_err();

        assert_eq!(error.kind, runifold_model::ModelErrorKind::InvalidRequest);
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
