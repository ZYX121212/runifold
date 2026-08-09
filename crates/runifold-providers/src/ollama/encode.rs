//! Ollama request encoding.

use serde_json::{Map, Value, json};

use crate::content_projection::{
    encode_content_envelope_many, encode_tool_result_envelope, validate_inline_media,
};

use runifold_model::{
    ContentPart, MediaSource, ModelError, ModelErrorKind, ModelRequest, OutputFormat, Role,
    ToolChoice, ToolResult,
};

/// Encodes a canonical request for Ollama's native `/api/chat`.
///
/// # Errors
///
/// Returns an error when content cannot be represented losslessly.
pub fn encode_request(request: &ModelRequest) -> Result<Value, ModelError> {
    if request.messages.is_empty() {
        return Err(invalid("a model request must contain messages"));
    }
    let messages = request
        .messages
        .iter()
        .map(|message| encode_message(message.role, &message.content))
        .collect::<Result<Vec<_>, _>>()?;
    let mut body = Map::new();
    body.insert("model".into(), Value::String(request.model.name.clone()));
    body.insert("messages".into(), Value::Array(messages));
    body.insert("stream".into(), Value::Bool(true));

    if !request.tools.is_empty() && !matches!(request.tool_choice, ToolChoice::None) {
        body.insert(
            "tools".into(),
            Value::Array(
                request
                    .tools
                    .iter()
                    .map(|tool| {
                        json!({"type":"function","function":{
                            "name":tool.name,
                            "description":tool.description,
                            "parameters":tool.input_schema
                        }})
                    })
                    .collect(),
            ),
        );
    }
    if matches!(
        request.tool_choice,
        ToolChoice::Required | ToolChoice::Named { .. }
    ) {
        return Err(unsupported(
            "Ollama native chat does not expose portable forced tool choice",
        ));
    }
    match &request.output_format {
        OutputFormat::Text => {}
        OutputFormat::Json => {
            body.insert("format".into(), Value::String("json".into()));
        }
        OutputFormat::JsonSchema { schema, .. } => {
            body.insert("format".into(), schema.clone());
        }
        _ => {
            return Err(unsupported(
                "output format is newer than this Ollama adapter",
            ));
        }
    }
    let mut options = Map::new();
    optional(&mut options, "temperature", request.generation.temperature);
    optional(&mut options, "top_p", request.generation.top_p);
    if let Some(seed) = request.generation.seed {
        options.insert("seed".into(), Value::from(seed));
    }
    if let Some(limit) = request.generation.max_output_tokens {
        options.insert("num_predict".into(), Value::from(limit));
    }
    if !request.generation.stop.is_empty() {
        options.insert(
            "stop".into(),
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
    if !options.is_empty() {
        body.insert("options".into(), Value::Object(options));
    }
    merge_options(&mut body, request)?;
    Ok(Value::Object(body))
}

fn encode_message(role: Role, parts: &[ContentPart]) -> Result<Value, ModelError> {
    if parts.len() == 1
        && let ContentPart::ToolResult(result) = &parts[0]
    {
        return encode_tool_result(result);
    }
    let mut content = String::new();
    let mut thinking = String::new();
    let mut images = Vec::new();
    let mut tool_calls = Vec::new();
    let mut presentation = Vec::new();
    let mut requires_projection = false;
    for part in parts {
        match part {
            ContentPart::Text { text } => {
                content.push_str(text);
                presentation.push(part.clone());
            }
            ContentPart::Reasoning(reasoning) => {
                if reasoning.signature.is_some()
                    || reasoning.redacted
                    || !reasoning.provider_data.is_empty()
                {
                    return Err(unsupported(
                        "Ollama cannot safely round-trip signed or private reasoning",
                    ));
                }
                if let Some(text) = &reasoning.text {
                    thinking.push_str(text);
                }
            }
            ContentPart::Image { source } => {
                images.push(image(source)?);
                presentation.push(part.clone());
            }
            ContentPart::Audio { .. }
            | ContentPart::Document { .. }
            | ContentPart::ResourceLink { .. }
            | ContentPart::Refusal { .. }
            | ContentPart::Citation(_) => {
                presentation.push(part.clone());
                requires_projection = true;
            }
            ContentPart::ToolCall(call) => tool_calls.push(json!({"function":{
                "name":call.name,
                "arguments":call.arguments
            }})),
            _ => return Err(unsupported("content cannot be represented by Ollama chat")),
        }
    }
    if requires_projection {
        content = encode_content_envelope_many(&presentation)?;
    }
    let mut message = Map::new();
    message.insert("role".into(), Value::String(role_name(role)?.into()));
    message.insert("content".into(), Value::String(content));
    if !thinking.is_empty() {
        message.insert("thinking".into(), Value::String(thinking));
    }
    if !images.is_empty() {
        message.insert("images".into(), Value::Array(images));
    }
    if !tool_calls.is_empty() {
        message.insert("tool_calls".into(), Value::Array(tool_calls));
    }
    Ok(Value::Object(message))
}

fn encode_tool_result(result: &ToolResult) -> Result<Value, ModelError> {
    let mut images = Vec::new();
    for part in &result.content {
        if let ContentPart::Image { source } = part {
            match image(source) {
                Ok(encoded) => images.push(encoded),
                Err(error) if error.kind == ModelErrorKind::UnsupportedFeature => {}
                Err(error) => return Err(error),
            }
        }
    }
    let content = encode_tool_result_envelope(result)?;
    let mut message = json!({"role":"tool","content":content});
    if !images.is_empty() {
        message["images"] = Value::Array(images);
    }
    if let Some(name) = &result.name {
        message["tool_name"] = Value::String(name.clone());
    }
    Ok(message)
}

fn image(source: &MediaSource) -> Result<Value, ModelError> {
    match source {
        MediaSource::Base64 { media_type, data } => {
            validate_inline_media(media_type, data)?;
            Ok(Value::String(data.clone()))
        }
        _ => Err(unsupported(
            "Ollama image input requires inline base64 data",
        )),
    }
}

fn role_name(role: Role) -> Result<&'static str, ModelError> {
    match role {
        Role::System => Ok("system"),
        Role::User => Ok("user"),
        Role::Assistant => Ok("assistant"),
        Role::Tool => Ok("tool"),
        _ => Err(unsupported("role is newer than this Ollama adapter")),
    }
}

fn optional(body: &mut Map<String, Value>, key: &str, value: Option<f64>) {
    if let Some(value) = value {
        body.insert(key.into(), Value::from(value));
    }
}

fn merge_options(body: &mut Map<String, Value>, request: &ModelRequest) -> Result<(), ModelError> {
    let Some(extra) = request.provider_options.get("ollama") else {
        return Ok(());
    };
    let extra = extra
        .as_object()
        .ok_or_else(|| invalid("provider_options.ollama must be an object"))?;
    for (key, value) in extra {
        if body.contains_key(key)
            || matches!(
                key.as_str(),
                "model" | "messages" | "stream" | "tools" | "format" | "options"
            )
        {
            return Err(invalid(format!("provider option `{key}` is adapter-owned")));
        }
        body.insert(key.clone(), value.clone());
    }
    Ok(())
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
        ContentPart, MediaSource, Message, ModelRef, ModelRequest, Role, ToolResult,
    };

    use crate::content_projection::{decode_content_envelope, decode_tool_result_envelope};

    use super::encode_request;

    #[test]
    fn encodes_native_chat_request() {
        let request = ModelRequest::new(ModelRef::new("ollama", "qwen3"), Message::user("hello"));
        let body = encode_request(&request).unwrap();

        assert_eq!(body["model"], "qwen3");
        assert_eq!(body["messages"][0]["content"], "hello");
        assert_eq!(body["stream"], true);
    }

    #[test]
    fn tool_results_keep_native_images_and_bridge_all_rich_content() {
        let result = ToolResult {
            call_id: "call-rich".into(),
            name: Some("inspect".into()),
            content: vec![
                ContentPart::Image {
                    source: MediaSource::Base64 {
                        media_type: "image/png".into(),
                        data: "aW1hZ2U=".into(),
                    },
                },
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

        let body = encode_request(&ModelRequest::new(
            ModelRef::new("ollama", "qwen3"),
            message,
        ))
        .unwrap();
        let message = &body["messages"][0];
        let content = message["content"].as_str().unwrap();

        assert_eq!(message["images"][0], "aW1hZ2U=");
        let decoded = decode_tool_result_envelope(content).unwrap().unwrap();
        assert_eq!(decoded.content.len(), 3);
        assert_eq!(decoded.name.as_deref(), Some("inspect"));
    }

    #[test]
    fn mixed_text_and_audio_message_has_one_unambiguous_projection() {
        let message = Message::new(
            Role::User,
            vec![
                ContentPart::text("listen"),
                ContentPart::Audio {
                    source: MediaSource::Base64 {
                        media_type: "audio/wav".into(),
                        data: "YXVkaW8=".into(),
                    },
                },
            ],
        )
        .unwrap();

        let body = encode_request(&ModelRequest::new(
            ModelRef::new("ollama", "qwen3"),
            message,
        ))
        .unwrap();
        let envelope = body["messages"][0]["content"].as_str().unwrap();
        let decoded = decode_content_envelope(envelope).unwrap().unwrap();

        assert_eq!(decoded.len(), 2);
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
            ModelRef::new("ollama", "qwen3"),
            message,
        ))
        .unwrap_err();

        assert_eq!(error.kind, runifold_model::ModelErrorKind::InvalidRequest);
    }
}
