//! Ollama request encoding.

use serde_json::{Map, Value, json};

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
    if parts.len() == 1 {
        if let ContentPart::ToolResult(result) = &parts[0] {
            return encode_tool_result(result);
        }
    }
    let mut content = String::new();
    let mut thinking = String::new();
    let mut images = Vec::new();
    let mut tool_calls = Vec::new();
    for part in parts {
        match part {
            ContentPart::Text { text } => content.push_str(text),
            ContentPart::Reasoning(reasoning) => {
                if let Some(text) = &reasoning.text {
                    thinking.push_str(text);
                }
            }
            ContentPart::Image { source } => images.push(image(source)?),
            ContentPart::ToolCall(call) => tool_calls.push(json!({"function":{
                "name":call.name,
                "arguments":call.arguments
            }})),
            _ => return Err(unsupported("content cannot be represented by Ollama chat")),
        }
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
    let content = result
        .content
        .iter()
        .map(|part| match part {
            ContentPart::Text { text } => Ok(text.clone()),
            _ => serde_json::to_string(part)
                .map_err(|error| invalid(format!("invalid tool result: {error}"))),
        })
        .collect::<Result<Vec<_>, _>>()?
        .join("\n");
    let mut message = json!({"role":"tool","content":content});
    if let Some(name) = &result.name {
        message["tool_name"] = Value::String(name.clone());
    }
    Ok(message)
}

fn image(source: &MediaSource) -> Result<Value, ModelError> {
    match source {
        MediaSource::Base64 { data, .. } => Ok(Value::String(data.clone())),
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
    use runifold_model::{Message, ModelRef, ModelRequest};

    use super::encode_request;

    #[test]
    fn encodes_native_chat_request() {
        let request = ModelRequest::new(ModelRef::new("ollama", "qwen3"), Message::user("hello"));
        let body = encode_request(&request).unwrap();

        assert_eq!(body["model"], "qwen3");
        assert_eq!(body["messages"][0]["content"], "hello");
        assert_eq!(body["stream"], true);
    }
}
