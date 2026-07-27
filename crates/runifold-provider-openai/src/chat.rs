use std::collections::{BTreeMap, BTreeSet};

use runifold_model::{
    ContentBlockKind, ContentPart, FinishReason, MediaSource, ModelError, ModelErrorKind, ModelRef,
    ModelRequest, ModelStreamEvent, ModelUsage, OutputFormat, ProviderEvent, Role, ToolChoice,
};
use serde_json::{Map, Value, json};

/// Encodes a canonical request using the OpenAI-compatible Chat Completions
/// protocol.
///
/// # Errors
///
/// Returns [`ModelError`] when content cannot be represented losslessly.
pub fn encode_chat_request(request: &ModelRequest, provider: &str) -> Result<Value, ModelError> {
    if request.messages.is_empty() {
        return Err(invalid("a model request must contain at least one message"));
    }
    let mut body = Map::new();
    body.insert("model".into(), Value::String(request.model.name.clone()));
    body.insert(
        "messages".into(),
        Value::Array(encode_chat_messages(request)?),
    );
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
        "max_tokens",
        request.generation.max_output_tokens.map(Value::from),
    );
    insert_optional(&mut body, "seed", request.generation.seed.map(Value::from));
    if !request.generation.stop.is_empty() {
        body.insert(
            "stop".into(),
            serde_json::to_value(&request.generation.stop)
                .map_err(|error| invalid(format!("failed to encode stop sequences: {error}")))?,
        );
    }
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
                            "function": {
                                "name": tool.name,
                                "description": tool.description,
                                "parameters": tool.input_schema,
                                "strict": true
                            }
                        })
                    })
                    .collect(),
            ),
        );
    }
    body.insert(
        "tool_choice".into(),
        encode_chat_tool_choice(&request.tool_choice)?,
    );
    if !matches!(request.output_format, OutputFormat::Text) {
        body.insert(
            "response_format".into(),
            encode_chat_output_format(&request.output_format)?,
        );
    }
    merge_options(&mut body, request, provider)?;
    Ok(Value::Object(body))
}

fn encode_chat_messages(request: &ModelRequest) -> Result<Vec<Value>, ModelError> {
    let mut messages = Vec::new();
    for message in &request.messages {
        let mut content = Vec::new();
        let mut tool_calls = Vec::new();
        for part in &message.content {
            match part {
                ContentPart::Text { text } => content.push(chat_text(text)),
                ContentPart::Image { source } if message.role == Role::User => {
                    content.push(chat_image(source)?);
                }
                ContentPart::ToolCall(call) if message.role == Role::Assistant => {
                    tool_calls.push(json!({
                        "id": call.id,
                        "type": "function",
                        "function": {
                            "name": call.name,
                            "arguments": call.raw_arguments.clone()
                                .unwrap_or_else(|| call.arguments.to_string())
                        }
                    }));
                }
                ContentPart::ToolResult(result) => {
                    flush_chat_message(&mut messages, message.role, &mut content, &mut tool_calls)?;
                    let output = result
                        .content
                        .iter()
                        .map(tool_result_part)
                        .collect::<Result<Vec<_>, _>>()?
                        .join("\n");
                    messages.push(json!({
                        "role": "tool",
                        "tool_call_id": result.call_id,
                        "content": output
                    }));
                }
                _ => {
                    return Err(unsupported(
                        "content part has no lossless Chat Completions representation",
                    ));
                }
            }
        }
        flush_chat_message(&mut messages, message.role, &mut content, &mut tool_calls)?;
    }
    Ok(messages)
}

fn flush_chat_message(
    messages: &mut Vec<Value>,
    role: Role,
    content: &mut Vec<Value>,
    tool_calls: &mut Vec<Value>,
) -> Result<(), ModelError> {
    if content.is_empty() && tool_calls.is_empty() {
        return Ok(());
    }
    if role == Role::Tool {
        return Err(unsupported(
            "tool messages must contain canonical tool-result parts",
        ));
    }
    let content_value = if content.len() == 1 && content[0]["type"] == "text" {
        content[0]["text"].clone()
    } else {
        Value::Array(std::mem::take(content))
    };
    let mut message = Map::new();
    message.insert("role".into(), Value::String(chat_role(role)?.into()));
    message.insert("content".into(), content_value);
    if !tool_calls.is_empty() {
        message.insert(
            "tool_calls".into(),
            Value::Array(std::mem::take(tool_calls)),
        );
    }
    messages.push(Value::Object(message));
    content.clear();
    Ok(())
}

fn chat_text(text: &str) -> Value {
    json!({"type": "text", "text": text})
}

fn chat_image(source: &MediaSource) -> Result<Value, ModelError> {
    let url = match source {
        MediaSource::Url { url, .. } => url.clone(),
        MediaSource::Base64 { media_type, data } => format!("data:{media_type};base64,{data}"),
        MediaSource::Artifact { .. } => {
            return Err(unsupported(
                "artifact images must be resolved before provider invocation",
            ));
        }
        _ => return Err(unsupported("image source is newer than this adapter")),
    };
    Ok(json!({"type": "image_url", "image_url": {"url": url}}))
}

fn tool_result_part(part: &ContentPart) -> Result<String, ModelError> {
    match part {
        ContentPart::Text { text } => Ok(text.clone()),
        _ => serde_json::to_string(part)
            .map_err(|error| invalid(format!("failed to encode tool result: {error}"))),
    }
}

fn encode_chat_tool_choice(choice: &ToolChoice) -> Result<Value, ModelError> {
    Ok(match choice {
        ToolChoice::Auto => Value::String("auto".into()),
        ToolChoice::None => Value::String("none".into()),
        ToolChoice::Required => Value::String("required".into()),
        ToolChoice::Named { name } => {
            json!({"type": "function", "function": {"name": name}})
        }
        _ => return Err(unsupported("tool choice is newer than this adapter")),
    })
}

fn encode_chat_output_format(format: &OutputFormat) -> Result<Value, ModelError> {
    Ok(match format {
        OutputFormat::Text => json!({"type": "text"}),
        OutputFormat::Json => json!({"type": "json_object"}),
        OutputFormat::JsonSchema {
            name,
            schema,
            strict,
        } => json!({
            "type": "json_schema",
            "json_schema": {"name": name, "schema": schema, "strict": strict}
        }),
        _ => return Err(unsupported("output format is newer than this adapter")),
    })
}

fn merge_options(
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

fn chat_role(role: Role) -> Result<&'static str, ModelError> {
    match role {
        Role::System => Ok("system"),
        Role::User => Ok("user"),
        Role::Assistant => Ok("assistant"),
        Role::Tool => Err(unsupported("tool role requires a tool result")),
        _ => Err(unsupported("role is newer than this adapter")),
    }
}

fn insert_optional(body: &mut Map<String, Value>, name: &str, value: Option<Value>) {
    if let Some(value) = value {
        body.insert(name.into(), value);
    }
}

/// Stateful decoder for OpenAI-compatible Chat Completions chunks.
#[derive(Debug)]
pub struct ChatCompletionsDecoder {
    provider: String,
    started: bool,
    text_index: Option<u32>,
    tool_indices: BTreeMap<u64, u32>,
    open: BTreeSet<u32>,
    next_index: u32,
    finish_reason: Option<FinishReason>,
    usage: ModelUsage,
}

impl ChatCompletionsDecoder {
    /// Creates a decoder retaining the configured provider identity.
    pub fn new(provider: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            started: false,
            text_index: None,
            tool_indices: BTreeMap::new(),
            open: BTreeSet::new(),
            next_index: 0,
            finish_reason: None,
            usage: ModelUsage::default(),
        }
    }

    /// Decodes one `chat.completion.chunk` payload.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError`] for malformed chunks or unsupported multiple
    /// choices.
    pub fn decode(&mut self, payload: Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        let mut events = Vec::new();
        if !self.started {
            events.push(ModelStreamEvent::ResponseStarted {
                id: payload.get("id").and_then(Value::as_str).map(String::from),
                model: ModelRef::new(self.provider.clone(), required_string(&payload, "model")?),
            });
            self.started = true;
        }
        if let Some(usage) = payload.get("usage").filter(|value| value.is_object()) {
            self.usage = chat_usage(usage);
            events.push(ModelStreamEvent::UsageUpdated { usage: self.usage });
        }
        let choices = payload
            .get("choices")
            .and_then(Value::as_array)
            .ok_or_else(|| protocol("chat chunk field `choices` must be an array"))?;
        for choice in choices {
            if choice.get("index").and_then(Value::as_u64).unwrap_or(0) != 0 {
                return Err(protocol(
                    "multiple Chat Completions choices are not supported",
                ));
            }
            let delta = choice.get("delta").unwrap_or(&Value::Null);
            if let Some(text) = delta.get("content").and_then(Value::as_str) {
                let index = self.ensure_text(&mut events)?;
                events.push(ModelStreamEvent::TextDelta {
                    index,
                    text: text.into(),
                });
            }
            if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
                for call in tool_calls {
                    self.decode_tool_delta(call, &mut events)?;
                }
            }
            if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
                self.finish_reason = Some(chat_finish_reason(reason));
            }
        }
        events.push(ModelStreamEvent::Provider {
            event: ProviderEvent {
                provider: self.provider.clone(),
                name: "chat.completion.chunk".into(),
                payload,
            },
        });
        Ok(events)
    }

    /// Finalizes a stream after `[DONE]` or a clean EOF.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError`] if no terminal finish reason was observed.
    pub fn finish(&mut self) -> Result<Vec<ModelStreamEvent>, ModelError> {
        let finish_reason = self
            .finish_reason
            .take()
            .ok_or_else(|| protocol("chat stream ended without a finish reason"))?;
        let mut events = self
            .open
            .iter()
            .copied()
            .map(|index| ModelStreamEvent::ContentBlockCompleted { index })
            .collect::<Vec<_>>();
        self.open.clear();
        events.push(ModelStreamEvent::UsageUpdated { usage: self.usage });
        events.push(ModelStreamEvent::ResponseCompleted {
            finish_reason,
            provider_metadata: BTreeMap::new(),
        });
        Ok(events)
    }

    fn ensure_text(&mut self, events: &mut Vec<ModelStreamEvent>) -> Result<u32, ModelError> {
        if let Some(index) = self.text_index {
            return Ok(index);
        }
        let index = self.allocate()?;
        self.text_index = Some(index);
        self.open.insert(index);
        events.push(ModelStreamEvent::ContentBlockStarted {
            index,
            kind: ContentBlockKind::Text,
        });
        Ok(index)
    }

    fn decode_tool_delta(
        &mut self,
        call: &Value,
        events: &mut Vec<ModelStreamEvent>,
    ) -> Result<(), ModelError> {
        let call_index = call
            .get("index")
            .and_then(Value::as_u64)
            .ok_or_else(|| protocol("tool-call delta is missing `index`"))?;
        let index = if let Some(index) = self.tool_indices.get(&call_index).copied() {
            index
        } else {
            let function = call.get("function").unwrap_or(&Value::Null);
            let index = self.allocate()?;
            self.tool_indices.insert(call_index, index);
            self.open.insert(index);
            events.push(ModelStreamEvent::ContentBlockStarted {
                index,
                kind: ContentBlockKind::ToolCall {
                    id: required_string(call, "id")?.into(),
                    name: required_string(function, "name")?.into(),
                },
            });
            index
        };
        if let Some(arguments) = call
            .get("function")
            .and_then(|function| function.get("arguments"))
            .and_then(Value::as_str)
        {
            events.push(ModelStreamEvent::ToolArgumentsDelta {
                index,
                json: arguments.into(),
            });
        }
        Ok(())
    }

    fn allocate(&mut self) -> Result<u32, ModelError> {
        let index = self.next_index;
        self.next_index = self
            .next_index
            .checked_add(1)
            .ok_or_else(|| protocol("canonical content index overflow"))?;
        Ok(index)
    }
}

fn chat_usage(usage: &Value) -> ModelUsage {
    ModelUsage {
        input_tokens: usage
            .get("prompt_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        output_tokens: usage
            .get("completion_tokens")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        ..ModelUsage::default()
    }
}

fn chat_finish_reason(reason: &str) -> FinishReason {
    match reason {
        "stop" => FinishReason::Stop,
        "length" => FinishReason::Length,
        "tool_calls" | "function_call" => FinishReason::ToolCalls,
        "content_filter" => FinishReason::ContentFilter,
        other => FinishReason::Other(other.into()),
    }
}

fn required_string<'a>(value: &'a Value, name: &str) -> Result<&'a str, ModelError> {
    value
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| protocol(format!("chat chunk field `{name}` must be a string")))
}

fn invalid(message: impl Into<String>) -> ModelError {
    ModelError::local(ModelErrorKind::InvalidRequest, message)
}

fn unsupported(message: impl Into<String>) -> ModelError {
    ModelError::local(ModelErrorKind::UnsupportedFeature, message)
}

fn protocol(message: impl Into<String>) -> ModelError {
    ModelError::local(ModelErrorKind::Protocol, message)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use runifold_model::{
        ContentPart, Message, ModelRef, ModelRequest, ModelStreamAccumulator, ToolCall,
    };
    use serde_json::{Value, json};

    use super::{ChatCompletionsDecoder, encode_chat_request};

    #[test]
    fn encodes_standard_chat_messages() {
        let request = ModelRequest::new(ModelRef::new("qwen", "qwen-plus"), Message::user("hello"));

        let body = encode_chat_request(&request, "qwen").unwrap();

        assert_eq!(body["model"], "qwen-plus");
        assert_eq!(body["messages"][0]["role"], "user");
        assert_eq!(body["messages"][0]["content"], "hello");
        assert_eq!(body["stream"], true);
    }

    #[test]
    fn decodes_text_chat_stream() {
        let chunks = [
            json!({
                "id": "chat_1",
                "model": "qwen-plus",
                "choices": [{"index": 0, "delta": {"content": "hel"}, "finish_reason": null}]
            }),
            json!({
                "id": "chat_1",
                "model": "qwen-plus",
                "choices": [{"index": 0, "delta": {"content": "lo"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 2, "completion_tokens": 1}
            }),
        ];
        let response = decode_chunks(chunks);

        assert_eq!(response.model.provider, "qwen");
        assert_eq!(response.content, vec![ContentPart::text("hello")]);
        assert_eq!(response.usage.input_tokens, 2);
    }

    #[test]
    fn decodes_fragmented_chat_tool_calls() {
        let chunks = [
            json!({
                "id": "chat_2",
                "model": "doubao",
                "choices": [{"index": 0, "delta": {"tool_calls": [{
                    "index": 0,
                    "id": "call_1",
                    "type": "function",
                    "function": {"name": "lookup", "arguments": "{\"x\":"}
                }]}, "finish_reason": null}]
            }),
            json!({
                "id": "chat_2",
                "model": "doubao",
                "choices": [{"index": 0, "delta": {"tool_calls": [{
                    "index": 0,
                    "function": {"arguments": "1}"}
                }]}, "finish_reason": "tool_calls"}]
            }),
        ];
        let response = decode_chunks(chunks);

        assert_eq!(
            response.content[0],
            ContentPart::ToolCall(ToolCall {
                id: "call_1".into(),
                name: "lookup".into(),
                arguments: json!({"x": 1}),
                raw_arguments: Some("{\"x\":1}".into()),
                metadata: BTreeMap::new(),
            })
        );
    }

    fn decode_chunks(chunks: impl IntoIterator<Item = Value>) -> runifold_model::ModelResponse {
        let mut decoder = ChatCompletionsDecoder::new("qwen");
        let mut accumulator = ModelStreamAccumulator::new();
        for chunk in chunks {
            for event in decoder.decode(chunk).unwrap() {
                accumulator.push(event).unwrap();
            }
        }
        decoder
            .finish()
            .unwrap()
            .into_iter()
            .find_map(|event| accumulator.push(event).unwrap())
            .unwrap()
    }
}
