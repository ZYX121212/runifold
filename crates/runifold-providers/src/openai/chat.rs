//! `OpenAI` Chat Completions wire adaptation.

use std::collections::{BTreeMap, BTreeSet};

use runifold_model::{
    ContentBlockKind, ContentPart, FinishReason, MediaSource, ModelError, ModelErrorKind, ModelRef,
    ModelRequest, ModelStreamEvent, ModelUsage, OutputFormat, ProviderEvent, ResponseMode, Role,
    ToolChoice,
};
use serde_json::{Map, Value, json};
use smallvec::SmallVec;

use super::{OpenAiChatDialect, encode::function_tool_schema, schema::prepare_strict_schema};
use crate::content_projection::{
    encode_content_envelope, encode_tool_result_envelope, validate_image_media_type,
    validate_inline_image, validate_media_url,
};

/// Inline canonical events produced by one Chat Completions chunk.
pub(crate) type ChatEvents = SmallVec<[ModelStreamEvent; 4]>;

/// Encodes a canonical request using the OpenAI-compatible Chat Completions
/// protocol.
///
/// # Errors
///
/// Returns [`ModelError`] when content cannot be represented losslessly.
pub fn encode_chat_request(request: &ModelRequest, provider: &str) -> Result<Value, ModelError> {
    encode_chat_request_for(request, provider, OpenAiChatDialect::Compatible)
}

pub(crate) fn encode_chat_request_for(
    request: &ModelRequest,
    provider: &str,
    dialect: OpenAiChatDialect,
) -> Result<Value, ModelError> {
    if request.messages.is_empty() {
        return Err(invalid("a model request must contain at least one message"));
    }
    let mut body = Map::new();
    body.insert("model".into(), Value::String(request.model.name.clone()));
    body.insert(
        "messages".into(),
        Value::Array(encode_chat_messages(request, provider, dialect)?),
    );
    if !request.provider_tools().is_empty() {
        return Err(unsupported(
            "provider-native hosted tools require the Responses protocol",
        ));
    }
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
    let token_limit_field = match dialect {
        OpenAiChatDialect::OpenAi => "max_completion_tokens",
        OpenAiChatDialect::Compatible => "max_tokens",
    };
    insert_optional(
        &mut body,
        token_limit_field,
        request.generation.max_output_tokens.map(Value::from),
    );
    if dialect == OpenAiChatDialect::OpenAi
        && matches!(request.selected_response_mode(), ResponseMode::Streaming)
    {
        body.insert("stream_options".into(), json!({"include_usage": true}));
    }
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
                    .map(|tool| -> Result<Value, ModelError> {
                        let (parameters, strict) = function_tool_schema(tool, provider)?;
                        Ok(json!({
                            "type": "function",
                            "function": {
                                "name": tool.name,
                                "description": tool.description,
                                "parameters": parameters,
                                "strict": strict
                            }
                        }))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        );
    }
    if !request.tools.is_empty() || !matches!(request.tool_choice, ToolChoice::Auto) {
        body.insert(
            "tool_choice".into(),
            encode_chat_tool_choice(&request.tool_choice)?,
        );
    }
    if !matches!(request.output_format, OutputFormat::Text) {
        body.insert(
            "response_format".into(),
            encode_chat_output_format(&request.output_format)?,
        );
    }
    merge_options(&mut body, request, provider)?;
    Ok(Value::Object(body))
}

fn encode_chat_messages(
    request: &ModelRequest,
    provider: &str,
    dialect: OpenAiChatDialect,
) -> Result<Vec<Value>, ModelError> {
    let mut messages = Vec::with_capacity(request.messages.len());
    for message in &request.messages {
        if let [ContentPart::Text { text }] = message.content.as_slice() {
            messages.push(json!({
                "role": chat_role(message.role, dialect)?,
                "content": text,
            }));
            continue;
        }
        let mut content = Vec::new();
        let mut tool_calls = Vec::new();
        for part in &message.content {
            match part {
                ContentPart::Text { text } => content.push(chat_text(text)),
                ContentPart::Image { source } if message.role == Role::User => {
                    content.push(chat_image(source)?);
                }
                ContentPart::Audio { .. }
                | ContentPart::Document { .. }
                | ContentPart::ResourceLink { .. }
                    if message.role == Role::User =>
                {
                    content.push(chat_text(&encode_content_envelope(part)?));
                }
                ContentPart::ToolCall(call) if message.role == Role::Assistant => {
                    let mut encoded = json!({
                        "id": call.id,
                        "type": "function",
                        "function": {
                            "name": call.name,
                            "arguments": call.raw_arguments.clone()
                                .unwrap_or_else(|| call.arguments.to_string())
                        }
                    });
                    if let Some(extra_content) = call
                        .metadata
                        .get(&format!("{provider}.extra_content"))
                        .or_else(|| call.metadata.get("openai-compatible.extra_content"))
                    {
                        encoded["extra_content"] = extra_content.clone();
                    }
                    tool_calls.push(encoded);
                }
                ContentPart::ToolResult(result) => {
                    flush_chat_message(
                        &mut messages,
                        message.role,
                        &mut content,
                        &mut tool_calls,
                        dialect,
                    )?;
                    messages.push(json!({
                        "role": "tool",
                        "tool_call_id": result.call_id,
                        "content": encode_tool_result_envelope(result)?
                    }));
                }
                ContentPart::Refusal { text } if message.role == Role::Assistant => {
                    content.push(json!({"type":"refusal","refusal":text}));
                }
                _ => {
                    return Err(unsupported(
                        "content part has no lossless Chat Completions representation",
                    ));
                }
            }
        }
        flush_chat_message(
            &mut messages,
            message.role,
            &mut content,
            &mut tool_calls,
            dialect,
        )?;
    }
    Ok(messages)
}

fn flush_chat_message(
    messages: &mut Vec<Value>,
    role: Role,
    content: &mut Vec<Value>,
    tool_calls: &mut Vec<Value>,
    dialect: OpenAiChatDialect,
) -> Result<(), ModelError> {
    if content.is_empty() && tool_calls.is_empty() {
        return Ok(());
    }
    if role == Role::Tool {
        return Err(unsupported(
            "tool messages must contain canonical tool-result parts",
        ));
    }
    let content_value = if content.is_empty() {
        Value::Null
    } else if content.len() == 1 && content[0]["type"] == "text" {
        content[0]["text"].clone()
    } else {
        Value::Array(std::mem::take(content))
    };
    let mut message = Map::new();
    message.insert(
        "role".into(),
        Value::String(chat_role(role, dialect)?.into()),
    );
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
        MediaSource::Url { url, media_type } => {
            validate_media_url(url, &["http", "https"])?;
            validate_image_media_type(media_type.as_deref())?;
            url.clone()
        }
        MediaSource::Base64 { media_type, data } => {
            validate_inline_image(media_type, data)?;
            format!("data:{media_type};base64,{data}")
        }
        MediaSource::Artifact { .. } => {
            return Err(unsupported(
                "artifact images must be resolved before provider invocation",
            ));
        }
        _ => return Err(unsupported("image source is newer than this adapter")),
    };
    Ok(json!({"type": "image_url", "image_url": {"url": url}}))
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
        } => {
            let wire_schema = if *strict {
                prepare_strict_schema(schema)?
            } else {
                schema.clone()
            };
            json!({
                "type": "json_schema",
                "json_schema": {"name": name, "schema": wire_schema, "strict": strict}
            })
        }
        _ => return Err(unsupported("output format is newer than this adapter")),
    })
}

fn merge_options(
    body: &mut Map<String, Value>,
    request: &ModelRequest,
    provider: &str,
) -> Result<(), ModelError> {
    for namespace in std::iter::once("openai-compatible")
        .chain((provider != "openai-compatible").then_some(provider))
    {
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

fn chat_role(role: Role, dialect: OpenAiChatDialect) -> Result<&'static str, ModelError> {
    match role {
        Role::System if dialect == OpenAiChatDialect::OpenAi => Ok("developer"),
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
    response_id: Option<String>,
    model: Option<String>,
    reasoning_index: Option<u32>,
    text_index: Option<u32>,
    refusal_index: Option<u32>,
    tool_indices: BTreeMap<u64, u32>,
    tool_identities: BTreeMap<u64, (String, String)>,
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
            response_id: None,
            model: None,
            reasoning_index: None,
            text_index: None,
            refusal_index: None,
            tool_indices: BTreeMap::new(),
            tool_identities: BTreeMap::new(),
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
        self.decode_compact(payload)
            .map(|events| events.into_iter().collect())
    }

    /// Decodes one chunk without allocating for the common event count.
    pub(crate) fn decode_compact(&mut self, payload: Value) -> Result<ChatEvents, ModelError> {
        if let Some(error) = payload.get("error").filter(|value| !value.is_null()) {
            return Err(chat_stream_error(&self.provider, error));
        }
        let mut events = ChatEvents::new();
        self.validate_chunk_identity(&payload)?;
        let choices = payload
            .get("choices")
            .and_then(Value::as_array)
            .ok_or_else(|| protocol("chat chunk field `choices` must be an array"))?;
        if self.finish_reason.is_some() && !choices.is_empty() {
            return Err(protocol(
                "chat content arrived after a terminal finish reason",
            ));
        }
        if !self.started {
            events.push(ModelStreamEvent::ResponseStarted {
                id: self.response_id.clone(),
                model: ModelRef::new(
                    self.provider.clone(),
                    self.model
                        .as_deref()
                        .ok_or_else(|| protocol("chat chunk is missing model identity"))?,
                ),
            });
            self.started = true;
        }
        if let Some(usage) = payload.get("usage").filter(|value| value.is_object()) {
            self.usage = chat_usage(usage);
            events.push(ModelStreamEvent::UsageUpdated { usage: self.usage });
        }
        if choices.len() > 1 {
            return Err(protocol(
                "multiple Chat Completions choices are not supported",
            ));
        }
        for choice in choices {
            if choice.get("index").and_then(Value::as_u64) != Some(0) {
                return Err(protocol("chat choice must explicitly identify index zero"));
            }
            let delta = choice.get("delta").unwrap_or(&Value::Null);
            if let Some(reasoning) = reasoning_delta(delta) {
                let index = self.ensure_reasoning(&mut events)?;
                events.push(ModelStreamEvent::ReasoningDelta {
                    index,
                    text: reasoning.into(),
                });
            }
            if let Some(text) = delta.get("content").and_then(Value::as_str)
                && !text.is_empty()
            {
                let index = self.ensure_text(&mut events)?;
                events.push(ModelStreamEvent::TextDelta {
                    index,
                    text: text.into(),
                });
            }
            if let Some(refusal) = delta
                .get("refusal")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
            {
                let index = self.ensure_refusal(&mut events)?;
                events.push(ModelStreamEvent::RefusalDelta {
                    index,
                    text: refusal.into(),
                });
            }
            if let Some(tool_calls) = delta.get("tool_calls").and_then(Value::as_array) {
                for call in tool_calls {
                    self.decode_tool_delta(call, &mut events)?;
                }
            }
            if let Some(error) = choice.get("error").filter(|error| !error.is_null()) {
                return Err(chat_stream_error(&self.provider, error));
            }
            if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
                if reason == "error" {
                    return Err(chat_stream_error(&self.provider, choice));
                }
                if self.finish_reason.is_some() {
                    return Err(protocol("chat stream emitted more than one finish reason"));
                }
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

    fn validate_chunk_identity(&mut self, payload: &Value) -> Result<(), ModelError> {
        let model = required_string(payload, "model")?;
        if let Some(expected) = &self.model {
            if expected != model {
                return Err(protocol(
                    "chat stream changed model identity between chunks",
                ));
            }
        } else {
            self.model = Some(model.into());
        }
        if let Some(id) = payload.get("id").and_then(Value::as_str) {
            if let Some(expected) = &self.response_id {
                if expected != id {
                    return Err(protocol("chat stream changed response ID between chunks"));
                }
            } else {
                self.response_id = Some(id.into());
            }
        }
        Ok(())
    }

    /// Finalizes a stream after `[DONE]` or a clean EOF.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError`] if no terminal finish reason was observed.
    pub fn finish(&mut self) -> Result<Vec<ModelStreamEvent>, ModelError> {
        self.finish_compact()
            .map(|events| events.into_iter().collect())
    }

    /// Finalizes the stream without allocating for the common event count.
    pub(crate) fn finish_compact(&mut self) -> Result<ChatEvents, ModelError> {
        let finish_reason = self
            .finish_reason
            .take()
            .ok_or_else(|| protocol("chat stream ended without a finish reason"))?;
        let mut events = self
            .open
            .iter()
            .copied()
            .map(|index| ModelStreamEvent::ContentBlockCompleted { index })
            .collect::<ChatEvents>();
        self.open.clear();
        events.push(ModelStreamEvent::UsageUpdated { usage: self.usage });
        events.push(ModelStreamEvent::ResponseCompleted {
            finish_reason,
            provider_metadata: BTreeMap::new(),
        });
        Ok(events)
    }

    fn ensure_text(&mut self, events: &mut ChatEvents) -> Result<u32, ModelError> {
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

    fn ensure_reasoning(&mut self, events: &mut ChatEvents) -> Result<u32, ModelError> {
        if let Some(index) = self.reasoning_index {
            return Ok(index);
        }
        let index = self.allocate()?;
        self.reasoning_index = Some(index);
        self.open.insert(index);
        events.push(ModelStreamEvent::ContentBlockStarted {
            index,
            kind: ContentBlockKind::Reasoning {
                signature: None,
                redacted: false,
            },
        });
        Ok(index)
    }

    fn ensure_refusal(&mut self, events: &mut ChatEvents) -> Result<u32, ModelError> {
        if let Some(index) = self.refusal_index {
            return Ok(index);
        }
        let index = self.allocate()?;
        self.refusal_index = Some(index);
        self.open.insert(index);
        events.push(ModelStreamEvent::ContentBlockStarted {
            index,
            kind: ContentBlockKind::Refusal,
        });
        Ok(index)
    }

    fn decode_tool_delta(
        &mut self,
        call: &Value,
        events: &mut ChatEvents,
    ) -> Result<(), ModelError> {
        let call_index = call
            .get("index")
            .and_then(Value::as_u64)
            .ok_or_else(|| protocol("tool-call delta is missing `index`"))?;
        let index = if let Some(index) = self.tool_indices.get(&call_index).copied() {
            self.validate_tool_identity(call_index, call)?;
            index
        } else {
            let function = call.get("function").unwrap_or(&Value::Null);
            let id = required_string(call, "id")?;
            let name = required_string(function, "name")?;
            let index = self.allocate()?;
            self.tool_indices.insert(call_index, index);
            self.tool_identities
                .insert(call_index, (id.into(), name.into()));
            self.open.insert(index);
            events.push(ModelStreamEvent::ContentBlockStarted {
                index,
                kind: ContentBlockKind::ToolCall {
                    id: id.into(),
                    name: name.into(),
                },
            });
            index
        };
        if let Some(extra_content) = call.get("extra_content") {
            events.push(ModelStreamEvent::ContentBlockMetadata {
                index,
                metadata: BTreeMap::from([(
                    format!("{}.extra_content", self.provider),
                    extra_content.clone(),
                )]),
            });
        }
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

    fn validate_tool_identity(&self, call_index: u64, call: &Value) -> Result<(), ModelError> {
        let Some((expected_id, expected_name)) = self.tool_identities.get(&call_index) else {
            return Err(protocol("chat tool-call identity state is missing"));
        };
        if call
            .get("id")
            .and_then(Value::as_str)
            .is_some_and(|id| id != expected_id)
        {
            return Err(protocol("chat tool-call ID changed between deltas"));
        }
        if call
            .get("function")
            .and_then(|function| function.get("name"))
            .and_then(Value::as_str)
            .is_some_and(|name| name != expected_name)
        {
            return Err(protocol("chat tool-call name changed between deltas"));
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

/// Normalizes one non-streaming Chat Completions response.
pub(crate) fn decode_complete_chat_response(
    provider: &str,
    payload: &Value,
    request_id: Option<String>,
) -> Result<Vec<ModelStreamEvent>, ModelError> {
    if let Some(error) = payload.get("error").filter(|value| !value.is_null()) {
        return Err(chat_stream_error(provider, error));
    }
    let choices = payload
        .get("choices")
        .and_then(Value::as_array)
        .ok_or_else(|| protocol("chat completion field `choices` must be an array"))?;
    if choices.len() != 1 {
        return Err(protocol(
            "complete Chat Completions response must contain exactly one choice",
        ));
    }
    let choice = &choices[0];
    if choice.get("index").and_then(Value::as_u64) != Some(0) {
        return Err(protocol(
            "complete chat choice must explicitly identify index zero",
        ));
    }
    if let Some(error) = choice.get("error").filter(|error| !error.is_null()) {
        return Err(chat_stream_error(provider, error));
    }
    if choice.get("finish_reason").and_then(Value::as_str) == Some("error") {
        return Err(chat_stream_error(provider, choice));
    }
    let message = choice
        .get("message")
        .and_then(Value::as_object)
        .ok_or_else(|| protocol("complete chat choice is missing `message`"))?;
    if message.get("role").and_then(Value::as_str) != Some("assistant") {
        return Err(protocol(
            "complete chat choice message must have assistant role",
        ));
    }
    let finish_reason = choice
        .get("finish_reason")
        .and_then(Value::as_str)
        .ok_or_else(|| protocol("complete chat choice is missing `finish_reason`"))?;

    let mut delta = Map::new();
    for field in ["reasoning_content", "reasoning", "thinking", "refusal"] {
        if let Some(value) = message.get(field).filter(|value| !value.is_null()) {
            delta.insert(field.into(), value.clone());
        }
    }
    normalize_complete_chat_content(message.get("content"), &mut delta)?;
    if let Some(tool_calls) = message.get("tool_calls").and_then(Value::as_array) {
        let normalized = tool_calls
            .iter()
            .enumerate()
            .map(|(index, call)| {
                let mut call = call
                    .as_object()
                    .cloned()
                    .ok_or_else(|| protocol("complete chat tool call must be an object"))?;
                call.insert("index".into(), Value::from(index));
                Ok(Value::Object(call))
            })
            .collect::<Result<Vec<_>, ModelError>>()?;
        delta.insert("tool_calls".into(), Value::Array(normalized));
    }

    let mut synthetic = Map::new();
    if let Some(id) = payload.get("id") {
        synthetic.insert("id".into(), id.clone());
    }
    synthetic.insert(
        "model".into(),
        Value::String(required_string(payload, "model")?.into()),
    );
    if let Some(usage) = payload.get("usage") {
        synthetic.insert("usage".into(), usage.clone());
    }
    synthetic.insert(
        "choices".into(),
        Value::Array(vec![json!({
            "index":0,
            "delta":Value::Object(delta),
            "finish_reason":finish_reason
        })]),
    );

    let mut decoder = ChatCompletionsDecoder::new(provider);
    let mut events = decoder.decode(Value::Object(synthetic))?;
    normalize_complete_provider_event(&mut events, payload);
    if let Some(request_id) = request_id {
        insert_complete_request_id(&mut events, provider, &request_id);
    }
    events.extend(decoder.finish()?);
    Ok(events)
}

fn normalize_complete_provider_event(events: &mut [ModelStreamEvent], payload: &Value) {
    for event in events {
        if let ModelStreamEvent::Provider { event } = event {
            event.name = "chat.completion".into();
            event.payload = payload.clone();
        }
    }
}

fn insert_complete_request_id(
    events: &mut Vec<ModelStreamEvent>,
    provider: &str,
    request_id: &str,
) {
    let position = events
        .iter()
        .position(|event| matches!(event, ModelStreamEvent::ResponseStarted { .. }))
        .map_or(0, |position| position + 1);
    events.insert(
        position,
        ModelStreamEvent::Provider {
            event: ProviderEvent {
                provider: provider.into(),
                name: "http.request_id".into(),
                payload: json!({"request_id":request_id}),
            },
        },
    );
}

fn normalize_complete_chat_content(
    content: Option<&Value>,
    delta: &mut Map<String, Value>,
) -> Result<(), ModelError> {
    match content {
        None | Some(Value::Null) => Ok(()),
        Some(Value::String(text)) => {
            delta.insert("content".into(), Value::String(text.clone()));
            Ok(())
        }
        Some(Value::Array(parts)) => {
            let mut text = String::new();
            let mut refusal = None;
            for part in parts {
                match part.get("type").and_then(Value::as_str) {
                    Some("text") => text.push_str(required_string(part, "text")?),
                    Some("refusal") => {
                        let value = required_string(part, "refusal")?;
                        if refusal.replace(value.to_owned()).is_some() {
                            return Err(protocol(
                                "complete chat message contains multiple refusal parts",
                            ));
                        }
                    }
                    Some(kind) => {
                        return Err(protocol(format!(
                            "unsupported complete chat content part `{kind}`"
                        )));
                    }
                    None => {
                        return Err(protocol(
                            "complete chat content part is missing a string `type`",
                        ));
                    }
                }
            }
            if !text.is_empty() {
                delta.insert("content".into(), Value::String(text));
            }
            if let Some(refusal) = refusal {
                delta.insert("refusal".into(), Value::String(refusal));
            }
            Ok(())
        }
        Some(_) => Err(protocol(
            "complete chat message content must be a string, array, or null",
        )),
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
        reasoning_tokens: nested_or_top_level_u64(
            usage,
            "completion_tokens_details",
            "reasoning_tokens",
        ),
        cached_input_tokens: nested_or_top_level_u64(
            usage,
            "prompt_tokens_details",
            "cached_tokens",
        ),
        ..ModelUsage::default()
    }
}

fn reasoning_delta(delta: &Value) -> Option<&str> {
    ["reasoning_content", "reasoning", "thinking"]
        .into_iter()
        .find_map(|name| {
            delta
                .get(name)
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
        })
}

fn nested_or_top_level_u64(usage: &Value, details: &str, field: &str) -> u64 {
    usage
        .get(details)
        .and_then(|value| value.get(field))
        .and_then(Value::as_u64)
        .or_else(|| usage.get(field).and_then(Value::as_u64))
        .unwrap_or(0)
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

fn chat_stream_error(provider: &str, value: &Value) -> ModelError {
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("compatible Chat Completions stream failed");
    let mut error = ModelError::local(ModelErrorKind::Provider, message);
    error.provider = Some(provider.into());
    for field in ["type", "code", "param"] {
        if let Some(field_value) = value.get(field).filter(|value| !value.is_null()) {
            error
                .metadata
                .insert(format!("{provider}.error.{field}"), field_value.clone());
        }
    }
    if !error
        .metadata
        .contains_key(&format!("{provider}.error.type"))
        && let Some(error_type) = value
            .get("metadata")
            .and_then(|metadata| metadata.get("error_type"))
    {
        error
            .metadata
            .insert(format!("{provider}.error.type"), error_type.clone());
    }
    error
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
        ContentPart, MediaSource, Message, ModelRef, ModelRequest, ModelStreamAccumulator,
        OutputFormat, Role, ToolCall, ToolResult,
    };
    use schemars::JsonSchema;
    use serde_json::{Value, json};

    use crate::content_projection::decode_content_envelope;
    use crate::content_projection::decode_tool_result_envelope;

    use super::{
        ChatCompletionsDecoder, OpenAiChatDialect, decode_complete_chat_response,
        encode_chat_request, encode_chat_request_for,
    };

    #[derive(JsonSchema)]
    struct ChatAnswer {
        value: String,
        explanation: Option<String>,
    }

    #[test]
    fn encodes_standard_chat_messages() {
        let request = ModelRequest::new(ModelRef::new("qwen", "qwen-plus"), Message::user("hello"));

        let body = encode_chat_request(&request, "qwen").unwrap();

        assert_eq!(body["model"], "qwen-plus");
        assert_eq!(body["messages"][0]["role"], "user");
        assert_eq!(body["messages"][0]["content"], "hello");
        assert_eq!(body["stream"], true);
        assert!(body.get("tool_choice").is_none());
    }

    #[test]
    fn typed_output_schema_is_normalized_for_strict_chat() {
        let example = ChatAnswer {
            value: "ok".into(),
            explanation: None,
        };
        assert_eq!(example.value, "ok");
        assert!(example.explanation.is_none());
        let request = ModelRequest::new(
            ModelRef::new("openai", "chat-model"),
            Message::user("answer"),
        )
        .output_format(OutputFormat::typed::<ChatAnswer>("chat_answer"));

        let body = encode_chat_request(&request, "openai").unwrap();
        let schema = &body["response_format"]["json_schema"]["schema"];

        assert!(schema.get("$schema").is_none());
        assert_eq!(schema["additionalProperties"], false);
        let required = schema["required"].as_array().unwrap();
        assert_eq!(required.len(), 2);
        for name in ["value", "explanation"] {
            assert!(required.iter().any(|value| value.as_str() == Some(name)));
        }
    }

    #[test]
    fn canonical_compatible_namespace_is_merged_once_for_chat() {
        let request = ModelRequest::new(
            ModelRef::new("openai-compatible", "model"),
            Message::user("hello"),
        )
        .provider_option("openai-compatible", json!({"parallel_tool_calls":false}));

        let body = encode_chat_request(&request, "openai-compatible").unwrap();

        assert_eq!(body["parallel_tool_calls"], false);
    }

    #[test]
    fn openai_chat_dialect_uses_current_token_and_usage_fields() {
        let mut request = ModelRequest::new(
            ModelRef::new("openai", "chat-model"),
            Message::user("hello"),
        );
        request.generation.max_output_tokens = Some(128);

        let body = encode_chat_request_for(&request, "openai", OpenAiChatDialect::OpenAi).unwrap();

        assert_eq!(body["max_completion_tokens"], 128);
        assert!(body.get("max_tokens").is_none());
        assert_eq!(body["stream_options"]["include_usage"], true);
    }

    #[test]
    fn chat_dialect_owns_system_role_mapping() {
        let request = ModelRequest::new(
            ModelRef::new("openai", "chat-model"),
            Message::system("be precise"),
        );

        let official =
            encode_chat_request_for(&request, "openai", OpenAiChatDialect::OpenAi).unwrap();
        let compatible =
            encode_chat_request_for(&request, "gateway", OpenAiChatDialect::Compatible).unwrap();

        assert_eq!(official["messages"][0]["role"], "developer");
        assert_eq!(compatible["messages"][0]["role"], "system");
    }

    #[test]
    fn assistant_refusal_replays_as_a_typed_content_part() {
        let message = Message::new(
            Role::Assistant,
            vec![ContentPart::Refusal {
                text: "cannot help".into(),
            }],
        )
        .unwrap();
        let request = ModelRequest::new(ModelRef::new("openai", "chat-model"), message);

        let body = encode_chat_request_for(&request, "openai", OpenAiChatDialect::OpenAi).unwrap();

        assert_eq!(body["messages"][0]["role"], "assistant");
        assert_eq!(body["messages"][0]["content"][0]["type"], "refusal");
        assert_eq!(body["messages"][0]["content"][0]["refusal"], "cannot help");
    }

    #[test]
    fn complete_chat_response_normalizes_refusal_and_usage() {
        let events = decode_complete_chat_response(
            "openai",
            &json!({
                "id":"chat-complete",
                "model":"chat-model",
                "choices":[{
                    "index":0,
                    "message":{
                        "role":"assistant",
                        "content":[{"type":"refusal","refusal":"cannot help"}]
                    },
                    "finish_reason":"stop"
                }],
                "usage":{"prompt_tokens":4,"completion_tokens":2}
            }),
            Some("req-1".into()),
        )
        .unwrap();
        let mut accumulator = ModelStreamAccumulator::new();
        let response = events
            .into_iter()
            .find_map(|event| accumulator.push(event).unwrap())
            .unwrap();

        assert_eq!(
            response.content,
            vec![ContentPart::Refusal {
                text: "cannot help".into()
            }]
        );
        assert_eq!(response.usage.input_tokens, 4);
        assert_eq!(response.usage.output_tokens, 2);
    }

    #[test]
    fn chat_decoder_rejects_duplicate_or_implicit_choice_zero() {
        let mut decoder = ChatCompletionsDecoder::new("gateway");
        let duplicate = decoder
            .decode(json!({
                "model":"model",
                "choices":[
                    {"index":0,"delta":{"content":"a"},"finish_reason":null},
                    {"index":0,"delta":{"content":"b"},"finish_reason":"stop"}
                ]
            }))
            .unwrap_err();
        assert!(duplicate.message.contains("multiple"));

        let mut decoder = ChatCompletionsDecoder::new("gateway");
        let implicit = decoder
            .decode(json!({
                "model":"model",
                "choices":[{"delta":{"content":"a"},"finish_reason":"stop"}]
            }))
            .unwrap_err();
        assert!(implicit.message.contains("index zero"));
    }

    #[test]
    fn chat_choice_errors_fail_before_message_decoding() {
        let error_payload = json!({
            "index":0,
            "error":{"message":"upstream failed","type":"server_error","code":"failed"},
            "finish_reason":null
        });
        let mut decoder = ChatCompletionsDecoder::new("gateway");
        let streaming = decoder
            .decode(json!({"model":"model","choices":[error_payload.clone()]}))
            .unwrap_err();
        assert_eq!(streaming.kind, runifold_model::ModelErrorKind::Provider);
        assert_eq!(streaming.metadata["gateway.error.code"], "failed");

        let complete = decode_complete_chat_response(
            "gateway",
            &json!({"model":"model","choices":[error_payload]}),
            None,
        )
        .unwrap_err();
        assert_eq!(complete.kind, runifold_model::ModelErrorKind::Provider);
        assert_eq!(complete.metadata["gateway.error.type"], "server_error");
    }

    #[test]
    fn tool_results_bridge_every_rich_content_kind() {
        let result = ToolResult {
            call_id: "call-rich".into(),
            name: Some("inspect".into()),
            content: vec![
                ContentPart::Image {
                    source: MediaSource::Base64 {
                        media_type: "image/png".into(),
                        data: "iVBORw0KGgo=".into(),
                    },
                },
                ContentPart::Audio {
                    source: MediaSource::Base64 {
                        media_type: "audio/wav".into(),
                        data: "YXVkaW8=".into(),
                    },
                },
                ContentPart::Document {
                    source: MediaSource::Base64 {
                        media_type: "application/pdf".into(),
                        data: "ZG9j".into(),
                    },
                    name: Some("report.pdf".into()),
                },
            ],
            structured_content: None,
            is_error: false,
            metadata: BTreeMap::new(),
        };
        let message = Message::new(Role::Tool, vec![ContentPart::ToolResult(result)]).unwrap();

        let body = encode_chat_request(
            &ModelRequest::new(ModelRef::new("qwen", "qwen-plus"), message),
            "qwen",
        )
        .unwrap();
        let content = body["messages"][0]["content"].as_str().unwrap();

        let decoded = decode_tool_result_envelope(content).unwrap().unwrap();
        assert_eq!(decoded.content.len(), 3);
        assert_eq!(decoded.name.as_deref(), Some("inspect"));
    }

    #[test]
    fn compatible_chat_projects_ordinary_audio_without_flattening_it() {
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

        let body = encode_chat_request(
            &ModelRequest::new(ModelRef::new("qwen", "qwen-plus"), message),
            "qwen",
        )
        .unwrap();
        let envelope = body["messages"][0]["content"].as_str().unwrap();

        assert!(decode_content_envelope(envelope).unwrap().is_some());
    }

    #[test]
    fn compatible_chat_rejects_invalid_native_image_base64() {
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

        let error = encode_chat_request(
            &ModelRequest::new(ModelRef::new("qwen", "qwen-plus"), message),
            "qwen",
        )
        .unwrap_err();

        assert_eq!(error.kind, runifold_model::ModelErrorKind::InvalidRequest);
    }

    #[test]
    fn compatible_chat_rejects_unsafe_native_image_url() {
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

        let error = encode_chat_request(
            &ModelRequest::new(ModelRef::new("qwen", "qwen-plus"), message),
            "qwen",
        )
        .unwrap_err();

        assert_eq!(error.kind, runifold_model::ModelErrorKind::InvalidRequest);
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

    #[test]
    fn compatible_tool_metadata_round_trips_exactly() {
        let chunks = [json!({
            "id":"chat-gemini",
            "model":"gemini-3",
            "choices":[{"index":0,"delta":{"tool_calls":[{
                "index":0,
                "id":"call-1",
                "type":"function",
                "function":{"name":"lookup","arguments":"{}"},
                "extra_content":{"google":{"thought_signature":"opaque"}}
            }]},"finish_reason":"tool_calls"}]
        })];
        let response = decode_chunks_for("gemini", chunks);
        let ContentPart::ToolCall(call) = &response.content[0] else {
            panic!("fixture must decode a tool call");
        };
        assert_eq!(
            call.metadata["gemini.extra_content"],
            json!({"google":{"thought_signature":"opaque"}})
        );

        let message = Message::new(Role::Assistant, response.content).unwrap();
        let request = ModelRequest::new(ModelRef::new("gemini", "gemini-3"), message);
        let body = encode_chat_request(&request, "gemini").unwrap();
        assert_eq!(
            body["messages"][0]["tool_calls"][0]["extra_content"],
            json!({"google":{"thought_signature":"opaque"}})
        );
        assert!(body["messages"][0]["content"].is_null());
    }

    #[test]
    fn rejects_tool_identity_changes_between_deltas() {
        let mut decoder = ChatCompletionsDecoder::new("compatible");
        decoder
            .decode(json!({
                "id":"chat-1","model":"model",
                "choices":[{"index":0,"delta":{"tool_calls":[{
                    "index":0,"id":"call-1","function":{"name":"lookup","arguments":"{"}
                }]},"finish_reason":null}]
            }))
            .unwrap();

        assert!(
            decoder
                .decode(json!({
                    "id":"chat-1","model":"model",
                    "choices":[{"index":0,"delta":{"tool_calls":[{
                        "index":0,"id":"call-2","function":{"arguments":"}"}
                    }]},"finish_reason":"tool_calls"}]
                }))
                .is_err()
        );
    }

    #[test]
    fn normalizes_reasoning_and_detailed_usage() {
        let chunks = [
            json!({
                "id": "chat_reasoning",
                "model": "deepseek-reasoner",
                "choices": [{
                    "index": 0,
                    "delta": {"reasoning_content": "first "},
                    "finish_reason": null
                }]
            }),
            json!({
                "id": "chat_reasoning",
                "model": "deepseek-reasoner",
                "choices": [{
                    "index": 0,
                    "delta": {"reasoning_content": "then", "content": "answer"},
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": 7,
                    "completion_tokens": 5,
                    "prompt_tokens_details": {"cached_tokens": 3},
                    "completion_tokens_details": {"reasoning_tokens": 2}
                }
            }),
        ];
        let response = decode_chunks_for("deepseek", chunks);

        assert_eq!(response.model.provider, "deepseek");
        assert_eq!(
            response.content,
            vec![
                ContentPart::Reasoning(runifold_model::ReasoningPart {
                    text: Some("first then".into()),
                    signature: None,
                    redacted: false,
                    provider_data: Vec::new(),
                }),
                ContentPart::text("answer"),
            ]
        );
        assert_eq!(response.usage.reasoning_tokens, 2);
        assert_eq!(response.usage.cached_input_tokens, 3);
    }

    #[test]
    fn normalizes_streamed_refusal() {
        let chunks = [
            json!({
                "id":"chat_refusal","model":"model",
                "choices":[{"index":0,"delta":{"refusal":"cannot "},"finish_reason":null}]
            }),
            json!({
                "id":"chat_refusal","model":"model",
                "choices":[{"index":0,"delta":{"refusal":"help"},"finish_reason":"stop"}]
            }),
        ];

        let response = decode_chunks_for("openai", chunks);

        assert_eq!(
            response.content,
            vec![ContentPart::Refusal {
                text: "cannot help".into()
            }]
        );
    }

    #[test]
    fn accepts_top_level_compatible_usage_details() {
        let chunks = [json!({
            "id": "chat_usage",
            "model": "compatible-model",
            "choices": [{
                "index": 0,
                "delta": {"thinking": "step", "content": "done"},
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 4,
                "completion_tokens": 3,
                "cached_tokens": 2,
                "reasoning_tokens": 1
            }
        })];
        let response = decode_chunks(chunks);

        assert_eq!(response.usage.reasoning_tokens, 1);
        assert_eq!(response.usage.cached_input_tokens, 2);
    }

    #[test]
    fn terminal_chunk_allows_only_empty_usage_tail_chunks() {
        let mut decoder = ChatCompletionsDecoder::new("qwen");
        decoder
            .decode(json!({
                "id":"chat-1",
                "model":"qwen-plus",
                "choices":[{"index":0,"delta":{"content":"done"},"finish_reason":"stop"}]
            }))
            .unwrap();
        decoder
            .decode(json!({
                "id":"chat-1",
                "model":"qwen-plus",
                "choices":[],
                "usage":{"prompt_tokens":2,"completion_tokens":1}
            }))
            .unwrap();

        let error = decoder
            .decode(json!({
                "id":"chat-1",
                "model":"qwen-plus",
                "choices":[{"index":0,"delta":{"content":"late"},"finish_reason":null}]
            }))
            .unwrap_err();
        assert_eq!(error.kind, runifold_model::ModelErrorKind::Protocol);
    }

    #[test]
    fn rejects_identity_changes_between_chunks() {
        let mut decoder = ChatCompletionsDecoder::new("qwen");
        decoder
            .decode(json!({
                "id":"chat-1","model":"qwen-plus",
                "choices":[{"index":0,"delta":{"content":"a"},"finish_reason":null}]
            }))
            .unwrap();

        assert!(
            decoder
                .decode(json!({
                    "id":"chat-2","model":"qwen-plus",
                    "choices":[{"index":0,"delta":{"content":"b"},"finish_reason":"stop"}]
                }))
                .is_err()
        );
    }

    #[test]
    fn classifies_in_stream_provider_errors_without_partial_success() {
        let mut decoder = ChatCompletionsDecoder::new("openrouter");
        let error = decoder
            .decode(json!({
                "error": {
                    "type":"upstream_error",
                    "code": 502,
                    "message": "upstream disconnected",
                    "param":"tools[0]",
                    "metadata": {"error_type":"provider_unavailable"}
                }
            }))
            .unwrap_err();

        assert_eq!(error.kind, runifold_model::ModelErrorKind::Provider);
        assert_eq!(error.provider.as_deref(), Some("openrouter"));
        assert_eq!(error.metadata["openrouter.error.type"], "upstream_error");
        assert_eq!(error.metadata["openrouter.error.param"], "tools[0]");
    }

    fn decode_chunks(chunks: impl IntoIterator<Item = Value>) -> runifold_model::ModelResponse {
        decode_chunks_for("qwen", chunks)
    }

    fn decode_chunks_for(
        provider: &str,
        chunks: impl IntoIterator<Item = Value>,
    ) -> runifold_model::ModelResponse {
        let mut decoder = ChatCompletionsDecoder::new(provider);
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
