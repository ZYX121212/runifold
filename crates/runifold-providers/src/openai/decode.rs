//! `OpenAI` Responses streaming decoder.

use std::collections::{BTreeMap, BTreeSet};

use runifold_model::{
    ContentBlockKind, ContentPart, FinishReason, MediaSource, ModelError, ModelErrorKind, ModelRef,
    ModelStreamEvent, ModelUsage, ProviderEvent,
};
use serde_json::Value;

use super::OpenAiResponsesDialect;

/// Stateful translator from `OpenAI` Responses SSE payloads to canonical events.
#[derive(Debug)]
pub struct OpenAiEventDecoder {
    provider: String,
    dialect: OpenAiResponsesDialect,
    started: bool,
    completed: bool,
    next_index: u32,
    block_indices: BTreeMap<String, u32>,
    open_content_indices: BTreeSet<u32>,
    open_tool_indices: BTreeSet<u32>,
    saw_tool_call: bool,
    request_id: Option<String>,
    last_sequence_number: Option<u64>,
    response_id: Option<String>,
    response_model: Option<String>,
}

impl OpenAiEventDecoder {
    /// Creates an empty event decoder.
    pub fn new() -> Self {
        Self::for_provider("openai")
    }

    /// Creates a decoder that retains a custom provider identity.
    pub fn for_provider(provider: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            dialect: OpenAiResponsesDialect::Compatible,
            started: false,
            completed: false,
            next_index: 0,
            block_indices: BTreeMap::new(),
            open_content_indices: BTreeSet::new(),
            open_tool_indices: BTreeSet::new(),
            saw_tool_call: false,
            request_id: None,
            last_sequence_number: None,
            response_id: None,
            response_model: None,
        }
    }

    /// Selects strict public-OpenAI or compatibility event validation.
    #[must_use]
    pub const fn with_dialect(mut self, dialect: OpenAiResponsesDialect) -> Self {
        self.dialect = dialect;
        self
    }

    /// Attaches the HTTP request ID to the canonical provider-event stream.
    #[must_use]
    pub fn with_request_id(mut self, request_id: Option<String>) -> Self {
        self.request_id = request_id;
        self
    }

    /// Decodes one `OpenAI` SSE JSON payload.
    ///
    /// One provider event may become multiple canonical events, for example a
    /// usage update followed by response completion.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError`] for malformed known events, failed responses, or
    /// deltas that cannot be associated with a started content block.
    pub fn decode(&mut self, payload: Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        let provider = self.provider.clone();
        let event_type = payload
            .get("type")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let raw_payload = redact_generated_media(payload.clone());
        self.decode_inner(payload)
            .map(|mut events| {
                for event in &mut events {
                    match event {
                        ModelStreamEvent::ResponseStarted { model, .. } => {
                            model.provider.clone_from(&provider);
                        }
                        ModelStreamEvent::Provider { event } => {
                            event.provider.clone_from(&provider);
                        }
                        _ => {}
                    }
                }
                if let Some(event_type) = event_type
                    && is_known_success_event(&event_type)
                    && !contains_raw_event(&events, &event_type, &raw_payload)
                {
                    let position = events
                        .iter()
                        .position(|event| {
                            matches!(event, ModelStreamEvent::ResponseCompleted { .. })
                        })
                        .unwrap_or(events.len());
                    events.insert(
                        position,
                        provider_event_for(&provider, &event_type, raw_payload),
                    );
                }
                events
            })
            .map_err(|mut error| {
                error.provider = Some(provider);
                error
            })
    }

    fn decode_inner(&mut self, payload: Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        let event_type = payload
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| protocol("OpenAI stream event is missing a string `type`"))?
            .to_owned();
        self.validate_sequence_number(&payload)?;
        if self.completed {
            return Err(protocol(format!(
                "OpenAI event `{event_type}` arrived after response completion"
            )));
        }

        match event_type.as_str() {
            "response.created" => self.response_started(&payload),
            "response.content_part.added" => self.content_part_started(&payload),
            "response.output_text.delta" => self.text_delta(&payload),
            "response.refusal.delta" => self.refusal_delta(&payload),
            "response.content_part.done" => self.content_part_completed(&payload),
            "response.output_item.added" => self.output_item_started(&payload),
            "response.output_item.done" => self.output_item_completed(&payload),
            "response.function_call_arguments.delta" => self.tool_arguments_delta(&payload),
            "response.function_call_arguments.done" => self.tool_call_completed(&payload),
            "response.completed" => self.response_completed(&payload, false),
            "response.incomplete" => self.response_completed(&payload, true),
            "response.failed" | "error" => Err(provider_failure(&self.provider, &payload)),
            _ => Ok(vec![provider_event(
                &event_type,
                redact_generated_media(payload),
            )]),
        }
    }

    fn response_started(&mut self, payload: &Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        if self.started {
            return Err(protocol("received response.created more than once"));
        }
        let response = object(payload, "response")?;
        let model = string(response, "model")?;
        let response_id = optional_string(response, "id");
        self.response_id.clone_from(&response_id);
        self.response_model = Some(model.into());
        self.started = true;
        let mut events = vec![ModelStreamEvent::ResponseStarted {
            id: response_id,
            model: ModelRef::new("openai", model),
        }];
        if let Some(request_id) = self.request_id.take() {
            events.push(provider_event(
                "http.request_id",
                serde_json::json!({"x_request_id": request_id}),
            ));
        }
        Ok(events)
    }

    fn content_part_started(
        &mut self,
        payload: &Value,
    ) -> Result<Vec<ModelStreamEvent>, ModelError> {
        let output_index = integer(payload, "output_index")?;
        let content_index = integer(payload, "content_index")?;
        let part = object(payload, "part")?;
        let kind = match string(part, "type")? {
            "output_text" => ContentBlockKind::Text,
            "refusal" => ContentBlockKind::Refusal,
            other => {
                return Ok(vec![
                    provider_event("response.content_part.added", payload.clone()),
                    warning_event(
                        "openai.unknown_content_part",
                        format!("preserved unsupported OpenAI content part `{other}`"),
                    ),
                ]);
            }
        };
        let key = content_key(output_index, content_index);
        let index = self.allocate(key)?;
        self.open_content_indices.insert(index);
        Ok(vec![ModelStreamEvent::ContentBlockStarted { index, kind }])
    }

    fn text_delta(&self, payload: &Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        let index = self.content_index(payload)?;
        Ok(vec![ModelStreamEvent::TextDelta {
            index,
            text: string(payload, "delta")?.into(),
        }])
    }

    fn refusal_delta(&self, payload: &Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        let index = self.content_index(payload)?;
        Ok(vec![ModelStreamEvent::RefusalDelta {
            index,
            text: string(payload, "delta")?.into(),
        }])
    }

    fn content_part_completed(
        &mut self,
        payload: &Value,
    ) -> Result<Vec<ModelStreamEvent>, ModelError> {
        if payload
            .get("part")
            .and_then(|part| part.get("type"))
            .and_then(Value::as_str)
            .is_some_and(|kind| !matches!(kind, "output_text" | "refusal"))
        {
            return Ok(vec![provider_event(
                "response.content_part.done",
                payload.clone(),
            )]);
        }
        let index = self.content_index(payload)?;
        if !self.open_content_indices.remove(&index) {
            return Err(protocol(format!(
                "OpenAI completed content block {index} more than once"
            )));
        }
        Ok(vec![ModelStreamEvent::ContentBlockCompleted { index }])
    }

    fn output_item_started(
        &mut self,
        payload: &Value,
    ) -> Result<Vec<ModelStreamEvent>, ModelError> {
        let item = object(payload, "item")?;
        if string(item, "type")? != "function_call" {
            return Ok(vec![provider_event(
                "response.output_item.added",
                payload.clone(),
            )]);
        }

        let output_index = integer(payload, "output_index")?;
        let item_id = string(item, "id")?;
        let call_id = optional_string(item, "call_id").unwrap_or_else(|| item_id.into());
        let name = string(item, "name")?;
        let index = self.allocate(tool_key(item_id))?;
        self.block_indices
            .insert(tool_output_key(output_index), index);
        self.open_tool_indices.insert(index);
        self.saw_tool_call = true;
        Ok(vec![
            ModelStreamEvent::ContentBlockStarted {
                index,
                kind: ContentBlockKind::ToolCall {
                    id: call_id,
                    name: name.into(),
                },
            },
            ModelStreamEvent::ContentBlockMetadata {
                index,
                metadata: function_call_metadata(&self.provider, item),
            },
        ])
    }

    fn output_item_completed(
        &mut self,
        payload: &Value,
    ) -> Result<Vec<ModelStreamEvent>, ModelError> {
        let item = object(payload, "item")?;
        let item_type = string(item, "type")?;
        if item_type != "function_call" {
            if item_type == "message" {
                require_completed_item(item, "streamed message")?;
                return Ok(vec![provider_event(
                    "response.output_item.done",
                    payload.clone(),
                )]);
            }
            if item_type == "image_generation_call" {
                require_completed_item(item, "streamed image generation call")?;
                let index =
                    self.allocate(format!("image:{}", integer(payload, "output_index")?))?;
                return Ok(vec![
                    ModelStreamEvent::ContentPartCompleted {
                        index,
                        part: generated_image(string(item, "result")?)?,
                    },
                    provider_event(
                        "response.output_item.done",
                        redact_generated_media(payload.clone()),
                    ),
                ]);
            }
            let identity = optional_string(item, "id").unwrap_or_else(|| {
                integer(payload, "output_index")
                    .map_or_else(|_| "unknown".into(), |value| value.to_string())
            });
            let index = self.allocate(format!("opaque:{identity}"))?;
            return Ok(vec![ModelStreamEvent::ContentPartCompleted {
                index,
                part: provider_input_item(&self.provider, item.clone()),
            }]);
        }
        require_completed_item(item, "streamed function call")?;
        let index = self.tool_index(payload)?;
        let mut events = Vec::new();
        if self.open_tool_indices.remove(&index) {
            if let Some(arguments) = optional_string(item, "arguments") {
                events.push(ModelStreamEvent::ToolArgumentsCompleted {
                    index,
                    json: arguments,
                });
            }
            events.push(ModelStreamEvent::ContentBlockCompleted { index });
        }
        events.push(ModelStreamEvent::ContentBlockMetadata {
            index,
            metadata: function_call_metadata(&self.provider, item),
        });
        Ok(events)
    }

    fn tool_arguments_delta(&self, payload: &Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        let index = self.tool_index(payload)?;
        Ok(vec![ModelStreamEvent::ToolArgumentsDelta {
            index,
            json: string(payload, "delta")?.into(),
        }])
    }

    fn tool_call_completed(
        &mut self,
        payload: &Value,
    ) -> Result<Vec<ModelStreamEvent>, ModelError> {
        let index = self.tool_index(payload)?;
        if !self.open_tool_indices.remove(&index) {
            return Err(protocol(format!(
                "OpenAI completed tool-call block {index} more than once"
            )));
        }
        let mut events = Vec::new();
        if let Some(arguments) = optional_string(payload, "arguments") {
            events.push(ModelStreamEvent::ToolArgumentsCompleted {
                index,
                json: arguments,
            });
        }
        events.extend([
            ModelStreamEvent::ContentBlockMetadata {
                index,
                metadata: BTreeMap::from([(
                    format!("{}.status", self.provider),
                    Value::String("completed".into()),
                )]),
            },
            ModelStreamEvent::ContentBlockCompleted { index },
        ]);
        Ok(events)
    }

    fn response_completed(
        &mut self,
        payload: &Value,
        incomplete: bool,
    ) -> Result<Vec<ModelStreamEvent>, ModelError> {
        if !self.started {
            return Err(protocol(
                "OpenAI response completed before response.created",
            ));
        }
        if !self.open_tool_indices.is_empty() {
            return Err(protocol(
                "OpenAI response completed with unfinished function calls",
            ));
        }
        if !self.open_content_indices.is_empty() {
            return Err(protocol(
                "OpenAI response completed with unfinished message content",
            ));
        }
        if incomplete && self.saw_tool_call {
            return Err(protocol(
                "OpenAI incomplete response contained function calls that are not safe to execute",
            ));
        }
        let response = payload.get("response").unwrap_or(&Value::Null);
        self.validate_terminal_identity(response)?;
        validate_terminal_status(response, incomplete, self.dialect)?;
        let usage = decode_usage(response.get("usage"));
        let finish_reason = if incomplete {
            incomplete_reason(response)
        } else if self.saw_tool_call {
            FinishReason::ToolCalls
        } else {
            FinishReason::Stop
        };
        self.completed = true;
        Ok(vec![
            ModelStreamEvent::UsageUpdated { usage },
            ModelStreamEvent::ResponseCompleted {
                finish_reason,
                provider_metadata: response_metadata(response),
            },
        ])
    }

    /// Validates that a Responses stream reached exactly one terminal event.
    pub(crate) fn finish(&self) -> Result<(), ModelError> {
        if !self.started {
            return Err(protocol("OpenAI stream ended before response.created"));
        }
        if !self.completed {
            return Err(protocol(
                "OpenAI stream ended before a terminal response event",
            ));
        }
        if !self.open_tool_indices.is_empty() {
            return Err(protocol(
                "OpenAI stream ended with unfinished function calls",
            ));
        }
        if !self.open_content_indices.is_empty() {
            return Err(protocol(
                "OpenAI stream ended with unfinished message content",
            ));
        }
        Ok(())
    }

    fn validate_sequence_number(&mut self, payload: &Value) -> Result<(), ModelError> {
        let Some(sequence) = payload.get("sequence_number") else {
            if self.dialect == OpenAiResponsesDialect::OpenAi {
                return Err(protocol(
                    "OpenAI event is missing required `sequence_number`",
                ));
            }
            return Ok(());
        };
        let sequence = sequence
            .as_u64()
            .ok_or_else(|| protocol("OpenAI event `sequence_number` must be unsigned"))?;
        if self
            .last_sequence_number
            .is_some_and(|previous| sequence <= previous)
        {
            return Err(protocol(
                "OpenAI event sequence was duplicated or arrived out of order",
            ));
        }
        self.last_sequence_number = Some(sequence);
        Ok(())
    }

    fn validate_terminal_identity(&self, response: &Value) -> Result<(), ModelError> {
        if let Some(id) = response.get("id").and_then(Value::as_str)
            && self
                .response_id
                .as_deref()
                .is_some_and(|expected| expected != id)
        {
            return Err(protocol("OpenAI terminal response changed response ID"));
        }
        if let Some(model) = response.get("model").and_then(Value::as_str)
            && self
                .response_model
                .as_deref()
                .is_some_and(|expected| expected != model)
        {
            return Err(protocol("OpenAI terminal response changed model identity"));
        }
        Ok(())
    }

    fn content_index(&self, payload: &Value) -> Result<u32, ModelError> {
        let key = content_key(
            integer(payload, "output_index")?,
            integer(payload, "content_index")?,
        );
        self.lookup(&key)
    }

    fn tool_index(&self, payload: &Value) -> Result<u32, ModelError> {
        if let Some(item_id) = payload.get("item_id").and_then(Value::as_str)
            && let Ok(index) = self.lookup(&tool_key(item_id))
        {
            return Ok(index);
        }
        self.lookup(&tool_output_key(integer(payload, "output_index")?))
    }

    fn allocate(&mut self, key: String) -> Result<u32, ModelError> {
        if self.block_indices.contains_key(&key) {
            return Err(protocol(format!(
                "OpenAI stream started content block `{key}` more than once"
            )));
        }
        let index = self.next_index;
        self.next_index = self
            .next_index
            .checked_add(1)
            .ok_or_else(|| protocol("canonical content index overflow"))?;
        self.block_indices.insert(key, index);
        Ok(index)
    }

    fn lookup(&self, key: &str) -> Result<u32, ModelError> {
        self.block_indices
            .get(key)
            .copied()
            .ok_or_else(|| protocol(format!("OpenAI delta targeted unknown block `{key}`")))
    }
}

fn validate_terminal_status(
    response: &Value,
    incomplete: bool,
    dialect: OpenAiResponsesDialect,
) -> Result<(), ModelError> {
    let expected = if incomplete {
        "incomplete"
    } else {
        "completed"
    };
    match response.get("status") {
        Some(Value::String(status)) if status == expected => {}
        Some(Value::String(status)) => {
            return Err(protocol(format!(
                "OpenAI terminal event expected response status `{expected}`, received `{status}`"
            )));
        }
        Some(_) => return Err(protocol("OpenAI terminal response status must be a string")),
        None if dialect == OpenAiResponsesDialect::OpenAi => {
            return Err(protocol(
                "OpenAI terminal response is missing required `status`",
            ));
        }
        None => {}
    }
    Ok(())
}

fn is_known_success_event(event_type: &str) -> bool {
    matches!(
        event_type,
        "response.created"
            | "response.content_part.added"
            | "response.output_text.delta"
            | "response.refusal.delta"
            | "response.content_part.done"
            | "response.output_item.added"
            | "response.output_item.done"
            | "response.function_call_arguments.delta"
            | "response.function_call_arguments.done"
            | "response.completed"
            | "response.incomplete"
    )
}

fn contains_raw_event(events: &[ModelStreamEvent], name: &str, payload: &Value) -> bool {
    events.iter().any(|event| {
        matches!(
            event,
            ModelStreamEvent::Provider { event }
                if event.name == name && event.payload == *payload
        )
    })
}

fn provider_event_for(provider: &str, name: &str, payload: Value) -> ModelStreamEvent {
    ModelStreamEvent::Provider {
        event: ProviderEvent {
            provider: provider.into(),
            name: name.into(),
            payload,
        },
    }
}

impl Default for OpenAiEventDecoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Normalizes one complete Responses API object into the canonical event model.
///
/// # Errors
///
/// Returns [`ModelError`] when required response fields are malformed or the
/// Provider reports a failed response.
#[cfg(test)]
pub(crate) fn decode_complete_response(
    provider: &str,
    payload: &Value,
    request_id: Option<String>,
) -> Result<Vec<ModelStreamEvent>, ModelError> {
    decode_complete_response_for(
        provider,
        payload,
        request_id,
        OpenAiResponsesDialect::Compatible,
    )
}

pub(crate) fn decode_complete_response_for(
    provider: &str,
    payload: &Value,
    request_id: Option<String>,
    dialect: OpenAiResponsesDialect,
) -> Result<Vec<ModelStreamEvent>, ModelError> {
    let status = optional_string(payload, "status");
    if payload.get("error").is_some_and(|error| !error.is_null())
        || status.as_deref() == Some("failed")
    {
        let envelope = serde_json::json!({"error": payload.get("error")});
        let mut error = provider_failure(provider, &envelope);
        error.provider = Some(provider.into());
        return Err(error);
    }
    if status
        .as_deref()
        .is_some_and(|status| !matches!(status, "completed" | "incomplete"))
    {
        return Err(protocol(format!(
            "complete OpenAI response is not terminal: status `{}`",
            status.as_deref().unwrap_or("unknown")
        )));
    }
    if status.is_none() && dialect == OpenAiResponsesDialect::OpenAi {
        return Err(protocol(
            "complete OpenAI response is missing required terminal `status`",
        ));
    }

    let model = string(payload, "model")?;
    let mut events = vec![ModelStreamEvent::ResponseStarted {
        id: optional_string(payload, "id"),
        model: ModelRef::new(provider, model),
    }];
    if let Some(request_id) = request_id {
        events.push(provider_event_for(
            provider,
            "http.request_id",
            serde_json::json!({"x_request_id": request_id}),
        ));
    }

    let output = payload
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(|| protocol("complete OpenAI response field `output` must be an array"))?;
    let mut next_index = 0_u32;
    let incomplete = status.as_deref() == Some("incomplete");
    let saw_tool_call =
        decode_complete_output(provider, output, incomplete, &mut next_index, &mut events)?;

    reject_incomplete_tool_calls(status.as_deref(), saw_tool_call)?;

    let usage = decode_usage(payload.get("usage"));
    let finish_reason = if incomplete {
        incomplete_reason(payload)
    } else if saw_tool_call {
        FinishReason::ToolCalls
    } else {
        FinishReason::Stop
    };
    events.push(provider_event_for(
        provider,
        "response.complete",
        redact_generated_media(payload.clone()),
    ));
    events.push(ModelStreamEvent::UsageUpdated { usage });
    events.push(ModelStreamEvent::ResponseCompleted {
        finish_reason,
        provider_metadata: response_metadata(payload),
    });
    Ok(events)
}

fn decode_complete_output(
    provider: &str,
    output: &[Value],
    response_incomplete: bool,
    next_index: &mut u32,
    events: &mut Vec<ModelStreamEvent>,
) -> Result<bool, ModelError> {
    let mut saw_tool_call = false;
    for item in output {
        match item.get("type").and_then(Value::as_str) {
            Some("message") => {
                decode_complete_message(provider, item, response_incomplete, next_index, events)?;
            }
            Some("function_call") => {
                require_completed_item(item, "complete function call")?;
                let index = take_index(next_index)?;
                let id = optional_string(item, "call_id")
                    .or_else(|| optional_string(item, "id"))
                    .ok_or_else(|| protocol("complete function call is missing an identity"))?;
                let name = string(item, "name")?.to_owned();
                let arguments = string(item, "arguments")?.to_owned();
                events.push(ModelStreamEvent::ContentBlockStarted {
                    index,
                    kind: ContentBlockKind::ToolCall { id, name },
                });
                events.push(ModelStreamEvent::ToolArgumentsDelta {
                    index,
                    json: arguments,
                });
                events.push(ModelStreamEvent::ContentBlockMetadata {
                    index,
                    metadata: function_call_metadata(provider, item),
                });
                events.push(ModelStreamEvent::ContentBlockCompleted { index });
                saw_tool_call = true;
            }
            Some("image_generation_call") => {
                let index = take_index(next_index)?;
                let data = string(item, "result")?.to_owned();
                events.push(ModelStreamEvent::ContentPartCompleted {
                    index,
                    part: generated_image(&data)?,
                });
            }
            Some(_) => {
                let index = take_index(next_index)?;
                events.push(ModelStreamEvent::ContentPartCompleted {
                    index,
                    part: provider_input_item(provider, item.clone()),
                });
            }
            None => {
                return Err(protocol(
                    "complete OpenAI response output item is missing a string `type`",
                ));
            }
        }
    }
    Ok(saw_tool_call)
}

fn reject_incomplete_tool_calls(
    status: Option<&str>,
    saw_tool_call: bool,
) -> Result<(), ModelError> {
    if status == Some("incomplete") && saw_tool_call {
        return Err(protocol(
            "incomplete OpenAI response contained function calls that are not safe to execute",
        ));
    }
    Ok(())
}

fn decode_complete_message(
    provider: &str,
    message: &Value,
    response_incomplete: bool,
    next_index: &mut u32,
    events: &mut Vec<ModelStreamEvent>,
) -> Result<(), ModelError> {
    require_terminal_message(message, response_incomplete)?;
    let content = message
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| protocol("complete response message content must be an array"))?;
    for part in content {
        let index = take_index(next_index)?;
        match part.get("type").and_then(Value::as_str) {
            Some("output_text") => {
                events.push(ModelStreamEvent::ContentBlockStarted {
                    index,
                    kind: ContentBlockKind::Text,
                });
                events.push(ModelStreamEvent::TextDelta {
                    index,
                    text: string(part, "text")?.into(),
                });
                events.push(ModelStreamEvent::ContentBlockCompleted { index });
                if part
                    .get("annotations")
                    .is_some_and(|value| !value.is_null())
                {
                    events.push(provider_event_for(
                        provider,
                        "response.output_text.annotations",
                        part.clone(),
                    ));
                }
            }
            Some("refusal") => {
                events.push(ModelStreamEvent::ContentBlockStarted {
                    index,
                    kind: ContentBlockKind::Refusal,
                });
                events.push(ModelStreamEvent::RefusalDelta {
                    index,
                    text: string(part, "refusal")?.into(),
                });
                events.push(ModelStreamEvent::ContentBlockCompleted { index });
            }
            Some(kind) => events.push(provider_event_for(
                provider,
                &format!("response.content_part.{kind}"),
                part.clone(),
            )),
            None => {
                return Err(protocol(
                    "complete response content part is missing a string `type`",
                ));
            }
        }
    }
    Ok(())
}

fn require_terminal_message(message: &Value, response_incomplete: bool) -> Result<(), ModelError> {
    match message.get("status").and_then(Value::as_str) {
        Some("completed") => Ok(()),
        Some("incomplete") if response_incomplete => Ok(()),
        Some(status) => Err(protocol(format!(
            "complete message has non-terminal status `{status}` inconsistent with the terminal response"
        ))),
        None => Err(protocol(
            "complete message is missing required completion status",
        )),
    }
}

fn take_index(next_index: &mut u32) -> Result<u32, ModelError> {
    let index = *next_index;
    *next_index = next_index
        .checked_add(1)
        .ok_or_else(|| protocol("canonical content index overflow"))?;
    Ok(index)
}

fn redact_generated_media(mut value: Value) -> Value {
    match &mut value {
        Value::Array(values) => {
            for value in values {
                *value = redact_generated_media(value.take());
            }
        }
        Value::Object(object) => {
            let is_image = object
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| kind.contains("image_generation_call"));
            for key in ["partial_image_b64", "image_base64"] {
                redact_base64_field(object, key);
            }
            if is_image {
                redact_base64_field(object, "result");
            }
            for value in object.values_mut() {
                *value = redact_generated_media(value.take());
            }
        }
        _ => {}
    }
    value
}

fn redact_base64_field(object: &mut serde_json::Map<String, Value>, key: &str) {
    if let Some(Value::String(data)) = object.get_mut(key) {
        let encoded_len = data.len();
        *data = format!("[redacted base64: {encoded_len} chars]");
    }
}

fn decode_usage(usage: Option<&Value>) -> ModelUsage {
    let usage = usage.unwrap_or(&Value::Null);
    ModelUsage {
        input_tokens: unsigned(usage, "input_tokens"),
        output_tokens: unsigned(usage, "output_tokens"),
        reasoning_tokens: usage
            .get("output_tokens_details")
            .map_or(0, |details| unsigned(details, "reasoning_tokens")),
        cached_input_tokens: usage
            .get("input_tokens_details")
            .map_or(0, |details| unsigned(details, "cached_tokens")),
        ..ModelUsage::default()
    }
}

fn incomplete_reason(response: &Value) -> FinishReason {
    match response
        .get("incomplete_details")
        .and_then(|details| details.get("reason"))
        .and_then(Value::as_str)
    {
        Some("max_output_tokens") => FinishReason::Length,
        Some("content_filter") => FinishReason::ContentFilter,
        Some(other) => FinishReason::Other(other.into()),
        None => FinishReason::Unknown,
    }
}

fn response_metadata(response: &Value) -> BTreeMap<String, Value> {
    let mut metadata = BTreeMap::new();
    for name in ["status", "service_tier"] {
        if let Some(value) = response.get(name) {
            metadata.insert(format!("openai.{name}"), value.clone());
        }
    }
    metadata
}

fn function_call_metadata(provider: &str, item: &Value) -> BTreeMap<String, Value> {
    let mut metadata = BTreeMap::new();
    for name in ["id", "status", "caller"] {
        if let Some(value) = item.get(name).filter(|value| !value.is_null()) {
            metadata.insert(format!("{provider}.{name}"), value.clone());
        }
    }
    metadata
}

fn require_completed_item(item: &Value, context: &str) -> Result<(), ModelError> {
    match item.get("status").and_then(Value::as_str) {
        Some("completed") => Ok(()),
        Some(status) => Err(protocol(format!(
            "{context} has non-terminal status `{status}`"
        ))),
        None => Err(protocol(format!(
            "{context} is missing required completion status"
        ))),
    }
}

fn provider_input_item(provider: &str, item: Value) -> ContentPart {
    ContentPart::ProviderOpaque(runifold_model::ProviderData {
        provider: provider.into(),
        kind: "input_item".into(),
        value: item,
    })
}

fn provider_event(name: &str, payload: Value) -> ModelStreamEvent {
    ModelStreamEvent::Provider {
        event: ProviderEvent {
            provider: "openai".into(),
            name: name.into(),
            payload,
        },
    }
}

fn warning_event(code: &str, message: String) -> ModelStreamEvent {
    ModelStreamEvent::Warning {
        warning: runifold_model::ModelWarning {
            code: code.into(),
            message,
            metadata: BTreeMap::new(),
        },
    }
}

fn provider_failure(provider: &str, payload: &Value) -> ModelError {
    let error = payload
        .get("response")
        .and_then(|response| response.get("error"))
        .or_else(|| payload.get("error"))
        .unwrap_or(payload);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("OpenAI response failed");
    let mut model_error = ModelError::local(ModelErrorKind::Provider, message);
    model_error.provider = Some(provider.into());
    for name in ["type", "code", "param"] {
        if let Some(value) = error.get(name).filter(|value| !value.is_null()) {
            model_error
                .metadata
                .insert(format!("{provider}.error.{name}"), value.clone());
        }
    }
    model_error
}

fn generated_image(data: &str) -> Result<ContentPart, ModelError> {
    let media_type = if data.starts_with("iVBORw0KGgo") {
        "image/png"
    } else if data.starts_with("/9j/") {
        "image/jpeg"
    } else if data.starts_with("UklGR") {
        "image/webp"
    } else {
        return Err(protocol(
            "OpenAI image generation returned an unrecognized image encoding",
        ));
    };
    Ok(ContentPart::Image {
        source: MediaSource::Base64 {
            media_type: media_type.into(),
            data: data.into(),
        },
    })
}

fn object<'a>(value: &'a Value, name: &str) -> Result<&'a Value, ModelError> {
    value
        .get(name)
        .filter(|field| field.is_object())
        .ok_or_else(|| protocol(format!("OpenAI event field `{name}` must be an object")))
}

fn string<'a>(value: &'a Value, name: &str) -> Result<&'a str, ModelError> {
    value
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| protocol(format!("OpenAI event field `{name}` must be a string")))
}

fn optional_string(value: &Value, name: &str) -> Option<String> {
    value.get(name).and_then(Value::as_str).map(String::from)
}

fn integer(value: &Value, name: &str) -> Result<u64, ModelError> {
    value.get(name).and_then(Value::as_u64).ok_or_else(|| {
        protocol(format!(
            "OpenAI event field `{name}` must be an unsigned integer"
        ))
    })
}

fn unsigned(value: &Value, name: &str) -> u64 {
    value.get(name).and_then(Value::as_u64).unwrap_or(0)
}

fn content_key(output_index: u64, content_index: u64) -> String {
    format!("content:{output_index}:{content_index}")
}

fn tool_key(item_id: &str) -> String {
    format!("tool:{item_id}")
}

fn tool_output_key(output_index: u64) -> String {
    format!("tool-output:{output_index}")
}

fn protocol(message: impl Into<String>) -> ModelError {
    let mut error = ModelError::local(ModelErrorKind::Protocol, message);
    error.provider = Some("openai".into());
    error
}

#[cfg(test)]
mod tests {
    use runifold_model::{
        ContentPart, FinishReason, ModelErrorKind, ModelStreamAccumulator, ModelStreamEvent,
        ToolCall,
    };

    use super::OpenAiEventDecoder;

    #[test]
    fn decodes_text_and_usage_into_a_complete_response() {
        let payloads = [
            serde_json::json!({
                "type": "response.created",
                "response": {"id": "resp_1", "model": "test-model"}
            }),
            serde_json::json!({
                "type": "response.content_part.added",
                "output_index": 0,
                "content_index": 0,
                "part": {"type": "output_text", "text": ""}
            }),
            serde_json::json!({
                "type": "response.output_text.delta",
                "output_index": 0,
                "content_index": 0,
                "delta": "hello"
            }),
            serde_json::json!({
                "type": "response.content_part.done",
                "output_index": 0,
                "content_index": 0,
                "part": {"type": "output_text", "text": "hello"}
            }),
            serde_json::json!({
                "type": "response.completed",
                "response": {
                    "status": "completed",
                    "usage": {
                        "input_tokens": 3,
                        "output_tokens": 2,
                        "input_tokens_details": {"cached_tokens": 1},
                        "output_tokens_details": {"reasoning_tokens": 1}
                    }
                }
            }),
        ];

        let response = decode_response(payloads);

        assert_eq!(response.content, vec![ContentPart::text("hello")]);
        assert_eq!(response.finish_reason, FinishReason::Stop);
        assert_eq!(response.usage.input_tokens, 3);
        assert_eq!(response.usage.reasoning_tokens, 1);
        assert_eq!(response.usage.cached_input_tokens, 1);
    }

    #[test]
    fn decodes_fragmented_function_arguments() {
        let payloads = [
            serde_json::json!({
                "type": "response.created",
                "response": {"id": "resp_2", "model": "test-model"}
            }),
            serde_json::json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "item_1",
                    "call_id": "call_1",
                    "name": "lookup",
                    "arguments": "",
                    "status": "in_progress"
                }
            }),
            serde_json::json!({
                "type": "response.function_call_arguments.delta",
                "item_id": "item_1",
                "output_index": 0,
                "delta": "{\"value\":"
            }),
            serde_json::json!({
                "type": "response.function_call_arguments.delta",
                "item_id": "item_1",
                "output_index": 0,
                "delta": "7}"
            }),
            serde_json::json!({
                "type": "response.function_call_arguments.done",
                "item_id": "item_1",
                "output_index": 0,
                "arguments": "{\"value\":7}"
            }),
            serde_json::json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "item_1",
                    "call_id": "call_1",
                    "name": "lookup",
                    "arguments": "{\"value\":7}",
                    "status": "completed"
                }
            }),
            serde_json::json!({
                "type": "response.completed",
                "response": {"status": "completed", "usage": {}}
            }),
        ];

        let response = decode_response(payloads);

        assert_eq!(response.finish_reason, FinishReason::ToolCalls);
        assert_eq!(
            response.content,
            vec![ContentPart::ToolCall(ToolCall {
                id: "call_1".into(),
                name: "lookup".into(),
                arguments: serde_json::json!({"value": 7}),
                raw_arguments: Some("{\"value\":7}".into()),
                metadata: std::collections::BTreeMap::from([
                    ("openai.id".into(), serde_json::json!("item_1")),
                    ("openai.status".into(), serde_json::json!("completed")),
                ]),
            })]
        );
    }

    #[test]
    fn function_arguments_done_is_authoritative_without_deltas() {
        let response = decode_response([
            serde_json::json!({
                "type": "response.created",
                "response": {"id": "resp_done_only", "model": "test-model"}
            }),
            serde_json::json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "item_done_only",
                    "call_id": "call_done_only",
                    "name": "lookup",
                    "arguments": "",
                    "status": "in_progress"
                }
            }),
            serde_json::json!({
                "type": "response.function_call_arguments.done",
                "item_id": "item_done_only",
                "output_index": 0,
                "arguments": "{\"value\":9}"
            }),
            serde_json::json!({
                "type": "response.completed",
                "response": {"status": "completed", "usage": {}}
            }),
        ]);

        let ContentPart::ToolCall(call) = &response.content[0] else {
            panic!("fixture must produce a tool call");
        };
        assert_eq!(call.arguments, serde_json::json!({"value": 9}));
        assert_eq!(call.raw_arguments.as_deref(), Some("{\"value\":9}"));
        assert_eq!(call.metadata["openai.status"], "completed");
    }

    #[test]
    fn output_item_done_can_complete_a_tool_call_without_arguments_done() {
        let response = decode_response([
            serde_json::json!({
                "type": "response.created",
                "response": {"id": "resp_item_done", "model": "test-model"}
            }),
            serde_json::json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "item_done",
                    "call_id": "call_done",
                    "name": "lookup",
                    "arguments": "",
                    "status": "in_progress"
                }
            }),
            serde_json::json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "item_done",
                    "call_id": "call_done",
                    "name": "lookup",
                    "arguments": "{\"value\":11}",
                    "status": "completed"
                }
            }),
            serde_json::json!({
                "type": "response.completed",
                "response": {"status": "completed", "usage": {}}
            }),
        ]);

        let ContentPart::ToolCall(call) = &response.content[0] else {
            panic!("fixture must produce a tool call");
        };
        assert_eq!(call.arguments, serde_json::json!({"value": 11}));
        assert_eq!(call.metadata["openai.status"], "completed");
    }

    #[test]
    fn complete_function_call_preserves_replay_metadata() {
        let payload = serde_json::json!({
            "id": "resp_complete_tool",
            "model": "doubao",
            "status": "completed",
            "output": [{
                "type": "function_call",
                "id": "item_complete",
                "call_id": "call_complete",
                "name": "lookup",
                "arguments": "{\"value\":7}",
                "caller": {"type":"program","caller_id":"call_program"},
                "status": "completed"
            }],
            "usage": {}
        });
        let events = super::decode_complete_response("ark", &payload, None).unwrap();
        let mut accumulator = ModelStreamAccumulator::new();
        let response = events
            .into_iter()
            .find_map(|event| accumulator.push(event).unwrap())
            .unwrap();

        assert_eq!(
            response.content,
            vec![ContentPart::ToolCall(ToolCall {
                id: "call_complete".into(),
                name: "lookup".into(),
                arguments: serde_json::json!({"value": 7}),
                raw_arguments: Some("{\"value\":7}".into()),
                metadata: std::collections::BTreeMap::from([
                    ("ark.id".into(), serde_json::json!("item_complete")),
                    ("ark.status".into(), serde_json::json!("completed")),
                    (
                        "ark.caller".into(),
                        serde_json::json!({"type":"program","caller_id":"call_program"}),
                    ),
                ]),
            })]
        );
    }

    #[test]
    fn official_responses_dialect_requires_sequence_and_terminal_status() {
        let mut decoder =
            OpenAiEventDecoder::new().with_dialect(super::OpenAiResponsesDialect::OpenAi);
        let error = decoder
            .decode(serde_json::json!({
                "type":"response.created",
                "response":{"id":"resp","model":"model"}
            }))
            .unwrap_err();
        assert!(error.message.contains("sequence_number"));

        let payload = serde_json::json!({
            "id":"resp",
            "model":"model",
            "output":[],
            "usage":{}
        });
        let error = super::decode_complete_response_for(
            "openai",
            &payload,
            None,
            super::OpenAiResponsesDialect::OpenAi,
        )
        .unwrap_err();
        assert!(error.message.contains("status"));
    }

    #[test]
    fn compatible_responses_dialect_allows_omitted_sequence_and_status() {
        let mut decoder = OpenAiEventDecoder::for_provider("gateway");
        decoder
            .decode(serde_json::json!({
                "type":"response.created",
                "response":{"id":"resp","model":"model"}
            }))
            .unwrap();
        decoder
            .decode(serde_json::json!({
                "type":"response.completed",
                "response":{"usage":{}}
            }))
            .unwrap();
        decoder.finish().unwrap();
    }

    #[test]
    fn complete_native_output_item_is_retained_for_manual_context() {
        let payload = serde_json::json!({
            "id": "resp_native",
            "model": "doubao",
            "status": "completed",
            "output": [{
                "type": "web_search_call",
                "id": "ws_1",
                "status": "completed",
                "action": {"type": "search", "query": "Runifold"}
            }],
            "usage": {}
        });
        let events = super::decode_complete_response("ark", &payload, None).unwrap();
        let mut accumulator = ModelStreamAccumulator::new();
        let response = events
            .into_iter()
            .find_map(|event| accumulator.push(event).unwrap())
            .unwrap();

        let ContentPart::ProviderOpaque(item) = &response.content[0] else {
            panic!("native output item must remain replayable");
        };
        assert_eq!(item.provider, "ark");
        assert_eq!(item.kind, "input_item");
        assert_eq!(item.value["id"], "ws_1");
        assert_eq!(item.value["status"], "completed");
    }

    #[test]
    fn complete_non_terminal_response_is_rejected() {
        for status in ["queued", "in_progress", "cancelled"] {
            let payload = serde_json::json!({
                "id": "resp_non_terminal",
                "model": "doubao",
                "status": status,
                "output": []
            });

            let error = super::decode_complete_response("ark", &payload, None).unwrap_err();

            assert_eq!(error.kind, ModelErrorKind::Protocol);
        }
    }

    #[test]
    fn complete_incomplete_function_call_is_not_executable() {
        let payload = serde_json::json!({
            "id": "resp_partial_tool",
            "model": "doubao",
            "status": "incomplete",
            "incomplete_details": {"reason": "max_output_tokens"},
            "output": [{
                "type": "function_call",
                "id": "item_partial",
                "call_id": "call_partial",
                "name": "dangerous_write",
                "arguments": "{\"partial\":",
                "status": "incomplete"
            }]
        });

        let error = super::decode_complete_response("ark", &payload, None).unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::Protocol);
    }

    #[test]
    fn incomplete_response_cannot_expose_even_a_completed_function_call() {
        let error = super::decode_complete_response(
            "ark",
            &serde_json::json!({
                "id": "resp_incomplete",
                "model": "doubao",
                "status": "incomplete",
                "output": [{
                    "type": "function_call",
                    "id": "fc_1",
                    "call_id": "call_1",
                    "name": "lookup",
                    "arguments": "{}",
                    "status": "completed"
                }],
                "incomplete_details": {"reason": "max_output_tokens"}
            }),
            None,
        )
        .unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::Protocol);
        assert!(error.message.contains("not safe to execute"));
    }

    #[test]
    fn responses_stream_rejects_eof_without_terminal_event() {
        let mut decoder = OpenAiEventDecoder::new();
        decoder
            .decode(serde_json::json!({
                "type": "response.created",
                "response": {"id": "resp_1", "model": "gpt-test"}
            }))
            .unwrap();

        let error = decoder.finish().unwrap_err();
        assert_eq!(error.kind, ModelErrorKind::Protocol);
        assert!(error.message.contains("terminal response event"));
    }

    #[test]
    fn responses_stream_rejects_events_after_completion() {
        let mut decoder = OpenAiEventDecoder::new();
        decoder
            .decode(serde_json::json!({
                "type": "response.created",
                "response": {"id": "resp_1", "model": "gpt-test"}
            }))
            .unwrap();
        decoder
            .decode(serde_json::json!({
                "type": "response.completed",
                "response": {"status": "completed", "usage": {}}
            }))
            .unwrap();

        let error = decoder
            .decode(serde_json::json!({"type": "response.in_progress"}))
            .unwrap_err();
        assert_eq!(error.kind, ModelErrorKind::Protocol);
        assert!(error.message.contains("after response completion"));
    }

    #[test]
    fn responses_stream_rejects_unclosed_message_content() {
        let mut decoder = OpenAiEventDecoder::new();
        decoder
            .decode(serde_json::json!({
                "type":"response.created","sequence_number":0,
                "response":{"id":"resp","model":"model"}
            }))
            .unwrap();
        decoder
            .decode(serde_json::json!({
                "type":"response.content_part.added","sequence_number":1,
                "output_index":0,"content_index":0,
                "part":{"type":"output_text","text":""}
            }))
            .unwrap();

        let error = decoder
            .decode(serde_json::json!({
                "type":"response.completed","sequence_number":2,
                "response":{"status":"completed","usage":{}}
            }))
            .unwrap_err();
        assert!(error.message.contains("unfinished message content"));
    }

    #[test]
    fn responses_stream_rejects_duplicate_or_reordered_sequence_numbers() {
        let mut decoder = OpenAiEventDecoder::new();
        decoder
            .decode(serde_json::json!({
                "type":"response.created","sequence_number":4,
                "response":{"id":"resp","model":"model"}
            }))
            .unwrap();

        let error = decoder
            .decode(serde_json::json!({
                "type":"response.in_progress","sequence_number":4
            }))
            .unwrap_err();
        assert!(error.message.contains("out of order"));
    }

    #[test]
    fn responses_stream_rejects_terminal_identity_drift() {
        let mut decoder = OpenAiEventDecoder::new();
        decoder
            .decode(serde_json::json!({
                "type":"response.created",
                "response":{"id":"resp-one","model":"model-one"}
            }))
            .unwrap();

        let error = decoder
            .decode(serde_json::json!({
                "type":"response.completed",
                "response":{
                    "id":"resp-two","model":"model-one",
                    "status":"completed","usage":{}
                }
            }))
            .unwrap_err();
        assert!(error.message.contains("response ID"));
    }

    #[test]
    fn complete_response_rejects_incomplete_message_items() {
        let error = super::decode_complete_response(
            "openai",
            &serde_json::json!({
                "id":"resp","model":"model","status":"completed",
                "output":[{
                    "type":"message","status":"incomplete",
                    "content":[{"type":"output_text","text":"partial"}]
                }],
                "usage":{}
            }),
            None,
        )
        .unwrap_err();
        assert!(error.message.contains("non-terminal status"));
    }

    #[test]
    fn incomplete_response_decodes_incomplete_message_for_terminal_repair() {
        let events = super::decode_complete_response(
            "openai",
            &serde_json::json!({
                "id":"resp","model":"model","status":"incomplete",
                "output":[{
                    "type":"message","status":"incomplete",
                    "content":[{"type":"output_text","text":"{\"partial\":"}]
                }],
                "incomplete_details":{"reason":"max_output_tokens"},
                "usage":{}
            }),
            None,
        )
        .unwrap();
        let mut accumulator = ModelStreamAccumulator::new();
        let response = events
            .into_iter()
            .find_map(|event| accumulator.push(event).unwrap())
            .unwrap();

        assert_eq!(response.text(), "{\"partial\":");
        assert_eq!(response.finish_reason, FinishReason::Length);
    }

    #[test]
    fn streamed_incomplete_function_call_item_is_not_executable() {
        let mut decoder = OpenAiEventDecoder::new();
        decoder
            .decode(serde_json::json!({
                "type": "response.created",
                "response": {"id": "resp_partial", "model": "test-model"}
            }))
            .unwrap();
        decoder
            .decode(serde_json::json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "item_partial",
                    "call_id": "call_partial",
                    "name": "dangerous_write",
                    "arguments": "",
                    "status": "in_progress"
                }
            }))
            .unwrap();

        let error = decoder
            .decode(serde_json::json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "item_partial",
                    "call_id": "call_partial",
                    "name": "dangerous_write",
                    "arguments": "{\"partial\":",
                    "status": "incomplete"
                }
            }))
            .unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::Protocol);
    }

    #[test]
    fn streamed_native_output_item_is_retained_on_item_done() {
        let response = decode_response([
            serde_json::json!({
                "type": "response.created",
                "response": {"id": "resp_native", "model": "test-model"}
            }),
            serde_json::json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {
                    "type": "web_search_call",
                    "id": "ws_stream",
                    "status": "in_progress"
                }
            }),
            serde_json::json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "type": "web_search_call",
                    "id": "ws_stream",
                    "status": "completed",
                    "action": {"type": "search", "query": "Runifold"}
                }
            }),
            serde_json::json!({
                "type": "response.completed",
                "response": {"status": "completed", "usage": {}}
            }),
        ]);

        let ContentPart::ProviderOpaque(item) = &response.content[0] else {
            panic!("native output item must remain replayable");
        };
        assert_eq!(item.value["id"], "ws_stream");
        assert_eq!(item.value["status"], "completed");
    }

    #[test]
    fn unknown_events_are_preserved_losslessly() {
        let payload = serde_json::json!({
            "type": "response.future_event",
            "new_field": {"value": 7}
        });
        let mut decoder = OpenAiEventDecoder::new();

        let events = decoder.decode(payload.clone()).unwrap();

        let ModelStreamEvent::Provider { event } = &events[0] else {
            panic!("unknown event should be preserved");
        };
        assert_eq!(event.payload, payload);
    }

    #[test]
    fn complete_response_uses_the_same_canonical_accumulator() {
        let payload = serde_json::json!({
            "id": "resp_complete",
            "model": "doubao",
            "status": "completed",
            "output": [{
                "type": "message",
                "status": "completed",
                "content": [{"type": "output_text", "text": "{\"ok\":true}"}]
            }],
            "usage": {"input_tokens": 4, "output_tokens": 3}
        });
        let events =
            super::decode_complete_response("ark", &payload, Some("req_1".into())).unwrap();
        let mut accumulator = ModelStreamAccumulator::new();
        let response = events
            .into_iter()
            .find_map(|event| accumulator.push(event).unwrap())
            .unwrap();

        assert_eq!(response.model.provider, "ark");
        assert_eq!(response.content, vec![ContentPart::text("{\"ok\":true}")]);
        assert_eq!(response.usage.input_tokens, 4);
        assert_eq!(response.usage.output_tokens, 3);
    }

    #[test]
    fn image_generation_completion_becomes_canonical_media_without_raw_duplication() {
        let response = decode_response([
            serde_json::json!({
                "type":"response.created",
                "response":{"id":"resp_image","model":"image-model"}
            }),
            serde_json::json!({
                "type":"response.image_generation_call.completed",
                "output_index":0,
                "item_id":"image_1"
            }),
            serde_json::json!({
                "type":"response.output_item.done",
                "output_index":0,
                "item":{
                    "id":"image_1",
                    "type":"image_generation_call",
                    "status":"completed",
                    "result":"iVBORw0KGgoAAA=="
                }
            }),
            serde_json::json!({
                "type":"response.completed",
                "response":{"status":"completed","usage":{}}
            }),
        ]);

        assert!(matches!(
            &response.content[0],
            ContentPart::Image {
                source: runifold_model::MediaSource::Base64 { media_type, data }
            } if media_type == "image/png" && data == "iVBORw0KGgoAAA=="
        ));
        assert!(
            response
                .provider_events
                .iter()
                .all(|event| !event.value.to_string().contains("iVBORw0KGgoAAA=="))
        );
    }

    #[test]
    fn partial_and_complete_provider_events_redact_generated_media() {
        let mut decoder = OpenAiEventDecoder::new();
        let partial = decoder
            .decode(serde_json::json!({
                "type":"response.image_generation_call.partial_image",
                "output_index":0,
                "partial_image_index":1,
                "partial_image_b64":"cHJldmlldw=="
            }))
            .unwrap();
        assert!(partial.iter().all(|event| match event {
            ModelStreamEvent::Provider { event } => {
                !event.payload.to_string().contains("cHJldmlldw==")
            }
            _ => true,
        }));

        let payload = serde_json::json!({
            "id":"resp_complete_image",
            "model":"image-model",
            "status":"completed",
            "output":[{
                "type":"image_generation_call",
                "status":"completed",
                "result":"UklGRiQAAABXRUJQ"
            }],
            "usage":{}
        });
        let events = super::decode_complete_response("openai", &payload, None).unwrap();
        assert!(events.iter().all(|event| match event {
            ModelStreamEvent::Provider { event } => {
                !event.payload.to_string().contains("UklGRiQAAABXRUJQ")
            }
            _ => true,
        }));
        assert!(events.iter().any(|event| matches!(
            event,
            ModelStreamEvent::ContentPartCompleted {
                part: ContentPart::Image {
                    source: runifold_model::MediaSource::Base64 { media_type, .. }
                },
                ..
            } if media_type == "image/webp"
        )));
    }

    #[test]
    fn failed_response_extracts_nested_provider_error() {
        let mut decoder = OpenAiEventDecoder::for_provider("custom-openai");
        let error = decoder
            .decode(serde_json::json!({
                "type":"response.failed",
                "response":{
                    "status":"failed",
                    "error":{
                        "type":"provider_error",
                        "code":"server_error",
                        "message":"generation failed",
                        "param":"tools[0]"
                    }
                }
            }))
            .unwrap_err();

        assert_eq!(error.provider.as_deref(), Some("custom-openai"));
        assert_eq!(error.message, "generation failed");
        assert_eq!(error.metadata["custom-openai.error.type"], "provider_error");
        assert_eq!(error.metadata["custom-openai.error.code"], "server_error");
        assert_eq!(error.metadata["custom-openai.error.param"], "tools[0]");
    }

    fn decode_response(
        payloads: impl IntoIterator<Item = serde_json::Value>,
    ) -> runifold_model::ModelResponse {
        let mut decoder = OpenAiEventDecoder::new();
        let mut accumulator = ModelStreamAccumulator::new();
        for payload in payloads {
            for event in decoder.decode(payload).unwrap() {
                if let Some(response) = accumulator.push(event).unwrap() {
                    return response;
                }
            }
        }
        panic!("fixture did not complete")
    }
}
