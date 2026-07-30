//! `OpenAI` Responses streaming decoder.

use std::collections::BTreeMap;

use runifold_model::{
    ContentBlockKind, FinishReason, ModelError, ModelErrorKind, ModelRef, ModelStreamEvent,
    ModelUsage, ProviderEvent,
};
use serde_json::Value;

/// Stateful translator from `OpenAI` Responses SSE payloads to canonical events.
#[derive(Debug)]
pub struct OpenAiEventDecoder {
    provider: String,
    next_index: u32,
    block_indices: BTreeMap<String, u32>,
    saw_tool_call: bool,
    request_id: Option<String>,
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
            next_index: 0,
            block_indices: BTreeMap::new(),
            saw_tool_call: false,
            request_id: None,
        }
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
        let raw_payload = payload.clone();
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
                if let Some(event_type) = event_type {
                    if is_known_success_event(&event_type)
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

        match event_type.as_str() {
            "response.created" => self.response_started(&payload),
            "response.content_part.added" => self.content_part_started(&payload),
            "response.output_text.delta" => self.text_delta(&payload),
            "response.refusal.delta" => self.refusal_delta(&payload),
            "response.content_part.done" => self.content_part_completed(&payload),
            "response.output_item.added" => self.output_item_started(&payload),
            "response.function_call_arguments.delta" => self.tool_arguments_delta(&payload),
            "response.function_call_arguments.done" => self.tool_call_completed(&payload),
            "response.completed" => Ok(self.response_completed(&payload, false)),
            "response.incomplete" => Ok(self.response_completed(&payload, true)),
            "response.failed" | "error" => Err(provider_failure(&payload)),
            _ => Ok(vec![provider_event(&event_type, payload)]),
        }
    }

    fn response_started(&mut self, payload: &Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        let response = object(payload, "response")?;
        let model = string(response, "model")?;
        let mut events = vec![ModelStreamEvent::ResponseStarted {
            id: optional_string(response, "id"),
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

    fn content_part_completed(&self, payload: &Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
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
        self.saw_tool_call = true;
        Ok(vec![ModelStreamEvent::ContentBlockStarted {
            index,
            kind: ContentBlockKind::ToolCall {
                id: call_id,
                name: name.into(),
            },
        }])
    }

    fn tool_arguments_delta(&self, payload: &Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        let index = self.tool_index(payload)?;
        Ok(vec![ModelStreamEvent::ToolArgumentsDelta {
            index,
            json: string(payload, "delta")?.into(),
        }])
    }

    fn tool_call_completed(&self, payload: &Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        let index = self.tool_index(payload)?;
        Ok(vec![ModelStreamEvent::ContentBlockCompleted { index }])
    }

    fn response_completed(&self, payload: &Value, incomplete: bool) -> Vec<ModelStreamEvent> {
        let response = payload.get("response").unwrap_or(&Value::Null);
        let usage = decode_usage(response.get("usage"));
        let finish_reason = if incomplete {
            incomplete_reason(response)
        } else if self.saw_tool_call {
            FinishReason::ToolCalls
        } else {
            FinishReason::Stop
        };
        vec![
            ModelStreamEvent::UsageUpdated { usage },
            ModelStreamEvent::ResponseCompleted {
                finish_reason,
                provider_metadata: response_metadata(response),
            },
        ]
    }

    fn content_index(&self, payload: &Value) -> Result<u32, ModelError> {
        let key = content_key(
            integer(payload, "output_index")?,
            integer(payload, "content_index")?,
        );
        self.lookup(&key)
    }

    fn tool_index(&self, payload: &Value) -> Result<u32, ModelError> {
        if let Some(item_id) = payload.get("item_id").and_then(Value::as_str) {
            if let Ok(index) = self.lookup(&tool_key(item_id)) {
                return Ok(index);
            }
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

fn is_known_success_event(event_type: &str) -> bool {
    matches!(
        event_type,
        "response.created"
            | "response.content_part.added"
            | "response.output_text.delta"
            | "response.refusal.delta"
            | "response.content_part.done"
            | "response.output_item.added"
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

fn provider_failure(payload: &Value) -> ModelError {
    let error = payload.get("error").unwrap_or(payload);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("OpenAI response failed");
    let mut model_error = ModelError::local(ModelErrorKind::Provider, message);
    model_error.provider = Some("openai".into());
    if let Some(code) = error.get("code") {
        model_error
            .metadata
            .insert("openai.code".into(), code.clone());
    }
    model_error
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
        ContentPart, FinishReason, ModelStreamAccumulator, ModelStreamEvent, ToolCall,
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
                    "arguments": ""
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
                metadata: std::collections::BTreeMap::new(),
            })]
        );
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
