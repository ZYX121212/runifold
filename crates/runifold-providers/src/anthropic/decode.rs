//! Anthropic Messages streaming decoder.

use std::collections::{BTreeMap, BTreeSet};

use runifold_model::{
    ContentBlockKind, ContentPart, FinishReason, ModelError, ModelErrorKind, ModelRef,
    ModelStreamEvent, ModelUsage, ProviderData, ProviderEvent,
};
use serde_json::Value;

/// Stateful translator from Anthropic Messages SSE payloads to canonical events.
#[derive(Debug, Default)]
pub struct AnthropicEventDecoder {
    started: bool,
    completed: bool,
    open_blocks: BTreeSet<u32>,
    opaque_blocks: BTreeMap<u32, OpaqueBlock>,
    saw_terminal_delta: bool,
    usage: ModelUsage,
    finish_reason: FinishReason,
    request_id: Option<String>,
}

#[derive(Debug)]
struct OpaqueBlock {
    value: Value,
    input_json: String,
}

impl AnthropicEventDecoder {
    /// Creates an empty decoder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attaches the HTTP request ID to the canonical provider-event stream.
    #[must_use]
    pub fn with_request_id(mut self, request_id: Option<String>) -> Self {
        self.request_id = request_id;
        self
    }

    /// Decodes one Anthropic SSE JSON payload.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError`] for malformed known events, provider error
    /// events, duplicate blocks, or deltas targeting unknown blocks.
    pub fn decode(&mut self, payload: Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        if self.completed {
            return Err(with_provider(protocol(
                "Anthropic event arrived after response completion",
            )));
        }
        let event_type = string(&payload, "type")?.to_owned();
        let known = matches!(
            event_type.as_str(),
            "message_start"
                | "content_block_start"
                | "content_block_delta"
                | "content_block_stop"
                | "message_delta"
                | "message_stop"
                | "ping"
        );
        let mut events = match event_type.as_str() {
            "message_start" => self.message_start(&payload),
            "content_block_start" => self.content_block_start(&payload),
            "content_block_delta" => self.content_block_delta(&payload),
            "content_block_stop" => self.content_block_stop(&payload),
            "message_delta" => self.message_delta(&payload),
            "message_stop" => self.message_stop(),
            "ping" => Ok(vec![ModelStreamEvent::Heartbeat]),
            "error" => Err(provider_error(&payload)),
            _ => Ok(vec![provider_event(&event_type, payload.clone())]),
        }
        .map_err(with_provider)?;
        if known {
            let position = events
                .iter()
                .position(|event| matches!(event, ModelStreamEvent::ResponseCompleted { .. }))
                .unwrap_or(events.len());
            events.insert(position, provider_event(&event_type, payload));
        }
        Ok(events)
    }

    /// Validates that the stream ended at an Anthropic `message_stop`.
    ///
    /// # Errors
    ///
    /// Returns a protocol error for a truncated or incomplete stream.
    pub fn finish(&self) -> Result<(), ModelError> {
        if !self.started {
            return Err(protocol("Anthropic stream ended before message_start"));
        }
        if !self.open_blocks.is_empty() {
            return Err(protocol(
                "Anthropic stream ended with unfinished content blocks",
            ));
        }
        if !self.opaque_blocks.is_empty() {
            return Err(protocol(
                "Anthropic stream ended with unfinished unknown content blocks",
            ));
        }
        if !self.completed {
            return Err(protocol("Anthropic stream ended before message_stop"));
        }
        Ok(())
    }

    fn message_start(&mut self, payload: &Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        if self.started {
            return Err(protocol("received message_start more than once"));
        }
        let message = object(payload, "message")?;
        self.started = true;
        self.usage = decode_usage(message.get("usage"), self.usage);
        let mut events = vec![
            ModelStreamEvent::ResponseStarted {
                id: optional_string(message, "id"),
                model: ModelRef::new("anthropic", string(message, "model")?),
            },
            ModelStreamEvent::UsageUpdated { usage: self.usage },
        ];
        if let Some(request_id) = self.request_id.take() {
            events.push(provider_event(
                "http.request_id",
                serde_json::json!({"request_id": request_id}),
            ));
        }
        Ok(events)
    }

    fn content_block_start(
        &mut self,
        payload: &Value,
    ) -> Result<Vec<ModelStreamEvent>, ModelError> {
        self.require_started()?;
        let index = index(payload)?;
        if self.open_blocks.contains(&index) || self.opaque_blocks.contains_key(&index) {
            return Err(protocol(format!(
                "Anthropic content block {index} started more than once"
            )));
        }
        let block = object(payload, "content_block")?;
        let block_type = string(block, "type")?;
        let kind = match block_type {
            "text" => ContentBlockKind::Text,
            "tool_use" => ContentBlockKind::ToolCall {
                id: string(block, "id")?.into(),
                name: string(block, "name")?.into(),
            },
            "thinking" => ContentBlockKind::Reasoning {
                signature: optional_string(block, "signature"),
                redacted: false,
            },
            "redacted_thinking" => ContentBlockKind::Reasoning {
                signature: None,
                redacted: true,
            },
            _ => {
                self.opaque_blocks.insert(
                    index,
                    OpaqueBlock {
                        value: block.clone(),
                        input_json: String::new(),
                    },
                );
                return Ok(vec![provider_event("content_block_start", payload.clone())]);
            }
        };
        self.open_blocks.insert(index);
        let mut events = vec![ModelStreamEvent::ContentBlockStarted { index, kind }];
        match block_type {
            "text" => {
                if let Some(text) = block.get("text").and_then(Value::as_str)
                    && !text.is_empty()
                {
                    events.push(ModelStreamEvent::TextDelta {
                        index,
                        text: text.into(),
                    });
                }
            }
            "tool_use" => {
                if let Some(input) = block.get("input")
                    && !input.as_object().is_some_and(serde_json::Map::is_empty)
                    && !input.is_null()
                {
                    events.push(ModelStreamEvent::ToolArgumentsDelta {
                        index,
                        json: input.to_string(),
                    });
                }
            }
            "thinking" => {
                if let Some(thinking) = block.get("thinking").and_then(Value::as_str)
                    && !thinking.is_empty()
                {
                    events.push(ModelStreamEvent::ReasoningDelta {
                        index,
                        text: thinking.into(),
                    });
                }
            }
            "redacted_thinking" => {
                events.push(provider_event("redacted_thinking", block.clone()));
            }
            _ => {}
        }
        Ok(events)
    }

    fn content_block_delta(
        &mut self,
        payload: &Value,
    ) -> Result<Vec<ModelStreamEvent>, ModelError> {
        self.require_started()?;
        let index = index(payload)?;
        if let Some(block) = self.opaque_blocks.get_mut(&index) {
            if let Some(fragment) = payload
                .get("delta")
                .filter(|delta| {
                    delta.get("type").and_then(Value::as_str) == Some("input_json_delta")
                })
                .and_then(|delta| delta.get("partial_json"))
                .and_then(Value::as_str)
            {
                block.input_json.push_str(fragment);
            }
            return Ok(vec![provider_event("content_block_delta", payload.clone())]);
        }
        if !self.open_blocks.contains(&index) {
            return Err(protocol(format!(
                "Anthropic delta targeted unknown content block {index}"
            )));
        }
        let delta = object(payload, "delta")?;
        let event = match string(delta, "type")? {
            "text_delta" => ModelStreamEvent::TextDelta {
                index,
                text: string(delta, "text")?.into(),
            },
            "input_json_delta" => ModelStreamEvent::ToolArgumentsDelta {
                index,
                json: string(delta, "partial_json")?.into(),
            },
            "thinking_delta" => ModelStreamEvent::ReasoningDelta {
                index,
                text: string(delta, "thinking")?.into(),
            },
            "signature_delta" => ModelStreamEvent::ReasoningSignatureDelta {
                index,
                signature: string(delta, "signature")?.into(),
            },
            other => return Ok(vec![provider_event(other, payload.clone())]),
        };
        Ok(vec![event])
    }

    fn content_block_stop(&mut self, payload: &Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        self.require_started()?;
        let index = index(payload)?;
        if let Some(mut block) = self.opaque_blocks.remove(&index) {
            if !block.input_json.trim().is_empty() {
                let input = serde_json::from_str(&block.input_json).map_err(|error| {
                    protocol(format!(
                        "Anthropic opaque tool block {index} returned invalid JSON input: {error}"
                    ))
                })?;
                block.value["input"] = input;
            }
            return Ok(vec![ModelStreamEvent::ContentPartCompleted {
                index,
                part: ContentPart::ProviderOpaque(ProviderData {
                    provider: "anthropic".into(),
                    kind: "content_block".into(),
                    value: block.value,
                }),
            }]);
        }
        if !self.open_blocks.remove(&index) {
            return Err(protocol(format!(
                "Anthropic stopped unknown content block {index}"
            )));
        }
        Ok(vec![ModelStreamEvent::ContentBlockCompleted { index }])
    }

    fn message_delta(&mut self, payload: &Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        self.require_started()?;
        self.usage = decode_usage(payload.get("usage"), self.usage);
        let delta = object(payload, "delta")?;
        if let Some(reason) = delta.get("stop_reason").and_then(Value::as_str) {
            if self.saw_terminal_delta {
                return Err(protocol(
                    "Anthropic stream emitted more than one terminal stop reason",
                ));
            }
            self.finish_reason = finish_reason(reason);
            self.saw_terminal_delta = true;
        }
        Ok(vec![ModelStreamEvent::UsageUpdated { usage: self.usage }])
    }

    fn message_stop(&mut self) -> Result<Vec<ModelStreamEvent>, ModelError> {
        self.require_started()?;
        if !self.open_blocks.is_empty() || !self.opaque_blocks.is_empty() {
            return Err(protocol(
                "message_stop arrived with unfinished content blocks",
            ));
        }
        if !self.saw_terminal_delta {
            return Err(protocol(
                "message_stop arrived before a terminal message_delta",
            ));
        }
        if self.completed {
            return Err(protocol("received message_stop more than once"));
        }
        self.completed = true;
        Ok(vec![ModelStreamEvent::ResponseCompleted {
            finish_reason: self.finish_reason.clone(),
            provider_metadata: BTreeMap::new(),
        }])
    }

    fn require_started(&self) -> Result<(), ModelError> {
        if self.started {
            Ok(())
        } else {
            Err(protocol("Anthropic content arrived before message_start"))
        }
    }
}

fn decode_usage(value: Option<&Value>, previous: ModelUsage) -> ModelUsage {
    let value = value.unwrap_or(&Value::Null);
    ModelUsage {
        input_tokens: optional_unsigned(value, "input_tokens").unwrap_or(previous.input_tokens),
        output_tokens: optional_unsigned(value, "output_tokens").unwrap_or(previous.output_tokens),
        cached_input_tokens: optional_unsigned(value, "cache_read_input_tokens")
            .unwrap_or(previous.cached_input_tokens),
        cache_write_tokens: optional_unsigned(value, "cache_creation_input_tokens")
            .unwrap_or(previous.cache_write_tokens),
        ..previous
    }
}

fn finish_reason(reason: &str) -> FinishReason {
    match reason {
        "end_turn" | "stop_sequence" => FinishReason::Stop,
        "max_tokens" | "model_context_window_exceeded" => FinishReason::Length,
        "tool_use" => FinishReason::ToolCalls,
        "refusal" => FinishReason::ContentFilter,
        "pause_turn" => FinishReason::Other("pause_turn".into()),
        other => FinishReason::Other(other.into()),
    }
}

fn provider_error(payload: &Value) -> ModelError {
    let error = payload.get("error").unwrap_or(&Value::Null);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Anthropic returned a stream error");
    let mut model_error = ModelError::local(ModelErrorKind::Provider, message);
    if let Some(error_type) = error.get("type") {
        model_error
            .metadata
            .insert("anthropic.error.type".into(), error_type.clone());
    }
    model_error
}

fn provider_event(name: &str, payload: Value) -> ModelStreamEvent {
    ModelStreamEvent::Provider {
        event: ProviderEvent {
            provider: "anthropic".into(),
            name: name.into(),
            payload,
        },
    }
}

fn with_provider(mut error: ModelError) -> ModelError {
    error.provider = Some("anthropic".into());
    error
}

fn protocol(message: impl Into<String>) -> ModelError {
    ModelError::local(ModelErrorKind::Protocol, message)
}

fn object<'a>(value: &'a Value, key: &str) -> Result<&'a Value, ModelError> {
    value
        .get(key)
        .filter(|value| value.is_object())
        .ok_or_else(|| protocol(format!("Anthropic event is missing object `{key}`")))
}

fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str, ModelError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| protocol(format!("Anthropic event is missing string `{key}`")))
}

fn optional_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(String::from)
}

fn optional_unsigned(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

fn index(value: &Value) -> Result<u32, ModelError> {
    let raw = value
        .get("index")
        .and_then(Value::as_u64)
        .ok_or_else(|| protocol("Anthropic event is missing unsigned `index`"))?;
    u32::try_from(raw).map_err(|_| protocol("Anthropic content index exceeds u32"))
}

#[cfg(test)]
mod tests {
    use runifold_model::{
        ContentBlockKind, ContentPart, FinishReason, ModelStreamAccumulator, ModelStreamEvent,
    };
    use serde_json::json;

    use super::AnthropicEventDecoder;

    #[test]
    fn decodes_fragmented_tool_input_and_usage() {
        let mut decoder = AnthropicEventDecoder::new();
        let started = decoder
            .decode(json!({
                "type": "message_start",
                "message": {
                    "id": "msg_1",
                    "model": "claude-test",
                    "usage": {"input_tokens": 9, "output_tokens": 1}
                }
            }))
            .unwrap();
        let block = decoder
            .decode(json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type":"tool_use","id":"tool_1","name":"weather","input":{}}
            }))
            .unwrap();
        let delta = decoder
            .decode(json!({
                "type":"content_block_delta",
                "index":0,
                "delta":{"type":"input_json_delta","partial_json":"{\"city\":\""}
            }))
            .unwrap();

        assert!(matches!(
            started[0],
            ModelStreamEvent::ResponseStarted { .. }
        ));
        assert!(matches!(
            block[0],
            ModelStreamEvent::ContentBlockStarted {
                kind: ContentBlockKind::ToolCall { .. },
                ..
            }
        ));
        assert!(matches!(
            &delta[0],
            ModelStreamEvent::ToolArgumentsDelta { json, .. } if json == "{\"city\":\""
        ));
    }

    #[test]
    fn retains_non_empty_content_block_start_payloads() {
        let mut decoder = AnthropicEventDecoder::new();
        let mut accumulator = ModelStreamAccumulator::new();
        let payloads = [
            json!({
                "type":"message_start",
                "message":{"id":"msg","model":"claude","usage":{}}
            }),
            json!({
                "type":"content_block_start","index":0,
                "content_block":{
                    "type":"tool_use","id":"call","name":"lookup","input":{"city":"Paris"}
                }
            }),
            json!({"type":"content_block_stop","index":0}),
            json!({
                "type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{}
            }),
            json!({"type":"message_stop"}),
        ];
        let mut response = None;
        for payload in payloads {
            for event in decoder.decode(payload).unwrap() {
                response = accumulator.push(event).unwrap().or(response);
            }
        }
        let ContentPart::ToolCall(call) = &response.unwrap().content[0] else {
            panic!("fixture must decode a tool call");
        };
        assert_eq!(call.arguments, json!({"city":"Paris"}));
    }

    #[test]
    fn rejects_duplicate_terminal_stop_reasons() {
        let mut decoder = AnthropicEventDecoder::new();
        decoder
            .decode(json!({
                "type":"message_start",
                "message":{"id":"msg","model":"claude","usage":{}}
            }))
            .unwrap();
        decoder
            .decode(json!({
                "type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{}
            }))
            .unwrap();

        assert!(
            decoder
                .decode(json!({
                    "type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{}
                }))
                .is_err()
        );
    }

    #[test]
    fn message_stop_requires_closed_blocks() {
        let mut decoder = AnthropicEventDecoder::new();
        decoder
            .decode(json!({
                "type":"message_start",
                "message":{"id":"msg","model":"claude","usage":{}}
            }))
            .unwrap();
        decoder
            .decode(json!({
                "type":"content_block_start",
                "index":0,
                "content_block":{"type":"text","text":""}
            }))
            .unwrap();

        assert!(decoder.decode(json!({"type":"message_stop"})).is_err());
    }

    #[test]
    fn tool_stop_reason_is_normalized() {
        let mut decoder = AnthropicEventDecoder::new();
        decoder
            .decode(json!({
                "type":"message_start",
                "message":{"id":"msg","model":"claude","usage":{}}
            }))
            .unwrap();
        decoder
            .decode(json!({
                "type":"message_delta",
                "delta":{"stop_reason":"tool_use"},
                "usage":{"output_tokens":8}
            }))
            .unwrap();
        let completed = decoder.decode(json!({"type":"message_stop"})).unwrap();

        assert!(completed.iter().any(|event| matches!(
            event,
            ModelStreamEvent::ResponseCompleted {
                finish_reason: FinishReason::ToolCalls,
                ..
            }
        )));
        decoder.finish().unwrap();
    }

    #[test]
    fn rejects_events_after_message_stop() {
        let mut decoder = AnthropicEventDecoder::new();
        decoder
            .decode(json!({
                "type":"message_start",
                "message":{"id":"msg","model":"claude","usage":{}}
            }))
            .unwrap();
        decoder
            .decode(json!({
                "type":"message_delta",
                "delta":{"stop_reason":"end_turn"},
                "usage":{"output_tokens":1}
            }))
            .unwrap();
        decoder.decode(json!({"type":"message_stop"})).unwrap();

        assert!(decoder.decode(json!({"type":"ping"})).is_err());
    }

    #[test]
    fn preserves_server_tool_blocks_for_exact_history_replay() {
        let mut decoder = AnthropicEventDecoder::new();
        decoder
            .decode(json!({
                "type":"message_start",
                "message":{"id":"msg","model":"claude","usage":{}}
            }))
            .unwrap();
        decoder
            .decode(json!({
                "type":"content_block_start",
                "index":0,
                "content_block":{
                    "type":"server_tool_use","id":"srvtoolu_1","name":"web_search","input":{}
                }
            }))
            .unwrap();
        decoder
            .decode(json!({
                "type":"content_block_delta","index":0,
                "delta":{"type":"input_json_delta","partial_json":"{\"query\":\"Rust\"}"}
            }))
            .unwrap();
        let events = decoder
            .decode(json!({"type":"content_block_stop","index":0}))
            .unwrap();

        let block = events.iter().find_map(|event| match event {
            ModelStreamEvent::ContentPartCompleted {
                part: runifold_model::ContentPart::ProviderOpaque(block),
                ..
            } => Some(block),
            _ => None,
        });
        let block = block.expect("server tool must become replayable opaque content");
        assert_eq!(block.value["input"], json!({"query":"Rust"}));
    }
}
