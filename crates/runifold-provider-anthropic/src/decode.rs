use std::collections::{BTreeMap, BTreeSet};

use runifold_model::{
    ContentBlockKind, FinishReason, ModelError, ModelErrorKind, ModelRef, ModelStreamEvent,
    ModelUsage, ProviderEvent,
};
use serde_json::Value;

/// Stateful translator from Anthropic Messages SSE payloads to canonical events.
#[derive(Debug, Default)]
pub struct AnthropicEventDecoder {
    started: bool,
    completed: bool,
    open_blocks: BTreeSet<u32>,
    usage: ModelUsage,
    finish_reason: FinishReason,
    request_id: Option<String>,
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
        let event_type = string(&payload, "type")?.to_owned();
        match event_type.as_str() {
            "message_start" => self.message_start(&payload),
            "content_block_start" => self.content_block_start(&payload),
            "content_block_delta" => self.content_block_delta(&payload),
            "content_block_stop" => self.content_block_stop(&payload),
            "message_delta" => self.message_delta(&payload),
            "message_stop" => self.message_stop(),
            "ping" => Ok(vec![ModelStreamEvent::Heartbeat]),
            "error" => Err(provider_error(&payload)),
            _ => Ok(vec![provider_event(&event_type, payload)]),
        }
        .map_err(with_provider)
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
        if !self.open_blocks.insert(index) {
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
                self.open_blocks.remove(&index);
                return Ok(vec![provider_event("content_block_start", payload.clone())]);
            }
        };
        let mut events = vec![ModelStreamEvent::ContentBlockStarted { index, kind }];
        if block_type == "redacted_thinking" {
            events.push(provider_event("redacted_thinking", block.clone()));
        }
        Ok(events)
    }

    fn content_block_delta(&self, payload: &Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        self.require_started()?;
        let index = index(payload)?;
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
        if !self.open_blocks.remove(&index) {
            return Ok(vec![provider_event("content_block_stop", payload.clone())]);
        }
        Ok(vec![ModelStreamEvent::ContentBlockCompleted { index }])
    }

    fn message_delta(&mut self, payload: &Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        self.require_started()?;
        self.usage = decode_usage(payload.get("usage"), self.usage);
        let delta = object(payload, "delta")?;
        self.finish_reason = finish_reason(
            delta
                .get("stop_reason")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
        );
        Ok(vec![ModelStreamEvent::UsageUpdated { usage: self.usage }])
    }

    fn message_stop(&mut self) -> Result<Vec<ModelStreamEvent>, ModelError> {
        self.require_started()?;
        if !self.open_blocks.is_empty() {
            return Err(protocol(
                "message_stop arrived with unfinished content blocks",
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
    use runifold_model::{ContentBlockKind, FinishReason, ModelStreamEvent};
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

        assert!(matches!(
            completed[0],
            ModelStreamEvent::ResponseCompleted {
                finish_reason: FinishReason::ToolCalls,
                ..
            }
        ));
        decoder.finish().unwrap();
    }
}
