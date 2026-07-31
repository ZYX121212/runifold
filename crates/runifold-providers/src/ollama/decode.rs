//! Ollama NDJSON decoder.

use std::collections::{BTreeMap, BTreeSet};

use runifold_model::{
    ContentBlockKind, ContentPart, FinishReason, ModelError, ModelErrorKind, ModelRef,
    ModelStreamEvent, ModelUsage, ProviderEvent, ToolCall,
};
use serde_json::Value;

/// Stateful decoder for Ollama's native NDJSON chat chunks.
#[derive(Debug, Default)]
pub struct OllamaChunkDecoder {
    started: bool,
    completed: bool,
    open_blocks: BTreeSet<u32>,
    next_part_index: u32,
    model: String,
}

impl OllamaChunkDecoder {
    /// Creates a decoder for the requested model.
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            next_part_index: 2,
            model: model.into(),
            ..Self::default()
        }
    }

    /// Decodes one NDJSON object.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed chunks or provider error objects.
    pub fn decode(&mut self, chunk: &Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        if let Some(message) = chunk.get("error").and_then(Value::as_str) {
            return Err(provider_error(message));
        }
        let mut events = Vec::new();
        if !self.started {
            self.started = true;
            events.push(ModelStreamEvent::ResponseStarted {
                id: None,
                model: ModelRef::new(
                    "ollama",
                    chunk
                        .get("model")
                        .and_then(Value::as_str)
                        .unwrap_or(&self.model),
                ),
            });
        }
        if let Some(message) = chunk.get("message") {
            if let Some(thinking) = message.get("thinking").and_then(Value::as_str)
                && !thinking.is_empty()
            {
                if self.open_blocks.insert(0) {
                    events.push(ModelStreamEvent::ContentBlockStarted {
                        index: 0,
                        kind: ContentBlockKind::Reasoning {
                            signature: None,
                            redacted: false,
                        },
                    });
                }
                events.push(ModelStreamEvent::ReasoningDelta {
                    index: 0,
                    text: thinking.into(),
                });
            }
            if let Some(text) = message.get("content").and_then(Value::as_str)
                && !text.is_empty()
            {
                if self.open_blocks.insert(1) {
                    events.push(ModelStreamEvent::ContentBlockStarted {
                        index: 1,
                        kind: ContentBlockKind::Text,
                    });
                }
                events.push(ModelStreamEvent::TextDelta {
                    index: 1,
                    text: text.into(),
                });
            }
            if let Some(calls) = message.get("tool_calls").and_then(Value::as_array) {
                for call in calls {
                    let index = self.next_part_index;
                    self.next_part_index = self
                        .next_part_index
                        .checked_add(1)
                        .ok_or_else(|| protocol("Ollama content index overflow"))?;
                    events.push(ModelStreamEvent::ContentPartCompleted {
                        index,
                        part: decode_call(call, index)?,
                    });
                }
            }
        }
        events.push(ModelStreamEvent::Provider {
            event: ProviderEvent {
                provider: "ollama".into(),
                name: "chat.chunk".into(),
                payload: chunk.clone(),
            },
        });
        if chunk.get("done").and_then(Value::as_bool).unwrap_or(false) {
            for index in std::mem::take(&mut self.open_blocks) {
                events.push(ModelStreamEvent::ContentBlockCompleted { index });
            }
            events.push(ModelStreamEvent::UsageUpdated {
                usage: ModelUsage {
                    input_tokens: unsigned(chunk, "prompt_eval_count"),
                    output_tokens: unsigned(chunk, "eval_count"),
                    ..ModelUsage::default()
                },
            });
            let reason = chunk
                .get("done_reason")
                .and_then(Value::as_str)
                .unwrap_or("stop");
            events.push(ModelStreamEvent::ResponseCompleted {
                finish_reason: finish_reason(reason),
                provider_metadata: duration_metadata(chunk),
            });
            self.completed = true;
        }
        Ok(events)
    }

    /// Ensures a terminal `done` chunk was observed.
    ///
    /// # Errors
    ///
    /// Returns an error for a truncated NDJSON stream.
    pub fn finish(&self) -> Result<(), ModelError> {
        if self.started && self.completed && self.open_blocks.is_empty() {
            Ok(())
        } else {
            Err(protocol("Ollama stream ended before a terminal done chunk"))
        }
    }
}

fn decode_call(value: &Value, index: u32) -> Result<ContentPart, ModelError> {
    let function = value
        .get("function")
        .ok_or_else(|| protocol("Ollama tool call is missing function"))?;
    let name = function
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| protocol("Ollama tool call is missing function name"))?;
    let arguments = function
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let id = value
        .get("id")
        .and_then(Value::as_str)
        .map_or_else(|| format!("ollama-call-{index}"), String::from);
    Ok(ContentPart::ToolCall(ToolCall {
        id,
        name: name.into(),
        raw_arguments: Some(arguments.to_string()),
        arguments,
        metadata: BTreeMap::new(),
    }))
}

fn finish_reason(reason: &str) -> FinishReason {
    match reason {
        "stop" => FinishReason::Stop,
        "length" => FinishReason::Length,
        "tool_calls" | "tool_use" => FinishReason::ToolCalls,
        other => FinishReason::Other(other.into()),
    }
}

fn duration_metadata(value: &Value) -> BTreeMap<String, Value> {
    ["total_duration", "load_duration", "eval_duration"]
        .into_iter()
        .filter_map(|key| value.get(key).cloned().map(|value| (key.into(), value)))
        .collect()
}

fn unsigned(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}

fn provider_error(message: &str) -> ModelError {
    let mut error = ModelError::local(ModelErrorKind::Provider, message);
    error.provider = Some("ollama".into());
    error
}

fn protocol(message: impl Into<String>) -> ModelError {
    let mut error = ModelError::local(ModelErrorKind::Protocol, message);
    error.provider = Some("ollama".into());
    error
}

#[cfg(test)]
mod tests {
    use runifold_model::{FinishReason, ModelStreamEvent};
    use serde_json::json;

    use super::OllamaChunkDecoder;

    #[test]
    fn decodes_thinking_text_and_usage() {
        let mut decoder = OllamaChunkDecoder::new("qwen3");
        decoder
            .decode(&json!({
                "model":"qwen3",
                "message":{"thinking":"hmm","content":"answer"},
                "done":false
            }))
            .unwrap();
        let terminal = decoder
            .decode(&json!({
                "model":"qwen3",
                "message":{"content":""},
                "done":true,
                "done_reason":"stop",
                "prompt_eval_count":4,
                "eval_count":5
            }))
            .unwrap();

        assert!(terminal.iter().any(|event| matches!(
            event,
            ModelStreamEvent::ResponseCompleted {
                finish_reason: FinishReason::Stop,
                ..
            }
        )));
        decoder.finish().unwrap();
    }
}
