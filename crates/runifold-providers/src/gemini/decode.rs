//! Gemini streaming decoder.

use std::collections::{BTreeMap, BTreeSet};

use runifold_model::{
    ContentBlockKind, ContentPart, FinishReason, ModelError, ModelErrorKind, ModelRef,
    ModelStreamEvent, ModelUsage, ProviderEvent, ReasoningPart, ToolCall,
};
use serde_json::Value;

/// Stateful Gemini `GenerateContentResponse` stream decoder.
#[derive(Debug, Default)]
pub struct GeminiEventDecoder {
    started: bool,
    completed: bool,
    open_blocks: BTreeSet<u32>,
    next_index: u32,
    text_index: Option<u32>,
    model: String,
    response_id: Option<String>,
    model_version: Option<String>,
}

impl GeminiEventDecoder {
    /// Creates a decoder for the requested model.
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            ..Self::default()
        }
    }

    /// Decodes one SSE response object.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed responses or provider-reported blocks.
    pub fn decode(&mut self, payload: &Value) -> Result<Vec<ModelStreamEvent>, ModelError> {
        if self.completed {
            return Err(protocol("Gemini chunk arrived after response completion"));
        }
        if let Some(error) = payload.get("error") {
            return Err(provider_error(error));
        }
        validate_prompt(payload)?;
        self.validate_identity(payload)?;
        let mut events = Vec::new();
        if !self.started {
            self.started = true;
            events.push(ModelStreamEvent::ResponseStarted {
                id: payload
                    .get("responseId")
                    .and_then(Value::as_str)
                    .map(String::from),
                model: ModelRef::new(
                    "gemini",
                    payload
                        .get("modelVersion")
                        .and_then(Value::as_str)
                        .unwrap_or(&self.model),
                ),
            });
        }
        if let Some(usage) = payload.get("usageMetadata") {
            events.push(ModelStreamEvent::UsageUpdated {
                usage: decode_usage(usage),
            });
        }
        events.push(provider_event(
            "generate_content.chunk",
            redact_inline_media(payload.clone()),
        ));
        let Some(candidates) = payload.get("candidates").and_then(Value::as_array) else {
            return Ok(events);
        };
        for (candidate_position, candidate) in candidates.iter().enumerate() {
            if candidate_position != 0 {
                events.push(provider_event("additional_candidate", candidate.clone()));
                continue;
            }
            self.decode_candidate(candidate, &mut events)?;
        }
        Ok(events)
    }

    fn decode_candidate(
        &mut self,
        candidate: &Value,
        events: &mut Vec<ModelStreamEvent>,
    ) -> Result<(), ModelError> {
        let parts = candidate
            .get("content")
            .and_then(|content| content.get("parts"))
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for part in parts {
            decode_part(self, part, events)?;
        }
        if let Some(reason) = candidate.get("finishReason").and_then(Value::as_str) {
            for index in std::mem::take(&mut self.open_blocks) {
                events.push(ModelStreamEvent::ContentBlockCompleted { index });
            }
            events.push(ModelStreamEvent::ResponseCompleted {
                finish_reason: finish_reason(reason),
                provider_metadata: BTreeMap::new(),
            });
            self.completed = true;
        }
        Ok(())
    }

    fn allocate(&mut self) -> Result<u32, ModelError> {
        let index = self.next_index;
        self.next_index = self
            .next_index
            .checked_add(1)
            .ok_or_else(|| protocol("Gemini content index overflow"))?;
        Ok(index)
    }

    fn validate_identity(&mut self, payload: &Value) -> Result<(), ModelError> {
        validate_stable_string(
            &mut self.response_id,
            payload.get("responseId").and_then(Value::as_str),
            "response ID",
        )?;
        validate_stable_string(
            &mut self.model_version,
            payload.get("modelVersion").and_then(Value::as_str),
            "model version",
        )
    }

    fn text_index(&mut self, events: &mut Vec<ModelStreamEvent>) -> Result<u32, ModelError> {
        if let Some(index) = self.text_index {
            return Ok(index);
        }
        let index = self.allocate()?;
        self.text_index = Some(index);
        self.open_blocks.insert(index);
        events.push(ModelStreamEvent::ContentBlockStarted {
            index,
            kind: ContentBlockKind::Text,
        });
        Ok(index)
    }

    /// Ensures the body ended after a terminal candidate.
    ///
    /// # Errors
    ///
    /// Returns an error for a truncated stream.
    pub fn finish(&self) -> Result<(), ModelError> {
        if self.started && self.completed && self.open_blocks.is_empty() {
            Ok(())
        } else {
            Err(protocol(
                "Gemini stream ended before a terminal finishReason",
            ))
        }
    }
}

fn validate_stable_string(
    state: &mut Option<String>,
    incoming: Option<&str>,
    label: &str,
) -> Result<(), ModelError> {
    let Some(incoming) = incoming else {
        return Ok(());
    };
    if state
        .as_deref()
        .is_some_and(|expected| expected != incoming)
    {
        return Err(protocol(format!(
            "Gemini stream changed {label} between chunks"
        )));
    }
    state.get_or_insert_with(|| incoming.into());
    Ok(())
}

fn decode_part(
    decoder: &mut GeminiEventDecoder,
    part: &Value,
    events: &mut Vec<ModelStreamEvent>,
) -> Result<(), ModelError> {
    if let Some(call) = part.get("functionCall") {
        let index = decoder.allocate()?;
        events.push(ModelStreamEvent::ContentPartCompleted {
            index,
            part: decode_tool_call(call, part, index)?,
        });
    } else if part
        .get("thought")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        let index = decoder.allocate()?;
        events.push(ModelStreamEvent::ContentPartCompleted {
            index,
            part: ContentPart::Reasoning(ReasoningPart {
                text: part.get("text").and_then(Value::as_str).map(String::from),
                signature: part
                    .get("thoughtSignature")
                    .and_then(Value::as_str)
                    .map(String::from),
                redacted: false,
                provider_data: Vec::new(),
            }),
        });
    } else if let Some(text) = part.get("text").and_then(Value::as_str) {
        let signature = thought_signature(part);
        if !text.is_empty() {
            let index = decoder.text_index(events)?;
            events.push(ModelStreamEvent::TextDelta {
                index,
                text: text.into(),
            });
        }
        preserve_signed_part(decoder, part, signature, !text.is_empty(), events)?;
    } else if let Some(inline) = part.get("inlineData") {
        let index = decoder.allocate()?;
        let media_type = required_string(inline, "mimeType")?.to_owned();
        let data = required_string(inline, "data")?.to_owned();
        let content = media_part(&media_type, data)?;
        events.push(ModelStreamEvent::ContentPartCompleted {
            index,
            part: content,
        });
        preserve_signed_part(decoder, part, thought_signature(part), true, events)?;
    } else if let Some(file) = part.get("fileData") {
        let index = decoder.allocate()?;
        events.push(ModelStreamEvent::ContentPartCompleted {
            index,
            part: ContentPart::ResourceLink {
                uri: required_string(file, "fileUri")?.to_owned(),
                name: format!("gemini-file-{index}"),
                title: None,
                description: None,
                media_type: file
                    .get("mimeType")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                size: None,
            },
        });
        preserve_signed_part(decoder, part, thought_signature(part), true, events)?;
    } else {
        events.push(provider_event("part", part.clone()));
    }
    Ok(())
}

fn thought_signature(part: &Value) -> Option<&str> {
    part.get("thoughtSignature")
        .or_else(|| part.get("thought_signature"))
        .and_then(Value::as_str)
}

fn preserve_signed_part(
    decoder: &mut GeminiEventDecoder,
    part: &Value,
    signature: Option<&str>,
    replaces_previous: bool,
    events: &mut Vec<ModelStreamEvent>,
) -> Result<(), ModelError> {
    if signature.is_none() {
        return Ok(());
    }
    let index = decoder.allocate()?;
    events.push(ModelStreamEvent::ContentPartCompleted {
        index,
        part: ContentPart::ProviderOpaque(runifold_model::ProviderData {
            provider: "gemini".into(),
            kind: if replaces_previous {
                "signed_part_replay".into()
            } else {
                "part".into()
            },
            value: part.clone(),
        }),
    });
    Ok(())
}

fn media_part(media_type: &str, data: String) -> Result<ContentPart, ModelError> {
    let source = runifold_model::MediaSource::Base64 {
        media_type: media_type.into(),
        data,
    };
    if media_type.starts_with("image/") {
        Ok(ContentPart::Image { source })
    } else if media_type.starts_with("audio/") {
        Ok(ContentPart::Audio { source })
    } else if media_type.contains('/') {
        Ok(ContentPart::Document { source, name: None })
    } else {
        Err(protocol("Gemini inlineData MIME type is invalid"))
    }
}

fn decode_tool_call(call: &Value, part: &Value, index: u32) -> Result<ContentPart, ModelError> {
    let name = required_string(call, "name")?;
    let arguments = call
        .get("args")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let id = call
        .get("id")
        .and_then(Value::as_str)
        .map_or_else(|| format!("gemini-call-{index}"), String::from);
    let mut metadata = BTreeMap::new();
    if let Some(signature) = part
        .get("thoughtSignature")
        .or_else(|| part.get("thought_signature"))
        .and_then(Value::as_str)
    {
        metadata.insert(
            "gemini.thought_signature".into(),
            Value::String(signature.into()),
        );
    }
    Ok(ContentPart::ToolCall(ToolCall {
        id,
        name: name.into(),
        raw_arguments: Some(arguments.to_string()),
        arguments,
        metadata,
    }))
}

fn validate_prompt(payload: &Value) -> Result<(), ModelError> {
    let reason = payload
        .get("promptFeedback")
        .and_then(|feedback| feedback.get("blockReason"))
        .and_then(Value::as_str);
    reason.map_or(Ok(()), |reason| {
        Err(provider_error(&serde_json::json!({
            "message":format!("Gemini blocked the prompt: {reason}"),
            "status":reason
        })))
    })
}

fn decode_usage(value: &Value) -> ModelUsage {
    ModelUsage {
        input_tokens: unsigned(value, "promptTokenCount"),
        output_tokens: unsigned(value, "candidatesTokenCount"),
        reasoning_tokens: unsigned(value, "thoughtsTokenCount"),
        cached_input_tokens: unsigned(value, "cachedContentTokenCount"),
        ..ModelUsage::default()
    }
}

fn finish_reason(reason: &str) -> FinishReason {
    match reason {
        "STOP" => FinishReason::Stop,
        "MAX_TOKENS" => FinishReason::Length,
        "SAFETY" | "RECITATION" | "LANGUAGE" | "BLOCKLIST" | "PROHIBITED_CONTENT" | "SPII"
        | "IMAGE_SAFETY" => FinishReason::ContentFilter,
        "MALFORMED_FUNCTION_CALL" => FinishReason::Error,
        other => FinishReason::Other(other.into()),
    }
}

fn provider_error(value: &Value) -> ModelError {
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Gemini returned an error");
    let mut error = ModelError::local(ModelErrorKind::Provider, message);
    error.provider = Some("gemini".into());
    if let Some(status) = value.get("status") {
        error
            .metadata
            .insert("gemini.error.status".into(), status.clone());
    }
    error
}

fn protocol(message: impl Into<String>) -> ModelError {
    let mut error = ModelError::local(ModelErrorKind::Protocol, message);
    error.provider = Some("gemini".into());
    error
}

fn provider_event(name: &str, payload: Value) -> ModelStreamEvent {
    ModelStreamEvent::Provider {
        event: ProviderEvent {
            provider: "gemini".into(),
            name: name.into(),
            payload,
        },
    }
}

fn redact_inline_media(mut value: Value) -> Value {
    match &mut value {
        Value::Array(values) => {
            for value in values {
                *value = redact_inline_media(value.take());
            }
        }
        Value::Object(object) => {
            if let Some(Value::Object(inline)) = object.get_mut("inlineData")
                && let Some(Value::String(data)) = inline.get_mut("data")
            {
                let encoded_len = data.len();
                *data = format!("[redacted base64: {encoded_len} chars]");
            }
            for value in object.values_mut() {
                *value = redact_inline_media(value.take());
            }
        }
        _ => {}
    }
    value
}

fn required_string<'a>(value: &'a Value, key: &str) -> Result<&'a str, ModelError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| protocol(format!("Gemini object is missing string `{key}`")))
}

fn unsigned(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use runifold_model::{
        ContentPart, FinishReason, MediaSource, ModelStreamAccumulator, ModelStreamEvent,
    };
    use serde_json::json;

    use super::GeminiEventDecoder;

    #[test]
    fn decodes_text_usage_and_terminal_reason() {
        let mut decoder = GeminiEventDecoder::new("gemini-test");
        let events = decoder
            .decode(&json!({
                "responseId":"resp",
                "modelVersion":"gemini-test-1",
                "candidates":[{
                    "content":{"parts":[{"text":"hello"}]},
                    "finishReason":"STOP"
                }],
                "usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":3}
            }))
            .unwrap();

        assert!(events.iter().any(|event| matches!(
            event,
            ModelStreamEvent::ResponseCompleted {
                finish_reason: FinishReason::Stop,
                ..
            }
        )));
        decoder.finish().unwrap();
    }

    #[test]
    fn decodes_inline_binary_media_into_the_canonical_stream() {
        let mut decoder = GeminiEventDecoder::new("gemini-image");
        let events = decoder
            .decode(&json!({
                "candidates":[{
                    "content":{"parts":[{
                        "inlineData":{"mimeType":"image/png","data":"aW1hZ2U="}
                    }]},
                    "finishReason":"STOP"
                }]
            }))
            .unwrap();
        assert!(
            events
                .iter()
                .filter_map(|event| match event {
                    ModelStreamEvent::Provider { event } => Some(event.payload.to_string()),
                    _ => None,
                })
                .all(|payload| !payload.contains("aW1hZ2U="))
        );
        let mut accumulator = ModelStreamAccumulator::new();
        let response = events
            .into_iter()
            .find_map(|event| accumulator.push(event).unwrap())
            .unwrap();

        assert!(matches!(
            &response.content[0],
            ContentPart::Image {
                source: MediaSource::Base64 { media_type, data }
            } if media_type == "image/png" && data == "aW1hZ2U="
        ));
        decoder.finish().unwrap();
    }

    #[test]
    fn rejects_chunks_after_terminal_candidate() {
        let mut decoder = GeminiEventDecoder::new("gemini-test");
        decoder
            .decode(&json!({"candidates":[{"finishReason":"STOP"}]}))
            .unwrap();

        assert!(decoder.decode(&json!({"candidates":[]})).is_err());
    }

    #[test]
    fn rejects_response_identity_changes_between_chunks() {
        let mut decoder = GeminiEventDecoder::new("gemini-test");
        decoder
            .decode(&json!({"responseId":"one","modelVersion":"v1","candidates":[]}))
            .unwrap();

        assert!(
            decoder
                .decode(&json!({"responseId":"two","modelVersion":"v1","candidates":[]}))
                .is_err()
        );
    }

    #[test]
    fn preserves_function_call_thought_signature_for_replay() {
        let mut decoder = GeminiEventDecoder::new("gemini-3-test");
        let events = decoder
            .decode(&json!({
                "candidates":[{
                    "content":{"parts":[{
                        "functionCall":{"id":"call-1","name":"lookup","args":{}},
                        "thoughtSignature":"opaque-signature"
                    }]},
                    "finishReason":"STOP"
                }]
            }))
            .unwrap();
        let mut accumulator = ModelStreamAccumulator::new();
        let response = events
            .into_iter()
            .find_map(|event| accumulator.push(event).unwrap())
            .unwrap();
        let ContentPart::ToolCall(call) = &response.content[0] else {
            panic!("fixture must decode a function call");
        };

        assert_eq!(
            call.metadata["gemini.thought_signature"],
            "opaque-signature"
        );
    }
}
