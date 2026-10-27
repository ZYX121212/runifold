//! Amazon Bedrock Converse Stream to canonical event translation.

use std::collections::{BTreeMap, BTreeSet};

use aws_sdk_bedrockruntime::types::{
    ContentBlockDelta, ContentBlockStart, ConverseStreamOutput, ReasoningContentBlockDelta,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use runifold_model::{
    ContentBlockKind, FinishReason, ModelError, ModelErrorKind, ModelRef, ModelStreamEvent,
    ModelUsage, ProviderEvent,
};
use serde_json::{Value, json};

/// Stateful decoder for native Amazon Bedrock Converse Stream events.
pub struct BedrockEventDecoder {
    model: String,
    phase: MessagePhase,
    saw_metadata: bool,
    finish_reason: FinishReason,
    pending_blocks: BTreeSet<u32>,
    open_blocks: BTreeSet<u32>,
    redacted_blocks: BTreeMap<u32, Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MessagePhase {
    AwaitingStart,
    Streaming,
    Stopped,
    Completed,
}

impl std::fmt::Debug for BedrockEventDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BedrockEventDecoder")
            .finish_non_exhaustive()
    }
}

impl BedrockEventDecoder {
    /// Creates a decoder for one Bedrock model or inference profile.
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            phase: MessagePhase::AwaitingStart,
            saw_metadata: false,
            finish_reason: FinishReason::Unknown,
            pending_blocks: BTreeSet::new(),
            open_blocks: BTreeSet::new(),
            redacted_blocks: BTreeMap::new(),
        }
    }

    /// Translates one native SDK stream event.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError`] when Bedrock violates the canonical stream
    /// lifecycle or emits an invalid content index.
    #[allow(clippy::too_many_lines)]
    pub fn decode(
        &mut self,
        output: ConverseStreamOutput,
    ) -> Result<Vec<ModelStreamEvent>, ModelError> {
        if self.phase == MessagePhase::Completed {
            return Err(protocol("Bedrock event arrived after response completion"));
        }
        if self.phase == MessagePhase::Stopped
            && !matches!(&output, ConverseStreamOutput::Metadata(_))
        {
            return Err(protocol(
                "Bedrock emitted a non-metadata event after messageStop",
            ));
        }
        let (name, payload, mut events) = match output {
            ConverseStreamOutput::MessageStart(event) => {
                if self.phase != MessagePhase::AwaitingStart {
                    return Err(protocol("Bedrock emitted messageStart more than once"));
                }
                self.phase = MessagePhase::Streaming;
                (
                    "message_start",
                    json!({"role": event.role().as_str()}),
                    vec![ModelStreamEvent::ResponseStarted {
                        id: None,
                        model: ModelRef::new("bedrock", self.model.clone()),
                    }],
                )
            }
            ConverseStreamOutput::ContentBlockStart(event) => {
                self.require_streaming()?;
                let index = index(event.content_block_index())?;
                if self.pending_blocks.contains(&index) || self.open_blocks.contains(&index) {
                    return Err(protocol(format!(
                        "Bedrock content block {index} started more than once"
                    )));
                }
                let mut events = Vec::new();
                let payload = match event.start() {
                    Some(ContentBlockStart::ToolUse(tool)) => {
                        self.open_blocks.insert(index);
                        events.push(ModelStreamEvent::ContentBlockStarted {
                            index,
                            kind: ContentBlockKind::ToolCall {
                                id: tool.tool_use_id().into(),
                                name: tool.name().into(),
                            },
                        });
                        json!({
                            "content_block_index": index,
                            "start": {
                                "type": "tool_use",
                                "tool_use_id": tool.tool_use_id(),
                                "name": tool.name()
                            }
                        })
                    }
                    Some(_) => {
                        self.pending_blocks.insert(index);
                        json!({"content_block_index": index, "start": {"type": "unknown"}})
                    }
                    None => {
                        self.pending_blocks.insert(index);
                        json!({"content_block_index": index})
                    }
                };
                ("content_block_start", payload, events)
            }
            ConverseStreamOutput::ContentBlockDelta(event) => {
                self.require_streaming()?;
                let index = index(event.content_block_index())?;
                let delta = event
                    .delta()
                    .ok_or_else(|| protocol("Bedrock contentBlockDelta omitted its delta"))?;
                let (payload, events) = self.decode_delta(index, delta)?;
                ("content_block_delta", payload, events)
            }
            ConverseStreamOutput::ContentBlockStop(event) => {
                self.require_streaming()?;
                let index = index(event.content_block_index())?;
                let mut events = Vec::new();
                if self.pending_blocks.remove(&index) {
                    return Err(protocol(format!(
                        "Bedrock stopped content block {index} before emitting a supported delta"
                    )));
                }
                if !self.open_blocks.remove(&index) {
                    return Err(protocol(format!(
                        "Bedrock stopped unknown content block {index}"
                    )));
                }
                if let Some(bytes) = self.redacted_blocks.remove(&index) {
                    events.push(ModelStreamEvent::ContentPartCompleted {
                        index,
                        part: runifold_model::ContentPart::ProviderOpaque(
                            runifold_model::ProviderData {
                                provider: "bedrock".into(),
                                kind: "reasoning_redacted".into(),
                                value: Value::String(STANDARD.encode(bytes)),
                            },
                        ),
                    });
                } else {
                    events.push(ModelStreamEvent::ContentBlockCompleted { index });
                }
                (
                    "content_block_stop",
                    json!({"content_block_index": index}),
                    events,
                )
            }
            ConverseStreamOutput::MessageStop(event) => {
                self.require_streaming()?;
                if !self.open_blocks.is_empty() || !self.pending_blocks.is_empty() {
                    return Err(protocol(
                        "Bedrock stopped the message with unfinished content blocks",
                    ));
                }
                if self.phase != MessagePhase::Streaming {
                    return Err(protocol("Bedrock emitted messageStop more than once"));
                }
                self.phase = MessagePhase::Stopped;
                self.finish_reason = finish_reason(event.stop_reason().as_str());
                let payload = json!({"stop_reason": event.stop_reason().as_str()});
                let events = if self.saw_metadata {
                    self.complete()
                } else {
                    Vec::new()
                };
                ("message_stop", payload, events)
            }
            ConverseStreamOutput::Metadata(event) => {
                self.require_message_started()?;
                if self.saw_metadata {
                    return Err(protocol("Bedrock emitted metadata more than once"));
                }
                self.saw_metadata = true;
                let usage = event.usage().map(decode_usage).unwrap_or_default();
                let payload = json!({
                    "usage": {
                        "input_tokens": usage.input_tokens,
                        "output_tokens": usage.output_tokens,
                        "cached_input_tokens": usage.cached_input_tokens,
                        "cache_write_tokens": usage.cache_write_tokens
                    },
                    "latency_ms": event.metrics().map(
                        aws_sdk_bedrockruntime::types::ConverseStreamMetrics::latency_ms
                    )
                });
                let mut events = vec![ModelStreamEvent::UsageUpdated { usage }];
                if self.phase == MessagePhase::Stopped {
                    events.extend(self.complete());
                }
                ("metadata", payload, events)
            }
            _ => (
                "unknown",
                json!({"note": "SDK could not decode a newer Bedrock event variant"}),
                Vec::new(),
            ),
        };

        let position = events
            .iter()
            .position(|event| matches!(event, ModelStreamEvent::ResponseCompleted { .. }))
            .unwrap_or(events.len());
        events.insert(position, provider_event(name, payload));
        Ok(events)
    }

    /// Finalizes a stream that ended after `messageStop` but without metadata.
    ///
    /// # Errors
    ///
    /// Returns a protocol error for truncated streams or unfinished blocks.
    pub fn finish(&mut self) -> Result<Vec<ModelStreamEvent>, ModelError> {
        if !self.open_blocks.is_empty() || !self.pending_blocks.is_empty() {
            return Err(protocol(
                "Bedrock stream ended with unfinished content blocks",
            ));
        }
        match self.phase {
            MessagePhase::Completed => Ok(Vec::new()),
            MessagePhase::Stopped => Ok(self.complete()),
            MessagePhase::AwaitingStart => {
                Err(protocol("Bedrock stream ended before messageStart"))
            }
            MessagePhase::Streaming => Err(protocol("Bedrock stream ended before messageStop")),
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Exhaustive native delta mapping shares one block lifecycle"
    )]
    fn decode_delta(
        &mut self,
        index: u32,
        delta: &ContentBlockDelta,
    ) -> Result<(Value, Vec<ModelStreamEvent>), ModelError> {
        let mut events = Vec::new();
        match delta {
            ContentBlockDelta::Text(text) => {
                self.ensure_started(index, ContentBlockKind::Text, &mut events)?;
                events.push(ModelStreamEvent::TextDelta {
                    index,
                    text: text.clone(),
                });
                Ok((json!({"type": "text", "text": text}), events))
            }
            ContentBlockDelta::ToolUse(tool) => {
                if !self.open_blocks.contains(&index) {
                    return Err(protocol(format!(
                        "Bedrock tool delta targeted unknown content block {index}"
                    )));
                }
                events.push(ModelStreamEvent::ToolArgumentsDelta {
                    index,
                    json: tool.input().into(),
                });
                Ok((json!({"type": "tool_use", "input": tool.input()}), events))
            }
            ContentBlockDelta::ReasoningContent(reasoning) => {
                if let ReasoningContentBlockDelta::RedactedContent(bytes) = reasoning {
                    if self.open_blocks.contains(&index)
                        && !self.redacted_blocks.contains_key(&index)
                    {
                        return Err(protocol(
                            "Bedrock switched an open block to redacted reasoning",
                        ));
                    }
                    self.pending_blocks.remove(&index);
                    self.open_blocks.insert(index);
                    let buffered = self.redacted_blocks.entry(index).or_default();
                    if buffered.len().saturating_add(bytes.as_ref().len()) > 16 * 1024 * 1024 {
                        return Err(protocol("Bedrock redacted reasoning exceeds 16 MiB"));
                    }
                    buffered.extend_from_slice(bytes.as_ref());
                    return Ok((
                        json!({"type":"reasoning_redacted"}),
                        vec![provider_event(
                            "reasoning_redacted",
                            json!({"content_block_index":index}),
                        )],
                    ));
                }
                if self.redacted_blocks.contains_key(&index) {
                    return Err(protocol(
                        "Bedrock switched redacted reasoning to another kind",
                    ));
                }

                let (kind, event, payload) = match reasoning {
                    ReasoningContentBlockDelta::Text(text) => (
                        ContentBlockKind::Reasoning {
                            signature: None,
                            redacted: false,
                        },
                        ModelStreamEvent::ReasoningDelta {
                            index,
                            text: text.clone(),
                        },
                        json!({"type": "reasoning_text", "text": text}),
                    ),
                    ReasoningContentBlockDelta::Signature(signature) => (
                        ContentBlockKind::Reasoning {
                            signature: None,
                            redacted: false,
                        },
                        ModelStreamEvent::ReasoningSignatureDelta {
                            index,
                            signature: signature.clone(),
                        },
                        json!({"type": "reasoning_signature", "signature": signature}),
                    ),
                    _ => {
                        return Err(protocol(
                            "Bedrock reasoning delta cannot be represented losslessly",
                        ));
                    }
                };
                self.ensure_started(index, kind, &mut events)?;
                events.push(event);
                Ok((payload, events))
            }
            ContentBlockDelta::Citation(_) => Ok((
                json!({"type": "citation"}),
                vec![provider_event(
                    "citation_delta",
                    json!({"content_block_index": index}),
                )],
            )),
            _ => Ok((
                json!({"type": "unknown"}),
                vec![provider_event(
                    "unknown_delta",
                    json!({"content_block_index": index}),
                )],
            )),
        }
    }

    fn ensure_started(
        &mut self,
        index: u32,
        kind: ContentBlockKind,
        events: &mut Vec<ModelStreamEvent>,
    ) -> Result<(), ModelError> {
        if self.pending_blocks.remove(&index) {
            self.open_blocks.insert(index);
            events.push(ModelStreamEvent::ContentBlockStarted { index, kind });
            return Ok(());
        }
        if self.open_blocks.contains(&index) {
            Ok(())
        } else {
            Err(protocol(format!(
                "Bedrock delta targeted unknown content block {index}"
            )))
        }
    }

    fn complete(&mut self) -> Vec<ModelStreamEvent> {
        if self.phase == MessagePhase::Completed {
            return Vec::new();
        }
        self.phase = MessagePhase::Completed;
        vec![ModelStreamEvent::ResponseCompleted {
            finish_reason: self.finish_reason.clone(),
            provider_metadata: BTreeMap::new(),
        }]
    }

    fn require_streaming(&self) -> Result<(), ModelError> {
        if self.phase == MessagePhase::Streaming {
            Ok(())
        } else {
            Err(protocol("Bedrock content arrived before messageStart"))
        }
    }

    fn require_message_started(&self) -> Result<(), ModelError> {
        if self.phase == MessagePhase::AwaitingStart {
            Err(protocol("Bedrock metadata arrived before messageStart"))
        } else {
            Ok(())
        }
    }
}

fn decode_usage(usage: &aws_sdk_bedrockruntime::types::TokenUsage) -> ModelUsage {
    ModelUsage {
        input_tokens: non_negative(usage.input_tokens()),
        output_tokens: non_negative(usage.output_tokens()),
        cached_input_tokens: usage.cache_read_input_tokens().map_or(0, non_negative),
        cache_write_tokens: usage.cache_write_input_tokens().map_or(0, non_negative),
        ..ModelUsage::default()
    }
}

fn non_negative(value: i32) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn index(value: i32) -> Result<u32, ModelError> {
    u32::try_from(value).map_err(|_| protocol("Bedrock content index cannot be negative"))
}

fn finish_reason(reason: &str) -> FinishReason {
    match reason {
        "end_turn" | "stop_sequence" => FinishReason::Stop,
        "max_tokens" | "model_context_window_exceeded" => FinishReason::Length,
        "tool_use" => FinishReason::ToolCalls,
        "guardrail_intervened" | "content_filtered" => FinishReason::ContentFilter,
        other => FinishReason::Other(other.into()),
    }
}

fn provider_event(name: &str, payload: Value) -> ModelStreamEvent {
    ModelStreamEvent::Provider {
        event: ProviderEvent {
            provider: "bedrock".into(),
            name: name.into(),
            payload,
        },
    }
}

fn protocol(message: impl Into<String>) -> ModelError {
    let mut error = ModelError::local(ModelErrorKind::Protocol, message);
    error.provider = Some("bedrock".into());
    error
}

#[cfg(test)]
mod tests {
    use aws_sdk_bedrockruntime::types::{
        ContentBlockDelta, ContentBlockDeltaEvent, ContentBlockStartEvent, ContentBlockStopEvent,
        ConversationRole, ConverseStreamMetadataEvent, ConverseStreamMetrics, ConverseStreamOutput,
        MessageStartEvent, MessageStopEvent, StopReason, TokenUsage,
    };
    use runifold_model::{ContentPart, FinishReason, ModelStreamAccumulator};

    use super::BedrockEventDecoder;

    #[test]
    fn decodes_text_usage_raw_events_and_terminal_reason() {
        let outputs = [
            ConverseStreamOutput::MessageStart(
                MessageStartEvent::builder()
                    .role(ConversationRole::Assistant)
                    .build()
                    .unwrap(),
            ),
            ConverseStreamOutput::ContentBlockStart(
                ContentBlockStartEvent::builder()
                    .content_block_index(0)
                    .build()
                    .unwrap(),
            ),
            ConverseStreamOutput::ContentBlockDelta(
                ContentBlockDeltaEvent::builder()
                    .content_block_index(0)
                    .delta(ContentBlockDelta::Text("hello".into()))
                    .build()
                    .unwrap(),
            ),
            ConverseStreamOutput::ContentBlockStop(
                ContentBlockStopEvent::builder()
                    .content_block_index(0)
                    .build()
                    .unwrap(),
            ),
            ConverseStreamOutput::MessageStop(
                MessageStopEvent::builder()
                    .stop_reason(StopReason::EndTurn)
                    .build()
                    .unwrap(),
            ),
            ConverseStreamOutput::Metadata(
                ConverseStreamMetadataEvent::builder()
                    .usage(
                        TokenUsage::builder()
                            .input_tokens(3)
                            .output_tokens(2)
                            .total_tokens(5)
                            .cache_read_input_tokens(1)
                            .cache_write_input_tokens(1)
                            .build()
                            .unwrap(),
                    )
                    .metrics(
                        ConverseStreamMetrics::builder()
                            .latency_ms(12)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            ),
        ];
        let mut decoder = BedrockEventDecoder::new("model");
        let mut accumulator = ModelStreamAccumulator::new();
        let mut response = None;
        for output in outputs {
            for event in decoder.decode(output).unwrap() {
                if let Some(completed) = accumulator.push(event).unwrap() {
                    response = Some(completed);
                }
            }
        }
        let response = response.expect("metadata should complete the response");

        assert_eq!(response.model.provider, "bedrock");
        assert_eq!(response.content, vec![ContentPart::text("hello")]);
        assert_eq!(response.finish_reason, FinishReason::Stop);
        assert_eq!(response.usage.input_tokens, 3);
        assert_eq!(response.usage.cached_input_tokens, 1);
        assert_eq!(response.provider_events.len(), 6);
        assert!(
            response
                .provider_events
                .iter()
                .all(|event| event.provider == "bedrock")
        );
        assert_eq!(
            decoder.finish().unwrap(),
            [] as [runifold_model::ModelStreamEvent; 0]
        );
    }

    #[test]
    fn rejects_a_block_without_a_supported_delta() {
        let mut decoder = BedrockEventDecoder::new("model");
        decoder
            .decode(ConverseStreamOutput::MessageStart(
                MessageStartEvent::builder()
                    .role(ConversationRole::Assistant)
                    .build()
                    .unwrap(),
            ))
            .unwrap();
        decoder
            .decode(ConverseStreamOutput::ContentBlockStart(
                ContentBlockStartEvent::builder()
                    .content_block_index(0)
                    .build()
                    .unwrap(),
            ))
            .unwrap();

        let error = decoder
            .decode(ConverseStreamOutput::ContentBlockStop(
                ContentBlockStopEvent::builder()
                    .content_block_index(0)
                    .build()
                    .unwrap(),
            ))
            .unwrap_err();
        assert!(error.message.contains("supported delta"));
    }
}
