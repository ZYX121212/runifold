use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    ContentPart, DEFAULT_MAX_ARTIFACT_BYTES, FinishReason, MediaSource, ModelError, ModelErrorKind,
    ModelRef, ModelResponse, ModelUsage, ModelWarning, ProviderData, ReasoningPart, ToolCall,
};

/// The type and initial metadata of a streamed content block.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ContentBlockKind {
    /// Text output.
    Text,
    /// Reasoning or thinking output.
    Reasoning {
        /// Initial signature or continuation token.
        signature: Option<String>,
        /// Whether the provider redacted the reasoning body.
        redacted: bool,
    },
    /// A streamed tool call.
    ToolCall {
        /// Provider- or runtime-assigned call identity.
        id: String,
        /// Tool name.
        name: String,
    },
    /// A streamed refusal.
    Refusal,
    /// Streamed image output.
    Image {
        /// MIME type of the completed image.
        media_type: String,
    },
    /// Streamed audio output.
    Audio {
        /// MIME type of the completed audio.
        media_type: String,
    },
    /// Streamed document output.
    Document {
        /// MIME type of the completed document.
        media_type: String,
        /// Optional neutral display name.
        name: Option<String>,
    },
}

/// A provider event retained without normalization.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ProviderEvent {
    /// Provider namespace.
    pub provider: String,
    /// Provider event name.
    pub name: String,
    /// Original structured payload.
    pub payload: Value,
}

/// Canonical events emitted by a streaming model call.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ModelStreamEvent {
    /// The provider accepted the request and started a response.
    ResponseStarted {
        /// Provider response identity.
        id: Option<String>,
        /// Actual model serving the request.
        model: ModelRef,
    },
    /// A delta-capable content block started.
    ContentBlockStarted {
        /// Stable output ordering index.
        index: u32,
        /// Block type and initial metadata.
        kind: ContentBlockKind,
    },
    /// A text delta.
    TextDelta {
        /// Target block index.
        index: u32,
        /// Appended text.
        text: String,
    },
    /// A reasoning-text delta.
    ReasoningDelta {
        /// Target block index.
        index: u32,
        /// Appended reasoning text.
        text: String,
    },
    /// A reasoning-signature delta.
    ReasoningSignatureDelta {
        /// Target block index.
        index: u32,
        /// Appended signature data.
        signature: String,
    },
    /// A raw JSON fragment for tool arguments.
    ToolArgumentsDelta {
        /// Target block index.
        index: u32,
        /// Appended raw JSON text.
        json: String,
    },
    /// A refusal-text delta.
    RefusalDelta {
        /// Target block index.
        index: u32,
        /// Appended refusal text.
        text: String,
    },
    /// One independently base64-encoded binary media chunk.
    BinaryDelta {
        /// Target image, audio, or document block index.
        index: u32,
        /// Base64-encoded chunk bytes.
        data: String,
    },
    /// A delta-capable block completed.
    ContentBlockCompleted {
        /// Completed block index.
        index: u32,
    },
    /// A complete non-delta content part arrived.
    ContentPartCompleted {
        /// Stable output ordering index.
        index: u32,
        /// Completed content.
        part: ContentPart,
    },
    /// A cumulative usage snapshot.
    UsageUpdated {
        /// Latest cumulative model usage.
        usage: ModelUsage,
    },
    /// A translation or feature-degradation warning.
    Warning {
        /// Visible warning.
        warning: ModelWarning,
    },
    /// A provider heartbeat without model content.
    Heartbeat,
    /// An unknown or provider-specific event.
    Provider {
        /// Retained provider event.
        event: ProviderEvent,
    },
    /// The response completed.
    ResponseCompleted {
        /// Normalized terminal reason.
        finish_reason: FinishReason,
        /// Namespaced terminal provider metadata.
        provider_metadata: BTreeMap<String, Value>,
    },
    /// The provider's authoritative complete JSON argument text.
    ToolArgumentsCompleted {
        /// Target tool-call block index.
        index: u32,
        /// Complete raw JSON text, replacing any accumulated deltas.
        json: String,
    },
    /// Namespaced metadata for an open or completed content block.
    ///
    /// Providers may learn final item metadata only after the delta-capable
    /// block itself has completed. Accumulators therefore apply these updates
    /// to either state without reopening the block.
    ContentBlockMetadata {
        /// Target block index.
        index: u32,
        /// Namespaced metadata to merge into the canonical content part.
        metadata: BTreeMap<String, Value>,
    },
}

#[derive(Debug)]
enum PartialBlock {
    Text(String),
    Reasoning {
        text: String,
        signature: Option<String>,
        redacted: bool,
    },
    ToolCall {
        id: String,
        name: String,
        arguments: String,
        metadata: BTreeMap<String, Value>,
    },
    Refusal(String),
    Media {
        kind: PartialMediaKind,
        bytes: Vec<u8>,
    },
}

#[derive(Debug)]
enum PartialMediaKind {
    Image {
        media_type: String,
    },
    Audio {
        media_type: String,
    },
    Document {
        media_type: String,
        name: Option<String>,
    },
}

impl PartialBlock {
    fn from_kind(kind: ContentBlockKind) -> Self {
        match kind {
            ContentBlockKind::Text => Self::Text(String::new()),
            ContentBlockKind::Reasoning {
                signature,
                redacted,
            } => Self::Reasoning {
                text: String::new(),
                signature,
                redacted,
            },
            ContentBlockKind::ToolCall { id, name } => Self::ToolCall {
                id,
                name,
                arguments: String::new(),
                metadata: BTreeMap::new(),
            },
            ContentBlockKind::Refusal => Self::Refusal(String::new()),
            ContentBlockKind::Image { media_type } => Self::Media {
                kind: PartialMediaKind::Image { media_type },
                bytes: Vec::new(),
            },
            ContentBlockKind::Audio { media_type } => Self::Media {
                kind: PartialMediaKind::Audio { media_type },
                bytes: Vec::new(),
            },
            ContentBlockKind::Document { media_type, name } => Self::Media {
                kind: PartialMediaKind::Document { media_type, name },
                bytes: Vec::new(),
            },
        }
    }

    fn complete(self) -> Result<ContentPart, ModelError> {
        match self {
            Self::Text(text) => Ok(ContentPart::Text { text }),
            Self::Reasoning {
                text,
                signature,
                redacted,
            } => Ok(ContentPart::Reasoning(ReasoningPart {
                text: (!text.is_empty()).then_some(text),
                signature,
                redacted,
                provider_data: Vec::new(),
            })),
            Self::ToolCall {
                id,
                name,
                arguments,
                metadata,
            } => {
                let parsed = if arguments.trim().is_empty() {
                    serde_json::json!({})
                } else {
                    serde_json::from_str(&arguments).map_err(|error| {
                        ModelError::local(
                            ModelErrorKind::MalformedToolArguments,
                            format!("tool call {id} returned invalid JSON arguments: {error}"),
                        )
                    })?
                };
                Ok(ContentPart::ToolCall(ToolCall {
                    id,
                    name,
                    arguments: parsed,
                    raw_arguments: Some(arguments),
                    metadata,
                }))
            }
            Self::Refusal(text) => Ok(ContentPart::Refusal { text }),
            Self::Media { kind, bytes } => {
                let data = STANDARD.encode(bytes);
                Ok(match kind {
                    PartialMediaKind::Image { media_type } => ContentPart::Image {
                        source: MediaSource::Base64 { media_type, data },
                    },
                    PartialMediaKind::Audio { media_type } => ContentPart::Audio {
                        source: MediaSource::Base64 { media_type, data },
                    },
                    PartialMediaKind::Document { media_type, name } => ContentPart::Document {
                        source: MediaSource::Base64 { media_type, data },
                        name,
                    },
                })
            }
        }
    }
}

/// Strictly reconstructs a canonical response from model stream events.
#[derive(Debug, Default)]
pub struct ModelStreamAccumulator {
    started: bool,
    completed: bool,
    id: Option<String>,
    model: Option<ModelRef>,
    open_blocks: BTreeMap<u32, PartialBlock>,
    content: BTreeMap<u32, ContentPart>,
    usage: ModelUsage,
    warnings: Vec<ModelWarning>,
    provider_events: Vec<ProviderData>,
}

impl ModelStreamAccumulator {
    /// Creates an empty accumulator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies one event and returns the response when the terminal event arrives.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError`] when event order is invalid, a delta targets the
    /// wrong block type, indices collide, a response completes with open
    /// blocks, or tool arguments contain malformed JSON.
    pub fn push(&mut self, event: ModelStreamEvent) -> Result<Option<ModelResponse>, ModelError> {
        if self.completed {
            return Err(state_error("received an event after response completion"));
        }

        match event {
            ModelStreamEvent::ResponseStarted { id, model } => self.start(id, model),
            ModelStreamEvent::ContentBlockStarted { index, kind } => self.start_block(index, kind),
            ModelStreamEvent::TextDelta { index, text } => {
                match self.open_block_mut(index)? {
                    PartialBlock::Text(current) => current.push_str(&text),
                    _ => return Err(wrong_delta(index, "text")),
                }
                Ok(None)
            }
            ModelStreamEvent::ReasoningDelta { index, text } => {
                match self.open_block_mut(index)? {
                    PartialBlock::Reasoning { text: current, .. } => current.push_str(&text),
                    _ => return Err(wrong_delta(index, "reasoning")),
                }
                Ok(None)
            }
            ModelStreamEvent::ReasoningSignatureDelta { index, signature } => {
                match self.open_block_mut(index)? {
                    PartialBlock::Reasoning {
                        signature: current, ..
                    } => current.get_or_insert_with(String::new).push_str(&signature),
                    _ => return Err(wrong_delta(index, "reasoning signature")),
                }
                Ok(None)
            }
            ModelStreamEvent::ToolArgumentsDelta { index, json } => {
                self.update_tool_arguments(index, json, false)?;
                Ok(None)
            }
            ModelStreamEvent::ToolArgumentsCompleted { index, json } => {
                self.update_tool_arguments(index, json, true)?;
                Ok(None)
            }
            ModelStreamEvent::ContentBlockMetadata { index, metadata } => {
                self.merge_block_metadata(index, metadata)
            }
            ModelStreamEvent::RefusalDelta { index, text } => {
                match self.open_block_mut(index)? {
                    PartialBlock::Refusal(current) => current.push_str(&text),
                    _ => return Err(wrong_delta(index, "refusal")),
                }
                Ok(None)
            }
            ModelStreamEvent::BinaryDelta { index, data } => {
                let decoded = STANDARD.decode(data).map_err(|error| {
                    state_error(format!("binary delta {index} is invalid base64: {error}"))
                })?;
                match self.open_block_mut(index)? {
                    PartialBlock::Media { bytes, .. } => {
                        let next = bytes.len().checked_add(decoded.len()).ok_or_else(|| {
                            state_error(format!("binary block {index} size overflow"))
                        })?;
                        if next > DEFAULT_MAX_ARTIFACT_BYTES {
                            return Err(state_error(format!(
                                "binary block {index} exceeds the {DEFAULT_MAX_ARTIFACT_BYTES}-byte limit"
                            )));
                        }
                        bytes.extend_from_slice(&decoded);
                    }
                    _ => return Err(wrong_delta(index, "binary media")),
                }
                Ok(None)
            }
            ModelStreamEvent::ContentBlockCompleted { index } => self.complete_block(index),
            ModelStreamEvent::ContentPartCompleted { index, part } => {
                self.complete_part(index, part)
            }
            ModelStreamEvent::UsageUpdated { usage } => {
                self.require_started()?;
                self.usage = usage;
                Ok(None)
            }
            ModelStreamEvent::Warning { warning } => {
                self.require_started()?;
                self.warnings.push(warning);
                Ok(None)
            }
            ModelStreamEvent::Heartbeat => {
                self.require_started()?;
                Ok(None)
            }
            ModelStreamEvent::Provider { event } => {
                self.require_started()?;
                self.provider_events.push(ProviderData {
                    provider: event.provider,
                    kind: event.name,
                    value: event.payload,
                });
                Ok(None)
            }
            ModelStreamEvent::ResponseCompleted {
                finish_reason,
                provider_metadata,
            } => self.complete(finish_reason, provider_metadata),
        }
    }

    fn start(
        &mut self,
        id: Option<String>,
        model: ModelRef,
    ) -> Result<Option<ModelResponse>, ModelError> {
        if self.started {
            return Err(state_error("received more than one response-start event"));
        }
        self.started = true;
        self.id = id;
        self.model = Some(model);
        Ok(None)
    }

    fn update_tool_arguments(
        &mut self,
        index: u32,
        json: String,
        complete: bool,
    ) -> Result<(), ModelError> {
        match self.open_block_mut(index)? {
            PartialBlock::ToolCall { arguments, .. } if complete => *arguments = json,
            PartialBlock::ToolCall { arguments, .. } => arguments.push_str(&json),
            _ if complete => return Err(wrong_delta(index, "completed tool arguments")),
            _ => return Err(wrong_delta(index, "tool arguments")),
        }
        Ok(())
    }

    fn start_block(
        &mut self,
        index: u32,
        kind: ContentBlockKind,
    ) -> Result<Option<ModelResponse>, ModelError> {
        self.require_started()?;
        self.require_unused_index(index)?;
        self.open_blocks
            .insert(index, PartialBlock::from_kind(kind));
        Ok(None)
    }

    fn complete_block(&mut self, index: u32) -> Result<Option<ModelResponse>, ModelError> {
        self.require_started()?;
        let block = self
            .open_blocks
            .remove(&index)
            .ok_or_else(|| state_error(format!("content block {index} is not open")))?;
        self.content.insert(index, block.complete()?);
        Ok(None)
    }

    fn complete_part(
        &mut self,
        index: u32,
        part: ContentPart,
    ) -> Result<Option<ModelResponse>, ModelError> {
        self.require_started()?;
        self.require_unused_index(index)?;
        self.content.insert(index, part);
        Ok(None)
    }

    fn merge_block_metadata(
        &mut self,
        index: u32,
        metadata: BTreeMap<String, Value>,
    ) -> Result<Option<ModelResponse>, ModelError> {
        self.require_started()?;
        if let Some(block) = self.open_blocks.get_mut(&index) {
            return match block {
                PartialBlock::ToolCall {
                    metadata: current, ..
                } => {
                    current.extend(metadata);
                    Ok(None)
                }
                _ => Err(wrong_delta(index, "content metadata")),
            };
        }
        match self.content.get_mut(&index) {
            Some(ContentPart::ToolCall(call)) => {
                call.metadata.extend(metadata);
                Ok(None)
            }
            Some(_) => Err(wrong_delta(index, "content metadata")),
            None => Err(state_error(format!(
                "content metadata targeted unknown block {index}"
            ))),
        }
    }

    fn complete(
        &mut self,
        mut finish_reason: FinishReason,
        provider_metadata: BTreeMap<String, Value>,
    ) -> Result<Option<ModelResponse>, ModelError> {
        self.require_started()?;
        if !self.open_blocks.is_empty() {
            let open = self
                .open_blocks
                .keys()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            return Err(state_error(format!(
                "response completed with open content blocks: {open}"
            )));
        }
        self.completed = true;
        let has_tool_calls = self
            .content
            .values()
            .any(|part| matches!(part, ContentPart::ToolCall(_)));
        if has_tool_calls && matches!(finish_reason, FinishReason::Stop) {
            finish_reason = FinishReason::ToolCalls;
        }
        let model = self
            .model
            .clone()
            .ok_or_else(|| state_error("response model is missing"))?;
        Ok(Some(ModelResponse {
            id: self.id.clone(),
            model,
            content: std::mem::take(&mut self.content).into_values().collect(),
            finish_reason,
            usage: self.usage,
            warnings: std::mem::take(&mut self.warnings),
            provider_metadata,
            provider_events: std::mem::take(&mut self.provider_events),
        }))
    }

    fn require_started(&self) -> Result<(), ModelError> {
        if self.started {
            Ok(())
        } else {
            Err(state_error("received content before response start"))
        }
    }

    fn require_unused_index(&self, index: u32) -> Result<(), ModelError> {
        if self.open_blocks.contains_key(&index) || self.content.contains_key(&index) {
            Err(state_error(format!(
                "content block index {index} was already used"
            )))
        } else {
            Ok(())
        }
    }

    fn open_block_mut(&mut self, index: u32) -> Result<&mut PartialBlock, ModelError> {
        self.require_started()?;
        self.open_blocks
            .get_mut(&index)
            .ok_or_else(|| state_error(format!("content block {index} is not open")))
    }
}

fn wrong_delta(index: u32, delta: &str) -> ModelError {
    state_error(format!(
        "{delta} delta does not match content block {index}"
    ))
}

fn state_error(message: impl Into<String>) -> ModelError {
    ModelError::local(ModelErrorKind::StreamState, message)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{ContentBlockKind, ModelStreamAccumulator, ModelStreamEvent, ProviderEvent};
    use crate::{
        ContentPart, FinishReason, MediaSource, ModelErrorKind, ModelRef, ModelUsage, ModelWarning,
        ToolCall,
    };

    fn started() -> ModelStreamEvent {
        ModelStreamEvent::ResponseStarted {
            id: Some("response-1".into()),
            model: ModelRef::new("test", "model"),
        }
    }

    fn completed() -> ModelStreamEvent {
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::Stop,
            provider_metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn accumulates_ordered_text_and_tool_calls() {
        let mut accumulator = ModelStreamAccumulator::new();
        let events = [
            started(),
            ModelStreamEvent::ContentBlockStarted {
                index: 1,
                kind: ContentBlockKind::ToolCall {
                    id: "call-1".into(),
                    name: "search".into(),
                },
            },
            ModelStreamEvent::ToolArgumentsDelta {
                index: 1,
                json: "{\"query\":".into(),
            },
            ModelStreamEvent::ContentBlockStarted {
                index: 0,
                kind: ContentBlockKind::Text,
            },
            ModelStreamEvent::TextDelta {
                index: 0,
                text: "I will search.".into(),
            },
            ModelStreamEvent::ToolArgumentsDelta {
                index: 1,
                json: "\"rust\"}".into(),
            },
            ModelStreamEvent::ContentBlockMetadata {
                index: 1,
                metadata: BTreeMap::from([("test.status".into(), serde_json::json!("completed"))]),
            },
            ModelStreamEvent::ContentBlockCompleted { index: 0 },
            ModelStreamEvent::ContentBlockCompleted { index: 1 },
            ModelStreamEvent::UsageUpdated {
                usage: ModelUsage {
                    input_tokens: 5,
                    output_tokens: 3,
                    ..ModelUsage::default()
                },
            },
            completed(),
        ];

        let response = events
            .into_iter()
            .find_map(|event| accumulator.push(event).unwrap())
            .unwrap();

        assert_eq!(response.content[0], ContentPart::text("I will search."));
        assert_eq!(
            response.content[1],
            ContentPart::ToolCall(ToolCall {
                id: "call-1".into(),
                name: "search".into(),
                arguments: serde_json::json!({"query": "rust"}),
                raw_arguments: Some("{\"query\":\"rust\"}".into()),
                metadata: BTreeMap::from([("test.status".into(), serde_json::json!("completed"),)]),
            })
        );
        assert_eq!(response.usage.input_tokens, 5);
    }

    #[test]
    fn preserves_provider_events_and_warnings() {
        let mut accumulator = ModelStreamAccumulator::new();
        accumulator.push(started()).unwrap();
        accumulator
            .push(ModelStreamEvent::Provider {
                event: ProviderEvent {
                    provider: "test".into(),
                    name: "ping".into(),
                    payload: serde_json::json!({"alive": true}),
                },
            })
            .unwrap();
        accumulator
            .push(ModelStreamEvent::Warning {
                warning: ModelWarning {
                    code: "emulated".into(),
                    message: "structured output was emulated".into(),
                    metadata: BTreeMap::new(),
                },
            })
            .unwrap();
        let response = accumulator.push(completed()).unwrap().unwrap();

        assert_eq!(response.provider_events.len(), 1);
        assert_eq!(response.provider_events[0].kind, "ping");
        assert_eq!(response.warnings.len(), 1);
    }

    #[test]
    fn rejects_delta_without_matching_open_block() {
        let mut accumulator = ModelStreamAccumulator::new();
        accumulator.push(started()).unwrap();

        let error = accumulator
            .push(ModelStreamEvent::TextDelta {
                index: 4,
                text: "orphan".into(),
            })
            .unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::StreamState);
    }

    #[test]
    fn rejects_completion_with_open_blocks() {
        let mut accumulator = ModelStreamAccumulator::new();
        accumulator.push(started()).unwrap();
        accumulator
            .push(ModelStreamEvent::ContentBlockStarted {
                index: 0,
                kind: ContentBlockKind::Text,
            })
            .unwrap();

        let error = accumulator.push(completed()).unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::StreamState);
    }

    #[test]
    fn rejects_malformed_tool_arguments() {
        let mut accumulator = ModelStreamAccumulator::new();
        accumulator.push(started()).unwrap();
        accumulator
            .push(ModelStreamEvent::ContentBlockStarted {
                index: 0,
                kind: ContentBlockKind::ToolCall {
                    id: "bad".into(),
                    name: "tool".into(),
                },
            })
            .unwrap();
        accumulator
            .push(ModelStreamEvent::ToolArgumentsDelta {
                index: 0,
                json: "{invalid".into(),
            })
            .unwrap();

        let error = accumulator
            .push(ModelStreamEvent::ContentBlockCompleted { index: 0 })
            .unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::MalformedToolArguments);
    }

    #[test]
    fn completed_arguments_replace_partial_deltas() {
        let mut accumulator = ModelStreamAccumulator::new();
        accumulator.push(started()).unwrap();
        accumulator
            .push(ModelStreamEvent::ContentBlockStarted {
                index: 0,
                kind: ContentBlockKind::ToolCall {
                    id: "call".into(),
                    name: "tool".into(),
                },
            })
            .unwrap();
        accumulator
            .push(ModelStreamEvent::ToolArgumentsDelta {
                index: 0,
                json: "{\"stale\":".into(),
            })
            .unwrap();
        accumulator
            .push(ModelStreamEvent::ToolArgumentsCompleted {
                index: 0,
                json: "{\"final\":true}".into(),
            })
            .unwrap();
        accumulator
            .push(ModelStreamEvent::ContentBlockCompleted { index: 0 })
            .unwrap();
        let response = accumulator.push(completed()).unwrap().unwrap();

        let ContentPart::ToolCall(call) = &response.content[0] else {
            panic!("fixture must produce a tool call");
        };
        assert_eq!(call.arguments, serde_json::json!({"final": true}));
        assert_eq!(call.raw_arguments.as_deref(), Some("{\"final\":true}"));
    }

    #[test]
    fn tool_content_normalizes_stop_reason() {
        let mut accumulator = ModelStreamAccumulator::new();
        accumulator.push(started()).unwrap();
        accumulator
            .push(ModelStreamEvent::ContentPartCompleted {
                index: 0,
                part: ContentPart::ToolCall(ToolCall {
                    id: "call".into(),
                    name: "tool".into(),
                    arguments: serde_json::json!({}),
                    raw_arguments: Some("{}".into()),
                    metadata: BTreeMap::new(),
                }),
            })
            .unwrap();
        let response = accumulator.push(completed()).unwrap().unwrap();

        assert_eq!(response.finish_reason, FinishReason::ToolCalls);
    }

    #[test]
    fn tool_content_does_not_hide_unknown_finish_reason() {
        let mut accumulator = ModelStreamAccumulator::new();
        accumulator
            .push(ModelStreamEvent::ResponseStarted {
                id: None,
                model: ModelRef::new("test", "model"),
            })
            .unwrap();
        accumulator
            .push(ModelStreamEvent::ContentPartCompleted {
                index: 0,
                part: ContentPart::ToolCall(ToolCall {
                    id: "call-1".into(),
                    name: "lookup".into(),
                    arguments: serde_json::json!({}),
                    raw_arguments: Some("{}".into()),
                    metadata: BTreeMap::new(),
                }),
            })
            .unwrap();
        let response = accumulator
            .push(ModelStreamEvent::ResponseCompleted {
                finish_reason: FinishReason::Unknown,
                provider_metadata: BTreeMap::new(),
            })
            .unwrap()
            .unwrap();

        assert_eq!(response.finish_reason, FinishReason::Unknown);
    }

    #[test]
    fn explicit_failure_reason_is_not_hidden_by_tool_content() {
        let mut accumulator = ModelStreamAccumulator::new();
        accumulator.push(started()).unwrap();
        accumulator
            .push(ModelStreamEvent::ContentPartCompleted {
                index: 0,
                part: ContentPart::ToolCall(ToolCall {
                    id: "partial".into(),
                    name: "tool".into(),
                    arguments: serde_json::json!({}),
                    raw_arguments: Some("{}".into()),
                    metadata: BTreeMap::new(),
                }),
            })
            .unwrap();
        let response = accumulator
            .push(ModelStreamEvent::ResponseCompleted {
                finish_reason: FinishReason::Length,
                provider_metadata: BTreeMap::new(),
            })
            .unwrap()
            .unwrap();

        assert_eq!(response.finish_reason, FinishReason::Length);
    }

    #[test]
    fn accumulates_bounded_binary_media_chunks() {
        let mut accumulator = ModelStreamAccumulator::new();
        accumulator.push(started()).unwrap();
        accumulator
            .push(ModelStreamEvent::ContentBlockStarted {
                index: 0,
                kind: ContentBlockKind::Image {
                    media_type: "image/png".into(),
                },
            })
            .unwrap();
        accumulator
            .push(ModelStreamEvent::BinaryDelta {
                index: 0,
                data: "cG5n".into(),
            })
            .unwrap();
        accumulator
            .push(ModelStreamEvent::ContentBlockCompleted { index: 0 })
            .unwrap();
        let response = accumulator.push(completed()).unwrap().unwrap();

        assert!(matches!(
            &response.content[0],
            ContentPart::Image {
                source: MediaSource::Base64 { media_type, data }
            } if media_type == "image/png" && data == "cG5n"
        ));
    }
}
