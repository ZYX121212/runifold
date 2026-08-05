use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{ExtensionMap, ModelError, ModelErrorKind};

/// The author role of a model message.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Role {
    /// High-priority system or developer instruction.
    System,
    /// End-user input.
    User,
    /// Model output.
    Assistant,
    /// A tool result represented as a message by a provider.
    Tool,
}

/// A serializable media source.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum MediaSource {
    /// An externally accessible URL.
    Url {
        /// Media URL.
        url: String,
        /// Optional MIME type.
        media_type: Option<String>,
    },
    /// An inline base64 payload.
    Base64 {
        /// MIME type.
        media_type: String,
        /// Base64-encoded bytes.
        data: String,
    },
    /// A reference into an application-owned artifact store.
    Artifact {
        /// Stable artifact identity.
        artifact_id: String,
        /// Optional MIME type.
        media_type: Option<String>,
    },
    /// A file already uploaded to a provider control plane.
    ProviderFile {
        /// Provider namespace that owns the file.
        provider: String,
        /// Provider-assigned file identity.
        file_id: String,
    },
}

impl MediaSource {
    /// Creates a provider-owned file reference.
    pub fn provider_file(provider: impl Into<String>, file_id: impl Into<String>) -> Self {
        Self::ProviderFile {
            provider: provider.into(),
            file_id: file_id.into(),
        }
    }
}

/// Provider-specific data retained without normalization.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ProviderData {
    /// Provider namespace.
    pub provider: String,
    /// Provider-defined data kind.
    pub kind: String,
    /// Unmodified structured data.
    pub value: Value,
}

/// A normalized citation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Citation {
    /// Referenced URI, when available.
    pub uri: Option<String>,
    /// Human-readable title.
    pub title: Option<String>,
    /// Optional character start offset in the associated text.
    pub start: Option<u64>,
    /// Optional character end offset in the associated text.
    pub end: Option<u64>,
}

/// Model reasoning retained for valid round trips.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ReasoningPart {
    /// Reasoning text or provider-generated summary, when exposed.
    pub text: Option<String>,
    /// Provider signature or encrypted continuation token.
    pub signature: Option<String>,
    /// Whether the reasoning body was redacted by the provider.
    pub redacted: bool,
    /// Provider information that has no normalized representation.
    pub provider_data: Vec<ProviderData>,
}

/// A completed tool call requested by a model.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ToolCall {
    /// Provider- or runtime-assigned call identity.
    pub id: String,
    /// Tool name.
    pub name: String,
    /// Parsed JSON arguments.
    pub arguments: Value,
    /// Original argument text, when preserving it matters.
    pub raw_arguments: Option<String>,
    /// Namespaced metadata.
    pub metadata: ExtensionMap,
}

/// A completed tool result supplied to a model.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ToolResult {
    /// Identity of the tool call being answered.
    pub call_id: String,
    /// Tool name, required by providers that do not correlate results by ID.
    #[serde(default)]
    pub name: Option<String>,
    /// Rich result content.
    pub content: Vec<ContentPart>,
    /// Whether tool execution failed.
    pub is_error: bool,
    /// Namespaced metadata.
    pub metadata: ExtensionMap,
}

/// One ordered unit of model-visible content.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ContentPart {
    /// Plain text.
    Text {
        /// Text body.
        text: String,
    },
    /// Image content.
    Image {
        /// Image source.
        source: MediaSource,
    },
    /// Audio content.
    Audio {
        /// Audio source.
        source: MediaSource,
    },
    /// Document content.
    Document {
        /// Document source.
        source: MediaSource,
        /// Optional display name.
        name: Option<String>,
    },
    /// A model-requested tool call.
    ToolCall(ToolCall),
    /// A tool result returned to a model.
    ToolResult(ToolResult),
    /// Provider reasoning or thinking data.
    Reasoning(ReasoningPart),
    /// A provider refusal.
    Refusal {
        /// Refusal explanation.
        text: String,
    },
    /// A citation associated with preceding or adjacent content.
    Citation(Citation),
    /// Information that cannot yet be normalized without loss.
    ProviderOpaque(ProviderData),
}

impl ContentPart {
    /// Creates a text content part.
    pub fn text(value: impl Into<String>) -> Self {
        Self::Text { text: value.into() }
    }
}

/// An ordered message sent to or returned by a model.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Message {
    /// Message author role.
    pub role: Role,
    /// Ordered rich content.
    pub content: Vec<ContentPart>,
    /// Namespaced metadata.
    pub metadata: BTreeMap<String, Value>,
}

impl Message {
    /// Creates a non-empty message.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError`] when `content` is empty.
    pub fn new(role: Role, content: Vec<ContentPart>) -> Result<Self, ModelError> {
        if content.is_empty() {
            return Err(ModelError::local(
                ModelErrorKind::InvalidRequest,
                "a message must contain at least one content part",
            ));
        }
        Ok(Self {
            role,
            content,
            metadata: BTreeMap::new(),
        })
    }

    /// Creates a user text message.
    pub fn user(text: impl Into<String>) -> Self {
        Self {
            role: Role::User,
            content: vec![ContentPart::text(text)],
            metadata: BTreeMap::new(),
        }
    }

    /// Creates a system text message.
    pub fn system(text: impl Into<String>) -> Self {
        Self {
            role: Role::System,
            content: vec![ContentPart::text(text)],
            metadata: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ContentPart, Message, Role, ToolResult};
    use crate::ModelErrorKind;

    #[test]
    fn empty_messages_are_rejected() {
        let error = Message::new(Role::User, Vec::new()).unwrap_err();
        assert_eq!(error.kind, ModelErrorKind::InvalidRequest);
    }

    #[test]
    fn content_round_trips_without_erasing_opaque_data() {
        let message = Message::new(
            Role::Assistant,
            vec![
                ContentPart::text("answer"),
                ContentPart::ProviderOpaque(super::ProviderData {
                    provider: "example".into(),
                    kind: "future_block".into(),
                    value: serde_json::json!({"x": 1}),
                }),
            ],
        )
        .unwrap();

        let encoded = serde_json::to_value(&message).unwrap();
        let decoded: Message = serde_json::from_value(encoded).unwrap();

        assert_eq!(decoded, message);
    }

    #[test]
    fn legacy_tool_results_without_a_name_still_deserialize() {
        let result: ToolResult = serde_json::from_value(serde_json::json!({
            "call_id":"call_1",
            "content":[{"type":"text","text":"ok"}],
            "is_error":false,
            "metadata":{}
        }))
        .unwrap();

        assert_eq!(result.name, None);
    }
}
