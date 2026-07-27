use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ContentBlock;

/// Intended recipient of annotated MCP content.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AudienceRole {
    /// Content intended for the human user.
    User,
    /// Content intended for the model assistant.
    Assistant,
}

/// Optional presentation and relevance hints.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Annotations {
    /// Intended recipients.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audience: Vec<AudienceRole>,
    /// Relative importance from 0.0 through 1.0.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<f64>,
    /// ISO-8601 modification timestamp.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
}

/// Display icon advertised by an MCP server.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Icon {
    /// Icon URI.
    pub src: String,
    /// Optional MIME type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    /// Optional supported sizes such as `48x48` or `any`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sizes: Vec<String>,
}

/// Model-facing description of a readable MCP resource.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpResource {
    /// Globally unique resource URI.
    pub uri: String,
    /// Stable logical name.
    pub name: String,
    /// Optional human-facing title.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Optional model-facing description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Optional content MIME type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    /// Optional display icons.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub icons: Vec<Icon>,
    /// Optional usage hints.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Annotations>,
    /// Raw resource size before base64 encoding or tokenization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// Protocol extension metadata.
    #[serde(rename = "_meta", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub meta: BTreeMap<String, Value>,
}

/// Paginated resource-list parameters.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ListResourcesParams {
    /// Opaque continuation cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Result of `resources/list`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListResourcesResult {
    /// Authorized resources in deterministic URI order.
    pub resources: Vec<McpResource>,
    /// Optional continuation cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Model-facing description of a parameterized MCP resource.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpResourceTemplate {
    /// RFC 6570 URI template.
    pub uri_template: String,
    /// Stable logical name.
    pub name: String,
    /// Optional human-facing title.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Optional model-facing description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Optional content MIME type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    /// Optional display icons.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub icons: Vec<Icon>,
    /// Optional usage hints.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Annotations>,
    /// Protocol extension metadata.
    #[serde(rename = "_meta", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub meta: BTreeMap<String, Value>,
}

/// Paginated resource-template-list parameters.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ListResourceTemplatesParams {
    /// Opaque continuation cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Result of `resources/templates/list`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListResourceTemplatesResult {
    /// Authorized templates in deterministic URI-template order.
    pub resource_templates: Vec<McpResourceTemplate>,
    /// Optional continuation cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Parameters for `resources/read`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReadResourceParams {
    /// Exact resource URI.
    pub uri: String,
}

/// Text or binary resource content.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ResourceContents {
    /// UTF-8 text content.
    Text {
        /// Resource URI.
        uri: String,
        /// Optional MIME type.
        #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
        /// Text payload.
        text: String,
        /// Protocol extension metadata.
        #[serde(rename = "_meta", default, skip_serializing_if = "BTreeMap::is_empty")]
        meta: BTreeMap<String, Value>,
    },
    /// Base64-encoded binary content.
    Blob {
        /// Resource URI.
        uri: String,
        /// Optional MIME type.
        #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
        mime_type: Option<String>,
        /// Base64-encoded bytes.
        blob: String,
        /// Protocol extension metadata.
        #[serde(rename = "_meta", default, skip_serializing_if = "BTreeMap::is_empty")]
        meta: BTreeMap<String, Value>,
    },
}

impl ResourceContents {
    /// Creates text resource content.
    pub fn text(uri: impl Into<String>, text: impl Into<String>) -> Self {
        Self::Text {
            uri: uri.into(),
            mime_type: None,
            text: text.into(),
            meta: BTreeMap::new(),
        }
    }

    /// Creates binary resource content from already encoded base64.
    pub fn blob(uri: impl Into<String>, blob: impl Into<String>) -> Self {
        Self::Blob {
            uri: uri.into(),
            mime_type: None,
            blob: blob.into(),
            meta: BTreeMap::new(),
        }
    }

    /// Returns the content's resource URI.
    pub fn uri(&self) -> &str {
        match self {
            Self::Text { uri, .. } | Self::Blob { uri, .. } => uri,
        }
    }
}

/// Result of `resources/read`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ReadResourceResult {
    /// One or more content parts for the requested resource.
    pub contents: Vec<ResourceContents>,
}

/// One declared prompt argument.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PromptArgument {
    /// Stable argument name.
    pub name: String,
    /// Optional human-facing title.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Optional human-facing description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Whether callers must supply this argument.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub required: bool,
}

/// Resource subscription parameters.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResourceSubscriptionParams {
    /// Exact resource URI to subscribe to or unsubscribe from.
    pub uri: String,
}

/// Reference accepted by `completion/complete`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum CompletionReference {
    /// Prompt argument reference.
    #[serde(rename = "ref/prompt")]
    Prompt {
        /// Registered prompt name.
        name: String,
    },
    /// Resource-template argument reference.
    #[serde(rename = "ref/resource")]
    Resource {
        /// Registered RFC 6570 URI template.
        uri: String,
    },
}

/// Argument prefix supplied to a completion provider.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompletionArgument {
    /// Argument or URI-template variable name.
    pub name: String,
    /// Current value prefix.
    pub value: String,
}

/// Previously resolved arguments available to a completion provider.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompletionContext {
    /// Known argument values.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub arguments: BTreeMap<String, String>,
}

/// Parameters for `completion/complete`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompleteParams {
    /// Prompt or resource-template reference.
    #[serde(rename = "ref")]
    pub reference: CompletionReference,
    /// Argument being completed.
    pub argument: CompletionArgument,
    /// Optional values of other arguments.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<CompletionContext>,
}

/// Completion suggestions and pagination hints.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Completion {
    /// Suggested values, capped at 100.
    pub values: Vec<String>,
    /// Optional total suggestion count.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    /// Whether more suggestions exist.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_more: Option<bool>,
}

/// Result of `completion/complete`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompleteResult {
    /// Completion payload.
    pub completion: Completion,
}

/// Model-facing description of a prompt.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct McpPrompt {
    /// Stable prompt name.
    pub name: String,
    /// Optional human-facing title.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Optional model-facing description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Prompt arguments.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub arguments: Vec<PromptArgument>,
    /// Optional display icons.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub icons: Vec<Icon>,
    /// Protocol extension metadata.
    #[serde(rename = "_meta", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub meta: BTreeMap<String, Value>,
}

/// Paginated prompt-list parameters.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ListPromptsParams {
    /// Opaque continuation cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Result of `prompts/list`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListPromptsResult {
    /// Authorized prompts in deterministic name order.
    pub prompts: Vec<McpPrompt>,
    /// Optional continuation cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// Parameters for `prompts/get`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GetPromptParams {
    /// Exact prompt name.
    pub name: String,
    /// String arguments supplied by the user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<BTreeMap<String, String>>,
}

/// Role of one rendered prompt message.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PromptRole {
    /// Human-authored message.
    User,
    /// Assistant-authored message.
    Assistant,
}

/// One rendered prompt message.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PromptMessage {
    /// Message role.
    pub role: PromptRole,
    /// Text, media, resource link, or embedded resource content.
    pub content: ContentBlock,
}

impl PromptMessage {
    /// Creates one user text message.
    pub fn user_text(text: impl Into<String>) -> Self {
        Self {
            role: PromptRole::User,
            content: ContentBlock::text(text),
        }
    }

    /// Creates one assistant text message.
    pub fn assistant_text(text: impl Into<String>) -> Self {
        Self {
            role: PromptRole::Assistant,
            content: ContentBlock::text(text),
        }
    }
}

/// Result of `prompts/get`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GetPromptResult {
    /// Optional rendered-prompt description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Ordered prompt messages.
    pub messages: Vec<PromptMessage>,
}
