use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Latest finalized MCP revision supported by this crate.
pub const LATEST_PROTOCOL_VERSION: &str = "2025-11-25";
/// Stateless MCP revision used for discovery probes.
///
/// Runifold does not advertise this revision as fully supported yet. The
/// constant exists so modern clients can use `server/discover` to detect the
/// legacy revision without guessing or depending on a transport session.
pub const STATELESS_PROTOCOL_VERSION: &str = "2026-07-28";
/// Protocol revisions currently supported for ordinary MCP requests.
pub const SUPPORTED_PROTOCOL_VERSIONS: &[&str] =
    &[STATELESS_PROTOCOL_VERSION, LATEST_PROTOCOL_VERSION];
pub(crate) const JSON_RPC_VERSION: &str = "2.0";

/// JSON-RPC request identity.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(untagged)]
pub enum RequestId {
    /// Null identity used only when a malformed request identity is unavailable.
    Null,
    /// Numeric identity.
    Number(i64),
    /// String identity.
    String(String),
}

/// JSON-RPC request.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JsonRpcRequest {
    /// Always `2.0`.
    pub jsonrpc: String,
    /// Request identity.
    pub id: RequestId,
    /// MCP method.
    pub method: String,
    /// Optional method parameters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl JsonRpcRequest {
    pub(crate) fn new(id: RequestId, method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: JSON_RPC_VERSION.into(),
            id,
            method: method.into(),
            params,
        }
    }
}

/// JSON-RPC notification.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JsonRpcNotification {
    /// Always `2.0`.
    pub jsonrpc: String,
    /// MCP notification method.
    pub method: String,
    /// Optional notification parameters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

impl JsonRpcNotification {
    /// Creates a valid JSON-RPC notification.
    pub fn new(method: impl Into<String>, params: Option<Value>) -> Self {
        Self {
            jsonrpc: JSON_RPC_VERSION.into(),
            method: method.into(),
            params,
        }
    }
}

/// JSON-RPC error body.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JsonRpcError {
    /// Stable JSON-RPC or application error code.
    pub code: i64,
    /// Safe error explanation.
    pub message: String,
    /// Optional structured details.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// JSON-RPC response.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum JsonRpcResponse {
    /// Successful response.
    Success {
        /// Always `2.0`.
        jsonrpc: String,
        /// Matching request identity.
        id: RequestId,
        /// Method result.
        result: Value,
    },
    /// Error response.
    Error {
        /// Always `2.0`.
        jsonrpc: String,
        /// Matching request identity.
        id: RequestId,
        /// Error body.
        error: JsonRpcError,
    },
}

impl JsonRpcResponse {
    pub(crate) fn success(id: RequestId, result: Value) -> Self {
        Self::Success {
            jsonrpc: JSON_RPC_VERSION.into(),
            id,
            result,
        }
    }

    pub(crate) fn error(
        id: RequestId,
        code: i64,
        message: impl Into<String>,
        data: Option<Value>,
    ) -> Self {
        Self::Error {
            jsonrpc: JSON_RPC_VERSION.into(),
            id,
            error: JsonRpcError {
                code,
                message: message.into(),
                data,
            },
        }
    }

    pub(crate) const fn id(&self) -> &RequestId {
        match self {
            Self::Success { id, .. } | Self::Error { id, .. } => id,
        }
    }

    pub(crate) fn into_result(self) -> Result<Value, crate::McpError> {
        match self {
            Self::Success { result, .. } => Ok(result),
            Self::Error { error, .. } => Err(crate::McpError::Remote {
                code: error.code,
                message: error.message,
                data: error.data,
            }),
        }
    }
}

/// MCP implementation identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Implementation {
    /// Machine-readable implementation name.
    pub name: String,
    /// Implementation version.
    pub version: String,
    /// Optional display title.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Optional human-facing description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl Implementation {
    /// Creates a minimal implementation descriptor.
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            title: None,
            description: None,
        }
    }
}

/// Client-side Sampling capability.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct SamplingCapability {
    /// Whether deprecated context inclusion is supported.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<BTreeMap<String, Value>>,
    /// Whether Tool-enabled Sampling is supported.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<BTreeMap<String, Value>>,
}

/// Client capabilities supported by this MCP edge.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ClientCapabilities {
    /// Client-side model Sampling support.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampling: Option<SamplingCapability>,
    /// Client-side Roots support, including MRTR `roots/list`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub roots: Option<BTreeMap<String, Value>>,
    /// Client-side Elicitation support advertised for MRTR requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elicitation: Option<BTreeMap<String, Value>>,
    /// Negotiated protocol extensions.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, Value>,
    /// Namespaced experimental capabilities.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub experimental: BTreeMap<String, Value>,
}

/// Tool-list capability.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolsCapability {
    /// Whether the server can notify clients that its tool list changed.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub list_changed: bool,
}

/// Resource discovery capability.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourcesCapability {
    /// Whether individual resource update subscriptions are supported.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub subscribe: bool,
    /// Whether resource-list change notifications are supported.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub list_changed: bool,
}

/// Prompt discovery capability.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptsCapability {
    /// Whether prompt-list change notifications are supported.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub list_changed: bool,
}

/// Server capabilities.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ServerCapabilities {
    /// Tool support.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<ToolsCapability>,
    /// Resource discovery and reading support.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourcesCapability>,
    /// Prompt discovery and rendering support.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompts: Option<PromptsCapability>,
    /// Prompt and resource-template argument completion support.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completions: Option<BTreeMap<String, Value>>,
    /// Negotiated protocol extensions.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extensions: BTreeMap<String, Value>,
    /// Namespaced experimental capabilities.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub experimental: BTreeMap<String, Value>,
}

/// Per-request metadata used by the stateless MCP revision.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct StatelessRequestMetadata {
    /// Protocol revision selected for this request.
    #[serde(rename = "io.modelcontextprotocol/protocolVersion")]
    pub protocol_version: String,
    /// Self-reported client identity. It is never an authorization input.
    #[serde(
        rename = "io.modelcontextprotocol/clientInfo",
        skip_serializing_if = "Option::is_none"
    )]
    pub client_info: Option<Implementation>,
    /// Client capabilities relevant to this request.
    #[serde(rename = "io.modelcontextprotocol/clientCapabilities")]
    pub client_capabilities: ClientCapabilities,
}

/// `server/discover` request parameters.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DiscoverParams {
    /// Stateless request metadata.
    #[serde(rename = "_meta")]
    pub metadata: StatelessRequestMetadata,
}

/// Metadata returned by `server/discover`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DiscoverMetadata {
    /// Self-reported server identity. It is never an authorization input.
    #[serde(rename = "io.modelcontextprotocol/serverInfo")]
    pub server_info: Implementation,
}

/// Backward-compatible name for the unified MCP cache scope.
pub type DiscoveryCacheScope = crate::CacheScope;

/// Result discriminator used by the stateless MCP protocol.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum McpResultType {
    /// The request completed and contains its final result.
    Complete,
    /// The request requires another client round trip.
    InputRequired,
    /// The request was durably materialized as an asynchronous Task.
    Task,
}

/// `server/discover` result.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoverResult {
    /// Polymorphic MCP result discriminator.
    pub result_type: McpResultType,
    /// Protocol revisions supported for ordinary requests.
    pub supported_versions: Vec<String>,
    /// Server capabilities available under the supported revisions.
    pub capabilities: ServerCapabilities,
    /// Per-response server identity metadata.
    #[serde(rename = "_meta")]
    pub metadata: DiscoverMetadata,
    /// Optional server usage instructions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    /// Optional discovery cache lifetime in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    /// Optional discovery cache visibility.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_scope: Option<DiscoveryCacheScope>,
}

/// `initialize` request parameters.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeParams {
    /// Latest protocol revision supported by the client.
    pub protocol_version: String,
    /// Client capabilities.
    pub capabilities: ClientCapabilities,
    /// Client identity.
    pub client_info: Implementation,
}

/// `initialize` result.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResult {
    /// Negotiated protocol revision.
    pub protocol_version: String,
    /// Server capabilities.
    pub capabilities: ServerCapabilities,
    /// Server identity.
    pub server_info: Implementation,
    /// Optional server usage instructions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

/// MCP Tool descriptor.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpTool {
    /// Stable model-facing name.
    pub name: String,
    /// Optional display title.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Model-facing description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema for arguments.
    pub input_schema: Value,
    /// Optional JSON Schema for structured output.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
    /// Untrusted MCP behavior annotations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<Value>,
}

/// `tools/list` parameters.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ListToolsParams {
    /// Opaque pagination cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// `tools/list` result.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListToolsResult {
    /// Deterministically ordered tools.
    pub tools: Vec<McpTool>,
    /// Opaque cursor for the next page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Server-provided freshness lifetime in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    /// Visibility of this response across authorization contexts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_scope: Option<crate::CacheScope>,
}

/// `tools/call` parameters.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CallToolParams {
    /// Tool name.
    pub name: String,
    /// Structured Tool arguments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<serde_json::Map<String, Value>>,
}

/// Forward-compatible MCP content block.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ContentBlock {
    /// MCP content type.
    #[serde(rename = "type")]
    pub kind: String,
    /// Type-specific fields.
    #[serde(flatten)]
    pub fields: BTreeMap<String, Value>,
}

impl ContentBlock {
    /// Creates a text content block.
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            kind: "text".into(),
            fields: BTreeMap::from([("text".into(), Value::String(text.into()))]),
        }
    }

    /// Returns text when this is a valid text block.
    pub fn as_text(&self) -> Option<&str> {
        (self.kind == "text")
            .then(|| self.fields.get("text").and_then(Value::as_str))
            .flatten()
    }
}

/// `tools/call` result.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CallToolResult {
    /// Model-visible content.
    pub content: Vec<ContentBlock>,
    /// Structured object output.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub structured_content: Option<Value>,
    /// Whether the Tool reported an application-level error.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_error: bool,
}
