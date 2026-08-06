use std::collections::BTreeMap;

use runifold_core::{CapabilityId, EffectClass, RetrySafety, RiskLevel};
use runifold_model::{ContentPart, MediaSource, ProviderData};
use runifold_tool::{
    Tool, ToolContext, ToolDescriptor, ToolError, ToolErrorKind, ToolFuture, ToolOutput,
};
use serde_json::{Value, json};

use crate::{CallToolParams, McpClient, McpError, McpErrorKind, McpTool};

/// Host-selected authority policy for an imported MCP Tool.
///
/// MCP annotations are untrusted hints and never set these values
/// automatically.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoteToolPolicy {
    /// External effect classification.
    pub effect: EffectClass,
    /// Approval and policy risk classification.
    pub risk: RiskLevel,
}

impl RemoteToolPolicy {
    /// Creates an explicit remote Tool policy.
    pub const fn new(effect: EffectClass, risk: RiskLevel) -> Self {
        Self { effect, risk }
    }
}

/// A remote MCP Tool adapted to Runifold's canonical [`Tool`] boundary.
#[derive(Clone, Debug)]
pub struct McpRemoteTool {
    client: McpClient,
    descriptor: ToolDescriptor,
}

impl McpRemoteTool {
    /// Adapts a discovered MCP Tool with explicit host authority semantics.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when the remote descriptor has no usable name.
    pub fn new(
        client: McpClient,
        remote: McpTool,
        policy: RemoteToolPolicy,
    ) -> Result<Self, McpError> {
        if remote.name.trim().is_empty() {
            return Err(McpError::protocol("remote MCP tool name is empty"));
        }
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "runifold.mcp.remote".into(),
            json!({
                "annotations": remote.annotations,
                "title": remote.title,
            }),
        );
        Ok(Self {
            client,
            descriptor: ToolDescriptor {
                id: CapabilityId::new(),
                name: remote.name,
                version: "mcp-2025-11-25".into(),
                description: remote
                    .description
                    .unwrap_or_else(|| "Remote MCP Tool".into()),
                input_schema: remote.input_schema,
                output_schema: remote.output_schema.unwrap_or_else(
                    || json!({"type": ["object", "array", "string", "number", "boolean", "null"]}),
                ),
                effect: policy.effect,
                risk: policy.risk,
                metadata,
            },
        })
    }
}

impl Tool for McpRemoteTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        input: Value,
        context: ToolContext,
    ) -> ToolFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async move {
            let arguments = input.as_object().cloned().ok_or_else(|| {
                ToolError::local(
                    ToolErrorKind::InvalidInput,
                    "MCP Tool arguments must be a JSON object",
                )
            })?;
            let result = self
                .client
                .call_tool_scoped(
                    CallToolParams {
                        name: self.descriptor.name.clone(),
                        arguments: Some(arguments),
                    },
                    &context,
                )
                .await
                .map_err(|error| map_mcp_error(&error))?;
            let content_metadata = result
                .content
                .iter()
                .enumerate()
                .filter_map(|(index, block)| {
                    let annotations = block.fields.get("annotations");
                    let meta = block.fields.get("_meta");
                    (annotations.is_some() || meta.is_some())
                        .then(|| json!({"index":index,"annotations":annotations,"_meta":meta}))
                })
                .collect::<Vec<_>>();
            let decoded_parts = result
                .content
                .iter()
                .map(decode_content_block)
                .collect::<Result<Vec<_>, _>>()?;
            let mut output = if result.is_error {
                ToolOutput::model_error(decoded_parts)
            } else {
                ToolOutput::rich(decoded_parts)
            };
            if !content_metadata.is_empty() {
                output = output.with_metadata(
                    "runifold.mcp.content_metadata",
                    Value::Array(content_metadata),
                );
            }
            let mut metadata = result.metadata;
            let extended_structured_content = metadata.remove("runifold.structured_content");
            for (key, value) in metadata {
                output = output.with_metadata(key, value);
            }
            if let Some(structured_content) =
                result.structured_content.or(extended_structured_content)
            {
                output = output.with_structured_content(structured_content);
            }
            Ok(output)
        })
    }
}

fn decode_content_block(block: &crate::ContentBlock) -> Result<ContentPart, ToolError> {
    match block.kind.as_str() {
        "text" => field_string(block, "text").map(ContentPart::text),
        "image" => decode_inline_media(block).map(|source| ContentPart::Image { source }),
        "audio" => decode_inline_media(block).map(|source| ContentPart::Audio { source }),
        "resource_link" => Ok(ContentPart::ResourceLink {
            uri: field_string(block, "uri")?,
            name: field_string(block, "name")?,
            title: optional_string(block, "title")?,
            description: optional_string(block, "description")?,
            media_type: optional_string(block, "mimeType")?,
            size: optional_u64(block, "size")?,
        }),
        "resource" => decode_embedded_resource(block),
        kind => Ok(ContentPart::ProviderOpaque(ProviderData {
            provider: "mcp".into(),
            kind: kind.into(),
            value: serde_json::to_value(block).map_err(|error| {
                ToolError::local(
                    ToolErrorKind::InvalidOutput,
                    format!("MCP content block cannot be retained: {error}"),
                )
            })?,
        })),
    }
}

fn decode_inline_media(block: &crate::ContentBlock) -> Result<MediaSource, ToolError> {
    Ok(MediaSource::Base64 {
        media_type: field_string(block, "mimeType")?,
        data: field_string(block, "data")?,
    })
}

fn decode_embedded_resource(block: &crate::ContentBlock) -> Result<ContentPart, ToolError> {
    let resource = block
        .fields
        .get("resource")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid_mcp_output("resource block is missing object `resource`"))?;
    let uri = object_string(resource, "uri")?;
    if let Some(text) = resource.get("text").and_then(Value::as_str) {
        return Ok(ContentPart::text(text));
    }
    let data = resource
        .get("blob")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_mcp_output("embedded resource is missing `text` or `blob`"))?;
    let media_type = resource
        .get("mimeType")
        .and_then(Value::as_str)
        .unwrap_or("application/octet-stream");
    Ok(ContentPart::Document {
        source: MediaSource::Base64 {
            media_type: media_type.into(),
            data: data.into(),
        },
        name: Some(uri),
    })
}

fn field_string(block: &crate::ContentBlock, field: &str) -> Result<String, ToolError> {
    block
        .fields
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            invalid_mcp_output(format!("{} block is missing string `{field}`", block.kind))
        })
}

fn optional_string(block: &crate::ContentBlock, field: &str) -> Result<Option<String>, ToolError> {
    match block.fields.get(field) {
        None => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(invalid_mcp_output(format!(
            "{} block field `{field}` must be a string",
            block.kind
        ))),
    }
}

fn optional_u64(block: &crate::ContentBlock, field: &str) -> Result<Option<u64>, ToolError> {
    match block.fields.get(field) {
        None => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or_else(|| {
            invalid_mcp_output(format!(
                "{} block field `{field}` must be an unsigned integer",
                block.kind
            ))
        }),
    }
}

fn object_string(
    object: &serde_json::Map<String, Value>,
    field: &str,
) -> Result<String, ToolError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| invalid_mcp_output(format!("resource is missing string `{field}`")))
}

fn invalid_mcp_output(message: impl Into<String>) -> ToolError {
    ToolError::local(ToolErrorKind::InvalidOutput, message)
}

fn map_mcp_error(error: &McpError) -> ToolError {
    let kind = match error.kind() {
        McpErrorKind::Cancelled => ToolErrorKind::Cancelled,
        McpErrorKind::DeadlineExceeded => ToolErrorKind::DeadlineExceeded,
        McpErrorKind::Authentication => ToolErrorKind::CapabilityDenied,
        McpErrorKind::TaskExpired
        | McpErrorKind::Transport
        | McpErrorKind::Protocol
        | McpErrorKind::Remote
        | McpErrorKind::Lifecycle
        | McpErrorKind::UnsupportedVersion
        | McpErrorKind::SessionExpired
        | McpErrorKind::Observability => ToolErrorKind::Execution,
    };
    let mut mapped = ToolError::local(kind, "remote MCP Tool invocation failed");
    mapped.retry_safety = RetrySafety::Unknown;
    mapped.metadata.insert(
        "runifold.mcp.error".into(),
        Value::String(error.to_string()),
    );
    mapped
}
