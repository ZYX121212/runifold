use std::collections::BTreeMap;

use runifold_core::{CapabilityId, EffectClass, RetrySafety, RiskLevel};
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
            if result.is_error {
                let mut error = ToolError::local(
                    ToolErrorKind::Execution,
                    "remote MCP Tool reported an application error",
                );
                error.metadata.insert(
                    "runifold.mcp.error_content".into(),
                    serde_json::to_value(result.content).unwrap_or(Value::Null),
                );
                return Err(error);
            }
            let value = match result.structured_content {
                Some(value) => value,
                None if result.content.len() == 1 => result.content[0].as_text().map_or_else(
                    || serde_json::to_value(&result.content[0]).unwrap_or(Value::Null),
                    |text| Value::String(text.into()),
                ),
                None => serde_json::to_value(result.content).unwrap_or(Value::Null),
            };
            Ok(ToolOutput::model_visible(value))
        })
    }
}

fn map_mcp_error(error: &McpError) -> ToolError {
    let kind = match error.kind() {
        McpErrorKind::Cancelled => ToolErrorKind::Cancelled,
        McpErrorKind::DeadlineExceeded => ToolErrorKind::DeadlineExceeded,
        McpErrorKind::Authentication => ToolErrorKind::CapabilityDenied,
        McpErrorKind::Transport
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
