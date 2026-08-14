//! Typed constructors for stable `OpenAI` Responses hosted-tool shapes.

use std::collections::BTreeMap;

use runifold_model::ProviderToolSpec;
use serde_json::{Value, json};
use thiserror::Error;
use url::Url;

/// Invalid hosted-tool configuration.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiHostedToolError {
    /// File search requires at least one non-blank vector store.
    #[error("file search requires 1..=100 non-empty vector store ids")]
    InvalidVectorStores,
    /// Remote MCP label is blank or contains control characters.
    #[error("remote MCP server label is invalid")]
    InvalidMcpLabel,
    /// Remote MCP URL is not HTTP(S).
    #[error("remote MCP server URL must be HTTP(S)")]
    InvalidMcpUrl,
    /// `type` is owned by the tool descriptor.
    #[error("hosted-tool option `type` is reserved")]
    ReservedTypeOption,
}

/// One typed `OpenAI` Responses hosted tool.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiHostedTool {
    tool_type: String,
    options: BTreeMap<String, Value>,
}

impl OpenAiHostedTool {
    /// Creates the provider-managed web-search tool.
    #[must_use]
    pub fn web_search() -> Self {
        Self::new("web_search")
    }

    /// Creates the provider-managed image-generation tool.
    #[must_use]
    pub fn image_generation() -> Self {
        Self::new("image_generation")
    }

    /// Creates a provider-managed code-interpreter container.
    #[must_use]
    pub fn code_interpreter_auto() -> Self {
        Self::new("code_interpreter").with_unchecked_option("container", json!({"type":"auto"}))
    }

    /// Creates file search over explicit vector stores.
    ///
    /// # Errors
    ///
    /// Rejects empty, blank, or more than 100 vector-store identities.
    pub fn file_search<I, S>(vector_store_ids: I) -> Result<Self, OpenAiHostedToolError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let vector_store_ids = vector_store_ids
            .into_iter()
            .map(Into::into)
            .collect::<Vec<String>>();
        if vector_store_ids.is_empty()
            || vector_store_ids.len() > 100
            || vector_store_ids.iter().any(|id| id.trim().is_empty())
        {
            return Err(OpenAiHostedToolError::InvalidVectorStores);
        }
        Ok(Self::new("file_search")
            .with_unchecked_option("vector_store_ids", json!(vector_store_ids)))
    }

    /// Creates a remote MCP tool without weakening its approval default.
    ///
    /// # Errors
    ///
    /// Rejects unsafe labels and non-HTTP(S) server URLs.
    pub fn remote_mcp(
        server_label: impl Into<String>,
        server_url: &str,
    ) -> Result<Self, OpenAiHostedToolError> {
        let server_label = server_label.into();
        if server_label.trim().is_empty() || server_label.chars().any(char::is_control) {
            return Err(OpenAiHostedToolError::InvalidMcpLabel);
        }
        let url = Url::parse(server_url).map_err(|_| OpenAiHostedToolError::InvalidMcpUrl)?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(OpenAiHostedToolError::InvalidMcpUrl);
        }
        Ok(Self::new("mcp")
            .with_unchecked_option("server_label", Value::String(server_label))
            .with_unchecked_option("server_url", Value::String(url.to_string())))
    }

    /// Adds a provider option while preserving adapter-owned `type`.
    ///
    /// # Errors
    ///
    /// Rejects the reserved `type` key.
    pub fn with_option(
        mut self,
        key: impl Into<String>,
        value: Value,
    ) -> Result<Self, OpenAiHostedToolError> {
        let key = key.into();
        if key == "type" {
            return Err(OpenAiHostedToolError::ReservedTypeOption);
        }
        self.options.insert(key, value);
        Ok(self)
    }

    fn new(tool_type: &str) -> Self {
        Self {
            tool_type: tool_type.into(),
            options: BTreeMap::new(),
        }
    }

    fn with_unchecked_option(mut self, key: &str, value: Value) -> Self {
        self.options.insert(key.into(), value);
        self
    }
}

impl From<OpenAiHostedTool> for ProviderToolSpec {
    fn from(tool: OpenAiHostedTool) -> Self {
        Self {
            provider: "openai".into(),
            tool_type: tool.tool_type,
            options: tool.options,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_tools_preserve_wire_shape_and_security_defaults() {
        let file: ProviderToolSpec = OpenAiHostedTool::file_search(vec!["vs_test"])
            .unwrap()
            .into();
        assert_eq!(file.tool_type, "file_search");
        assert_eq!(file.options["vector_store_ids"], json!(["vs_test"]));

        let mcp: ProviderToolSpec =
            OpenAiHostedTool::remote_mcp("docs", "https://mcp.example.test/server")
                .unwrap()
                .into();
        assert!(!mcp.options.contains_key("require_approval"));
        assert!(
            OpenAiHostedTool::web_search()
                .with_option("type", json!("override"))
                .is_err()
        );
    }
}
