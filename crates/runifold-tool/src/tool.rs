use std::{collections::BTreeMap, future::Future, pin::Pin};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use runifold_model::ContentPart;

use crate::{ToolContext, ToolDescriptor, ToolError};

/// A boxed, sendable future returned by a tool.
#[cfg(not(target_arch = "wasm32"))]
pub type ToolFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A boxed Tool future on single-threaded WASM.
#[cfg(target_arch = "wasm32")]
pub type ToolFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Successful canonical tool output.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ToolOutput {
    /// Ordered model-visible presentation content.
    pub content: Vec<ContentPart>,
    /// Optional structured output value used for contract validation and by
    /// protocols that support a separate structured result channel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub structured_content: Option<Value>,
    /// Namespaced host metadata retained across Agent and protocol bridges.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, Value>,
    /// Whether execution completed with a model-visible application error.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub is_error: bool,
    /// Whether the value is safe to expose verbatim to a model.
    pub model_visible: bool,
}

impl ToolOutput {
    /// Creates model-visible output from a JSON value.
    ///
    /// JSON values are retained as structured content and mirrored as text
    /// for providers that only accept textual function results.
    pub fn model_visible(value: Value) -> Self {
        let structured_content = Some(value.clone());
        let text = match value {
            Value::String(text) => text,
            value => value.to_string(),
        };
        Self {
            content: vec![ContentPart::text(text)],
            structured_content,
            metadata: BTreeMap::new(),
            is_error: false,
            model_visible: true,
        }
    }

    /// Creates a model-visible rich result.
    pub fn rich(content: Vec<ContentPart>) -> Self {
        Self {
            content,
            structured_content: None,
            metadata: BTreeMap::new(),
            is_error: false,
            model_visible: true,
        }
    }

    /// Attaches a structured value alongside the presentation content.
    #[must_use]
    pub fn with_structured_content(mut self, value: Value) -> Self {
        self.structured_content = Some(value);
        self
    }

    /// Adds namespaced host metadata.
    #[must_use]
    pub fn with_metadata(mut self, key: impl Into<String>, value: Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Creates host-only output that must not be exposed to a model.
    pub fn host_only(content: Vec<ContentPart>) -> Self {
        Self {
            content,
            structured_content: None,
            metadata: BTreeMap::new(),
            is_error: false,
            model_visible: false,
        }
    }

    /// Creates a rich, model-visible application error result.
    pub fn model_error(content: Vec<ContentPart>) -> Self {
        Self {
            content,
            structured_content: None,
            metadata: BTreeMap::new(),
            is_error: true,
            model_visible: true,
        }
    }
}

/// Object-safe execution boundary implemented by tools.
pub trait Tool: Send + Sync {
    /// Returns the tool's immutable semantic contract.
    fn descriptor(&self) -> &ToolDescriptor;

    /// Executes one invocation.
    fn invoke(
        &self,
        input: Value,
        context: ToolContext,
    ) -> ToolFuture<'_, Result<ToolOutput, ToolError>>;
}
