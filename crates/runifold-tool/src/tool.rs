use std::{future::Future, pin::Pin};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{ToolContext, ToolDescriptor, ToolError};

/// A boxed, sendable future returned by a tool.
pub type ToolFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Successful canonical tool output.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ToolOutput {
    /// Structured output value.
    pub value: Value,
    /// Whether the value is safe to expose verbatim to a model.
    pub model_visible: bool,
}

impl ToolOutput {
    /// Creates model-visible output.
    pub const fn model_visible(value: Value) -> Self {
        Self {
            value,
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
