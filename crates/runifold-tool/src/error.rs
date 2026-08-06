use std::collections::BTreeMap;

use runifold_core::RetrySafety;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Normalized tool failure category.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum ToolErrorKind {
    /// The requested tool is not registered.
    NotFound,
    /// Input failed tool-level validation.
    InvalidInput,
    /// The owning run was not granted the tool capability.
    CapabilityDenied,
    /// Execution was cancelled.
    Cancelled,
    /// The invocation deadline elapsed.
    DeadlineExceeded,
    /// Tool implementation failed.
    Execution,
    /// Tool output violated its declared contract.
    InvalidOutput,
}

/// Structured tool execution error.
#[derive(Clone, Debug, Deserialize, Error, PartialEq, Serialize)]
#[error("{kind:?}: {message}")]
pub struct ToolError {
    /// Normalized category.
    pub kind: ToolErrorKind,
    /// Safe human-readable explanation.
    pub message: String,
    /// Retry-safety classification.
    pub retry_safety: RetrySafety,
    /// Namespaced diagnostic metadata.
    pub metadata: BTreeMap<String, Value>,
}

impl ToolError {
    /// Creates a local tool error with unknown retry safety.
    pub fn local(kind: ToolErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            retry_safety: RetrySafety::Unknown,
            metadata: BTreeMap::new(),
        }
    }
}

/// Explicit conversion from an application error into a safe Tool failure.
///
/// Implementations decide which message is safe for model and operator
/// visibility, as well as the normalized kind and retry-safety classification.
pub trait IntoToolError {
    /// Converts this application failure into the canonical Tool error.
    fn into_tool_error(self) -> ToolError;
}

impl IntoToolError for ToolError {
    fn into_tool_error(self) -> ToolError {
        self
    }
}

/// Failure to add a tool to a registry.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ToolRegistrationError {
    /// Tool names must not be blank.
    #[error("tool name cannot be empty")]
    EmptyName,
    /// A different tool already owns this model-facing name.
    #[error("tool `{0}` is already registered")]
    DuplicateName(String),
    /// A declared input or output schema could not be compiled.
    #[error("tool `{tool}` has an invalid {direction} schema: {message}")]
    InvalidSchema {
        /// Tool name.
        tool: String,
        /// Schema direction (`input` or `output`).
        direction: &'static str,
        /// Safe compiler diagnostic.
        message: String,
    },
}
