use std::collections::BTreeMap;

use runifold_core::RetrySafety;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// A normalized model-layer error category.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum ModelErrorKind {
    /// Request data failed local validation.
    InvalidRequest,
    /// The selected model or provider does not support a required feature.
    UnsupportedFeature,
    /// A provider transport failed.
    Transport,
    /// A provider response violated its protocol.
    Protocol,
    /// Stream events violated the canonical lifecycle.
    StreamState,
    /// Accumulated tool arguments were not valid JSON.
    MalformedToolArguments,
    /// A provider rejected or failed a request.
    Provider,
    /// The model call was cancelled.
    Cancelled,
    /// The model call exceeded its deadline.
    DeadlineExceeded,
}

/// A structured model-layer error.
#[derive(Clone, Debug, Deserialize, Error, PartialEq, Serialize)]
#[error("{kind:?}: {message}")]
pub struct ModelError {
    /// Normalized category.
    pub kind: ModelErrorKind,
    /// Safe human-readable explanation.
    pub message: String,
    /// Provider namespace, when known.
    pub provider: Option<String>,
    /// Retry-safety classification.
    pub retry_safety: RetrySafety,
    /// Namespaced diagnostic metadata.
    pub metadata: BTreeMap<String, Value>,
}

impl ModelError {
    /// Creates a non-retryable local validation or state error.
    pub fn local(kind: ModelErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            provider: None,
            retry_safety: RetrySafety::Unknown,
            metadata: BTreeMap::new(),
        }
    }
}
