use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Whether retrying an operation is safe.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum RetrySafety {
    /// The same operation can be retried safely.
    Safe,
    /// Retry is safe only when the external system honors an idempotency key.
    RequiresIdempotency,
    /// Visible output has been emitted, so retry may duplicate output.
    UnsafeAfterVisibleOutput,
    /// An external side effect may already have occurred.
    UnsafeAfterSideEffect,
    /// Safety cannot be determined.
    Unknown,
}

/// A normalized run failure category.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum RunErrorKind {
    /// Input or configuration is invalid.
    InvalidInput,
    /// A required capability was not granted.
    CapabilityDenied,
    /// A resource budget was exhausted.
    BudgetExceeded,
    /// A deadline elapsed.
    DeadlineExceeded,
    /// The run was cancelled.
    Cancelled,
    /// A transport failed.
    Transport,
    /// A remote protocol was malformed or violated its contract.
    Protocol,
    /// An invoked component failed.
    Invocation,
    /// A namespaced extension error.
    Extension(String),
}

/// A structured run error suitable for policy decisions.
#[derive(Clone, Debug, Deserialize, Error, PartialEq, Serialize)]
#[error("{kind:?}: {message}")]
pub struct RunError {
    /// Normalized error category.
    pub kind: RunErrorKind,
    /// Safe human-readable explanation.
    pub message: String,
    /// Retry-safety classification.
    pub retry_safety: RetrySafety,
    /// Namespaced diagnostic metadata.
    pub metadata: BTreeMap<String, Value>,
}
