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

impl RunErrorKind {
    /// Returns a stable machine-readable diagnostic code.
    pub fn code(&self) -> &str {
        match self {
            Self::InvalidInput => "runifold.invalid_input",
            Self::CapabilityDenied => "runifold.capability_denied",
            Self::BudgetExceeded => "runifold.budget_exceeded",
            Self::DeadlineExceeded => "runifold.deadline_exceeded",
            Self::Cancelled => "runifold.cancelled",
            Self::Transport => "runifold.transport",
            Self::Protocol => "runifold.protocol",
            Self::Invocation => "runifold.invocation",
            Self::Extension(namespace) => namespace,
        }
    }

    /// Returns a safe default remediation hint for operators.
    pub fn recommendation(&self) -> &'static str {
        match self {
            Self::InvalidInput => "Validate configuration and request data before retrying.",
            Self::CapabilityDenied => {
                "Grant only the required capability or remove the unauthorized operation."
            }
            Self::BudgetExceeded => "Increase the explicit budget or reduce bounded work.",
            Self::DeadlineExceeded => {
                "Review the deadline and upstream latency before deciding whether to retry."
            }
            Self::Cancelled => "Do not retry unless the caller starts a new operation.",
            Self::Transport => "Inspect retry safety, endpoint health, and network diagnostics.",
            Self::Protocol => {
                "Inspect the Provider response and adapter compatibility before retrying."
            }
            Self::Invocation => "Inspect the invoked component's typed cause and metadata.",
            Self::Extension(_) => "Inspect the namespaced extension metadata and documentation.",
        }
    }
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

impl RunError {
    /// Returns the stable machine-readable diagnostic code.
    pub fn code(&self) -> &str {
        self.kind.code()
    }

    /// Returns a safe default remediation hint.
    pub fn recommendation(&self) -> &'static str {
        self.kind.recommendation()
    }
}

#[cfg(test)]
mod tests {
    use super::RunErrorKind;

    #[test]
    fn diagnostic_codes_are_stable_and_extension_aware() {
        assert_eq!(RunErrorKind::Protocol.code(), "runifold.protocol");
        assert_eq!(
            RunErrorKind::Extension("acme.custom".into()).code(),
            "acme.custom"
        );
        assert!(!RunErrorKind::Transport.recommendation().is_empty());
    }
}
