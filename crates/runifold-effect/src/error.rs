use runifold_core::{JournalError, RunError};
use thiserror::Error;

/// Normalized effect-coordination failure category.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EffectExecutorErrorKind {
    /// The owning Run lacks the required capability.
    CapabilityDenied,
    /// An idempotency key was reused for different work.
    IdempotencyConflict,
    /// Recovery cannot prove that retry is safe.
    Ambiguous,
    /// The effect store rejected an operation.
    Store,
    /// Structured event recording failed.
    Observability,
    /// The handler failed.
    Handler,
    /// Execution was cancelled.
    Cancelled,
    /// The effective deadline elapsed.
    DeadlineExceeded,
    /// Stored state violated the protocol.
    Protocol,
    /// A remote reconciliation query failed without resolving the effect.
    Reconciliation,
}

/// Structured failure from effect coordination.
#[derive(Clone, Debug, Error, PartialEq)]
#[error("{kind:?}: {message}")]
pub struct EffectExecutorError {
    /// Normalized category.
    pub kind: EffectExecutorErrorKind,
    /// Safe failure explanation.
    pub message: String,
    /// Original handler error, when applicable.
    #[source]
    pub source_error: Option<RunError>,
}

impl EffectExecutorError {
    /// Creates an error without a handler source.
    pub fn new(kind: EffectExecutorErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source_error: None,
        }
    }

    pub(crate) fn handler(error: RunError) -> Self {
        Self {
            kind: EffectExecutorErrorKind::Handler,
            message: error.to_string(),
            source_error: Some(error),
        }
    }

    pub(crate) fn reconciliation(error: RunError) -> Self {
        Self {
            kind: EffectExecutorErrorKind::Reconciliation,
            message: error.to_string(),
            source_error: Some(error),
        }
    }

    pub(crate) fn ambiguous_handler(error: RunError) -> Self {
        Self {
            kind: EffectExecutorErrorKind::Ambiguous,
            message: "effect handler failed after the remote outcome became uncertain".into(),
            source_error: Some(error),
        }
    }
}

impl From<JournalError> for EffectExecutorError {
    fn from(error: JournalError) -> Self {
        Self::new(EffectExecutorErrorKind::Observability, error.to_string())
    }
}
