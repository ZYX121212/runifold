use runifold_core::{EffectRequest, RunError};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Persisted external-effect state.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
#[non_exhaustive]
pub enum EffectStatus {
    /// The request is durable and no handler has been started.
    Prepared,
    /// The handler may be executing or may have executed.
    Started,
    /// The effect completed with a durable output.
    Completed {
        /// Canonical effect output.
        output: Value,
    },
    /// The handler returned a durable failure.
    Failed {
        /// Structured terminal handler error.
        error: RunError,
    },
}

/// Revisioned write-ahead record for one logical effect.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EffectRecord {
    /// Monotonic compare-and-swap revision.
    pub revision: u64,
    /// Original canonical request.
    pub request: EffectRequest,
    /// Current persisted state.
    pub status: EffectStatus,
}

impl EffectRecord {
    /// Creates a prepared revision-zero record.
    pub const fn prepared(request: EffectRequest) -> Self {
        Self {
            revision: 0,
            request,
            status: EffectStatus::Prepared,
        }
    }

    /// Creates the next revision.
    pub(crate) fn next(&self, status: EffectStatus) -> Result<Self, crate::EffectExecutorError> {
        Ok(Self {
            revision: self.revision.checked_add(1).ok_or_else(|| {
                crate::EffectExecutorError::new(
                    crate::EffectExecutorErrorKind::Store,
                    "effect revision overflow",
                )
            })?,
            request: self.request.clone(),
            status,
        })
    }
}
