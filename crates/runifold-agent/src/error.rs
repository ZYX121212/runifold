use std::collections::BTreeMap;

use runifold_core::{
    BudgetExceeded, CheckpointError, JournalError, RetrySafety, RunError, RunErrorKind,
};
use runifold_effect::{EffectExecutorError, EffectExecutorErrorKind};
use runifold_model::{ModelError, StructuredOutputErrorKind};
use runifold_retrieval::RetrievalError;
use runifold_tool::{ToolError, ToolErrorKind};
use thiserror::Error;

use crate::{GatewayError, GatewayErrorKind};

/// Failure of an agent run.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AgentError {
    /// Model invocation failed.
    #[error("model invocation failed: {0}")]
    Model(#[from] ModelError),
    /// Tool execution failed.
    #[error("tool execution failed: {0}")]
    Tool(#[from] ToolError),
    /// Static or dynamic context retrieval failed.
    #[error("agent retrieval failed: {0}")]
    Retrieval(#[from] RetrievalError),
    /// A shared run-tree budget was exceeded.
    #[error("agent budget exceeded: {0}")]
    Budget(#[from] BudgetExceeded),
    /// Agent delegation failed.
    #[error("agent delegation failed: {0}")]
    Gateway(#[from] GatewayError),
    /// Structured event recording failed.
    #[error("agent observability failed: {0}")]
    Journal(#[from] JournalError),
    /// Checkpoint persistence or validation failed.
    #[error("agent checkpoint failed: {0}")]
    Checkpoint(#[from] CheckpointError),
    /// Write-ahead effect coordination failed.
    #[error("agent effect failed: {0}")]
    Effect(#[from] EffectExecutorError),
    /// Recovery would silently retry a possibly partial external turn.
    #[error("checkpoint contains an ambiguous in-flight turn {turn}")]
    AmbiguousCheckpoint {
        /// One-based interrupted turn number.
        turn: u32,
    },
    /// Agent configuration is invalid.
    #[error("invalid agent configuration: {0}")]
    InvalidConfig(String),
    /// Model output violated the agent-loop protocol.
    #[error("agent protocol error: {0}")]
    Protocol(String),
    /// The configured local turn bound was reached.
    #[error("agent exceeded its local maximum of {max_turns} turns")]
    MaxTurns {
        /// Configured local turn bound.
        max_turns: u32,
    },
    /// The model terminated before the successful local Tool-call minimum.
    #[error(
        "agent completed only {successful} successful local Tool calls; at least {required} required"
    )]
    ToolRequirementUnsatisfied {
        /// Required successful local Tool calls.
        required: u32,
        /// Successful local Tool calls observed in this execution.
        successful: u32,
    },
    /// The remaining shared Tool-call budget cannot satisfy the local minimum.
    #[error(
        "agent requires {required} successful local Tool calls but only {remaining} Tool calls remain in the shared budget"
    )]
    ToolRequirementExceedsBudget {
        /// Additional successful local Tool calls still required.
        required: u32,
        /// Remaining shared Tool-call budget.
        remaining: u64,
    },
    /// The provider exhausted bounded repairs without producing usable content.
    #[error("model produced no usable terminal content after {attempts} repair attempts")]
    EmptyTerminalResponse {
        /// Repair turns completed before failing.
        attempts: u32,
    },
    /// The provider exhausted bounded repairs without satisfying the Rust type.
    #[error(
        "structured terminal output remained unsatisfied after {attempts} repair attempts: {kind:?}"
    )]
    StructuredOutputUnsatisfied {
        /// Repair turns completed before failing.
        attempts: u32,
        /// Stable local structured-output failure category.
        kind: StructuredOutputErrorKind,
        /// One-based JSON line, when available.
        line: Option<usize>,
        /// One-based JSON column, when available.
        column: Option<usize>,
    },
    /// A tool produced output that policy forbids exposing to the model.
    #[error("tool `{tool}` returned host-only output")]
    ToolOutputNotVisible {
        /// Tool name.
        tool: String,
    },
}

impl AgentError {
    /// Returns the stable run-level failure category for business policy.
    pub fn run_error_kind(&self) -> RunErrorKind {
        match self {
            Self::Model(error) => match error.kind {
                runifold_model::ModelErrorKind::InvalidRequest
                | runifold_model::ModelErrorKind::UnsupportedFeature => RunErrorKind::InvalidInput,
                runifold_model::ModelErrorKind::Transport => RunErrorKind::Transport,
                runifold_model::ModelErrorKind::Cancelled => RunErrorKind::Cancelled,
                runifold_model::ModelErrorKind::DeadlineExceeded => RunErrorKind::DeadlineExceeded,
                runifold_model::ModelErrorKind::Protocol
                | runifold_model::ModelErrorKind::StreamState
                | runifold_model::ModelErrorKind::MalformedToolArguments => RunErrorKind::Protocol,
                _ => RunErrorKind::Invocation,
            },
            Self::Tool(error) => match error.kind {
                ToolErrorKind::InvalidInput => RunErrorKind::InvalidInput,
                ToolErrorKind::CapabilityDenied => RunErrorKind::CapabilityDenied,
                ToolErrorKind::Cancelled => RunErrorKind::Cancelled,
                ToolErrorKind::DeadlineExceeded => RunErrorKind::DeadlineExceeded,
                _ => RunErrorKind::Invocation,
            },
            Self::Retrieval(error) => match error {
                RetrievalError::EmptyDocumentId
                | RetrievalError::EmptyDocumentText { .. }
                | RetrievalError::EmptyQuery
                | RetrievalError::ZeroLimit
                | RetrievalError::EmptyEmbedding
                | RetrievalError::NonFiniteEmbedding { .. }
                | RetrievalError::EmbeddingCoordinateOutOfRange { .. }
                | RetrievalError::ZeroNormEmbedding
                | RetrievalError::DimensionMismatch { .. }
                | RetrievalError::EmbeddingCountMismatch { .. }
                | RetrievalError::EmptyEmbeddingInput { .. }
                | RetrievalError::DuplicateDocument(_) => RunErrorKind::InvalidInput,
                RetrievalError::UsageOverflow => RunErrorKind::BudgetExceeded,
                RetrievalError::CapabilityDenied { .. } => RunErrorKind::CapabilityDenied,
                RetrievalError::Cancelled => RunErrorKind::Cancelled,
                RetrievalError::DeadlineExceeded => RunErrorKind::DeadlineExceeded,
                _ => RunErrorKind::Invocation,
            },
            Self::Budget(_) | Self::MaxTurns { .. } | Self::ToolRequirementExceedsBudget { .. } => {
                RunErrorKind::BudgetExceeded
            }
            Self::Gateway(error) => match error.kind {
                GatewayErrorKind::CapabilityDenied
                | GatewayErrorKind::AuthorityEscalation
                | GatewayErrorKind::PolicyDenied => RunErrorKind::CapabilityDenied,
                GatewayErrorKind::BudgetExceeded | GatewayErrorKind::MaxDepth => {
                    RunErrorKind::BudgetExceeded
                }
                GatewayErrorKind::Cancelled => RunErrorKind::Cancelled,
                GatewayErrorKind::DeadlineExceeded => RunErrorKind::DeadlineExceeded,
                GatewayErrorKind::InvalidInput => RunErrorKind::InvalidInput,
                GatewayErrorKind::NotFound | GatewayErrorKind::ChildFailed => {
                    RunErrorKind::Invocation
                }
                GatewayErrorKind::ObservabilityFailed => {
                    RunErrorKind::Extension("runifold.observability".into())
                }
            },
            Self::InvalidConfig(_) => RunErrorKind::InvalidInput,
            Self::Protocol(_)
            | Self::ToolRequirementUnsatisfied { .. }
            | Self::EmptyTerminalResponse { .. }
            | Self::StructuredOutputUnsatisfied { .. }
            | Self::ToolOutputNotVisible { .. } => RunErrorKind::Protocol,
            Self::Journal(_) => RunErrorKind::Extension("runifold.observability".into()),
            Self::Checkpoint(_) | Self::AmbiguousCheckpoint { .. } => {
                RunErrorKind::Extension("runifold.checkpoint".into())
            }
            Self::Effect(error) => match error.kind {
                EffectExecutorErrorKind::CapabilityDenied => RunErrorKind::CapabilityDenied,
                EffectExecutorErrorKind::Cancelled => RunErrorKind::Cancelled,
                EffectExecutorErrorKind::DeadlineExceeded => RunErrorKind::DeadlineExceeded,
                EffectExecutorErrorKind::IdempotencyConflict
                | EffectExecutorErrorKind::Protocol => RunErrorKind::Protocol,
                EffectExecutorErrorKind::Handler => error
                    .source_error
                    .as_ref()
                    .map_or(RunErrorKind::Invocation, |error| error.kind.clone()),
                EffectExecutorErrorKind::Ambiguous
                | EffectExecutorErrorKind::Store
                | EffectExecutorErrorKind::Observability => {
                    RunErrorKind::Extension("runifold.effect".into())
                }
                _ => RunErrorKind::Extension("runifold.effect".into()),
            },
        }
    }

    /// Returns whether retrying this failed Agent run is known to be safe.
    pub fn retry_safety(&self) -> RetrySafety {
        match self {
            Self::Model(error) => error.retry_safety,
            Self::Tool(error) => error.retry_safety,
            Self::Effect(error) => error
                .source_error
                .as_ref()
                .map_or(RetrySafety::Unknown, |error| error.retry_safety),
            _ => RetrySafety::Unknown,
        }
    }

    /// Normalizes this failure into the public run-level policy contract.
    pub fn to_run_error(&self) -> RunError {
        let metadata = match self {
            Self::Model(error) => error.metadata.clone(),
            _ => BTreeMap::new(),
        };
        RunError {
            kind: self.run_error_kind(),
            message: self.to_string(),
            retry_safety: self.retry_safety(),
            metadata,
        }
    }
}

#[cfg(test)]
mod tests {
    use runifold_core::{RetrySafety, RunErrorKind};
    use runifold_model::{ModelError, ModelErrorKind};

    use super::AgentError;

    #[test]
    fn model_failure_normalization_preserves_kind_and_retry_safety() {
        let mut model = ModelError::local(ModelErrorKind::MalformedToolArguments, "invalid JSON");
        model.retry_safety = RetrySafety::Safe;
        let error = AgentError::Model(model);

        let normalized = error.to_run_error();

        assert_eq!(normalized.kind, RunErrorKind::Protocol);
        assert_eq!(normalized.retry_safety, RetrySafety::Safe);
    }

    #[test]
    fn local_agent_limits_have_a_stable_budget_classification() {
        let error = AgentError::MaxTurns { max_turns: 3 };

        assert_eq!(error.run_error_kind(), RunErrorKind::BudgetExceeded);
        assert_eq!(error.retry_safety(), RetrySafety::Unknown);
    }
}
