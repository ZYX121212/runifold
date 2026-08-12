use runifold_core::{BudgetExceeded, CheckpointError, JournalError};
use runifold_effect::EffectExecutorError;
use runifold_model::{ModelError, StructuredOutputErrorKind};
use runifold_retrieval::RetrievalError;
use runifold_tool::ToolError;
use thiserror::Error;

use crate::GatewayError;

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
