//! Agent terminal-completion contracts and bounded repair policy.

use serde::{Deserialize, Serialize};

/// Bounded policy applied when a model returns an invalid terminal candidate.
///
/// Repairs are explicit and consume the ordinary turn, token, cost, duration,
/// and deadline budgets. A zero repair limit preserves fail-fast behavior.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct CompletionRequirement {
    max_terminal_repairs: u32,
    retry_empty_response: bool,
    #[serde(default)]
    retry_truncated_response: bool,
}

impl CompletionRequirement {
    /// Creates a fail-fast completion policy.
    pub const fn new() -> Self {
        Self {
            max_terminal_repairs: 0,
            retry_empty_response: false,
            retry_truncated_response: false,
        }
    }

    /// Sets the independent maximum number of terminal repair turns.
    #[must_use]
    pub const fn max_repairs(mut self, maximum: u32) -> Self {
        self.max_terminal_repairs = maximum;
        self
    }

    /// Allows an empty or non-model-visible terminal response to consume a
    /// repair turn instead of failing immediately.
    #[must_use]
    pub const fn retry_empty_response(mut self, enabled: bool) -> Self {
        self.retry_empty_response = enabled;
        self
    }

    /// Allows Length responses to consume the existing terminal repair allowance.
    /// Partial history and usage are retained; partial Tool calls are never executed.
    /// This continues a known completed response, never an unknown request outcome.
    #[must_use]
    pub const fn retry_truncated_response(mut self, enabled: bool) -> Self {
        self.retry_truncated_response = enabled;
        self
    }

    /// Returns whether Length completions may consume a repair turn.
    pub const fn retries_truncated_response(self) -> bool {
        self.retry_truncated_response
    }

    /// Returns the configured terminal repair limit.
    pub const fn max_terminal_repairs(self) -> u32 {
        self.max_terminal_repairs
    }

    /// Returns whether empty terminal candidates are repairable.
    pub const fn retries_empty_response(self) -> bool {
        self.retry_empty_response
    }
}

/// Stable reason why a terminal model response did not satisfy the Agent.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TerminalRequirementFailureKind {
    /// No model-visible terminal content was produced.
    EmptyResponse,
    /// A structured response contained no textual JSON body.
    MissingStructuredText,
    /// Structured text did not decode as the requested Rust type.
    InvalidStructuredOutput,
    /// The provider returned an explicit refusal.
    Refusal,
    /// The provider ended a response at its output length limit.
    TruncatedResponse,
}

/// Safe, bounded diagnostic for a rejected terminal candidate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TerminalRequirementFailure {
    /// Stable failure category.
    pub kind: TerminalRequirementFailureKind,
    /// One-based JSON line for a structured decoding failure.
    pub line: Option<usize>,
    /// One-based JSON column for a structured decoding failure.
    pub column: Option<usize>,
}

impl TerminalRequirementFailure {
    pub(crate) const fn new(kind: TerminalRequirementFailureKind) -> Self {
        Self {
            kind,
            line: None,
            column: None,
        }
    }

    pub(crate) const fn repairable(&self, policy: CompletionRequirement) -> bool {
        match self.kind {
            TerminalRequirementFailureKind::TruncatedResponse => {
                policy.retries_truncated_response()
            }
            TerminalRequirementFailureKind::EmptyResponse
            | TerminalRequirementFailureKind::MissingStructuredText => {
                policy.retries_empty_response()
            }
            TerminalRequirementFailureKind::InvalidStructuredOutput => true,
            TerminalRequirementFailureKind::Refusal => false,
        }
    }
}
