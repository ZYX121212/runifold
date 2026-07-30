use std::time::Duration;

use runifold_core::{CancellationToken, Instant, InvocationId, RunContext, RunId};

/// Execution scope for one tool invocation.
#[derive(Clone, Debug)]
pub struct ToolContext {
    invocation_id: InvocationId,
    run_id: RunId,
    deadline: Option<Instant>,
    cancellation: CancellationToken,
}

impl ToolContext {
    pub(crate) fn for_run(run: &RunContext) -> Self {
        Self {
            invocation_id: InvocationId::new(),
            run_id: run.run_id(),
            deadline: run.deadline(),
            cancellation: run.cancellation().child_token(),
        }
    }

    /// Returns the invocation identity.
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the owning run identity.
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the effective deadline.
    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Returns the remaining time before the deadline.
    pub fn remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    /// Returns the hierarchical cancellation token.
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
}
