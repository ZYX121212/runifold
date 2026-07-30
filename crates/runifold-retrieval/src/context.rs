use std::time::Duration;

use runifold_core::{CancellationToken, Instant, InvocationId, RunContext, RunId};

use crate::RetrievalError;

/// Lifecycle scope for one embedding or retrieval invocation.
///
/// Provider adapters and storage backends receive lifecycle authority rather
/// than ambient runtime or provider configuration.
#[derive(Clone, Debug)]
pub struct RetrievalContext {
    invocation_id: InvocationId,
    run_id: Option<RunId>,
    deadline: Option<Instant>,
    cancellation: CancellationToken,
}

impl RetrievalContext {
    /// Creates a standalone retrieval context.
    pub fn new() -> Self {
        Self {
            invocation_id: InvocationId::new(),
            run_id: None,
            deadline: None,
            cancellation: CancellationToken::new(),
        }
    }

    /// Creates a retrieval invocation scoped beneath a run.
    pub fn for_run(run: &RunContext) -> Self {
        Self {
            invocation_id: InvocationId::new(),
            run_id: Some(run.run_id()),
            deadline: run.deadline(),
            cancellation: run.cancellation().child_token(),
        }
    }

    /// Returns the invocation identity.
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the owning run identity, when present.
    pub const fn run_id(&self) -> Option<RunId> {
        self.run_id
    }

    /// Returns the effective deadline.
    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Returns remaining time before the deadline.
    pub fn remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    /// Returns the hierarchical cancellation token.
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Rejects work after cancellation or deadline expiry.
    ///
    /// # Errors
    ///
    /// Returns [`RetrievalError::Cancelled`] or
    /// [`RetrievalError::DeadlineExceeded`] when the scope is no longer live.
    pub fn check_live(&self) -> Result<(), RetrievalError> {
        if self.cancellation.is_cancelled() {
            return Err(RetrievalError::Cancelled);
        }
        if self
            .remaining()
            .is_some_and(|remaining| remaining.is_zero())
        {
            return Err(RetrievalError::DeadlineExceeded);
        }
        Ok(())
    }

    /// Sets a deadline while preserving an existing earlier deadline.
    #[must_use]
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(
            self.deadline
                .map_or(deadline, |current| current.min(deadline)),
        );
        self
    }

    /// Replaces the cancellation root with a child of the supplied token.
    #[must_use]
    pub fn with_cancellation(mut self, cancellation: &CancellationToken) -> Self {
        self.cancellation = cancellation.child_token();
        self
    }

    /// Creates a distinct backend attempt in the same lifecycle scope.
    #[must_use]
    pub fn child_attempt(&self) -> Self {
        Self {
            invocation_id: InvocationId::new(),
            run_id: self.run_id,
            deadline: self.deadline,
            cancellation: self.cancellation.child_token(),
        }
    }
}

impl Default for RetrievalContext {
    fn default() -> Self {
        Self::new()
    }
}
