use std::{fmt, sync::Arc, time::Duration};

use runifold_core::{CancellationToken, Instant, InvocationId, RunContext, RunId};
use runifold_model::{ArtifactScope, ArtifactStore};

/// Execution scope for one tool invocation.
#[derive(Clone)]
pub struct ToolContext {
    invocation_id: InvocationId,
    run_id: RunId,
    deadline: Option<Instant>,
    cancellation: CancellationToken,
    artifact_store: Option<Arc<dyn ArtifactStore>>,
    artifact_scope: Option<ArtifactScope>,
}

impl ToolContext {
    pub(crate) fn for_run(run: &RunContext) -> Self {
        Self {
            invocation_id: InvocationId::new(),
            run_id: run.run_id(),
            deadline: run.deadline(),
            cancellation: run.cancellation().child_token(),
            artifact_store: None,
            artifact_scope: None,
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

    pub(crate) fn with_artifact_store(
        mut self,
        scope: Option<ArtifactScope>,
        store: Option<Arc<dyn ArtifactStore>>,
    ) -> Self {
        self.artifact_scope = scope;
        self.artifact_store = store;
        self
    }

    /// Returns the configured artifact store for producing reference-only rich
    /// results.
    pub fn artifact_store(&self) -> Option<&Arc<dyn ArtifactStore>> {
        self.artifact_store.as_ref()
    }

    /// Returns the mandatory isolation scope paired with the artifact store.
    pub const fn artifact_scope(&self) -> Option<&ArtifactScope> {
        self.artifact_scope.as_ref()
    }
}

impl fmt::Debug for ToolContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolContext")
            .field("invocation_id", &self.invocation_id)
            .field("run_id", &self.run_id)
            .field("deadline", &self.deadline)
            .field("artifact_store", &self.artifact_store.is_some())
            .field("artifact_scope", &self.artifact_scope)
            .finish_non_exhaustive()
    }
}
