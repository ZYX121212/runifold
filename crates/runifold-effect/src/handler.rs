use std::{future::Future, pin::Pin};

use runifold_core::{CancellationToken, EffectRequest, Instant, RunContext, RunError, RunId};
use serde_json::Value;

/// Boxed future returned by an effect handler.
#[cfg(not(target_arch = "wasm32"))]
pub type EffectFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Boxed effect-handler future on single-threaded WASM.
#[cfg(target_arch = "wasm32")]
pub type EffectFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Lifecycle-only context passed to an external-effect handler.
#[derive(Clone, Debug)]
pub struct EffectExecutionContext {
    run_id: RunId,
    deadline: Option<Instant>,
    cancellation: CancellationToken,
}

impl EffectExecutionContext {
    pub(crate) fn for_run(run: &RunContext) -> Self {
        Self {
            run_id: run.run_id(),
            deadline: run.deadline(),
            cancellation: run.cancellation().child_token(),
        }
    }

    /// Returns the owning Run identity.
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns the effective deadline.
    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Returns the descendant cancellation token.
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
}

/// Object-safe implementation boundary for one class of external effect.
pub trait EffectHandler: Send + Sync {
    /// Executes a prepared effect request.
    fn execute(
        &self,
        request: &EffectRequest,
        context: EffectExecutionContext,
    ) -> EffectFuture<'_, Result<Value, RunError>>;
}

/// Result of querying an external system for an ambiguously started effect.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum EffectReconciliation {
    /// The remote operation completed and this is its canonical output.
    Completed(Value),
    /// The remote system proves the operation did not execute.
    NotExecuted,
    /// The remote system cannot determine the outcome safely.
    Ambiguous,
}

/// Optional remote-state boundary for resolving started effects after a crash.
///
/// Implementations should query by the request's stable idempotency key or an
/// equivalent remote operation identity. They must not perform the effect.
pub trait EffectReconciler: Send + Sync {
    /// Queries the external system for the effect's durable outcome.
    fn reconcile(
        &self,
        request: &EffectRequest,
        context: EffectExecutionContext,
    ) -> EffectFuture<'_, Result<EffectReconciliation, RunError>>;
}
