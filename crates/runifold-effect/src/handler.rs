use std::{future::Future, pin::Pin, time::Instant};

use runifold_core::{CancellationToken, EffectRequest, RunContext, RunError, RunId};
use serde_json::Value;

/// Boxed future returned by an effect handler.
pub type EffectFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

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
