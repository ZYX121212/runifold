//! Write-ahead coordination for recoverable external effects.

mod error;
mod executor;
mod handler;
mod record;
mod store;

pub use error::{EffectExecutorError, EffectExecutorErrorKind};
pub use executor::{EffectEventPayloadPolicy, EffectExecutor, EffectOutcome, EffectRecoveryPolicy};
pub use handler::{
    EffectExecutionContext, EffectFuture, EffectHandler, EffectReconciler, EffectReconciliation,
};
pub use record::{EffectRecord, EffectStatus};
pub use store::{EffectStore, InMemoryEffectStore};
