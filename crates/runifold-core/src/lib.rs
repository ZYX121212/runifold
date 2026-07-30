//! Stable runtime-kernel primitives for Runifold.
//!
//! This crate deliberately contains no model provider, network transport,
//! agent loop, or external protocol implementation.

mod budget;
mod cancellation;
mod capability;
mod checkpoint;
mod context;
mod effect;
mod error;
mod event;
mod id;
mod journal;
mod recorder;

pub use budget::{Budget, BudgetExceeded, BudgetReservation, BudgetResource, BudgetTracker, Usage};
pub use cancellation::CancellationToken;
pub use capability::{CapabilityDescriptor, CapabilityKind, CapabilitySet, EffectClass, RiskLevel};
pub use checkpoint::{
    Checkpoint, CheckpointError, CheckpointErrorKind, CheckpointStore, InMemoryCheckpointStore,
};
pub use context::{
    AuthorityAmplification, BudgetReservationMismatch, ChildRunError, Metadata, RunContext,
};
pub use effect::{EffectKind, EffectRequest};
pub use error::{RetrySafety, RunError, RunErrorKind};
pub use event::{
    BudgetEvent, ChildEvent, DomainEvent, EffectEvent, EventFactory, EventMeta, LifecycleEvent,
    RunEvent, RunEventKind,
};
pub use id::{CapabilityId, CheckpointId, EffectId, EventId, InvocationId, RunId};
pub use journal::{InMemoryJournal, Journal, JournalError};
pub use recorder::RunRecorder;
/// Monotonic runtime clock on native targets.
#[cfg(not(target_arch = "wasm32"))]
pub use std::time::Instant;
/// Monotonic runtime clock backed by the browser performance clock.
#[cfg(target_arch = "wasm32")]
pub use web_time::Instant;
