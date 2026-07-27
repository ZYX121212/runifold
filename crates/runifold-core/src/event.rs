use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{EffectId, EventId, RunError, RunId, Usage};

/// Metadata shared by every runtime event.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EventMeta {
    /// Globally unique event identity.
    pub event_id: EventId,
    /// Monotonic sequence number within a run.
    pub sequence: u64,
    /// Run that emitted this event.
    pub run_id: RunId,
    /// Parent of the emitting run.
    pub parent_run_id: Option<RunId>,
    /// Event that caused this event, when known.
    pub caused_by: Option<EventId>,
    /// Milliseconds since the Unix epoch.
    pub timestamp_ms: u64,
}

/// An immutable runtime fact.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RunEvent {
    /// Event envelope metadata.
    pub meta: EventMeta,
    /// Event payload.
    pub kind: RunEventKind,
}

/// Runtime event categories.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[non_exhaustive]
pub enum RunEventKind {
    /// Run lifecycle event.
    Lifecycle(LifecycleEvent),
    /// External-effect event.
    Effect(EffectEvent),
    /// Parent-child relationship event.
    Child(ChildEvent),
    /// Budget-accounting event.
    Budget(BudgetEvent),
    /// Namespaced domain-specific event.
    Domain(DomainEvent),
}

/// Run lifecycle facts.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[non_exhaustive]
pub enum LifecycleEvent {
    /// The run started.
    Started,
    /// The run completed with a serializable output.
    Completed {
        /// Final serializable output.
        output: Value,
    },
    /// The run failed.
    Failed {
        /// Structured terminal error.
        error: RunError,
    },
    /// The run was cancelled.
    Cancelled,
}

/// External-effect facts.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[non_exhaustive]
pub enum EffectEvent {
    /// An effect was requested.
    Requested {
        /// Requested effect identity.
        effect_id: EffectId,
    },
    /// Policy allowed the effect to start.
    Started {
        /// Started effect identity.
        effect_id: EffectId,
    },
    /// The effect completed.
    Completed {
        /// Completed effect identity.
        effect_id: EffectId,
        /// Serializable effect output.
        output: Value,
    },
    /// The effect failed.
    Failed {
        /// Failed effect identity.
        effect_id: EffectId,
        /// Structured effect error.
        error: RunError,
    },
}

/// Parent-child run relationship facts.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum ChildEvent {
    /// A child run was created.
    Started {
        /// Child run identity.
        child_run_id: RunId,
    },
    /// A child run completed.
    Completed {
        /// Child run identity.
        child_run_id: RunId,
    },
    /// A child run failed.
    Failed {
        /// Child run identity.
        child_run_id: RunId,
    },
    /// A child run was cancelled.
    Cancelled {
        /// Child run identity.
        child_run_id: RunId,
    },
}

/// Budget-accounting facts.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum BudgetEvent {
    /// Cumulative usage changed.
    Updated {
        /// New cumulative usage snapshot.
        usage: Usage,
    },
}

/// A forward-compatible namespaced event.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DomainEvent {
    /// Stable namespace owned by a crate or protocol adapter.
    pub namespace: String,
    /// Event type within the namespace.
    pub name: String,
    /// Structured payload.
    pub payload: Value,
}

/// Creates events with monotonic per-run sequence numbers.
#[derive(Debug)]
pub struct EventFactory {
    run_id: RunId,
    parent_run_id: Option<RunId>,
    sequence: AtomicU64,
}

impl EventFactory {
    /// Creates an event factory for a run.
    pub const fn new(run_id: RunId, parent_run_id: Option<RunId>) -> Self {
        Self {
            run_id,
            parent_run_id,
            sequence: AtomicU64::new(0),
        }
    }

    /// Emits the next event.
    pub fn emit(&self, kind: RunEventKind, caused_by: Option<EventId>) -> RunEvent {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
            });

        RunEvent {
            meta: EventMeta {
                event_id: EventId::new(),
                sequence,
                run_id: self.run_id,
                parent_run_id: self.parent_run_id,
                caused_by,
                timestamp_ms,
            },
            kind,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EventFactory, LifecycleEvent, RunEventKind};
    use crate::RunId;

    #[test]
    fn event_sequences_are_monotonic_and_causal() {
        let factory = EventFactory::new(RunId::new(), None);
        let started = factory.emit(RunEventKind::Lifecycle(LifecycleEvent::Started), None);
        let completed = factory.emit(
            RunEventKind::Lifecycle(LifecycleEvent::Completed {
                output: serde_json::json!("done"),
            }),
            Some(started.meta.event_id),
        );

        assert_eq!(started.meta.sequence, 0);
        assert_eq!(completed.meta.sequence, 1);
        assert_eq!(completed.meta.caused_by, Some(started.meta.event_id));
    }
}
