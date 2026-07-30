use std::{collections::BTreeMap, sync::Arc};

use serde_json::Value;

use crate::{
    BudgetReservation, BudgetTracker, CancellationToken, CapabilityId, CapabilitySet, EventId,
    Instant, Journal, JournalError, RunEvent, RunEventKind, RunId, RunRecorder,
};
use thiserror::Error;

/// A child Run cannot receive authority absent from its parent.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("child Run requested capability `{capability}` ({capability_id}) absent from its parent")]
pub struct AuthorityAmplification {
    /// Stable identity of the rejected capability.
    pub capability_id: CapabilityId,
    /// Human-readable capability name.
    pub capability: String,
}

/// A reservation from another run tree cannot fund a child run.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("budget reservation does not belong to this run tree")]
pub struct BudgetReservationMismatch;

/// Failure to create a capability-attenuated child Run.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChildRunError {
    /// The child requested authority absent from its parent.
    #[error(transparent)]
    Authority(#[from] AuthorityAmplification),
    /// The budget reservation belongs to another run tree.
    #[error(transparent)]
    BudgetReservation(#[from] BudgetReservationMismatch),
}

/// Namespaced runtime metadata.
pub type Metadata = BTreeMap<String, Value>;

/// The authority, lifetime, and accounting scope of one run.
#[derive(Clone, Debug)]
pub struct RunContext {
    run_id: RunId,
    parent_run_id: Option<RunId>,
    root_run_id: RunId,
    caused_by: Option<EventId>,
    deadline: Option<Instant>,
    cancellation: CancellationToken,
    budget: BudgetTracker,
    capabilities: CapabilitySet,
    metadata: Metadata,
    recorder: Option<RunRecorder>,
}

impl RunContext {
    /// Creates a root run context.
    pub fn root(budget: BudgetTracker, capabilities: CapabilitySet) -> Self {
        let run_id = RunId::new();
        Self {
            run_id,
            parent_run_id: None,
            root_run_id: run_id,
            caused_by: None,
            deadline: None,
            cancellation: CancellationToken::new(),
            budget,
            capabilities,
            metadata: Metadata::new(),
            recorder: None,
        }
    }

    /// Returns this run's identity.
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    /// Returns this run's parent identity.
    pub const fn parent_run_id(&self) -> Option<RunId> {
        self.parent_run_id
    }

    /// Returns the root run identity.
    pub const fn root_run_id(&self) -> RunId {
        self.root_run_id
    }

    /// Returns the event that caused this run to start, when known.
    pub const fn caused_by(&self) -> Option<EventId> {
        self.caused_by
    }

    /// Returns the effective deadline.
    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Returns the hierarchical cancellation token.
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Returns the shared run-tree budget tracker.
    pub const fn budget(&self) -> &BudgetTracker {
        &self.budget
    }

    /// Returns capabilities explicitly granted to this run.
    pub const fn capabilities(&self) -> &CapabilitySet {
        &self.capabilities
    }

    /// Returns runtime metadata.
    pub const fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Mutably returns runtime metadata.
    pub const fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }

    /// Returns the configured event recorder, when observability is enabled.
    pub const fn recorder(&self) -> Option<&RunRecorder> {
        self.recorder.as_ref()
    }

    /// Enables structured event recording for this run and future children.
    #[must_use]
    pub fn with_journal(mut self, journal: Arc<dyn Journal>) -> Self {
        self.recorder = Some(RunRecorder::new(journal, self.run_id, self.parent_run_id));
        self
    }

    /// Records one event when observability is enabled.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError`] when the configured journal rejects the event.
    pub fn record(
        &self,
        kind: RunEventKind,
        caused_by: Option<EventId>,
    ) -> Result<Option<RunEvent>, JournalError> {
        self.recorder
            .as_ref()
            .map(|recorder| recorder.record(kind, caused_by))
            .transpose()
    }

    /// Sets a deadline, clamped to any existing earlier deadline.
    #[must_use]
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(
            self.deadline
                .map_or(deadline, |current| current.min(deadline)),
        );
        self
    }

    /// Sets the event that caused this run to start.
    #[must_use]
    pub const fn with_cause(mut self, event_id: EventId) -> Self {
        self.caused_by = Some(event_id);
        self
    }

    /// Creates a child with an explicitly attenuated capability set.
    ///
    /// The child shares the root budget tracker, receives a descendant
    /// cancellation token, and does not inherit metadata or capabilities.
    ///
    /// # Errors
    ///
    /// Returns [`AuthorityAmplification`] when the child requests any
    /// capability absent from this Run.
    pub fn child(&self, capabilities: CapabilitySet) -> Result<Self, AuthorityAmplification> {
        self.validate_child_authority(&capabilities)?;
        Ok(self.child_with_budget(capabilities, self.budget.clone()))
    }

    /// Creates an attenuated child funded by a scoped budget reservation.
    ///
    /// # Errors
    ///
    /// Returns [`ChildRunError`] when the child requests authority absent from
    /// this Run or the reservation was created by another run tree.
    pub fn child_reserved(
        &self,
        capabilities: CapabilitySet,
        reservation: &BudgetReservation,
    ) -> Result<Self, ChildRunError> {
        self.validate_child_authority(&capabilities)?;
        if !reservation.belongs_to(&self.budget) {
            return Err(BudgetReservationMismatch.into());
        }
        Ok(self.child_with_budget(capabilities, reservation.tracker()))
    }

    fn validate_child_authority(
        &self,
        capabilities: &CapabilitySet,
    ) -> Result<(), AuthorityAmplification> {
        if let Some(missing) = capabilities.first_missing_from(&self.capabilities) {
            return Err(AuthorityAmplification {
                capability_id: missing.id,
                capability: missing.name.clone(),
            });
        }
        Ok(())
    }

    fn child_with_budget(&self, capabilities: CapabilitySet, budget: BudgetTracker) -> Self {
        let run_id = RunId::new();
        Self {
            run_id,
            parent_run_id: Some(self.run_id),
            root_run_id: self.root_run_id,
            caused_by: None,
            deadline: self.deadline,
            cancellation: self.cancellation.child_token(),
            budget,
            capabilities,
            metadata: Metadata::new(),
            recorder: self
                .recorder
                .as_ref()
                .map(|recorder| recorder.child(run_id, self.run_id)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{AuthorityAmplification, BudgetReservationMismatch, ChildRunError, RunContext};
    use crate::{
        Budget, BudgetTracker, CapabilityDescriptor, CapabilityId, CapabilityKind, CapabilitySet,
        EffectClass, InMemoryJournal, LifecycleEvent, RiskLevel, RunEventKind, Usage,
    };

    #[test]
    fn child_preserves_lineage_but_not_ambient_authority() {
        let mut parent_capabilities = CapabilitySet::new();
        parent_capabilities.grant(CapabilityDescriptor {
            id: CapabilityId::new(),
            name: "write-file".into(),
            version: "1".into(),
            kind: CapabilityKind::Tool,
            input_schema: serde_json::json!({}),
            output_schema: serde_json::json!({}),
            effect: EffectClass::NonIdempotentWrite,
            risk: RiskLevel::High,
            metadata: std::collections::BTreeMap::default(),
        });

        let parent = RunContext::root(
            BudgetTracker::new(Budget {
                tokens: Some(10),
                ..Budget::default()
            }),
            parent_capabilities,
        );
        let child = parent.child(CapabilitySet::new()).unwrap();

        assert_eq!(child.parent_run_id(), Some(parent.run_id()));
        assert_eq!(child.root_run_id(), parent.root_run_id());
        assert!(child.capabilities().is_empty());

        child
            .budget()
            .try_consume(Usage {
                tokens: 4,
                ..Usage::default()
            })
            .unwrap();
        assert_eq!(parent.budget().usage().tokens, 4);

        parent.cancellation().cancel();
        assert!(child.cancellation().is_cancelled());
    }

    #[test]
    fn child_recorders_share_a_journal_but_keep_per_run_sequences() {
        let journal = InMemoryJournal::new();
        let parent = RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new())
            .with_journal(Arc::new(journal.clone()));
        let child = parent.child(CapabilitySet::new()).unwrap();

        parent
            .record(RunEventKind::Lifecycle(LifecycleEvent::Started), None)
            .unwrap();
        child
            .record(RunEventKind::Lifecycle(LifecycleEvent::Started), None)
            .unwrap();
        parent
            .record(
                RunEventKind::Lifecycle(LifecycleEvent::Completed {
                    output: serde_json::json!({}),
                }),
                None,
            )
            .unwrap();

        let events = journal.events();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].meta.sequence, 0);
        assert_eq!(events[1].meta.sequence, 0);
        assert_eq!(events[2].meta.sequence, 1);
        assert_eq!(events[1].meta.parent_run_id, Some(parent.run_id()));
    }

    #[test]
    fn child_rejects_a_reservation_from_another_run_tree() {
        let first = RunContext::root(
            BudgetTracker::new(Budget {
                turns: Some(1),
                ..Budget::default()
            }),
            CapabilitySet::new(),
        );
        let second = RunContext::root(
            BudgetTracker::new(Budget {
                turns: Some(1),
                ..Budget::default()
            }),
            CapabilitySet::new(),
        );
        let reservation = first
            .budget()
            .try_reserve(Usage {
                turns: 1,
                ..Usage::default()
            })
            .unwrap();

        let error = second
            .child_reserved(CapabilitySet::new(), &reservation)
            .unwrap_err();

        assert_eq!(
            error,
            ChildRunError::BudgetReservation(BudgetReservationMismatch)
        );
    }

    #[test]
    fn child_rejects_authority_absent_from_parent() {
        let parent = RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new());
        let requested = CapabilityDescriptor {
            id: CapabilityId::new(),
            name: "filesystem.write".into(),
            version: "1".into(),
            kind: CapabilityKind::Tool,
            input_schema: serde_json::json!({}),
            output_schema: serde_json::json!({}),
            effect: EffectClass::NonIdempotentWrite,
            risk: RiskLevel::High,
            metadata: std::collections::BTreeMap::default(),
        };
        let mut capabilities = CapabilitySet::new();
        capabilities.grant(requested.clone());

        let error = parent.child(capabilities).unwrap_err();

        assert_eq!(
            error,
            AuthorityAmplification {
                capability_id: requested.id,
                capability: requested.name,
            }
        );
    }

    #[test]
    fn reserved_child_checks_authority_before_using_the_reservation() {
        let parent = RunContext::root(
            BudgetTracker::new(Budget {
                turns: Some(1),
                ..Budget::default()
            }),
            CapabilitySet::new(),
        );
        let reservation = parent
            .budget()
            .try_reserve(Usage {
                turns: 1,
                ..Usage::default()
            })
            .unwrap();
        let requested = CapabilityDescriptor {
            id: CapabilityId::new(),
            name: "network.write".into(),
            version: "1".into(),
            kind: CapabilityKind::Tool,
            input_schema: serde_json::json!({}),
            output_schema: serde_json::json!({}),
            effect: EffectClass::IdempotentWrite,
            risk: RiskLevel::Medium,
            metadata: std::collections::BTreeMap::default(),
        };
        let mut capabilities = CapabilitySet::new();
        capabilities.grant(requested.clone());

        let error = parent
            .child_reserved(capabilities, &reservation)
            .unwrap_err();

        assert_eq!(
            error,
            ChildRunError::Authority(AuthorityAmplification {
                capability_id: requested.id,
                capability: requested.name,
            })
        );
        assert_eq!(reservation.remaining().turns, 1);
    }
}
