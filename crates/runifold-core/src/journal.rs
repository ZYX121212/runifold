use std::sync::{Arc, Mutex, MutexGuard};

use thiserror::Error;

use crate::RunEvent;

/// A journal storage failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("journal error: {message}")]
pub struct JournalError {
    /// Safe failure explanation.
    pub message: String,
}

/// Receives immutable runtime events.
pub trait Journal: Send + Sync {
    /// Records one event.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError`] when the backing store cannot durably accept
    /// the event.
    fn record(&self, event: &RunEvent) -> Result<(), JournalError>;
}

/// A cloneable in-memory journal for tests and ephemeral runs.
#[derive(Clone, Debug, Default)]
pub struct InMemoryJournal {
    events: Arc<Mutex<Vec<RunEvent>>>,
}

impl InMemoryJournal {
    /// Creates an empty journal.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a snapshot of all recorded events.
    pub fn events(&self) -> Vec<RunEvent> {
        self.lock_events().clone()
    }

    /// Returns the number of recorded events.
    pub fn len(&self) -> usize {
        self.lock_events().len()
    }

    /// Returns whether no events have been recorded.
    pub fn is_empty(&self) -> bool {
        self.lock_events().is_empty()
    }

    /// Appends an event to the in-memory log.
    pub fn push(&self, event: RunEvent) {
        self.lock_events().push(event);
    }

    fn lock_events(&self) -> MutexGuard<'_, Vec<RunEvent>> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Journal for InMemoryJournal {
    fn record(&self, event: &RunEvent) -> Result<(), JournalError> {
        self.push(event.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{InMemoryJournal, Journal};
    use crate::{EventFactory, LifecycleEvent, RunEventKind, RunId};

    #[test]
    fn clones_share_the_same_event_log() {
        let journal = InMemoryJournal::new();
        let clone = journal.clone();
        let event = EventFactory::new(RunId::new(), None)
            .emit(RunEventKind::Lifecycle(LifecycleEvent::Started), None);

        journal.record(&event).unwrap();

        assert_eq!(clone.events(), vec![event]);
    }
}
