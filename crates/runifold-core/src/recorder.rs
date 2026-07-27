use std::{fmt, sync::Arc};

use crate::{EventFactory, EventId, Journal, JournalError, RunEvent, RunEventKind, RunId};

/// Cloneable event emitter bound to one run and a shared journal.
#[derive(Clone)]
pub struct RunRecorder {
    journal: Arc<dyn Journal>,
    events: Arc<EventFactory>,
}

impl RunRecorder {
    /// Creates a recorder for one run.
    pub fn new(journal: Arc<dyn Journal>, run_id: RunId, parent_run_id: Option<RunId>) -> Self {
        Self {
            journal,
            events: Arc::new(EventFactory::new(run_id, parent_run_id)),
        }
    }

    /// Creates a recorder for a child run using the same journal.
    #[must_use]
    pub fn child(&self, run_id: RunId, parent_run_id: RunId) -> Self {
        Self::new(self.journal.clone(), run_id, Some(parent_run_id))
    }

    /// Emits and records one immutable event.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError`] when the journal rejects the event.
    pub fn record(
        &self,
        kind: RunEventKind,
        caused_by: Option<EventId>,
    ) -> Result<RunEvent, JournalError> {
        let event = self.events.emit(kind, caused_by);
        self.journal.record(&event)?;
        Ok(event)
    }
}

impl fmt::Debug for RunRecorder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RunRecorder(..)")
    }
}
