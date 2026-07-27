use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::{CheckpointId, RunId};

/// Versioned, domain-neutral persisted execution state.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Checkpoint {
    /// Stable checkpoint identity across revisions.
    pub id: CheckpointId,
    /// Monotonic compare-and-swap revision.
    pub revision: u64,
    /// Run that owns the checkpoint.
    pub run_id: RunId,
    /// Namespaced payload kind.
    pub kind: String,
    /// Schema version owned by `kind`.
    pub schema_version: u32,
    /// Domain-owned serializable state.
    pub payload: Value,
    /// Milliseconds since the Unix epoch.
    pub updated_at_ms: u64,
}

impl Checkpoint {
    /// Creates revision zero of a checkpoint.
    pub fn initial(
        id: CheckpointId,
        run_id: RunId,
        kind: impl Into<String>,
        schema_version: u32,
        payload: Value,
    ) -> Self {
        Self {
            id,
            revision: 0,
            run_id,
            kind: kind.into(),
            schema_version,
            payload,
            updated_at_ms: now_ms(),
        }
    }

    /// Creates the next revision with a replacement payload.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError`] if the revision counter overflows.
    pub fn next(&self, payload: Value) -> Result<Self, CheckpointError> {
        Ok(Self {
            id: self.id,
            revision: self.revision.checked_add(1).ok_or_else(|| {
                CheckpointError::new(
                    CheckpointErrorKind::Conflict,
                    "checkpoint revision overflow",
                )
            })?,
            run_id: self.run_id,
            kind: self.kind.clone(),
            schema_version: self.schema_version,
            payload,
            updated_at_ms: now_ms(),
        })
    }
}

/// Atomic persistence boundary for checkpoints.
pub trait CheckpointStore: Send + Sync {
    /// Loads the latest checkpoint revision.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError`] when storage fails or the checkpoint does
    /// not exist.
    fn load(&self, id: CheckpointId) -> Result<Checkpoint, CheckpointError>;

    /// Creates or atomically replaces a checkpoint.
    ///
    /// `expected_revision = None` means create-only. An existing checkpoint
    /// makes that operation conflict. Updates must provide the exact current
    /// revision and a checkpoint whose revision is one greater.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError`] on storage failure or revision conflict.
    fn compare_and_swap(
        &self,
        checkpoint: &Checkpoint,
        expected_revision: Option<u64>,
    ) -> Result<(), CheckpointError>;
}

/// Cloneable in-memory checkpoint store for tests and ephemeral processes.
#[derive(Clone, Debug, Default)]
pub struct InMemoryCheckpointStore {
    checkpoints: Arc<Mutex<BTreeMap<CheckpointId, Checkpoint>>>,
}

impl InMemoryCheckpointStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    fn checkpoints(&self) -> MutexGuard<'_, BTreeMap<CheckpointId, Checkpoint>> {
        self.checkpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl CheckpointStore for InMemoryCheckpointStore {
    fn load(&self, id: CheckpointId) -> Result<Checkpoint, CheckpointError> {
        self.checkpoints().get(&id).cloned().ok_or_else(|| {
            CheckpointError::new(
                CheckpointErrorKind::NotFound,
                format!("checkpoint `{id}` does not exist"),
            )
        })
    }

    fn compare_and_swap(
        &self,
        checkpoint: &Checkpoint,
        expected_revision: Option<u64>,
    ) -> Result<(), CheckpointError> {
        let mut checkpoints = self.checkpoints();
        let current = checkpoints.get(&checkpoint.id);
        match (current, expected_revision) {
            (None, None) if checkpoint.revision == 0 => {}
            (Some(current), Some(expected))
                if current.revision == expected
                    && expected
                        .checked_add(1)
                        .is_some_and(|next| checkpoint.revision == next) => {}
            (None, Some(_)) => {
                return Err(CheckpointError::new(
                    CheckpointErrorKind::NotFound,
                    format!("checkpoint `{}` does not exist", checkpoint.id),
                ));
            }
            _ => {
                return Err(CheckpointError::new(
                    CheckpointErrorKind::Conflict,
                    format!(
                        "checkpoint `{}` revision precondition failed",
                        checkpoint.id
                    ),
                ));
            }
        }
        checkpoints.insert(checkpoint.id, checkpoint.clone());
        Ok(())
    }
}

/// Normalized checkpoint storage failure category.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CheckpointErrorKind {
    /// The requested checkpoint does not exist.
    NotFound,
    /// A create or revision precondition failed.
    Conflict,
    /// Serialized state violated its domain schema.
    InvalidPayload,
    /// The backing store failed.
    Storage,
}

/// Structured checkpoint failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{kind:?}: {message}")]
pub struct CheckpointError {
    /// Normalized category.
    pub kind: CheckpointErrorKind,
    /// Safe failure explanation.
    pub message: String,
}

impl CheckpointError {
    /// Creates a checkpoint error.
    pub fn new(kind: CheckpointErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{Checkpoint, CheckpointErrorKind, CheckpointStore, InMemoryCheckpointStore};
    use crate::{CheckpointId, RunId};

    #[test]
    fn compare_and_swap_rejects_stale_writers() {
        let store = InMemoryCheckpointStore::new();
        let first = Checkpoint::initial(
            CheckpointId::new(),
            RunId::new(),
            "test",
            1,
            json!({"value": 1}),
        );
        store.compare_and_swap(&first, None).unwrap();
        let second = first.next(json!({"value": 2})).unwrap();
        store.compare_and_swap(&second, Some(0)).unwrap();
        let stale = first.next(json!({"value": 3})).unwrap();

        let error = store.compare_and_swap(&stale, Some(0)).unwrap_err();

        assert_eq!(error.kind, CheckpointErrorKind::Conflict);
        assert_eq!(store.load(first.id).unwrap(), second);
    }
}
