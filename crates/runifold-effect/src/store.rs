use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
};

use runifold_core::{CapabilityId, EffectId};

use crate::{EffectExecutorError, EffectExecutorErrorKind, EffectRecord};

/// Atomic persistence boundary for write-ahead effect records.
pub trait EffectStore: Send + Sync {
    /// Loads a record by effect identity.
    ///
    /// # Errors
    ///
    /// Returns [`EffectExecutorError`] on storage failure.
    fn load(&self, id: EffectId) -> Result<Option<EffectRecord>, EffectExecutorError>;

    /// Resolves a logical effect by capability-scoped idempotency key.
    ///
    /// # Errors
    ///
    /// Returns [`EffectExecutorError`] on storage failure.
    fn find_by_idempotency(
        &self,
        capability_id: CapabilityId,
        key: &str,
    ) -> Result<Option<EffectRecord>, EffectExecutorError>;

    /// Creates or atomically replaces a record.
    ///
    /// `None` is create-only. Updates require the exact current revision and
    /// a new revision exactly one greater.
    ///
    /// # Errors
    ///
    /// Returns [`EffectExecutorError`] on storage failure or conflict.
    fn compare_and_swap(
        &self,
        record: &EffectRecord,
        expected_revision: Option<u64>,
    ) -> Result<(), EffectExecutorError>;
}

/// In-memory effect store with atomic idempotency indexing.
#[derive(Clone, Debug, Default)]
pub struct InMemoryEffectStore {
    state: Arc<Mutex<StoreState>>,
}

#[derive(Debug, Default)]
struct StoreState {
    records: BTreeMap<EffectId, EffectRecord>,
    idempotency: BTreeMap<(CapabilityId, String), EffectId>,
}

impl InMemoryEffectStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    fn state(&self) -> MutexGuard<'_, StoreState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl EffectStore for InMemoryEffectStore {
    fn load(&self, id: EffectId) -> Result<Option<EffectRecord>, EffectExecutorError> {
        Ok(self.state().records.get(&id).cloned())
    }

    fn find_by_idempotency(
        &self,
        capability_id: CapabilityId,
        key: &str,
    ) -> Result<Option<EffectRecord>, EffectExecutorError> {
        let state = self.state();
        Ok(state
            .idempotency
            .get(&(capability_id, key.into()))
            .and_then(|id| state.records.get(id))
            .cloned())
    }

    fn compare_and_swap(
        &self,
        record: &EffectRecord,
        expected_revision: Option<u64>,
    ) -> Result<(), EffectExecutorError> {
        let mut state = self.state();
        let current = state.records.get(&record.request.effect_id);
        let valid = match (current, expected_revision) {
            (None, None) => record.revision == 0,
            (Some(current), Some(expected)) => {
                current.revision == expected
                    && expected
                        .checked_add(1)
                        .is_some_and(|next| record.revision == next)
            }
            _ => false,
        };
        if !valid {
            return Err(EffectExecutorError::new(
                EffectExecutorErrorKind::Store,
                "effect record revision precondition failed",
            ));
        }

        if let Some(key) = &record.request.idempotency_key {
            let index = (record.request.capability_id, key.clone());
            if let Some(existing) = state.idempotency.get(&index)
                && *existing != record.request.effect_id
            {
                return Err(EffectExecutorError::new(
                    EffectExecutorErrorKind::IdempotencyConflict,
                    "idempotency key already belongs to another effect",
                ));
            }
            state.idempotency.insert(index, record.request.effect_id);
        }
        state
            .records
            .insert(record.request.effect_id, record.clone());
        Ok(())
    }
}
