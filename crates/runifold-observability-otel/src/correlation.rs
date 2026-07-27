use std::{
    collections::HashMap,
    sync::{Mutex, MutexGuard},
};

use opentelemetry::Context;
use runifold_core::RunId;

#[derive(Debug, Default)]
pub(crate) struct CorrelationRegistry {
    roots: Mutex<HashMap<RunId, Context>>,
    active: Mutex<HashMap<RunId, Context>>,
    child_parents: Mutex<HashMap<RunId, Context>>,
}

impl CorrelationRegistry {
    pub(crate) fn root(&self, run_id: RunId) -> Option<Context> {
        self.lock_roots().get(&run_id).cloned()
    }

    pub(crate) fn current(&self, run_id: RunId) -> Option<Context> {
        self.lock_active()
            .get(&run_id)
            .cloned()
            .or_else(|| self.root(run_id))
    }

    pub(crate) fn set_active(&self, run_id: RunId, context: Context) {
        self.lock_active().insert(run_id, context);
    }

    pub(crate) fn clear_active(&self, run_id: RunId) {
        self.lock_active().remove(&run_id);
    }

    pub(crate) fn bind_child(&self, child_run_id: RunId, parent: Context) {
        self.lock_child_parents().insert(child_run_id, parent);
    }

    pub(crate) fn take_child_parent(&self, child_run_id: RunId) -> Option<Context> {
        self.lock_child_parents().remove(&child_run_id)
    }

    pub(crate) fn get_or_insert_with(
        &self,
        run_id: RunId,
        create: impl FnOnce() -> Context,
    ) -> Context {
        self.lock_roots()
            .entry(run_id)
            .or_insert_with(create)
            .clone()
    }

    pub(crate) fn remove(&self, run_id: RunId) -> Option<Context> {
        self.clear_active(run_id);
        self.lock_roots().remove(&run_id)
    }

    fn lock_roots(&self) -> MutexGuard<'_, HashMap<RunId, Context>> {
        self.roots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_active(&self) -> MutexGuard<'_, HashMap<RunId, Context>> {
        self.active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_child_parents(&self) -> MutexGuard<'_, HashMap<RunId, Context>> {
        self.child_parents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
