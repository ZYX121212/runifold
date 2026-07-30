use super::{
    BTreeMap, CheckpointId, InMemoryWorkflowStore, StoredSignal, StoredState, WorkflowSignal,
    WorkflowSignalId, WorkflowSignalOutcome, WorkflowSignalRetention, WorkflowSignalSnapshot,
    WorkflowSignalState, WorkflowStoreError, WorkflowStoreErrorKind, WorkflowStoreFuture,
    WorkflowTenantId, WorkflowWake, require_tenant,
};

pub(super) fn take_buffered_signal(
    signals: &mut BTreeMap<WorkflowSignalId, StoredSignal>,
    checkpoint_id: CheckpointId,
    name: &crate::WorkflowSignalName,
) -> Option<WorkflowWake> {
    signals
        .values_mut()
        .find(|stored_signal| {
            !stored_signal.consumed
                && !stored_signal.dead_lettered
                && stored_signal.signal.checkpoint_id == checkpoint_id
                && stored_signal.signal.name == *name
        })
        .map(|stored_signal| {
            stored_signal.consumed = true;
            WorkflowWake::Signal {
                signal_id: stored_signal.signal.signal_id,
                name: stored_signal.signal.name.clone(),
                payload: stored_signal.signal.payload.clone(),
            }
        })
}

pub(super) fn signal_snapshot(stored: &StoredSignal) -> WorkflowSignalSnapshot {
    let state = if stored.consumed {
        WorkflowSignalState::Consumed
    } else if stored.dead_lettered {
        WorkflowSignalState::DeadLettered
    } else {
        WorkflowSignalState::Pending
    };
    WorkflowSignalSnapshot {
        signal_id: stored.signal.signal_id,
        tenant_id: stored.tenant_id.clone(),
        checkpoint_id: stored.signal.checkpoint_id,
        name: stored.signal.name.clone(),
        state,
        accepted_at_ms: stored.accepted_at_ms,
    }
}

impl InMemoryWorkflowStore {
    pub(super) fn publish_signal_impl(
        &self,
        tenant_id: WorkflowTenantId,
        signal: WorkflowSignal,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowSignalOutcome, WorkflowStoreError>> {
        Box::pin(async move {
            let now = self.clock.now_ms();
            let mut tasks = self.tasks();
            let stored = tasks.get_mut(&signal.checkpoint_id).ok_or_else(|| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::NotFound,
                    format!("workflow task `{}` does not exist", signal.checkpoint_id),
                )
            })?;
            require_tenant(&stored.task.tenant_id, &tenant_id)?;
            let mut signals = self
                .signals
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(existing) = signals.get(&signal.signal_id) {
                return if existing.signal == signal {
                    Ok(WorkflowSignalOutcome::Duplicate)
                } else {
                    Err(WorkflowStoreError::new(
                        WorkflowStoreErrorKind::Conflict,
                        "workflow signal identity is already bound to different content",
                    ))
                };
            }
            let wakes = match &stored.state {
                StoredState::WaitingSignal { name } => *name == signal.name,
                StoredState::WaitingSignalOrTimeout { name, wake_at_ms } => {
                    *name == signal.name && now < *wake_at_ms
                }
                StoredState::WaitingInterrupt { request } => request.signal_name() == signal.name,
                _ => false,
            };
            let dead_lettered = matches!(
                stored.state,
                StoredState::Completed | StoredState::Failed(_) | StoredState::Cancelled
            ) || matches!(
                &stored.state,
                StoredState::WaitingSignalOrTimeout { name, wake_at_ms }
                    if *name == signal.name && now >= *wake_at_ms
            );
            let wake = WorkflowWake::Signal {
                signal_id: signal.signal_id,
                name: signal.name.clone(),
                payload: signal.payload.clone(),
            };
            signals.insert(
                signal.signal_id,
                StoredSignal {
                    tenant_id,
                    signal,
                    consumed: wakes,
                    dead_lettered,
                    accepted_at_ms: now,
                },
            );
            if wakes {
                stored.wake = Some(wake);
                stored.state = StoredState::Queued {
                    available_at_ms: now,
                };
                stored.updated_at_ms = now;
                Ok(WorkflowSignalOutcome::WokeWorkflow)
            } else if dead_lettered {
                Ok(WorkflowSignalOutcome::DeadLettered)
            } else {
                Ok(WorkflowSignalOutcome::Buffered)
            }
        })
    }

    pub(super) fn inspect_signal_impl(
        &self,
        tenant_id: WorkflowTenantId,
        signal_id: WorkflowSignalId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowSignalSnapshot, WorkflowStoreError>> {
        Box::pin(async move {
            let signals = self
                .signals
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let stored = signals.get(&signal_id).ok_or_else(|| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::NotFound,
                    format!("workflow signal `{signal_id:?}` does not exist"),
                )
            })?;
            require_tenant(&stored.tenant_id, &tenant_id)?;
            Ok(signal_snapshot(stored))
        })
    }

    pub(super) fn compact_signals_impl(
        &self,
        tenant_id: WorkflowTenantId,
        retention: WorkflowSignalRetention,
    ) -> WorkflowStoreFuture<'_, Result<u64, WorkflowStoreError>> {
        Box::pin(async move {
            let Some(cutoff) = self.clock.now_ms().checked_sub(retention.as_millis()) else {
                return Ok(0);
            };
            let mut signals = self
                .signals
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let before = signals.len();
            signals.retain(|_, stored| {
                stored.tenant_id != tenant_id
                    || stored.accepted_at_ms > cutoff
                    || (!stored.consumed && !stored.dead_lettered)
            });
            u64::try_from(before.saturating_sub(signals.len())).map_err(|_| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::Storage,
                    "compacted workflow signal count overflowed",
                )
            })
        })
    }
}
