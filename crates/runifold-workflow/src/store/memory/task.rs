use super::budget::{forfeit_budget_reservation, maintain_budget_ledger};
use super::{
    BTreeMap, CheckpointId, ClaimedWorkflow, Duration, InMemoryWorkflowStore, LeaseDuration,
    Reverse, StoredState, StoredTask, StoredTenant, WorkerId, WorkflowBudgetForfeitReason,
    WorkflowCancelOutcome, WorkflowDisposition, WorkflowLease, WorkflowStoreError,
    WorkflowStoreErrorKind, WorkflowStoreFuture, WorkflowTask, WorkflowTaskSnapshot,
    WorkflowTaskStatus, WorkflowTenantId, WorkflowTenantPolicy, WorkflowWait, WorkflowWake,
};

pub(super) fn eligibility(state: &StoredState, now: u64) -> Option<u64> {
    match state {
        StoredState::Queued { available_at_ms } if *available_at_ms <= now => {
            Some(*available_at_ms)
        }
        StoredState::Leased(lease) if lease.expires_at_ms <= now => Some(lease.expires_at_ms),
        StoredState::WaitingTimer { wake_at_ms } if *wake_at_ms <= now => Some(*wake_at_ms),
        StoredState::WaitingSignalOrTimeout { wake_at_ms, .. } if *wake_at_ms <= now => {
            Some(*wake_at_ms)
        }
        _ => None,
    }
}

pub(super) fn is_non_terminal(state: &StoredState) -> bool {
    !matches!(
        state,
        StoredState::Completed | StoredState::Failed(_) | StoredState::Cancelled
    )
}

pub(super) fn workflow_not_found(checkpoint_id: CheckpointId) -> WorkflowStoreError {
    WorkflowStoreError::new(
        WorkflowStoreErrorKind::NotFound,
        format!("workflow task `{checkpoint_id}` does not exist"),
    )
}

pub(super) fn require_current_lease<'a>(
    stored: &'a StoredTask,
    supplied: &WorkflowLease,
    now: u64,
) -> Result<&'a WorkflowLease, WorkflowStoreError> {
    let StoredState::Leased(current) = &stored.state else {
        return Err(lease_lost());
    };
    if current.worker != supplied.worker
        || current.tenant_id != supplied.tenant_id
        || current.fencing_token != supplied.fencing_token
        || current.expires_at_ms <= now
    {
        return Err(lease_lost());
    }
    Ok(current)
}

pub(super) fn require_tenant(
    actual: &WorkflowTenantId,
    supplied: &WorkflowTenantId,
) -> Result<(), WorkflowStoreError> {
    if actual == supplied {
        Ok(())
    } else {
        Err(WorkflowStoreError::new(
            WorkflowStoreErrorKind::TenantMismatch,
            "workflow resource does not belong to the supplied tenant",
        ))
    }
}

pub(super) fn lease_lost() -> WorkflowStoreError {
    WorkflowStoreError::new(
        WorkflowStoreErrorKind::LeaseLost,
        "workflow lease is expired, superseded, or owned by another worker",
    )
}

pub(super) fn validate_disposition(
    disposition: &WorkflowDisposition,
) -> Result<(), WorkflowStoreError> {
    if let WorkflowDisposition::Failed(reason) = disposition {
        if reason.trim().is_empty() || reason.len() > 1_024 {
            return Err(WorkflowStoreError::invalid_input(
                "workflow failure reason must contain 1..=1024 bytes",
            ));
        }
    }
    if let WorkflowDisposition::RetryAfter(delay) = disposition {
        duration_millis(*delay)?;
    }
    if matches!(
        disposition,
        WorkflowDisposition::Suspend(
            WorkflowWait::Timer { delay_ms: 0 }
                | WorkflowWait::SignalOrTimeout { timeout_ms: 0, .. }
        )
    ) {
        return Err(WorkflowStoreError::invalid_input(
            "durable timer delay must be positive",
        ));
    }
    Ok(())
}

pub(super) fn duration_millis(duration: Duration) -> Result<u64, WorkflowStoreError> {
    u64::try_from(duration.as_millis()).map_err(|_| {
        WorkflowStoreError::invalid_input("workflow delay exceeds the supported millisecond range")
    })
}

pub(super) fn snapshot(stored: &StoredTask) -> WorkflowTaskSnapshot {
    let (status, owner, lease_expires_at_ms) = match &stored.state {
        StoredState::Queued { .. } => (WorkflowTaskStatus::Queued, None, None),
        StoredState::Leased(lease) => (
            WorkflowTaskStatus::Leased,
            Some(lease.worker.clone()),
            Some(lease.expires_at_ms),
        ),
        StoredState::WaitingTimer { .. }
        | StoredState::WaitingSignal { .. }
        | StoredState::WaitingSignalOrTimeout { .. }
        | StoredState::WaitingInterrupt { .. } => (WorkflowTaskStatus::Waiting, None, None),
        StoredState::Completed => (WorkflowTaskStatus::Completed, None, None),
        StoredState::Failed(_) => (WorkflowTaskStatus::Failed, None, None),
        StoredState::Cancelled => (WorkflowTaskStatus::Cancelled, None, None),
    };
    let interrupt = match &stored.state {
        StoredState::WaitingInterrupt { request } => Some(request.clone()),
        _ => None,
    };
    let failure_message = match &stored.state {
        StoredState::Failed(message) => Some(message.clone()),
        _ => None,
    };
    WorkflowTaskSnapshot {
        checkpoint_id: stored.task.checkpoint_id,
        tenant_id: stored.task.tenant_id.clone(),
        workflow: stored.task.workflow.clone(),
        workflow_version: stored.task.workflow_version,
        status,
        created_at_ms: stored.created_at_ms,
        updated_at_ms: stored.updated_at_ms,
        attempts: stored.attempts,
        fencing_token: stored.fencing_token,
        owner,
        lease_expires_at_ms,
        interrupt,
        failure_message,
        lineage: stored.lineage.clone(),
    }
}

impl InMemoryWorkflowStore {
    pub(super) fn set_tenant_policy_impl(
        &self,
        tenant_id: WorkflowTenantId,
        policy: WorkflowTenantPolicy,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        Box::pin(async move {
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            admission
                .tenants
                .entry(tenant_id)
                .and_modify(|tenant| tenant.policy = policy)
                .or_insert(StoredTenant {
                    policy,
                    last_claim_sequence: 0,
                });
            Ok(())
        })
    }

    pub(super) fn enqueue_impl(
        &self,
        task: WorkflowTask,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        Box::pin(async move {
            task.validate()?;
            let now = self.clock.now_ms();
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut tasks = self.tasks();
            if tasks.contains_key(&task.checkpoint_id) {
                return Err(WorkflowStoreError::new(
                    WorkflowStoreErrorKind::Conflict,
                    format!("workflow task `{}` already exists", task.checkpoint_id),
                ));
            }
            let tenant = admission
                .tenants
                .entry(task.tenant_id.clone())
                .or_insert(StoredTenant {
                    policy: WorkflowTenantPolicy::default(),
                    last_claim_sequence: 0,
                });
            let outstanding = tasks
                .values()
                .filter(|stored| {
                    stored.task.tenant_id == task.tenant_id && is_non_terminal(&stored.state)
                })
                .count();
            let limit =
                usize::try_from(tenant.policy.max_outstanding_tasks()).unwrap_or(usize::MAX);
            if outstanding >= limit {
                return Err(WorkflowStoreError::new(
                    WorkflowStoreErrorKind::AdmissionDenied,
                    format!(
                        "workflow tenant `{}` reached its outstanding task limit",
                        task.tenant_id.as_str()
                    ),
                ));
            }
            tasks.insert(
                task.checkpoint_id,
                StoredTask {
                    task,
                    state: StoredState::Queued {
                        available_at_ms: now,
                    },
                    attempts: 0,
                    fencing_token: 0,
                    wake: None,
                    lineage: None,
                    created_at_ms: now,
                    updated_at_ms: now,
                },
            );
            Ok(())
        })
    }

    pub(super) fn claim_impl(
        &self,
        worker: WorkerId,
        lease: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<Option<ClaimedWorkflow>, WorkflowStoreError>> {
        Box::pin(async move {
            let now = self.clock.now_ms();
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut tasks = self.tasks();
            let mut active_leases = BTreeMap::<WorkflowTenantId, usize>::new();
            for stored in tasks.values() {
                if matches!(
                    &stored.state,
                    StoredState::Leased(current) if current.expires_at_ms > now
                ) {
                    *active_leases
                        .entry(stored.task.tenant_id.clone())
                        .or_default() += 1;
                }
            }
            let candidate = tasks
                .iter()
                .filter_map(|(id, stored)| {
                    let tenant = admission.tenants.get(&stored.task.tenant_id)?;
                    let lease_limit = usize::try_from(tenant.policy.max_concurrent_leases())
                        .unwrap_or(usize::MAX);
                    if active_leases
                        .get(&stored.task.tenant_id)
                        .copied()
                        .unwrap_or_default()
                        >= lease_limit
                    {
                        return None;
                    }
                    eligibility(&stored.state, now).map(|available| {
                        (
                            tenant.last_claim_sequence,
                            Reverse(stored.task.priority),
                            available,
                            *id,
                        )
                    })
                })
                .min()
                .map(|(_, _, _, id)| id);
            let Some(id) = candidate else {
                return Ok(None);
            };
            let stored = tasks
                .get_mut(&id)
                .expect("selected workflow task remains present under the queue lock");
            admission.next_claim_sequence = admission
                .next_claim_sequence
                .checked_add(1)
                .ok_or_else(|| {
                    WorkflowStoreError::new(
                        WorkflowStoreErrorKind::Storage,
                        "workflow tenant claim sequence overflow",
                    )
                })?;
            let claim_sequence = admission.next_claim_sequence;
            admission
                .tenants
                .get_mut(&stored.task.tenant_id)
                .expect("enqueued workflow tenant remains registered")
                .last_claim_sequence = claim_sequence;
            stored.fencing_token = stored.fencing_token.checked_add(1).ok_or_else(|| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::Storage,
                    "workflow fencing token overflow",
                )
            })?;
            if matches!(stored.state, StoredState::WaitingTimer { .. }) {
                stored.wake = Some(WorkflowWake::Timer);
            } else if matches!(stored.state, StoredState::WaitingSignalOrTimeout { .. }) {
                stored.wake = Some(WorkflowWake::Timeout);
            }
            stored.attempts = stored.attempts.checked_add(1).ok_or_else(|| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::Storage,
                    "workflow attempt counter overflow",
                )
            })?;
            let lease = WorkflowLease {
                checkpoint_id: id,
                tenant_id: stored.task.tenant_id.clone(),
                worker,
                fencing_token: stored.fencing_token,
                attempt: stored.attempts,
                expires_at_ms: now.saturating_add(lease.as_millis()),
            };
            stored.state = StoredState::Leased(lease.clone());
            stored.updated_at_ms = now;
            Ok(Some(ClaimedWorkflow {
                task: stored.task.clone(),
                lease,
                wake: stored.wake.clone(),
            }))
        })
    }

    pub(super) fn heartbeat_impl(
        &self,
        lease: WorkflowLease,
        extension: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowLease, WorkflowStoreError>> {
        Box::pin(async move {
            let now = self.clock.now_ms();
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut tasks = self.tasks();
            let stored = tasks.get_mut(&lease.checkpoint_id).ok_or_else(|| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::NotFound,
                    format!("workflow task `{}` does not exist", lease.checkpoint_id),
                )
            })?;
            let current = require_current_lease(stored, &lease, now)?;
            let renewed = WorkflowLease {
                expires_at_ms: current
                    .expires_at_ms
                    .max(now.saturating_add(extension.as_millis())),
                ..current.clone()
            };
            stored.state = StoredState::Leased(renewed.clone());
            stored.updated_at_ms = now;
            if let Some(ledger) = admission.budgets.get_mut(&lease.tenant_id) {
                maintain_budget_ledger(ledger, now)?;
                if let Some(reservation) = ledger.reservations.get_mut(&lease.checkpoint_id) {
                    reservation.expires_at_ms = renewed
                        .expires_at_ms
                        .saturating_add(ledger.policy.recovery_grace_millis());
                }
            }
            Ok(renewed)
        })
    }

    pub(super) fn finish_impl(
        &self,
        lease: WorkflowLease,
        disposition: WorkflowDisposition,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        Box::pin(async move {
            validate_disposition(&disposition)?;
            let now = self.clock.now_ms();
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut tasks = self.tasks();
            let stored = tasks.get_mut(&lease.checkpoint_id).ok_or_else(|| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::NotFound,
                    format!("workflow task `{}` does not exist", lease.checkpoint_id),
                )
            })?;
            require_current_lease(stored, &lease, now)?;
            if let Some(ledger) = admission.budgets.get_mut(&lease.tenant_id) {
                maintain_budget_ledger(ledger, now)?;
                if ledger.reservations.contains_key(&lease.checkpoint_id) {
                    return Err(WorkflowStoreError::new(
                        WorkflowStoreErrorKind::Conflict,
                        "workflow tenant budget reservation must be settled before finish",
                    ));
                }
            }
            stored.state = match disposition {
                WorkflowDisposition::Completed => {
                    stored.wake = None;
                    StoredState::Completed
                }
                WorkflowDisposition::RetryAfter(delay) => StoredState::Queued {
                    available_at_ms: now.saturating_add(duration_millis(delay)?),
                },
                WorkflowDisposition::Suspend(wait) => {
                    self.suspend(stored, lease.checkpoint_id, wait, now)
                }
                WorkflowDisposition::Failed(reason) => {
                    stored.wake = None;
                    StoredState::Failed(reason)
                }
                WorkflowDisposition::Cancelled => {
                    stored.wake = None;
                    StoredState::Cancelled
                }
            };
            stored.updated_at_ms = now;
            Ok(())
        })
    }

    pub(super) fn cancel_impl(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowCancelOutcome, WorkflowStoreError>> {
        Box::pin(async move {
            let now = self.clock.now_ms();
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut tasks = self.tasks();
            let stored = tasks.get_mut(&checkpoint_id).ok_or_else(|| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::NotFound,
                    format!("workflow task `{checkpoint_id}` does not exist"),
                )
            })?;
            require_tenant(&stored.task.tenant_id, &tenant_id)?;
            if matches!(
                stored.state,
                StoredState::Completed | StoredState::Failed(_) | StoredState::Cancelled
            ) {
                return Ok(WorkflowCancelOutcome::AlreadyTerminal);
            }
            stored.state = StoredState::Cancelled;
            stored.updated_at_ms = now;
            stored.wake = None;
            if let Some(ledger) = admission.budgets.get_mut(&tenant_id) {
                maintain_budget_ledger(ledger, now)?;
                forfeit_budget_reservation(
                    ledger,
                    checkpoint_id,
                    now,
                    WorkflowBudgetForfeitReason::Cancelled,
                )?;
            }
            let mut signals = self
                .signals
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for stored_signal in signals.values_mut().filter(|stored_signal| {
                stored_signal.signal.checkpoint_id == checkpoint_id
                    && !stored_signal.consumed
                    && !stored_signal.dead_lettered
            }) {
                stored_signal.dead_lettered = true;
            }
            Ok(WorkflowCancelOutcome::Cancelled)
        })
    }

    pub(super) fn inspect_impl(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskSnapshot, WorkflowStoreError>> {
        Box::pin(async move {
            let tasks = self.tasks();
            let stored = tasks.get(&checkpoint_id).ok_or_else(|| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::NotFound,
                    format!("workflow task `{checkpoint_id}` does not exist"),
                )
            })?;
            require_tenant(&stored.task.tenant_id, &tenant_id)?;
            Ok(snapshot(stored))
        })
    }
}
