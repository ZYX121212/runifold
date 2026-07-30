use super::{
    Arc, BTreeMap, Budget, Checkpoint, CheckpointError, CheckpointErrorKind, CheckpointId,
    ClaimedWorkflow, Duration, LeaseDuration, Mutex, MutexGuard, Reverse, SystemWorkflowClock,
    Usage, WorkerId, WorkflowBudgetAuditCursor, WorkflowBudgetAuditEvent, WorkflowBudgetAuditKind,
    WorkflowBudgetAuditLimit, WorkflowBudgetAuditProjectionId, WorkflowBudgetAuditProjectionLease,
    WorkflowBudgetForfeitReason, WorkflowBudgetReservationOutcome, WorkflowCancelOutcome,
    WorkflowCheckpointHistoryLimit, WorkflowCheckpointPhase, WorkflowCheckpointRevision,
    WorkflowClock, WorkflowDisposition, WorkflowForkCommand, WorkflowForkOutcome,
    WorkflowInterruptRequest, WorkflowLease, WorkflowLineage, WorkflowSignal, WorkflowSignalId,
    WorkflowSignalOutcome, WorkflowSignalRetention, WorkflowSignalSnapshot, WorkflowSignalState,
    WorkflowStore, WorkflowStoreError, WorkflowStoreErrorKind, WorkflowStoreFuture, WorkflowTask,
    WorkflowTaskSnapshot, WorkflowTaskStatus, WorkflowTenantBudgetPolicy,
    WorkflowTenantBudgetSnapshot, WorkflowTenantId, WorkflowTenantListLimit, WorkflowTenantPolicy,
    WorkflowWait, WorkflowWake, decode_revision, fork_checkpoint,
};

mod budget;
mod checkpoint;
mod signal;
mod task;

use signal::take_buffered_signal;
use task::{is_non_terminal, require_current_lease, require_tenant, workflow_not_found};

/// Deterministic in-memory implementation of the distributed store contract.
#[derive(Clone)]
pub struct InMemoryWorkflowStore {
    tasks: Arc<Mutex<BTreeMap<CheckpointId, StoredTask>>>,
    checkpoints: Arc<Mutex<StoredCheckpoints>>,
    signals: Arc<Mutex<BTreeMap<WorkflowSignalId, StoredSignal>>>,
    admission: Arc<Mutex<AdmissionState>>,
    clock: Arc<dyn WorkflowClock>,
}

impl Default for InMemoryWorkflowStore {
    fn default() -> Self {
        Self::with_clock(Arc::new(SystemWorkflowClock))
    }
}

impl InMemoryWorkflowStore {
    /// Creates a store backed by the system clock.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a store with an explicit authoritative clock.
    pub fn with_clock(clock: Arc<dyn WorkflowClock>) -> Self {
        Self {
            tasks: Arc::new(Mutex::new(BTreeMap::new())),
            checkpoints: Arc::new(Mutex::new(StoredCheckpoints::default())),
            signals: Arc::new(Mutex::new(BTreeMap::new())),
            admission: Arc::new(Mutex::new(AdmissionState::default())),
            clock,
        }
    }

    fn tasks(&self) -> MutexGuard<'_, BTreeMap<CheckpointId, StoredTask>> {
        self.tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl std::fmt::Debug for InMemoryWorkflowStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InMemoryWorkflowStore")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
struct AdmissionState {
    tenants: BTreeMap<WorkflowTenantId, StoredTenant>,
    budgets: BTreeMap<WorkflowTenantId, StoredTenantBudget>,
    budget_audit_projections:
        BTreeMap<(WorkflowTenantId, WorkflowBudgetAuditProjectionId), StoredBudgetAuditProjection>,
    next_claim_sequence: u64,
}

#[derive(Clone, Copy, Debug)]
struct StoredTenant {
    policy: WorkflowTenantPolicy,
    last_claim_sequence: u64,
}

#[derive(Clone, Debug)]
struct StoredTenantBudget {
    policy: WorkflowTenantBudgetPolicy,
    window_started_at_ms: u64,
    committed: Usage,
    reserved: Usage,
    reservations: BTreeMap<CheckpointId, StoredBudgetReservation>,
    next_audit_sequence: u64,
    audit_events: Vec<StoredBudgetAuditEvent>,
}

#[derive(Clone, Copy, Debug)]
struct StoredBudgetReservation {
    baseline: Usage,
    amount: Usage,
    reserved_at_ms: u64,
    expires_at_ms: u64,
}

#[derive(Clone, Copy, Debug)]
struct StoredBudgetAuditEvent {
    cursor: WorkflowBudgetAuditCursor,
    checkpoint_id: Option<CheckpointId>,
    occurred_at_ms: u64,
    kind: WorkflowBudgetAuditKind,
    usage: Usage,
    reservation_age_ms: Option<u64>,
    limit: Budget,
    committed: Usage,
    reserved: Usage,
}

#[derive(Clone, Debug, Default)]
struct StoredBudgetAuditProjection {
    cursor: WorkflowBudgetAuditCursor,
    owner: Option<WorkerId>,
    fencing_token: u64,
    expires_at_ms: Option<u64>,
}

#[derive(Clone, Debug)]
struct StoredTask {
    task: WorkflowTask,
    state: StoredState,
    attempts: u64,
    fencing_token: u64,
    wake: Option<WorkflowWake>,
    lineage: Option<WorkflowLineage>,
    created_at_ms: u64,
    updated_at_ms: u64,
}

#[derive(Debug, Default)]
struct StoredCheckpoints {
    latest: BTreeMap<CheckpointId, Checkpoint>,
    history: BTreeMap<(CheckpointId, u64), Checkpoint>,
}

#[derive(Clone, Debug)]
enum StoredState {
    Queued {
        available_at_ms: u64,
    },
    Leased(WorkflowLease),
    WaitingTimer {
        wake_at_ms: u64,
    },
    WaitingSignal {
        name: crate::WorkflowSignalName,
    },
    WaitingSignalOrTimeout {
        name: crate::WorkflowSignalName,
        wake_at_ms: u64,
    },
    WaitingInterrupt {
        request: WorkflowInterruptRequest,
    },
    Completed,
    Failed(String),
    Cancelled,
}

#[derive(Clone, Debug)]
struct StoredSignal {
    tenant_id: WorkflowTenantId,
    signal: WorkflowSignal,
    consumed: bool,
    dead_lettered: bool,
    accepted_at_ms: u64,
}

impl InMemoryWorkflowStore {
    fn suspend(
        &self,
        stored: &mut StoredTask,
        checkpoint_id: CheckpointId,
        wait: WorkflowWait,
        now: u64,
    ) -> StoredState {
        match wait {
            WorkflowWait::Timer { delay_ms } => {
                stored.wake = None;
                StoredState::WaitingTimer {
                    wake_at_ms: now.saturating_add(delay_ms),
                }
            }
            WorkflowWait::Signal { name } => {
                self.suspend_signal(stored, checkpoint_id, name, None, now)
            }
            WorkflowWait::SignalOrTimeout { name, timeout_ms } => {
                self.suspend_signal(stored, checkpoint_id, name, Some(timeout_ms), now)
            }
            WorkflowWait::Interrupt { request } => {
                let name = request.signal_name();
                let mut signals = self
                    .signals
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(wake) = take_buffered_signal(&mut signals, checkpoint_id, &name) {
                    stored.wake = Some(wake);
                    StoredState::Queued {
                        available_at_ms: now,
                    }
                } else {
                    stored.wake = None;
                    StoredState::WaitingInterrupt { request }
                }
            }
        }
    }

    fn suspend_signal(
        &self,
        stored: &mut StoredTask,
        checkpoint_id: CheckpointId,
        name: crate::WorkflowSignalName,
        timeout_ms: Option<u64>,
        now: u64,
    ) -> StoredState {
        let mut signals = self
            .signals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(wake) = take_buffered_signal(&mut signals, checkpoint_id, &name) {
            stored.wake = Some(wake);
            return StoredState::Queued {
                available_at_ms: now,
            };
        }
        stored.wake = None;
        match timeout_ms {
            Some(timeout_ms) => StoredState::WaitingSignalOrTimeout {
                name,
                wake_at_ms: now.saturating_add(timeout_ms),
            },
            None => StoredState::WaitingSignal { name },
        }
    }
}

impl WorkflowStore for InMemoryWorkflowStore {
    fn set_tenant_policy(
        &self,
        tenant_id: WorkflowTenantId,
        policy: WorkflowTenantPolicy,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        self.set_tenant_policy_impl(tenant_id, policy)
    }

    fn set_tenant_budget_policy(
        &self,
        tenant_id: WorkflowTenantId,
        policy: WorkflowTenantBudgetPolicy,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        self.set_tenant_budget_policy_impl(tenant_id, policy)
    }

    fn list_tenant_budgets(
        &self,
        after: Option<WorkflowTenantId>,
        limit: WorkflowTenantListLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowTenantId>, WorkflowStoreError>> {
        self.list_tenant_budgets_impl(after, limit)
    }

    fn inspect_tenant_budget(
        &self,
        tenant_id: WorkflowTenantId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTenantBudgetSnapshot, WorkflowStoreError>> {
        self.inspect_tenant_budget_impl(tenant_id)
    }

    fn list_tenant_budget_audit(
        &self,
        tenant_id: WorkflowTenantId,
        after: Option<WorkflowBudgetAuditCursor>,
        limit: WorkflowBudgetAuditLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowBudgetAuditEvent>, WorkflowStoreError>> {
        self.list_tenant_budget_audit_impl(tenant_id, after, limit)
    }

    fn compact_tenant_budget_audit(
        &self,
        tenant_id: WorkflowTenantId,
        through: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<u64, WorkflowStoreError>> {
        self.compact_tenant_budget_audit_impl(tenant_id, through)
    }

    fn load_or_create_tenant_budget_audit_projection(
        &self,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditCursor, WorkflowStoreError>> {
        self.load_or_create_tenant_budget_audit_projection_impl(tenant_id, projection_id)
    }

    fn advance_tenant_budget_audit_projection(
        &self,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
        expected: WorkflowBudgetAuditCursor,
        next: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<bool, WorkflowStoreError>> {
        self.advance_tenant_budget_audit_projection_impl(tenant_id, projection_id, expected, next)
    }

    fn claim_tenant_budget_audit_projection(
        &self,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
        owner: WorkerId,
        lease: LeaseDuration,
    ) -> WorkflowStoreFuture<
        '_,
        Result<Option<WorkflowBudgetAuditProjectionLease>, WorkflowStoreError>,
    > {
        self.claim_tenant_budget_audit_projection_impl(tenant_id, projection_id, owner, lease)
    }

    fn heartbeat_tenant_budget_audit_projection(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
        extension: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditProjectionLease, WorkflowStoreError>>
    {
        self.heartbeat_tenant_budget_audit_projection_impl(lease, extension)
    }

    fn advance_tenant_budget_audit_projection_lease(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
        next: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditProjectionLease, WorkflowStoreError>>
    {
        self.advance_tenant_budget_audit_projection_lease_impl(lease, next)
    }

    fn release_tenant_budget_audit_projection(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        self.release_tenant_budget_audit_projection_impl(lease)
    }

    fn reserve_budget(
        &self,
        lease: WorkflowLease,
        workflow_limit: Budget,
        baseline: Usage,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetReservationOutcome, WorkflowStoreError>> {
        self.reserve_budget_impl(lease, workflow_limit, baseline)
    }

    fn settle_budget(
        &self,
        lease: WorkflowLease,
        cumulative: Usage,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        self.settle_budget_impl(lease, cumulative)
    }

    fn enqueue(
        &self,
        task: WorkflowTask,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        self.enqueue_impl(task)
    }

    fn claim(
        &self,
        worker: WorkerId,
        lease: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<Option<ClaimedWorkflow>, WorkflowStoreError>> {
        self.claim_impl(worker, lease)
    }

    fn heartbeat(
        &self,
        lease: WorkflowLease,
        extension: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowLease, WorkflowStoreError>> {
        self.heartbeat_impl(lease, extension)
    }

    fn finish(
        &self,
        lease: WorkflowLease,
        disposition: WorkflowDisposition,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        self.finish_impl(lease, disposition)
    }

    fn publish_signal(
        &self,
        tenant_id: WorkflowTenantId,
        signal: WorkflowSignal,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowSignalOutcome, WorkflowStoreError>> {
        self.publish_signal_impl(tenant_id, signal)
    }

    fn cancel(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowCancelOutcome, WorkflowStoreError>> {
        self.cancel_impl(tenant_id, checkpoint_id)
    }

    fn inspect_signal(
        &self,
        tenant_id: WorkflowTenantId,
        signal_id: WorkflowSignalId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowSignalSnapshot, WorkflowStoreError>> {
        self.inspect_signal_impl(tenant_id, signal_id)
    }

    fn compact_signals(
        &self,
        tenant_id: WorkflowTenantId,
        retention: WorkflowSignalRetention,
    ) -> WorkflowStoreFuture<'_, Result<u64, WorkflowStoreError>> {
        self.compact_signals_impl(tenant_id, retention)
    }

    fn inspect(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskSnapshot, WorkflowStoreError>> {
        self.inspect_impl(tenant_id, checkpoint_id)
    }

    fn list_checkpoint_history(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        after_revision: Option<u64>,
        limit: WorkflowCheckpointHistoryLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowCheckpointRevision>, WorkflowStoreError>> {
        self.list_checkpoint_history_impl(tenant_id, checkpoint_id, after_revision, limit)
    }

    fn load_checkpoint_revision(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        revision: u64,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowCheckpointRevision, WorkflowStoreError>> {
        self.load_checkpoint_revision_impl(tenant_id, checkpoint_id, revision)
    }

    fn fork_workflow(
        &self,
        tenant_id: WorkflowTenantId,
        command: WorkflowForkCommand,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowForkOutcome, WorkflowStoreError>> {
        self.fork_workflow_impl(tenant_id, command)
    }

    fn load_checkpoint(
        &self,
        lease: WorkflowLease,
    ) -> WorkflowStoreFuture<'_, Result<Checkpoint, CheckpointError>> {
        self.load_checkpoint_impl(lease)
    }

    fn compare_and_swap_checkpoint(
        &self,
        lease: WorkflowLease,
        checkpoint: Checkpoint,
        expected_revision: Option<u64>,
    ) -> WorkflowStoreFuture<'_, Result<(), CheckpointError>> {
        self.compare_and_swap_checkpoint_impl(lease, checkpoint, expected_revision)
    }
}
