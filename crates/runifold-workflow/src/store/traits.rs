use super::{
    Budget, Checkpoint, CheckpointError, CheckpointId, ClaimedWorkflow, LeaseDuration, SystemTime,
    UNIX_EPOCH, Usage, WorkerId, WorkflowBudgetAuditCursor, WorkflowBudgetAuditEvent,
    WorkflowBudgetAuditLimit, WorkflowBudgetAuditProjectionId, WorkflowBudgetAuditProjectionLease,
    WorkflowBudgetReservationOutcome, WorkflowCancelOutcome, WorkflowCheckpointHistoryLimit,
    WorkflowCheckpointRevision, WorkflowDisposition, WorkflowForkCommand, WorkflowForkOutcome,
    WorkflowInterruptCommand, WorkflowInterruptDecisionOutcome, WorkflowLease, WorkflowSignal,
    WorkflowSignalId, WorkflowSignalOutcome, WorkflowSignalRetention, WorkflowSignalSnapshot,
    WorkflowStoreError, WorkflowStoreFuture, WorkflowTask, WorkflowTaskCleanupLease,
    WorkflowTaskCleanupLimit, WorkflowTaskRetention, WorkflowTaskSnapshot, WorkflowTaskTombstone,
    WorkflowTaskTombstoneCursor, WorkflowTaskTombstoneLimit, WorkflowTenantBudgetPolicy,
    WorkflowTenantBudgetSnapshot, WorkflowTenantId, WorkflowTenantListLimit, WorkflowTenantPolicy,
};

/// Asynchronous distributed workflow task-control boundary.
///
/// Implementations must use a store-authoritative clock for claim expiration.
/// Every ownership-sensitive mutation must compare both worker identity and
/// fencing token.
pub trait WorkflowStore: Send + Sync {
    /// Creates or replaces one tenant's admission policy.
    fn set_tenant_policy(
        &self,
        tenant_id: WorkflowTenantId,
        policy: WorkflowTenantPolicy,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>>;

    /// Creates or replaces one tenant's persistent aggregate budget policy.
    fn set_tenant_budget_policy(
        &self,
        tenant_id: WorkflowTenantId,
        policy: WorkflowTenantBudgetPolicy,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>>;

    /// Discovers budget-enabled tenants in stable identity order.
    fn list_tenant_budgets(
        &self,
        after: Option<WorkflowTenantId>,
        limit: WorkflowTenantListLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowTenantId>, WorkflowStoreError>>;

    /// Reads a tenant budget after reclaiming expired reservations.
    fn inspect_tenant_budget(
        &self,
        tenant_id: WorkflowTenantId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTenantBudgetSnapshot, WorkflowStoreError>>;

    /// Reads durable budget decisions strictly after an optional cursor.
    fn list_tenant_budget_audit(
        &self,
        tenant_id: WorkflowTenantId,
        after: Option<WorkflowBudgetAuditCursor>,
        limit: WorkflowBudgetAuditLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowBudgetAuditEvent>, WorkflowStoreError>>;

    /// Deletes tenant audit facts at or before an explicitly acknowledged cursor.
    fn compact_tenant_budget_audit(
        &self,
        tenant_id: WorkflowTenantId,
        through: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<u64, WorkflowStoreError>>;

    /// Loads or atomically registers one named consumer at cursor zero.
    fn load_or_create_tenant_budget_audit_projection(
        &self,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditCursor, WorkflowStoreError>>;

    /// Monotonically advances a projection cursor using compare-and-set.
    ///
    /// Returns `false` when another projector changed the cursor after
    /// `expected` was loaded.
    fn advance_tenant_budget_audit_projection(
        &self,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
        expected: WorkflowBudgetAuditCursor,
        next: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<bool, WorkflowStoreError>>;

    /// Exclusively claims an idle or expired named audit projection.
    fn claim_tenant_budget_audit_projection(
        &self,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
        owner: WorkerId,
        lease: LeaseDuration,
    ) -> WorkflowStoreFuture<
        '_,
        Result<Option<WorkflowBudgetAuditProjectionLease>, WorkflowStoreError>,
    >;

    /// Extends an active projection lease under its current fencing token.
    fn heartbeat_tenant_budget_audit_projection(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
        extension: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditProjectionLease, WorkflowStoreError>>;

    /// Advances a projection cursor only while its fenced lease remains active.
    fn advance_tenant_budget_audit_projection_lease(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
        next: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditProjectionLease, WorkflowStoreError>>;

    /// Releases a currently fenced projection without changing its cursor.
    fn release_tenant_budget_audit_projection(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>>;

    /// Idempotently reserves the remaining workflow envelope under a lease.
    fn reserve_budget(
        &self,
        lease: WorkflowLease,
        workflow_limit: Budget,
        baseline: Usage,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetReservationOutcome, WorkflowStoreError>>;

    /// Commits observed cumulative usage and releases unused reservation.
    fn settle_budget(
        &self,
        lease: WorkflowLease,
        cumulative: Usage,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>>;

    /// Enqueues a task exactly once.
    fn enqueue(
        &self,
        task: WorkflowTask,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>>;

    /// Atomically claims the highest-priority eligible task.
    fn claim(
        &self,
        worker: WorkerId,
        lease: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<Option<ClaimedWorkflow>, WorkflowStoreError>>;

    /// Extends a currently owned, unexpired lease.
    fn heartbeat(
        &self,
        lease: WorkflowLease,
        extension: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowLease, WorkflowStoreError>>;

    /// Applies a terminal or retry disposition under the current lease.
    fn finish(
        &self,
        lease: WorkflowLease,
        disposition: WorkflowDisposition,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>>;

    /// Idempotently publishes an external signal, buffering it when necessary.
    fn publish_signal(
        &self,
        tenant_id: WorkflowTenantId,
        signal: WorkflowSignal,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowSignalOutcome, WorkflowStoreError>>;

    /// Idempotently applies a typed human decision to a durable interrupt.
    fn decide_interrupt(
        &self,
        tenant_id: WorkflowTenantId,
        command: WorkflowInterruptCommand,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowInterruptDecisionOutcome, WorkflowStoreError>> {
        Box::pin(async move {
            let signal = command
                .into_signal()
                .map_err(|error| WorkflowStoreError::invalid_input(error.to_string()))?;
            self.publish_signal(tenant_id, signal).await.map(Into::into)
        })
    }

    /// Idempotently cancels queued, waiting, or currently leased work.
    fn cancel(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowCancelOutcome, WorkflowStoreError>>;

    /// Loads safe signal lifecycle metadata without exposing its payload.
    fn inspect_signal(
        &self,
        tenant_id: WorkflowTenantId,
        signal_id: WorkflowSignalId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowSignalSnapshot, WorkflowStoreError>>;

    /// Deletes only consumed or dead-letter signals older than retention.
    fn compact_signals(
        &self,
        tenant_id: WorkflowTenantId,
        retention: WorkflowSignalRetention,
    ) -> WorkflowStoreFuture<'_, Result<u64, WorkflowStoreError>>;

    /// Loads safe control-plane state for inspection.
    fn inspect(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskSnapshot, WorkflowStoreError>>;

    /// Lists immutable checkpoint revisions after an optional revision cursor.
    fn list_checkpoint_history(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        after_revision: Option<u64>,
        limit: WorkflowCheckpointHistoryLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowCheckpointRevision>, WorkflowStoreError>>;

    /// Loads one exact immutable checkpoint revision for state inspection.
    fn load_checkpoint_revision(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        revision: u64,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowCheckpointRevision, WorkflowStoreError>>;

    /// Idempotently creates a new execution branch from immutable history.
    fn fork_workflow(
        &self,
        tenant_id: WorkflowTenantId,
        command: WorkflowForkCommand,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowForkOutcome, WorkflowStoreError>>;

    /// Loads a checkpoint under a current worker lease.
    fn load_checkpoint(
        &self,
        lease: WorkflowLease,
    ) -> WorkflowStoreFuture<'_, Result<Checkpoint, CheckpointError>>;

    /// Creates or compare-and-swaps a checkpoint under a current worker lease.
    fn compare_and_swap_checkpoint(
        &self,
        lease: WorkflowLease,
        checkpoint: Checkpoint,
        expected_revision: Option<u64>,
    ) -> WorkflowStoreFuture<'_, Result<(), CheckpointError>>;
}

/// Optional fenced control plane for physically removing terminal Tasks.
///
/// Implementations must write an immutable tombstone in the same atomic
/// operation that removes execution state. Active Tasks are never eligible.
pub trait WorkflowTaskRetentionStore: WorkflowStore {
    /// Discovers tenants that currently own terminal Tasks.
    ///
    /// Results are ordered lexicographically and strictly follow `after`.
    fn list_task_cleanup_tenants(
        &self,
        after: Option<WorkflowTenantId>,
        limit: WorkflowTenantListLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowTenantId>, WorkflowStoreError>>;

    /// Claims one tenant's cleanup partition if it is idle or expired.
    fn claim_task_cleanup(
        &self,
        tenant_id: WorkflowTenantId,
        owner: WorkerId,
        lease: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<Option<WorkflowTaskCleanupLease>, WorkflowStoreError>>;

    /// Atomically tombstones and removes one bounded terminal Task batch.
    fn compact_terminal_tasks(
        &self,
        lease: WorkflowTaskCleanupLease,
        retention: WorkflowTaskRetention,
        limit: WorkflowTaskCleanupLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowTaskTombstone>, WorkflowStoreError>>;

    /// Extends an exact current cleanup lease using store-authoritative time.
    fn heartbeat_task_cleanup(
        &self,
        lease: WorkflowTaskCleanupLease,
        extension: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskCleanupLease, WorkflowStoreError>>;

    /// Lists immutable tombstones strictly after an optional cursor.
    fn list_task_tombstones(
        &self,
        tenant_id: WorkflowTenantId,
        after: Option<WorkflowTaskTombstoneCursor>,
        limit: WorkflowTaskTombstoneLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowTaskTombstone>, WorkflowStoreError>>;

    /// Releases a current unexpired cleanup lease.
    fn release_task_cleanup(
        &self,
        lease: WorkflowTaskCleanupLease,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>>;
}

/// Store-authoritative time source used by the in-memory reference adapter.
pub trait WorkflowClock: Send + Sync {
    /// Returns Unix time in milliseconds.
    fn now_ms(&self) -> u64;
}

/// System clock used by default for ephemeral workflow queues.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemWorkflowClock;

impl WorkflowClock for SystemWorkflowClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
            })
    }
}
