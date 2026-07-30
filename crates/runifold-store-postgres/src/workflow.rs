//! `PostgreSQL` distributed workflow task-control adapter for Runifold.

mod budget;
mod budget_store;
mod checkpoint;
mod codec;
mod lifecycle;
mod retention;
mod signal;
mod sql;
mod support;
mod tombstone;

use budget::{
    StoredBudgetLimit, budget_decoding, budget_encoding, decode_budget_reservation_status,
    decode_budget_settlement_status, postgres_budget_request,
};
use checkpoint::CheckpointStoreExt;
use codec::{
    ForkSource, PreparedFork, decode_budget_audit_event, decode_budget_audit_projection_lease,
    decode_budget_snapshot, decode_snapshot, fork_storage_fields,
};
use support::{
    checkpoint_decoding, checkpoint_domain_error, checkpoint_encoding, checkpoint_i64,
    checkpoint_lease_lost, checkpoint_storage, database_i64, decode_u64, lease_lost,
    projection_lease_lost, storage, tenant_mismatch, validate_identifier,
};

use std::sync::Arc;

use runifold_core::{
    Budget, Checkpoint, CheckpointError, CheckpointErrorKind, CheckpointId, Usage,
};
use runifold_workflow::{
    ClaimedWorkflow, LeaseDuration, WorkerId, WorkflowBudgetAuditCursor, WorkflowBudgetAuditEvent,
    WorkflowBudgetAuditLimit, WorkflowBudgetAuditProjectionId, WorkflowBudgetAuditProjectionLease,
    WorkflowBudgetReservationOutcome, WorkflowCancelOutcome, WorkflowCheckpointHistoryLimit,
    WorkflowCheckpointRevision, WorkflowDisposition, WorkflowForkCommand, WorkflowForkOutcome,
    WorkflowLease, WorkflowLineage, WorkflowSignal, WorkflowSignalId, WorkflowSignalOutcome,
    WorkflowSignalRetention, WorkflowSignalSnapshot, WorkflowStore, WorkflowStoreError,
    WorkflowStoreErrorKind, WorkflowStoreFuture, WorkflowTask, WorkflowTaskCleanupLease,
    WorkflowTaskCleanupLimit, WorkflowTaskLegalHold, WorkflowTaskLegalHoldReason,
    WorkflowTaskRetention, WorkflowTaskRetentionStore, WorkflowTaskSnapshot, WorkflowTaskTombstone,
    WorkflowTaskTombstoneApprovalInboxItem, WorkflowTaskTombstoneApprovalInboxLimit,
    WorkflowTaskTombstoneApprovalLease, WorkflowTaskTombstoneApprovalWindow,
    WorkflowTaskTombstoneCursor, WorkflowTaskTombstoneExport, WorkflowTaskTombstoneExportReceipt,
    WorkflowTaskTombstoneGovernanceStore, WorkflowTaskTombstoneLimit,
    WorkflowTaskTombstonePurgeEvidence, WorkflowTaskTombstonePurgeId,
    WorkflowTaskTombstonePurgeIntent, WorkflowTaskTombstonePurgeLimit,
    WorkflowTaskTombstoneRejectionReason, WorkflowTaskTombstoneRetention,
    WorkflowTenantBudgetPolicy, WorkflowTenantBudgetSnapshot, WorkflowTenantId,
    WorkflowTenantListLimit, WorkflowTenantPolicy,
};
use serde_json::Value;
use thiserror::Error;
use tokio_postgres::{Client, NoTls, error::SqlState};

/// `PostgreSQL` workflow-store configuration or connection failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PostgresWorkflowStoreError {
    /// The configured table name is unsafe for SQL interpolation.
    #[error("workflow table must be a portable PostgreSQL identifier of at most 48 bytes")]
    InvalidTable,
    /// `PostgreSQL` connection or schema setup failed.
    #[error("PostgreSQL workflow store operation failed: {0}")]
    Database(#[from] tokio_postgres::Error),
}

/// PostgreSQL-backed distributed workflow task store.
///
/// Claim, heartbeat, and finish operations use the database clock. Every
/// ownership mutation compares worker identity, fencing token, and lease
/// expiration.
#[derive(Clone, Debug)]
pub struct PostgresWorkflowStore {
    client: Arc<Client>,
    table: String,
}

impl PostgresWorkflowStore {
    /// Connects without creating or changing schema.
    ///
    /// # Errors
    ///
    /// Rejects unsafe table identifiers and propagates connection failures.
    pub async fn connect(
        connection: &str,
        table: &str,
    ) -> Result<Self, PostgresWorkflowStoreError> {
        validate_identifier(table)?;
        let (client, connection) = tokio_postgres::connect(connection, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(Self {
            client: Arc::new(client),
            table: table.into(),
        })
    }

    /// Explicitly creates the workflow task table and claim index.
    ///
    /// Runtime queue operations never perform hidden migrations.
    ///
    /// # Errors
    ///
    /// Propagates `PostgreSQL` DDL failures.
    pub async fn ensure_schema(&self) -> Result<(), PostgresWorkflowStoreError> {
        self.client
            .batch_execute(&Self::task_schema_sql(&self.table))
            .await?;
        self.client
            .batch_execute(&Self::checkpoint_history_schema_sql(&self.table))
            .await?;
        self.client
            .batch_execute(&Self::signal_schema_sql(&self.table))
            .await?;
        self.client
            .batch_execute(&Self::budget_schema_sql(&self.table))
            .await?;
        self.client
            .batch_execute(&Self::task_retention_schema_sql(&self.table))
            .await?;
        Ok(())
    }
}

impl WorkflowTaskRetentionStore for PostgresWorkflowStore {
    fn list_task_cleanup_tenants(
        &self,
        after: Option<WorkflowTenantId>,
        limit: WorkflowTenantListLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowTenantId>, WorkflowStoreError>> {
        retention::list_tenants(self, after, limit)
    }

    fn claim_task_cleanup(
        &self,
        tenant_id: WorkflowTenantId,
        owner: WorkerId,
        lease: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<Option<WorkflowTaskCleanupLease>, WorkflowStoreError>> {
        retention::claim(self, tenant_id, owner, lease)
    }

    fn compact_terminal_tasks(
        &self,
        lease: WorkflowTaskCleanupLease,
        retention: WorkflowTaskRetention,
        limit: WorkflowTaskCleanupLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowTaskTombstone>, WorkflowStoreError>> {
        retention::compact(self, lease, retention, limit)
    }

    fn heartbeat_task_cleanup(
        &self,
        lease: WorkflowTaskCleanupLease,
        extension: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskCleanupLease, WorkflowStoreError>> {
        retention::heartbeat(self, lease, extension)
    }

    fn list_task_tombstones(
        &self,
        tenant_id: WorkflowTenantId,
        after: Option<WorkflowTaskTombstoneCursor>,
        limit: WorkflowTaskTombstoneLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowTaskTombstone>, WorkflowStoreError>> {
        retention::list(self, tenant_id, after, limit)
    }

    fn release_task_cleanup(
        &self,
        lease: WorkflowTaskCleanupLease,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        retention::release(self, lease)
    }
}

impl WorkflowTaskTombstoneGovernanceStore for PostgresWorkflowStore {
    fn place_task_tombstone_hold(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        actor: WorkerId,
        reason: WorkflowTaskLegalHoldReason,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskLegalHold, WorkflowStoreError>> {
        tombstone::place_hold(self, tenant_id, checkpoint_id, actor, reason)
    }

    fn release_task_tombstone_hold(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        actor: WorkerId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskLegalHold, WorkflowStoreError>> {
        tombstone::release_hold(self, tenant_id, checkpoint_id, actor)
    }

    fn confirm_task_tombstone_export(
        &self,
        tenant_id: WorkflowTenantId,
        through: WorkflowTaskTombstoneCursor,
        receipt: WorkflowTaskTombstoneExportReceipt,
        actor: WorkerId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstoneExport, WorkflowStoreError>> {
        tombstone::confirm_export(self, tenant_id, through, receipt, actor)
    }

    fn prepare_task_tombstone_purge(
        &self,
        lease: WorkflowTaskCleanupLease,
        retention: WorkflowTaskTombstoneRetention,
        limit: WorkflowTaskTombstonePurgeLimit,
        approval_window: WorkflowTaskTombstoneApprovalWindow,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstonePurgeIntent, WorkflowStoreError>> {
        tombstone::prepare_purge(self, lease, retention, limit, approval_window)
    }

    fn approve_task_tombstone_purge(
        &self,
        tenant_id: WorkflowTenantId,
        purge_id: WorkflowTaskTombstonePurgeId,
        approver: WorkerId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstonePurgeIntent, WorkflowStoreError>> {
        tombstone::approve_purge(self, tenant_id, purge_id, approver)
    }

    fn list_task_tombstone_purge_approvals(
        &self,
        tenant_id: WorkflowTenantId,
        limit: WorkflowTaskTombstoneApprovalInboxLimit,
    ) -> WorkflowStoreFuture<
        '_,
        Result<Vec<WorkflowTaskTombstoneApprovalInboxItem>, WorkflowStoreError>,
    > {
        tombstone::list_approvals(self, tenant_id, limit)
    }

    fn claim_task_tombstone_purge_approval(
        &self,
        tenant_id: WorkflowTenantId,
        reviewer: WorkerId,
        lease: LeaseDuration,
    ) -> WorkflowStoreFuture<
        '_,
        Result<Option<WorkflowTaskTombstoneApprovalLease>, WorkflowStoreError>,
    > {
        tombstone::claim_approval(self, tenant_id, reviewer, lease)
    }

    fn approve_claimed_task_tombstone_purge(
        &self,
        lease: WorkflowTaskTombstoneApprovalLease,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstonePurgeIntent, WorkflowStoreError>> {
        tombstone::approve_claimed(self, lease)
    }

    fn reject_claimed_task_tombstone_purge(
        &self,
        lease: WorkflowTaskTombstoneApprovalLease,
        reason: WorkflowTaskTombstoneRejectionReason,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstoneApprovalInboxItem, WorkflowStoreError>>
    {
        tombstone::reject_claimed(self, lease, reason)
    }

    fn execute_task_tombstone_purge(
        &self,
        lease: WorkflowTaskCleanupLease,
        purge_id: WorkflowTaskTombstonePurgeId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstonePurgeEvidence, WorkflowStoreError>>
    {
        tombstone::execute_purge(self, lease, purge_id)
    }

    fn get_task_tombstone_purge_evidence(
        &self,
        tenant_id: WorkflowTenantId,
        purge_id: WorkflowTaskTombstonePurgeId,
    ) -> WorkflowStoreFuture<
        '_,
        Result<Option<WorkflowTaskTombstonePurgeEvidence>, WorkflowStoreError>,
    > {
        tombstone::get_evidence(self, tenant_id, purge_id)
    }
}

impl WorkflowStore for PostgresWorkflowStore {
    fn set_tenant_policy(
        &self,
        tenant_id: WorkflowTenantId,
        policy: WorkflowTenantPolicy,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        Box::pin(async move {
            let outstanding = i64::from(policy.max_outstanding_tasks());
            let leases = i64::from(policy.max_concurrent_leases());
            self.client
                .execute(
                    &format!(
                        "INSERT INTO {table}_tenants (
                            tenant_id, max_outstanding_tasks, max_concurrent_leases
                         ) VALUES ($1, $2, $3)
                         ON CONFLICT (tenant_id) DO UPDATE SET
                            max_outstanding_tasks = EXCLUDED.max_outstanding_tasks,
                            max_concurrent_leases = EXCLUDED.max_concurrent_leases,
                            updated_at = clock_timestamp()",
                        table = self.table
                    ),
                    &[&tenant_id.as_str(), &outstanding, &leases],
                )
                .await
                .map_err(storage)?;
            Ok(())
        })
    }

    fn set_tenant_budget_policy(
        &self,
        tenant_id: WorkflowTenantId,
        policy: WorkflowTenantBudgetPolicy,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        self.set_tenant_budget_policy_inner(tenant_id, policy)
    }

    fn list_tenant_budgets(
        &self,
        after: Option<WorkflowTenantId>,
        limit: WorkflowTenantListLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowTenantId>, WorkflowStoreError>> {
        self.list_tenant_budgets_inner(after, limit)
    }

    fn inspect_tenant_budget(
        &self,
        tenant_id: WorkflowTenantId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTenantBudgetSnapshot, WorkflowStoreError>> {
        self.inspect_tenant_budget_inner(tenant_id)
    }

    fn list_tenant_budget_audit(
        &self,
        tenant_id: WorkflowTenantId,
        after: Option<WorkflowBudgetAuditCursor>,
        limit: WorkflowBudgetAuditLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowBudgetAuditEvent>, WorkflowStoreError>> {
        self.list_tenant_budget_audit_inner(tenant_id, after, limit)
    }

    fn compact_tenant_budget_audit(
        &self,
        tenant_id: WorkflowTenantId,
        through: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<u64, WorkflowStoreError>> {
        self.compact_tenant_budget_audit_inner(tenant_id, through)
    }

    fn load_or_create_tenant_budget_audit_projection(
        &self,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditCursor, WorkflowStoreError>> {
        self.load_or_create_tenant_budget_audit_projection_inner(tenant_id, projection_id)
    }

    fn advance_tenant_budget_audit_projection(
        &self,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
        expected: WorkflowBudgetAuditCursor,
        next: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<bool, WorkflowStoreError>> {
        self.advance_tenant_budget_audit_projection_inner(tenant_id, projection_id, expected, next)
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
        self.claim_tenant_budget_audit_projection_inner(tenant_id, projection_id, owner, lease)
    }

    fn heartbeat_tenant_budget_audit_projection(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
        extension: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditProjectionLease, WorkflowStoreError>>
    {
        self.heartbeat_tenant_budget_audit_projection_inner(lease, extension)
    }

    fn advance_tenant_budget_audit_projection_lease(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
        next: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditProjectionLease, WorkflowStoreError>>
    {
        self.advance_tenant_budget_audit_projection_lease_inner(lease, next)
    }

    fn release_tenant_budget_audit_projection(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        self.release_tenant_budget_audit_projection_inner(lease)
    }

    fn reserve_budget(
        &self,
        lease: WorkflowLease,
        workflow_limit: Budget,
        baseline: Usage,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetReservationOutcome, WorkflowStoreError>> {
        self.reserve_budget_inner(lease, workflow_limit, baseline)
    }

    fn settle_budget(
        &self,
        lease: WorkflowLease,
        cumulative: Usage,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        self.settle_budget_inner(lease, cumulative)
    }
    fn enqueue(
        &self,
        task: WorkflowTask,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        Box::pin(async move {
            task.validate()?;
            let version = i32::try_from(task.workflow_version).map_err(|_| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::InvalidInput,
                    "workflow version exceeds PostgreSQL INTEGER",
                )
            })?;
            self.client
                .execute(
                    &format!(
                        "INSERT INTO {table}_tenants (
                            tenant_id, max_outstanding_tasks, max_concurrent_leases
                         ) VALUES ($1, 10000, 100)
                         ON CONFLICT (tenant_id) DO NOTHING",
                        table = self.table
                    ),
                    &[&task.tenant_id.as_str()],
                )
                .await
                .map_err(storage)?;
            let inserted = self
                .client
                .execute(
                    &format!(
                        r"
                        WITH admitted AS (
                            UPDATE {table}_tenants
                            SET
                                outstanding_tasks = outstanding_tasks + 1,
                                updated_at = clock_timestamp()
                            WHERE tenant_id = $2
                              AND outstanding_tasks < max_outstanding_tasks
                            RETURNING tenant_id
                        )
                        INSERT INTO {table} (
                            checkpoint_id, tenant_id, workflow, workflow_version,
                            input, priority, state
                        )
                        SELECT $1, $2, $3, $4, $5, $6, 'queued'
                        FROM admitted
                        ",
                        table = self.table
                    ),
                    &[
                        &task.checkpoint_id.as_uuid(),
                        &task.tenant_id.as_str(),
                        &task.workflow,
                        &version,
                        &task.input,
                        &task.priority,
                    ],
                )
                .await;
            match inserted {
                Ok(1) => Ok(()),
                Ok(_) => Err(WorkflowStoreError::new(
                    WorkflowStoreErrorKind::AdmissionDenied,
                    format!(
                        "workflow tenant `{}` reached its outstanding task limit",
                        task.tenant_id.as_str()
                    ),
                )),
                Err(error)
                    if error
                        .as_db_error()
                        .is_some_and(|db| db.code() == &SqlState::UNIQUE_VIOLATION) =>
                {
                    Err(WorkflowStoreError::new(
                        WorkflowStoreErrorKind::Conflict,
                        format!("workflow task `{}` already exists", task.checkpoint_id),
                    ))
                }
                Err(error) => Err(storage(error)),
            }
        })
    }

    fn claim(
        &self,
        worker: WorkerId,
        lease: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<Option<ClaimedWorkflow>, WorkflowStoreError>> {
        Box::pin(async move { self.claim_inner(worker, lease).await })
    }

    fn heartbeat(
        &self,
        lease: WorkflowLease,
        extension: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowLease, WorkflowStoreError>> {
        Box::pin(async move { self.heartbeat_inner(lease, extension).await })
    }

    fn finish(
        &self,
        lease: WorkflowLease,
        disposition: WorkflowDisposition,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        Box::pin(async move { self.finish_inner(&lease, disposition).await })
    }

    fn publish_signal(
        &self,
        tenant_id: WorkflowTenantId,
        signal: WorkflowSignal,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowSignalOutcome, WorkflowStoreError>> {
        Box::pin(signal::publish(self, tenant_id, signal))
    }

    fn cancel(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowCancelOutcome, WorkflowStoreError>> {
        Box::pin(signal::cancel(self, tenant_id, checkpoint_id))
    }

    fn inspect_signal(
        &self,
        tenant_id: WorkflowTenantId,
        signal_id: WorkflowSignalId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowSignalSnapshot, WorkflowStoreError>> {
        Box::pin(signal::inspect(self, tenant_id, signal_id))
    }

    fn compact_signals(
        &self,
        tenant_id: WorkflowTenantId,
        retention: WorkflowSignalRetention,
    ) -> WorkflowStoreFuture<'_, Result<u64, WorkflowStoreError>> {
        Box::pin(signal::compact(self, tenant_id, retention))
    }
    fn inspect(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskSnapshot, WorkflowStoreError>> {
        self.inspect_ext(tenant_id, checkpoint_id)
    }

    fn list_checkpoint_history(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        after_revision: Option<u64>,
        limit: WorkflowCheckpointHistoryLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowCheckpointRevision>, WorkflowStoreError>> {
        self.list_checkpoint_history_ext(tenant_id, checkpoint_id, after_revision, limit)
    }

    fn load_checkpoint_revision(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        revision: u64,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowCheckpointRevision, WorkflowStoreError>> {
        self.load_checkpoint_revision_ext(tenant_id, checkpoint_id, revision)
    }

    fn fork_workflow(
        &self,
        tenant_id: WorkflowTenantId,
        command: WorkflowForkCommand,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowForkOutcome, WorkflowStoreError>> {
        self.fork_workflow_ext(tenant_id, command)
    }

    fn load_checkpoint(
        &self,
        lease: WorkflowLease,
    ) -> WorkflowStoreFuture<'_, Result<Checkpoint, CheckpointError>> {
        self.load_checkpoint_ext(lease)
    }

    fn compare_and_swap_checkpoint(
        &self,
        lease: WorkflowLease,
        checkpoint: Checkpoint,
        expected_revision: Option<u64>,
    ) -> WorkflowStoreFuture<'_, Result<(), CheckpointError>> {
        self.compare_and_swap_checkpoint_ext(lease, checkpoint, expected_revision)
    }
}

#[cfg(test)]
mod tests {
    use super::{PostgresWorkflowStore, PostgresWorkflowStoreError, validate_identifier};

    #[test]
    fn table_identifiers_are_validated_before_sql_construction() {
        assert!(validate_identifier("runifold_workflows").is_ok());
        assert!(matches!(
            validate_identifier("workflow; DROP TABLE users"),
            Err(PostgresWorkflowStoreError::InvalidTable)
        ));
        assert!(matches!(
            validate_identifier("9workflow"),
            Err(PostgresWorkflowStoreError::InvalidTable)
        ));
    }

    #[test]
    fn lease_statements_preserve_keyword_boundaries() {
        let claim = PostgresWorkflowStore::claim_sql_for("runifold_workflows");
        let heartbeat = PostgresWorkflowStore::heartbeat_sql_for("runifold_workflows");

        assert!(claim.contains("FROM runifold_workflows"));
        assert!(claim.contains("FOR UPDATE OF task, tenant SKIP LOCKED"));
        assert!(claim.contains("tenant.max_concurrent_leases"));
        assert!(claim.contains("pg_try_advisory_xact_lock"));
        assert!(claim.contains("nextval('runifold_workflows_claim_seq')"));
        assert!(claim.contains("UPDATE runifold_workflows AS task"));
        assert!(heartbeat.contains("UPDATE runifold_workflows"));
        assert!(heartbeat.contains("FROM renewed"));
        assert!(heartbeat.contains("_budgets AS reservation"));
    }
}
