//! Fenced terminal Task cleanup and immutable tombstone audit.

use runifold_core::CheckpointId;
use runifold_workflow::{
    LeaseDuration, WorkerId, WorkflowStoreError, WorkflowStoreErrorKind, WorkflowStoreFuture,
    WorkflowTaskCleanupLease, WorkflowTaskCleanupLimit, WorkflowTaskRetention, WorkflowTaskStatus,
    WorkflowTaskTombstone, WorkflowTaskTombstoneCursor, WorkflowTaskTombstoneLimit,
    WorkflowTenantId, WorkflowTenantListLimit,
};
use tokio_postgres::Row;
use uuid::Uuid;

use super::{
    PostgresWorkflowStore,
    support::{database_i64, decode_u64, storage},
};

pub(super) fn list_tenants(
    store: &PostgresWorkflowStore,
    after: Option<WorkflowTenantId>,
    limit: WorkflowTenantListLimit,
) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowTenantId>, WorkflowStoreError>> {
    Box::pin(async move {
        let after = after.as_ref().map_or("", WorkflowTenantId::as_str);
        let limit = i64::from(limit.get());
        store
            .client
            .query(
                &format!(
                    "SELECT DISTINCT tenant_id
                     FROM {table}
                     WHERE state IN ('completed', 'failed', 'cancelled')
                       AND tenant_id > $1
                     ORDER BY tenant_id
                     LIMIT $2",
                    table = store.table
                ),
                &[&after, &limit],
            )
            .await
            .map_err(storage)?
            .into_iter()
            .map(|row| {
                let tenant: String = row.try_get(0).map_err(storage)?;
                WorkflowTenantId::parse(tenant)
            })
            .collect()
    })
}

pub(super) fn claim(
    store: &PostgresWorkflowStore,
    tenant_id: WorkflowTenantId,
    owner: WorkerId,
    lease: LeaseDuration,
) -> WorkflowStoreFuture<'_, Result<Option<WorkflowTaskCleanupLease>, WorkflowStoreError>> {
    Box::pin(async move {
        let lease_ms = database_i64(lease.as_millis(), "Task cleanup lease")?;
        let row = store
            .client
            .query_opt(
                &format!(
                    "INSERT INTO {table}_task_cleanup (
                        tenant_id, owner, fencing_token, lease_expires_at
                     ) VALUES (
                        $1, $2, 1,
                        clock_timestamp() + ($3::BIGINT * INTERVAL '1 millisecond')
                     )
                     ON CONFLICT (tenant_id) DO UPDATE SET
                        owner = EXCLUDED.owner,
                        fencing_token = {table}_task_cleanup.fencing_token + 1,
                        lease_expires_at = EXCLUDED.lease_expires_at,
                        updated_at = clock_timestamp()
                     WHERE {table}_task_cleanup.owner IS NULL
                        OR {table}_task_cleanup.lease_expires_at <= clock_timestamp()
                     RETURNING fencing_token,
                        FLOOR(EXTRACT(EPOCH FROM lease_expires_at) * 1000)::BIGINT",
                    table = store.table
                ),
                &[&tenant_id.as_str(), &owner.as_str(), &lease_ms],
            )
            .await
            .map_err(storage)?;
        row.map(|row| {
            Ok(WorkflowTaskCleanupLease {
                tenant_id,
                owner,
                fencing_token: decode_u64(row.try_get(0).map_err(storage)?, "cleanup token")?,
                expires_at_ms: decode_u64(
                    row.try_get(1).map_err(storage)?,
                    "cleanup lease expiration",
                )?,
            })
        })
        .transpose()
    })
}

pub(super) fn compact(
    store: &PostgresWorkflowStore,
    lease: WorkflowTaskCleanupLease,
    retention: WorkflowTaskRetention,
    limit: WorkflowTaskCleanupLimit,
) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowTaskTombstone>, WorkflowStoreError>> {
    Box::pin(async move {
        let token = database_i64(lease.fencing_token, "Task cleanup fencing token")?;
        let retention_ms = database_i64(retention.as_millis(), "Task retention")?;
        let limit = i64::from(limit.get());
        let rows = store
            .client
            .query(
                &format!(
                    "WITH valid AS (
                        SELECT TRUE AS held
                        FROM {table}_task_cleanup
                        WHERE tenant_id = $1 AND owner = $2
                          AND fencing_token = $3
                          AND lease_expires_at > clock_timestamp()
                     ), candidates AS (
                        SELECT task.checkpoint_id, task.tenant_id, task.workflow,
                               task.workflow_version, task.state,
                               task.created_at, task.updated_at
                        FROM {table} AS task CROSS JOIN valid
                        WHERE task.tenant_id = $1
                          AND task.state IN ('completed', 'failed', 'cancelled')
                          AND task.updated_at <= clock_timestamp()
                              - ($4::BIGINT * INTERVAL '1 millisecond')
                        ORDER BY task.updated_at, task.checkpoint_id
                        LIMIT $5
                        FOR UPDATE OF task SKIP LOCKED
                     ), inserted AS (
                        INSERT INTO {table}_task_tombstones (
                            checkpoint_id, tenant_id, workflow, workflow_version,
                            final_status, created_at, terminal_at
                        )
                        SELECT checkpoint_id, tenant_id, workflow, workflow_version,
                               state, created_at, updated_at
                        FROM candidates
                        ON CONFLICT (checkpoint_id) DO NOTHING
                        RETURNING sequence, checkpoint_id, tenant_id, workflow,
                                  workflow_version, final_status, created_at,
                                  terminal_at, deleted_at
                     ), deleted_signals AS (
                        DELETE FROM {table}_signals
                        WHERE checkpoint_id IN (SELECT checkpoint_id FROM inserted)
                     ), deleted_history AS (
                        DELETE FROM {table}_checkpoint_history
                        WHERE checkpoint_id IN (SELECT checkpoint_id FROM inserted)
                     ), deleted_budgets AS (
                        DELETE FROM {table}_budgets
                        WHERE checkpoint_id IN (SELECT checkpoint_id FROM inserted)
                     ), deleted_tasks AS (
                        DELETE FROM {table}
                        WHERE checkpoint_id IN (SELECT checkpoint_id FROM inserted)
                        RETURNING checkpoint_id
                     )
                     SELECT valid.held, inserted.sequence, inserted.checkpoint_id,
                            inserted.tenant_id, inserted.workflow,
                            inserted.workflow_version, inserted.final_status,
                            FLOOR(EXTRACT(EPOCH FROM inserted.created_at) * 1000)::BIGINT,
                            FLOOR(EXTRACT(EPOCH FROM inserted.terminal_at) * 1000)::BIGINT,
                            FLOOR(EXTRACT(EPOCH FROM inserted.deleted_at) * 1000)::BIGINT
                     FROM valid
                     LEFT JOIN (
                        inserted INNER JOIN deleted_tasks USING (checkpoint_id)
                     ) ON TRUE
                     ORDER BY inserted.sequence",
                    table = store.table
                ),
                &[
                    &lease.tenant_id.as_str(),
                    &lease.owner.as_str(),
                    &token,
                    &retention_ms,
                    &limit,
                ],
            )
            .await
            .map_err(storage)?;
        if rows.is_empty() {
            return Err(cleanup_lease_lost());
        }
        rows.iter()
            .filter_map(|row| match row.try_get::<_, Option<i64>>(1) {
                Ok(Some(_)) => Some(decode_tombstone(row)),
                Ok(None) => None,
                Err(error) => Some(Err(storage(error))),
            })
            .collect()
    })
}

pub(super) fn list(
    store: &PostgresWorkflowStore,
    tenant_id: WorkflowTenantId,
    after: Option<WorkflowTaskTombstoneCursor>,
    limit: WorkflowTaskTombstoneLimit,
) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowTaskTombstone>, WorkflowStoreError>> {
    Box::pin(async move {
        let after = database_i64(after.unwrap_or_default().get(), "Task tombstone cursor")?;
        let limit = i64::from(limit.get());
        store
            .client
            .query(
                &format!(
                    "SELECT sequence, checkpoint_id, tenant_id, workflow,
                            workflow_version, final_status,
                            FLOOR(EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT,
                            FLOOR(EXTRACT(EPOCH FROM terminal_at) * 1000)::BIGINT,
                            FLOOR(EXTRACT(EPOCH FROM deleted_at) * 1000)::BIGINT
                     FROM {table}_task_tombstones
                     WHERE tenant_id = $1 AND sequence > $2
                     ORDER BY sequence
                     LIMIT $3",
                    table = store.table
                ),
                &[&tenant_id.as_str(), &after, &limit],
            )
            .await
            .map_err(storage)?
            .iter()
            .map(decode_listed_tombstone)
            .collect()
    })
}

pub(super) fn release(
    store: &PostgresWorkflowStore,
    lease: WorkflowTaskCleanupLease,
) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
    Box::pin(async move {
        let token = database_i64(lease.fencing_token, "Task cleanup fencing token")?;
        let changed = store
            .client
            .execute(
                &format!(
                    "UPDATE {table}_task_cleanup
                     SET owner = NULL, lease_expires_at = NULL,
                         updated_at = clock_timestamp()
                     WHERE tenant_id = $1 AND owner = $2
                       AND fencing_token = $3
                       AND lease_expires_at > clock_timestamp()",
                    table = store.table
                ),
                &[&lease.tenant_id.as_str(), &lease.owner.as_str(), &token],
            )
            .await
            .map_err(storage)?;
        if changed == 1 {
            Ok(())
        } else {
            Err(cleanup_lease_lost())
        }
    })
}

pub(super) fn heartbeat(
    store: &PostgresWorkflowStore,
    lease: WorkflowTaskCleanupLease,
    extension: LeaseDuration,
) -> WorkflowStoreFuture<'_, Result<WorkflowTaskCleanupLease, WorkflowStoreError>> {
    Box::pin(async move {
        let token = database_i64(lease.fencing_token, "Task cleanup fencing token")?;
        let extension_ms = database_i64(extension.as_millis(), "Task cleanup lease extension")?;
        let row = store
            .client
            .query_opt(
                &format!(
                    "UPDATE {table}_task_cleanup
                     SET lease_expires_at =
                            clock_timestamp() + ($4::BIGINT * INTERVAL '1 millisecond'),
                         updated_at = clock_timestamp()
                     WHERE tenant_id = $1 AND owner = $2
                       AND fencing_token = $3
                       AND lease_expires_at > clock_timestamp()
                     RETURNING FLOOR(
                         EXTRACT(EPOCH FROM lease_expires_at) * 1000
                     )::BIGINT",
                    table = store.table
                ),
                &[
                    &lease.tenant_id.as_str(),
                    &lease.owner.as_str(),
                    &token,
                    &extension_ms,
                ],
            )
            .await
            .map_err(storage)?
            .ok_or_else(cleanup_lease_lost)?;
        Ok(WorkflowTaskCleanupLease {
            expires_at_ms: decode_u64(
                row.try_get(0).map_err(storage)?,
                "cleanup lease expiration",
            )?,
            ..lease
        })
    })
}

fn decode_tombstone(row: &Row) -> Result<WorkflowTaskTombstone, WorkflowStoreError> {
    decode_fields(row, 1)
}

fn decode_listed_tombstone(row: &Row) -> Result<WorkflowTaskTombstone, WorkflowStoreError> {
    decode_fields(row, 0)
}

fn decode_fields(row: &Row, offset: usize) -> Result<WorkflowTaskTombstone, WorkflowStoreError> {
    let sequence: i64 = row.try_get(offset).map_err(storage)?;
    let checkpoint_id: Uuid = row.try_get(offset + 1).map_err(storage)?;
    let tenant_id: String = row.try_get(offset + 2).map_err(storage)?;
    let workflow_version: i32 = row.try_get(offset + 4).map_err(storage)?;
    Ok(WorkflowTaskTombstone {
        cursor: WorkflowTaskTombstoneCursor::new(decode_u64(sequence, "tombstone sequence")?),
        checkpoint_id: CheckpointId::from_uuid(checkpoint_id),
        tenant_id: WorkflowTenantId::parse(tenant_id)?,
        workflow: row.try_get(offset + 3).map_err(storage)?,
        workflow_version: u32::try_from(workflow_version).map_err(|_| {
            WorkflowStoreError::new(
                WorkflowStoreErrorKind::Storage,
                "stored tombstone workflow version is invalid",
            )
        })?,
        final_status: decode_terminal_status(row.try_get(offset + 5).map_err(storage)?)?,
        created_at_ms: decode_u64(
            row.try_get(offset + 6).map_err(storage)?,
            "tombstone creation time",
        )?,
        terminal_at_ms: decode_u64(
            row.try_get(offset + 7).map_err(storage)?,
            "tombstone terminal time",
        )?,
        deleted_at_ms: decode_u64(
            row.try_get(offset + 8).map_err(storage)?,
            "tombstone deletion time",
        )?,
    })
}

fn decode_terminal_status(value: &str) -> Result<WorkflowTaskStatus, WorkflowStoreError> {
    match value {
        "completed" => Ok(WorkflowTaskStatus::Completed),
        "failed" => Ok(WorkflowTaskStatus::Failed),
        "cancelled" => Ok(WorkflowTaskStatus::Cancelled),
        _ => Err(WorkflowStoreError::new(
            WorkflowStoreErrorKind::Storage,
            "stored Task tombstone status is not terminal",
        )),
    }
}

fn cleanup_lease_lost() -> WorkflowStoreError {
    WorkflowStoreError::new(
        WorkflowStoreErrorKind::LeaseLost,
        "workflow Task cleanup lease is expired, superseded, or owned by another worker",
    )
}
