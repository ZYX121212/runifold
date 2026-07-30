//! Legal-hold and external-export persistence.

use runifold_core::CheckpointId;
use runifold_workflow::{
    WorkerId, WorkflowStoreError, WorkflowStoreErrorKind, WorkflowStoreFuture,
    WorkflowTaskLegalHold, WorkflowTaskLegalHoldReason, WorkflowTaskTombstoneCursor,
    WorkflowTaskTombstoneExport, WorkflowTaskTombstoneExportReceipt, WorkflowTenantId,
};

use super::{
    super::{
        PostgresWorkflowStore,
        support::{database_i64, storage},
    },
    support::{decode_export, decode_hold, governance_conflict, governance_not_found},
};

pub(in crate::workflow) fn place_hold(
    store: &PostgresWorkflowStore,
    tenant_id: WorkflowTenantId,
    checkpoint_id: CheckpointId,
    actor: WorkerId,
    reason: WorkflowTaskLegalHoldReason,
) -> WorkflowStoreFuture<'_, Result<WorkflowTaskLegalHold, WorkflowStoreError>> {
    Box::pin(async move {
        let row = store
            .client
            .query_opt(
                &format!(
                    "WITH target AS (
                        SELECT checkpoint_id
                        FROM {table}_task_tombstones
                        WHERE checkpoint_id = $1 AND tenant_id = $2
                     ), inserted AS (
                        INSERT INTO {table}_tg_hold (
                            checkpoint_id, tenant_id, placed_by, reason
                        )
                        SELECT checkpoint_id, $2, $3, $4
                        FROM target
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM {table}_tg_hold
                            WHERE checkpoint_id = $1 AND released_at IS NULL
                        )
                        ON CONFLICT DO NOTHING
                        RETURNING checkpoint_id, tenant_id, placed_by, reason,
                                  FLOOR(EXTRACT(EPOCH FROM placed_at) * 1000)::BIGINT,
                                  released_by,
                                  FLOOR(EXTRACT(EPOCH FROM released_at) * 1000)::BIGINT
                     )
                     SELECT * FROM inserted
                     UNION ALL
                     SELECT hold.checkpoint_id, hold.tenant_id, hold.placed_by,
                            hold.reason,
                            FLOOR(EXTRACT(EPOCH FROM hold.placed_at) * 1000)::BIGINT,
                            hold.released_by,
                            FLOOR(EXTRACT(EPOCH FROM hold.released_at) * 1000)::BIGINT
                     FROM {table}_tg_hold AS hold
                     CROSS JOIN target
                     WHERE hold.checkpoint_id = $1 AND hold.released_at IS NULL
                     ORDER BY 5 DESC
                     LIMIT 1",
                    table = store.table
                ),
                &[
                    &checkpoint_id.as_uuid(),
                    &tenant_id.as_str(),
                    &actor.as_str(),
                    &reason.as_str(),
                ],
            )
            .await
            .map_err(storage)?
            .ok_or_else(|| governance_not_found("Task tombstone does not exist"))?;
        decode_hold(&row)
    })
}

pub(in crate::workflow) fn release_hold(
    store: &PostgresWorkflowStore,
    tenant_id: WorkflowTenantId,
    checkpoint_id: CheckpointId,
    actor: WorkerId,
) -> WorkflowStoreFuture<'_, Result<WorkflowTaskLegalHold, WorkflowStoreError>> {
    Box::pin(async move {
        let row = store
            .client
            .query_opt(
                &format!(
                    "WITH released AS (
                        UPDATE {table}_tg_hold
                        SET released_by = $3, released_at = clock_timestamp()
                        WHERE checkpoint_id = $1 AND tenant_id = $2
                          AND released_at IS NULL
                        RETURNING checkpoint_id, tenant_id, placed_by, reason,
                                  FLOOR(EXTRACT(EPOCH FROM placed_at) * 1000)::BIGINT,
                                  released_by,
                                  FLOOR(EXTRACT(EPOCH FROM released_at) * 1000)::BIGINT
                     )
                     SELECT * FROM released
                     UNION ALL
                     SELECT checkpoint_id, tenant_id, placed_by, reason,
                            FLOOR(EXTRACT(EPOCH FROM placed_at) * 1000)::BIGINT,
                            released_by,
                            FLOOR(EXTRACT(EPOCH FROM released_at) * 1000)::BIGINT
                     FROM {table}_tg_hold
                     WHERE checkpoint_id = $1 AND tenant_id = $2
                       AND released_by = $3 AND released_at IS NOT NULL
                     ORDER BY 5 DESC
                     LIMIT 1",
                    table = store.table
                ),
                &[
                    &checkpoint_id.as_uuid(),
                    &tenant_id.as_str(),
                    &actor.as_str(),
                ],
            )
            .await
            .map_err(storage)?
            .ok_or_else(|| governance_conflict("no active legal hold may be released"))?;
        decode_hold(&row)
    })
}

pub(in crate::workflow) fn confirm_export(
    store: &PostgresWorkflowStore,
    tenant_id: WorkflowTenantId,
    through: WorkflowTaskTombstoneCursor,
    receipt: WorkflowTaskTombstoneExportReceipt,
    actor: WorkerId,
) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstoneExport, WorkflowStoreError>> {
    Box::pin(async move {
        if through.get() == 0 {
            return Err(WorkflowStoreError::new(
                WorkflowStoreErrorKind::InvalidInput,
                "Task tombstone export cursor must be positive",
            ));
        }
        let through_value = database_i64(through.get(), "Task tombstone export cursor")?;
        let row = store
            .client
            .query_opt(
                &format!(
                    "WITH confirmed AS (
                        INSERT INTO {table}_tg_export (
                            tenant_id, through_sequence, receipt, confirmed_by
                        ) VALUES ($1, $2, $3, $4)
                        ON CONFLICT (tenant_id) DO UPDATE SET
                            through_sequence = EXCLUDED.through_sequence,
                            receipt = EXCLUDED.receipt,
                            confirmed_by = EXCLUDED.confirmed_by,
                            confirmed_at = clock_timestamp()
                        WHERE {table}_tg_export.through_sequence
                              < EXCLUDED.through_sequence
                        RETURNING tenant_id, through_sequence, receipt, confirmed_by,
                                  FLOOR(EXTRACT(EPOCH FROM confirmed_at) * 1000)::BIGINT
                     )
                     SELECT * FROM confirmed
                     UNION ALL
                     SELECT tenant_id, through_sequence, receipt, confirmed_by,
                            FLOOR(EXTRACT(EPOCH FROM confirmed_at) * 1000)::BIGINT
                     FROM {table}_tg_export
                     WHERE tenant_id = $1 AND through_sequence = $2 AND receipt = $3
                     LIMIT 1",
                    table = store.table
                ),
                &[
                    &tenant_id.as_str(),
                    &through_value,
                    &receipt.as_str(),
                    &actor.as_str(),
                ],
            )
            .await
            .map_err(storage)?
            .ok_or_else(|| {
                governance_conflict("export confirmation cannot move backward or replace a receipt")
            })?;
        decode_export(&row)
    })
}
