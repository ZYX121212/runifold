//! Durable signal, cancellation, and HITL delivery operations.

use runifold_core::CheckpointId;
use runifold_workflow::{
    WorkflowCancelOutcome, WorkflowSignal, WorkflowSignalId, WorkflowSignalOutcome,
    WorkflowSignalRetention, WorkflowSignalSnapshot, WorkflowStoreError, WorkflowStoreErrorKind,
    WorkflowTenantId, WorkflowWake,
};
use serde_json::Value;
use uuid::Uuid;

use super::{
    PostgresWorkflowStore,
    codec::decode_signal_snapshot,
    support::{database_i64, storage, tenant_mismatch},
};

pub(super) async fn publish(
    store: &PostgresWorkflowStore,
    tenant_id: WorkflowTenantId,
    signal: WorkflowSignal,
) -> Result<WorkflowSignalOutcome, WorkflowStoreError> {
    let signal_id = signal.signal_id.as_checkpoint_id().as_uuid();
    let checkpoint_id = signal.checkpoint_id.as_uuid();
    let wake = serde_json::to_value(WorkflowWake::Signal {
        signal_id: signal.signal_id,
        name: signal.name.clone(),
        payload: signal.payload.clone(),
    })
    .map_err(|_| {
        WorkflowStoreError::new(
            WorkflowStoreErrorKind::InvalidInput,
            "workflow signal cannot be encoded",
        )
    })?;
    let row = store
        .client
        .query_one(
            &store.publish_signal_sql(),
            &[
                &signal_id,
                &checkpoint_id,
                &tenant_id.as_str(),
                &signal.name.as_str(),
                &signal.payload,
                &wake,
            ],
        )
        .await
        .map_err(storage)?;
    let inserted: bool = row.try_get(0).map_err(storage)?;
    let did_wake: bool = row.try_get(1).map_err(storage)?;
    let dead_lettered: bool = row.try_get(2).map_err(storage)?;
    if inserted {
        return Ok(if did_wake {
            WorkflowSignalOutcome::WokeWorkflow
        } else if dead_lettered {
            WorkflowSignalOutcome::DeadLettered
        } else {
            WorkflowSignalOutcome::Buffered
        });
    }
    resolve_replay(store, &tenant_id, signal_id, &signal).await
}

async fn resolve_replay(
    store: &PostgresWorkflowStore,
    tenant_id: &WorkflowTenantId,
    signal_id: Uuid,
    signal: &WorkflowSignal,
) -> Result<WorkflowSignalOutcome, WorkflowStoreError> {
    let existing = store
        .client
        .query_opt(
            &format!(
                "SELECT tenant_id, checkpoint_id, name, payload FROM {table}_signals \
                 WHERE signal_id = $1",
                table = store.table
            ),
            &[&signal_id],
        )
        .await
        .map_err(storage)?;
    let Some(existing) = existing else {
        let target_tenant = store
            .client
            .query_opt(
                &format!(
                    "SELECT tenant_id FROM {table} WHERE checkpoint_id = $1",
                    table = store.table
                ),
                &[&signal.checkpoint_id.as_uuid()],
            )
            .await
            .map_err(storage)?
            .map(|row| row.try_get::<_, String>(0))
            .transpose()
            .map_err(storage)?;
        return Err(match target_tenant {
            Some(actual) if actual != tenant_id.as_str() => tenant_mismatch(),
            _ => WorkflowStoreError::new(
                WorkflowStoreErrorKind::NotFound,
                format!("workflow task `{}` does not exist", signal.checkpoint_id),
            ),
        });
    };
    let existing_tenant: String = existing.try_get(0).map_err(storage)?;
    if existing_tenant != tenant_id.as_str() {
        return Err(tenant_mismatch());
    }
    let existing_checkpoint: Uuid = existing.try_get(1).map_err(storage)?;
    let existing_name: String = existing.try_get(2).map_err(storage)?;
    let existing_payload: Value = existing.try_get(3).map_err(storage)?;
    if existing_checkpoint == signal.checkpoint_id.as_uuid()
        && existing_name == signal.name.as_str()
        && existing_payload == signal.payload
    {
        Ok(WorkflowSignalOutcome::Duplicate)
    } else {
        Err(WorkflowStoreError::new(
            WorkflowStoreErrorKind::Conflict,
            "workflow signal identity is already bound to different content",
        ))
    }
}

pub(super) async fn cancel(
    store: &PostgresWorkflowStore,
    tenant_id: WorkflowTenantId,
    checkpoint_id: CheckpointId,
) -> Result<WorkflowCancelOutcome, WorkflowStoreError> {
    let row = store
        .client
        .query_one(
            &format!(
                r"
                WITH cancelled AS (
                    UPDATE {table}
                    SET
                        state = 'cancelled',
                        owner = NULL,
                        lease_expires_at = NULL,
                        wait_kind = NULL,
                        wait_name = NULL,
                        wait = NULL,
                        wake_at = NULL,
                        wake = NULL,
                        updated_at = clock_timestamp()
                    WHERE checkpoint_id = $1 AND tenant_id = $2
                      AND state IN (
                          'queued', 'leased', 'waiting_timer', 'waiting_signal'
                      )
                    RETURNING checkpoint_id, tenant_id
                ),
                dead_lettered AS (
                    UPDATE {table}_signals AS signal
                    SET dead_lettered = TRUE
                    WHERE signal.checkpoint_id = $1
                      AND signal.tenant_id = $2
                      AND NOT signal.consumed
                      AND NOT signal.dead_lettered
                      AND EXISTS (SELECT 1 FROM cancelled)
                    RETURNING signal_id
                ),
                released AS (
                    UPDATE {table}_tenants AS tenant
                    SET
                        outstanding_tasks =
                            GREATEST(tenant.outstanding_tasks - 1, 0),
                        updated_at = clock_timestamp()
                    FROM cancelled
                    WHERE tenant.tenant_id = cancelled.tenant_id
                    RETURNING tenant.tenant_id
                ),
                forfeited AS (
                    SELECT {table}_b_forfeit($1, $2)
                    WHERE EXISTS (SELECT 1 FROM cancelled)
                )
                SELECT
                    EXISTS (SELECT 1 FROM released),
                    EXISTS (
                        SELECT 1 FROM {table}
                        WHERE checkpoint_id = $1 AND tenant_id = $2
                    ),
                    (SELECT COUNT(*) FROM forfeited)
                ",
                table = store.table
            ),
            &[&checkpoint_id.as_uuid(), &tenant_id.as_str()],
        )
        .await
        .map_err(storage)?;
    let cancelled: bool = row.try_get(0).map_err(storage)?;
    let exists: bool = row.try_get(1).map_err(storage)?;
    if cancelled {
        return Ok(WorkflowCancelOutcome::Cancelled);
    }
    if exists {
        Ok(WorkflowCancelOutcome::AlreadyTerminal)
    } else {
        Err(store
            .tenant_scoped_not_found(&tenant_id, checkpoint_id)
            .await?)
    }
}

pub(super) async fn inspect(
    store: &PostgresWorkflowStore,
    tenant_id: WorkflowTenantId,
    signal_id: WorkflowSignalId,
) -> Result<WorkflowSignalSnapshot, WorkflowStoreError> {
    let row = store
        .client
        .query_opt(
            &format!(
                "SELECT tenant_id, checkpoint_id, name, consumed, dead_lettered, \
                   (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT \
                 FROM {table}_signals WHERE signal_id = $1",
                table = store.table
            ),
            &[&signal_id.as_checkpoint_id().as_uuid()],
        )
        .await
        .map_err(storage)?
        .ok_or_else(|| {
            WorkflowStoreError::new(
                WorkflowStoreErrorKind::NotFound,
                "workflow signal does not exist",
            )
        })?;
    let snapshot = decode_signal_snapshot(signal_id, &row)?;
    if snapshot.tenant_id != tenant_id {
        return Err(tenant_mismatch());
    }
    Ok(snapshot)
}

pub(super) async fn compact(
    store: &PostgresWorkflowStore,
    tenant_id: WorkflowTenantId,
    retention: WorkflowSignalRetention,
) -> Result<u64, WorkflowStoreError> {
    let retention = database_i64(retention.as_millis(), "signal retention")?;
    store
        .client
        .execute(
            &format!(
                "DELETE FROM {table}_signals \
                 WHERE tenant_id = $1 AND (consumed OR dead_lettered) \
                   AND created_at <= clock_timestamp() \
                       - ($2::BIGINT * INTERVAL '1 millisecond')",
                table = store.table
            ),
            &[&tenant_id.as_str(), &retention],
        )
        .await
        .map_err(storage)
}
