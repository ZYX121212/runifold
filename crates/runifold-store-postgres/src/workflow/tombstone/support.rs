//! Shared tombstone row decoding and store error mapping.

use runifold_core::CheckpointId;
use runifold_workflow::{
    WorkerId, WorkflowStoreError, WorkflowStoreErrorKind, WorkflowTaskLegalHold,
    WorkflowTaskLegalHoldReason, WorkflowTaskTombstoneCursor, WorkflowTaskTombstoneExport,
    WorkflowTaskTombstoneExportReceipt, WorkflowTaskTombstonePurgeEvidence,
    WorkflowTaskTombstonePurgeId, WorkflowTaskTombstonePurgeIntent, WorkflowTenantId,
};
use tokio_postgres::Row;
use uuid::Uuid;

use super::super::support::{decode_u64, storage};

pub(super) fn decode_hold(row: &Row) -> Result<WorkflowTaskLegalHold, WorkflowStoreError> {
    let checkpoint_id: Uuid = row.try_get(0).map_err(storage)?;
    let tenant_id: String = row.try_get(1).map_err(storage)?;
    let placed_by: String = row.try_get(2).map_err(storage)?;
    let reason: String = row.try_get(3).map_err(storage)?;
    let released_by: Option<String> = row.try_get(5).map_err(storage)?;
    let released_at: Option<i64> = row.try_get(6).map_err(storage)?;
    Ok(WorkflowTaskLegalHold {
        checkpoint_id: CheckpointId::from_uuid(checkpoint_id),
        tenant_id: WorkflowTenantId::parse(tenant_id)?,
        placed_by: WorkerId::parse(placed_by)?,
        reason: WorkflowTaskLegalHoldReason::parse(reason)?,
        placed_at_ms: decode_u64(row.try_get(4).map_err(storage)?, "hold placement time")?,
        released_by: released_by.map(WorkerId::parse).transpose()?,
        released_at_ms: released_at
            .map(|value| decode_u64(value, "hold release time"))
            .transpose()?,
    })
}

pub(super) fn decode_export(row: &Row) -> Result<WorkflowTaskTombstoneExport, WorkflowStoreError> {
    let tenant_id: String = row.try_get(0).map_err(storage)?;
    let through: i64 = row.try_get(1).map_err(storage)?;
    let receipt: String = row.try_get(2).map_err(storage)?;
    let actor: String = row.try_get(3).map_err(storage)?;
    Ok(WorkflowTaskTombstoneExport {
        tenant_id: WorkflowTenantId::parse(tenant_id)?,
        through: WorkflowTaskTombstoneCursor::new(decode_u64(through, "export cursor")?),
        receipt: WorkflowTaskTombstoneExportReceipt::parse(receipt)?,
        confirmed_by: WorkerId::parse(actor)?,
        confirmed_at_ms: decode_u64(row.try_get(4).map_err(storage)?, "export confirmation time")?,
    })
}

pub(super) fn decode_intent(
    row: &Row,
    offset: usize,
) -> Result<WorkflowTaskTombstonePurgeIntent, WorkflowStoreError> {
    let purge_id: Uuid = row.try_get(offset).map_err(storage)?;
    let tenant_id: String = row.try_get(offset + 1).map_err(storage)?;
    let prepared_by: String = row.try_get(offset + 2).map_err(storage)?;
    let count: i32 = row.try_get(offset + 3).map_err(storage)?;
    let first: i64 = row.try_get(offset + 4).map_err(storage)?;
    let last: i64 = row.try_get(offset + 5).map_err(storage)?;
    let export: i64 = row.try_get(offset + 6).map_err(storage)?;
    let approved_by: Option<String> = row.try_get(offset + 10).map_err(storage)?;
    let approved_at: Option<i64> = row.try_get(offset + 11).map_err(storage)?;
    Ok(WorkflowTaskTombstonePurgeIntent {
        purge_id: WorkflowTaskTombstonePurgeId::from_checkpoint_id(CheckpointId::from_uuid(
            purge_id,
        )),
        tenant_id: WorkflowTenantId::parse(tenant_id)?,
        prepared_by: WorkerId::parse(prepared_by)?,
        tombstone_count: decode_count(count)?,
        first_cursor: Some(WorkflowTaskTombstoneCursor::new(decode_u64(
            first,
            "purge first cursor",
        )?)),
        last_cursor: Some(WorkflowTaskTombstoneCursor::new(decode_u64(
            last,
            "purge last cursor",
        )?)),
        export_through: WorkflowTaskTombstoneCursor::new(decode_u64(
            export,
            "purge export cursor",
        )?),
        fingerprint: row.try_get(offset + 7).map_err(storage)?,
        prepared_at_ms: decode_u64(
            row.try_get(offset + 8).map_err(storage)?,
            "purge preparation time",
        )?,
        expires_at_ms: decode_u64(
            row.try_get(offset + 9).map_err(storage)?,
            "purge expiration",
        )?,
        approved_by: approved_by.map(WorkerId::parse).transpose()?,
        approved_at_ms: approved_at
            .map(|value| decode_u64(value, "purge approval time"))
            .transpose()?,
    })
}

pub(super) fn decode_evidence(
    row: &Row,
    offset: usize,
) -> Result<WorkflowTaskTombstonePurgeEvidence, WorkflowStoreError> {
    let purge_id: Uuid = row.try_get(offset).map_err(storage)?;
    let tenant_id: String = row.try_get(offset + 1).map_err(storage)?;
    let prepared_by: String = row.try_get(offset + 2).map_err(storage)?;
    let approved_by: String = row.try_get(offset + 3).map_err(storage)?;
    let executed_by: String = row.try_get(offset + 4).map_err(storage)?;
    let count: i32 = row.try_get(offset + 5).map_err(storage)?;
    let first: i64 = row.try_get(offset + 6).map_err(storage)?;
    let last: i64 = row.try_get(offset + 7).map_err(storage)?;
    let export: i64 = row.try_get(offset + 8).map_err(storage)?;
    Ok(WorkflowTaskTombstonePurgeEvidence {
        purge_id: WorkflowTaskTombstonePurgeId::from_checkpoint_id(CheckpointId::from_uuid(
            purge_id,
        )),
        tenant_id: WorkflowTenantId::parse(tenant_id)?,
        prepared_by: WorkerId::parse(prepared_by)?,
        approved_by: WorkerId::parse(approved_by)?,
        executed_by: WorkerId::parse(executed_by)?,
        tombstone_count: decode_count(count)?,
        first_cursor: WorkflowTaskTombstoneCursor::new(decode_u64(
            first,
            "purge evidence first cursor",
        )?),
        last_cursor: WorkflowTaskTombstoneCursor::new(decode_u64(
            last,
            "purge evidence last cursor",
        )?),
        export_through: WorkflowTaskTombstoneCursor::new(decode_u64(
            export,
            "purge evidence export cursor",
        )?),
        fingerprint: row.try_get(offset + 9).map_err(storage)?,
        executed_at_ms: decode_u64(
            row.try_get(offset + 10).map_err(storage)?,
            "purge execution time",
        )?,
    })
}

fn decode_count(value: i32) -> Result<u32, WorkflowStoreError> {
    u32::try_from(value).map_err(|_| {
        WorkflowStoreError::new(
            WorkflowStoreErrorKind::Storage,
            "stored Task tombstone purge count is invalid",
        )
    })
}

pub(super) fn cleanup_lease_lost() -> WorkflowStoreError {
    WorkflowStoreError::new(
        WorkflowStoreErrorKind::LeaseLost,
        "workflow Task cleanup lease is expired, superseded, or owned by another worker",
    )
}

pub(super) fn approval_lease_lost() -> WorkflowStoreError {
    WorkflowStoreError::new(
        WorkflowStoreErrorKind::LeaseLost,
        "purge approval lease is expired, superseded, or owned by another reviewer",
    )
}

pub(super) fn governance_conflict(message: &'static str) -> WorkflowStoreError {
    WorkflowStoreError::new(WorkflowStoreErrorKind::Conflict, message)
}

pub(super) fn governance_not_found(message: &'static str) -> WorkflowStoreError {
    WorkflowStoreError::new(WorkflowStoreErrorKind::NotFound, message)
}
