//! Durable purge approval inbox persistence.

use runifold_core::CheckpointId;
use runifold_workflow::{
    LeaseDuration, WorkerId, WorkflowStoreError, WorkflowStoreErrorKind, WorkflowStoreFuture,
    WorkflowTaskTombstoneApprovalInboxItem, WorkflowTaskTombstoneApprovalInboxLimit,
    WorkflowTaskTombstoneApprovalLease, WorkflowTaskTombstoneApprovalState,
    WorkflowTaskTombstonePurgeId, WorkflowTaskTombstonePurgeIntent,
    WorkflowTaskTombstoneRejectionReason, WorkflowTenantId,
};
use tokio_postgres::Row;
use uuid::Uuid;

use super::{
    super::{
        PostgresWorkflowStore,
        support::{database_i64, decode_u64, storage},
    },
    support::{approval_lease_lost, decode_intent, governance_conflict},
};

pub(in crate::workflow) fn approve_purge(
    store: &PostgresWorkflowStore,
    tenant_id: WorkflowTenantId,
    purge_id: WorkflowTaskTombstonePurgeId,
    approver: WorkerId,
) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstonePurgeIntent, WorkflowStoreError>> {
    Box::pin(async move {
        let row = store
            .client
            .query_opt(
                &format!(
                    "WITH approved AS (
                        UPDATE {table}_tg_purge
                        SET status = 'approved', approved_by = $3,
                            approved_at = clock_timestamp()
                        WHERE purge_id = $1 AND tenant_id = $2
                          AND status = 'pending'
                          AND expires_at > clock_timestamp()
                          AND prepared_by <> $3
                        RETURNING purge_id, tenant_id, prepared_by,
                                  tombstone_count, first_sequence, last_sequence,
                                  export_through, fingerprint,
                                  FLOOR(EXTRACT(EPOCH FROM prepared_at) * 1000)::BIGINT,
                                  FLOOR(EXTRACT(EPOCH FROM expires_at) * 1000)::BIGINT,
                                  approved_by,
                                  FLOOR(EXTRACT(EPOCH FROM approved_at) * 1000)::BIGINT
                     ), marked AS (
                        UPDATE {table}_tg_approval AS approval
                        SET status = 'approved', claimed_by = NULL,
                            claim_expires_at = NULL, updated_at = clock_timestamp()
                        FROM approved
                        WHERE approval.purge_id = approved.purge_id
                        RETURNING approval.purge_id
                     )
                     SELECT * FROM approved
                     UNION ALL
                     SELECT purge_id, tenant_id, prepared_by,
                            tombstone_count, first_sequence, last_sequence,
                            export_through, fingerprint,
                            FLOOR(EXTRACT(EPOCH FROM prepared_at) * 1000)::BIGINT,
                            FLOOR(EXTRACT(EPOCH FROM expires_at) * 1000)::BIGINT,
                            approved_by,
                            FLOOR(EXTRACT(EPOCH FROM approved_at) * 1000)::BIGINT
                     FROM {table}_tg_purge
                     WHERE purge_id = $1 AND tenant_id = $2
                       AND status = 'approved' AND approved_by = $3
                       AND expires_at > clock_timestamp()
                     LIMIT 1",
                    table = store.table
                ),
                &[
                    &purge_id.as_checkpoint_id().as_uuid(),
                    &tenant_id.as_str(),
                    &approver.as_str(),
                ],
            )
            .await
            .map_err(storage)?
            .ok_or_else(|| {
                governance_conflict(
                    "purge approval requires a pending unexpired intent and independent principal",
                )
            })?;
        decode_intent(&row, 0)
    })
}

pub(in crate::workflow) fn list_approvals(
    store: &PostgresWorkflowStore,
    tenant_id: WorkflowTenantId,
    limit: WorkflowTaskTombstoneApprovalInboxLimit,
) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowTaskTombstoneApprovalInboxItem>, WorkflowStoreError>>
{
    Box::pin(async move {
        backfill_approval_rows(store, &tenant_id).await?;
        let rows = store
            .client
            .query(
                &format!(
                    "SELECT intent.purge_id, intent.tenant_id, intent.prepared_by,
                            intent.tombstone_count, intent.first_sequence,
                            intent.last_sequence, intent.export_through,
                            intent.fingerprint,
                            FLOOR(EXTRACT(EPOCH FROM intent.prepared_at) * 1000)::BIGINT,
                            FLOOR(EXTRACT(EPOCH FROM intent.expires_at) * 1000)::BIGINT,
                            intent.approved_by,
                            FLOOR(EXTRACT(EPOCH FROM intent.approved_at) * 1000)::BIGINT,
                            CASE
                              WHEN intent.expires_at <= clock_timestamp()
                                AND intent.status <> 'executed' THEN 'expired'
                              WHEN approval.status = 'claimed'
                                AND approval.claim_expires_at <= clock_timestamp() THEN 'pending'
                              ELSE approval.status
                            END,
                            CASE WHEN approval.status = 'claimed'
                                      AND approval.claim_expires_at > clock_timestamp()
                                 THEN approval.claimed_by END,
                            CASE WHEN approval.status = 'claimed'
                                      AND approval.claim_expires_at > clock_timestamp()
                                 THEN FLOOR(EXTRACT(EPOCH FROM approval.claim_expires_at)
                                      * 1000)::BIGINT END,
                            approval.rejected_by, approval.rejection_reason,
                            FLOOR(EXTRACT(EPOCH FROM approval.rejected_at) * 1000)::BIGINT
                     FROM {table}_tg_approval AS approval
                     JOIN {table}_tg_purge AS intent
                       ON intent.purge_id = approval.purge_id
                     WHERE approval.tenant_id = $1 AND intent.status <> 'executed'
                     ORDER BY intent.prepared_at, intent.purge_id
                     LIMIT $2",
                    table = store.table
                ),
                &[&tenant_id.as_str(), &i64::from(limit.get())],
            )
            .await
            .map_err(storage)?;
        rows.iter().map(decode_approval_item).collect()
    })
}

pub(in crate::workflow) fn claim_approval(
    store: &PostgresWorkflowStore,
    tenant_id: WorkflowTenantId,
    reviewer: WorkerId,
    lease: LeaseDuration,
) -> WorkflowStoreFuture<'_, Result<Option<WorkflowTaskTombstoneApprovalLease>, WorkflowStoreError>>
{
    Box::pin(async move {
        backfill_approval_rows(store, &tenant_id).await?;
        let lease_ms = database_i64(lease.as_millis(), "purge approval lease")?;
        store
            .client
            .query_opt(
                &format!(
                    "WITH candidate AS (
                        SELECT approval.purge_id
                        FROM {table}_tg_approval AS approval
                        JOIN {table}_tg_purge AS intent
                          ON intent.purge_id = approval.purge_id
                        WHERE approval.tenant_id = $1
                          AND (
                              approval.status = 'pending'
                              OR (
                                  approval.status = 'claimed'
                                  AND approval.claim_expires_at <= clock_timestamp()
                              )
                          )
                          AND intent.status = 'pending'
                          AND intent.expires_at > clock_timestamp()
                          AND intent.prepared_by <> $2
                        ORDER BY intent.prepared_at, intent.purge_id
                        FOR UPDATE OF approval SKIP LOCKED
                        LIMIT 1
                     ), claimed AS (
                        UPDATE {table}_tg_approval AS approval
                        SET status = 'claimed', claimed_by = $2,
                            claim_expires_at = LEAST(
                                clock_timestamp()
                                    + ($3::BIGINT * INTERVAL '1 millisecond'),
                                intent.expires_at
                            ),
                            fencing_token = approval.fencing_token + 1,
                            updated_at = clock_timestamp()
                        FROM candidate
                        JOIN {table}_tg_purge AS intent
                          ON intent.purge_id = candidate.purge_id
                        WHERE approval.purge_id = candidate.purge_id
                        RETURNING approval.purge_id, approval.tenant_id,
                                  approval.claimed_by, approval.fencing_token,
                                  FLOOR(EXTRACT(EPOCH FROM approval.claim_expires_at)
                                      * 1000)::BIGINT
                     )
                     SELECT * FROM claimed",
                    table = store.table
                ),
                &[&tenant_id.as_str(), &reviewer.as_str(), &lease_ms],
            )
            .await
            .map_err(storage)?
            .as_ref()
            .map(decode_approval_lease)
            .transpose()
    })
}

pub(in crate::workflow) fn approve_claimed(
    store: &PostgresWorkflowStore,
    lease: WorkflowTaskTombstoneApprovalLease,
) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstonePurgeIntent, WorkflowStoreError>> {
    Box::pin(async move {
        let token = database_i64(lease.fencing_token, "purge approval fencing token")?;
        let row = store
            .client
            .query_opt(
                &format!(
                    "WITH owned AS (
                        UPDATE {table}_tg_approval
                        SET status = 'approved', claimed_by = NULL,
                            claim_expires_at = NULL, updated_at = clock_timestamp()
                        WHERE purge_id = $1 AND tenant_id = $2
                          AND status = 'claimed' AND claimed_by = $3
                          AND fencing_token = $4
                          AND claim_expires_at > clock_timestamp()
                        RETURNING purge_id
                     ), approved AS (
                        UPDATE {table}_tg_purge AS intent
                        SET status = 'approved', approved_by = $3,
                            approved_at = clock_timestamp()
                        FROM owned
                        WHERE intent.purge_id = owned.purge_id
                          AND intent.tenant_id = $2 AND intent.status = 'pending'
                          AND intent.expires_at > clock_timestamp()
                          AND intent.prepared_by <> $3
                        RETURNING intent.purge_id, intent.tenant_id,
                                  intent.prepared_by, intent.tombstone_count,
                                  intent.first_sequence, intent.last_sequence,
                                  intent.export_through, intent.fingerprint,
                                  FLOOR(EXTRACT(EPOCH FROM intent.prepared_at) * 1000)::BIGINT,
                                  FLOOR(EXTRACT(EPOCH FROM intent.expires_at) * 1000)::BIGINT,
                                  intent.approved_by,
                                  FLOOR(EXTRACT(EPOCH FROM intent.approved_at) * 1000)::BIGINT
                     )
                     SELECT * FROM approved",
                    table = store.table
                ),
                &[
                    &lease.purge_id.as_checkpoint_id().as_uuid(),
                    &lease.tenant_id.as_str(),
                    &lease.reviewer.as_str(),
                    &token,
                ],
            )
            .await
            .map_err(storage)?
            .ok_or_else(approval_lease_lost)?;
        decode_intent(&row, 0)
    })
}

pub(in crate::workflow) fn reject_claimed(
    store: &PostgresWorkflowStore,
    lease: WorkflowTaskTombstoneApprovalLease,
    reason: WorkflowTaskTombstoneRejectionReason,
) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstoneApprovalInboxItem, WorkflowStoreError>> {
    Box::pin(async move {
        let token = database_i64(lease.fencing_token, "purge approval fencing token")?;
        let row = store
            .client
            .query_opt(
                &format!(
                    "WITH rejected AS (
                        UPDATE {table}_tg_approval
                        SET status = 'rejected', claimed_by = NULL,
                            claim_expires_at = NULL, rejected_by = $3,
                            rejection_reason = $5, rejected_at = clock_timestamp(),
                            updated_at = clock_timestamp()
                        WHERE purge_id = $1 AND tenant_id = $2
                          AND status = 'claimed' AND claimed_by = $3
                          AND fencing_token = $4
                          AND claim_expires_at > clock_timestamp()
                        RETURNING *
                     )
                     SELECT intent.purge_id, intent.tenant_id, intent.prepared_by,
                            intent.tombstone_count, intent.first_sequence,
                            intent.last_sequence, intent.export_through,
                            intent.fingerprint,
                            FLOOR(EXTRACT(EPOCH FROM intent.prepared_at) * 1000)::BIGINT,
                            FLOOR(EXTRACT(EPOCH FROM intent.expires_at) * 1000)::BIGINT,
                            intent.approved_by,
                            FLOOR(EXTRACT(EPOCH FROM intent.approved_at) * 1000)::BIGINT,
                            rejected.status, rejected.claimed_by,
                            FLOOR(EXTRACT(EPOCH FROM rejected.claim_expires_at) * 1000)::BIGINT,
                            rejected.rejected_by, rejected.rejection_reason,
                            FLOOR(EXTRACT(EPOCH FROM rejected.rejected_at) * 1000)::BIGINT
                     FROM rejected
                     JOIN {table}_tg_purge AS intent
                       ON intent.purge_id = rejected.purge_id
                     WHERE intent.status = 'pending'
                       AND intent.expires_at > clock_timestamp()",
                    table = store.table
                ),
                &[
                    &lease.purge_id.as_checkpoint_id().as_uuid(),
                    &lease.tenant_id.as_str(),
                    &lease.reviewer.as_str(),
                    &token,
                    &reason.as_str(),
                ],
            )
            .await
            .map_err(storage)?
            .ok_or_else(approval_lease_lost)?;
        decode_approval_item(&row)
    })
}

async fn backfill_approval_rows(
    store: &PostgresWorkflowStore,
    tenant_id: &WorkflowTenantId,
) -> Result<(), WorkflowStoreError> {
    store
        .client
        .execute(
            &format!(
                "INSERT INTO {table}_tg_approval (purge_id, tenant_id, status)
                 SELECT purge_id, tenant_id,
                        CASE WHEN status = 'approved' THEN 'approved' ELSE 'pending' END
                 FROM {table}_tg_purge
                 WHERE tenant_id = $1 AND status IN ('pending', 'approved')
                 ON CONFLICT (purge_id) DO NOTHING",
                table = store.table
            ),
            &[&tenant_id.as_str()],
        )
        .await
        .map_err(storage)?;
    Ok(())
}

fn decode_approval_lease(
    row: &Row,
) -> Result<WorkflowTaskTombstoneApprovalLease, WorkflowStoreError> {
    let purge_id: Uuid = row.try_get(0).map_err(storage)?;
    let tenant_id: String = row.try_get(1).map_err(storage)?;
    let reviewer: String = row.try_get(2).map_err(storage)?;
    let fencing_token: i64 = row.try_get(3).map_err(storage)?;
    let expires_at_ms: i64 = row.try_get(4).map_err(storage)?;
    Ok(WorkflowTaskTombstoneApprovalLease {
        tenant_id: WorkflowTenantId::parse(tenant_id)?,
        purge_id: WorkflowTaskTombstonePurgeId::from_checkpoint_id(CheckpointId::from_uuid(
            purge_id,
        )),
        reviewer: WorkerId::parse(reviewer)?,
        fencing_token: decode_u64(fencing_token, "purge approval fencing token")?,
        expires_at_ms: decode_u64(expires_at_ms, "purge approval lease expiration")?,
    })
}

fn decode_approval_item(
    row: &Row,
) -> Result<WorkflowTaskTombstoneApprovalInboxItem, WorkflowStoreError> {
    let state: String = row.try_get(12).map_err(storage)?;
    let claimed_by: Option<String> = row.try_get(13).map_err(storage)?;
    let claim_expires_at_ms: Option<i64> = row.try_get(14).map_err(storage)?;
    let rejected_by: Option<String> = row.try_get(15).map_err(storage)?;
    let rejection_reason: Option<String> = row.try_get(16).map_err(storage)?;
    let rejected_at_ms: Option<i64> = row.try_get(17).map_err(storage)?;
    Ok(WorkflowTaskTombstoneApprovalInboxItem {
        intent: decode_intent(row, 0)?,
        state: decode_approval_state(&state)?,
        claimed_by: claimed_by.map(WorkerId::parse).transpose()?,
        claim_expires_at_ms: claim_expires_at_ms
            .map(|value| decode_u64(value, "purge approval claim expiration"))
            .transpose()?,
        rejected_by: rejected_by.map(WorkerId::parse).transpose()?,
        rejection_reason: rejection_reason
            .map(WorkflowTaskTombstoneRejectionReason::parse)
            .transpose()?,
        rejected_at_ms: rejected_at_ms
            .map(|value| decode_u64(value, "purge rejection time"))
            .transpose()?,
    })
}

fn decode_approval_state(
    value: &str,
) -> Result<WorkflowTaskTombstoneApprovalState, WorkflowStoreError> {
    match value {
        "pending" => Ok(WorkflowTaskTombstoneApprovalState::Pending),
        "claimed" => Ok(WorkflowTaskTombstoneApprovalState::Claimed),
        "approved" => Ok(WorkflowTaskTombstoneApprovalState::Approved),
        "rejected" => Ok(WorkflowTaskTombstoneApprovalState::Rejected),
        "expired" => Ok(WorkflowTaskTombstoneApprovalState::Expired),
        _ => Err(WorkflowStoreError::new(
            WorkflowStoreErrorKind::Storage,
            "stored Task tombstone approval state is invalid",
        )),
    }
}
