//! Prepared purge, atomic execution, and evidence persistence.

use runifold_workflow::{
    WorkflowStoreError, WorkflowStoreFuture, WorkflowTaskCleanupLease,
    WorkflowTaskTombstoneApprovalWindow, WorkflowTaskTombstonePurgeEvidence,
    WorkflowTaskTombstonePurgeId, WorkflowTaskTombstonePurgeIntent,
    WorkflowTaskTombstonePurgeLimit, WorkflowTaskTombstoneRetention, WorkflowTenantId,
};
use uuid::Uuid;

use super::{
    super::{
        PostgresWorkflowStore,
        support::{database_i64, storage},
    },
    support::{
        cleanup_lease_lost, decode_evidence, decode_intent, governance_conflict,
        governance_not_found,
    },
};

#[expect(
    clippy::too_many_lines,
    reason = "the atomic prepared-set SQL is kept contiguous for auditability"
)]
pub(in crate::workflow) fn prepare_purge(
    store: &PostgresWorkflowStore,
    lease: WorkflowTaskCleanupLease,
    retention: WorkflowTaskTombstoneRetention,
    limit: WorkflowTaskTombstonePurgeLimit,
    approval_window: WorkflowTaskTombstoneApprovalWindow,
) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstonePurgeIntent, WorkflowStoreError>> {
    Box::pin(async move {
        let purge_id = WorkflowTaskTombstonePurgeId::new();
        let token = database_i64(lease.fencing_token, "Task cleanup fencing token")?;
        let retention_ms = database_i64(retention.as_millis(), "Task tombstone retention")?;
        let approval_ms = database_i64(approval_window.as_millis(), "Task purge approval window")?;
        let limit = i64::from(limit.get());
        let row = store
            .client
            .query_one(
                &format!(
                    "WITH valid AS (
                        SELECT TRUE AS held
                        FROM {table}_task_cleanup
                        WHERE tenant_id = $1 AND owner = $2
                          AND fencing_token = $3
                          AND lease_expires_at > clock_timestamp()
                     ), exported AS (
                        SELECT through_sequence
                        FROM {table}_tg_export
                        WHERE tenant_id = $1
                     ), candidates AS MATERIALIZED (
                        SELECT tombstone.sequence, tombstone.checkpoint_id
                        FROM {table}_task_tombstones AS tombstone
                        CROSS JOIN valid
                        CROSS JOIN exported
                        WHERE tombstone.tenant_id = $1
                          AND tombstone.sequence <= exported.through_sequence
                          AND tombstone.deleted_at <= clock_timestamp()
                              - ($4::BIGINT * INTERVAL '1 millisecond')
                          AND NOT EXISTS (
                              SELECT 1
                              FROM {table}_tg_hold AS hold
                              WHERE hold.checkpoint_id = tombstone.checkpoint_id
                                AND hold.released_at IS NULL
                          )
                          AND NOT EXISTS (
                              SELECT 1
                              FROM {table}_tg_item AS item
                              JOIN {table}_tg_purge AS intent
                                ON intent.purge_id = item.purge_id
                              WHERE item.tombstone_sequence = tombstone.sequence
                                AND intent.status <> 'executed'
                                AND intent.expires_at > clock_timestamp()
                          )
                        ORDER BY tombstone.sequence
                        LIMIT $5
                        FOR UPDATE OF tombstone SKIP LOCKED
                     ), prepared AS (
                        INSERT INTO {table}_tg_purge (
                            purge_id, tenant_id, prepared_by, tombstone_count,
                            first_sequence, last_sequence, export_through,
                            fingerprint, status, expires_at
                        )
                        SELECT $6, $1, $2, COUNT(*)::INTEGER,
                               MIN(sequence), MAX(sequence),
                               (SELECT through_sequence FROM exported),
                               MD5(STRING_AGG(
                                   checkpoint_id::TEXT || ':' || sequence::TEXT,
                                   ',' ORDER BY sequence
                               )),
                               'pending',
                               clock_timestamp()
                                   + ($7::BIGINT * INTERVAL '1 millisecond')
                        FROM candidates
                        HAVING COUNT(*) > 0
                        RETURNING purge_id, tenant_id, prepared_by,
                                  tombstone_count, first_sequence, last_sequence,
                                  export_through, fingerprint,
                                  FLOOR(EXTRACT(EPOCH FROM prepared_at) * 1000)::BIGINT
                                      AS prepared_at,
                                  FLOOR(EXTRACT(EPOCH FROM expires_at) * 1000)::BIGINT
                                      AS expires_at,
                                  approved_by,
                                  FLOOR(EXTRACT(EPOCH FROM approved_at) * 1000)::BIGINT
                                      AS approved_at
                     ), items AS (
                        INSERT INTO {table}_tg_item (
                            purge_id, tombstone_sequence
                        )
                        SELECT prepared.purge_id, candidates.sequence
                        FROM prepared CROSS JOIN candidates
                        RETURNING tombstone_sequence
                     ), approval AS (
                        INSERT INTO {table}_tg_approval (
                            purge_id, tenant_id
                        )
                        SELECT purge_id, tenant_id FROM prepared
                        RETURNING purge_id
                     )
                     SELECT EXISTS(SELECT 1 FROM valid),
                            (SELECT through_sequence FROM exported),
                            prepared.purge_id, prepared.tenant_id,
                            prepared.prepared_by, prepared.tombstone_count,
                            prepared.first_sequence, prepared.last_sequence,
                            prepared.export_through, prepared.fingerprint,
                            prepared.prepared_at, prepared.expires_at,
                            prepared.approved_by, prepared.approved_at
                     FROM (VALUES (TRUE)) AS root(dummy)
                     LEFT JOIN prepared ON TRUE",
                    table = store.table
                ),
                &[
                    &lease.tenant_id.as_str(),
                    &lease.owner.as_str(),
                    &token,
                    &retention_ms,
                    &limit,
                    &purge_id.as_checkpoint_id().as_uuid(),
                    &approval_ms,
                ],
            )
            .await
            .map_err(storage)?;
        let held: bool = row.try_get(0).map_err(storage)?;
        if !held {
            return Err(cleanup_lease_lost());
        }
        if row.try_get::<_, Option<i64>>(1).map_err(storage)?.is_none() {
            return Err(governance_conflict(
                "Task tombstones must be export-confirmed before purge preparation",
            ));
        }
        if row
            .try_get::<_, Option<Uuid>>(2)
            .map_err(storage)?
            .is_none()
        {
            return Err(governance_not_found(
                "no exported, unheld tombstones satisfy purge retention",
            ));
        }
        decode_intent(&row, 2)
    })
}

#[expect(
    clippy::too_many_lines,
    reason = "the atomic delete-and-evidence SQL is kept contiguous for auditability"
)]
pub(in crate::workflow) fn execute_purge(
    store: &PostgresWorkflowStore,
    lease: WorkflowTaskCleanupLease,
    purge_id: WorkflowTaskTombstonePurgeId,
) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstonePurgeEvidence, WorkflowStoreError>> {
    Box::pin(async move {
        let token = database_i64(lease.fencing_token, "Task cleanup fencing token")?;
        let row = store
            .client
            .query_one(
                &format!(
                    "WITH valid AS (
                        SELECT TRUE AS held
                        FROM {table}_task_cleanup
                        WHERE tenant_id = $1 AND owner = $2
                          AND fencing_token = $3
                          AND lease_expires_at > clock_timestamp()
                     ), existing AS (
                        SELECT purge_id, tenant_id, prepared_by, approved_by,
                               executed_by, tombstone_count, first_sequence,
                               last_sequence, export_through, fingerprint,
                               FLOOR(EXTRACT(EPOCH FROM executed_at) * 1000)::BIGINT
                                   AS executed_at
                        FROM {table}_tg_evidence
                        WHERE purge_id = $4 AND tenant_id = $1
                     ), ready AS (
                        SELECT intent.*
                        FROM {table}_tg_purge AS intent
                        CROSS JOIN valid
                        WHERE intent.purge_id = $4 AND intent.tenant_id = $1
                          AND intent.status = 'approved'
                          AND intent.expires_at > clock_timestamp()
                          AND (
                              SELECT COUNT(*)
                              FROM {table}_tg_item AS item
                              WHERE item.purge_id = intent.purge_id
                          ) = intent.tombstone_count
                          AND (
                              SELECT COUNT(*)
                              FROM {table}_task_tombstones AS tombstone
                              JOIN {table}_tg_item AS item
                                ON item.tombstone_sequence = tombstone.sequence
                              WHERE item.purge_id = intent.purge_id
                                AND tombstone.tenant_id = intent.tenant_id
                          ) = intent.tombstone_count
                          AND NOT EXISTS (
                              SELECT 1
                              FROM {table}_tg_hold AS hold
                              JOIN {table}_task_tombstones AS tombstone
                                ON tombstone.checkpoint_id = hold.checkpoint_id
                              JOIN {table}_tg_item AS item
                                ON item.tombstone_sequence = tombstone.sequence
                              WHERE item.purge_id = intent.purge_id
                                AND hold.released_at IS NULL
                          )
                     ), deleted AS (
                        DELETE FROM {table}_task_tombstones AS tombstone
                        USING {table}_tg_item AS item, ready
                        WHERE item.purge_id = ready.purge_id
                          AND tombstone.sequence = item.tombstone_sequence
                        RETURNING tombstone.sequence
                     ), recorded AS (
                        INSERT INTO {table}_tg_evidence (
                            purge_id, tenant_id, prepared_by, approved_by,
                            executed_by, tombstone_count, first_sequence,
                            last_sequence, export_through, fingerprint
                        )
                        SELECT purge_id, tenant_id, prepared_by, approved_by,
                               $2, tombstone_count, first_sequence,
                               last_sequence, export_through, fingerprint
                        FROM ready
                        WHERE (SELECT COUNT(*) FROM deleted) = tombstone_count
                        RETURNING purge_id, tenant_id, prepared_by, approved_by,
                                  executed_by, tombstone_count, first_sequence,
                                  last_sequence, export_through, fingerprint,
                                  FLOOR(EXTRACT(EPOCH FROM executed_at) * 1000)::BIGINT
                                      AS executed_at
                     ), marked AS (
                        UPDATE {table}_tg_purge AS intent
                        SET status = 'executed', executed_at = clock_timestamp()
                        FROM recorded
                        WHERE intent.purge_id = recorded.purge_id
                        RETURNING intent.purge_id
                     ), removed_items AS (
                        DELETE FROM {table}_tg_item AS item
                        USING recorded
                        WHERE item.purge_id = recorded.purge_id
                     ), evidence AS (
                        SELECT * FROM existing
                        UNION ALL
                        SELECT * FROM recorded
                     )
                     SELECT COALESCE(valid.held, FALSE),
                            evidence.purge_id, evidence.tenant_id,
                            evidence.prepared_by, evidence.approved_by,
                            evidence.executed_by, evidence.tombstone_count,
                            evidence.first_sequence, evidence.last_sequence,
                            evidence.export_through, evidence.fingerprint,
                            evidence.executed_at
                     FROM (VALUES (TRUE)) AS root(dummy)
                     LEFT JOIN valid ON TRUE
                     LEFT JOIN evidence ON TRUE
                     LIMIT 1",
                    table = store.table
                ),
                &[
                    &lease.tenant_id.as_str(),
                    &lease.owner.as_str(),
                    &token,
                    &purge_id.as_checkpoint_id().as_uuid(),
                ],
            )
            .await
            .map_err(storage)?;
        let held: bool = row.try_get(0).map_err(storage)?;
        if !held {
            return Err(cleanup_lease_lost());
        }
        if row
            .try_get::<_, Option<Uuid>>(1)
            .map_err(storage)?
            .is_none()
        {
            return Err(governance_conflict(
                "purge execution requires an approved unexpired complete set without legal holds",
            ));
        }
        decode_evidence(&row, 1)
    })
}

pub(in crate::workflow) fn get_evidence(
    store: &PostgresWorkflowStore,
    tenant_id: WorkflowTenantId,
    purge_id: WorkflowTaskTombstonePurgeId,
) -> WorkflowStoreFuture<'_, Result<Option<WorkflowTaskTombstonePurgeEvidence>, WorkflowStoreError>>
{
    Box::pin(async move {
        store
            .client
            .query_opt(
                &format!(
                    "SELECT purge_id, tenant_id, prepared_by, approved_by,
                            executed_by, tombstone_count, first_sequence,
                            last_sequence, export_through, fingerprint,
                            FLOOR(EXTRACT(EPOCH FROM executed_at) * 1000)::BIGINT
                     FROM {table}_tg_evidence
                     WHERE purge_id = $1 AND tenant_id = $2",
                    table = store.table
                ),
                &[&purge_id.as_checkpoint_id().as_uuid(), &tenant_id.as_str()],
            )
            .await
            .map_err(storage)?
            .as_ref()
            .map(|row| decode_evidence(row, 0))
            .transpose()
    })
}
