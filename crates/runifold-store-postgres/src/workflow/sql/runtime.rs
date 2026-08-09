//! Runtime claim, heartbeat, and signal SQL statement builders.

use super::super::PostgresWorkflowStore;

impl PostgresWorkflowStore {
    pub(in crate::workflow) fn claim_sql(&self) -> String {
        Self::claim_sql_for(&self.table)
    }

    pub(in crate::workflow) fn claim_sql_for(table: &str) -> String {
        format!(
            r"
            WITH candidate AS (
                SELECT
                    task.checkpoint_id, task.tenant_id, pending.signal_id,
                    pending.name AS signal_name,
                    pending.payload AS signal_payload
                FROM {table} AS task
                JOIN {table}_tenants AS tenant ON tenant.tenant_id = task.tenant_id
                LEFT JOIN LATERAL (
                    SELECT signal_id, name, payload
                    FROM {table}_signals
                    WHERE checkpoint_id = task.checkpoint_id AND tenant_id = task.tenant_id
                      AND name = task.wait_name AND NOT consumed AND NOT dead_lettered
                      AND (task.wake_at IS NULL OR created_at < task.wake_at)
                    ORDER BY created_at ASC, signal_id ASC
                    LIMIT 1
                ) AS pending ON task.state = 'waiting_signal'
                WHERE (
                    (task.state = 'queued' AND task.available_at <= clock_timestamp())
                    OR (task.state = 'leased' AND task.lease_expires_at <= clock_timestamp())
                    OR (task.state = 'waiting_timer' AND task.wake_at <= clock_timestamp())
                    OR (
                        task.state = 'waiting_signal' AND (
                            pending.signal_id IS NOT NULL OR task.wake_at <= clock_timestamp()
                        )
                    )
                )
                  AND pg_try_advisory_xact_lock(hashtextextended(task.tenant_id, 0)) AND (
                    SELECT COUNT(*)
                    FROM {table} AS active
                    WHERE active.tenant_id = task.tenant_id
                      AND active.state = 'leased'
                      AND active.lease_expires_at > clock_timestamp()
                  ) < tenant.max_concurrent_leases
                ORDER BY
                    tenant.last_claim_sequence ASC,
                    task.priority DESC,
                    CASE
                        WHEN task.state = 'queued' THEN task.available_at
                        WHEN task.state = 'waiting_timer' THEN task.wake_at
                        WHEN task.state = 'waiting_signal' THEN task.updated_at ELSE task.lease_expires_at
                    END ASC,
                    task.checkpoint_id ASC
                FOR UPDATE OF task, tenant SKIP LOCKED
                LIMIT 1
            ),
            claimed_tenant AS (
                UPDATE {table}_tenants AS tenant
                SET last_claim_sequence = nextval('{table}_claim_seq'),
                    updated_at = clock_timestamp()
                FROM candidate
                WHERE tenant.tenant_id = candidate.tenant_id
                RETURNING tenant.tenant_id
            ),
            consumed AS (
                UPDATE {table}_signals AS signal
                SET consumed = TRUE
                FROM candidate
                WHERE signal.signal_id = candidate.signal_id
                RETURNING signal.signal_id
            )
            UPDATE {table} AS task
            SET
                state = 'leased',
                owner = $1,
                fencing_token = task.fencing_token + 1,
                attempts = task.attempts + 1,
                lease_expires_at =
                    clock_timestamp() + ($2::BIGINT * INTERVAL '1 millisecond'),
                wake = CASE
                    WHEN candidate.signal_id IS NOT NULL THEN jsonb_build_object(
                        'kind', 'signal',
                        'signal_id', candidate.signal_id,
                        'name', candidate.signal_name,
                        'payload', candidate.signal_payload
                    )
                    WHEN task.state = 'waiting_timer' THEN jsonb_build_object('kind', 'timer')
                    WHEN task.state = 'waiting_signal' AND task.wake_at <= clock_timestamp()
                        THEN jsonb_build_object('kind', 'timeout')
                    ELSE task.wake
                END,
                wait_kind = NULL, wait_name = NULL, wait = NULL,
                wake_at = NULL,
                failure_reason = NULL,
                updated_at = clock_timestamp()
            FROM candidate, claimed_tenant
            WHERE task.checkpoint_id = candidate.checkpoint_id
            RETURNING
                task.checkpoint_id, task.tenant_id,
                task.workflow,
                task.workflow_version,
                task.input,
                task.priority,
                task.fencing_token,
                task.attempts,
                (EXTRACT(EPOCH FROM task.lease_expires_at) * 1000)::BIGINT,
                task.wake
            "
        )
    }

    pub(in crate::workflow) fn heartbeat_sql(&self) -> String {
        Self::heartbeat_sql_for(&self.table)
    }

    pub(in crate::workflow) fn heartbeat_sql_for(table: &str) -> String {
        format!(
            r"
            WITH renewed AS (
                UPDATE {table}
                SET
                    lease_expires_at = GREATEST(
                        lease_expires_at,
                        clock_timestamp() + ($5::BIGINT * INTERVAL '1 millisecond')
                    ),
                    updated_at = clock_timestamp()
                WHERE checkpoint_id = $1
                  AND tenant_id = $2
                  AND state = 'leased'
                  AND owner = $3
                  AND fencing_token = $4
                  AND lease_expires_at > clock_timestamp()
                RETURNING checkpoint_id, tenant_id, attempts, lease_expires_at
            ),
            extended AS (
                UPDATE {table}_budgets AS reservation
                SET
                    expires_at = renewed.lease_expires_at
                        + (tenant.budget_recovery_grace_ms
                            * INTERVAL '1 millisecond'),
                    updated_at = clock_timestamp()
                FROM renewed
                JOIN {table}_tenants AS tenant
                  ON tenant.tenant_id = renewed.tenant_id
                WHERE reservation.checkpoint_id = renewed.checkpoint_id
                RETURNING reservation.checkpoint_id
            )
            SELECT
                renewed.attempts,
                (EXTRACT(EPOCH FROM renewed.lease_expires_at) * 1000)::BIGINT
            FROM renewed
            "
        )
    }

    pub(in crate::workflow) fn publish_signal_sql(&self) -> String {
        format!(
            r"
            WITH target AS (
                SELECT checkpoint_id, state, wait_name, wake_at
                FROM {table}
                WHERE checkpoint_id = $2 AND tenant_id = $3
                FOR UPDATE
            ),
            inserted AS (
                INSERT INTO {table}_signals (
                    signal_id, tenant_id, checkpoint_id, name, payload,
                    consumed, dead_lettered, compaction_protected
                )
                SELECT
                    $1,
                    $3,
                    $2,
                    $4,
                    $5,
                    EXISTS (
                        SELECT 1 FROM target
                        WHERE state = 'waiting_signal'
                          AND wait_name = $4
                          AND (wake_at IS NULL OR clock_timestamp() < wake_at)
                    ),
                    EXISTS (
                        SELECT 1 FROM target
                        WHERE state IN ('completed', 'failed', 'cancelled')
                           OR (
                               state = 'waiting_signal'
                               AND wait_name = $4
                               AND wake_at IS NOT NULL
                               AND clock_timestamp() >= wake_at
                           )
                    ),
                    $7
                WHERE EXISTS (SELECT 1 FROM target)
                ON CONFLICT (signal_id) DO NOTHING
                RETURNING signal_id, consumed, dead_lettered
            ),
            woke AS (
                UPDATE {table}
                SET
                    state = 'queued',
                    available_at = clock_timestamp(),
                    wait_kind = NULL,
                    wait_name = NULL,
                    wait = NULL,
                    wake_at = NULL,
                    wake = $6,
                    updated_at = clock_timestamp()
                WHERE checkpoint_id = $2
                  AND tenant_id = $3
                  AND state = 'waiting_signal'
                  AND wait_name = $4
                  AND EXISTS (SELECT 1 FROM inserted WHERE consumed)
                RETURNING checkpoint_id
            )
            SELECT
                EXISTS (SELECT 1 FROM inserted),
                EXISTS (SELECT 1 FROM woke),
                EXISTS (SELECT 1 FROM inserted WHERE dead_lettered)
            ",
            table = self.table
        )
    }
}
