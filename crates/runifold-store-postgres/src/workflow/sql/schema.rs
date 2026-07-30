//! Core workflow, checkpoint-history, and signal schema builders.

use super::super::PostgresWorkflowStore;

impl PostgresWorkflowStore {
    pub(in crate::workflow) fn task_schema_sql(table: &str) -> String {
        format!(
            r"
            CREATE TABLE IF NOT EXISTS {table} (
                checkpoint_id UUID PRIMARY KEY, tenant_id TEXT NOT NULL,
                workflow TEXT NOT NULL,
                workflow_version INTEGER NOT NULL CHECK (workflow_version > 0),
                input JSONB NOT NULL, priority INTEGER NOT NULL,
                state TEXT NOT NULL CHECK (state IN (
                    'queued', 'leased', 'waiting_timer', 'waiting_signal',
                    'completed', 'failed', 'cancelled'
                )),
                available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                owner TEXT, fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
                attempts BIGINT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
                lease_expires_at TIMESTAMPTZ, failure_reason TEXT,
                wait_kind TEXT, wait_name TEXT, wait JSONB,
                wake_at TIMESTAMPTZ, wake JSONB,
                checkpoint_revision BIGINT CHECK (checkpoint_revision >= 0), checkpoint JSONB,
                lineage JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                CHECK ((state = 'leased') = (owner IS NOT NULL AND lease_expires_at IS NOT NULL)),
                CHECK ((checkpoint_revision IS NULL) = (checkpoint IS NULL))
            );
            ALTER TABLE {table}
                ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default';
            ALTER TABLE {table} ADD COLUMN IF NOT EXISTS wait_kind TEXT;
            ALTER TABLE {table} ADD COLUMN IF NOT EXISTS wait_name TEXT;
            ALTER TABLE {table} ADD COLUMN IF NOT EXISTS wait JSONB;
            ALTER TABLE {table} ADD COLUMN IF NOT EXISTS wake_at TIMESTAMPTZ;
            ALTER TABLE {table} ADD COLUMN IF NOT EXISTS wake JSONB;
            ALTER TABLE {table} ADD COLUMN IF NOT EXISTS lineage JSONB;
            ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {table}_state_check;
            ALTER TABLE {table} ADD CONSTRAINT {table}_state_check CHECK (state IN (
                'queued', 'leased', 'waiting_timer', 'waiting_signal',
                'completed', 'failed', 'cancelled'
            ));
            DROP INDEX IF EXISTS {table}_claim_idx;
            CREATE INDEX {table}_claim_idx ON {table}
                (tenant_id, priority DESC, available_at ASC, checkpoint_id ASC)
                WHERE state IN ('queued', 'leased', 'waiting_timer', 'waiting_signal');
            CREATE TABLE IF NOT EXISTS {table}_tenants (
                tenant_id TEXT PRIMARY KEY,
                max_outstanding_tasks BIGINT NOT NULL CHECK (max_outstanding_tasks > 0),
                max_concurrent_leases BIGINT NOT NULL CHECK (max_concurrent_leases > 0),
                outstanding_tasks BIGINT NOT NULL DEFAULT 0 CHECK (outstanding_tasks >= 0),
                last_claim_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_claim_sequence >= 0),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                CHECK (max_concurrent_leases <= max_outstanding_tasks)
            );
            ALTER TABLE {table}_tenants
                ADD COLUMN IF NOT EXISTS outstanding_tasks BIGINT NOT NULL DEFAULT 0;
            CREATE SEQUENCE IF NOT EXISTS {table}_claim_seq;
            INSERT INTO {table}_tenants (
                tenant_id, max_outstanding_tasks, max_concurrent_leases
            )
            SELECT DISTINCT tenant_id, 10000, 100 FROM {table}
            ON CONFLICT (tenant_id) DO NOTHING;
            UPDATE {table}_tenants AS tenant SET outstanding_tasks = (
                SELECT COUNT(*) FROM {table} AS task
                WHERE task.tenant_id = tenant.tenant_id
                  AND task.state NOT IN ('completed', 'failed', 'cancelled')
            );
            "
        )
    }

    pub(in crate::workflow) fn checkpoint_history_schema_sql(table: &str) -> String {
        format!(
            r"
            CREATE TABLE IF NOT EXISTS {table}_checkpoint_history (
                checkpoint_id UUID NOT NULL,
                revision BIGINT NOT NULL CHECK (revision >= 0),
                tenant_id TEXT NOT NULL,
                checkpoint JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (checkpoint_id, revision)
            );
            CREATE INDEX IF NOT EXISTS {table}_checkpoint_history_tenant_idx
                ON {table}_checkpoint_history (tenant_id, checkpoint_id, revision);
            CREATE OR REPLACE FUNCTION {table}_capture_checkpoint()
            RETURNS TRIGGER LANGUAGE plpgsql AS $$
            DECLARE
                should_capture BOOLEAN := FALSE;
            BEGIN
                IF TG_OP = 'INSERT' THEN
                    should_capture := NEW.checkpoint IS NOT NULL;
                ELSE
                    should_capture := NEW.checkpoint IS NOT NULL
                        AND OLD.checkpoint_revision IS DISTINCT FROM NEW.checkpoint_revision;
                END IF;
                IF should_capture THEN
                    INSERT INTO {table}_checkpoint_history (
                        checkpoint_id, revision, tenant_id, checkpoint
                    ) VALUES (
                        NEW.checkpoint_id, NEW.checkpoint_revision,
                        NEW.tenant_id, NEW.checkpoint
                    )
                    ON CONFLICT (checkpoint_id, revision) DO NOTHING;
                END IF;
                RETURN NEW;
            END
            $$;
            DROP TRIGGER IF EXISTS {table}_capture_checkpoint_trigger ON {table};
            CREATE TRIGGER {table}_capture_checkpoint_trigger
                AFTER INSERT OR UPDATE OF checkpoint ON {table}
                FOR EACH ROW EXECUTE FUNCTION {table}_capture_checkpoint();
            INSERT INTO {table}_checkpoint_history (
                checkpoint_id, revision, tenant_id, checkpoint
            )
            SELECT checkpoint_id, checkpoint_revision, tenant_id, checkpoint
            FROM {table}
            WHERE checkpoint IS NOT NULL
            ON CONFLICT (checkpoint_id, revision) DO NOTHING;
            "
        )
    }

    pub(in crate::workflow) fn signal_schema_sql(table: &str) -> String {
        format!(
            r"
            CREATE TABLE IF NOT EXISTS {table}_signals (
                signal_id UUID PRIMARY KEY, tenant_id TEXT NOT NULL,
                checkpoint_id UUID NOT NULL, name TEXT NOT NULL, payload JSONB NOT NULL,
                consumed BOOLEAN NOT NULL DEFAULT FALSE,
                dead_lettered BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
            );
            ALTER TABLE {table}_signals
                ADD COLUMN IF NOT EXISTS dead_lettered BOOLEAN NOT NULL DEFAULT FALSE;
            ALTER TABLE {table}_signals
                ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT 'default';
            DROP INDEX IF EXISTS {table}_signals_pending_idx;
            CREATE INDEX {table}_signals_pending_idx
                ON {table}_signals (checkpoint_id, name, created_at, signal_id)
                WHERE NOT consumed AND NOT dead_lettered;
            "
        )
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the tombstone governance schema remains one auditable migration unit"
    )]
    pub(in crate::workflow) fn task_retention_schema_sql(table: &str) -> String {
        format!(
            r"
            CREATE TABLE IF NOT EXISTS {table}_task_cleanup (
                tenant_id TEXT PRIMARY KEY,
                owner TEXT,
                fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
                lease_expires_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                CHECK ((owner IS NULL) = (lease_expires_at IS NULL))
            );
            CREATE SEQUENCE IF NOT EXISTS {table}_task_tombstone_seq;
            CREATE TABLE IF NOT EXISTS {table}_task_tombstones (
                sequence BIGINT PRIMARY KEY
                    DEFAULT nextval('{table}_task_tombstone_seq'),
                checkpoint_id UUID NOT NULL UNIQUE,
                tenant_id TEXT NOT NULL,
                workflow TEXT NOT NULL,
                workflow_version INTEGER NOT NULL CHECK (workflow_version > 0),
                final_status TEXT NOT NULL CHECK (
                    final_status IN ('completed', 'failed', 'cancelled')
                ),
                created_at TIMESTAMPTZ NOT NULL,
                terminal_at TIMESTAMPTZ NOT NULL,
                deleted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
            );
            CREATE INDEX IF NOT EXISTS {table}_task_tombstones_tenant_idx
                ON {table}_task_tombstones (tenant_id, sequence);
            CREATE INDEX IF NOT EXISTS {table}_terminal_task_cleanup_idx
                ON {table} (tenant_id, updated_at, checkpoint_id)
                WHERE state IN ('completed', 'failed', 'cancelled');
            CREATE TABLE IF NOT EXISTS {table}_tg_hold (
                hold_id BIGSERIAL PRIMARY KEY,
                checkpoint_id UUID NOT NULL,
                tenant_id TEXT NOT NULL,
                placed_by TEXT NOT NULL,
                reason TEXT NOT NULL,
                placed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                released_by TEXT,
                released_at TIMESTAMPTZ,
                CHECK ((released_by IS NULL) = (released_at IS NULL))
            );
            CREATE UNIQUE INDEX IF NOT EXISTS {table}_tg_hold_uq
                ON {table}_tg_hold (checkpoint_id)
                WHERE released_at IS NULL;
            CREATE INDEX IF NOT EXISTS {table}_tg_hold_ix
                ON {table}_tg_hold (tenant_id, checkpoint_id, hold_id);
            CREATE TABLE IF NOT EXISTS {table}_tg_export (
                tenant_id TEXT PRIMARY KEY,
                through_sequence BIGINT NOT NULL CHECK (through_sequence > 0),
                receipt TEXT NOT NULL,
                confirmed_by TEXT NOT NULL,
                confirmed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
            );
            CREATE TABLE IF NOT EXISTS {table}_tg_purge (
                purge_id UUID PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                prepared_by TEXT NOT NULL,
                tombstone_count INTEGER NOT NULL CHECK (tombstone_count > 0),
                first_sequence BIGINT NOT NULL,
                last_sequence BIGINT NOT NULL,
                export_through BIGINT NOT NULL,
                fingerprint TEXT NOT NULL,
                status TEXT NOT NULL CHECK (
                    status IN ('pending', 'approved', 'executed')
                ),
                prepared_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                expires_at TIMESTAMPTZ NOT NULL,
                approved_by TEXT,
                approved_at TIMESTAMPTZ,
                executed_at TIMESTAMPTZ,
                CHECK ((approved_by IS NULL) = (approved_at IS NULL)),
                CHECK (first_sequence <= last_sequence),
                CHECK (last_sequence <= export_through)
            );
            CREATE TABLE IF NOT EXISTS {table}_tg_item (
                purge_id UUID NOT NULL,
                tombstone_sequence BIGINT NOT NULL,
                PRIMARY KEY (purge_id, tombstone_sequence)
            );
            CREATE INDEX IF NOT EXISTS {table}_tg_item_ix
                ON {table}_tg_item (tombstone_sequence);
            CREATE TABLE IF NOT EXISTS {table}_tg_approval (
                purge_id UUID PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending' CHECK (
                    status IN ('pending', 'claimed', 'approved', 'rejected')
                ),
                claimed_by TEXT,
                claim_expires_at TIMESTAMPTZ,
                fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
                rejected_by TEXT,
                rejection_reason TEXT,
                rejected_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                CHECK (
                    (status = 'claimed')
                    = (claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL)
                ),
                CHECK (
                    (status = 'rejected')
                    = (
                        rejected_by IS NOT NULL
                        AND rejection_reason IS NOT NULL
                        AND rejected_at IS NOT NULL
                    )
                )
            );
            CREATE INDEX IF NOT EXISTS {table}_tg_approval_ix
                ON {table}_tg_approval (tenant_id, status, updated_at, purge_id);
            CREATE TABLE IF NOT EXISTS {table}_tg_evidence (
                purge_id UUID PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                prepared_by TEXT NOT NULL,
                approved_by TEXT NOT NULL,
                executed_by TEXT NOT NULL,
                tombstone_count INTEGER NOT NULL CHECK (tombstone_count > 0),
                first_sequence BIGINT NOT NULL,
                last_sequence BIGINT NOT NULL,
                export_through BIGINT NOT NULL,
                fingerprint TEXT NOT NULL,
                executed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
            );
            CREATE INDEX IF NOT EXISTS {table}_tg_evid_ix
                ON {table}_tg_evidence (tenant_id, executed_at, purge_id);
            "
        )
    }
}
