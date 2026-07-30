//! Tenant-budget tables and database-function schema builder.

use super::super::PostgresWorkflowStore;

impl PostgresWorkflowStore {
    #[allow(
        clippy::too_many_lines,
        reason = "the explicit PostgreSQL schema is kept together for transactional review"
    )]
    pub(in crate::workflow) fn budget_schema_sql(table: &str) -> String {
        format!(
            r"
            ALTER TABLE {table}_tenants ADD COLUMN IF NOT EXISTS budget_limit JSONB;
            ALTER TABLE {table}_tenants ADD COLUMN IF NOT EXISTS budget_window_ms BIGINT;
            ALTER TABLE {table}_tenants ADD COLUMN IF NOT EXISTS budget_recovery_grace_ms BIGINT;
            ALTER TABLE {table}_tenants ADD COLUMN IF NOT EXISTS budget_window_started_at TIMESTAMPTZ;
            ALTER TABLE {table}_tenants ADD COLUMN IF NOT EXISTS budget_committed JSONB
                NOT NULL DEFAULT '{usage}';
            ALTER TABLE {table}_tenants ADD COLUMN IF NOT EXISTS budget_reserved JSONB
                NOT NULL DEFAULT '{usage}';
            ALTER TABLE {table}_tenants ADD COLUMN IF NOT EXISTS
                budget_active_reservations BIGINT NOT NULL DEFAULT 0;
            CREATE TABLE IF NOT EXISTS {table}_budgets (
                checkpoint_id UUID PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                baseline JSONB NOT NULL,
                amount JSONB NOT NULL,
                reserved_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                expires_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
            );
            ALTER TABLE {table}_budgets ADD COLUMN IF NOT EXISTS reserved_at
                TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp();
            CREATE INDEX IF NOT EXISTS {table}_budgets_expiry_idx
                ON {table}_budgets (tenant_id, expires_at);
            CREATE SEQUENCE IF NOT EXISTS {table}_b_audit_seq;
            CREATE TABLE IF NOT EXISTS {table}_b_audit (
                sequence BIGINT PRIMARY KEY
                    DEFAULT nextval('{table}_b_audit_seq'),
                tenant_id TEXT NOT NULL,
                checkpoint_id UUID,
                kind TEXT NOT NULL CHECK (kind IN (
                    'policy_configured', 'reserved', 'adopted',
                    'admission_denied', 'usage_exceeded', 'settled',
                    'forfeited', 'window_reset'
                )),
                reason TEXT CHECK (
                    reason IS NULL OR reason IN ('cancelled', 'recovery_expired')
                ),
                usage JSONB NOT NULL,
                reservation_age_ms BIGINT,
                budget_limit JSONB NOT NULL,
                committed JSONB NOT NULL,
                reserved JSONB NOT NULL,
                occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
            );
            ALTER TABLE {table}_b_audit ADD COLUMN IF NOT EXISTS reservation_age_ms BIGINT;
            CREATE INDEX IF NOT EXISTS {table}_ba_idx
                ON {table}_b_audit (tenant_id, sequence);
            CREATE TABLE IF NOT EXISTS {table}_b_audit_projection (
                tenant_id TEXT NOT NULL,
                projection_id TEXT NOT NULL,
                sequence BIGINT NOT NULL CHECK (sequence >= 0),
                owner TEXT,
                fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
                lease_expires_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (tenant_id, projection_id),
                CHECK ((owner IS NULL) = (lease_expires_at IS NULL))
            );
            ALTER TABLE {table}_b_audit_projection ADD COLUMN IF NOT EXISTS owner TEXT;
            ALTER TABLE {table}_b_audit_projection ADD COLUMN IF NOT EXISTS
                fencing_token BIGINT NOT NULL DEFAULT 0;
            ALTER TABLE {table}_b_audit_projection ADD COLUMN IF NOT EXISTS
                lease_expires_at TIMESTAMPTZ;
            UPDATE {table}_tenants AS tenant SET budget_active_reservations = (
                SELECT COUNT(*) FROM {table}_budgets AS reservation
                WHERE reservation.tenant_id = tenant.tenant_id
            );

            CREATE OR REPLACE FUNCTION {table}_b_add(left_usage JSONB, right_usage JSONB)
            RETURNS JSONB LANGUAGE SQL IMMUTABLE STRICT AS $$
                SELECT jsonb_build_object(
                    'tokens', (left_usage->>'tokens')::NUMERIC
                        + (right_usage->>'tokens')::NUMERIC,
                    'cost_microusd', (left_usage->>'cost_microusd')::NUMERIC
                        + (right_usage->>'cost_microusd')::NUMERIC,
                    'duration_micros', (left_usage->>'duration_micros')::NUMERIC
                        + (right_usage->>'duration_micros')::NUMERIC,
                    'turns', (left_usage->>'turns')::NUMERIC
                        + (right_usage->>'turns')::NUMERIC,
                    'tool_calls', (left_usage->>'tool_calls')::NUMERIC
                        + (right_usage->>'tool_calls')::NUMERIC,
                    'delegations', (left_usage->>'delegations')::NUMERIC
                        + (right_usage->>'delegations')::NUMERIC
                )
            $$;
            CREATE OR REPLACE FUNCTION {table}_b_sub(left_usage JSONB, right_usage JSONB)
            RETURNS JSONB LANGUAGE SQL IMMUTABLE STRICT AS $$
                SELECT CASE WHEN
                    (left_usage->>'tokens')::NUMERIC >= (right_usage->>'tokens')::NUMERIC
                    AND (left_usage->>'cost_microusd')::NUMERIC
                        >= (right_usage->>'cost_microusd')::NUMERIC
                    AND (left_usage->>'duration_micros')::NUMERIC
                        >= (right_usage->>'duration_micros')::NUMERIC
                    AND (left_usage->>'turns')::NUMERIC >= (right_usage->>'turns')::NUMERIC
                    AND (left_usage->>'tool_calls')::NUMERIC
                        >= (right_usage->>'tool_calls')::NUMERIC
                    AND (left_usage->>'delegations')::NUMERIC
                        >= (right_usage->>'delegations')::NUMERIC
                THEN jsonb_build_object(
                    'tokens', (left_usage->>'tokens')::NUMERIC
                        - (right_usage->>'tokens')::NUMERIC,
                    'cost_microusd', (left_usage->>'cost_microusd')::NUMERIC
                        - (right_usage->>'cost_microusd')::NUMERIC,
                    'duration_micros', (left_usage->>'duration_micros')::NUMERIC
                        - (right_usage->>'duration_micros')::NUMERIC,
                    'turns', (left_usage->>'turns')::NUMERIC
                        - (right_usage->>'turns')::NUMERIC,
                    'tool_calls', (left_usage->>'tool_calls')::NUMERIC
                        - (right_usage->>'tool_calls')::NUMERIC,
                    'delegations', (left_usage->>'delegations')::NUMERIC
                        - (right_usage->>'delegations')::NUMERIC
                ) END
            $$;
            CREATE OR REPLACE FUNCTION {table}_b_controlled(limit_value JSONB, used JSONB)
            RETURNS JSONB LANGUAGE SQL IMMUTABLE STRICT AS $$
                SELECT jsonb_build_object(
                    'tokens', CASE WHEN limit_value->'tokens' = 'null'::JSONB
                        THEN 0 ELSE (used->>'tokens')::NUMERIC END,
                    'cost_microusd', CASE WHEN limit_value->'cost_microusd' = 'null'::JSONB
                        THEN 0 ELSE (used->>'cost_microusd')::NUMERIC END,
                    'duration_micros', CASE WHEN limit_value->'duration_micros' = 'null'::JSONB
                        THEN 0 ELSE (used->>'duration_micros')::NUMERIC END,
                    'turns', CASE WHEN limit_value->'turns' = 'null'::JSONB
                        THEN 0 ELSE (used->>'turns')::NUMERIC END,
                    'tool_calls', CASE WHEN limit_value->'tool_calls' = 'null'::JSONB
                        THEN 0 ELSE (used->>'tool_calls')::NUMERIC END,
                    'delegations', CASE WHEN limit_value->'delegations' = 'null'::JSONB
                        THEN 0 ELSE (used->>'delegations')::NUMERIC END
                )
            $$;
            CREATE OR REPLACE FUNCTION {table}_b_fits(used JSONB, limit_value JSONB)
            RETURNS BOOLEAN LANGUAGE SQL IMMUTABLE STRICT AS $$
                SELECT
                    (limit_value->'tokens' = 'null'::JSONB
                        OR (used->>'tokens')::NUMERIC <= (limit_value->>'tokens')::NUMERIC)
                    AND (limit_value->'cost_microusd' = 'null'::JSONB
                        OR (used->>'cost_microusd')::NUMERIC
                            <= (limit_value->>'cost_microusd')::NUMERIC)
                    AND (limit_value->'duration_micros' = 'null'::JSONB
                        OR (used->>'duration_micros')::NUMERIC
                            <= (limit_value->>'duration_micros')::NUMERIC)
                    AND (limit_value->'turns' = 'null'::JSONB
                        OR (used->>'turns')::NUMERIC <= (limit_value->>'turns')::NUMERIC)
                    AND (limit_value->'tool_calls' = 'null'::JSONB
                        OR (used->>'tool_calls')::NUMERIC
                            <= (limit_value->>'tool_calls')::NUMERIC)
                    AND (limit_value->'delegations' = 'null'::JSONB
                        OR (used->>'delegations')::NUMERIC
                            <= (limit_value->>'delegations')::NUMERIC)
            $$;
            CREATE OR REPLACE FUNCTION {table}_b_maintain(target_tenant TEXT)
            RETURNS VOID LANGUAGE plpgsql AS $$
            DECLARE expired_checkpoint UUID;
            DECLARE expired_amount JSONB;
            DECLARE expired_reserved_at TIMESTAMPTZ;
            DECLARE drained_usage JSONB;
            DECLARE active_limit JSONB;
            BEGIN
                PERFORM 1 FROM {table}_tenants
                    WHERE tenant_id = target_tenant FOR UPDATE;
                FOR expired_checkpoint, expired_amount, expired_reserved_at IN
                    DELETE FROM {table}_budgets
                    WHERE tenant_id = target_tenant AND expires_at <= clock_timestamp()
                    RETURNING checkpoint_id, amount, reserved_at
                LOOP
                    UPDATE {table}_tenants SET
                        budget_committed = {table}_b_add(budget_committed, expired_amount),
                        budget_reserved = {table}_b_sub(budget_reserved, expired_amount),
                        budget_active_reservations =
                            GREATEST(budget_active_reservations - 1, 0)
                    WHERE tenant_id = target_tenant;
                    INSERT INTO {table}_b_audit (
                        tenant_id, checkpoint_id, kind, reason, usage,
                        reservation_age_ms,
                        budget_limit, committed, reserved
                    )
                    SELECT
                        target_tenant, expired_checkpoint, 'forfeited',
                        'recovery_expired', expired_amount,
                        GREATEST(
                            (EXTRACT(EPOCH FROM (
                                clock_timestamp() - expired_reserved_at
                            )) * 1000)::BIGINT,
                            0
                        ),
                        budget_limit,
                        budget_committed, budget_reserved
                    FROM {table}_tenants WHERE tenant_id = target_tenant;
                END LOOP;
                SELECT budget_committed, budget_limit
                    INTO drained_usage, active_limit
                FROM {table}_tenants
                WHERE tenant_id = target_tenant
                  AND budget_limit IS NOT NULL
                  AND clock_timestamp() >= budget_window_started_at
                    + (budget_window_ms * INTERVAL '1 millisecond')
                  AND budget_active_reservations = 0;
                IF FOUND THEN
                    UPDATE {table}_tenants SET
                        budget_window_started_at = clock_timestamp(),
                        budget_committed = '{usage}',
                        budget_reserved = '{usage}'
                    WHERE tenant_id = target_tenant;
                    INSERT INTO {table}_b_audit (
                        tenant_id, kind, usage, budget_limit, committed, reserved
                    ) VALUES (
                        target_tenant, 'window_reset', drained_usage, active_limit,
                        '{usage}', '{usage}'
                    );
                END IF;
            END
            $$;
            CREATE OR REPLACE FUNCTION {table}_b_reserve(
                target_checkpoint UUID, target_tenant TEXT, target_owner TEXT,
                target_fence BIGINT, baseline_value JSONB, request_value JSONB,
                expected_limit JSONB
            ) RETURNS TEXT LANGUAGE plpgsql AS $$
            DECLARE tenant_row RECORD;
            DECLARE reservation_row RECORD;
            DECLARE observed JSONB;
            DECLARE charged JSONB;
            DECLARE remaining JSONB;
            DECLARE next_total JSONB;
            DECLARE reservation_expiry TIMESTAMPTZ;
            BEGIN
                SELECT task.lease_expires_at INTO reservation_expiry
                FROM {table} AS task
                WHERE task.checkpoint_id = target_checkpoint
                  AND task.tenant_id = target_tenant
                  AND task.state = 'leased'
                  AND task.owner = target_owner
                  AND task.fencing_token = target_fence
                  AND task.lease_expires_at > clock_timestamp()
                FOR UPDATE;
                IF NOT FOUND THEN RETURN 'lease_lost'; END IF;
                PERFORM {table}_b_maintain(target_tenant);
                SELECT * INTO tenant_row FROM {table}_tenants
                    WHERE tenant_id = target_tenant FOR UPDATE;
                IF tenant_row.budget_limit IS NULL THEN RETURN 'not_configured'; END IF;
                IF expected_limit IS NULL OR tenant_row.budget_limit <> expected_limit
                    THEN RETURN 'policy_changed'; END IF;
                SELECT * INTO reservation_row FROM {table}_budgets
                    WHERE checkpoint_id = target_checkpoint FOR UPDATE;
                IF FOUND THEN
                    IF reservation_row.tenant_id <> target_tenant THEN RETURN 'conflict'; END IF;
                    observed := {table}_b_sub(baseline_value, reservation_row.baseline);
                    IF observed IS NULL THEN RETURN 'baseline_backwards'; END IF;
                    charged := {table}_b_controlled(tenant_row.budget_limit, observed);
                    remaining := {table}_b_sub(reservation_row.amount, charged);
                    IF remaining IS NULL THEN
                        INSERT INTO {table}_b_audit (
                            tenant_id, checkpoint_id, kind, usage,
                            reservation_age_ms, budget_limit, committed, reserved
                        ) VALUES (
                            target_tenant, target_checkpoint, 'usage_exceeded',
                            charged,
                            GREATEST(
                                (EXTRACT(EPOCH FROM (
                                    clock_timestamp() - reservation_row.reserved_at
                                )) * 1000)::BIGINT,
                                0
                            ),
                            tenant_row.budget_limit, tenant_row.budget_committed,
                            tenant_row.budget_reserved
                        );
                        RETURN 'reservation_exceeded';
                    END IF;
                    IF remaining <> request_value THEN RETURN 'envelope_changed'; END IF;
                    UPDATE {table}_tenants SET
                        budget_committed = {table}_b_add(budget_committed, charged),
                        budget_reserved = {table}_b_sub(budget_reserved, charged)
                    WHERE tenant_id = target_tenant;
                    UPDATE {table}_budgets SET
                        baseline = baseline_value,
                        amount = remaining,
                        expires_at = reservation_expiry
                            + (tenant_row.budget_recovery_grace_ms
                                * INTERVAL '1 millisecond'),
                        updated_at = clock_timestamp()
                    WHERE checkpoint_id = target_checkpoint;
                    INSERT INTO {table}_b_audit (
                        tenant_id, checkpoint_id, kind, usage,
                        reservation_age_ms,
                        budget_limit, committed, reserved
                    )
                    SELECT
                        target_tenant, target_checkpoint, 'adopted', charged,
                        GREATEST(
                            (EXTRACT(EPOCH FROM (
                                clock_timestamp() - reservation_row.reserved_at
                            )) * 1000)::BIGINT,
                            0
                        ),
                        budget_limit, budget_committed, budget_reserved
                    FROM {table}_tenants WHERE tenant_id = target_tenant;
                    RETURN 'reserved';
                END IF;
                next_total := {table}_b_add(
                    tenant_row.budget_committed,
                    {table}_b_add(tenant_row.budget_reserved, request_value)
                );
                IF NOT {table}_b_fits(next_total, tenant_row.budget_limit) THEN
                    INSERT INTO {table}_b_audit (
                        tenant_id, checkpoint_id, kind, usage,
                        budget_limit, committed, reserved
                    ) VALUES (
                        target_tenant, target_checkpoint, 'admission_denied',
                        request_value, tenant_row.budget_limit,
                        tenant_row.budget_committed, tenant_row.budget_reserved
                    );
                    RETURN 'admission_denied';
                END IF;
                INSERT INTO {table}_budgets (
                    checkpoint_id, tenant_id, baseline, amount, expires_at
                ) VALUES (
                    target_checkpoint, target_tenant, baseline_value, request_value,
                    reservation_expiry + (tenant_row.budget_recovery_grace_ms
                        * INTERVAL '1 millisecond')
                );
                UPDATE {table}_tenants SET
                    budget_reserved = {table}_b_add(budget_reserved, request_value),
                    budget_active_reservations = budget_active_reservations + 1
                WHERE tenant_id = target_tenant;
                INSERT INTO {table}_b_audit (
                    tenant_id, checkpoint_id, kind, usage, reservation_age_ms,
                    budget_limit, committed, reserved
                )
                SELECT
                    target_tenant, target_checkpoint, 'reserved', request_value, 0,
                    budget_limit, budget_committed, budget_reserved
                FROM {table}_tenants WHERE tenant_id = target_tenant;
                RETURN 'reserved';
            END
            $$;
            CREATE OR REPLACE FUNCTION {table}_b_settle(
                target_checkpoint UUID, target_tenant TEXT, target_owner TEXT,
                target_fence BIGINT, cumulative_value JSONB
            ) RETURNS TEXT LANGUAGE plpgsql AS $$
            DECLARE tenant_row RECORD;
            DECLARE reservation_row RECORD;
            DECLARE observed JSONB;
            DECLARE charged JSONB;
            BEGIN
                PERFORM 1 FROM {table} AS task
                WHERE task.checkpoint_id = target_checkpoint
                  AND task.tenant_id = target_tenant
                  AND task.state = 'leased'
                  AND task.owner = target_owner
                  AND task.fencing_token = target_fence
                  AND task.lease_expires_at > clock_timestamp()
                FOR UPDATE;
                IF NOT FOUND THEN RETURN 'lease_lost'; END IF;
                PERFORM {table}_b_maintain(target_tenant);
                SELECT * INTO tenant_row FROM {table}_tenants
                    WHERE tenant_id = target_tenant FOR UPDATE;
                IF tenant_row.budget_limit IS NULL THEN RETURN 'settled'; END IF;
                SELECT * INTO reservation_row FROM {table}_budgets
                    WHERE checkpoint_id = target_checkpoint FOR UPDATE;
                IF NOT FOUND THEN RETURN 'missing_reservation'; END IF;
                observed := {table}_b_sub(cumulative_value, reservation_row.baseline);
                IF observed IS NULL THEN RETURN 'baseline_backwards'; END IF;
                charged := {table}_b_controlled(tenant_row.budget_limit, observed);
                IF {table}_b_sub(reservation_row.amount, charged) IS NULL THEN
                    INSERT INTO {table}_b_audit (
                        tenant_id, checkpoint_id, kind, usage,
                        reservation_age_ms, budget_limit, committed, reserved
                    ) VALUES (
                        target_tenant, target_checkpoint, 'usage_exceeded',
                        charged,
                        GREATEST(
                            (EXTRACT(EPOCH FROM (
                                clock_timestamp() - reservation_row.reserved_at
                            )) * 1000)::BIGINT,
                            0
                        ),
                        tenant_row.budget_limit, tenant_row.budget_committed,
                        tenant_row.budget_reserved
                    );
                    RETURN 'reservation_exceeded';
                END IF;
                UPDATE {table}_tenants SET
                    budget_committed = {table}_b_add(budget_committed, charged),
                    budget_reserved = {table}_b_sub(
                        budget_reserved, reservation_row.amount
                    ),
                    budget_active_reservations =
                        GREATEST(budget_active_reservations - 1, 0)
                WHERE tenant_id = target_tenant;
                DELETE FROM {table}_budgets
                    WHERE checkpoint_id = target_checkpoint;
                INSERT INTO {table}_b_audit (
                    tenant_id, checkpoint_id, kind, usage,
                    reservation_age_ms,
                    budget_limit, committed, reserved
                )
                SELECT
                    target_tenant, target_checkpoint, 'settled', charged,
                    GREATEST(
                        (EXTRACT(EPOCH FROM (
                            clock_timestamp() - reservation_row.reserved_at
                        )) * 1000)::BIGINT,
                        0
                    ),
                    budget_limit, budget_committed, budget_reserved
                FROM {table}_tenants WHERE tenant_id = target_tenant;
                RETURN 'settled';
            END
            $$;
            CREATE OR REPLACE FUNCTION {table}_b_forfeit(
                target_checkpoint UUID, target_tenant TEXT
            ) RETURNS VOID LANGUAGE plpgsql AS $$
            DECLARE reservation_row RECORD;
            BEGIN
                PERFORM 1 FROM {table}_tenants
                    WHERE tenant_id = target_tenant FOR UPDATE;
                SELECT * INTO reservation_row FROM {table}_budgets
                    WHERE checkpoint_id = target_checkpoint
                      AND tenant_id = target_tenant
                    FOR UPDATE;
                IF NOT FOUND THEN RETURN; END IF;
                UPDATE {table}_tenants SET
                    budget_committed = {table}_b_add(
                        budget_committed, reservation_row.amount
                    ),
                    budget_reserved = {table}_b_sub(
                        budget_reserved, reservation_row.amount
                    ),
                    budget_active_reservations =
                        GREATEST(budget_active_reservations - 1, 0)
                WHERE tenant_id = target_tenant;
                DELETE FROM {table}_budgets
                    WHERE checkpoint_id = target_checkpoint;
                INSERT INTO {table}_b_audit (
                    tenant_id, checkpoint_id, kind, reason, usage,
                    reservation_age_ms,
                    budget_limit, committed, reserved
                )
                SELECT
                    target_tenant, target_checkpoint, 'forfeited', 'cancelled',
                    reservation_row.amount,
                    GREATEST(
                        (EXTRACT(EPOCH FROM (
                            clock_timestamp() - reservation_row.reserved_at
                        )) * 1000)::BIGINT,
                        0
                    ),
                    budget_limit,
                    budget_committed, budget_reserved
                FROM {table}_tenants WHERE tenant_id = target_tenant;
            END
            $$;
            ",
            usage = r#"{"tokens":0,"cost_microusd":0,"duration_micros":0,"turns":0,"tool_calls":0,"delegations":0}"#
        )
    }
}
