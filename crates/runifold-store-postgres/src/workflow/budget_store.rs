//! `PostgreSQL` tenant-budget policy, audit, projection, and ledger operations.

use super::{
    Budget, LeaseDuration, PostgresWorkflowStore, StoredBudgetLimit, Usage, Value, WorkerId,
    WorkflowBudgetAuditCursor, WorkflowBudgetAuditEvent, WorkflowBudgetAuditLimit,
    WorkflowBudgetAuditProjectionId, WorkflowBudgetAuditProjectionLease,
    WorkflowBudgetReservationOutcome, WorkflowLease, WorkflowStore, WorkflowStoreError,
    WorkflowStoreErrorKind, WorkflowStoreFuture, WorkflowTenantBudgetPolicy,
    WorkflowTenantBudgetSnapshot, WorkflowTenantId, WorkflowTenantListLimit, budget_decoding,
    budget_encoding, database_i64, decode_budget_audit_event, decode_budget_audit_projection_lease,
    decode_budget_reservation_status, decode_budget_settlement_status, decode_budget_snapshot,
    decode_u64, lease_lost, postgres_budget_request, projection_lease_lost, storage,
};

impl PostgresWorkflowStore {
    pub(super) fn set_tenant_budget_policy_inner(
        &self,
        tenant_id: WorkflowTenantId,
        policy: WorkflowTenantBudgetPolicy,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        Box::pin(async move {
            let limit = serde_json::to_value(StoredBudgetLimit::from_budget(policy.limit())?)
                .map_err(budget_encoding)?;
            let window = database_i64(policy.window_millis(), "tenant budget window")?;
            let grace = database_i64(
                policy.recovery_grace_millis(),
                "tenant budget recovery grace",
            )?;
            let zero = serde_json::to_value(Usage::default()).map_err(budget_encoding)?;
            self.client
                .execute(
                    &format!(
                        "INSERT INTO {table}_tenants (
                            tenant_id, max_outstanding_tasks, max_concurrent_leases
                         ) VALUES ($1, 10000, 100)
                         ON CONFLICT (tenant_id) DO NOTHING",
                        table = self.table
                    ),
                    &[&tenant_id.as_str()],
                )
                .await
                .map_err(storage)?;
            let row = self
                .client
                .query_one(
                    &format!(
                        "WITH changed AS (
                            UPDATE {table}_tenants SET
                                budget_limit = $2,
                                budget_window_ms = $3,
                                budget_recovery_grace_ms = $4,
                                budget_window_started_at = clock_timestamp(),
                                budget_committed = $5,
                                budget_reserved = $5,
                                budget_active_reservations = 0,
                                updated_at = clock_timestamp()
                            WHERE tenant_id = $1
                              AND budget_active_reservations = 0
                              AND NOT EXISTS (
                                  SELECT 1 FROM {table}_budgets
                                  WHERE tenant_id = $1
                              )
                            RETURNING tenant_id, budget_limit,
                                      budget_committed, budget_reserved
                         ),
                         audited AS (
                            INSERT INTO {table}_b_audit (
                                tenant_id, kind, usage, budget_limit,
                                committed, reserved
                            )
                            SELECT
                                tenant_id, 'policy_configured', $5,
                                budget_limit, budget_committed, budget_reserved
                            FROM changed
                            RETURNING sequence
                         )
                         SELECT EXISTS (SELECT 1 FROM audited)",
                        table = self.table
                    ),
                    &[&tenant_id.as_str(), &limit, &window, &grace, &zero],
                )
                .await
                .map_err(storage)?;
            let changed: bool = row.try_get(0).map_err(storage)?;
            if !changed {
                return Err(WorkflowStoreError::new(
                    WorkflowStoreErrorKind::Conflict,
                    "tenant budget policy cannot change while reservations are active",
                ));
            }
            Ok(())
        })
    }

    pub(super) fn list_tenant_budgets_inner(
        &self,
        after: Option<WorkflowTenantId>,
        limit: WorkflowTenantListLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowTenantId>, WorkflowStoreError>> {
        Box::pin(async move {
            let after = after.map_or_else(String::new, |tenant_id| tenant_id.as_str().to_owned());
            let limit = i64::from(limit.get());
            self.client
                .query(
                    &format!(
                        "SELECT tenant_id
                         FROM {table}_tenants
                         WHERE budget_limit IS NOT NULL AND tenant_id > $1
                         ORDER BY tenant_id ASC
                         LIMIT $2",
                        table = self.table
                    ),
                    &[&after, &limit],
                )
                .await
                .map_err(storage)?
                .into_iter()
                .map(|row| WorkflowTenantId::parse(row.get::<_, String>("tenant_id")))
                .collect()
        })
    }

    pub(super) fn inspect_tenant_budget_inner(
        &self,
        tenant_id: WorkflowTenantId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTenantBudgetSnapshot, WorkflowStoreError>> {
        Box::pin(async move {
            self.client
                .execute(
                    &format!("SELECT {table}_b_maintain($1)", table = self.table),
                    &[&tenant_id.as_str()],
                )
                .await
                .map_err(storage)?;
            let row = self
                .client
                .query_opt(
                    &format!(
                        "SELECT budget_limit,
                            (EXTRACT(EPOCH FROM budget_window_started_at) * 1000)::BIGINT,
                            budget_committed,
                            budget_reserved,
                            budget_active_reservations
                         FROM {table}_tenants AS tenant
                         WHERE tenant_id = $1 AND budget_limit IS NOT NULL",
                        table = self.table
                    ),
                    &[&tenant_id.as_str()],
                )
                .await
                .map_err(storage)?
                .ok_or_else(|| {
                    WorkflowStoreError::new(
                        WorkflowStoreErrorKind::NotFound,
                        format!(
                            "workflow tenant `{}` has no budget policy",
                            tenant_id.as_str()
                        ),
                    )
                })?;
            decode_budget_snapshot(tenant_id, &row)
        })
    }

    pub(super) fn list_tenant_budget_audit_inner(
        &self,
        tenant_id: WorkflowTenantId,
        after: Option<WorkflowBudgetAuditCursor>,
        limit: WorkflowBudgetAuditLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowBudgetAuditEvent>, WorkflowStoreError>> {
        Box::pin(async move {
            self.inspect_tenant_budget(tenant_id.clone()).await?;
            let after = database_i64(
                after.map_or(0, WorkflowBudgetAuditCursor::sequence),
                "workflow budget audit cursor",
            )?;
            let limit = i64::from(limit.get());
            self.client
                .query(
                    &format!(
                        "SELECT sequence, checkpoint_id,
                            (EXTRACT(EPOCH FROM occurred_at) * 1000)::BIGINT,
                            kind, reason, usage, reservation_age_ms,
                            budget_limit, committed, reserved
                         FROM {table}_b_audit
                         WHERE tenant_id = $1 AND sequence > $2
                         ORDER BY sequence ASC
                         LIMIT $3",
                        table = self.table
                    ),
                    &[&tenant_id.as_str(), &after, &limit],
                )
                .await
                .map_err(storage)?
                .iter()
                .map(|row| decode_budget_audit_event(tenant_id.clone(), row))
                .collect()
        })
    }

    pub(super) fn compact_tenant_budget_audit_inner(
        &self,
        tenant_id: WorkflowTenantId,
        through: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<u64, WorkflowStoreError>> {
        Box::pin(async move {
            self.inspect_tenant_budget(tenant_id.clone()).await?;
            let through = database_i64(
                through.sequence(),
                "workflow budget audit compaction cursor",
            )?;
            let row = self
                .client
                .query_one(
                    &format!(
                        "WITH projection_guard AS (
                            SELECT NOT EXISTS (
                                SELECT 1 FROM {table}_b_audit_projection
                                WHERE tenant_id = $1 AND sequence < $2
                            ) AS allowed
                         ), deleted AS (
                            DELETE FROM {table}_b_audit
                            WHERE tenant_id = $1 AND sequence <= $2
                              AND (SELECT allowed FROM projection_guard)
                            RETURNING 1
                         )
                         SELECT
                            (SELECT allowed FROM projection_guard) AS allowed,
                            COUNT(*)::BIGINT AS deleted
                         FROM deleted",
                        table = self.table
                    ),
                    &[&tenant_id.as_str(), &through],
                )
                .await
                .map_err(storage)?;
            if !row.get::<_, bool>("allowed") {
                return Err(WorkflowStoreError::new(
                    WorkflowStoreErrorKind::Conflict,
                    "workflow budget audit compaction would overrun a durable projection",
                ));
            }
            decode_u64(
                row.get::<_, i64>("deleted"),
                "workflow budget audit compaction count",
            )
        })
    }

    pub(super) fn load_or_create_tenant_budget_audit_projection_inner(
        &self,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditCursor, WorkflowStoreError>> {
        Box::pin(async move {
            self.inspect_tenant_budget(tenant_id.clone()).await?;
            let row = self
                .client
                .query_one(
                    &format!(
                        "INSERT INTO {table}_b_audit_projection (
                            tenant_id, projection_id, sequence
                         ) VALUES ($1, $2, 0)
                         ON CONFLICT (tenant_id, projection_id) DO UPDATE
                         SET projection_id = EXCLUDED.projection_id
                         RETURNING sequence",
                        table = self.table
                    ),
                    &[&tenant_id.as_str(), &projection_id.as_str()],
                )
                .await
                .map_err(storage)?;
            decode_u64(
                row.get::<_, i64>("sequence"),
                "workflow budget audit projection cursor",
            )
            .map(WorkflowBudgetAuditCursor::new)
        })
    }

    pub(super) fn advance_tenant_budget_audit_projection_inner(
        &self,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
        expected: WorkflowBudgetAuditCursor,
        next: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<bool, WorkflowStoreError>> {
        Box::pin(async move {
            if next <= expected {
                return Err(WorkflowStoreError::new(
                    WorkflowStoreErrorKind::InvalidInput,
                    "workflow budget audit projection cursor must advance monotonically",
                ));
            }
            self.inspect_tenant_budget(tenant_id.clone()).await?;
            let next = database_i64(next.sequence(), "workflow budget audit projection cursor")?;
            let expected = database_i64(
                expected.sequence(),
                "workflow budget audit expected projection cursor",
            )?;
            let changed = self
                .client
                .execute(
                    &format!(
                        "UPDATE {table}_b_audit_projection
                         SET sequence = $3, updated_at = clock_timestamp()
                         WHERE tenant_id = $1 AND projection_id = $2 AND sequence = $4",
                        table = self.table
                    ),
                    &[
                        &tenant_id.as_str(),
                        &projection_id.as_str(),
                        &next,
                        &expected,
                    ],
                )
                .await
                .map_err(storage)?;
            Ok(changed == 1)
        })
    }

    pub(super) fn claim_tenant_budget_audit_projection_inner(
        &self,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
        owner: WorkerId,
        lease: LeaseDuration,
    ) -> WorkflowStoreFuture<
        '_,
        Result<Option<WorkflowBudgetAuditProjectionLease>, WorkflowStoreError>,
    > {
        Box::pin(async move {
            self.inspect_tenant_budget(tenant_id.clone()).await?;
            let lease_ms = database_i64(lease.as_millis(), "budget projection lease")?;
            self.client
                .execute(
                    &format!(
                        "INSERT INTO {table}_b_audit_projection (
                            tenant_id, projection_id, sequence
                         ) VALUES ($1, $2, 0)
                         ON CONFLICT (tenant_id, projection_id) DO NOTHING",
                        table = self.table
                    ),
                    &[&tenant_id.as_str(), &projection_id.as_str()],
                )
                .await
                .map_err(storage)?;
            self.client
                .query_opt(
                    &format!(
                        "UPDATE {table}_b_audit_projection
                         SET owner = $3,
                             fencing_token = fencing_token + 1,
                             lease_expires_at = clock_timestamp()
                                + ($4::BIGINT * INTERVAL '1 millisecond'),
                             updated_at = clock_timestamp()
                         WHERE tenant_id = $1 AND projection_id = $2
                           AND (owner IS NULL OR lease_expires_at <= clock_timestamp())
                         RETURNING sequence, fencing_token,
                            (EXTRACT(EPOCH FROM lease_expires_at) * 1000)::BIGINT
                                AS expires_at_ms",
                        table = self.table
                    ),
                    &[
                        &tenant_id.as_str(),
                        &projection_id.as_str(),
                        &owner.as_str(),
                        &lease_ms,
                    ],
                )
                .await
                .map_err(storage)?
                .map(|row| {
                    decode_budget_audit_projection_lease(tenant_id, projection_id, owner, &row)
                })
                .transpose()
        })
    }

    pub(super) fn heartbeat_tenant_budget_audit_projection_inner(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
        extension: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditProjectionLease, WorkflowStoreError>>
    {
        Box::pin(async move {
            let extension =
                database_i64(extension.as_millis(), "budget projection lease extension")?;
            let fencing_token =
                database_i64(lease.fencing_token, "budget projection fencing token")?;
            let row = self
                .client
                .query_opt(
                    &format!(
                        "UPDATE {table}_b_audit_projection
                         SET lease_expires_at = clock_timestamp()
                                + ($4::BIGINT * INTERVAL '1 millisecond'),
                             updated_at = clock_timestamp()
                         WHERE tenant_id = $1 AND projection_id = $2
                           AND owner = $3 AND fencing_token = $5
                           AND lease_expires_at > clock_timestamp()
                         RETURNING sequence, fencing_token,
                            (EXTRACT(EPOCH FROM lease_expires_at) * 1000)::BIGINT
                                AS expires_at_ms",
                        table = self.table
                    ),
                    &[
                        &lease.tenant_id.as_str(),
                        &lease.projection_id.as_str(),
                        &lease.owner.as_str(),
                        &extension,
                        &fencing_token,
                    ],
                )
                .await
                .map_err(storage)?
                .ok_or_else(projection_lease_lost)?;
            decode_budget_audit_projection_lease(
                lease.tenant_id,
                lease.projection_id,
                lease.owner,
                &row,
            )
        })
    }

    pub(super) fn advance_tenant_budget_audit_projection_lease_inner(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
        next: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditProjectionLease, WorkflowStoreError>>
    {
        Box::pin(async move {
            if next <= lease.cursor {
                return Err(WorkflowStoreError::new(
                    WorkflowStoreErrorKind::InvalidInput,
                    "workflow budget audit projection cursor must advance monotonically",
                ));
            }
            let expected =
                database_i64(lease.cursor.sequence(), "budget projection expected cursor")?;
            let next = database_i64(next.sequence(), "budget projection next cursor")?;
            let fencing_token =
                database_i64(lease.fencing_token, "budget projection fencing token")?;
            let row = self
                .client
                .query_opt(
                    &format!(
                        "UPDATE {table}_b_audit_projection
                         SET sequence = $4, updated_at = clock_timestamp()
                         WHERE tenant_id = $1 AND projection_id = $2
                           AND owner = $3 AND sequence = $5 AND fencing_token = $6
                           AND lease_expires_at > clock_timestamp()
                         RETURNING sequence, fencing_token,
                            (EXTRACT(EPOCH FROM lease_expires_at) * 1000)::BIGINT
                                AS expires_at_ms",
                        table = self.table
                    ),
                    &[
                        &lease.tenant_id.as_str(),
                        &lease.projection_id.as_str(),
                        &lease.owner.as_str(),
                        &next,
                        &expected,
                        &fencing_token,
                    ],
                )
                .await
                .map_err(storage)?
                .ok_or_else(projection_lease_lost)?;
            decode_budget_audit_projection_lease(
                lease.tenant_id,
                lease.projection_id,
                lease.owner,
                &row,
            )
        })
    }

    pub(super) fn release_tenant_budget_audit_projection_inner(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        Box::pin(async move {
            let fencing_token =
                database_i64(lease.fencing_token, "budget projection fencing token")?;
            let changed = self
                .client
                .execute(
                    &format!(
                        "UPDATE {table}_b_audit_projection
                         SET owner = NULL, lease_expires_at = NULL,
                             updated_at = clock_timestamp()
                         WHERE tenant_id = $1 AND projection_id = $2
                           AND owner = $3 AND fencing_token = $4",
                        table = self.table
                    ),
                    &[
                        &lease.tenant_id.as_str(),
                        &lease.projection_id.as_str(),
                        &lease.owner.as_str(),
                        &fencing_token,
                    ],
                )
                .await
                .map_err(storage)?;
            if changed != 1 {
                return Err(projection_lease_lost());
            }
            Ok(())
        })
    }

    pub(super) fn reserve_budget_inner(
        &self,
        lease: WorkflowLease,
        workflow_limit: Budget,
        baseline: Usage,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetReservationOutcome, WorkflowStoreError>> {
        Box::pin(async move {
            let policy_row = self
                .client
                .query_opt(
                    &format!(
                        "SELECT budget_limit FROM {table}_tenants WHERE tenant_id = $1",
                        table = self.table
                    ),
                    &[&lease.tenant_id.as_str()],
                )
                .await
                .map_err(storage)?;
            let Some(policy_row) = policy_row else {
                return Err(lease_lost());
            };
            let limit_value = policy_row.try_get::<_, Option<Value>>(0).map_err(storage)?;
            let request = limit_value
                .clone()
                .map(|value| {
                    let stored: StoredBudgetLimit =
                        serde_json::from_value(value).map_err(budget_decoding)?;
                    postgres_budget_request(stored.into_budget(), workflow_limit, baseline)
                })
                .transpose()?
                .unwrap_or_default();
            let baseline = serde_json::to_value(baseline).map_err(budget_encoding)?;
            let request = serde_json::to_value(request).map_err(budget_encoding)?;
            let token = database_i64(lease.fencing_token, "fencing token")?;
            let row = self
                .client
                .query_one(
                    &format!(
                        "SELECT {table}_b_reserve($1, $2, $3, $4, $5, $6, $7)",
                        table = self.table
                    ),
                    &[
                        &lease.checkpoint_id.as_uuid(),
                        &lease.tenant_id.as_str(),
                        &lease.worker.as_str(),
                        &token,
                        &baseline,
                        &request,
                        &limit_value,
                    ],
                )
                .await
                .map_err(storage)?;
            let status = row.try_get::<_, String>(0).map_err(storage)?;
            decode_budget_reservation_status(&status)
        })
    }

    pub(super) fn settle_budget_inner(
        &self,
        lease: WorkflowLease,
        cumulative: Usage,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        Box::pin(async move {
            let cumulative = serde_json::to_value(cumulative).map_err(budget_encoding)?;
            let token = database_i64(lease.fencing_token, "fencing token")?;
            let row = self
                .client
                .query_one(
                    &format!(
                        "SELECT {table}_b_settle($1, $2, $3, $4, $5)",
                        table = self.table
                    ),
                    &[
                        &lease.checkpoint_id.as_uuid(),
                        &lease.tenant_id.as_str(),
                        &lease.worker.as_str(),
                        &token,
                        &cumulative,
                    ],
                )
                .await
                .map_err(storage)?;
            let status = row.try_get::<_, String>(0).map_err(storage)?;
            decode_budget_settlement_status(&status)
        })
    }
}
