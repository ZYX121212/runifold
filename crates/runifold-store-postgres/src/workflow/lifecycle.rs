//! Lease-fenced workflow suspension and terminal lifecycle transitions.

use runifold_workflow::{
    ClaimedWorkflow, LeaseDuration, WorkerId, WorkflowDisposition, WorkflowLease,
    WorkflowStoreError, WorkflowStoreErrorKind, WorkflowWait,
};

use super::{
    PostgresWorkflowStore,
    codec::decode_claim,
    support::{
        database_i64, decode_u64, duration_millis, lease_lost, storage, validate_failure_reason,
    },
};

impl PostgresWorkflowStore {
    pub(super) async fn claim_inner(
        &self,
        worker: WorkerId,
        lease: LeaseDuration,
    ) -> Result<Option<ClaimedWorkflow>, WorkflowStoreError> {
        let duration = database_i64(lease.as_millis(), "lease duration")?;
        self.client
            .query_opt(&self.claim_sql(), &[&worker.as_str(), &duration])
            .await
            .map_err(storage)?
            .map(|row| decode_claim(&row, worker))
            .transpose()
    }

    pub(super) async fn heartbeat_inner(
        &self,
        lease: WorkflowLease,
        extension: LeaseDuration,
    ) -> Result<WorkflowLease, WorkflowStoreError> {
        let token = database_i64(lease.fencing_token, "fencing token")?;
        let extension = database_i64(extension.as_millis(), "lease extension")?;
        let row = self
            .client
            .query_opt(
                &self.heartbeat_sql(),
                &[
                    &lease.checkpoint_id.as_uuid(),
                    &lease.tenant_id.as_str(),
                    &lease.worker.as_str(),
                    &token,
                    &extension,
                ],
            )
            .await
            .map_err(storage)?
            .ok_or_else(lease_lost)?;
        let attempt = decode_u64(row.try_get::<_, i64>(0).map_err(storage)?, "attempt")?;
        let expires_at_ms = decode_u64(
            row.try_get::<_, i64>(1).map_err(storage)?,
            "lease expiration",
        )?;
        Ok(WorkflowLease {
            attempt,
            expires_at_ms,
            ..lease
        })
    }

    async fn suspend_timer(
        &self,
        lease: &WorkflowLease,
        delay_ms: u64,
    ) -> Result<u64, WorkflowStoreError> {
        if delay_ms == 0 {
            return Err(WorkflowStoreError::new(
                WorkflowStoreErrorKind::InvalidInput,
                "durable timer delay must be positive",
            ));
        }
        let delay = database_i64(delay_ms, "durable timer delay")?;
        let token = database_i64(lease.fencing_token, "fencing token")?;
        self.client
            .execute(
                &format!(
                    "UPDATE {table} SET state = 'waiting_timer', owner = NULL,\
                       lease_expires_at = NULL, wait_kind = 'timer', wait_name = NULL, wait = NULL,\
                       wake_at = clock_timestamp() + \
                         ($5::BIGINT * INTERVAL '1 millisecond'), wake = NULL,\
                       updated_at = clock_timestamp()\
                     WHERE checkpoint_id = $1 AND tenant_id = $2 \
                       AND state = 'leased' AND owner = $3 \
                       AND fencing_token = $4 AND lease_expires_at > clock_timestamp()
                       AND NOT EXISTS (
                           SELECT 1 FROM {table}_budgets
                           WHERE checkpoint_id = $1
                       )",
                    table = self.table
                ),
                &[
                    &lease.checkpoint_id.as_uuid(),
                    &lease.tenant_id.as_str(),
                    &lease.worker.as_str(),
                    &token,
                    &delay,
                ],
            )
            .await
            .map_err(storage)
    }

    async fn suspend_signal(
        &self,
        lease: &WorkflowLease,
        name: &runifold_workflow::WorkflowSignalName,
        timeout_ms: Option<u64>,
        wait: Option<&WorkflowWait>,
    ) -> Result<u64, WorkflowStoreError> {
        let token = database_i64(lease.fencing_token, "fencing token")?;
        let timeout = timeout_ms
            .map(|value| database_i64(value, "durable signal timeout"))
            .transpose()?;
        let wait = wait.map(serde_json::to_value).transpose().map_err(|_| {
            WorkflowStoreError::new(
                WorkflowStoreErrorKind::InvalidInput,
                "workflow wait cannot be encoded",
            )
        })?;
        self.client
            .execute(
                &format!(
                    r"
                    WITH candidate AS (
                        SELECT signal_id, name, payload
                        FROM {table}_signals
                        WHERE checkpoint_id = $1 AND tenant_id = $2 AND name = $5
                          AND NOT consumed AND NOT dead_lettered
                        ORDER BY created_at ASC, signal_id ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                    ),
                    consumed AS (
                        UPDATE {table}_signals AS signal
                        SET consumed = TRUE
                        FROM candidate
                        WHERE signal.signal_id = candidate.signal_id
                        RETURNING candidate.signal_id, candidate.name, candidate.payload
                    )
                    UPDATE {table}
                    SET
                        state = CASE
                            WHEN EXISTS (SELECT 1 FROM consumed) THEN 'queued'
                            ELSE 'waiting_signal'
                        END,
                        owner = NULL,
                        lease_expires_at = NULL,
                        available_at = clock_timestamp(),
                        wait_kind = CASE
                            WHEN EXISTS (SELECT 1 FROM consumed) THEN NULL
                            ELSE 'signal'
                        END,
                        wait_name = CASE
                            WHEN EXISTS (SELECT 1 FROM consumed) THEN NULL
                            ELSE $5
                        END,
                        wait = CASE
                            WHEN EXISTS (SELECT 1 FROM consumed) THEN NULL
                            ELSE $7::JSONB
                        END,
                        wake_at = CASE
                            WHEN EXISTS (SELECT 1 FROM consumed) OR $6::BIGINT IS NULL
                            THEN NULL
                            ELSE clock_timestamp() + ($6::BIGINT * INTERVAL '1 millisecond')
                        END,
                        wake = (
                            SELECT jsonb_build_object(
                                'kind', 'signal',
                                'signal_id', signal_id,
                                'name', name,
                                'payload', payload
                            )
                            FROM consumed
                        ),
                        updated_at = clock_timestamp()
                    WHERE checkpoint_id = $1 AND tenant_id = $2
                      AND state = 'leased' AND owner = $3
                      AND fencing_token = $4 AND lease_expires_at > clock_timestamp()
                      AND NOT EXISTS (
                          SELECT 1 FROM {table}_budgets
                          WHERE checkpoint_id = $1
                      )
                    ",
                    table = self.table
                ),
                &[
                    &lease.checkpoint_id.as_uuid(),
                    &lease.tenant_id.as_str(),
                    &lease.worker.as_str(),
                    &token,
                    &name.as_str(),
                    &timeout,
                    &wait,
                ],
            )
            .await
            .map_err(storage)
    }

    async fn finish_terminal(
        &self,
        lease: &WorkflowLease,
        state: &'static str,
        failure_reason: Option<&str>,
    ) -> Result<u64, WorkflowStoreError> {
        let token = database_i64(lease.fencing_token, "fencing token")?;
        self.client
            .execute(
                &format!(
                    r"
                    WITH finished AS (
                        UPDATE {table}
                        SET
                            state = '{state}',
                            owner = NULL,
                            lease_expires_at = NULL,
                            failure_reason = $5,
                            wait_kind = NULL,
                            wait_name = NULL,
                            wait = NULL,
                            wake_at = NULL,
                            wake = NULL,
                            updated_at = clock_timestamp()
                        WHERE checkpoint_id = $1
                          AND tenant_id = $2
                          AND state = 'leased'
                          AND owner = $3
                          AND fencing_token = $4
                          AND lease_expires_at > clock_timestamp()
                          AND NOT EXISTS (
                              SELECT 1 FROM {table}_budgets
                              WHERE checkpoint_id = $1
                          )
                        RETURNING tenant_id
                    )
                    UPDATE {table}_tenants AS tenant
                    SET
                        outstanding_tasks = GREATEST(tenant.outstanding_tasks - 1, 0),
                        updated_at = clock_timestamp()
                    FROM finished
                    WHERE tenant.tenant_id = finished.tenant_id
                    ",
                    table = self.table
                ),
                &[
                    &lease.checkpoint_id.as_uuid(),
                    &lease.tenant_id.as_str(),
                    &lease.worker.as_str(),
                    &token,
                    &failure_reason,
                ],
            )
            .await
            .map_err(storage)
    }

    async fn resolve_finish_changed(
        &self,
        lease: &WorkflowLease,
        changed: u64,
    ) -> Result<(), WorkflowStoreError> {
        if changed != 0 {
            return Ok(());
        }
        let reservation_exists = self
            .client
            .query_opt(
                &format!(
                    "SELECT 1 FROM {table}_budgets
                     WHERE checkpoint_id = $1 AND tenant_id = $2",
                    table = self.table
                ),
                &[&lease.checkpoint_id.as_uuid(), &lease.tenant_id.as_str()],
            )
            .await
            .map_err(storage)?
            .is_some();
        if reservation_exists {
            return Err(WorkflowStoreError::new(
                WorkflowStoreErrorKind::Conflict,
                "workflow budget reservation must be settled before finish",
            ));
        }
        Err(lease_lost())
    }

    async fn finish_suspended(
        &self,
        lease: &WorkflowLease,
        wait: WorkflowWait,
    ) -> Result<(), WorkflowStoreError> {
        let changed = match wait {
            WorkflowWait::Timer { delay_ms } => self.suspend_timer(lease, delay_ms).await?,
            WorkflowWait::Signal { name } => self.suspend_signal(lease, &name, None, None).await?,
            WorkflowWait::SignalOrTimeout { name, timeout_ms } => {
                if timeout_ms == 0 {
                    return Err(WorkflowStoreError::new(
                        WorkflowStoreErrorKind::InvalidInput,
                        "durable signal timeout must be positive",
                    ));
                }
                self.suspend_signal(lease, &name, Some(timeout_ms), None)
                    .await?
            }
            WorkflowWait::Interrupt { request } => {
                let name = request.signal_name();
                let wait = WorkflowWait::Interrupt { request };
                self.suspend_signal(lease, &name, None, Some(&wait)).await?
            }
            _ => {
                return Err(WorkflowStoreError::new(
                    WorkflowStoreErrorKind::InvalidInput,
                    "workflow wait is not supported by this adapter",
                ));
            }
        };
        self.resolve_finish_changed(lease, changed).await
    }

    pub(super) async fn finish_inner(
        &self,
        lease: &WorkflowLease,
        disposition: WorkflowDisposition,
    ) -> Result<(), WorkflowStoreError> {
        let id = lease.checkpoint_id.as_uuid();
        let token = database_i64(lease.fencing_token, "fencing token")?;
        let base = " WHERE checkpoint_id = $1 AND tenant_id = $2 \
                    AND state = 'leased' AND owner = $3 \
                    AND fencing_token = $4 AND lease_expires_at > clock_timestamp() \
                    AND NOT EXISTS (
                        SELECT 1 FROM {table}_budgets WHERE checkpoint_id = $1
                    )";
        let base = base.replace("{table}", &self.table);
        let changed = match disposition {
            WorkflowDisposition::Completed => {
                return self
                    .resolve_finish_changed(
                        lease,
                        self.finish_terminal(lease, "completed", None).await?,
                    )
                    .await;
            }
            WorkflowDisposition::RetryAfter(delay) => {
                let delay = duration_millis(delay)?;
                self.client
                    .execute(
                        &format!(
                            "UPDATE {table} SET state = 'queued', owner = NULL,\
                               lease_expires_at = NULL, wait = NULL,\
                               available_at = clock_timestamp() + \
                                 ($5::BIGINT * INTERVAL '1 millisecond'),\
                               updated_at = clock_timestamp(){base}",
                            table = self.table
                        ),
                        &[
                            &id,
                            &lease.tenant_id.as_str(),
                            &lease.worker.as_str(),
                            &token,
                            &delay,
                        ],
                    )
                    .await
            }
            WorkflowDisposition::Suspend(wait) => {
                return self.finish_suspended(lease, wait).await;
            }
            WorkflowDisposition::Failed(reason) => {
                validate_failure_reason(&reason)?;
                return self
                    .resolve_finish_changed(
                        lease,
                        self.finish_terminal(lease, "failed", Some(&reason)).await?,
                    )
                    .await;
            }
            WorkflowDisposition::Cancelled => {
                return self
                    .resolve_finish_changed(
                        lease,
                        self.finish_terminal(lease, "cancelled", None).await?,
                    )
                    .await;
            }
            _ => {
                return Err(WorkflowStoreError::new(
                    WorkflowStoreErrorKind::InvalidInput,
                    "workflow disposition is not supported by this adapter",
                ));
            }
        }
        .map_err(storage)?;
        self.resolve_finish_changed(lease, changed).await
    }
}
