//! Workflow checkpoint history, fork/replay, and lease-fenced CAS operations.

use super::{
    Checkpoint, CheckpointError, CheckpointErrorKind, CheckpointId, ForkSource,
    PostgresWorkflowStore, PreparedFork, SqlState, Value, WorkflowCheckpointHistoryLimit,
    WorkflowCheckpointRevision, WorkflowForkCommand, WorkflowForkOutcome, WorkflowLease,
    WorkflowLineage, WorkflowStoreError, WorkflowStoreErrorKind, WorkflowStoreFuture,
    WorkflowTaskSnapshot, WorkflowTenantId, checkpoint_decoding, checkpoint_domain_error,
    checkpoint_encoding, checkpoint_i64, checkpoint_lease_lost, checkpoint_storage, database_i64,
    decode_snapshot, fork_storage_fields, storage, tenant_mismatch,
};

pub(super) trait CheckpointStoreExt {
    fn inspect_ext(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskSnapshot, WorkflowStoreError>>;

    fn list_checkpoint_history_ext(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        after_revision: Option<u64>,
        limit: WorkflowCheckpointHistoryLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowCheckpointRevision>, WorkflowStoreError>>;

    fn load_checkpoint_revision_ext(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        revision: u64,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowCheckpointRevision, WorkflowStoreError>>;

    fn fork_workflow_ext(
        &self,
        tenant_id: WorkflowTenantId,
        command: WorkflowForkCommand,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowForkOutcome, WorkflowStoreError>>;

    fn load_checkpoint_ext(
        &self,
        lease: WorkflowLease,
    ) -> WorkflowStoreFuture<'_, Result<Checkpoint, CheckpointError>>;

    fn compare_and_swap_checkpoint_ext(
        &self,
        lease: WorkflowLease,
        checkpoint: Checkpoint,
        expected_revision: Option<u64>,
    ) -> WorkflowStoreFuture<'_, Result<(), CheckpointError>>;
}

impl PostgresWorkflowStore {
    pub(super) async fn tenant_scoped_not_found(
        &self,
        tenant_id: &WorkflowTenantId,
        checkpoint_id: CheckpointId,
    ) -> Result<WorkflowStoreError, WorkflowStoreError> {
        let actual = self
            .client
            .query_opt(
                &format!(
                    "SELECT tenant_id FROM {table} WHERE checkpoint_id = $1",
                    table = self.table
                ),
                &[&checkpoint_id.as_uuid()],
            )
            .await
            .map_err(storage)?
            .map(|row| row.try_get::<_, String>(0))
            .transpose()
            .map_err(storage)?;
        Ok(match actual {
            Some(actual) if actual != tenant_id.as_str() => tenant_mismatch(),
            _ => WorkflowStoreError::new(
                WorkflowStoreErrorKind::NotFound,
                format!("workflow task `{checkpoint_id}` does not exist"),
            ),
        })
    }

    async fn load_fork_source(
        &self,
        tenant_id: &WorkflowTenantId,
        command: &WorkflowForkCommand,
    ) -> Result<ForkSource, WorkflowStoreError> {
        let source_revision = database_i64(command.source_revision, "source checkpoint revision")?;
        let source = self
            .client
            .query_opt(
                &format!(
                    "SELECT task.workflow, task.workflow_version, task.input, task.priority, \
                            history.checkpoint \
                     FROM {table} AS task \
                     JOIN {table}_checkpoint_history AS history \
                       ON history.checkpoint_id = task.checkpoint_id \
                      AND history.revision = $3 \
                     WHERE task.checkpoint_id = $1 AND task.tenant_id = $2",
                    table = self.table
                ),
                &[
                    &command.source_checkpoint_id.as_uuid(),
                    &tenant_id.as_str(),
                    &source_revision,
                ],
            )
            .await
            .map_err(storage)?
            .ok_or_else(|| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::NotFound,
                    "source workflow checkpoint revision does not exist",
                )
            })?;
        let checkpoint = source.try_get::<_, Value>(4).map_err(storage)?;
        Ok(ForkSource {
            workflow: source.try_get(0).map_err(storage)?,
            workflow_version: source.try_get(1).map_err(storage)?,
            input: source.try_get(2).map_err(storage)?,
            priority: source.try_get(3).map_err(storage)?,
            checkpoint: serde_json::from_value(checkpoint).map_err(checkpoint_decoding)?,
        })
    }

    async fn insert_fork(
        &self,
        tenant_id: &WorkflowTenantId,
        command: &WorkflowForkCommand,
        prepared: &PreparedFork,
    ) -> Result<u64, tokio_postgres::Error> {
        self.client
            .execute(
                &format!(
                    r"
                    WITH admitted AS (
                        UPDATE {table}_tenants
                        SET outstanding_tasks = outstanding_tasks + 1,
                            updated_at = clock_timestamp()
                        WHERE tenant_id = $2
                          AND outstanding_tasks < max_outstanding_tasks
                        RETURNING tenant_id
                    )
                    INSERT INTO {table} (
                        checkpoint_id, tenant_id, workflow, workflow_version,
                        input, priority, state, available_at,
                        wait_kind, wait_name, wait, wake_at,
                        checkpoint_revision, checkpoint, lineage
                    )
                    SELECT
                        $1, $2, $3, $4, $5, $6, $7, clock_timestamp(),
                        $8, $9, $10,
                        CASE WHEN $11::BIGINT IS NULL THEN NULL
                             ELSE clock_timestamp()
                                + ($11::BIGINT * INTERVAL '1 millisecond')
                        END,
                        0, $12, $13
                    FROM admitted
                    ",
                    table = self.table
                ),
                &[
                    &command.fork_checkpoint_id.as_uuid(),
                    &tenant_id.as_str(),
                    &prepared.source.workflow,
                    &prepared.source.workflow_version,
                    &prepared.source.input,
                    &prepared.source.priority,
                    &prepared.fields.state,
                    &prepared.fields.wait_kind,
                    &prepared.fields.wait_name,
                    &prepared.fields.wait,
                    &prepared.fields.wake_delay_ms,
                    &prepared.checkpoint,
                    &prepared.lineage,
                ],
            )
            .await
    }

    async fn resolve_fork_conflict(
        &self,
        tenant_id: &WorkflowTenantId,
        command: &WorkflowForkCommand,
        lineage: &Value,
    ) -> Result<WorkflowForkOutcome, WorkflowStoreError> {
        let existing = self
            .client
            .query_opt(
                &format!(
                    "SELECT tenant_id, lineage FROM {table} WHERE checkpoint_id = $1",
                    table = self.table
                ),
                &[&command.fork_checkpoint_id.as_uuid()],
            )
            .await
            .map_err(storage)?;
        let duplicate = existing.is_some_and(|row| {
            row.try_get::<_, String>(0)
                .is_ok_and(|value| value == tenant_id.as_str())
                && row
                    .try_get::<_, Option<Value>>(1)
                    .is_ok_and(|value| value.as_ref() == Some(lineage))
        });
        if duplicate {
            Ok(WorkflowForkOutcome::Duplicate {
                checkpoint_id: command.fork_checkpoint_id,
            })
        } else {
            Err(WorkflowStoreError::new(
                WorkflowStoreErrorKind::Conflict,
                "workflow fork identity is already bound to different content",
            ))
        }
    }
}

impl CheckpointStoreExt for PostgresWorkflowStore {
    fn inspect_ext(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskSnapshot, WorkflowStoreError>> {
        Box::pin(async move {
            let row = self
                .client
                .query_opt(
                    &format!(
                        "SELECT state, attempts, fencing_token, owner, \
                           (EXTRACT(EPOCH FROM lease_expires_at) * 1000)::BIGINT, wait, lineage, \
                           failure_reason, \
                           (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT, \
                           (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT, \
                           workflow, workflow_version \
                         FROM {table} WHERE checkpoint_id = $1 AND tenant_id = $2",
                        table = self.table
                    ),
                    &[&checkpoint_id.as_uuid(), &tenant_id.as_str()],
                )
                .await
                .map_err(storage)?;
            let Some(row) = row else {
                return Err(self
                    .tenant_scoped_not_found(&tenant_id, checkpoint_id)
                    .await?);
            };
            decode_snapshot(checkpoint_id, tenant_id, &row)
        })
    }

    fn list_checkpoint_history_ext(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        after_revision: Option<u64>,
        limit: WorkflowCheckpointHistoryLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowCheckpointRevision>, WorkflowStoreError>> {
        Box::pin(async move {
            let after = after_revision
                .map(|value| database_i64(value, "checkpoint history cursor"))
                .transpose()?;
            let limit = i64::from(limit.get());
            let rows = self
                .client
                .query(
                    &format!(
                        "SELECT history.checkpoint \
                         FROM {table}_checkpoint_history AS history \
                         JOIN {table} AS task \
                           ON task.checkpoint_id = history.checkpoint_id \
                         WHERE history.checkpoint_id = $1 AND task.tenant_id = $2 \
                           AND ($3::BIGINT IS NULL OR history.revision > $3) \
                         ORDER BY history.revision ASC LIMIT $4",
                        table = self.table
                    ),
                    &[
                        &checkpoint_id.as_uuid(),
                        &tenant_id.as_str(),
                        &after,
                        &limit,
                    ],
                )
                .await
                .map_err(storage)?;
            if rows.is_empty() {
                self.client
                    .query_opt(
                        &format!(
                            "SELECT 1 FROM {table} WHERE checkpoint_id = $1 AND tenant_id = $2",
                            table = self.table
                        ),
                        &[&checkpoint_id.as_uuid(), &tenant_id.as_str()],
                    )
                    .await
                    .map_err(storage)?
                    .ok_or(
                        self.tenant_scoped_not_found(&tenant_id, checkpoint_id)
                            .await?,
                    )?;
            }
            rows.into_iter()
                .map(|row| {
                    let value = row.try_get::<_, Value>(0).map_err(storage)?;
                    let checkpoint = serde_json::from_value(value).map_err(checkpoint_decoding)?;
                    WorkflowCheckpointRevision::from_checkpoint(checkpoint)
                        .map_err(checkpoint_domain_error)
                })
                .collect()
        })
    }

    fn load_checkpoint_revision_ext(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        revision: u64,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowCheckpointRevision, WorkflowStoreError>> {
        Box::pin(async move {
            let revision = database_i64(revision, "checkpoint revision")?;
            let row = self
                .client
                .query_opt(
                    &format!(
                        "SELECT history.checkpoint \
                         FROM {table}_checkpoint_history AS history \
                         JOIN {table} AS task \
                           ON task.checkpoint_id = history.checkpoint_id \
                         WHERE history.checkpoint_id = $1 AND history.revision = $2 \
                           AND task.tenant_id = $3",
                        table = self.table
                    ),
                    &[&checkpoint_id.as_uuid(), &revision, &tenant_id.as_str()],
                )
                .await
                .map_err(storage)?
                .ok_or_else(|| {
                    WorkflowStoreError::new(
                        WorkflowStoreErrorKind::NotFound,
                        "workflow checkpoint revision does not exist",
                    )
                })?;
            let value = row.try_get::<_, Value>(0).map_err(storage)?;
            let checkpoint = serde_json::from_value(value).map_err(checkpoint_decoding)?;
            WorkflowCheckpointRevision::from_checkpoint(checkpoint).map_err(checkpoint_domain_error)
        })
    }

    fn fork_workflow_ext(
        &self,
        tenant_id: WorkflowTenantId,
        command: WorkflowForkCommand,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowForkOutcome, WorkflowStoreError>> {
        Box::pin(async move {
            if command.fork_checkpoint_id == command.source_checkpoint_id {
                return Err(WorkflowStoreError::new(
                    WorkflowStoreErrorKind::InvalidInput,
                    "workflow fork target must differ from its source",
                ));
            }
            let source = self.load_fork_source(&tenant_id, &command).await?;
            let forked = command
                .prepare_checkpoint(source.checkpoint.clone())
                .map_err(checkpoint_domain_error)?;
            let revision = WorkflowCheckpointRevision::from_checkpoint(forked.clone())
                .map_err(checkpoint_domain_error)?;
            let lineage = WorkflowLineage {
                parent_checkpoint_id: command.source_checkpoint_id,
                parent_revision: command.source_revision,
                policy: command.policy,
            };
            let prepared = PreparedFork {
                source,
                checkpoint: serde_json::to_value(forked).map_err(checkpoint_encoding)?,
                lineage: serde_json::to_value(lineage).map_err(checkpoint_encoding)?,
                fields: fork_storage_fields(&revision)?,
            };
            let inserted = self.insert_fork(&tenant_id, &command, &prepared).await;
            match inserted {
                Ok(1) => Ok(WorkflowForkOutcome::Created {
                    checkpoint_id: command.fork_checkpoint_id,
                }),
                Ok(_) => Err(WorkflowStoreError::new(
                    WorkflowStoreErrorKind::AdmissionDenied,
                    "workflow tenant reached its outstanding task limit",
                )),
                Err(error) if error.code() == Some(&SqlState::UNIQUE_VIOLATION) => {
                    self.resolve_fork_conflict(&tenant_id, &command, &prepared.lineage)
                        .await
                }
                Err(error) => Err(storage(error)),
            }
        })
    }

    fn load_checkpoint_ext(
        &self,
        lease: WorkflowLease,
    ) -> WorkflowStoreFuture<'_, Result<Checkpoint, CheckpointError>> {
        Box::pin(async move {
            let token = checkpoint_i64(lease.fencing_token, "fencing token")?;
            let row = self
                .client
                .query_opt(
                    &format!(
                        "SELECT checkpoint FROM {table} \
                         WHERE checkpoint_id = $1 AND tenant_id = $2 \
                           AND state = 'leased' AND owner = $3 \
                           AND fencing_token = $4 AND lease_expires_at > clock_timestamp()",
                        table = self.table
                    ),
                    &[
                        &lease.checkpoint_id.as_uuid(),
                        &lease.tenant_id.as_str(),
                        &lease.worker.as_str(),
                        &token,
                    ],
                )
                .await
                .map_err(checkpoint_storage)?
                .ok_or_else(checkpoint_lease_lost)?;
            let value = row
                .try_get::<_, Option<Value>>(0)
                .map_err(checkpoint_storage)?
                .ok_or_else(|| {
                    CheckpointError::new(
                        CheckpointErrorKind::NotFound,
                        "workflow checkpoint does not exist",
                    )
                })?;
            serde_json::from_value(value).map_err(|error| {
                CheckpointError::new(CheckpointErrorKind::InvalidPayload, error.to_string())
            })
        })
    }

    fn compare_and_swap_checkpoint_ext(
        &self,
        lease: WorkflowLease,
        checkpoint: Checkpoint,
        expected_revision: Option<u64>,
    ) -> WorkflowStoreFuture<'_, Result<(), CheckpointError>> {
        Box::pin(async move {
            if checkpoint.id != lease.checkpoint_id {
                return Err(CheckpointError::new(
                    CheckpointErrorKind::InvalidPayload,
                    "checkpoint identity does not match the workflow lease",
                ));
            }
            let token = checkpoint_i64(lease.fencing_token, "fencing token")?;
            let revision = checkpoint_i64(checkpoint.revision, "checkpoint revision")?;
            let value = serde_json::to_value(&checkpoint).map_err(|error| {
                CheckpointError::new(CheckpointErrorKind::InvalidPayload, error.to_string())
            })?;
            let (sql, changed) = match expected_revision {
                None => {
                    if checkpoint.revision != 0 {
                        return Err(CheckpointError::new(
                            CheckpointErrorKind::Conflict,
                            "initial workflow checkpoint revision must be zero",
                        ));
                    }
                    let changed = self
                        .client
                        .execute(
                            &format!(
                                "UPDATE {table} SET checkpoint_revision = $5, checkpoint = $6, \
                                   updated_at = clock_timestamp() \
                                 WHERE checkpoint_id = $1 AND tenant_id = $2 \
                                   AND state = 'leased' AND owner = $3 \
                                   AND fencing_token = $4 \
                                   AND lease_expires_at > clock_timestamp() \
                                   AND checkpoint IS NULL",
                                table = self.table
                            ),
                            &[
                                &lease.checkpoint_id.as_uuid(),
                                &lease.tenant_id.as_str(),
                                &lease.worker.as_str(),
                                &token,
                                &revision,
                                &value,
                            ],
                        )
                        .await
                        .map_err(checkpoint_storage)?;
                    ("create", changed)
                }
                Some(expected) => {
                    if expected
                        .checked_add(1)
                        .is_none_or(|next| checkpoint.revision != next)
                    {
                        return Err(CheckpointError::new(
                            CheckpointErrorKind::Conflict,
                            "workflow checkpoint revision is not the expected successor",
                        ));
                    }
                    let expected = checkpoint_i64(expected, "expected checkpoint revision")?;
                    let changed = self
                        .client
                        .execute(
                            &format!(
                                "UPDATE {table} SET checkpoint_revision = $6, checkpoint = $7, \
                                   updated_at = clock_timestamp() \
                                 WHERE checkpoint_id = $1 AND tenant_id = $2 \
                                   AND state = 'leased' AND owner = $3 \
                                   AND fencing_token = $4 \
                                   AND lease_expires_at > clock_timestamp() \
                                   AND checkpoint_revision = $5",
                                table = self.table
                            ),
                            &[
                                &lease.checkpoint_id.as_uuid(),
                                &lease.tenant_id.as_str(),
                                &lease.worker.as_str(),
                                &token,
                                &expected,
                                &revision,
                                &value,
                            ],
                        )
                        .await
                        .map_err(checkpoint_storage)?;
                    ("update", changed)
                }
            };
            if changed == 0 {
                return Err(CheckpointError::new(
                    CheckpointErrorKind::Conflict,
                    format!("workflow checkpoint {sql} precondition failed"),
                ));
            }
            Ok(())
        })
    }
}
