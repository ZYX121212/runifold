use super::{
    AdmissionState, BTreeMap, Checkpoint, CheckpointError, CheckpointErrorKind, CheckpointId,
    InMemoryWorkflowStore, StoredState, StoredTask, StoredTenant, WorkflowCheckpointHistoryLimit,
    WorkflowCheckpointPhase, WorkflowCheckpointRevision, WorkflowForkCommand, WorkflowForkOutcome,
    WorkflowLease, WorkflowLineage, WorkflowStoreError, WorkflowStoreErrorKind,
    WorkflowStoreFuture, WorkflowTask, WorkflowTenantId, WorkflowTenantPolicy, WorkflowWait,
    decode_revision, fork_checkpoint, is_non_terminal, require_current_lease, require_tenant,
};

pub(super) fn checkpoint_store_error(error: CheckpointError) -> WorkflowStoreError {
    let kind = match error.kind {
        CheckpointErrorKind::NotFound => WorkflowStoreErrorKind::NotFound,
        CheckpointErrorKind::Conflict => WorkflowStoreErrorKind::Conflict,
        CheckpointErrorKind::InvalidPayload => WorkflowStoreErrorKind::InvalidInput,
        _ => WorkflowStoreErrorKind::Storage,
    };
    WorkflowStoreError::new(kind, error.message)
}

pub(super) fn admit_fork(
    admission: &mut AdmissionState,
    tasks: &BTreeMap<CheckpointId, StoredTask>,
    source: &WorkflowTask,
) -> Result<(), WorkflowStoreError> {
    let tenant = admission
        .tenants
        .entry(source.tenant_id.clone())
        .or_insert(StoredTenant {
            policy: WorkflowTenantPolicy::default(),
            last_claim_sequence: 0,
        });
    let outstanding = tasks
        .values()
        .filter(|stored| {
            stored.task.tenant_id == source.tenant_id && is_non_terminal(&stored.state)
        })
        .count();
    let limit = usize::try_from(tenant.policy.max_outstanding_tasks()).unwrap_or(usize::MAX);
    if outstanding >= limit {
        return Err(WorkflowStoreError::new(
            WorkflowStoreErrorKind::AdmissionDenied,
            format!(
                "workflow tenant `{}` reached its outstanding task limit",
                source.tenant_id.as_str()
            ),
        ));
    }
    Ok(())
}

pub(super) fn forked_state(state: &crate::WorkflowCheckpointState, now: u64) -> StoredState {
    match &state.phase {
        WorkflowCheckpointPhase::Waiting { wait, .. } => match wait {
            WorkflowWait::Timer { delay_ms } => StoredState::WaitingTimer {
                wake_at_ms: now.saturating_add(*delay_ms),
            },
            WorkflowWait::Signal { name } => StoredState::WaitingSignal { name: name.clone() },
            WorkflowWait::SignalOrTimeout { name, timeout_ms } => {
                StoredState::WaitingSignalOrTimeout {
                    name: name.clone(),
                    wake_at_ms: now.saturating_add(*timeout_ms),
                }
            }
            WorkflowWait::Interrupt { request } => StoredState::WaitingInterrupt {
                request: request.clone(),
            },
        },
        _ => StoredState::Queued {
            available_at_ms: now,
        },
    }
}

pub(super) fn validate_checkpoint_cas(
    current: Option<&Checkpoint>,
    checkpoint: &Checkpoint,
    expected_revision: Option<u64>,
) -> Result<(), CheckpointError> {
    match (current, expected_revision) {
        (None, None) if checkpoint.revision == 0 => Ok(()),
        (Some(current), Some(expected))
            if current.revision == expected
                && expected
                    .checked_add(1)
                    .is_some_and(|next| checkpoint.revision == next) =>
        {
            Ok(())
        }
        (None, Some(_)) => Err(checkpoint_not_found()),
        _ => Err(CheckpointError::new(
            CheckpointErrorKind::Conflict,
            format!(
                "checkpoint `{}` revision precondition failed",
                checkpoint.id
            ),
        )),
    }
}

pub(super) fn checkpoint_not_found() -> CheckpointError {
    CheckpointError::new(
        CheckpointErrorKind::NotFound,
        "workflow checkpoint does not exist",
    )
}

pub(super) fn checkpoint_lease_error(error: WorkflowStoreError) -> CheckpointError {
    let kind = match error.kind {
        WorkflowStoreErrorKind::LeaseLost
        | WorkflowStoreErrorKind::Conflict
        | WorkflowStoreErrorKind::AdmissionDenied
        | WorkflowStoreErrorKind::TenantMismatch => CheckpointErrorKind::Conflict,
        WorkflowStoreErrorKind::NotFound => CheckpointErrorKind::NotFound,
        WorkflowStoreErrorKind::InvalidInput => CheckpointErrorKind::InvalidPayload,
        WorkflowStoreErrorKind::Storage => CheckpointErrorKind::Storage,
    };
    CheckpointError::new(kind, error.message)
}

impl InMemoryWorkflowStore {
    pub(super) fn list_checkpoint_history_impl(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        after_revision: Option<u64>,
        limit: WorkflowCheckpointHistoryLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowCheckpointRevision>, WorkflowStoreError>> {
        Box::pin(async move {
            let tasks = self.tasks();
            let stored = tasks.get(&checkpoint_id).ok_or_else(|| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::NotFound,
                    format!("workflow task `{checkpoint_id}` does not exist"),
                )
            })?;
            require_tenant(&stored.task.tenant_id, &tenant_id)?;
            let checkpoints = self
                .checkpoints
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            checkpoints
                .history
                .range((checkpoint_id, 0)..=(checkpoint_id, u64::MAX))
                .filter(|((_, revision), _)| after_revision.is_none_or(|after| *revision > after))
                .take(usize::from(limit.get()))
                .map(|(_, checkpoint)| {
                    decode_revision(checkpoint.clone()).map_err(checkpoint_store_error)
                })
                .collect()
        })
    }

    pub(super) fn load_checkpoint_revision_impl(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        revision: u64,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowCheckpointRevision, WorkflowStoreError>> {
        Box::pin(async move {
            let tasks = self.tasks();
            let stored = tasks.get(&checkpoint_id).ok_or_else(|| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::NotFound,
                    format!("workflow task `{checkpoint_id}` does not exist"),
                )
            })?;
            require_tenant(&stored.task.tenant_id, &tenant_id)?;
            let checkpoints = self
                .checkpoints
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let checkpoint = checkpoints
                .history
                .get(&(checkpoint_id, revision))
                .cloned()
                .ok_or_else(|| {
                    WorkflowStoreError::new(
                        WorkflowStoreErrorKind::NotFound,
                        format!(
                            "workflow checkpoint `{checkpoint_id}` revision `{revision}` does not exist"
                        ),
                    )
                })?;
            decode_revision(checkpoint).map_err(checkpoint_store_error)
        })
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "the owned command must outlive the returned asynchronous store operation"
    )]
    pub(super) fn fork_workflow_impl(
        &self,
        tenant_id: WorkflowTenantId,
        command: WorkflowForkCommand,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowForkOutcome, WorkflowStoreError>> {
        Box::pin(async move {
            let now = self.clock.now_ms();
            if command.fork_checkpoint_id == command.source_checkpoint_id {
                return Err(WorkflowStoreError::invalid_input(
                    "workflow fork target must differ from its source",
                ));
            }
            let mut admission = self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut tasks = self.tasks();
            let source = tasks
                .get(&command.source_checkpoint_id)
                .ok_or_else(|| {
                    WorkflowStoreError::new(
                        WorkflowStoreErrorKind::NotFound,
                        format!(
                            "workflow task `{}` does not exist",
                            command.source_checkpoint_id
                        ),
                    )
                })?
                .clone();
            require_tenant(&source.task.tenant_id, &tenant_id)?;
            let lineage = WorkflowLineage {
                parent_checkpoint_id: command.source_checkpoint_id,
                parent_revision: command.source_revision,
                policy: command.policy,
            };
            if let Some(existing) = tasks.get(&command.fork_checkpoint_id) {
                return if existing.task.tenant_id == tenant_id
                    && existing.lineage.as_ref() == Some(&lineage)
                {
                    Ok(WorkflowForkOutcome::Duplicate {
                        checkpoint_id: command.fork_checkpoint_id,
                    })
                } else {
                    Err(WorkflowStoreError::new(
                        WorkflowStoreErrorKind::Conflict,
                        "workflow fork identity is already bound to different content",
                    ))
                };
            }
            let mut checkpoints = self
                .checkpoints
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let historical = checkpoints
                .history
                .get(&(command.source_checkpoint_id, command.source_revision))
                .cloned()
                .ok_or_else(|| {
                    WorkflowStoreError::new(
                        WorkflowStoreErrorKind::NotFound,
                        "source workflow checkpoint revision does not exist",
                    )
                })?;
            let forked = fork_checkpoint(historical, command.fork_checkpoint_id, command.policy)
                .map_err(checkpoint_store_error)?;
            let revision = decode_revision(forked.clone()).map_err(checkpoint_store_error)?;
            admit_fork(&mut admission, &tasks, &source.task)?;
            let task = WorkflowTask {
                checkpoint_id: command.fork_checkpoint_id,
                tenant_id,
                workflow: source.task.workflow,
                workflow_version: source.task.workflow_version,
                input: source.task.input,
                priority: source.task.priority,
            };
            tasks.insert(
                task.checkpoint_id,
                StoredTask {
                    task,
                    state: forked_state(&revision.state, now),
                    attempts: 0,
                    fencing_token: 0,
                    wake: None,
                    lineage: Some(lineage),
                    created_at_ms: now,
                    updated_at_ms: now,
                },
            );
            checkpoints.latest.insert(forked.id, forked.clone());
            checkpoints.history.insert((forked.id, 0), forked);
            Ok(WorkflowForkOutcome::Created {
                checkpoint_id: command.fork_checkpoint_id,
            })
        })
    }

    pub(super) fn load_checkpoint_impl(
        &self,
        lease: WorkflowLease,
    ) -> WorkflowStoreFuture<'_, Result<Checkpoint, CheckpointError>> {
        Box::pin(async move {
            let now = self.clock.now_ms();
            let tasks = self.tasks();
            let stored = tasks
                .get(&lease.checkpoint_id)
                .ok_or_else(checkpoint_not_found)?;
            require_current_lease(stored, &lease, now).map_err(checkpoint_lease_error)?;
            let checkpoints = self
                .checkpoints
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            checkpoints
                .latest
                .get(&lease.checkpoint_id)
                .cloned()
                .ok_or_else(checkpoint_not_found)
        })
    }

    pub(super) fn compare_and_swap_checkpoint_impl(
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
            let now = self.clock.now_ms();
            let tasks = self.tasks();
            let stored = tasks
                .get(&lease.checkpoint_id)
                .ok_or_else(checkpoint_not_found)?;
            require_current_lease(stored, &lease, now).map_err(checkpoint_lease_error)?;
            let mut checkpoints = self
                .checkpoints
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            validate_checkpoint_cas(
                checkpoints.latest.get(&checkpoint.id),
                &checkpoint,
                expected_revision,
            )?;
            checkpoints.latest.insert(checkpoint.id, checkpoint.clone());
            checkpoints
                .history
                .insert((checkpoint.id, checkpoint.revision), checkpoint);
            Ok(())
        })
    }
}
