use super::{
    Arc, CheckpointErrorKind, Duration, Either, LeaseDuration, SystemWorkflowWorkerSleeper, Usage,
    WorkerId, WorkflowCheckpoint, WorkflowDefinition, WorkflowDisposition, WorkflowError,
    WorkflowExecution, WorkflowFailurePolicy, WorkflowLease, WorkflowRegistry, WorkflowStore,
    WorkflowStoreError, WorkflowStoreErrorKind, WorkflowTask, WorkflowWake, WorkflowWorker,
    WorkflowWorkerError, WorkflowWorkerOutcome, WorkflowWorkerSleeper, select,
};

impl WorkflowWorker {
    /// Creates a worker with validated heartbeat timing.
    ///
    /// # Errors
    ///
    /// Rejects a zero heartbeat interval or one not shorter than the lease.
    pub fn new(
        store: Arc<dyn WorkflowStore>,
        registry: WorkflowRegistry,
        worker: WorkerId,
        lease_duration: LeaseDuration,
        heartbeat_interval: Duration,
    ) -> Result<Self, WorkflowWorkerError> {
        let heartbeat_ms = u64::try_from(heartbeat_interval.as_millis()).map_err(|_| {
            WorkflowWorkerError::InvalidConfig(
                "heartbeat interval exceeds the supported millisecond range".into(),
            )
        })?;
        if heartbeat_ms == 0 || heartbeat_ms >= lease_duration.as_millis() {
            return Err(WorkflowWorkerError::InvalidConfig(
                "heartbeat interval must be positive and shorter than the lease".into(),
            ));
        }
        Ok(Self {
            store,
            registry,
            worker,
            lease_duration,
            heartbeat_interval,
            missing_definition_retry: Duration::from_secs(5),
            budget_denied_retry: Duration::from_secs(5),
            sleeper: Arc::new(SystemWorkflowWorkerSleeper),
        })
    }

    /// Overrides the retry delay for definitions absent from this worker.
    #[must_use]
    pub const fn with_missing_definition_retry(mut self, delay: Duration) -> Self {
        self.missing_definition_retry = delay;
        self
    }

    /// Overrides the retry delay when a tenant aggregate budget is exhausted.
    #[must_use]
    pub const fn with_budget_denied_retry(mut self, delay: Duration) -> Self {
        self.budget_denied_retry = delay;
        self
    }

    /// Overrides heartbeat sleeping for deterministic runtimes and tests.
    #[must_use]
    pub fn with_sleeper(mut self, sleeper: Arc<dyn WorkflowWorkerSleeper>) -> Self {
        self.sleeper = sleeper;
        self
    }

    /// Claims and processes at most one task.
    ///
    /// # Errors
    ///
    /// Returns infrastructure, checkpoint, or restored-budget failures.
    pub async fn run_once(&self) -> Result<WorkflowWorkerOutcome, WorkflowWorkerError> {
        let Some(claimed) = self
            .store
            .claim(self.worker.clone(), self.lease_duration)
            .await?
        else {
            return Ok(WorkflowWorkerOutcome::Idle);
        };
        let id = claimed.task.checkpoint_id;
        let Some(definition) = self.registry.get(&claimed.task).cloned() else {
            return self
                .finish_or_lose(
                    claimed.lease,
                    WorkflowDisposition::RetryAfter(self.missing_definition_retry),
                    WorkflowWorkerOutcome::DefinitionUnavailable { checkpoint_id: id },
                )
                .await;
        };
        self.execute_claim(claimed.task, claimed.lease, claimed.wake, definition)
            .await
    }

    async fn execute_claim(
        &self,
        task: WorkflowTask,
        lease: WorkflowLease,
        wake: Option<WorkflowWake>,
        definition: Arc<WorkflowDefinition>,
    ) -> Result<WorkflowWorkerOutcome, WorkflowWorkerError> {
        let checkpoint = WorkflowCheckpoint::distributed(self.store.clone(), lease.clone());
        let persisted = match checkpoint.load_async().await {
            Ok((_, state)) => Some(state),
            Err(error) if error.kind == CheckpointErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let usage = persisted
            .as_ref()
            .map_or_else(Usage::default, |state| state.usage);
        let run = definition.run_context(usage)?;
        match self
            .store
            .reserve_budget(lease.clone(), definition.budget, usage)
            .await
        {
            Ok(_) => {}
            Err(error) if error.kind == WorkflowStoreErrorKind::AdmissionDenied => {
                return self
                    .finish_or_lose(
                        lease,
                        WorkflowDisposition::RetryAfter(self.budget_denied_retry),
                        WorkflowWorkerOutcome::Retried {
                            checkpoint_id: task.checkpoint_id,
                        },
                    )
                    .await;
            }
            Err(error) if error.kind == WorkflowStoreErrorKind::InvalidInput => {
                return self
                    .finish_or_lose(
                        lease,
                        WorkflowDisposition::Failed(
                            "workflow definition is incompatible with tenant budget policy".into(),
                        ),
                        WorkflowWorkerOutcome::Failed {
                            checkpoint_id: task.checkpoint_id,
                        },
                    )
                    .await;
            }
            Err(error) => return Err(error.into()),
        }
        let execution = async {
            if persisted.is_some() {
                definition
                    .workflow
                    .resume_controlled(&checkpoint, &run, definition.resume_policy, wake)
                    .await
            } else {
                definition
                    .workflow
                    .run_checkpointed_controlled(task.input, &run, &checkpoint)
                    .await
            }
        };
        let heartbeat = heartbeat_until_failure(
            self.store.clone(),
            lease.clone(),
            self.lease_duration,
            self.heartbeat_interval,
            self.sleeper.clone(),
        );
        match select(Box::pin(execution), Box::pin(heartbeat)).await {
            Either::Left((result, pending_heartbeat)) => {
                drop(pending_heartbeat);
                let usage = run.budget().usage();
                self.finish_execution(lease, definition.failure_policy, result, usage)
                    .await
            }
            Either::Right((_heartbeat_error, pending_execution)) => {
                run.cancellation().cancel();
                let _ = pending_execution.await;
                Ok(WorkflowWorkerOutcome::LeaseLost {
                    checkpoint_id: lease.checkpoint_id,
                })
            }
        }
    }

    async fn finish_execution(
        &self,
        lease: WorkflowLease,
        failure_policy: WorkflowFailurePolicy,
        result: Result<WorkflowExecution, WorkflowError>,
        usage: Usage,
    ) -> Result<WorkflowWorkerOutcome, WorkflowWorkerError> {
        let id = lease.checkpoint_id;
        match self.store.settle_budget(lease.clone(), usage).await {
            Ok(()) => {}
            Err(error) if error.kind == WorkflowStoreErrorKind::LeaseLost => {
                return Ok(WorkflowWorkerOutcome::LeaseLost { checkpoint_id: id });
            }
            Err(error) => return Err(error.into()),
        }
        match result {
            Ok(WorkflowExecution::Completed(outcome)) => {
                self.finish_or_lose(
                    lease,
                    WorkflowDisposition::Completed,
                    WorkflowWorkerOutcome::Completed {
                        checkpoint_id: id,
                        outcome,
                    },
                )
                .await
            }
            Ok(WorkflowExecution::Suspended(wait)) => {
                self.finish_or_lose(
                    lease,
                    WorkflowDisposition::Suspend(wait),
                    WorkflowWorkerOutcome::Suspended { checkpoint_id: id },
                )
                .await
            }
            Err(error) => match failure_policy {
                WorkflowFailurePolicy::Fail => {
                    self.finish_or_lose(
                        lease,
                        WorkflowDisposition::Failed(safe_failure_reason(&error)),
                        WorkflowWorkerOutcome::Failed { checkpoint_id: id },
                    )
                    .await
                }
                WorkflowFailurePolicy::RetryAfter(delay) => {
                    self.finish_or_lose(
                        lease,
                        WorkflowDisposition::RetryAfter(delay),
                        WorkflowWorkerOutcome::Retried { checkpoint_id: id },
                    )
                    .await
                }
            },
        }
    }

    async fn finish_or_lose(
        &self,
        lease: WorkflowLease,
        disposition: WorkflowDisposition,
        outcome: WorkflowWorkerOutcome,
    ) -> Result<WorkflowWorkerOutcome, WorkflowWorkerError> {
        let id = lease.checkpoint_id;
        match self.store.finish(lease, disposition).await {
            Ok(()) => Ok(outcome),
            Err(error) if error.kind == WorkflowStoreErrorKind::LeaseLost => {
                Ok(WorkflowWorkerOutcome::LeaseLost { checkpoint_id: id })
            }
            Err(error) => Err(error.into()),
        }
    }
}

impl std::fmt::Debug for WorkflowWorker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowWorker")
            .field("registry", &self.registry)
            .field("worker", &self.worker)
            .field("lease_duration", &self.lease_duration)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .finish_non_exhaustive()
    }
}

async fn heartbeat_until_failure(
    store: Arc<dyn WorkflowStore>,
    mut lease: WorkflowLease,
    extension: LeaseDuration,
    interval: Duration,
    sleeper: Arc<dyn WorkflowWorkerSleeper>,
) -> WorkflowStoreError {
    loop {
        sleeper.sleep(interval).await;
        match store.heartbeat(lease, extension).await {
            Ok(renewed) => lease = renewed,
            Err(error) => return error,
        }
    }
}

fn safe_failure_reason(error: &WorkflowError) -> String {
    let message = error.to_string();
    if message.len() <= 1_024 {
        return message;
    }
    let mut boundary = 1_024;
    while !message.is_char_boundary(boundary) {
        boundary -= 1;
    }
    message[..boundary].to_owned()
}
