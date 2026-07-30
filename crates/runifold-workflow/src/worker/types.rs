use super::{
    Arc, AtomicU64, AtomicUsize, BTreeMap, Budget, BudgetExceeded, BudgetTracker, CapabilitySet,
    CheckpointError, CheckpointId, Delay, Duration, Error, Future, Journal, LeaseDuration,
    Ordering, Pin, RunContext, Usage, WorkerId, Workflow, WorkflowOutcome, WorkflowResumePolicy,
    WorkflowStore, WorkflowStoreError, WorkflowTask,
};

/// A boxed worker sleep operation.
pub type WorkflowWorkerSleepFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Replaceable worker timer used by heartbeat supervision.
pub trait WorkflowWorkerSleeper: Send + Sync {
    /// Waits for one heartbeat interval.
    fn sleep(&self, duration: Duration) -> WorkflowWorkerSleepFuture<'_>;
}

/// System-timer implementation for production workers.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemWorkflowWorkerSleeper;

impl WorkflowWorkerSleeper for SystemWorkflowWorkerSleeper {
    fn sleep(&self, duration: Duration) -> WorkflowWorkerSleepFuture<'_> {
        Box::pin(Delay::new(duration))
    }
}

/// How a registered definition handles non-infrastructure execution failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowFailurePolicy {
    /// Persist a permanent failed task.
    Fail,
    /// Return the task to the queue after a delay.
    RetryAfter(Duration),
}

/// Executable workflow definition and its root authority policy.
#[derive(Clone)]
pub struct WorkflowDefinition {
    pub(super) workflow: Arc<Workflow>,
    pub(super) budget: Budget,
    capabilities: CapabilitySet,
    journal: Option<Arc<dyn Journal>>,
    pub(super) resume_policy: WorkflowResumePolicy,
    pub(super) failure_policy: WorkflowFailurePolicy,
}

impl WorkflowDefinition {
    /// Registers a workflow with explicit root budget and authority.
    pub fn new(workflow: Arc<Workflow>, budget: Budget, capabilities: CapabilitySet) -> Self {
        Self {
            workflow,
            budget,
            capabilities,
            journal: None,
            resume_policy: WorkflowResumePolicy::RejectAmbiguous,
            failure_policy: WorkflowFailurePolicy::Fail,
        }
    }

    /// Attaches a durable event journal to worker-created Runs.
    #[must_use]
    pub fn with_journal(mut self, journal: Arc<dyn Journal>) -> Self {
        self.journal = Some(journal);
        self
    }

    /// Selects explicit interrupted-step recovery authority.
    #[must_use]
    pub const fn with_resume_policy(mut self, policy: WorkflowResumePolicy) -> Self {
        self.resume_policy = policy;
        self
    }

    /// Selects how ordinary workflow execution failures affect the task.
    #[must_use]
    pub const fn with_failure_policy(mut self, policy: WorkflowFailurePolicy) -> Self {
        self.failure_policy = policy;
        self
    }

    /// Returns the immutable workflow.
    pub const fn workflow(&self) -> &Arc<Workflow> {
        &self.workflow
    }

    pub(super) fn run_context(&self, usage: Usage) -> Result<RunContext, BudgetExceeded> {
        let mut run = RunContext::root(
            BudgetTracker::restore(self.budget, usage)?,
            self.capabilities.clone(),
        );
        if let Some(journal) = &self.journal {
            run = run.with_journal(journal.clone());
        }
        Ok(run)
    }
}

impl std::fmt::Debug for WorkflowDefinition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowDefinition")
            .field("workflow", &self.workflow)
            .field("budget", &self.budget)
            .field("capabilities", &self.capabilities)
            .field("resume_policy", &self.resume_policy)
            .field("failure_policy", &self.failure_policy)
            .finish_non_exhaustive()
    }
}

/// Immutable workflow-definition registry used by workers.
#[derive(Clone, Debug, Default)]
pub struct WorkflowRegistry {
    definitions: BTreeMap<(String, u32), Arc<WorkflowDefinition>>,
}

impl WorkflowRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one exact workflow name and version.
    ///
    /// # Errors
    ///
    /// Rejects duplicate definition identities.
    pub fn register(&mut self, definition: WorkflowDefinition) -> Result<(), WorkflowWorkerError> {
        let key = (
            definition.workflow.name().to_owned(),
            definition.workflow.version(),
        );
        if self.definitions.contains_key(&key) {
            return Err(WorkflowWorkerError::DuplicateDefinition {
                workflow: key.0,
                version: key.1,
            });
        }
        self.definitions.insert(key, Arc::new(definition));
        Ok(())
    }

    /// Finds one exact workflow name and version.
    pub fn get(&self, task: &WorkflowTask) -> Option<&Arc<WorkflowDefinition>> {
        self.definitions
            .get(&(task.workflow.clone(), task.workflow_version))
    }

    /// Returns the number of registered definitions.
    pub fn len(&self) -> usize {
        self.definitions.len()
    }

    /// Returns whether no definitions are registered.
    pub fn is_empty(&self) -> bool {
        self.definitions.is_empty()
    }
}

/// Result of one worker claim-and-execute cycle.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum WorkflowWorkerOutcome {
    /// No eligible task was available.
    Idle,
    /// A task completed and its terminal checkpoint is durable.
    Completed {
        /// Stable workflow identity.
        checkpoint_id: CheckpointId,
        /// Canonical workflow outcome.
        outcome: WorkflowOutcome,
    },
    /// Execution failed and the task was returned to the queue.
    Retried {
        /// Stable workflow identity.
        checkpoint_id: CheckpointId,
    },
    /// Execution durably released its lease while waiting for a wake condition.
    Suspended {
        /// Stable workflow identity.
        checkpoint_id: CheckpointId,
    },
    /// Execution failed and the task became terminal.
    Failed {
        /// Stable workflow identity.
        checkpoint_id: CheckpointId,
    },
    /// The claimed workflow definition is not installed on this worker.
    DefinitionUnavailable {
        /// Stable workflow identity.
        checkpoint_id: CheckpointId,
    },
    /// Heartbeat or fenced persistence proved ownership was lost or uncertain.
    LeaseLost {
        /// Stable workflow identity.
        checkpoint_id: CheckpointId,
    },
}

/// Validated policy for continuous worker polling and bounded concurrency.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkflowSupervisorConfig {
    pub(super) max_concurrency: usize,
    pub(super) initial_backoff: Duration,
    pub(super) max_backoff: Duration,
}

impl WorkflowSupervisorConfig {
    /// Creates a supervisor configuration with a 10 ms to 5 second idle backoff.
    ///
    /// # Errors
    ///
    /// Rejects zero concurrency.
    pub fn new(max_concurrency: usize) -> Result<Self, WorkflowWorkerError> {
        if max_concurrency == 0 {
            return Err(WorkflowWorkerError::InvalidConfig(
                "supervisor concurrency must be positive".into(),
            ));
        }
        Ok(Self {
            max_concurrency,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(5),
        })
    }

    /// Sets the exponential backoff used after idle polls and infrastructure errors.
    ///
    /// # Errors
    ///
    /// Rejects zero delays or a maximum shorter than the initial delay.
    pub fn with_backoff(
        mut self,
        initial: Duration,
        maximum: Duration,
    ) -> Result<Self, WorkflowWorkerError> {
        if initial.is_zero() || maximum < initial {
            return Err(WorkflowWorkerError::InvalidConfig(
                "supervisor backoff must be positive and maximum must not be shorter than initial"
                    .into(),
            ));
        }
        self.initial_backoff = initial;
        self.max_backoff = maximum;
        Ok(self)
    }

    /// Returns the maximum number of simultaneous claim-and-execute cycles.
    pub const fn max_concurrency(self) -> usize {
        self.max_concurrency
    }
}

/// Lock-free cumulative supervisor metrics suitable for polling by an exporter.
#[derive(Clone, Debug, Default)]
pub struct WorkflowSupervisorMetrics {
    state: Arc<WorkflowSupervisorMetricState>,
}

#[derive(Debug, Default)]
struct WorkflowSupervisorMetricState {
    cycles_started: AtomicU64,
    active_cycles: AtomicUsize,
    peak_active_cycles: AtomicUsize,
    idle_polls: AtomicU64,
    completed: AtomicU64,
    retried: AtomicU64,
    suspended: AtomicU64,
    failed: AtomicU64,
    definitions_unavailable: AtomicU64,
    leases_lost: AtomicU64,
    infrastructure_errors: AtomicU64,
    backoffs: AtomicU64,
}

/// Point-in-time, low-cardinality supervisor metric values.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WorkflowSupervisorMetricSnapshot {
    /// Claim-and-execute cycles started, including idle polls.
    pub cycles_started: u64,
    /// Cycles currently polling, sleeping, or executing.
    pub active_cycles: usize,
    /// Highest observed active-cycle count.
    pub peak_active_cycles: usize,
    /// Polls that found no eligible task.
    pub idle_polls: u64,
    /// Workflows durably completed.
    pub completed: u64,
    /// Workflows durably returned to the queue.
    pub retried: u64,
    /// Workflows durably suspended without retaining a worker.
    pub suspended: u64,
    /// Workflows durably failed.
    pub failed: u64,
    /// Claims returned because their definition was unavailable.
    pub definitions_unavailable: u64,
    /// Executions that lost or could not prove lease ownership.
    pub leases_lost: u64,
    /// Worker cycles that returned an infrastructure error.
    pub infrastructure_errors: u64,
    /// Backoff delays scheduled.
    pub backoffs: u64,
}

impl WorkflowSupervisorMetrics {
    /// Reads a consistent-enough operational snapshot without blocking workers.
    pub fn snapshot(&self) -> WorkflowSupervisorMetricSnapshot {
        WorkflowSupervisorMetricSnapshot {
            cycles_started: self.state.cycles_started.load(Ordering::Relaxed),
            active_cycles: self.state.active_cycles.load(Ordering::Relaxed),
            peak_active_cycles: self.state.peak_active_cycles.load(Ordering::Relaxed),
            idle_polls: self.state.idle_polls.load(Ordering::Relaxed),
            completed: self.state.completed.load(Ordering::Relaxed),
            retried: self.state.retried.load(Ordering::Relaxed),
            suspended: self.state.suspended.load(Ordering::Relaxed),
            failed: self.state.failed.load(Ordering::Relaxed),
            definitions_unavailable: self.state.definitions_unavailable.load(Ordering::Relaxed),
            leases_lost: self.state.leases_lost.load(Ordering::Relaxed),
            infrastructure_errors: self.state.infrastructure_errors.load(Ordering::Relaxed),
            backoffs: self.state.backoffs.load(Ordering::Relaxed),
        }
    }

    pub(super) fn cycle_started(&self) {
        self.state.cycles_started.fetch_add(1, Ordering::Relaxed);
        let active = self.state.active_cycles.fetch_add(1, Ordering::Relaxed) + 1;
        self.state
            .peak_active_cycles
            .fetch_max(active, Ordering::Relaxed);
    }

    pub(super) fn cycle_stopped(&self) {
        self.state.active_cycles.fetch_sub(1, Ordering::Relaxed);
    }

    pub(super) fn record_result(
        &self,
        result: &Result<WorkflowWorkerOutcome, WorkflowWorkerError>,
    ) {
        let counter = match result {
            Ok(WorkflowWorkerOutcome::Idle) => &self.state.idle_polls,
            Ok(WorkflowWorkerOutcome::Completed { .. }) => &self.state.completed,
            Ok(WorkflowWorkerOutcome::Retried { .. }) => &self.state.retried,
            Ok(WorkflowWorkerOutcome::Suspended { .. }) => &self.state.suspended,
            Ok(WorkflowWorkerOutcome::Failed { .. }) => &self.state.failed,
            Ok(WorkflowWorkerOutcome::DefinitionUnavailable { .. }) => {
                &self.state.definitions_unavailable
            }
            Ok(WorkflowWorkerOutcome::LeaseLost { .. }) => &self.state.leases_lost,
            Err(_) => &self.state.infrastructure_errors,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_backoff(&self) {
        self.state.backoffs.fetch_add(1, Ordering::Relaxed);
    }
}

/// Outcomes observed during one supervisor lifetime.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WorkflowSupervisorReport {
    /// Polls that found no eligible task.
    pub idle_polls: u64,
    /// Workflows durably completed.
    pub completed: u64,
    /// Workflows durably returned to the queue.
    pub retried: u64,
    /// Workflows durably suspended.
    pub suspended: u64,
    /// Workflows durably failed.
    pub failed: u64,
    /// Claims whose workflow definition was unavailable.
    pub definitions_unavailable: u64,
    /// Executions that lost lease ownership.
    pub leases_lost: u64,
    /// Non-terminal infrastructure failures.
    pub infrastructure_errors: u64,
}

/// Worker construction or execution failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum WorkflowWorkerError {
    /// The worker configuration violates a timing invariant.
    #[error("invalid workflow worker configuration: {0}")]
    InvalidConfig(String),
    /// A definition identity is already registered.
    #[error("workflow definition `{workflow}` version {version} is already registered")]
    DuplicateDefinition {
        /// Stable workflow name.
        workflow: String,
        /// Stable workflow version.
        version: u32,
    },
    /// Distributed task-control persistence failed.
    #[error("workflow task store failed: {0}")]
    Store(#[from] WorkflowStoreError),
    /// Checkpoint persistence or decoding failed.
    #[error("workflow checkpoint failed: {0}")]
    Checkpoint(#[from] CheckpointError),
    /// Persisted usage exceeds the registered workflow budget.
    #[error("workflow restored usage exceeds its registered budget: {0}")]
    Budget(#[from] BudgetExceeded),
}

/// One distributed workflow worker.
pub struct WorkflowWorker {
    pub(super) store: Arc<dyn WorkflowStore>,
    pub(super) registry: WorkflowRegistry,
    pub(super) worker: WorkerId,
    pub(super) lease_duration: LeaseDuration,
    pub(super) heartbeat_interval: Duration,
    pub(super) missing_definition_retry: Duration,
    pub(super) budget_denied_retry: Duration,
    pub(super) sleeper: Arc<dyn WorkflowWorkerSleeper>,
}

/// Continuous, bounded-concurrency host for a [`WorkflowWorker`].
pub struct WorkflowSupervisor {
    pub(super) worker: Arc<WorkflowWorker>,
    pub(super) config: WorkflowSupervisorConfig,
    pub(super) sleeper: Arc<dyn WorkflowWorkerSleeper>,
    pub(super) metrics: WorkflowSupervisorMetrics,
}
