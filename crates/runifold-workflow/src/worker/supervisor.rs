use super::{
    Arc, CancellationToken, Duration, Either, Future, FuturesUnordered, Pin, StreamExt,
    SystemWorkflowWorkerSleeper, WorkflowSupervisor, WorkflowSupervisorConfig,
    WorkflowSupervisorMetrics, WorkflowSupervisorReport, WorkflowWorker, WorkflowWorkerError,
    WorkflowWorkerOutcome, WorkflowWorkerSleeper, select,
};

impl WorkflowSupervisor {
    /// Creates a continuous supervisor around one shareable worker.
    pub fn new(worker: Arc<WorkflowWorker>, config: WorkflowSupervisorConfig) -> Self {
        Self {
            worker,
            config,
            sleeper: Arc::new(SystemWorkflowWorkerSleeper),
            metrics: WorkflowSupervisorMetrics::default(),
        }
    }

    /// Overrides supervisor sleeping for deterministic runtimes and tests.
    #[must_use]
    pub fn with_sleeper(mut self, sleeper: Arc<dyn WorkflowWorkerSleeper>) -> Self {
        self.sleeper = sleeper;
        self
    }

    /// Uses the supplied cumulative metric set.
    #[must_use]
    pub fn with_metrics(mut self, metrics: WorkflowSupervisorMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    /// Returns the cumulative metric set used by this supervisor.
    pub const fn metrics(&self) -> &WorkflowSupervisorMetrics {
        &self.metrics
    }

    /// Continuously polls and executes work until shutdown, then drains started cycles.
    ///
    /// Infrastructure errors are counted and retried with backoff. Once shutdown is
    /// observed, no replacement cycles are scheduled. Already-started claim or
    /// execution operations are awaited so their lease protocol can finish safely.
    pub async fn run(&self, shutdown: &CancellationToken) -> WorkflowSupervisorReport {
        let mut cycles = FuturesUnordered::new();
        for _ in 0..self.config.max_concurrency {
            self.schedule(&mut cycles, Duration::ZERO, shutdown.clone());
        }

        let mut report = WorkflowSupervisorReport::default();
        let mut next_backoff = self.config.initial_backoff;
        while let Either::Right((Some(result), _)) =
            select(Box::pin(shutdown.cancelled()), Box::pin(cycles.next())).await
        {
            self.metrics.cycle_stopped();
            let delay = self.observe_cycle(&mut report, &result, &mut next_backoff);
            if !matches!(result, WorkflowSupervisorCycleResult::Stopped) {
                self.schedule(&mut cycles, delay, shutdown.clone());
            }
        }

        while let Some(result) = cycles.next().await {
            self.metrics.cycle_stopped();
            self.observe_cycle(&mut report, &result, &mut next_backoff);
        }
        report
    }

    fn schedule(
        &self,
        cycles: &mut FuturesUnordered<WorkflowSupervisorCycleFuture>,
        delay: Duration,
        shutdown: CancellationToken,
    ) {
        if !delay.is_zero() {
            self.metrics.record_backoff();
        }
        self.metrics.cycle_started();
        cycles.push(Box::pin(run_supervisor_cycle(
            self.worker.clone(),
            self.sleeper.clone(),
            delay,
            shutdown,
        )));
    }

    fn observe_cycle(
        &self,
        report: &mut WorkflowSupervisorReport,
        result: &WorkflowSupervisorCycleResult,
        next_backoff: &mut Duration,
    ) -> Duration {
        let WorkflowSupervisorCycleResult::Finished(result) = result else {
            return Duration::ZERO;
        };
        self.metrics.record_result(result);
        report.record(result);
        if matches!(result, Ok(WorkflowWorkerOutcome::Idle) | Err(_)) {
            let delay = *next_backoff;
            *next_backoff = next_backoff.saturating_mul(2).min(self.config.max_backoff);
            delay
        } else {
            *next_backoff = self.config.initial_backoff;
            Duration::ZERO
        }
    }
}

impl std::fmt::Debug for WorkflowSupervisor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowSupervisor")
            .field("worker", &self.worker)
            .field("config", &self.config)
            .field("metrics", &self.metrics.snapshot())
            .finish_non_exhaustive()
    }
}

impl WorkflowSupervisorReport {
    fn record(&mut self, result: &Result<WorkflowWorkerOutcome, WorkflowWorkerError>) {
        match result {
            Ok(WorkflowWorkerOutcome::Idle) => self.idle_polls += 1,
            Ok(WorkflowWorkerOutcome::Completed { .. }) => self.completed += 1,
            Ok(WorkflowWorkerOutcome::Retried { .. }) => self.retried += 1,
            Ok(WorkflowWorkerOutcome::Suspended { .. }) => self.suspended += 1,
            Ok(WorkflowWorkerOutcome::Failed { .. }) => self.failed += 1,
            Ok(WorkflowWorkerOutcome::DefinitionUnavailable { .. }) => {
                self.definitions_unavailable += 1;
            }
            Ok(WorkflowWorkerOutcome::LeaseLost { .. }) => self.leases_lost += 1,
            Err(_) => self.infrastructure_errors += 1,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
type WorkflowSupervisorCycleFuture =
    Pin<Box<dyn Future<Output = WorkflowSupervisorCycleResult> + Send>>;

#[cfg(target_arch = "wasm32")]
type WorkflowSupervisorCycleFuture = Pin<Box<dyn Future<Output = WorkflowSupervisorCycleResult>>>;

enum WorkflowSupervisorCycleResult {
    Finished(Result<WorkflowWorkerOutcome, WorkflowWorkerError>),
    Stopped,
}

async fn run_supervisor_cycle(
    worker: Arc<WorkflowWorker>,
    sleeper: Arc<dyn WorkflowWorkerSleeper>,
    delay: Duration,
    shutdown: CancellationToken,
) -> WorkflowSupervisorCycleResult {
    if !delay.is_zero() {
        match select(
            Box::pin(shutdown.cancelled()),
            Box::pin(sleeper.sleep(delay)),
        )
        .await
        {
            Either::Left(_) => return WorkflowSupervisorCycleResult::Stopped,
            Either::Right(_) => {}
        }
    }
    if shutdown.is_cancelled() {
        return WorkflowSupervisorCycleResult::Stopped;
    }
    WorkflowSupervisorCycleResult::Finished(worker.run_once().await)
}
