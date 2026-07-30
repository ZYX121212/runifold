//! Exclusively leased supervision for durable tenant-budget telemetry projection.

use std::{
    fmt,
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::future::{Either, select};
use runifold_core::CancellationToken;
use runifold_workflow::{
    LeaseDuration, SystemWorkflowWorkerSleeper, WorkerId, WorkflowBudgetAuditCursor,
    WorkflowBudgetAuditLimit, WorkflowBudgetAuditProjectionId, WorkflowBudgetAuditProjectionLease,
    WorkflowStore, WorkflowStoreError, WorkflowTenantId, WorkflowWorkerSleeper,
};

use crate::workflow_budget::{
    OtelWorkflowBudgetMetrics, OtelWorkflowBudgetProjectionError,
    OtelWorkflowBudgetProjectionReport,
};

/// Validated timing and work bounds for a projection supervisor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OtelWorkflowBudgetSupervisorConfig {
    lease_duration: LeaseDuration,
    heartbeat_interval: Duration,
    idle_interval: Duration,
    error_backoff: Duration,
    max_batches_per_claim: NonZeroU32,
    page_limit: WorkflowBudgetAuditLimit,
}

impl OtelWorkflowBudgetSupervisorConfig {
    /// Creates safe lease timing with conservative polling defaults.
    ///
    /// # Errors
    ///
    /// Rejects a zero heartbeat or one not shorter than the lease.
    pub fn new(
        lease_duration: LeaseDuration,
        heartbeat_interval: Duration,
    ) -> Result<Self, OtelWorkflowBudgetProjectionError> {
        let heartbeat_ms = u64::try_from(heartbeat_interval.as_millis()).map_err(|_| {
            OtelWorkflowBudgetProjectionError::InvalidConfig(
                "heartbeat interval exceeds supported milliseconds",
            )
        })?;
        if heartbeat_ms == 0 || heartbeat_ms >= lease_duration.as_millis() {
            return Err(OtelWorkflowBudgetProjectionError::InvalidConfig(
                "heartbeat interval must be positive and shorter than the lease",
            ));
        }
        Ok(Self {
            lease_duration,
            heartbeat_interval,
            idle_interval: Duration::from_millis(250),
            error_backoff: Duration::from_secs(1),
            max_batches_per_claim: NonZeroU32::new(10).unwrap_or(NonZeroU32::MIN),
            page_limit: WorkflowBudgetAuditLimit::default(),
        })
    }

    /// Sets the delay after an idle claim attempt.
    ///
    /// # Errors
    ///
    /// Rejects zero to prevent a hot polling loop.
    pub fn with_idle_interval(
        mut self,
        interval: Duration,
    ) -> Result<Self, OtelWorkflowBudgetProjectionError> {
        if interval.is_zero() {
            return Err(OtelWorkflowBudgetProjectionError::InvalidConfig(
                "idle interval must be positive",
            ));
        }
        self.idle_interval = interval;
        Ok(self)
    }

    /// Sets the delay after a store or lease failure.
    ///
    /// # Errors
    ///
    /// Rejects zero to prevent an infrastructure-error hot loop.
    pub fn with_error_backoff(
        mut self,
        backoff: Duration,
    ) -> Result<Self, OtelWorkflowBudgetProjectionError> {
        if backoff.is_zero() {
            return Err(OtelWorkflowBudgetProjectionError::InvalidConfig(
                "error backoff must be positive",
            ));
        }
        self.error_backoff = backoff;
        Ok(self)
    }

    /// Sets bounded page and per-claim batch limits.
    #[must_use]
    pub const fn with_work_limits(
        mut self,
        page_limit: WorkflowBudgetAuditLimit,
        max_batches_per_claim: NonZeroU32,
    ) -> Self {
        self.page_limit = page_limit;
        self.max_batches_per_claim = max_batches_per_claim;
        self
    }
}

/// Cumulative result of one projection supervisor run.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OtelWorkflowBudgetSupervisorReport {
    /// Successfully acquired projection leases.
    pub claims: u64,
    /// Claim attempts that found another active owner.
    pub idle_polls: u64,
    /// Audit facts recorded into `OTel`.
    pub events_projected: u64,
    /// Non-empty pages durably acknowledged.
    pub batches_projected: u64,
    /// Lease-loss or fencing failures.
    pub leases_lost: u64,
    /// Other store failures observed and retried.
    pub infrastructure_errors: u64,
}

/// Outcome of one bounded fenced projection cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OtelWorkflowBudgetSupervisorCycleOutcome {
    /// Another process currently owns the named projection.
    Contended,
    /// This process claimed, projected, and released the projection.
    Projected(OtelWorkflowBudgetProjectionReport),
}

/// Lock-free cumulative health metrics shared with a running supervisor.
#[derive(Clone, Debug, Default)]
pub struct OtelWorkflowBudgetSupervisorMetrics {
    state: Arc<OtelWorkflowBudgetSupervisorMetricState>,
}

#[derive(Debug, Default)]
struct OtelWorkflowBudgetSupervisorMetricState {
    lease_active: AtomicBool,
    caught_up: AtomicBool,
    claims: AtomicU64,
    idle_polls: AtomicU64,
    events_projected: AtomicU64,
    batches_projected: AtomicU64,
    leases_lost: AtomicU64,
    infrastructure_errors: AtomicU64,
    last_cursor: AtomicU64,
}

/// Point-in-time operational state of a budget projection supervisor.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OtelWorkflowBudgetSupervisorMetricSnapshot {
    /// Whether this process currently believes it owns the projection lease.
    pub lease_active: bool,
    /// Whether the latest successful projection cycle reached the stream tail.
    pub caught_up: bool,
    /// Successful lease claims.
    pub claims: u64,
    /// Claim attempts blocked by another active owner.
    pub idle_polls: u64,
    /// Audit facts recorded into `OTel`.
    pub events_projected: u64,
    /// Non-empty pages durably acknowledged.
    pub batches_projected: u64,
    /// Lease-loss or fencing failures.
    pub leases_lost: u64,
    /// Other store failures.
    pub infrastructure_errors: u64,
    /// Last durably acknowledged cursor, absent before the first page.
    pub last_cursor: Option<WorkflowBudgetAuditCursor>,
}

impl OtelWorkflowBudgetSupervisorMetrics {
    /// Reads one internally consistent-enough lock-free health snapshot.
    pub fn snapshot(&self) -> OtelWorkflowBudgetSupervisorMetricSnapshot {
        let cursor = self.state.last_cursor.load(Ordering::Relaxed);
        OtelWorkflowBudgetSupervisorMetricSnapshot {
            lease_active: self.state.lease_active.load(Ordering::Relaxed),
            caught_up: self.state.caught_up.load(Ordering::Relaxed),
            claims: self.state.claims.load(Ordering::Relaxed),
            idle_polls: self.state.idle_polls.load(Ordering::Relaxed),
            events_projected: self.state.events_projected.load(Ordering::Relaxed),
            batches_projected: self.state.batches_projected.load(Ordering::Relaxed),
            leases_lost: self.state.leases_lost.load(Ordering::Relaxed),
            infrastructure_errors: self.state.infrastructure_errors.load(Ordering::Relaxed),
            last_cursor: (cursor != 0).then(|| WorkflowBudgetAuditCursor::new(cursor)),
        }
    }

    fn claimed(&self) {
        self.state.lease_active.store(true, Ordering::Relaxed);
        self.state.claims.fetch_add(1, Ordering::Relaxed);
    }

    fn released(&self) {
        self.state.lease_active.store(false, Ordering::Relaxed);
    }

    fn idle(&self) {
        self.state.idle_polls.fetch_add(1, Ordering::Relaxed);
    }

    fn projected(&self, report: OtelWorkflowBudgetProjectionReport) {
        self.state
            .events_projected
            .fetch_add(report.events_projected, Ordering::Relaxed);
        self.state
            .batches_projected
            .fetch_add(u64::from(report.batches_projected), Ordering::Relaxed);
        self.state
            .caught_up
            .store(report.caught_up, Ordering::Relaxed);
        if let Some(cursor) = report.cursor {
            self.state
                .last_cursor
                .store(cursor.sequence(), Ordering::Relaxed);
        }
    }

    fn lease_lost(&self) {
        self.state.lease_active.store(false, Ordering::Relaxed);
        self.state.leases_lost.fetch_add(1, Ordering::Relaxed);
    }

    fn infrastructure_error(&self) {
        self.state
            .infrastructure_errors
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Continuously projects one tenant stream under an exclusive fenced lease.
pub struct OtelWorkflowBudgetSupervisor<S> {
    store: Arc<S>,
    tenant_id: WorkflowTenantId,
    projection_id: WorkflowBudgetAuditProjectionId,
    owner: WorkerId,
    instruments: OtelWorkflowBudgetMetrics,
    metrics: OtelWorkflowBudgetSupervisorMetrics,
    config: OtelWorkflowBudgetSupervisorConfig,
    sleeper: Arc<dyn WorkflowWorkerSleeper>,
}

impl<S> OtelWorkflowBudgetSupervisor<S>
where
    S: WorkflowStore + 'static,
{
    /// Creates a supervisor with system timers.
    pub fn new(
        store: Arc<S>,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
        owner: WorkerId,
        instruments: OtelWorkflowBudgetMetrics,
        config: OtelWorkflowBudgetSupervisorConfig,
    ) -> Self {
        Self {
            store,
            tenant_id,
            projection_id,
            owner,
            instruments,
            metrics: OtelWorkflowBudgetSupervisorMetrics::default(),
            config,
            sleeper: Arc::new(SystemWorkflowWorkerSleeper),
        }
    }

    /// Overrides sleeping for deterministic runtimes and tests.
    #[must_use]
    pub fn with_sleeper(mut self, sleeper: Arc<dyn WorkflowWorkerSleeper>) -> Self {
        self.sleeper = sleeper;
        self
    }

    /// Uses the supplied lock-free cumulative health metrics.
    #[must_use]
    pub fn with_metrics(mut self, metrics: OtelWorkflowBudgetSupervisorMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    /// Returns the cumulative health metrics used by this supervisor.
    pub const fn metrics(&self) -> &OtelWorkflowBudgetSupervisorMetrics {
        &self.metrics
    }

    /// Claims, projects bounded work, and releases exactly one cycle.
    ///
    /// # Errors
    ///
    /// Returns a typed store failure after best-effort lease release.
    pub async fn run_once(
        &self,
    ) -> Result<OtelWorkflowBudgetSupervisorCycleOutcome, OtelWorkflowBudgetProjectionError> {
        let lease = match self.claim().await {
            Ok(Some(lease)) => {
                self.instruments.record_projection_operation("claimed");
                self.metrics.claimed();
                lease
            }
            Ok(None) => {
                self.instruments.record_projection_operation("contended");
                self.metrics.idle();
                return Ok(OtelWorkflowBudgetSupervisorCycleOutcome::Contended);
            }
            Err(error) => {
                self.instruments.record_projection_operation("store_error");
                self.metrics.infrastructure_error();
                return Err(error.into());
            }
        };
        match self.run_claimed(lease.clone()).await {
            Ok((batch, release_lease)) => {
                self.instruments.record_projection_operation("completed");
                self.metrics.projected(batch);
                if let Err(error) = self
                    .store
                    .release_tenant_budget_audit_projection(release_lease)
                    .await
                {
                    self.record_cycle_error(&error);
                    return Err(error.into());
                }
                self.metrics.released();
                Ok(OtelWorkflowBudgetSupervisorCycleOutcome::Projected(batch))
            }
            Err((error, release_lease)) => {
                let _ = self
                    .store
                    .release_tenant_budget_audit_projection(release_lease)
                    .await;
                self.record_cycle_error(&error);
                Err(error.into())
            }
        }
    }

    /// Runs until cancellation, releasing any projection lease before return.
    pub async fn run(&self, shutdown: &CancellationToken) -> OtelWorkflowBudgetSupervisorReport {
        let mut report = OtelWorkflowBudgetSupervisorReport::default();
        while !shutdown.is_cancelled() {
            match self.claim().await {
                Ok(Some(lease)) => {
                    self.instruments.record_projection_operation("claimed");
                    self.metrics.claimed();
                    report.claims = report.claims.saturating_add(1);
                    let failures_before = report
                        .leases_lost
                        .saturating_add(report.infrastructure_errors);
                    if self
                        .run_claimed_or_shutdown(lease, shutdown, &mut report)
                        .await
                    {
                        break;
                    }
                    let failures_after = report
                        .leases_lost
                        .saturating_add(report.infrastructure_errors);
                    let delay = if failures_after > failures_before {
                        self.config.error_backoff
                    } else {
                        self.config.idle_interval
                    };
                    if wait_or_shutdown(Arc::clone(&self.sleeper), delay, shutdown).await {
                        break;
                    }
                }
                Ok(None) => {
                    self.instruments.record_projection_operation("contended");
                    self.metrics.idle();
                    report.idle_polls = report.idle_polls.saturating_add(1);
                    if wait_or_shutdown(
                        Arc::clone(&self.sleeper),
                        self.config.idle_interval,
                        shutdown,
                    )
                    .await
                    {
                        break;
                    }
                }
                Err(_) => {
                    self.instruments.record_projection_operation("store_error");
                    self.metrics.infrastructure_error();
                    report.infrastructure_errors = report.infrastructure_errors.saturating_add(1);
                    if wait_or_shutdown(
                        Arc::clone(&self.sleeper),
                        self.config.error_backoff,
                        shutdown,
                    )
                    .await
                    {
                        break;
                    }
                }
            }
        }
        report
    }

    async fn claim(
        &self,
    ) -> Result<Option<WorkflowBudgetAuditProjectionLease>, WorkflowStoreError> {
        self.store
            .claim_tenant_budget_audit_projection(
                self.tenant_id.clone(),
                self.projection_id.clone(),
                self.owner.clone(),
                self.config.lease_duration,
            )
            .await
    }

    async fn run_claimed_or_shutdown(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
        shutdown: &CancellationToken,
        report: &mut OtelWorkflowBudgetSupervisorReport,
    ) -> bool {
        let claimed = self.run_claimed(lease.clone());
        match select(Box::pin(shutdown.cancelled()), Box::pin(claimed)).await {
            Either::Left(_) => {
                match self
                    .store
                    .release_tenant_budget_audit_projection(lease)
                    .await
                {
                    Ok(()) => self.metrics.released(),
                    Err(error) => self.record_error(report, &error),
                }
                true
            }
            Either::Right((result, _)) => {
                let (release_lease, already_failed) = match result {
                    Ok((batch, lease)) => {
                        self.instruments.record_projection_operation("completed");
                        self.metrics.projected(batch);
                        report.events_projected = report
                            .events_projected
                            .saturating_add(batch.events_projected);
                        report.batches_projected = report
                            .batches_projected
                            .saturating_add(u64::from(batch.batches_projected));
                        (lease, false)
                    }
                    Err((error, lease)) => {
                        self.record_error(report, &error);
                        (lease, true)
                    }
                };
                let release = self
                    .store
                    .release_tenant_budget_audit_projection(release_lease)
                    .await;
                match release {
                    Ok(()) => self.metrics.released(),
                    Err(error) if !already_failed => {
                        self.record_error(report, &error);
                    }
                    Err(_) => {}
                }
                false
            }
        }
    }

    async fn run_claimed(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
    ) -> Result<
        (
            OtelWorkflowBudgetProjectionReport,
            WorkflowBudgetAuditProjectionLease,
        ),
        (WorkflowStoreError, WorkflowBudgetAuditProjectionLease),
    > {
        let projection = project_claimed_available(
            Arc::clone(&self.store),
            self.instruments.clone(),
            lease.clone(),
            self.config.page_limit,
            self.config.max_batches_per_claim,
        );
        let heartbeat = heartbeat_projection_until_failure(
            Arc::clone(&self.store),
            lease.clone(),
            self.config.lease_duration,
            self.config.heartbeat_interval,
            Arc::clone(&self.sleeper),
        );
        match select(Box::pin(projection), Box::pin(heartbeat)).await {
            Either::Left((result, _)) => result.map_err(|error| (error, lease)),
            Either::Right((error, _)) => Err((error, lease)),
        }
    }

    fn record_error(
        &self,
        report: &mut OtelWorkflowBudgetSupervisorReport,
        error: &WorkflowStoreError,
    ) {
        if error.kind == runifold_workflow::WorkflowStoreErrorKind::LeaseLost {
            self.instruments.record_projection_operation("lease_lost");
            self.metrics.lease_lost();
            report.leases_lost = report.leases_lost.saturating_add(1);
        } else {
            self.instruments.record_projection_operation("store_error");
            self.metrics.infrastructure_error();
            report.infrastructure_errors = report.infrastructure_errors.saturating_add(1);
        }
    }

    fn record_cycle_error(&self, error: &WorkflowStoreError) {
        if error.kind == runifold_workflow::WorkflowStoreErrorKind::LeaseLost {
            self.instruments.record_projection_operation("lease_lost");
            self.metrics.lease_lost();
        } else {
            self.instruments.record_projection_operation("store_error");
            self.metrics.infrastructure_error();
        }
    }
}

impl<S> fmt::Debug for OtelWorkflowBudgetSupervisor<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OtelWorkflowBudgetSupervisor")
            .field("tenant_id", &self.tenant_id)
            .field("projection_id", &self.projection_id)
            .field("owner", &self.owner)
            .field("config", &self.config)
            .field("metrics", &self.metrics.snapshot())
            .finish_non_exhaustive()
    }
}

async fn project_claimed_available<S>(
    store: Arc<S>,
    metrics: OtelWorkflowBudgetMetrics,
    mut lease: WorkflowBudgetAuditProjectionLease,
    page_limit: WorkflowBudgetAuditLimit,
    max_batches: NonZeroU32,
) -> Result<
    (
        OtelWorkflowBudgetProjectionReport,
        WorkflowBudgetAuditProjectionLease,
    ),
    WorkflowStoreError,
>
where
    S: WorkflowStore,
{
    let mut report = OtelWorkflowBudgetProjectionReport {
        cursor: Some(lease.cursor),
        ..OtelWorkflowBudgetProjectionReport::default()
    };
    for _ in 0..max_batches.get() {
        let events = store
            .list_tenant_budget_audit(lease.tenant_id.clone(), Some(lease.cursor), page_limit)
            .await?;
        let Some(last) = events.last() else {
            report.caught_up = true;
            return Ok((report, lease));
        };
        let next = last.cursor;
        for event in &events {
            metrics.observe(event);
        }
        lease = store
            .advance_tenant_budget_audit_projection_lease(lease, next)
            .await?;
        report.events_projected = report
            .events_projected
            .saturating_add(u64::try_from(events.len()).unwrap_or(u64::MAX));
        report.batches_projected = report.batches_projected.saturating_add(1);
        report.cursor = Some(next);
        report.caught_up = events.len() < usize::try_from(page_limit.get()).unwrap_or(usize::MAX);
        if report.caught_up {
            return Ok((report, lease));
        }
    }
    Ok((report, lease))
}

async fn heartbeat_projection_until_failure<S>(
    store: Arc<S>,
    mut lease: WorkflowBudgetAuditProjectionLease,
    extension: LeaseDuration,
    interval: Duration,
    sleeper: Arc<dyn WorkflowWorkerSleeper>,
) -> WorkflowStoreError
where
    S: WorkflowStore,
{
    loop {
        sleeper.sleep(interval).await;
        match store
            .heartbeat_tenant_budget_audit_projection(lease, extension)
            .await
        {
            Ok(renewed) => lease = renewed,
            Err(error) => return error,
        }
    }
}

async fn wait_or_shutdown(
    sleeper: Arc<dyn WorkflowWorkerSleeper>,
    duration: Duration,
    shutdown: &CancellationToken,
) -> bool {
    matches!(
        select(
            Box::pin(shutdown.cancelled()),
            Box::pin(sleeper.sleep(duration))
        )
        .await,
        Either::Left(_)
    )
}
