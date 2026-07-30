//! Dynamically sharded supervision for fenced terminal Task cleanup.

use std::{
    fmt,
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::{
    StreamExt,
    future::{Either, select},
    stream::FuturesUnordered,
};
use runifold_core::CancellationToken;

use crate::{
    LeaseDuration, SystemWorkflowWorkerSleeper, WorkerId, WorkflowStoreError,
    WorkflowStoreErrorKind, WorkflowTaskCleanupLease, WorkflowTaskCleanupLimit,
    WorkflowTaskRetention, WorkflowTaskRetentionStore, WorkflowTenantId, WorkflowTenantListLimit,
    WorkflowWorkerSleeper,
};

/// Stable assignment of tenants across cleanup processes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkflowTaskCleanupShard {
    index: u32,
    count: NonZeroU32,
}

impl WorkflowTaskCleanupShard {
    /// Creates one zero-based shard.
    ///
    /// # Errors
    ///
    /// Rejects an index outside the shard count.
    pub fn new(index: u32, count: NonZeroU32) -> Result<Self, WorkflowStoreError> {
        if index >= count.get() {
            return Err(invalid_config(
                "Task cleanup shard index must be smaller than shard count",
            ));
        }
        Ok(Self { index, count })
    }

    /// Zero-based shard index.
    pub const fn index(self) -> u32 {
        self.index
    }

    /// Total configured shards.
    pub const fn count(self) -> u32 {
        self.count.get()
    }

    /// Returns whether this shard owns a tenant under the stable hash.
    pub fn owns(self, tenant_id: &WorkflowTenantId) -> bool {
        stable_tenant_hash(tenant_id.as_str()) % u64::from(self.count.get())
            == u64::from(self.index)
    }
}

/// Validated bounds and timing for automatic terminal Task cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkflowTaskCleanupSupervisorConfig {
    shard: WorkflowTaskCleanupShard,
    retention: WorkflowTaskRetention,
    lease_duration: LeaseDuration,
    heartbeat_interval: Duration,
    cleanup_limit: WorkflowTaskCleanupLimit,
    max_batches_per_tenant: NonZeroU32,
    discovery_limit: WorkflowTenantListLimit,
    max_concurrency: NonZeroUsize,
    scan_interval: Duration,
    error_backoff: Duration,
}

impl WorkflowTaskCleanupSupervisorConfig {
    /// Creates conservative cleanup timing and work bounds.
    ///
    /// # Errors
    ///
    /// Rejects a zero heartbeat or one not shorter than the lease.
    pub fn new(
        shard: WorkflowTaskCleanupShard,
        retention: WorkflowTaskRetention,
        lease_duration: LeaseDuration,
        heartbeat_interval: Duration,
    ) -> Result<Self, WorkflowStoreError> {
        let heartbeat_ms = u64::try_from(heartbeat_interval.as_millis())
            .map_err(|_| invalid_config("Task cleanup heartbeat exceeds supported milliseconds"))?;
        if heartbeat_ms == 0 || heartbeat_ms >= lease_duration.as_millis() {
            return Err(invalid_config(
                "Task cleanup heartbeat must be positive and shorter than the lease",
            ));
        }
        Ok(Self {
            shard,
            retention,
            lease_duration,
            heartbeat_interval,
            cleanup_limit: WorkflowTaskCleanupLimit::new(100)?,
            max_batches_per_tenant: NonZeroU32::new(10).unwrap_or(NonZeroU32::MIN),
            discovery_limit: WorkflowTenantListLimit::default(),
            max_concurrency: NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
            scan_interval: Duration::from_secs(30),
            error_backoff: Duration::from_secs(1),
        })
    }

    /// Sets bounded discovery, concurrency, batch size, and work per claim.
    #[must_use]
    pub const fn with_work_limits(
        mut self,
        discovery_limit: WorkflowTenantListLimit,
        max_concurrency: NonZeroUsize,
        cleanup_limit: WorkflowTaskCleanupLimit,
        max_batches_per_tenant: NonZeroU32,
    ) -> Self {
        self.discovery_limit = discovery_limit;
        self.max_concurrency = max_concurrency;
        self.cleanup_limit = cleanup_limit;
        self.max_batches_per_tenant = max_batches_per_tenant;
        self
    }

    /// Sets successful-scan and error delays.
    ///
    /// # Errors
    ///
    /// Rejects zero durations to prevent hot loops.
    pub fn with_intervals(
        mut self,
        scan_interval: Duration,
        error_backoff: Duration,
    ) -> Result<Self, WorkflowStoreError> {
        if scan_interval.is_zero() || error_backoff.is_zero() {
            return Err(invalid_config(
                "Task cleanup scan interval and error backoff must be positive",
            ));
        }
        self.scan_interval = scan_interval;
        self.error_backoff = error_backoff;
        Ok(self)
    }
}

/// Cumulative work performed by cleanup scans.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WorkflowTaskCleanupSupervisorReport {
    /// Completed full tenant discovery scans.
    pub scans: u64,
    /// Terminal-Task tenants returned by the store.
    pub tenants_discovered: u64,
    /// Discovered tenants assigned to this shard.
    pub tenants_assigned: u64,
    /// Successfully acquired tenant cleanup leases.
    pub claims: u64,
    /// Claims blocked by another active owner.
    pub contended: u64,
    /// Non-empty cleanup batches committed.
    pub batches_cleaned: u64,
    /// Tasks atomically tombstoned and deleted.
    pub tasks_deleted: u64,
    /// Lease-loss and stale-fencing failures.
    pub leases_lost: u64,
    /// Store failures isolated from other tenants.
    pub infrastructure_errors: u64,
    /// Discovery failures that aborted a scan.
    pub discovery_errors: u64,
}

impl WorkflowTaskCleanupSupervisorReport {
    fn merge(&mut self, other: Self) {
        self.scans = self.scans.saturating_add(other.scans);
        self.tenants_discovered = self
            .tenants_discovered
            .saturating_add(other.tenants_discovered);
        self.tenants_assigned = self.tenants_assigned.saturating_add(other.tenants_assigned);
        self.claims = self.claims.saturating_add(other.claims);
        self.contended = self.contended.saturating_add(other.contended);
        self.batches_cleaned = self.batches_cleaned.saturating_add(other.batches_cleaned);
        self.tasks_deleted = self.tasks_deleted.saturating_add(other.tasks_deleted);
        self.leases_lost = self.leases_lost.saturating_add(other.leases_lost);
        self.infrastructure_errors = self
            .infrastructure_errors
            .saturating_add(other.infrastructure_errors);
        self.discovery_errors = self.discovery_errors.saturating_add(other.discovery_errors);
    }
}

/// Lock-free, low-cardinality health state for readiness and telemetry export.
#[derive(Clone, Debug, Default)]
pub struct WorkflowTaskCleanupSupervisorMetrics {
    state: Arc<WorkflowTaskCleanupSupervisorMetricState>,
}

#[derive(Debug, Default)]
struct WorkflowTaskCleanupSupervisorMetricState {
    scan_active: AtomicBool,
    scans: AtomicU64,
    claims: AtomicU64,
    contended: AtomicU64,
    tasks_deleted: AtomicU64,
    leases_lost: AtomicU64,
    infrastructure_errors: AtomicU64,
}

/// Point-in-time Task cleanup supervisor health snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WorkflowTaskCleanupSupervisorMetricSnapshot {
    /// Whether a discovery scan is currently running.
    pub scan_active: bool,
    /// Completed scans.
    pub scans: u64,
    /// Successful tenant claims.
    pub claims: u64,
    /// Contended tenant claims.
    pub contended: u64,
    /// Tasks atomically deleted.
    pub tasks_deleted: u64,
    /// Lease-loss failures.
    pub leases_lost: u64,
    /// Other store failures.
    pub infrastructure_errors: u64,
}

/// Optional low-cardinality observer for cleanup control-plane outcomes.
///
/// Implementations must not retain tenant identities or block cleanup.
pub trait WorkflowTaskCleanupObserver: Send + Sync {
    /// Observes one completed full scan.
    fn observe_scan(&self, report: WorkflowTaskCleanupSupervisorReport);

    /// Observes a discovery failure that aborted a scan.
    fn observe_discovery_error(&self);
}

#[derive(Debug, Default)]
struct NoopWorkflowTaskCleanupObserver;

impl WorkflowTaskCleanupObserver for NoopWorkflowTaskCleanupObserver {
    fn observe_scan(&self, _report: WorkflowTaskCleanupSupervisorReport) {}

    fn observe_discovery_error(&self) {}
}

impl WorkflowTaskCleanupSupervisorMetrics {
    /// Reads one lock-free operational snapshot.
    pub fn snapshot(&self) -> WorkflowTaskCleanupSupervisorMetricSnapshot {
        WorkflowTaskCleanupSupervisorMetricSnapshot {
            scan_active: self.state.scan_active.load(Ordering::Relaxed),
            scans: self.state.scans.load(Ordering::Relaxed),
            claims: self.state.claims.load(Ordering::Relaxed),
            contended: self.state.contended.load(Ordering::Relaxed),
            tasks_deleted: self.state.tasks_deleted.load(Ordering::Relaxed),
            leases_lost: self.state.leases_lost.load(Ordering::Relaxed),
            infrastructure_errors: self.state.infrastructure_errors.load(Ordering::Relaxed),
        }
    }

    fn begin_scan(&self) {
        self.state.scan_active.store(true, Ordering::Relaxed);
    }

    fn finish_scan(&self, report: WorkflowTaskCleanupSupervisorReport) {
        self.state.scan_active.store(false, Ordering::Relaxed);
        self.state.scans.fetch_add(report.scans, Ordering::Relaxed);
        self.state
            .claims
            .fetch_add(report.claims, Ordering::Relaxed);
        self.state
            .contended
            .fetch_add(report.contended, Ordering::Relaxed);
        self.state
            .tasks_deleted
            .fetch_add(report.tasks_deleted, Ordering::Relaxed);
        self.state
            .leases_lost
            .fetch_add(report.leases_lost, Ordering::Relaxed);
        self.state
            .infrastructure_errors
            .fetch_add(report.infrastructure_errors, Ordering::Relaxed);
    }

    fn discovery_error(&self) {
        self.state.scan_active.store(false, Ordering::Relaxed);
        self.state
            .infrastructure_errors
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// Discovers, shards, and cleans terminal Tasks with bounded concurrency.
pub struct WorkflowTaskCleanupSupervisor<S> {
    store: Arc<S>,
    owner: WorkerId,
    config: WorkflowTaskCleanupSupervisorConfig,
    metrics: WorkflowTaskCleanupSupervisorMetrics,
    observer: Arc<dyn WorkflowTaskCleanupObserver>,
    sleeper: Arc<dyn WorkflowWorkerSleeper>,
}

impl<S> WorkflowTaskCleanupSupervisor<S>
where
    S: WorkflowTaskRetentionStore + 'static,
{
    /// Creates a cleanup supervisor with system timers.
    pub fn new(
        store: Arc<S>,
        owner: WorkerId,
        config: WorkflowTaskCleanupSupervisorConfig,
    ) -> Self {
        Self {
            store,
            owner,
            config,
            metrics: WorkflowTaskCleanupSupervisorMetrics::default(),
            observer: Arc::new(NoopWorkflowTaskCleanupObserver),
            sleeper: Arc::new(SystemWorkflowWorkerSleeper),
        }
    }

    /// Overrides sleeping for deterministic runtimes and tests.
    #[must_use]
    pub fn with_sleeper(mut self, sleeper: Arc<dyn WorkflowWorkerSleeper>) -> Self {
        self.sleeper = sleeper;
        self
    }

    /// Uses shared lock-free health metrics.
    #[must_use]
    pub fn with_metrics(mut self, metrics: WorkflowTaskCleanupSupervisorMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    /// Attaches a non-blocking low-cardinality outcome observer.
    #[must_use]
    pub fn with_observer(mut self, observer: Arc<dyn WorkflowTaskCleanupObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Returns this supervisor's health metrics.
    pub const fn metrics(&self) -> &WorkflowTaskCleanupSupervisorMetrics {
        &self.metrics
    }

    /// Performs one stable paginated discovery and bounded cleanup scan.
    ///
    /// Per-tenant failures are isolated. Discovery failure aborts the scan.
    ///
    /// # Errors
    ///
    /// Returns a typed store error when tenant discovery fails.
    pub async fn scan_once(
        &self,
    ) -> Result<WorkflowTaskCleanupSupervisorReport, WorkflowStoreError> {
        self.metrics.begin_scan();
        let result = self.scan_pages().await;
        match result {
            Ok(report) => {
                self.metrics.finish_scan(report);
                self.observer.observe_scan(report);
                Ok(report)
            }
            Err(error) => {
                self.metrics.discovery_error();
                self.observer.observe_discovery_error();
                Err(error)
            }
        }
    }

    /// Continuously rescans until cancellation.
    ///
    /// A scan already in progress is drained before shutdown.
    pub async fn run(&self, shutdown: &CancellationToken) -> WorkflowTaskCleanupSupervisorReport {
        let mut report = WorkflowTaskCleanupSupervisorReport::default();
        while !shutdown.is_cancelled() {
            let delay = if let Ok(scan) = self.scan_once().await {
                report.merge(scan);
                self.config.scan_interval
            } else {
                report.discovery_errors = report.discovery_errors.saturating_add(1);
                self.config.error_backoff
            };
            if wait_or_shutdown(Arc::clone(&self.sleeper), delay, shutdown).await {
                break;
            }
        }
        report
    }

    async fn scan_pages(&self) -> Result<WorkflowTaskCleanupSupervisorReport, WorkflowStoreError> {
        let mut report = WorkflowTaskCleanupSupervisorReport::default();
        let mut after = None;
        loop {
            let tenants = self
                .store
                .list_task_cleanup_tenants(after, self.config.discovery_limit)
                .await?;
            let page_len = tenants.len();
            report.tenants_discovered = report
                .tenants_discovered
                .saturating_add(u64::try_from(page_len).unwrap_or(u64::MAX));
            let last = tenants.last().cloned();
            let assigned = tenants
                .into_iter()
                .filter(|tenant| self.config.shard.owns(tenant))
                .collect::<Vec<_>>();
            report.tenants_assigned = report
                .tenants_assigned
                .saturating_add(u64::try_from(assigned.len()).unwrap_or(u64::MAX));
            self.cleanup_assigned(assigned, &mut report).await;
            if page_len < usize::try_from(self.config.discovery_limit.get()).unwrap_or(usize::MAX) {
                break;
            }
            after = last;
        }
        report.scans = 1;
        Ok(report)
    }

    async fn cleanup_assigned(
        &self,
        tenants: Vec<WorkflowTenantId>,
        report: &mut WorkflowTaskCleanupSupervisorReport,
    ) {
        let mut tenants = tenants.into_iter();
        let mut active = FuturesUnordered::new();
        for _ in 0..self.config.max_concurrency.get() {
            let Some(tenant) = tenants.next() else {
                break;
            };
            active.push(self.cleanup_tenant(tenant));
        }
        while let Some(outcome) = active.next().await {
            match outcome {
                CleanupOutcome::Contended => {
                    report.contended = report.contended.saturating_add(1);
                }
                CleanupOutcome::Cleaned {
                    batches,
                    tasks_deleted,
                } => {
                    record_cleaned(report, batches, tasks_deleted);
                }
                CleanupOutcome::CleanedLeaseLost {
                    batches,
                    tasks_deleted,
                } => {
                    record_cleaned(report, batches, tasks_deleted);
                    report.leases_lost = report.leases_lost.saturating_add(1);
                }
                CleanupOutcome::CleanedInfrastructureError {
                    batches,
                    tasks_deleted,
                } => {
                    record_cleaned(report, batches, tasks_deleted);
                    report.infrastructure_errors = report.infrastructure_errors.saturating_add(1);
                }
                CleanupOutcome::LeaseLost => {
                    report.claims = report.claims.saturating_add(1);
                    report.leases_lost = report.leases_lost.saturating_add(1);
                }
                CleanupOutcome::InfrastructureError { claimed } => {
                    report.claims = report.claims.saturating_add(u64::from(claimed));
                    report.infrastructure_errors = report.infrastructure_errors.saturating_add(1);
                }
            }
            if let Some(tenant) = tenants.next() {
                active.push(self.cleanup_tenant(tenant));
            }
        }
    }

    async fn cleanup_tenant(&self, tenant: WorkflowTenantId) -> CleanupOutcome {
        let lease = match self
            .store
            .claim_task_cleanup(tenant, self.owner.clone(), self.config.lease_duration)
            .await
        {
            Ok(Some(lease)) => lease,
            Ok(None) => return CleanupOutcome::Contended,
            Err(_) => return CleanupOutcome::InfrastructureError { claimed: false },
        };
        let cleanup = cleanup_claimed(
            Arc::clone(&self.store),
            lease.clone(),
            self.config.retention,
            self.config.cleanup_limit,
            self.config.max_batches_per_tenant,
        );
        let heartbeat = heartbeat_until_failure(
            Arc::clone(&self.store),
            lease.clone(),
            self.config.lease_duration,
            self.config.heartbeat_interval,
            Arc::clone(&self.sleeper),
        );
        let outcome = match select(Box::pin(cleanup), Box::pin(heartbeat)).await {
            Either::Left((result, _)) => match result {
                Ok((batches, tasks_deleted)) => CleanupOutcome::Cleaned {
                    batches,
                    tasks_deleted,
                },
                Err(error) => classify_claimed_error(&error),
            },
            Either::Right((error, _)) => classify_claimed_error(&error),
        };
        let release = self.store.release_task_cleanup(lease).await;
        if let (
            CleanupOutcome::Cleaned {
                batches,
                tasks_deleted,
            },
            Err(error),
        ) = (outcome, release)
        {
            return if error.kind == WorkflowStoreErrorKind::LeaseLost {
                CleanupOutcome::CleanedLeaseLost {
                    batches,
                    tasks_deleted,
                }
            } else {
                CleanupOutcome::CleanedInfrastructureError {
                    batches,
                    tasks_deleted,
                }
            };
        }
        outcome
    }
}

impl<S> fmt::Debug for WorkflowTaskCleanupSupervisor<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowTaskCleanupSupervisor")
            .field("owner", &self.owner)
            .field("config", &self.config)
            .field("metrics", &self.metrics.snapshot())
            .field("observer", &"<task-cleanup-observer>")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CleanupOutcome {
    Contended,
    Cleaned { batches: u32, tasks_deleted: u64 },
    CleanedLeaseLost { batches: u32, tasks_deleted: u64 },
    CleanedInfrastructureError { batches: u32, tasks_deleted: u64 },
    LeaseLost,
    InfrastructureError { claimed: bool },
}

fn record_cleaned(
    report: &mut WorkflowTaskCleanupSupervisorReport,
    batches: u32,
    tasks_deleted: u64,
) {
    report.claims = report.claims.saturating_add(1);
    report.batches_cleaned = report.batches_cleaned.saturating_add(u64::from(batches));
    report.tasks_deleted = report.tasks_deleted.saturating_add(tasks_deleted);
}

async fn cleanup_claimed<S>(
    store: Arc<S>,
    lease: WorkflowTaskCleanupLease,
    retention: WorkflowTaskRetention,
    limit: WorkflowTaskCleanupLimit,
    max_batches: NonZeroU32,
) -> Result<(u32, u64), WorkflowStoreError>
where
    S: WorkflowTaskRetentionStore,
{
    let mut batches = 0_u32;
    let mut tasks_deleted = 0_u64;
    for _ in 0..max_batches.get() {
        let tombstones = store
            .compact_terminal_tasks(lease.clone(), retention, limit)
            .await?;
        let count = tombstones.len();
        if count == 0 {
            break;
        }
        batches = batches.saturating_add(1);
        tasks_deleted = tasks_deleted.saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
        if count < usize::try_from(limit.get()).unwrap_or(usize::MAX) {
            break;
        }
    }
    Ok((batches, tasks_deleted))
}

async fn heartbeat_until_failure<S>(
    store: Arc<S>,
    mut lease: WorkflowTaskCleanupLease,
    extension: LeaseDuration,
    interval: Duration,
    sleeper: Arc<dyn WorkflowWorkerSleeper>,
) -> WorkflowStoreError
where
    S: WorkflowTaskRetentionStore,
{
    loop {
        sleeper.sleep(interval).await;
        match store.heartbeat_task_cleanup(lease, extension).await {
            Ok(renewed) => lease = renewed,
            Err(error) => return error,
        }
    }
}

fn classify_claimed_error(error: &WorkflowStoreError) -> CleanupOutcome {
    if error.kind == WorkflowStoreErrorKind::LeaseLost {
        CleanupOutcome::LeaseLost
    } else {
        CleanupOutcome::InfrastructureError { claimed: true }
    }
}

fn invalid_config(message: &'static str) -> WorkflowStoreError {
    WorkflowStoreError::new(WorkflowStoreErrorKind::InvalidInput, message)
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

fn stable_tenant_hash(value: &str) -> u64 {
    value
        .as_bytes()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::*;

    #[test]
    fn shards_form_a_stable_complete_partition() {
        let tenant = WorkflowTenantId::parse("tenant-a").unwrap();
        let owners = (0..4)
            .filter(|index| {
                WorkflowTaskCleanupShard::new(*index, NonZeroU32::new(4).unwrap())
                    .unwrap()
                    .owns(&tenant)
            })
            .count();
        assert_eq!(owners, 1);
    }

    #[test]
    fn supervisor_rejects_unsafe_heartbeat_timing() {
        let shard = WorkflowTaskCleanupShard::new(0, NonZeroU32::new(1).unwrap()).unwrap();
        let retention = WorkflowTaskRetention::new(Duration::from_secs(1)).unwrap();
        let lease = LeaseDuration::new(Duration::from_secs(1)).unwrap();
        assert!(
            WorkflowTaskCleanupSupervisorConfig::new(
                shard,
                retention,
                lease,
                Duration::from_secs(1),
            )
            .is_err()
        );
    }
}
