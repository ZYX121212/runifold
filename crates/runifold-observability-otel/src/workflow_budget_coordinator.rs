//! Deterministically sharded multi-tenant budget projection coordination.

use std::{
    future::Future,
    num::{NonZeroU32, NonZeroUsize},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use futures_util::{StreamExt, stream::FuturesUnordered};
use runifold_core::CancellationToken;
use runifold_workflow::{
    SystemWorkflowWorkerSleeper, WorkerId, WorkflowBudgetAuditProjectionId, WorkflowStore,
    WorkflowStoreError, WorkflowTenantId, WorkflowTenantListLimit, WorkflowWorkerSleeper,
};

use crate::workflow_budget::{OtelWorkflowBudgetMetrics, OtelWorkflowBudgetProjectionError};
use crate::workflow_budget_supervisor::{
    OtelWorkflowBudgetSupervisor, OtelWorkflowBudgetSupervisorConfig,
    OtelWorkflowBudgetSupervisorCycleOutcome,
};

/// Stable shard assignment for one coordinator process.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OtelWorkflowBudgetShard {
    index: u32,
    count: NonZeroU32,
}

impl OtelWorkflowBudgetShard {
    /// Creates one zero-based shard in a fixed shard set.
    ///
    /// # Errors
    ///
    /// Rejects an index outside the shard count.
    pub fn new(index: u32, count: NonZeroU32) -> Result<Self, OtelWorkflowBudgetProjectionError> {
        if index >= count.get() {
            return Err(OtelWorkflowBudgetProjectionError::InvalidConfig(
                "coordinator shard index must be smaller than shard count",
            ));
        }
        Ok(Self { index, count })
    }

    /// Zero-based shard index.
    pub const fn index(self) -> u32 {
        self.index
    }

    /// Number of shards participating in deterministic assignment.
    pub const fn count(self) -> u32 {
        self.count.get()
    }

    /// Returns whether this shard owns the tenant under the stable hash.
    pub fn owns(self, tenant_id: &WorkflowTenantId) -> bool {
        stable_tenant_hash(tenant_id.as_str()) % u64::from(self.count.get())
            == u64::from(self.index)
    }
}

/// Validated bounds and timing for multi-tenant discovery and projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OtelWorkflowBudgetCoordinatorConfig {
    shard: OtelWorkflowBudgetShard,
    supervisor: OtelWorkflowBudgetSupervisorConfig,
    discovery_limit: WorkflowTenantListLimit,
    max_concurrency: NonZeroUsize,
    scan_interval: Duration,
    error_backoff: Duration,
}

impl OtelWorkflowBudgetCoordinatorConfig {
    /// Creates a coordinator with bounded conservative defaults.
    pub fn new(
        shard: OtelWorkflowBudgetShard,
        supervisor: OtelWorkflowBudgetSupervisorConfig,
    ) -> Self {
        Self {
            shard,
            supervisor,
            discovery_limit: WorkflowTenantListLimit::default(),
            max_concurrency: NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
            scan_interval: Duration::from_secs(5),
            error_backoff: Duration::from_secs(1),
        }
    }

    /// Sets discovery page and projection concurrency bounds.
    #[must_use]
    pub const fn with_work_limits(
        mut self,
        discovery_limit: WorkflowTenantListLimit,
        max_concurrency: NonZeroUsize,
    ) -> Self {
        self.discovery_limit = discovery_limit;
        self.max_concurrency = max_concurrency;
        self
    }

    /// Sets successful-scan and failed-scan delays.
    ///
    /// # Errors
    ///
    /// Rejects zero durations to prevent hot loops.
    pub fn with_intervals(
        mut self,
        scan_interval: Duration,
        error_backoff: Duration,
    ) -> Result<Self, OtelWorkflowBudgetProjectionError> {
        if scan_interval.is_zero() || error_backoff.is_zero() {
            return Err(OtelWorkflowBudgetProjectionError::InvalidConfig(
                "coordinator scan interval and error backoff must be positive",
            ));
        }
        self.scan_interval = scan_interval;
        self.error_backoff = error_backoff;
        Ok(self)
    }
}

/// Cumulative work observed during coordinator scans.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OtelWorkflowBudgetCoordinatorReport {
    /// Completed full discovery scans.
    pub scans: u64,
    /// Budget-enabled tenants returned by discovery.
    pub tenants_discovered: u64,
    /// Discovered tenants assigned to this shard.
    pub tenants_assigned: u64,
    /// Tenant projection cycles blocked by another active owner.
    pub contended: u64,
    /// Audit facts recorded into `OTel`.
    pub events_projected: u64,
    /// Non-empty pages durably acknowledged.
    pub batches_projected: u64,
    /// Per-tenant projection failures that did not abort discovery.
    pub projection_errors: u64,
    /// Discovery failures that aborted a scan.
    pub discovery_errors: u64,
}

impl OtelWorkflowBudgetCoordinatorReport {
    fn merge(&mut self, other: Self) {
        self.scans = self.scans.saturating_add(other.scans);
        self.tenants_discovered = self
            .tenants_discovered
            .saturating_add(other.tenants_discovered);
        self.tenants_assigned = self.tenants_assigned.saturating_add(other.tenants_assigned);
        self.contended = self.contended.saturating_add(other.contended);
        self.events_projected = self.events_projected.saturating_add(other.events_projected);
        self.batches_projected = self
            .batches_projected
            .saturating_add(other.batches_projected);
        self.projection_errors = self
            .projection_errors
            .saturating_add(other.projection_errors);
        self.discovery_errors = self.discovery_errors.saturating_add(other.discovery_errors);
    }
}

/// Dynamically discovers and projects every tenant assigned to one shard.
pub struct OtelWorkflowBudgetCoordinator<S> {
    store: Arc<S>,
    projection_id: WorkflowBudgetAuditProjectionId,
    owner: WorkerId,
    instruments: OtelWorkflowBudgetMetrics,
    config: OtelWorkflowBudgetCoordinatorConfig,
    sleeper: Arc<dyn WorkflowWorkerSleeper>,
}

impl<S> OtelWorkflowBudgetCoordinator<S>
where
    S: WorkflowStore + 'static,
{
    /// Creates a coordinator with system timers.
    pub fn new(
        store: Arc<S>,
        projection_id: WorkflowBudgetAuditProjectionId,
        owner: WorkerId,
        instruments: OtelWorkflowBudgetMetrics,
        config: OtelWorkflowBudgetCoordinatorConfig,
    ) -> Self {
        Self {
            store,
            projection_id,
            owner,
            instruments,
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

    /// Performs one stable paginated discovery scan.
    ///
    /// Per-tenant projection failures are counted and isolated. A discovery
    /// failure aborts the scan so the next run restarts from the beginning.
    ///
    /// # Errors
    ///
    /// Returns a typed store failure when tenant discovery fails.
    pub async fn scan_once(
        &self,
    ) -> Result<OtelWorkflowBudgetCoordinatorReport, WorkflowStoreError> {
        let mut report = OtelWorkflowBudgetCoordinatorReport::default();
        let mut after = None;
        loop {
            let tenants = self
                .store
                .list_tenant_budgets(after, self.config.discovery_limit)
                .await?;
            let page_len = tenants.len();
            report.tenants_discovered = report
                .tenants_discovered
                .saturating_add(u64::try_from(page_len).unwrap_or(u64::MAX));
            let last = tenants.last().cloned();
            let assigned = tenants
                .into_iter()
                .filter(|tenant_id| self.config.shard.owns(tenant_id))
                .collect::<Vec<_>>();
            report.tenants_assigned = report
                .tenants_assigned
                .saturating_add(u64::try_from(assigned.len()).unwrap_or(u64::MAX));
            self.project_assigned(assigned, &mut report).await;
            if page_len < usize::try_from(self.config.discovery_limit.get()).unwrap_or(usize::MAX) {
                break;
            }
            after = last;
        }
        report.scans = 1;
        Ok(report)
    }

    /// Continuously rescans until shutdown, draining each started scan.
    pub async fn run(&self, shutdown: &CancellationToken) -> OtelWorkflowBudgetCoordinatorReport {
        let mut report = OtelWorkflowBudgetCoordinatorReport::default();
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

    async fn project_assigned(
        &self,
        tenants: Vec<WorkflowTenantId>,
        report: &mut OtelWorkflowBudgetCoordinatorReport,
    ) {
        let mut tenants = tenants.into_iter();
        let mut active = FuturesUnordered::new();
        for _ in 0..self.config.max_concurrency.get() {
            let Some(tenant_id) = tenants.next() else {
                break;
            };
            active.push(self.project_tenant(tenant_id));
        }
        while let Some(result) = active.next().await {
            match result {
                Ok(OtelWorkflowBudgetSupervisorCycleOutcome::Contended) => {
                    report.contended = report.contended.saturating_add(1);
                }
                Ok(OtelWorkflowBudgetSupervisorCycleOutcome::Projected(batch)) => {
                    report.events_projected = report
                        .events_projected
                        .saturating_add(batch.events_projected);
                    report.batches_projected = report
                        .batches_projected
                        .saturating_add(u64::from(batch.batches_projected));
                }
                Err(_) => {
                    report.projection_errors = report.projection_errors.saturating_add(1);
                }
            }
            if let Some(tenant_id) = tenants.next() {
                active.push(self.project_tenant(tenant_id));
            }
        }
    }

    fn project_tenant(
        &self,
        tenant_id: WorkflowTenantId,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<
                        OtelWorkflowBudgetSupervisorCycleOutcome,
                        OtelWorkflowBudgetProjectionError,
                    >,
                > + Send
                + '_,
        >,
    > {
        let supervisor = OtelWorkflowBudgetSupervisor::new(
            Arc::clone(&self.store),
            tenant_id,
            self.projection_id.clone(),
            self.owner.clone(),
            self.instruments.clone(),
            self.config.supervisor,
        )
        .with_sleeper(Arc::clone(&self.sleeper));
        Box::pin(async move { supervisor.run_once().await })
    }
}

impl<S> std::fmt::Debug for OtelWorkflowBudgetCoordinator<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OtelWorkflowBudgetCoordinator")
            .field("projection_id", &self.projection_id)
            .field("owner", &self.owner)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

async fn wait_or_shutdown(
    sleeper: Arc<dyn WorkflowWorkerSleeper>,
    duration: Duration,
    shutdown: &CancellationToken,
) -> bool {
    use futures_util::future::{Either, select};

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
    use std::{
        num::{NonZeroU32, NonZeroUsize},
        sync::Arc,
        time::Duration,
    };

    use opentelemetry::global;
    use runifold_core::Budget;
    use runifold_workflow::{
        InMemoryWorkflowStore, LeaseDuration, WorkflowStore, WorkflowTenantBudgetPolicy,
    };

    use super::*;

    fn shard(index: u32) -> OtelWorkflowBudgetShard {
        OtelWorkflowBudgetShard::new(index, NonZeroU32::new(2).unwrap()).unwrap()
    }

    fn supervisor_config() -> OtelWorkflowBudgetSupervisorConfig {
        OtelWorkflowBudgetSupervisorConfig::new(
            LeaseDuration::new(Duration::from_secs(5)).unwrap(),
            Duration::from_secs(1),
        )
        .unwrap()
    }

    #[test]
    fn shards_form_a_stable_complete_partition() {
        for name in ["tenant-a", "tenant-b", "tenant-c", "tenant-d"] {
            let tenant = WorkflowTenantId::parse(name).unwrap();
            assert_ne!(shard(0).owns(&tenant), shard(1).owns(&tenant));
            assert_eq!(shard(0).owns(&tenant), shard(0).owns(&tenant));
        }
        assert!(OtelWorkflowBudgetShard::new(2, NonZeroU32::new(2).unwrap()).is_err());
    }

    #[tokio::test]
    async fn coordinators_page_and_project_each_tenant_exactly_once() {
        let store = Arc::new(InMemoryWorkflowStore::new());
        let policy = WorkflowTenantBudgetPolicy::new(
            Budget {
                tokens: Some(100),
                ..Budget::default()
            },
            Duration::from_secs(60),
            Duration::from_secs(1),
        )
        .unwrap();
        for name in ["tenant-a", "tenant-b", "tenant-c", "tenant-d", "tenant-e"] {
            store
                .set_tenant_budget_policy(WorkflowTenantId::parse(name).unwrap(), policy)
                .await
                .unwrap();
        }

        let projection_id = WorkflowBudgetAuditProjectionId::parse("otel-all-tenants").unwrap();
        let instruments = OtelWorkflowBudgetMetrics::new(&global::meter("runifold.test"));
        let mut reports = Vec::new();
        for index in 0..2 {
            let config =
                OtelWorkflowBudgetCoordinatorConfig::new(shard(index), supervisor_config())
                    .with_work_limits(
                        WorkflowTenantListLimit::new(2).unwrap(),
                        NonZeroUsize::new(2).unwrap(),
                    );
            let coordinator = OtelWorkflowBudgetCoordinator::new(
                Arc::clone(&store),
                projection_id.clone(),
                WorkerId::parse(format!("coordinator-{index}")).unwrap(),
                instruments.clone(),
                config,
            );
            reports.push(coordinator.scan_once().await.unwrap());
        }

        assert_eq!(reports.iter().map(|report| report.scans).sum::<u64>(), 2);
        assert_eq!(
            reports
                .iter()
                .map(|report| report.tenants_discovered)
                .sum::<u64>(),
            10
        );
        assert_eq!(
            reports
                .iter()
                .map(|report| report.tenants_assigned)
                .sum::<u64>(),
            5
        );
        assert_eq!(
            reports
                .iter()
                .map(|report| report.events_projected)
                .sum::<u64>(),
            5
        );
        assert_eq!(
            reports
                .iter()
                .map(|report| report.batches_projected)
                .sum::<u64>(),
            5
        );
        assert_eq!(
            reports
                .iter()
                .map(|report| report.projection_errors + report.contended)
                .sum::<u64>(),
            0
        );
    }
}
