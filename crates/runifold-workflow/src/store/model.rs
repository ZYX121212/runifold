use super::{
    Budget, CheckpointId, Deserialize, Duration, Error, Future, NonZeroU32, NonZeroU64, Pin,
    Serialize, Usage, Value, WorkflowInterruptRequest, WorkflowLineage, WorkflowWait, WorkflowWake,
};

/// A boxed asynchronous workflow-store operation.
pub type WorkflowStoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Validated isolation identity for workflow admission and control-plane access.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct WorkflowTenantId(String);

impl WorkflowTenantId {
    /// Validates a portable tenant identity.
    ///
    /// # Errors
    ///
    /// Rejects blank, oversized, or non-portable identifiers.
    pub fn parse(value: impl Into<String>) -> Result<Self, WorkflowStoreError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(WorkflowStoreError::invalid_input(
                "workflow tenant must contain 1..=128 portable ASCII characters",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the validated tenant identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for WorkflowTenantId {
    fn default() -> Self {
        Self("default".into())
    }
}

/// Per-tenant workflow admission limits.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowTenantPolicy {
    max_outstanding_tasks: NonZeroU32,
    max_concurrent_leases: NonZeroU32,
}

impl WorkflowTenantPolicy {
    /// Creates a bounded tenant admission policy.
    ///
    /// # Errors
    ///
    /// Rejects zero limits or a lease limit larger than the outstanding limit.
    pub fn new(
        max_outstanding_tasks: u32,
        max_concurrent_leases: u32,
    ) -> Result<Self, WorkflowStoreError> {
        let max_outstanding_tasks = NonZeroU32::new(max_outstanding_tasks).ok_or_else(|| {
            WorkflowStoreError::invalid_input("tenant outstanding workflow limit must be positive")
        })?;
        let max_concurrent_leases = NonZeroU32::new(max_concurrent_leases).ok_or_else(|| {
            WorkflowStoreError::invalid_input(
                "tenant concurrent workflow lease limit must be positive",
            )
        })?;
        if max_concurrent_leases > max_outstanding_tasks {
            return Err(WorkflowStoreError::invalid_input(
                "tenant concurrent workflow lease limit cannot exceed outstanding limit",
            ));
        }
        Ok(Self {
            max_outstanding_tasks,
            max_concurrent_leases,
        })
    }

    /// Maximum non-terminal tasks admitted for this tenant.
    pub const fn max_outstanding_tasks(self) -> u32 {
        self.max_outstanding_tasks.get()
    }

    /// Maximum unexpired leases concurrently owned for this tenant.
    pub const fn max_concurrent_leases(self) -> u32 {
        self.max_concurrent_leases.get()
    }
}

impl Default for WorkflowTenantPolicy {
    fn default() -> Self {
        Self {
            max_outstanding_tasks: NonZeroU32::new(10_000)
                .expect("default outstanding limit is positive"),
            max_concurrent_leases: NonZeroU32::new(100)
                .expect("default concurrent lease limit is positive"),
        }
    }
}

/// Validated page size for discovering tenants with configured budgets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkflowTenantListLimit(NonZeroU32);

impl WorkflowTenantListLimit {
    /// Creates a bounded tenant-discovery page size.
    ///
    /// # Errors
    ///
    /// Rejects zero or values greater than 1,000.
    pub fn new(value: u32) -> Result<Self, WorkflowStoreError> {
        let value = NonZeroU32::new(value).ok_or_else(|| {
            WorkflowStoreError::invalid_input("workflow tenant list limit must be positive")
        })?;
        if value.get() > 1_000 {
            return Err(WorkflowStoreError::invalid_input(
                "workflow tenant list limit cannot exceed 1,000",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the validated maximum number of tenants.
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl Default for WorkflowTenantListLimit {
    fn default() -> Self {
        Self(NonZeroU32::new(100).expect("default tenant page size is positive"))
    }
}

/// Persistent aggregate budget policy for one workflow tenant.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowTenantBudgetPolicy {
    limit: Budget,
    window_ms: NonZeroU64,
    recovery_grace_ms: u64,
}

impl WorkflowTenantBudgetPolicy {
    /// Creates a fixed-window tenant budget with a crash-recovery grace period.
    ///
    /// # Errors
    ///
    /// Rejects an unbounded policy, invalid duration units, or overflowing
    /// window and recovery durations.
    pub fn new(
        limit: Budget,
        window: Duration,
        recovery_grace: Duration,
    ) -> Result<Self, WorkflowStoreError> {
        if budget_is_unbounded(limit) {
            return Err(WorkflowStoreError::invalid_input(
                "tenant budget policy must limit at least one resource",
            ));
        }
        validate_budget_duration(limit)?;
        let window_ms = u64::try_from(window.as_millis())
            .ok()
            .and_then(NonZeroU64::new)
            .ok_or_else(|| {
                WorkflowStoreError::invalid_input(
                    "tenant budget window must fit in positive whole milliseconds",
                )
            })?;
        let recovery_grace_ms = u64::try_from(recovery_grace.as_millis()).map_err(|_| {
            WorkflowStoreError::invalid_input(
                "tenant budget recovery grace exceeds supported milliseconds",
            )
        })?;
        Ok(Self {
            limit,
            window_ms,
            recovery_grace_ms,
        })
    }

    /// Returns the aggregate hard limit.
    pub const fn limit(self) -> Budget {
        self.limit
    }

    /// Returns the fixed-window length in milliseconds.
    pub const fn window_millis(self) -> u64 {
        self.window_ms.get()
    }

    /// Returns the reservation takeover grace in milliseconds.
    pub const fn recovery_grace_millis(self) -> u64 {
        self.recovery_grace_ms
    }
}

/// Safe point-in-time view of one tenant budget ledger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTenantBudgetSnapshot {
    /// Tenant owning the ledger.
    pub tenant_id: WorkflowTenantId,
    /// Configured aggregate hard limit.
    pub limit: Budget,
    /// Store-authoritative start of the current draining window.
    pub window_started_at_ms: u64,
    /// Usage durably settled or conservatively forfeited in this window.
    pub committed: Usage,
    /// Upper bounds held by live workflow reservations.
    pub reserved: Usage,
    /// Number of reservations awaiting settlement or takeover.
    pub active_reservations: u64,
}

/// Stable cursor for incrementally consuming one tenant's durable budget audit.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct WorkflowBudgetAuditCursor(u64);

impl WorkflowBudgetAuditCursor {
    /// Creates a cursor from a previously observed sequence.
    pub const fn new(sequence: u64) -> Self {
        Self(sequence)
    }

    /// Returns the durable sequence represented by this cursor.
    pub const fn sequence(self) -> u64 {
        self.0
    }
}

/// Validated page size for tenant budget audit reads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkflowBudgetAuditLimit(NonZeroU32);

impl WorkflowBudgetAuditLimit {
    /// Creates a bounded audit page size.
    ///
    /// # Errors
    ///
    /// Rejects zero or values greater than 1,000.
    pub fn new(value: u32) -> Result<Self, WorkflowStoreError> {
        let value = NonZeroU32::new(value).ok_or_else(|| {
            WorkflowStoreError::invalid_input("workflow budget audit limit must be positive")
        })?;
        if value.get() > 1_000 {
            return Err(WorkflowStoreError::invalid_input(
                "workflow budget audit limit cannot exceed 1,000",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the validated maximum number of events.
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl Default for WorkflowBudgetAuditLimit {
    fn default() -> Self {
        Self(NonZeroU32::new(100).expect("default audit page size is positive"))
    }
}

/// Stable identity of one independent tenant-budget audit projection.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct WorkflowBudgetAuditProjectionId(String);

impl WorkflowBudgetAuditProjectionId {
    /// Validates a portable projection identity.
    ///
    /// # Errors
    ///
    /// Rejects blank, oversized, or non-portable identifiers.
    pub fn parse(value: impl Into<String>) -> Result<Self, WorkflowStoreError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(WorkflowStoreError::invalid_input(
                "workflow budget audit projection must contain 1..=128 portable ASCII characters",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the validated projection identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Fenced ownership of one named tenant-budget audit projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowBudgetAuditProjectionLease {
    /// Tenant whose audit stream is projected.
    pub tenant_id: WorkflowTenantId,
    /// Stable identity of the projection.
    pub projection_id: WorkflowBudgetAuditProjectionId,
    /// Worker currently owning the projection.
    pub owner: WorkerId,
    /// Last fully acknowledged audit cursor.
    pub cursor: WorkflowBudgetAuditCursor,
    /// Monotonic token invalidating superseded projector instances.
    pub fencing_token: u64,
    /// Store-authoritative lease expiration in Unix milliseconds.
    pub expires_at_ms: u64,
}

impl WorkflowBudgetAuditProjectionLease {
    /// Tenant whose audit stream is projected.
    pub fn tenant_id(&self) -> &WorkflowTenantId {
        &self.tenant_id
    }

    /// Stable identity of the projection.
    pub fn projection_id(&self) -> &WorkflowBudgetAuditProjectionId {
        &self.projection_id
    }

    /// Worker currently owning the projection.
    pub fn owner(&self) -> &WorkerId {
        &self.owner
    }

    /// Last fully acknowledged audit cursor.
    pub const fn cursor(&self) -> WorkflowBudgetAuditCursor {
        self.cursor
    }

    /// Monotonic token invalidating superseded projector instances.
    pub const fn fencing_token(&self) -> u64 {
        self.fencing_token
    }

    /// Store-authoritative lease expiration in Unix milliseconds.
    pub const fn expires_at_ms(&self) -> u64 {
        self.expires_at_ms
    }
}

/// Why uncertain reserved capacity was conservatively committed.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum WorkflowBudgetForfeitReason {
    /// An operator cancelled the workflow before settlement.
    Cancelled,
    /// No fenced successor adopted the reservation before its recovery grace elapsed.
    RecoveryExpired,
}

/// One durable tenant-budget decision.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum WorkflowBudgetAuditKind {
    /// A policy was created or replaced.
    PolicyConfigured,
    /// A new workflow envelope was reserved.
    Reserved,
    /// A successor lease adopted a recoverable envelope.
    Adopted,
    /// Aggregate capacity rejected a requested envelope.
    AdmissionDenied,
    /// Observed cumulative usage exceeded the workflow's reserved envelope.
    UsageExceeded,
    /// Actual observed usage was committed and unused capacity released.
    Settled,
    /// Uncertain capacity was conservatively committed.
    Forfeited(WorkflowBudgetForfeitReason),
    /// A fully drained fixed window advanced.
    WindowReset,
}

/// Immutable audit fact recorded with a tenant-budget state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowBudgetAuditEvent {
    /// Monotonic sequence within this tenant's budget ledger.
    pub cursor: WorkflowBudgetAuditCursor,
    /// Tenant owning the decision.
    pub tenant_id: WorkflowTenantId,
    /// Workflow checkpoint involved, when the decision is workflow-specific.
    pub checkpoint_id: Option<CheckpointId>,
    /// Store-authoritative event time in Unix milliseconds.
    pub occurred_at_ms: u64,
    /// Stable decision category.
    pub kind: WorkflowBudgetAuditKind,
    /// Envelope or committed delta associated with the decision.
    pub usage: Usage,
    /// Age of the affected reservation, when applicable.
    pub reservation_age_ms: Option<u64>,
    /// Policy active when the event was recorded.
    pub limit: Budget,
    /// Committed usage immediately after the decision.
    pub committed: Usage,
    /// Reserved usage immediately after the decision.
    pub reserved: Usage,
}

/// Outcome of attempting to reserve a tenant budget envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowBudgetReservationOutcome {
    /// No aggregate tenant budget is configured.
    NotConfigured,
    /// The workflow envelope is durably reserved under the current lease.
    Reserved,
}

/// Stable identity of a distributed workflow worker.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct WorkerId(String);

impl WorkerId {
    /// Validates a worker identity.
    ///
    /// # Errors
    ///
    /// Rejects blank, oversized, or non-portable identifiers.
    pub fn parse(value: impl Into<String>) -> Result<Self, WorkflowStoreError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(WorkflowStoreError::invalid_input(
                "worker identity must contain 1..=128 portable ASCII characters",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the validated worker identity.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Positive lease duration represented in whole milliseconds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaseDuration(NonZeroU64);

impl LeaseDuration {
    /// Validates and normalizes one lease duration.
    ///
    /// # Errors
    ///
    /// Rejects sub-millisecond, zero, or overflowing durations.
    pub fn new(duration: Duration) -> Result<Self, WorkflowStoreError> {
        let millis = u64::try_from(duration.as_millis())
            .ok()
            .and_then(NonZeroU64::new)
            .ok_or_else(|| {
                WorkflowStoreError::invalid_input(
                    "workflow lease must fit in a positive whole-millisecond duration",
                )
            })?;
        Ok(Self(millis))
    }

    /// Returns the normalized duration in milliseconds.
    pub const fn as_millis(self) -> u64 {
        self.0.get()
    }
}

/// One durable workflow task awaiting execution.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkflowTask {
    /// Checkpoint identity shared across every retry and worker claim.
    pub checkpoint_id: CheckpointId,
    /// Tenant that owns admission and control-plane authority for this task.
    pub tenant_id: WorkflowTenantId,
    /// Stable workflow definition name.
    pub workflow: String,
    /// Caller-managed workflow definition version.
    pub workflow_version: u32,
    /// Canonical workflow input.
    pub input: Value,
    /// Higher values are claimed first.
    pub priority: i32,
}

impl WorkflowTask {
    /// Creates an immediately available workflow task.
    ///
    /// # Errors
    ///
    /// Rejects blank workflow names or version zero.
    pub fn new(
        workflow: impl Into<String>,
        workflow_version: u32,
        input: Value,
    ) -> Result<Self, WorkflowStoreError> {
        let workflow = workflow.into();
        if workflow.trim().is_empty() || workflow.len() > 256 {
            return Err(WorkflowStoreError::invalid_input(
                "workflow name must contain 1..=256 bytes",
            ));
        }
        if workflow_version == 0 {
            return Err(WorkflowStoreError::invalid_input(
                "workflow version must be greater than zero",
            ));
        }
        Ok(Self {
            checkpoint_id: CheckpointId::new(),
            tenant_id: WorkflowTenantId::default(),
            workflow,
            workflow_version,
            input,
            priority: 0,
        })
    }

    /// Assigns the task to an explicit tenant.
    #[must_use]
    pub fn with_tenant(mut self, tenant_id: WorkflowTenantId) -> Self {
        self.tenant_id = tenant_id;
        self
    }

    /// Uses an existing checkpoint identity.
    #[must_use]
    pub const fn with_checkpoint_id(mut self, checkpoint_id: CheckpointId) -> Self {
        self.checkpoint_id = checkpoint_id;
        self
    }

    /// Sets the task priority.
    #[must_use]
    pub const fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Revalidates invariants after deserialization or external construction.
    ///
    /// # Errors
    ///
    /// Rejects blank workflow names or version zero.
    pub fn validate(&self) -> Result<(), WorkflowStoreError> {
        if self.workflow.trim().is_empty() || self.workflow.len() > 256 {
            return Err(WorkflowStoreError::invalid_input(
                "workflow name must contain 1..=256 bytes",
            ));
        }
        if self.workflow_version == 0 {
            return Err(WorkflowStoreError::invalid_input(
                "workflow version must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// Fenced ownership of one workflow task.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowLease {
    /// Claimed workflow checkpoint.
    pub checkpoint_id: CheckpointId,
    /// Tenant that owns the leased task.
    pub tenant_id: WorkflowTenantId,
    /// Current worker owner.
    pub worker: WorkerId,
    /// Monotonic fencing token incremented on every successful claim.
    pub fencing_token: u64,
    /// One-based claim attempt.
    pub attempt: u64,
    /// Store-authoritative lease expiration in Unix milliseconds.
    pub expires_at_ms: u64,
}

/// A workflow task and its fenced worker lease.
#[derive(Clone, Debug, PartialEq)]
pub struct ClaimedWorkflow {
    /// Durable task definition.
    pub task: WorkflowTask,
    /// Current fenced ownership.
    pub lease: WorkflowLease,
    /// Durable wake value retained across lease takeover until the next wait.
    pub wake: Option<WorkflowWake>,
}

/// Durable task disposition accepted from the current lease owner.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowDisposition {
    /// The checkpoint contains a terminal successful outcome.
    Completed,
    /// Return the task to the queue after a store-relative delay.
    RetryAfter(Duration),
    /// Release the worker while waiting for time or an external signal.
    Suspend(WorkflowWait),
    /// Stop automatic execution with a safe operator-facing reason.
    Failed(String),
    /// Stop execution because the task was explicitly cancelled.
    Cancelled,
}

/// Durable workflow queue state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowTaskStatus {
    /// Available now or after a retry delay.
    Queued,
    /// Owned by a worker until its lease expires.
    Leased,
    /// Persisted without a worker lease until its wake condition is satisfied.
    Waiting,
    /// Successfully completed.
    Completed,
    /// Permanently failed.
    Failed,
    /// Explicitly cancelled.
    Cancelled,
}

/// Minimum time a terminal workflow remains queryable before physical cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkflowTaskRetention(NonZeroU64);

impl WorkflowTaskRetention {
    /// Creates a positive whole-millisecond retention.
    ///
    /// # Errors
    ///
    /// Rejects zero, sub-millisecond, or overflowing durations.
    pub fn new(duration: Duration) -> Result<Self, WorkflowStoreError> {
        let millis = u64::try_from(duration.as_millis())
            .ok()
            .and_then(NonZeroU64::new)
            .ok_or_else(|| {
                WorkflowStoreError::invalid_input(
                    "workflow Task retention must fit in positive whole milliseconds",
                )
            })?;
        Ok(Self(millis))
    }

    /// Returns normalized retention milliseconds.
    pub const fn as_millis(self) -> u64 {
        self.0.get()
    }
}

/// Maximum terminal Tasks removed by one fenced cleanup operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkflowTaskCleanupLimit(NonZeroU32);

impl WorkflowTaskCleanupLimit {
    /// Creates a bounded cleanup batch size.
    ///
    /// # Errors
    ///
    /// Rejects zero or values greater than 1,000.
    pub fn new(value: u32) -> Result<Self, WorkflowStoreError> {
        let value = NonZeroU32::new(value).ok_or_else(|| {
            WorkflowStoreError::invalid_input("workflow Task cleanup limit must be positive")
        })?;
        if value.get() > 1_000 {
            return Err(WorkflowStoreError::invalid_input(
                "workflow Task cleanup limit cannot exceed 1,000",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the validated cleanup batch size.
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Stable cursor for paginating immutable terminal Task tombstones.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct WorkflowTaskTombstoneCursor(u64);

impl WorkflowTaskTombstoneCursor {
    /// Creates a cursor from its durable sequence.
    pub const fn new(sequence: u64) -> Self {
        Self(sequence)
    }

    /// Returns the durable sequence.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Maximum tombstones returned by one audit page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkflowTaskTombstoneLimit(NonZeroU32);

impl WorkflowTaskTombstoneLimit {
    /// Creates a bounded tombstone page size.
    ///
    /// # Errors
    ///
    /// Rejects zero or values greater than 1,000.
    pub fn new(value: u32) -> Result<Self, WorkflowStoreError> {
        let value = NonZeroU32::new(value).ok_or_else(|| {
            WorkflowStoreError::invalid_input("workflow Task tombstone limit must be positive")
        })?;
        if value.get() > 1_000 {
            return Err(WorkflowStoreError::invalid_input(
                "workflow Task tombstone limit cannot exceed 1,000",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the validated audit page size.
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Fenced ownership of one tenant's terminal Task cleanup partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTaskCleanupLease {
    /// Tenant whose terminal Tasks may be removed.
    pub tenant_id: WorkflowTenantId,
    /// Exclusive cleanup owner.
    pub owner: WorkerId,
    /// Monotonic token incremented on every successful claim.
    pub fencing_token: u64,
    /// Store-authoritative expiration in Unix milliseconds.
    pub expires_at_ms: u64,
}

/// Immutable audit fact retained after a terminal Task is physically removed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTaskTombstone {
    /// Monotonic tenant-independent storage cursor.
    pub cursor: WorkflowTaskTombstoneCursor,
    /// Removed workflow identity.
    pub checkpoint_id: CheckpointId,
    /// Tenant that owned the removed Task.
    pub tenant_id: WorkflowTenantId,
    /// Stable workflow definition.
    pub workflow: String,
    /// Exact workflow version.
    pub workflow_version: u32,
    /// Terminal state observed atomically with deletion.
    pub final_status: WorkflowTaskStatus,
    /// Original store-authoritative creation time.
    pub created_at_ms: u64,
    /// Store-authoritative terminal transition time.
    pub terminal_at_ms: u64,
    /// Store-authoritative physical deletion time.
    pub deleted_at_ms: u64,
}

/// Result of an idempotent external workflow cancellation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowCancelOutcome {
    /// A queued, waiting, or leased workflow became cancelled.
    Cancelled,
    /// The workflow was already in a terminal state.
    AlreadyTerminal,
}

/// Safe operator snapshot of one workflow task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTaskSnapshot {
    /// Stable task identity.
    pub checkpoint_id: CheckpointId,
    /// Tenant that owns the task.
    pub tenant_id: WorkflowTenantId,
    /// Stable workflow definition name.
    pub workflow: String,
    /// Exact workflow definition version.
    pub workflow_version: u32,
    /// Current durable state.
    pub status: WorkflowTaskStatus,
    /// Store-authoritative creation time in Unix milliseconds.
    pub created_at_ms: u64,
    /// Store-authoritative last state-transition time in Unix milliseconds.
    pub updated_at_ms: u64,
    /// Number of successful claims.
    pub attempts: u64,
    /// Current fencing token.
    pub fencing_token: u64,
    /// Current lease owner, when leased.
    pub owner: Option<WorkerId>,
    /// Lease expiration, when leased.
    pub lease_expires_at_ms: Option<u64>,
    /// Pending human-review request, when the task is interrupted.
    pub interrupt: Option<WorkflowInterruptRequest>,
    /// Safe terminal failure explanation, when failed.
    pub failure_message: Option<String>,
    /// Immutable parent relation when this task was forked from history.
    pub lineage: Option<WorkflowLineage>,
}

/// Stable workflow-store failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowStoreErrorKind {
    /// Input violated a domain invariant.
    InvalidInput,
    /// The requested task does not exist.
    NotFound,
    /// A create-only operation found an existing task.
    Conflict,
    /// The caller no longer owns the current unexpired lease.
    LeaseLost,
    /// A tenant admission limit prevented the operation.
    AdmissionDenied,
    /// The supplied tenant does not own the requested resource.
    TenantMismatch,
    /// The backing store failed.
    Storage,
}

/// Typed workflow-store failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{kind:?}: {message}")]
pub struct WorkflowStoreError {
    /// Stable failure category.
    pub kind: WorkflowStoreErrorKind,
    /// Safe application-facing explanation.
    pub message: String,
}

impl WorkflowStoreError {
    /// Creates a normalized store failure.
    pub fn new(kind: WorkflowStoreErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub(super) fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(WorkflowStoreErrorKind::InvalidInput, message)
    }
}

fn budget_is_unbounded(budget: Budget) -> bool {
    budget.tokens.is_none()
        && budget.cost_microusd.is_none()
        && budget.duration.is_none()
        && budget.turns.is_none()
        && budget.tool_calls.is_none()
        && budget.delegations.is_none()
}

fn validate_budget_duration(budget: Budget) -> Result<(), WorkflowStoreError> {
    if let Some(duration) = budget.duration {
        duration_micros(duration)?;
    }
    Ok(())
}

pub(super) fn duration_micros(duration: Duration) -> Result<u64, WorkflowStoreError> {
    u64::try_from(duration.as_micros()).map_err(|_| {
        WorkflowStoreError::invalid_input("budget duration exceeds supported microseconds")
    })
}
