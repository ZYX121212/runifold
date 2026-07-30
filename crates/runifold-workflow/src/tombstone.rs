//! Governance contracts for exporting, holding, and purging Task tombstones.

use std::{num::NonZeroU32, num::NonZeroU64, time::Duration};

use runifold_core::CheckpointId;

use crate::{
    WorkerId, WorkflowStoreError, WorkflowStoreErrorKind, WorkflowStoreFuture,
    WorkflowTaskCleanupLease, WorkflowTaskRetentionStore, WorkflowTaskTombstoneCursor,
    WorkflowTenantId,
};

/// Minimum age of an exported tombstone before it may enter a purge intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkflowTaskTombstoneRetention(NonZeroU64);

impl WorkflowTaskTombstoneRetention {
    /// Creates a positive whole-millisecond tombstone retention.
    ///
    /// # Errors
    ///
    /// Rejects zero, sub-millisecond, or overflowing durations.
    pub fn new(duration: Duration) -> Result<Self, WorkflowStoreError> {
        let millis = u64::try_from(duration.as_millis())
            .ok()
            .and_then(NonZeroU64::new)
            .ok_or_else(|| {
                invalid_input("Task tombstone retention must fit in positive whole milliseconds")
            })?;
        Ok(Self(millis))
    }

    /// Returns normalized retention milliseconds.
    pub const fn as_millis(self) -> u64 {
        self.0.get()
    }
}

/// Maximum tombstones captured in one immutable purge intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkflowTaskTombstonePurgeLimit(NonZeroU32);

impl WorkflowTaskTombstonePurgeLimit {
    /// Creates a purge limit in `1..=1,000`.
    ///
    /// # Errors
    ///
    /// Rejects zero or values greater than 1,000.
    pub fn new(value: u32) -> Result<Self, WorkflowStoreError> {
        let value =
            NonZeroU32::new(value).ok_or_else(|| invalid_input("purge limit must be positive"))?;
        if value.get() > 1_000 {
            return Err(invalid_input("purge limit cannot exceed 1,000"));
        }
        Ok(Self(value))
    }

    /// Returns the validated maximum.
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Time available for an independent principal to approve a purge intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkflowTaskTombstoneApprovalWindow(NonZeroU64);

impl WorkflowTaskTombstoneApprovalWindow {
    /// Creates a positive whole-millisecond approval window.
    ///
    /// # Errors
    ///
    /// Rejects zero, sub-millisecond, or overflowing durations.
    pub fn new(duration: Duration) -> Result<Self, WorkflowStoreError> {
        let millis = u64::try_from(duration.as_millis())
            .ok()
            .and_then(NonZeroU64::new)
            .ok_or_else(|| {
                invalid_input("purge approval window must fit in positive whole milliseconds")
            })?;
        Ok(Self(millis))
    }

    /// Returns normalized approval-window milliseconds.
    pub const fn as_millis(self) -> u64 {
        self.0.get()
    }
}

/// Maximum approval inbox entries returned by one bounded query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkflowTaskTombstoneApprovalInboxLimit(NonZeroU32);

impl WorkflowTaskTombstoneApprovalInboxLimit {
    /// Creates an inbox limit in `1..=1,000`.
    ///
    /// # Errors
    ///
    /// Rejects zero or values greater than 1,000.
    pub fn new(value: u32) -> Result<Self, WorkflowStoreError> {
        let value = NonZeroU32::new(value)
            .ok_or_else(|| invalid_input("approval inbox limit must be positive"))?;
        if value.get() > 1_000 {
            return Err(invalid_input("approval inbox limit cannot exceed 1,000"));
        }
        Ok(Self(value))
    }

    /// Returns the validated maximum.
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// Bounded operator justification for rejecting a purge intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTaskTombstoneRejectionReason(String);

impl WorkflowTaskTombstoneRejectionReason {
    /// Validates a non-blank reason of at most 1,024 bytes.
    ///
    /// # Errors
    ///
    /// Rejects blank, oversized, or control-character-bearing values.
    pub fn parse(value: impl Into<String>) -> Result<Self, WorkflowStoreError> {
        let value = value.into();
        if value.trim().is_empty() || value.len() > 1_024 || value.chars().any(char::is_control) {
            return Err(invalid_input(
                "purge rejection reason must contain 1..=1,024 printable bytes",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the validated reason.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Bounded operator-facing justification for a legal hold.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTaskLegalHoldReason(String);

impl WorkflowTaskLegalHoldReason {
    /// Validates a non-blank reason of at most 1,024 bytes.
    ///
    /// # Errors
    ///
    /// Rejects blank or oversized values.
    pub fn parse(value: impl Into<String>) -> Result<Self, WorkflowStoreError> {
        let value = value.into();
        if value.trim().is_empty() || value.len() > 1_024 {
            return Err(invalid_input(
                "Task legal-hold reason must contain 1..=1,024 bytes",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the validated reason.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Opaque receipt proving an external archive accepted a tombstone prefix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTaskTombstoneExportReceipt(String);

impl WorkflowTaskTombstoneExportReceipt {
    /// Validates a portable non-blank receipt of at most 512 bytes.
    ///
    /// # Errors
    ///
    /// Rejects blank, oversized, or control-character-bearing receipts.
    pub fn parse(value: impl Into<String>) -> Result<Self, WorkflowStoreError> {
        let value = value.into();
        if value.trim().is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
            return Err(invalid_input(
                "Task tombstone export receipt must contain 1..=512 printable bytes",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the validated opaque receipt.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Stable identity of one prepared purge set.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkflowTaskTombstonePurgeId(CheckpointId);

impl WorkflowTaskTombstonePurgeId {
    /// Creates a time-ordered purge identity.
    pub fn new() -> Self {
        Self(CheckpointId::new())
    }

    /// Creates an identity from its stored UUID-shaped value.
    pub const fn from_checkpoint_id(value: CheckpointId) -> Self {
        Self(value)
    }

    /// Returns the underlying portable identity.
    pub const fn as_checkpoint_id(self) -> CheckpointId {
        self.0
    }
}

impl Default for WorkflowTaskTombstonePurgeId {
    fn default() -> Self {
        Self::new()
    }
}

/// Active or released legal-hold state retained for audit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTaskLegalHold {
    /// Tombstone protected by the hold.
    pub checkpoint_id: CheckpointId,
    /// Owning tenant.
    pub tenant_id: WorkflowTenantId,
    /// Principal that placed the hold.
    pub placed_by: WorkerId,
    /// Bounded operator justification.
    pub reason: WorkflowTaskLegalHoldReason,
    /// Store-authoritative placement time.
    pub placed_at_ms: u64,
    /// Principal that released the hold, when inactive.
    pub released_by: Option<WorkerId>,
    /// Store-authoritative release time, when inactive.
    pub released_at_ms: Option<u64>,
}

impl WorkflowTaskLegalHold {
    /// Returns whether this hold currently blocks purge.
    pub const fn is_active(&self) -> bool {
        self.released_at_ms.is_none()
    }
}

/// Monotonic confirmation that an external archive accepted a tenant prefix.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTaskTombstoneExport {
    /// Tenant whose tombstone prefix was exported.
    pub tenant_id: WorkflowTenantId,
    /// Greatest global tombstone cursor confirmed by the archive.
    pub through: WorkflowTaskTombstoneCursor,
    /// Opaque archive receipt.
    pub receipt: WorkflowTaskTombstoneExportReceipt,
    /// Principal confirming the archive response.
    pub confirmed_by: WorkerId,
    /// Store-authoritative confirmation time.
    pub confirmed_at_ms: u64,
}

/// Prepared, bounded, and independently approvable tombstone purge set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTaskTombstonePurgeIntent {
    /// Stable purge identity.
    pub purge_id: WorkflowTaskTombstonePurgeId,
    /// Tenant owning every selected tombstone.
    pub tenant_id: WorkflowTenantId,
    /// Principal that prepared the set under a cleanup lease.
    pub prepared_by: WorkerId,
    /// Number of selected tombstones.
    pub tombstone_count: u32,
    /// First selected cursor, absent for an empty intent.
    pub first_cursor: Option<WorkflowTaskTombstoneCursor>,
    /// Last selected cursor, absent for an empty intent.
    pub last_cursor: Option<WorkflowTaskTombstoneCursor>,
    /// Export watermark captured when the set was prepared.
    pub export_through: WorkflowTaskTombstoneCursor,
    /// Deterministic fingerprint of the ordered selected identities.
    pub fingerprint: String,
    /// Store-authoritative preparation time.
    pub prepared_at_ms: u64,
    /// Store-authoritative approval deadline.
    pub expires_at_ms: u64,
    /// Independent approving principal.
    pub approved_by: Option<WorkerId>,
    /// Store-authoritative approval time.
    pub approved_at_ms: Option<u64>,
}

/// Durable operator-visible state of a purge approval request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowTaskTombstoneApprovalState {
    /// Available for an independent reviewer.
    Pending,
    /// Temporarily owned by one fenced reviewer.
    Claimed,
    /// Independently approved.
    Approved,
    /// Explicitly rejected with a durable reason.
    Rejected,
    /// Its immutable approval window elapsed.
    Expired,
}

/// One tenant-scoped durable approval inbox entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTaskTombstoneApprovalInboxItem {
    /// Immutable purge intent being reviewed.
    pub intent: WorkflowTaskTombstonePurgeIntent,
    /// Current operator-facing state.
    pub state: WorkflowTaskTombstoneApprovalState,
    /// Current reviewer, only while actively claimed.
    pub claimed_by: Option<WorkerId>,
    /// Store-authoritative claim expiration.
    pub claim_expires_at_ms: Option<u64>,
    /// Principal that rejected the request.
    pub rejected_by: Option<WorkerId>,
    /// Durable rejection justification.
    pub rejection_reason: Option<WorkflowTaskTombstoneRejectionReason>,
    /// Store-authoritative rejection time.
    pub rejected_at_ms: Option<u64>,
}

/// Fenced, expiring ownership of one approval request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTaskTombstoneApprovalLease {
    /// Tenant owning the request.
    pub tenant_id: WorkflowTenantId,
    /// Purge request under review.
    pub purge_id: WorkflowTaskTombstonePurgeId,
    /// Authenticated reviewer holding the claim.
    pub reviewer: WorkerId,
    /// Monotonic token fencing stale reviewers.
    pub fencing_token: u64,
    /// Store-authoritative claim expiration.
    pub expires_at_ms: u64,
}

/// Immutable aggregate evidence retained after detailed tombstones are purged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTaskTombstonePurgeEvidence {
    /// Executed purge identity.
    pub purge_id: WorkflowTaskTombstonePurgeId,
    /// Tenant whose detailed tombstones were removed.
    pub tenant_id: WorkflowTenantId,
    /// Principal that prepared the set.
    pub prepared_by: WorkerId,
    /// Independent principal that approved it.
    pub approved_by: WorkerId,
    /// Fenced cleanup principal that executed it.
    pub executed_by: WorkerId,
    /// Number of removed tombstones.
    pub tombstone_count: u32,
    /// First removed cursor.
    pub first_cursor: WorkflowTaskTombstoneCursor,
    /// Last removed cursor.
    pub last_cursor: WorkflowTaskTombstoneCursor,
    /// Export watermark authorizing the purge.
    pub export_through: WorkflowTaskTombstoneCursor,
    /// Deterministic prepared-set fingerprint.
    pub fingerprint: String,
    /// Store-authoritative execution time.
    pub executed_at_ms: u64,
}

/// Optional governance plane for detailed Task tombstone lifecycle.
pub trait WorkflowTaskTombstoneGovernanceStore: WorkflowTaskRetentionStore {
    /// Places or idempotently reads a legal hold on an existing tombstone.
    fn place_task_tombstone_hold(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        actor: WorkerId,
        reason: WorkflowTaskLegalHoldReason,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskLegalHold, WorkflowStoreError>>;

    /// Releases an exact active legal hold while retaining its audit row.
    fn release_task_tombstone_hold(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        actor: WorkerId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskLegalHold, WorkflowStoreError>>;

    /// Monotonically confirms an externally archived tenant cursor prefix.
    fn confirm_task_tombstone_export(
        &self,
        tenant_id: WorkflowTenantId,
        through: WorkflowTaskTombstoneCursor,
        receipt: WorkflowTaskTombstoneExportReceipt,
        actor: WorkerId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstoneExport, WorkflowStoreError>>;

    /// Freezes one bounded, exported, unheld, old-enough purge candidate set.
    fn prepare_task_tombstone_purge(
        &self,
        lease: WorkflowTaskCleanupLease,
        retention: WorkflowTaskTombstoneRetention,
        limit: WorkflowTaskTombstonePurgeLimit,
        approval_window: WorkflowTaskTombstoneApprovalWindow,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstonePurgeIntent, WorkflowStoreError>>;

    /// Approves a pending intent using a different principal from its preparer.
    fn approve_task_tombstone_purge(
        &self,
        tenant_id: WorkflowTenantId,
        purge_id: WorkflowTaskTombstonePurgeId,
        approver: WorkerId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstonePurgeIntent, WorkflowStoreError>>;

    /// Lists a bounded tenant approval inbox with expired claims normalized.
    fn list_task_tombstone_purge_approvals(
        &self,
        tenant_id: WorkflowTenantId,
        limit: WorkflowTaskTombstoneApprovalInboxLimit,
    ) -> WorkflowStoreFuture<
        '_,
        Result<Vec<WorkflowTaskTombstoneApprovalInboxItem>, WorkflowStoreError>,
    >;

    /// Atomically claims the oldest eligible request for an independent reviewer.
    fn claim_task_tombstone_purge_approval(
        &self,
        tenant_id: WorkflowTenantId,
        reviewer: WorkerId,
        lease: crate::LeaseDuration,
    ) -> WorkflowStoreFuture<
        '_,
        Result<Option<WorkflowTaskTombstoneApprovalLease>, WorkflowStoreError>,
    >;

    /// Approves under an exact, unexpired, fenced reviewer lease.
    fn approve_claimed_task_tombstone_purge(
        &self,
        lease: WorkflowTaskTombstoneApprovalLease,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstonePurgeIntent, WorkflowStoreError>>;

    /// Rejects under an exact reviewer lease and preserves the reason.
    fn reject_claimed_task_tombstone_purge(
        &self,
        lease: WorkflowTaskTombstoneApprovalLease,
        reason: WorkflowTaskTombstoneRejectionReason,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstoneApprovalInboxItem, WorkflowStoreError>>;

    /// Executes an approved intent under a current fenced cleanup lease.
    ///
    /// Active legal holds are checked again atomically with deletion.
    fn execute_task_tombstone_purge(
        &self,
        lease: WorkflowTaskCleanupLease,
        purge_id: WorkflowTaskTombstonePurgeId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskTombstonePurgeEvidence, WorkflowStoreError>>;

    /// Reads immutable evidence for one executed purge.
    fn get_task_tombstone_purge_evidence(
        &self,
        tenant_id: WorkflowTenantId,
        purge_id: WorkflowTaskTombstonePurgeId,
    ) -> WorkflowStoreFuture<
        '_,
        Result<Option<WorkflowTaskTombstonePurgeEvidence>, WorkflowStoreError>,
    >;
}

fn invalid_input(message: &'static str) -> WorkflowStoreError {
    WorkflowStoreError::new(WorkflowStoreErrorKind::InvalidInput, message)
}
