//! Capability-scoped control plane for Task tombstone governance.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::Arc,
};

use runifold_core::CheckpointId;
use thiserror::Error;

use crate::{
    LeaseDuration, WorkerId, WorkflowStoreError, WorkflowTaskCleanupLease, WorkflowTaskLegalHold,
    WorkflowTaskLegalHoldReason, WorkflowTaskTombstone, WorkflowTaskTombstoneApprovalInboxItem,
    WorkflowTaskTombstoneApprovalInboxLimit, WorkflowTaskTombstoneApprovalLease,
    WorkflowTaskTombstoneApprovalWindow, WorkflowTaskTombstoneCursor,
    WorkflowTaskTombstoneExportReceipt, WorkflowTaskTombstoneGovernanceStore,
    WorkflowTaskTombstoneLimit, WorkflowTaskTombstonePurgeEvidence, WorkflowTaskTombstonePurgeId,
    WorkflowTaskTombstonePurgeIntent, WorkflowTaskTombstonePurgeLimit,
    WorkflowTaskTombstoneRejectionReason, WorkflowTaskTombstoneRetention, WorkflowTenantId,
};

/// One tenant-scoped tombstone governance authority.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum WorkflowTaskGovernancePermission {
    /// Place a legal hold.
    PlaceHold,
    /// Release a legal hold.
    ReleaseHold,
    /// Export tombstone details and confirm an archive receipt.
    Export,
    /// Prepare a bounded purge intent.
    PreparePurge,
    /// Independently approve a purge intent.
    ApprovePurge,
    /// Read the tenant's bounded approval inbox.
    ReadApprovalInbox,
    /// Claim one approval request for independent review.
    ClaimPurgeApproval,
    /// Reject a claimed purge request with a durable reason.
    RejectPurge,
    /// Execute an approved purge under a fenced lease.
    ExecutePurge,
    /// Read immutable purge evidence.
    ReadEvidence,
}

/// Low-cardinality governance outcome for telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowTaskGovernanceOutcome {
    /// The operation completed successfully.
    Succeeded,
    /// Policy rejected the principal.
    Denied,
    /// Authorization infrastructure failed closed.
    AuthorizationError,
    /// Durable store execution failed.
    StoreError,
    /// External archive execution failed.
    ArchiveError,
}

/// Optional observer that must not retain tenant or principal identities.
pub trait WorkflowTaskGovernanceObserver: Send + Sync {
    /// Observes one terminal control-plane outcome.
    fn observe(
        &self,
        permission: WorkflowTaskGovernancePermission,
        outcome: WorkflowTaskGovernanceOutcome,
    );
}

#[derive(Debug, Default)]
struct NoopWorkflowTaskGovernanceObserver;

impl WorkflowTaskGovernanceObserver for NoopWorkflowTaskGovernanceObserver {
    fn observe(
        &self,
        _permission: WorkflowTaskGovernancePermission,
        _outcome: WorkflowTaskGovernanceOutcome,
    ) {
    }
}

/// Safe authorizer failure. Control-plane operations fail closed.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("Task tombstone governance authorization failed: {message}")]
pub struct WorkflowTaskGovernanceAuthorizationError {
    message: String,
}

impl WorkflowTaskGovernanceAuthorizationError {
    /// Creates a bounded safe authorization failure.
    pub fn new(message: impl Into<String>) -> Self {
        let mut message = message.into();
        message.truncate(512);
        Self { message }
    }
}

/// Borrowing future returned by a governance authorizer.
pub type WorkflowTaskGovernanceAuthorizationFuture<'a> = Pin<
    Box<dyn Future<Output = Result<bool, WorkflowTaskGovernanceAuthorizationError>> + Send + 'a>,
>;

/// Pluggable policy boundary for principal, tenant, and permission checks.
pub trait WorkflowTaskGovernanceAuthorizer: Send + Sync {
    /// Returns `true` only for an explicit grant.
    fn authorize(
        &self,
        principal: &WorkerId,
        tenant_id: &WorkflowTenantId,
        permission: WorkflowTaskGovernancePermission,
    ) -> WorkflowTaskGovernanceAuthorizationFuture<'_>;
}

/// Immutable in-process authorizer for simple deployments and tests.
#[derive(Clone, Debug, Default)]
pub struct StaticWorkflowTaskGovernanceAuthorizer {
    grants: BTreeMap<(WorkerId, WorkflowTenantId), BTreeSet<WorkflowTaskGovernancePermission>>,
}

impl StaticWorkflowTaskGovernanceAuthorizer {
    /// Creates an empty deny-by-default policy.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one explicit principal and tenant grant.
    #[must_use]
    pub fn with_grant(
        mut self,
        principal: WorkerId,
        tenant_id: WorkflowTenantId,
        permission: WorkflowTaskGovernancePermission,
    ) -> Self {
        self.grants
            .entry((principal, tenant_id))
            .or_default()
            .insert(permission);
        self
    }
}

impl WorkflowTaskGovernanceAuthorizer for StaticWorkflowTaskGovernanceAuthorizer {
    fn authorize(
        &self,
        principal: &WorkerId,
        tenant_id: &WorkflowTenantId,
        permission: WorkflowTaskGovernancePermission,
    ) -> WorkflowTaskGovernanceAuthorizationFuture<'_> {
        let allowed = self
            .grants
            .get(&(principal.clone(), tenant_id.clone()))
            .is_some_and(|grants| grants.contains(&permission));
        Box::pin(async move { Ok(allowed) })
    }
}

/// Stable failure class for archive retry and observability policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowTaskTombstoneArchiveErrorKind {
    /// Archive configuration or local signing policy is invalid.
    Configuration,
    /// Remote credentials or policy rejected the request.
    Authorization,
    /// The bounded archive operation elapsed.
    Timeout,
    /// The remote archive is temporarily unavailable.
    Unavailable,
    /// A committed object did not match the stable batch payload.
    Integrity,
    /// Commit state could not be determined after reconciliation.
    Ambiguous,
    /// A safe fallback for adapters without a more specific classification.
    Other,
}

/// Safe archive adapter failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("Task tombstone archive failed ({kind:?}): {message}")]
pub struct WorkflowTaskTombstoneArchiveError {
    kind: WorkflowTaskTombstoneArchiveErrorKind,
    message: String,
}

impl WorkflowTaskTombstoneArchiveError {
    /// Creates a bounded safe archive failure with the fallback classification.
    pub fn new(message: impl Into<String>) -> Self {
        Self::with_kind(WorkflowTaskTombstoneArchiveErrorKind::Other, message)
    }

    /// Creates a bounded safe archive failure with a stable classification.
    pub fn with_kind(
        kind: WorkflowTaskTombstoneArchiveErrorKind,
        message: impl Into<String>,
    ) -> Self {
        let mut message = message.into();
        message.truncate(512);
        Self { kind, message }
    }

    /// Returns the stable failure class without exposing provider detail.
    pub const fn kind(&self) -> WorkflowTaskTombstoneArchiveErrorKind {
        self.kind
    }
}

/// Stable idempotency identity for one ordered archive batch.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct WorkflowTaskTombstoneArchiveBatchId(String);

impl WorkflowTaskTombstoneArchiveBatchId {
    /// Validates an externally restored stable batch identity.
    ///
    /// # Errors
    ///
    /// Rejects blank, oversized, or control-character-bearing values.
    pub fn parse(value: impl Into<String>) -> Result<Self, WorkflowStoreError> {
        let value = value.into();
        if value.trim().is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
            return Err(WorkflowStoreError::new(
                crate::WorkflowStoreErrorKind::InvalidInput,
                "Task tombstone archive batch ID must contain 1..=512 printable bytes",
            ));
        }
        Ok(Self(value))
    }

    fn from_batch(
        tenant_id: &WorkflowTenantId,
        first: WorkflowTaskTombstoneCursor,
        last: WorkflowTaskTombstoneCursor,
    ) -> Self {
        Self(format!(
            "{}:{}:{}",
            tenant_id.as_str(),
            first.get(),
            last.get()
        ))
    }

    /// Returns the stable archive idempotency key.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Exact ordered tombstone batch delivered to an external archive.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTaskTombstoneArchiveBatch {
    /// Stable idempotency key reused after partial failure.
    pub batch_id: WorkflowTaskTombstoneArchiveBatchId,
    /// Owning tenant.
    pub tenant_id: WorkflowTenantId,
    /// Detailed tombstones in ascending cursor order.
    pub tombstones: Vec<WorkflowTaskTombstone>,
}

/// Borrowing future returned by an external tombstone archive.
pub type WorkflowTaskTombstoneArchiveFuture<'a> = Pin<
    Box<
        dyn Future<
                Output = Result<
                    WorkflowTaskTombstoneExportReceipt,
                    WorkflowTaskTombstoneArchiveError,
                >,
            > + Send
            + 'a,
    >,
>;

/// Idempotent external archive boundary.
pub trait WorkflowTaskTombstoneArchive: Send + Sync {
    /// Stores the exact batch or replays its original receipt.
    fn archive(
        &self,
        batch: WorkflowTaskTombstoneArchiveBatch,
    ) -> WorkflowTaskTombstoneArchiveFuture<'_>;
}

/// Result of one bounded archive-and-confirm cycle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTaskTombstoneArchiveReport {
    /// Stable batch key, absent when the source page was empty.
    pub batch_id: Option<WorkflowTaskTombstoneArchiveBatchId>,
    /// Number of detailed tombstones archived.
    pub tombstones_exported: u32,
    /// Confirmed cursor, absent when the source page was empty.
    pub through: Option<WorkflowTaskTombstoneCursor>,
    /// Archive receipt persisted with the watermark.
    pub receipt: Option<WorkflowTaskTombstoneExportReceipt>,
}

/// Fail-closed tombstone governance failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum WorkflowTaskGovernanceError {
    /// The principal lacks the exact tenant-scoped permission.
    #[error("principal is not authorized for {permission:?}")]
    Denied {
        /// Rejected operation.
        permission: WorkflowTaskGovernancePermission,
    },
    /// The policy backend failed and the operation was not attempted.
    #[error(transparent)]
    Authorization(#[from] WorkflowTaskGovernanceAuthorizationError),
    /// Durable governance state failed.
    #[error(transparent)]
    Store(#[from] WorkflowStoreError),
    /// External archive failed before durable confirmation.
    #[error(transparent)]
    Archive(#[from] WorkflowTaskTombstoneArchiveError),
    /// A fenced lease was supplied by a different principal.
    #[error("cleanup lease owner does not match the authenticated principal")]
    LeasePrincipalMismatch,
    /// A reviewer lease was supplied by a different principal.
    #[error("approval lease reviewer does not match the authenticated principal")]
    ApprovalLeasePrincipalMismatch,
}

/// Authorized facade over the destructive tombstone governance store.
pub struct WorkflowTaskGovernanceControlPlane<S, A> {
    store: Arc<S>,
    authorizer: Arc<A>,
    observer: Arc<dyn WorkflowTaskGovernanceObserver>,
}

impl<S, A> WorkflowTaskGovernanceControlPlane<S, A>
where
    S: WorkflowTaskTombstoneGovernanceStore,
    A: WorkflowTaskGovernanceAuthorizer,
{
    /// Creates a fail-closed control plane.
    pub fn new(store: Arc<S>, authorizer: Arc<A>) -> Self {
        Self {
            store,
            authorizer,
            observer: Arc::new(NoopWorkflowTaskGovernanceObserver),
        }
    }

    /// Attaches a low-cardinality outcome observer.
    #[must_use]
    pub fn with_observer(mut self, observer: Arc<dyn WorkflowTaskGovernanceObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Places a hold using the authenticated principal as the audit actor.
    ///
    /// # Errors
    ///
    /// Fails closed on denied authority or store failure.
    pub async fn place_hold(
        &self,
        principal: &WorkerId,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        reason: WorkflowTaskLegalHoldReason,
    ) -> Result<WorkflowTaskLegalHold, WorkflowTaskGovernanceError> {
        let permission = WorkflowTaskGovernancePermission::PlaceHold;
        self.authorize(principal, &tenant_id, permission).await?;
        let result = self
            .store
            .place_task_tombstone_hold(tenant_id, checkpoint_id, principal.clone(), reason)
            .await;
        self.store_result(permission, result)
    }

    /// Releases a hold using the authenticated principal as the audit actor.
    ///
    /// # Errors
    ///
    /// Fails closed on denied authority or store failure.
    pub async fn release_hold(
        &self,
        principal: &WorkerId,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
    ) -> Result<WorkflowTaskLegalHold, WorkflowTaskGovernanceError> {
        let permission = WorkflowTaskGovernancePermission::ReleaseHold;
        self.authorize(principal, &tenant_id, permission).await?;
        let result = self
            .store
            .release_task_tombstone_hold(tenant_id, checkpoint_id, principal.clone())
            .await;
        self.store_result(permission, result)
    }

    /// Archives one page and confirms its watermark only after receipt.
    ///
    /// # Errors
    ///
    /// Fails closed on denied authority, source-store failure, archive
    /// failure, or durable confirmation failure.
    pub async fn export_next_page<R>(
        &self,
        principal: &WorkerId,
        tenant_id: WorkflowTenantId,
        after: Option<WorkflowTaskTombstoneCursor>,
        limit: WorkflowTaskTombstoneLimit,
        archive: &R,
    ) -> Result<WorkflowTaskTombstoneArchiveReport, WorkflowTaskGovernanceError>
    where
        R: WorkflowTaskTombstoneArchive,
    {
        let permission = WorkflowTaskGovernancePermission::Export;
        self.authorize(principal, &tenant_id, permission).await?;
        let tombstones = match self
            .store
            .list_task_tombstones(tenant_id.clone(), after, limit)
            .await
        {
            Ok(tombstones) => tombstones,
            Err(error) => return Err(self.store_error(permission, error)),
        };
        let (Some(first), Some(last)) = (tombstones.first(), tombstones.last()) else {
            self.observer
                .observe(permission, WorkflowTaskGovernanceOutcome::Succeeded);
            return Ok(WorkflowTaskTombstoneArchiveReport {
                batch_id: None,
                tombstones_exported: 0,
                through: None,
                receipt: None,
            });
        };
        let batch_id =
            WorkflowTaskTombstoneArchiveBatchId::from_batch(&tenant_id, first.cursor, last.cursor);
        let through = last.cursor;
        let count = u32::try_from(tombstones.len()).unwrap_or(u32::MAX);
        let receipt = match archive
            .archive(WorkflowTaskTombstoneArchiveBatch {
                batch_id: batch_id.clone(),
                tenant_id: tenant_id.clone(),
                tombstones,
            })
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => {
                self.observer
                    .observe(permission, WorkflowTaskGovernanceOutcome::ArchiveError);
                return Err(error.into());
            }
        };
        if let Err(error) = self
            .store
            .confirm_task_tombstone_export(tenant_id, through, receipt.clone(), principal.clone())
            .await
        {
            return Err(self.store_error(permission, error));
        }
        self.observer
            .observe(permission, WorkflowTaskGovernanceOutcome::Succeeded);
        Ok(WorkflowTaskTombstoneArchiveReport {
            batch_id: Some(batch_id),
            tombstones_exported: count,
            through: Some(through),
            receipt: Some(receipt),
        })
    }

    /// Prepares a purge only when the lease belongs to the principal.
    ///
    /// # Errors
    ///
    /// Rejects denied authority, a mismatched lease principal, or store
    /// failure.
    pub async fn prepare_purge(
        &self,
        principal: &WorkerId,
        lease: WorkflowTaskCleanupLease,
        retention: WorkflowTaskTombstoneRetention,
        limit: WorkflowTaskTombstonePurgeLimit,
        approval_window: WorkflowTaskTombstoneApprovalWindow,
    ) -> Result<WorkflowTaskTombstonePurgeIntent, WorkflowTaskGovernanceError> {
        let permission = WorkflowTaskGovernancePermission::PreparePurge;
        self.require_lease_principal(principal, &lease, permission)?;
        self.authorize(principal, &lease.tenant_id, permission)
            .await?;
        let result = self
            .store
            .prepare_task_tombstone_purge(lease, retention, limit, approval_window)
            .await;
        self.store_result(permission, result)
    }

    /// Approves using the authenticated principal, preserving four-eyes checks.
    ///
    /// # Errors
    ///
    /// Fails closed on denied authority or store governance failure.
    pub async fn approve_purge(
        &self,
        principal: &WorkerId,
        tenant_id: WorkflowTenantId,
        purge_id: WorkflowTaskTombstonePurgeId,
    ) -> Result<WorkflowTaskTombstonePurgeIntent, WorkflowTaskGovernanceError> {
        let permission = WorkflowTaskGovernancePermission::ApprovePurge;
        self.authorize(principal, &tenant_id, permission).await?;
        let result = self
            .store
            .approve_task_tombstone_purge(tenant_id, purge_id, principal.clone())
            .await;
        self.store_result(permission, result)
    }

    /// Lists the tenant's bounded durable approval inbox.
    ///
    /// # Errors
    ///
    /// Fails closed on denied authority or store failure.
    pub async fn list_purge_approvals(
        &self,
        principal: &WorkerId,
        tenant_id: WorkflowTenantId,
        limit: WorkflowTaskTombstoneApprovalInboxLimit,
    ) -> Result<Vec<WorkflowTaskTombstoneApprovalInboxItem>, WorkflowTaskGovernanceError> {
        let permission = WorkflowTaskGovernancePermission::ReadApprovalInbox;
        self.authorize(principal, &tenant_id, permission).await?;
        let result = self
            .store
            .list_task_tombstone_purge_approvals(tenant_id, limit)
            .await;
        self.store_result(permission, result)
    }

    /// Claims the oldest eligible approval using the authenticated reviewer.
    ///
    /// # Errors
    ///
    /// Fails closed on denied authority or store failure.
    pub async fn claim_purge_approval(
        &self,
        principal: &WorkerId,
        tenant_id: WorkflowTenantId,
        lease: LeaseDuration,
    ) -> Result<Option<WorkflowTaskTombstoneApprovalLease>, WorkflowTaskGovernanceError> {
        let permission = WorkflowTaskGovernancePermission::ClaimPurgeApproval;
        self.authorize(principal, &tenant_id, permission).await?;
        let result = self
            .store
            .claim_task_tombstone_purge_approval(tenant_id, principal.clone(), lease)
            .await;
        self.store_result(permission, result)
    }

    /// Approves an exact principal-owned reviewer lease.
    ///
    /// # Errors
    ///
    /// Rejects a mismatched reviewer, denied authority, or stale lease.
    pub async fn approve_claimed_purge(
        &self,
        principal: &WorkerId,
        lease: WorkflowTaskTombstoneApprovalLease,
    ) -> Result<WorkflowTaskTombstonePurgeIntent, WorkflowTaskGovernanceError> {
        let permission = WorkflowTaskGovernancePermission::ApprovePurge;
        self.require_approval_principal(principal, &lease, permission)?;
        self.authorize(principal, &lease.tenant_id, permission)
            .await?;
        let result = self.store.approve_claimed_task_tombstone_purge(lease).await;
        self.store_result(permission, result)
    }

    /// Rejects an exact principal-owned reviewer lease with durable evidence.
    ///
    /// # Errors
    ///
    /// Rejects a mismatched reviewer, denied authority, or stale lease.
    pub async fn reject_claimed_purge(
        &self,
        principal: &WorkerId,
        lease: WorkflowTaskTombstoneApprovalLease,
        reason: WorkflowTaskTombstoneRejectionReason,
    ) -> Result<WorkflowTaskTombstoneApprovalInboxItem, WorkflowTaskGovernanceError> {
        let permission = WorkflowTaskGovernancePermission::RejectPurge;
        self.require_approval_principal(principal, &lease, permission)?;
        self.authorize(principal, &lease.tenant_id, permission)
            .await?;
        let result = self
            .store
            .reject_claimed_task_tombstone_purge(lease, reason)
            .await;
        self.store_result(permission, result)
    }

    /// Executes using an exact principal-owned fenced lease.
    ///
    /// # Errors
    ///
    /// Rejects denied authority, a mismatched lease principal, or store
    /// execution failure.
    pub async fn execute_purge(
        &self,
        principal: &WorkerId,
        lease: WorkflowTaskCleanupLease,
        purge_id: WorkflowTaskTombstonePurgeId,
    ) -> Result<WorkflowTaskTombstonePurgeEvidence, WorkflowTaskGovernanceError> {
        let permission = WorkflowTaskGovernancePermission::ExecutePurge;
        self.require_lease_principal(principal, &lease, permission)?;
        self.authorize(principal, &lease.tenant_id, permission)
            .await?;
        let result = self
            .store
            .execute_task_tombstone_purge(lease, purge_id)
            .await;
        self.store_result(permission, result)
    }

    /// Reads evidence under an explicit tenant-scoped grant.
    ///
    /// # Errors
    ///
    /// Fails closed on denied authority or store failure.
    pub async fn get_evidence(
        &self,
        principal: &WorkerId,
        tenant_id: WorkflowTenantId,
        purge_id: WorkflowTaskTombstonePurgeId,
    ) -> Result<Option<WorkflowTaskTombstonePurgeEvidence>, WorkflowTaskGovernanceError> {
        let permission = WorkflowTaskGovernancePermission::ReadEvidence;
        self.authorize(principal, &tenant_id, permission).await?;
        let result = self
            .store
            .get_task_tombstone_purge_evidence(tenant_id, purge_id)
            .await;
        self.store_result(permission, result)
    }

    async fn authorize(
        &self,
        principal: &WorkerId,
        tenant_id: &WorkflowTenantId,
        permission: WorkflowTaskGovernancePermission,
    ) -> Result<(), WorkflowTaskGovernanceError> {
        match self
            .authorizer
            .authorize(principal, tenant_id, permission)
            .await
        {
            Ok(true) => Ok(()),
            Ok(false) => {
                self.observer
                    .observe(permission, WorkflowTaskGovernanceOutcome::Denied);
                Err(WorkflowTaskGovernanceError::Denied { permission })
            }
            Err(error) => {
                self.observer.observe(
                    permission,
                    WorkflowTaskGovernanceOutcome::AuthorizationError,
                );
                Err(error.into())
            }
        }
    }

    fn require_lease_principal(
        &self,
        principal: &WorkerId,
        lease: &WorkflowTaskCleanupLease,
        permission: WorkflowTaskGovernancePermission,
    ) -> Result<(), WorkflowTaskGovernanceError> {
        if lease.owner == *principal {
            Ok(())
        } else {
            self.observer
                .observe(permission, WorkflowTaskGovernanceOutcome::Denied);
            Err(WorkflowTaskGovernanceError::LeasePrincipalMismatch)
        }
    }

    fn require_approval_principal(
        &self,
        principal: &WorkerId,
        lease: &WorkflowTaskTombstoneApprovalLease,
        permission: WorkflowTaskGovernancePermission,
    ) -> Result<(), WorkflowTaskGovernanceError> {
        if lease.reviewer == *principal {
            Ok(())
        } else {
            self.observer
                .observe(permission, WorkflowTaskGovernanceOutcome::Denied);
            Err(WorkflowTaskGovernanceError::ApprovalLeasePrincipalMismatch)
        }
    }

    fn store_result<T>(
        &self,
        permission: WorkflowTaskGovernancePermission,
        result: Result<T, WorkflowStoreError>,
    ) -> Result<T, WorkflowTaskGovernanceError> {
        match result {
            Ok(value) => {
                self.observer
                    .observe(permission, WorkflowTaskGovernanceOutcome::Succeeded);
                Ok(value)
            }
            Err(error) => Err(self.store_error(permission, error)),
        }
    }

    fn store_error(
        &self,
        permission: WorkflowTaskGovernancePermission,
        error: WorkflowStoreError,
    ) -> WorkflowTaskGovernanceError {
        self.observer
            .observe(permission, WorkflowTaskGovernanceOutcome::StoreError);
        error.into()
    }
}

impl<S, A> std::fmt::Debug for WorkflowTaskGovernanceControlPlane<S, A> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowTaskGovernanceControlPlane")
            .field("store", &"<governance-store>")
            .field("authorizer", &"<governance-authorizer>")
            .field("observer", &"<governance-observer>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{WorkflowTaskTombstoneArchiveError, WorkflowTaskTombstoneArchiveErrorKind};

    #[test]
    fn archive_error_preserves_kind_and_bounds_safe_message() {
        let error = WorkflowTaskTombstoneArchiveError::with_kind(
            WorkflowTaskTombstoneArchiveErrorKind::Ambiguous,
            "x".repeat(1_024),
        );

        assert_eq!(
            error.kind(),
            WorkflowTaskTombstoneArchiveErrorKind::Ambiguous
        );
        assert!(error.to_string().len() < 600);
        assert_eq!(
            WorkflowTaskTombstoneArchiveError::new("fallback").kind(),
            WorkflowTaskTombstoneArchiveErrorKind::Other
        );
    }
}
