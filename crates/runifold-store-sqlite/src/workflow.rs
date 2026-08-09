//! SQLite-backed durable workflow control plane.

mod schema;

use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_executor::block_on;
use runifold_core::{
    Budget, Checkpoint, CheckpointError, CheckpointErrorKind, CheckpointId, Usage,
};
use runifold_workflow::{
    ClaimedWorkflow, InMemoryWorkflowStore, LeaseDuration, WorkerId, WorkflowBudgetAuditCursor,
    WorkflowBudgetAuditEvent, WorkflowBudgetAuditLimit, WorkflowBudgetAuditProjectionId,
    WorkflowBudgetAuditProjectionLease, WorkflowBudgetReservationOutcome, WorkflowCancelOutcome,
    WorkflowCheckpointHistoryLimit, WorkflowCheckpointRevision, WorkflowClock, WorkflowDisposition,
    WorkflowForkCommand, WorkflowForkOutcome, WorkflowLease, WorkflowSignal, WorkflowSignalId,
    WorkflowSignalOutcome, WorkflowSignalRetention, WorkflowSignalSnapshot, WorkflowStore,
    WorkflowStoreError, WorkflowStoreErrorKind, WorkflowStoreFuture, WorkflowTask,
    WorkflowTaskSnapshot, WorkflowTenantBudgetPolicy, WorkflowTenantBudgetSnapshot,
    WorkflowTenantId, WorkflowTenantListLimit, WorkflowTenantPolicy,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use thiserror::Error;

use self::schema::{SCHEMA, SNAPSHOT_FORMAT_VERSION};

/// Failure while opening or initializing a `SQLite` workflow store.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SqliteWorkflowStoreError {
    /// `SQLite` rejected connection or schema initialization.
    #[error("sqlite workflow store initialization failed: {0}")]
    Database(#[from] rusqlite::Error),
}

/// Durable local implementation of Runifold's complete workflow control plane.
///
/// Operations execute the shared workflow state machine inside an immediate
/// `SQLite` transaction. This deliberately serializes writers: `SQLite` is the
/// local and edge adapter, while `PostgreSQL` remains the horizontally scaled
/// coordination backend.
#[derive(Clone)]
pub struct SqliteWorkflowStore {
    connection: Arc<Mutex<Connection>>,
}

impl SqliteWorkflowStore {
    /// Opens or creates a file-backed workflow store.
    ///
    /// # Errors
    ///
    /// Returns an error when `SQLite` cannot open the database or initialize the
    /// workflow snapshot schema.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SqliteWorkflowStoreError> {
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        Self::from_connection(connection)
    }

    /// Creates a process-local workflow store, primarily for tests.
    ///
    /// # Errors
    ///
    /// Returns an error when `SQLite` initialization fails.
    pub fn open_in_memory() -> Result<Self, SqliteWorkflowStoreError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, SqliteWorkflowStoreError> {
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.execute_batch(SCHEMA)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    fn execute<T, F>(&self, operation: F) -> WorkflowStoreFuture<'_, Result<T, WorkflowStoreError>>
    where
        T: Send + 'static,
        F: FnOnce(&InMemoryWorkflowStore) -> Result<T, WorkflowStoreError> + Send + 'static,
    {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
                WorkflowStoreError::new(
                    WorkflowStoreErrorKind::Storage,
                    "SQLite workflow operations require a Tokio runtime",
                )
            })?;
            runtime
                .spawn_blocking(move || execute_transaction(&connection, operation))
                .await
                .map_err(|error| storage_error(format!("SQLite workflow task failed: {error}")))?
        })
    }
}

impl std::fmt::Debug for SqliteWorkflowStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteWorkflowStore")
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug)]
struct FixedClock(u64);

impl WorkflowClock for FixedClock {
    fn now_ms(&self) -> u64 {
        self.0
    }
}

fn execute_transaction<T, F>(
    connection: &Mutex<Connection>,
    operation: F,
) -> Result<T, WorkflowStoreError>
where
    F: FnOnce(&InMemoryWorkflowStore) -> Result<T, WorkflowStoreError>,
{
    let mut connection = connection
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| database_error(&error))?;
    let now = database_now_ms(&transaction)?;
    let state = load_state(&transaction, now)?;
    let output = operation(&state)?;
    save_state(&transaction, &state, now)?;
    transaction
        .commit()
        .map_err(|error| database_error(&error))?;
    Ok(output)
}

fn database_now_ms(transaction: &Transaction<'_>) -> Result<u64, WorkflowStoreError> {
    let value = transaction
        .query_row(
            "SELECT CAST(strftime('%s', 'now') AS INTEGER) * 1000
                    + CAST(substr(strftime('%f', 'now'), 4, 3) AS INTEGER)",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|error| database_error(&error))?;
    u64::try_from(value).map_err(|_| storage_error("SQLite returned a negative workflow clock"))
}

fn load_state(
    transaction: &Transaction<'_>,
    now: u64,
) -> Result<InMemoryWorkflowStore, WorkflowStoreError> {
    let stored = transaction
        .query_row(
            "SELECT format_version, state_blob
             FROM runifold_workflow_state
             WHERE singleton_id = 1",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()
        .map_err(|error| database_error(&error))?;
    let clock: Arc<dyn WorkflowClock> = Arc::new(FixedClock(now));
    match stored {
        Some((format_version, encoded)) if format_version == SNAPSHOT_FORMAT_VERSION => {
            InMemoryWorkflowStore::from_persistent_snapshot(&encoded, clock)
        }
        Some((format_version, _)) => Err(storage_error(format!(
            "unsupported SQLite workflow state format version {format_version}"
        ))),
        None => Ok(InMemoryWorkflowStore::with_clock(clock)),
    }
}

fn save_state(
    transaction: &Transaction<'_>,
    state: &InMemoryWorkflowStore,
    now: u64,
) -> Result<(), WorkflowStoreError> {
    let encoded = state.export_persistent_snapshot()?;
    let now = i64::try_from(now)
        .map_err(|_| storage_error("workflow clock exceeds SQLite integer range"))?;
    transaction
        .execute(
            "INSERT INTO runifold_workflow_state (
                 singleton_id, format_version, state_blob, updated_at_ms
             ) VALUES (1, ?1, ?2, ?3)
             ON CONFLICT(singleton_id) DO UPDATE SET
                 format_version = excluded.format_version,
                 state_blob = excluded.state_blob,
                 updated_at_ms = excluded.updated_at_ms",
            params![SNAPSHOT_FORMAT_VERSION, encoded, now],
        )
        .map_err(|error| database_error(&error))?;
    Ok(())
}

fn database_error(error: &rusqlite::Error) -> WorkflowStoreError {
    storage_error(format!("SQLite workflow operation failed: {error}"))
}

fn storage_error(message: impl Into<String>) -> WorkflowStoreError {
    WorkflowStoreError::new(WorkflowStoreErrorKind::Storage, message)
}

fn checkpoint_to_workflow(error: CheckpointError) -> WorkflowStoreError {
    let kind = match error.kind {
        CheckpointErrorKind::NotFound => WorkflowStoreErrorKind::NotFound,
        CheckpointErrorKind::Conflict => WorkflowStoreErrorKind::Conflict,
        CheckpointErrorKind::InvalidPayload => WorkflowStoreErrorKind::InvalidInput,
        _ => WorkflowStoreErrorKind::Storage,
    };
    WorkflowStoreError::new(kind, error.message)
}

fn workflow_to_checkpoint(error: WorkflowStoreError) -> CheckpointError {
    let kind = match error.kind {
        WorkflowStoreErrorKind::NotFound => CheckpointErrorKind::NotFound,
        WorkflowStoreErrorKind::Conflict
        | WorkflowStoreErrorKind::LeaseLost
        | WorkflowStoreErrorKind::AdmissionDenied
        | WorkflowStoreErrorKind::TenantMismatch => CheckpointErrorKind::Conflict,
        WorkflowStoreErrorKind::InvalidInput => CheckpointErrorKind::InvalidPayload,
        _ => CheckpointErrorKind::Storage,
    };
    CheckpointError::new(kind, error.message)
}

impl WorkflowStore for SqliteWorkflowStore {
    fn current_time_ms(&self) -> WorkflowStoreFuture<'_, Result<u64, WorkflowStoreError>> {
        self.execute(|store| block_on(store.current_time_ms()))
    }

    fn set_tenant_policy(
        &self,
        tenant_id: WorkflowTenantId,
        policy: WorkflowTenantPolicy,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        self.execute(move |store| block_on(store.set_tenant_policy(tenant_id, policy)))
    }

    fn set_tenant_budget_policy(
        &self,
        tenant_id: WorkflowTenantId,
        policy: WorkflowTenantBudgetPolicy,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        self.execute(move |store| block_on(store.set_tenant_budget_policy(tenant_id, policy)))
    }

    fn list_tenant_budgets(
        &self,
        after: Option<WorkflowTenantId>,
        limit: WorkflowTenantListLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowTenantId>, WorkflowStoreError>> {
        self.execute(move |store| block_on(store.list_tenant_budgets(after, limit)))
    }

    fn inspect_tenant_budget(
        &self,
        tenant_id: WorkflowTenantId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTenantBudgetSnapshot, WorkflowStoreError>> {
        self.execute(move |store| block_on(store.inspect_tenant_budget(tenant_id)))
    }

    fn list_tenant_budget_audit(
        &self,
        tenant_id: WorkflowTenantId,
        after: Option<WorkflowBudgetAuditCursor>,
        limit: WorkflowBudgetAuditLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowBudgetAuditEvent>, WorkflowStoreError>> {
        self.execute(move |store| block_on(store.list_tenant_budget_audit(tenant_id, after, limit)))
    }

    fn compact_tenant_budget_audit(
        &self,
        tenant_id: WorkflowTenantId,
        through: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<u64, WorkflowStoreError>> {
        self.execute(move |store| block_on(store.compact_tenant_budget_audit(tenant_id, through)))
    }

    fn load_or_create_tenant_budget_audit_projection(
        &self,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditCursor, WorkflowStoreError>> {
        self.execute(move |store| {
            block_on(store.load_or_create_tenant_budget_audit_projection(tenant_id, projection_id))
        })
    }

    fn advance_tenant_budget_audit_projection(
        &self,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
        expected: WorkflowBudgetAuditCursor,
        next: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<bool, WorkflowStoreError>> {
        self.execute(move |store| {
            block_on(store.advance_tenant_budget_audit_projection(
                tenant_id,
                projection_id,
                expected,
                next,
            ))
        })
    }

    fn claim_tenant_budget_audit_projection(
        &self,
        tenant_id: WorkflowTenantId,
        projection_id: WorkflowBudgetAuditProjectionId,
        owner: WorkerId,
        lease: LeaseDuration,
    ) -> WorkflowStoreFuture<
        '_,
        Result<Option<WorkflowBudgetAuditProjectionLease>, WorkflowStoreError>,
    > {
        self.execute(move |store| {
            block_on(store.claim_tenant_budget_audit_projection(
                tenant_id,
                projection_id,
                owner,
                lease,
            ))
        })
    }

    fn heartbeat_tenant_budget_audit_projection(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
        extension: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditProjectionLease, WorkflowStoreError>>
    {
        self.execute(move |store| {
            block_on(store.heartbeat_tenant_budget_audit_projection(lease, extension))
        })
    }

    fn advance_tenant_budget_audit_projection_lease(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
        next: WorkflowBudgetAuditCursor,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetAuditProjectionLease, WorkflowStoreError>>
    {
        self.execute(move |store| {
            block_on(store.advance_tenant_budget_audit_projection_lease(lease, next))
        })
    }

    fn release_tenant_budget_audit_projection(
        &self,
        lease: WorkflowBudgetAuditProjectionLease,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        self.execute(move |store| block_on(store.release_tenant_budget_audit_projection(lease)))
    }

    fn reserve_budget(
        &self,
        lease: WorkflowLease,
        workflow_limit: Budget,
        baseline: Usage,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowBudgetReservationOutcome, WorkflowStoreError>> {
        self.execute(move |store| block_on(store.reserve_budget(lease, workflow_limit, baseline)))
    }

    fn settle_budget(
        &self,
        lease: WorkflowLease,
        cumulative: Usage,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        self.execute(move |store| block_on(store.settle_budget(lease, cumulative)))
    }

    fn enqueue(
        &self,
        task: WorkflowTask,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        self.execute(move |store| block_on(store.enqueue(task)))
    }

    fn claim(
        &self,
        worker: WorkerId,
        lease: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<Option<ClaimedWorkflow>, WorkflowStoreError>> {
        self.execute(move |store| block_on(store.claim(worker, lease)))
    }

    fn heartbeat(
        &self,
        lease: WorkflowLease,
        extension: LeaseDuration,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowLease, WorkflowStoreError>> {
        self.execute(move |store| block_on(store.heartbeat(lease, extension)))
    }

    fn finish(
        &self,
        lease: WorkflowLease,
        disposition: WorkflowDisposition,
    ) -> WorkflowStoreFuture<'_, Result<(), WorkflowStoreError>> {
        self.execute(move |store| block_on(store.finish(lease, disposition)))
    }

    fn publish_signal(
        &self,
        tenant_id: WorkflowTenantId,
        signal: WorkflowSignal,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowSignalOutcome, WorkflowStoreError>> {
        self.execute(move |store| block_on(store.publish_signal(tenant_id, signal)))
    }

    fn publish_control_signal(
        &self,
        tenant_id: WorkflowTenantId,
        signal: WorkflowSignal,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowSignalOutcome, WorkflowStoreError>> {
        self.execute(move |store| block_on(store.publish_control_signal(tenant_id, signal)))
    }

    fn cancel(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowCancelOutcome, WorkflowStoreError>> {
        self.execute(move |store| block_on(store.cancel(tenant_id, checkpoint_id)))
    }

    fn inspect_signal(
        &self,
        tenant_id: WorkflowTenantId,
        signal_id: WorkflowSignalId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowSignalSnapshot, WorkflowStoreError>> {
        self.execute(move |store| block_on(store.inspect_signal(tenant_id, signal_id)))
    }

    fn load_signal_payload(
        &self,
        tenant_id: WorkflowTenantId,
        signal_id: WorkflowSignalId,
    ) -> WorkflowStoreFuture<'_, Result<serde_json::Value, WorkflowStoreError>> {
        self.execute(move |store| block_on(store.load_signal_payload(tenant_id, signal_id)))
    }

    fn compact_signals(
        &self,
        tenant_id: WorkflowTenantId,
        retention: WorkflowSignalRetention,
    ) -> WorkflowStoreFuture<'_, Result<u64, WorkflowStoreError>> {
        self.execute(move |store| block_on(store.compact_signals(tenant_id, retention)))
    }

    fn inspect(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowTaskSnapshot, WorkflowStoreError>> {
        self.execute(move |store| block_on(store.inspect(tenant_id, checkpoint_id)))
    }

    fn load_task_input(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
    ) -> WorkflowStoreFuture<'_, Result<serde_json::Value, WorkflowStoreError>> {
        self.execute(move |store| block_on(store.load_task_input(tenant_id, checkpoint_id)))
    }

    fn list_checkpoint_history(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        after_revision: Option<u64>,
        limit: WorkflowCheckpointHistoryLimit,
    ) -> WorkflowStoreFuture<'_, Result<Vec<WorkflowCheckpointRevision>, WorkflowStoreError>> {
        self.execute(move |store| {
            block_on(store.list_checkpoint_history(tenant_id, checkpoint_id, after_revision, limit))
        })
    }

    fn load_checkpoint_revision(
        &self,
        tenant_id: WorkflowTenantId,
        checkpoint_id: CheckpointId,
        revision: u64,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowCheckpointRevision, WorkflowStoreError>> {
        self.execute(move |store| {
            block_on(store.load_checkpoint_revision(tenant_id, checkpoint_id, revision))
        })
    }

    fn fork_workflow(
        &self,
        tenant_id: WorkflowTenantId,
        command: WorkflowForkCommand,
    ) -> WorkflowStoreFuture<'_, Result<WorkflowForkOutcome, WorkflowStoreError>> {
        self.execute(move |store| block_on(store.fork_workflow(tenant_id, command)))
    }

    fn load_checkpoint(
        &self,
        lease: WorkflowLease,
    ) -> WorkflowStoreFuture<'_, Result<Checkpoint, CheckpointError>> {
        let future = self.execute(move |store| {
            block_on(store.load_checkpoint(lease)).map_err(checkpoint_to_workflow)
        });
        Box::pin(async move { future.await.map_err(workflow_to_checkpoint) })
    }

    fn compare_and_swap_checkpoint(
        &self,
        lease: WorkflowLease,
        checkpoint: Checkpoint,
        expected_revision: Option<u64>,
    ) -> WorkflowStoreFuture<'_, Result<(), CheckpointError>> {
        let future = self.execute(move |store| {
            block_on(store.compare_and_swap_checkpoint(lease, checkpoint, expected_revision))
                .map_err(checkpoint_to_workflow)
        });
        Box::pin(async move { future.await.map_err(workflow_to_checkpoint) })
    }
}
