//! MCP Task fault injection against a disposable `PostgreSQL` workflow store.

mod support;

use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::StreamExt;
use runifold_core::{Budget, BudgetTracker, CapabilitySet, RunContext};
use runifold_mcp::{
    Implementation, McpClient, McpClientConfig, McpError, McpServer, McpTaskBackend, TaskStatus,
    ToolTaskRequest, WorkflowTaskAdapter, WorkflowTaskRoute,
};
use runifold_store_postgres::PostgresWorkflowStore;
use runifold_tool::ToolRegistry;
use runifold_workflow::{
    LeaseDuration, StaticWorkflowTaskGovernanceAuthorizer, WorkerId, WorkflowStore,
    WorkflowStoreErrorKind, WorkflowTaskCleanupLease, WorkflowTaskCleanupLimit,
    WorkflowTaskCleanupShard, WorkflowTaskCleanupSupervisor, WorkflowTaskCleanupSupervisorConfig,
    WorkflowTaskGovernanceAuthorizationError, WorkflowTaskGovernanceAuthorizationFuture,
    WorkflowTaskGovernanceAuthorizer, WorkflowTaskGovernanceControlPlane,
    WorkflowTaskGovernanceError, WorkflowTaskGovernancePermission, WorkflowTaskLegalHoldReason,
    WorkflowTaskRetention, WorkflowTaskRetentionStore, WorkflowTaskStatus, WorkflowTaskTombstone,
    WorkflowTaskTombstoneApprovalInboxLimit, WorkflowTaskTombstoneApprovalState,
    WorkflowTaskTombstoneApprovalWindow, WorkflowTaskTombstoneArchive,
    WorkflowTaskTombstoneArchiveBatch, WorkflowTaskTombstoneArchiveFuture,
    WorkflowTaskTombstoneExportReceipt, WorkflowTaskTombstoneGovernanceStore,
    WorkflowTaskTombstoneLimit, WorkflowTaskTombstonePurgeIntent, WorkflowTaskTombstonePurgeLimit,
    WorkflowTaskTombstoneRejectionReason, WorkflowTaskTombstoneRetention, WorkflowTenantId,
    WorkflowTenantListLimit,
};
use serde_json::json;
use tokio::time::timeout;
use uuid::Uuid;

pub use support::PostgresTestContext;

const TOOL_NAME: &str = "postgres_task";
const WORKFLOW_NAME: &str = "postgres-task-workflow";

#[tokio::test]
async fn task_survives_database_restart_and_concurrent_subscribers_converge() {
    let database = PostgresTestContext::isolated().await;
    let table = format!("runifold_mcp_{}", Uuid::now_v7().simple());
    let tenant = WorkflowTenantId::parse("mcp.production-test").unwrap();
    let store = Arc::new(
        PostgresWorkflowStore::connect(database.connection_url(), &table)
            .await
            .unwrap(),
    );
    store.ensure_schema().await.unwrap();

    let adapter = task_adapter(store, tenant.clone());
    let task = adapter
        .create_tool_task(ToolTaskRequest {
            name: TOOL_NAME.into(),
            arguments: serde_json::Map::from_iter([("value".into(), json!(7))]),
            context: root_context(),
        })
        .await
        .unwrap();
    assert_eq!(task.status, TaskStatus::Working);
    let client = task_client(task_server(adapter)).await;
    assert_eq!(client.get_task(&task.task_id).await.unwrap(), task);

    database.stop().await;
    let outage = timeout(Duration::from_secs(5), client.get_task(&task.task_id))
        .await
        .expect("database outage must surface within the bounded deadline")
        .unwrap_err();
    assert!(
        matches!(outage, McpError::Remote { code: -32603, .. }),
        "{outage:?}"
    );

    let recovered_url = database.restart().await;
    let recovered_store = Arc::new(
        PostgresWorkflowStore::connect(&recovered_url, &table)
            .await
            .unwrap(),
    );
    let control_store = recovered_store.clone();
    let recovered = task_client(task_server(task_adapter(recovered_store, tenant.clone()))).await;
    assert_eq!(
        recovered.get_task(&task.task_id).await.unwrap().status,
        TaskStatus::Working
    );
    assert_active_task_is_protected(&control_store, tenant.clone()).await;
    assert_concurrent_subscribers_converge(&recovered, &task.task_id).await;
    assert_fenced_terminal_cleanup(&control_store, &recovered, tenant, &task.task_id).await;
    assert_supervisor_discovers_and_cleans_tenants(&control_store).await;
    assert_authorized_governance_control_plane(&control_store).await;
}

async fn assert_active_task_is_protected(store: &PostgresWorkflowStore, tenant: WorkflowTenantId) {
    let cleanup_limit = WorkflowTaskCleanupLimit::new(10).unwrap();
    let cleanup_retention = WorkflowTaskRetention::new(Duration::from_millis(1)).unwrap();
    let active_probe = store
        .claim_task_cleanup(
            tenant.clone(),
            WorkerId::parse("cleanup-probe").unwrap(),
            LeaseDuration::new(Duration::from_secs(1)).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .compact_terminal_tasks(active_probe.clone(), cleanup_retention, cleanup_limit)
            .await
            .unwrap()
            .is_empty(),
        "active workflow Tasks must never be physically compacted"
    );
    store.release_task_cleanup(active_probe).await.unwrap();
}

async fn assert_concurrent_subscribers_converge(client: &McpClient, task_id: &str) {
    let mut subscriptions = Vec::new();
    for _ in 0..8 {
        subscriptions.push(client.listen_tasks([task_id]).await.unwrap());
    }
    for subscription in &mut subscriptions {
        let snapshot = timeout(Duration::from_secs(2), subscription.next())
            .await
            .expect("initial durable Task snapshot timed out")
            .expect("Task subscription closed")
            .unwrap();
        assert_eq!(snapshot.status, TaskStatus::Working);
    }

    client.cancel_task(task_id).await.unwrap();
    client.cancel_task(task_id).await.unwrap();
    for subscription in &mut subscriptions {
        let snapshot = timeout(Duration::from_secs(2), subscription.next())
            .await
            .expect("cancelled durable Task snapshot timed out")
            .expect("Task subscription closed")
            .unwrap();
        assert_eq!(snapshot.status, TaskStatus::Cancelled);
    }
    assert_eq!(
        client.get_task(task_id).await.unwrap().status,
        TaskStatus::Cancelled
    );
}

async fn assert_fenced_terminal_cleanup(
    store: &PostgresWorkflowStore,
    client: &McpClient,
    tenant: WorkflowTenantId,
    task_id: &str,
) {
    let cleanup_limit = WorkflowTaskCleanupLimit::new(10).unwrap();
    let cleanup_retention = WorkflowTaskRetention::new(Duration::from_millis(1)).unwrap();
    let short_lease = LeaseDuration::new(Duration::from_millis(40)).unwrap();
    let heartbeat_probe = store
        .claim_task_cleanup(
            tenant.clone(),
            WorkerId::parse("cleanup-heartbeat").unwrap(),
            short_lease,
        )
        .await
        .unwrap()
        .unwrap();
    let renewed = store
        .heartbeat_task_cleanup(
            heartbeat_probe,
            LeaseDuration::new(Duration::from_secs(1)).unwrap(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(45)).await;
    assert!(
        store
            .claim_task_cleanup(
                tenant.clone(),
                WorkerId::parse("cleanup-blocked").unwrap(),
                short_lease,
            )
            .await
            .unwrap()
            .is_none(),
        "database-clock heartbeat must prevent premature takeover"
    );
    store.release_task_cleanup(renewed).await.unwrap();

    let first_lease = store
        .claim_task_cleanup(
            tenant.clone(),
            WorkerId::parse("cleanup-a").unwrap(),
            short_lease,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        store
            .claim_task_cleanup(
                tenant.clone(),
                WorkerId::parse("cleanup-b").unwrap(),
                short_lease,
            )
            .await
            .unwrap()
            .is_none()
    );
    tokio::time::sleep(Duration::from_millis(45)).await;
    let takeover = store
        .claim_task_cleanup(
            tenant.clone(),
            WorkerId::parse("cleanup-b").unwrap(),
            LeaseDuration::new(Duration::from_secs(1)).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(takeover.fencing_token, first_lease.fencing_token + 1);
    let stale = store
        .compact_terminal_tasks(first_lease, cleanup_retention, cleanup_limit)
        .await
        .unwrap_err();
    assert_eq!(stale.kind, WorkflowStoreErrorKind::LeaseLost);

    let tombstones = store
        .compact_terminal_tasks(takeover.clone(), cleanup_retention, cleanup_limit)
        .await
        .unwrap();
    assert_eq!(tombstones.len(), 1);
    assert_eq!(tombstones[0].checkpoint_id.to_string(), task_id);
    assert_eq!(tombstones[0].final_status, WorkflowTaskStatus::Cancelled);
    assert!(
        store
            .compact_terminal_tasks(takeover.clone(), cleanup_retention, cleanup_limit)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .list_task_tombstones(tenant, None, WorkflowTaskTombstoneLimit::new(10).unwrap(),)
            .await
            .unwrap(),
        tombstones
    );
    store.release_task_cleanup(takeover).await.unwrap();
    assert!(matches!(
        client.get_task(task_id).await,
        Err(McpError::Remote { code: -32602, .. })
    ));
}

async fn assert_supervisor_discovers_and_cleans_tenants(store: &Arc<PostgresWorkflowStore>) {
    let tenants = ["cleanup-a", "cleanup-b", "cleanup-c"]
        .into_iter()
        .map(|name| WorkflowTenantId::parse(name).unwrap())
        .collect::<Vec<_>>();
    for (index, tenant) in tenants.iter().enumerate() {
        let task = runifold_workflow::WorkflowTask::new("retention-supervisor", 1, json!(null))
            .unwrap()
            .with_tenant(tenant.clone());
        let checkpoint_id = task.checkpoint_id;
        store.enqueue(task).await.unwrap();
        store.cancel(tenant.clone(), checkpoint_id).await.unwrap();
        if index == 0 {
            let second =
                runifold_workflow::WorkflowTask::new("retention-governance", 1, json!(null))
                    .unwrap()
                    .with_tenant(tenant.clone());
            let second_id = second.checkpoint_id;
            store.enqueue(second).await.unwrap();
            store.cancel(tenant.clone(), second_id).await.unwrap();
        }
    }
    assert_eq!(
        store
            .list_task_cleanup_tenants(None, WorkflowTenantListLimit::new(10).unwrap())
            .await
            .unwrap(),
        tenants
    );
    tokio::time::sleep(Duration::from_millis(2)).await;
    let shard = WorkflowTaskCleanupShard::new(0, NonZeroU32::new(1).unwrap()).unwrap();
    let config = WorkflowTaskCleanupSupervisorConfig::new(
        shard,
        WorkflowTaskRetention::new(Duration::from_millis(1)).unwrap(),
        LeaseDuration::new(Duration::from_secs(1)).unwrap(),
        Duration::from_millis(100),
    )
    .unwrap()
    .with_work_limits(
        WorkflowTenantListLimit::new(2).unwrap(),
        NonZeroUsize::new(2).unwrap(),
        WorkflowTaskCleanupLimit::new(1).unwrap(),
        NonZeroU32::new(2).unwrap(),
    );
    let supervisor = WorkflowTaskCleanupSupervisor::new(
        Arc::clone(store),
        WorkerId::parse("cleanup-supervisor").unwrap(),
        config,
    );
    let report = supervisor.scan_once().await.unwrap();
    assert_eq!(report.scans, 1);
    assert_eq!(report.tenants_discovered, 3);
    assert_eq!(report.tenants_assigned, 3);
    assert_eq!(report.claims, 3);
    assert_eq!(report.tasks_deleted, 4);
    assert_eq!(report.infrastructure_errors, 0);
    assert_eq!(supervisor.metrics().snapshot().tasks_deleted, 4);
    assert!(
        store
            .list_task_cleanup_tenants(None, WorkflowTenantListLimit::new(10).unwrap())
            .await
            .unwrap()
            .is_empty()
    );
    assert_tombstone_governance(store, tenants[0].clone()).await;
}

async fn assert_tombstone_governance(store: &Arc<PostgresWorkflowStore>, tenant: WorkflowTenantId) {
    let fixture = prepare_governance_intent(store, tenant.clone()).await;
    approve_and_execute_governed_purge(store, tenant, fixture).await;
}

struct GovernanceFixture {
    lease: WorkflowTaskCleanupLease,
    tombstones: Vec<WorkflowTaskTombstone>,
    intent: WorkflowTaskTombstonePurgeIntent,
}

async fn prepare_governance_intent(
    store: &PostgresWorkflowStore,
    tenant: WorkflowTenantId,
) -> GovernanceFixture {
    let tombstones = store
        .list_task_tombstones(
            tenant.clone(),
            None,
            WorkflowTaskTombstoneLimit::new(10).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(tombstones.len(), 2);
    let lease = store
        .claim_task_cleanup(
            tenant.clone(),
            WorkerId::parse("purge-preparer").unwrap(),
            LeaseDuration::new(Duration::from_secs(2)).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let no_export = store
        .prepare_task_tombstone_purge(
            lease.clone(),
            WorkflowTaskTombstoneRetention::new(Duration::from_millis(1)).unwrap(),
            WorkflowTaskTombstonePurgeLimit::new(10).unwrap(),
            WorkflowTaskTombstoneApprovalWindow::new(Duration::from_secs(1)).unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        no_export.kind,
        WorkflowStoreErrorKind::Conflict,
        "{no_export:?}"
    );

    let first_hold = store
        .place_task_tombstone_hold(
            tenant.clone(),
            tombstones[0].checkpoint_id,
            WorkerId::parse("legal").unwrap(),
            WorkflowTaskLegalHoldReason::parse("regulatory-review").unwrap(),
        )
        .await
        .unwrap();
    assert!(first_hold.is_active());
    let export = store
        .confirm_task_tombstone_export(
            tenant.clone(),
            tombstones[1].cursor,
            WorkflowTaskTombstoneExportReceipt::parse("archive://receipt/1").unwrap(),
            WorkerId::parse("exporter").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(export.through, tombstones[1].cursor);
    let rollback = store
        .confirm_task_tombstone_export(
            tenant.clone(),
            tombstones[0].cursor,
            WorkflowTaskTombstoneExportReceipt::parse("archive://receipt/rollback").unwrap(),
            WorkerId::parse("exporter").unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(rollback.kind, WorkflowStoreErrorKind::Conflict);

    tokio::time::sleep(Duration::from_millis(2)).await;
    let intent = store
        .prepare_task_tombstone_purge(
            lease.clone(),
            WorkflowTaskTombstoneRetention::new(Duration::from_millis(1)).unwrap(),
            WorkflowTaskTombstonePurgeLimit::new(10).unwrap(),
            WorkflowTaskTombstoneApprovalWindow::new(Duration::from_secs(1)).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(intent.tombstone_count, 1);
    GovernanceFixture {
        lease,
        tombstones,
        intent,
    }
}

async fn approve_and_execute_governed_purge(
    store: &PostgresWorkflowStore,
    tenant: WorkflowTenantId,
    fixture: GovernanceFixture,
) {
    let GovernanceFixture {
        lease,
        tombstones,
        intent,
    } = fixture;
    approve_independently(store, tenant.clone(), &intent).await;
    let late_hold = store
        .place_task_tombstone_hold(
            tenant.clone(),
            tombstones[1].checkpoint_id,
            WorkerId::parse("legal").unwrap(),
            WorkflowTaskLegalHoldReason::parse("late-litigation-hold").unwrap(),
        )
        .await
        .unwrap();
    assert!(late_hold.is_active());
    let blocked = store
        .execute_task_tombstone_purge(lease.clone(), intent.purge_id)
        .await
        .unwrap_err();
    assert_eq!(blocked.kind, WorkflowStoreErrorKind::Conflict);
    store
        .release_task_tombstone_hold(
            tenant.clone(),
            tombstones[1].checkpoint_id,
            WorkerId::parse("legal-release").unwrap(),
        )
        .await
        .unwrap();
    store.release_task_cleanup(lease.clone()).await.unwrap();
    let executor_lease = store
        .claim_task_cleanup(
            tenant.clone(),
            WorkerId::parse("purge-executor").unwrap(),
            LeaseDuration::new(Duration::from_secs(2)).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let stale = store
        .execute_task_tombstone_purge(lease, intent.purge_id)
        .await
        .unwrap_err();
    assert_eq!(stale.kind, WorkflowStoreErrorKind::LeaseLost);
    let evidence = store
        .execute_task_tombstone_purge(executor_lease.clone(), intent.purge_id)
        .await
        .unwrap();
    assert_eq!(evidence.tombstone_count, 1);
    assert_eq!(
        evidence.executed_by,
        WorkerId::parse("purge-executor").unwrap()
    );
    assert_eq!(
        store
            .execute_task_tombstone_purge(executor_lease.clone(), intent.purge_id)
            .await
            .unwrap(),
        evidence
    );
    assert_eq!(
        store
            .get_task_tombstone_purge_evidence(tenant.clone(), intent.purge_id)
            .await
            .unwrap(),
        Some(evidence)
    );
    let remaining = store
        .list_task_tombstones(
            tenant.clone(),
            None,
            WorkflowTaskTombstoneLimit::new(10).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].checkpoint_id, tombstones[0].checkpoint_id);
    assert_expired_intent_cannot_be_approved(
        store,
        tenant,
        executor_lease.clone(),
        tombstones[0].checkpoint_id,
    )
    .await;
    store.release_task_cleanup(executor_lease).await.unwrap();
}

async fn approve_independently(
    store: &PostgresWorkflowStore,
    tenant: WorkflowTenantId,
    intent: &WorkflowTaskTombstonePurgeIntent,
) {
    let self_approval = store
        .approve_task_tombstone_purge(
            tenant.clone(),
            intent.purge_id,
            WorkerId::parse("purge-preparer").unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(self_approval.kind, WorkflowStoreErrorKind::Conflict);
    let approved = store
        .approve_task_tombstone_purge(
            tenant.clone(),
            intent.purge_id,
            WorkerId::parse("purge-approver").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        approved.approved_by,
        Some(WorkerId::parse("purge-approver").unwrap())
    );
}

async fn assert_expired_intent_cannot_be_approved(
    store: &PostgresWorkflowStore,
    tenant: WorkflowTenantId,
    lease: WorkflowTaskCleanupLease,
    checkpoint_id: runifold_core::CheckpointId,
) {
    store
        .release_task_tombstone_hold(
            tenant.clone(),
            checkpoint_id,
            WorkerId::parse("legal-final-release").unwrap(),
        )
        .await
        .unwrap();
    let intent = store
        .prepare_task_tombstone_purge(
            lease,
            WorkflowTaskTombstoneRetention::new(Duration::from_millis(1)).unwrap(),
            WorkflowTaskTombstonePurgeLimit::new(10).unwrap(),
            WorkflowTaskTombstoneApprovalWindow::new(Duration::from_millis(10)).unwrap(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    let expired = store
        .approve_task_tombstone_purge(
            tenant,
            intent.purge_id,
            WorkerId::parse("purge-approver").unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(expired.kind, WorkflowStoreErrorKind::Conflict);
}

#[derive(Debug, Default)]
struct RecordingArchive {
    batch_ids: Mutex<Vec<String>>,
}

impl WorkflowTaskTombstoneArchive for RecordingArchive {
    fn archive(
        &self,
        batch: WorkflowTaskTombstoneArchiveBatch,
    ) -> WorkflowTaskTombstoneArchiveFuture<'_> {
        self.batch_ids
            .lock()
            .unwrap()
            .push(batch.batch_id.as_str().to_owned());
        Box::pin(async {
            Ok(WorkflowTaskTombstoneExportReceipt::parse("archive://control-plane/1").unwrap())
        })
    }
}

#[derive(Debug)]
struct FailingAuthorizer;

impl WorkflowTaskGovernanceAuthorizer for FailingAuthorizer {
    fn authorize(
        &self,
        _principal: &WorkerId,
        _tenant_id: &WorkflowTenantId,
        _permission: WorkflowTaskGovernancePermission,
    ) -> WorkflowTaskGovernanceAuthorizationFuture<'_> {
        Box::pin(async {
            Err(WorkflowTaskGovernanceAuthorizationError::new(
                "policy backend unavailable",
            ))
        })
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "the end-to-end authorization scenario remains contiguous"
)]
async fn assert_authorized_governance_control_plane(store: &Arc<PostgresWorkflowStore>) {
    let (tenant, checkpoint_id, cleaner) = seed_governance_tombstone(store).await;
    assert_authorizer_outage_fails_closed(store, tenant.clone()).await;
    let exporter = WorkerId::parse("authenticated-exporter").unwrap();
    let holder = WorkerId::parse("authenticated-holder").unwrap();
    let preparer = WorkerId::parse("authenticated-preparer").unwrap();
    let reviewer_a = WorkerId::parse("authenticated-reviewer-a").unwrap();
    let reviewer_b = WorkerId::parse("authenticated-reviewer-b").unwrap();
    let authorizer = StaticWorkflowTaskGovernanceAuthorizer::new()
        .with_grant(
            exporter.clone(),
            tenant.clone(),
            WorkflowTaskGovernancePermission::Export,
        )
        .with_grant(
            holder.clone(),
            tenant.clone(),
            WorkflowTaskGovernancePermission::PlaceHold,
        )
        .with_grant(
            preparer.clone(),
            tenant.clone(),
            WorkflowTaskGovernancePermission::PreparePurge,
        )
        .with_grant(
            reviewer_a.clone(),
            tenant.clone(),
            WorkflowTaskGovernancePermission::ReadApprovalInbox,
        )
        .with_grant(
            reviewer_a.clone(),
            tenant.clone(),
            WorkflowTaskGovernancePermission::ClaimPurgeApproval,
        )
        .with_grant(
            reviewer_a.clone(),
            tenant.clone(),
            WorkflowTaskGovernancePermission::ApprovePurge,
        )
        .with_grant(
            reviewer_a.clone(),
            tenant.clone(),
            WorkflowTaskGovernancePermission::RejectPurge,
        )
        .with_grant(
            reviewer_b.clone(),
            tenant.clone(),
            WorkflowTaskGovernancePermission::ClaimPurgeApproval,
        )
        .with_grant(
            reviewer_b.clone(),
            tenant.clone(),
            WorkflowTaskGovernancePermission::ApprovePurge,
        )
        .with_grant(
            reviewer_b.clone(),
            tenant.clone(),
            WorkflowTaskGovernancePermission::RejectPurge,
        );
    let control = WorkflowTaskGovernanceControlPlane::new(Arc::clone(store), Arc::new(authorizer));
    let archive = RecordingArchive::default();
    let denied = control
        .export_next_page(
            &WorkerId::parse("untrusted").unwrap(),
            tenant.clone(),
            None,
            WorkflowTaskTombstoneLimit::new(10).unwrap(),
            &archive,
        )
        .await
        .unwrap_err();
    assert!(matches!(denied, WorkflowTaskGovernanceError::Denied { .. }));
    assert!(archive.batch_ids.lock().unwrap().is_empty());

    let first = control
        .export_next_page(
            &exporter,
            tenant.clone(),
            None,
            WorkflowTaskTombstoneLimit::new(10).unwrap(),
            &archive,
        )
        .await
        .unwrap();
    let replay = control
        .export_next_page(
            &exporter,
            tenant.clone(),
            None,
            WorkflowTaskTombstoneLimit::new(10).unwrap(),
            &archive,
        )
        .await
        .unwrap();
    assert_eq!(first, replay);
    let batch_ids = archive.batch_ids.lock().unwrap().clone();
    assert_eq!(batch_ids.len(), 2);
    assert_eq!(batch_ids[0], batch_ids[1]);

    let hold = control
        .place_hold(
            &holder,
            tenant.clone(),
            checkpoint_id,
            WorkflowTaskLegalHoldReason::parse("authenticated-hold").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(hold.placed_by, holder);
    let foreign_lease = store
        .claim_task_cleanup(
            tenant.clone(),
            cleaner,
            LeaseDuration::new(Duration::from_secs(2)).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let mismatch = control
        .prepare_purge(
            &preparer,
            foreign_lease.clone(),
            WorkflowTaskTombstoneRetention::new(Duration::from_millis(1)).unwrap(),
            WorkflowTaskTombstonePurgeLimit::new(10).unwrap(),
            WorkflowTaskTombstoneApprovalWindow::new(Duration::from_secs(1)).unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        mismatch,
        WorkflowTaskGovernanceError::LeasePrincipalMismatch
    ));
    store.release_task_cleanup(foreign_lease).await.unwrap();

    assert_durable_approval_inbox(
        store, &control, tenant, exporter, preparer, reviewer_a, reviewer_b, &archive,
    )
    .await;
}

#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "the end-to-end governance actors make the four-eyes test explicit"
)]
async fn assert_durable_approval_inbox(
    store: &Arc<PostgresWorkflowStore>,
    control: &WorkflowTaskGovernanceControlPlane<
        PostgresWorkflowStore,
        StaticWorkflowTaskGovernanceAuthorizer,
    >,
    tenant: WorkflowTenantId,
    exporter: WorkerId,
    preparer: WorkerId,
    reviewer_a: WorkerId,
    reviewer_b: WorkerId,
    archive: &RecordingArchive,
) {
    seed_cancelled_tasks(store, tenant.clone(), 2).await;
    let cleanup = store
        .claim_task_cleanup(
            tenant.clone(),
            preparer.clone(),
            LeaseDuration::new(Duration::from_secs(5)).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    store
        .compact_terminal_tasks(
            cleanup.clone(),
            WorkflowTaskRetention::new(Duration::from_millis(1)).unwrap(),
            WorkflowTaskCleanupLimit::new(10).unwrap(),
        )
        .await
        .unwrap();
    control
        .export_next_page(
            &exporter,
            tenant.clone(),
            None,
            WorkflowTaskTombstoneLimit::new(20).unwrap(),
            archive,
        )
        .await
        .unwrap();

    control
        .prepare_purge(
            &preparer,
            cleanup.clone(),
            WorkflowTaskTombstoneRetention::new(Duration::from_millis(1)).unwrap(),
            WorkflowTaskTombstonePurgeLimit::new(1).unwrap(),
            WorkflowTaskTombstoneApprovalWindow::new(Duration::from_secs(3)).unwrap(),
        )
        .await
        .unwrap();
    let inbox = control
        .list_purge_approvals(
            &reviewer_a,
            tenant.clone(),
            WorkflowTaskTombstoneApprovalInboxLimit::new(10).unwrap(),
        )
        .await
        .unwrap();
    assert!(inbox.iter().any(|item| {
        item.state == WorkflowTaskTombstoneApprovalState::Pending
            && item.intent.prepared_by == preparer
    }));

    let stale = control
        .claim_purge_approval(
            &reviewer_a,
            tenant.clone(),
            LeaseDuration::new(Duration::from_millis(5)).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    let takeover = control
        .claim_purge_approval(
            &reviewer_b,
            tenant.clone(),
            LeaseDuration::new(Duration::from_secs(1)).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(takeover.fencing_token > stale.fencing_token);
    let stale_error = control
        .approve_claimed_purge(&reviewer_a, stale)
        .await
        .unwrap_err();
    assert!(matches!(
        stale_error,
        WorkflowTaskGovernanceError::Store(ref error)
            if error.kind == WorkflowStoreErrorKind::LeaseLost
    ));
    let rejected = control
        .reject_claimed_purge(
            &reviewer_b,
            takeover,
            WorkflowTaskTombstoneRejectionReason::parse("retention exception").unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rejected.state, WorkflowTaskTombstoneApprovalState::Rejected);
    assert_eq!(rejected.rejected_by.as_ref(), Some(&reviewer_b));

    control
        .prepare_purge(
            &preparer,
            cleanup.clone(),
            WorkflowTaskTombstoneRetention::new(Duration::from_millis(1)).unwrap(),
            WorkflowTaskTombstonePurgeLimit::new(1).unwrap(),
            WorkflowTaskTombstoneApprovalWindow::new(Duration::from_secs(3)).unwrap(),
        )
        .await
        .unwrap();
    let approval = control
        .claim_purge_approval(
            &reviewer_a,
            tenant.clone(),
            LeaseDuration::new(Duration::from_secs(1)).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let mismatch = control
        .approve_claimed_purge(&reviewer_b, approval.clone())
        .await
        .unwrap_err();
    assert!(matches!(
        mismatch,
        WorkflowTaskGovernanceError::ApprovalLeasePrincipalMismatch
    ));
    let approved = control
        .approve_claimed_purge(&reviewer_a, approval)
        .await
        .unwrap();
    assert_eq!(approved.approved_by, Some(reviewer_a));
    store.release_task_cleanup(cleanup).await.unwrap();
}

async fn seed_cancelled_tasks(
    store: &PostgresWorkflowStore,
    tenant: WorkflowTenantId,
    count: usize,
) {
    for index in 0..count {
        let task =
            runifold_workflow::WorkflowTask::new(format!("approval-{index}"), 1, json!(null))
                .unwrap()
                .with_tenant(tenant.clone());
        let checkpoint_id = task.checkpoint_id;
        store.enqueue(task).await.unwrap();
        store.cancel(tenant.clone(), checkpoint_id).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(2)).await;
}

async fn assert_authorizer_outage_fails_closed(
    store: &Arc<PostgresWorkflowStore>,
    tenant: WorkflowTenantId,
) {
    let control =
        WorkflowTaskGovernanceControlPlane::new(Arc::clone(store), Arc::new(FailingAuthorizer));
    let archive = RecordingArchive::default();
    let error = control
        .export_next_page(
            &WorkerId::parse("authenticated-exporter").unwrap(),
            tenant,
            None,
            WorkflowTaskTombstoneLimit::new(10).unwrap(),
            &archive,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        WorkflowTaskGovernanceError::Authorization(_)
    ));
    assert!(archive.batch_ids.lock().unwrap().is_empty());
}

async fn seed_governance_tombstone(
    store: &PostgresWorkflowStore,
) -> (WorkflowTenantId, runifold_core::CheckpointId, WorkerId) {
    let tenant = WorkflowTenantId::parse("governance-control").unwrap();
    let task = runifold_workflow::WorkflowTask::new("governance-control", 1, json!(null))
        .unwrap()
        .with_tenant(tenant.clone());
    let checkpoint_id = task.checkpoint_id;
    store.enqueue(task).await.unwrap();
    store.cancel(tenant.clone(), checkpoint_id).await.unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let cleaner = WorkerId::parse("governance-cleaner").unwrap();
    let lease = store
        .claim_task_cleanup(
            tenant.clone(),
            cleaner.clone(),
            LeaseDuration::new(Duration::from_secs(2)).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    store
        .compact_terminal_tasks(
            lease.clone(),
            WorkflowTaskRetention::new(Duration::from_millis(1)).unwrap(),
            WorkflowTaskCleanupLimit::new(10).unwrap(),
        )
        .await
        .unwrap();
    store.release_task_cleanup(lease).await.unwrap();
    (tenant, checkpoint_id, cleaner)
}

fn task_adapter(
    store: Arc<PostgresWorkflowStore>,
    tenant: WorkflowTenantId,
) -> WorkflowTaskAdapter {
    let store: Arc<dyn WorkflowStore> = store;
    let mut adapter = WorkflowTaskAdapter::new(store)
        .with_poll_interval_ms(10)
        .unwrap();
    adapter
        .register_route(WorkflowTaskRoute::new(TOOL_NAME, WORKFLOW_NAME, 1, tenant).unwrap())
        .unwrap();
    adapter
}

fn task_server(adapter: WorkflowTaskAdapter) -> McpServer {
    McpServer::new(
        Arc::new(ToolRegistry::new()),
        root_context(),
        Implementation::new("postgres-task-server", "1"),
    )
    .with_task_backend(Arc::new(adapter))
    .with_task_notification_interval(Duration::from_millis(10))
}

async fn task_client(server: McpServer) -> McpClient {
    let client = McpClient::new(
        Arc::new(server.session()),
        McpClientConfig::new(Implementation::new("postgres-task-client", "1")).with_tasks(),
    );
    client.connect().await.unwrap();
    client
}

fn root_context() -> RunContext {
    RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new())
}
