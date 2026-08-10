//! Disposable `PostgreSQL` workflow-store integration tests.

mod support;

use std::{collections::BTreeMap, time::Duration};

use runifold_core::{Budget, Checkpoint, CheckpointErrorKind, RunId, Usage};
use runifold_store_postgres::PostgresWorkflowStore;
use runifold_workflow::{
    LeaseDuration, WorkerId, WorkflowBudgetAuditKind, WorkflowBudgetAuditLimit,
    WorkflowBudgetAuditProjectionId, WorkflowBudgetForfeitReason, WorkflowCancelOutcome,
    WorkflowCheckpointHistoryLimit, WorkflowCheckpointPhase, WorkflowCheckpointState,
    WorkflowDisposition, WorkflowForkCommand, WorkflowForkOutcome, WorkflowForkPolicy,
    WorkflowInterruptCommand, WorkflowInterruptDecision, WorkflowInterruptDecisionOutcome,
    WorkflowInterruptRequest, WorkflowLineage, WorkflowSignal, WorkflowSignalName,
    WorkflowSignalOutcome, WorkflowSignalRetention, WorkflowSignalState, WorkflowStore,
    WorkflowStoreErrorKind, WorkflowTask, WorkflowTaskStatus, WorkflowTenantBudgetPolicy,
    WorkflowTenantId, WorkflowTenantListLimit, WorkflowTenantPolicy, WorkflowWait, WorkflowWake,
};
use serde_json::json;
use tokio_postgres::NoTls;
use uuid::Uuid;

pub use support::PostgresTestContext;

#[tokio::test]
async fn concurrent_claim_takeover_and_fencing_round_trip() {
    let database = PostgresTestContext::start("RUNIFOLD_TEST_POSTGRES_URL").await;
    let connection_url = database.connection_url().to_owned();
    let suffix = Uuid::now_v7().simple().to_string();
    let table = format!("runifold_wf_{suffix}");
    let first = PostgresWorkflowStore::connect(&connection_url, &table)
        .await
        .unwrap();
    let second = PostgresWorkflowStore::connect(&connection_url, &table)
        .await
        .unwrap();
    first.ensure_schema().await.unwrap();

    let task = WorkflowTask::new("distributed-test", 1, json!({"value": 7}))
        .unwrap()
        .with_priority(3);
    let id = task.checkpoint_id;
    first.enqueue(task).await.unwrap();
    let lease_duration = LeaseDuration::new(Duration::from_secs(30)).unwrap();
    let (first_claim, second_claim) = tokio::join!(
        first.claim(WorkerId::parse("worker-a").unwrap(), lease_duration),
        second.claim(WorkerId::parse("worker-b").unwrap(), lease_duration),
    );
    let claims = [first_claim.unwrap(), second_claim.unwrap()];
    assert_eq!(claims.iter().filter(|claim| claim.is_some()).count(), 1);
    let original = claims.into_iter().flatten().next().unwrap();
    let initial = Checkpoint::initial(
        id,
        RunId::new(),
        "runifold.workflow",
        3,
        json!({"state": "ready"}),
    );
    first
        .compare_and_swap_checkpoint(original.lease.clone(), initial.clone(), None)
        .await
        .unwrap();
    assert!(
        second
            .claim(WorkerId::parse("worker-c").unwrap(), lease_duration)
            .await
            .unwrap()
            .is_none()
    );

    let (client, connection) = tokio_postgres::connect(&connection_url, NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    force_expiration(&client, &table, id).await;

    let takeover = second
        .claim(WorkerId::parse("worker-c").unwrap(), lease_duration)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        takeover.lease.fencing_token,
        original.lease.fencing_token + 1
    );
    assert_eq!(takeover.lease.attempt, 2);

    let next = initial.next(json!({"state": "completed"})).unwrap();
    let stale_checkpoint = first
        .compare_and_swap_checkpoint(original.lease.clone(), next.clone(), Some(initial.revision))
        .await
        .unwrap_err();
    assert_eq!(stale_checkpoint.kind, CheckpointErrorKind::Conflict);
    assert_eq!(
        second
            .load_checkpoint(takeover.lease.clone())
            .await
            .unwrap(),
        initial
    );
    second
        .compare_and_swap_checkpoint(takeover.lease.clone(), next, Some(initial.revision))
        .await
        .unwrap();

    let stale = first
        .finish(original.lease, WorkflowDisposition::Completed)
        .await
        .unwrap_err();
    assert_eq!(stale.kind, WorkflowStoreErrorKind::LeaseLost);
    second
        .finish(takeover.lease, WorkflowDisposition::Completed)
        .await
        .unwrap();
    assert_eq!(
        second
            .inspect(WorkflowTenantId::default(), id)
            .await
            .unwrap()
            .status,
        WorkflowTaskStatus::Completed
    );

    assert_tenant_budget_ledger(&first, &second, &client, &table, lease_duration).await;
    assert_wait_round_trips(&first, lease_duration).await;
    assert_signal_governance(&first, &client, &table, lease_duration).await;
    assert_tenant_admission(&first, lease_duration).await;

    drop_schema(&client, &table).await;
}

#[tokio::test]
async fn concurrent_tenant_limits_are_enforced_across_connections() {
    let database = PostgresTestContext::start("RUNIFOLD_TEST_POSTGRES_URL").await;
    let connection_url = database.connection_url().to_owned();
    let suffix = Uuid::now_v7().simple().to_string();
    let table = format!("runifold_wf_{suffix}");
    let first = PostgresWorkflowStore::connect(&connection_url, &table)
        .await
        .unwrap();
    let second = PostgresWorkflowStore::connect(&connection_url, &table)
        .await
        .unwrap();
    first.ensure_schema().await.unwrap();
    let (client, connection) = tokio_postgres::connect(&connection_url, NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    assert_concurrent_tenant_limits(
        &first,
        &second,
        LeaseDuration::new(Duration::from_secs(30)).unwrap(),
    )
    .await;

    drop_schema(&client, &table).await;
}

async fn drop_schema(client: &tokio_postgres::Client, table: &str) {
    client
        .batch_execute(&format!(
            "DROP TABLE {table}_signals, {table}_checkpoint_history, \
                {table}_b_audit_projection, {table}_b_audit, \
                {table}_budgets, {table}, {table}_tenants; \
             DROP SEQUENCE {table}_claim_seq, {table}_b_audit_seq; \
             DROP FUNCTION {table}_capture_checkpoint()"
        ))
        .await
        .unwrap();
}

async fn assert_checkpoint_time_travel(
    store: &PostgresWorkflowStore,
    lease_duration: LeaseDuration,
) {
    let task = WorkflowTask::new("time-travel-test", 1, json!(0)).unwrap();
    let source_id = task.checkpoint_id;
    store.enqueue(task).await.unwrap();
    let claim = store
        .claim(
            WorkerId::parse("worker-time-travel").unwrap(),
            lease_duration,
        )
        .await
        .unwrap()
        .unwrap();
    let state = WorkflowCheckpointState {
        workflow: "time-travel-test".into(),
        workflow_version: 1,
        layout: Vec::new(),
        next_index: 0,
        value: json!(0),
        outputs: BTreeMap::default(),
        usage: Usage::default(),
        phase: WorkflowCheckpointPhase::Ready,
    };
    let initial = Checkpoint::initial(
        source_id,
        RunId::new(),
        "runifold.workflow",
        4,
        serde_json::to_value(&state).unwrap(),
    );
    store
        .compare_and_swap_checkpoint(claim.lease.clone(), initial.clone(), None)
        .await
        .unwrap();
    let next = initial.next(serde_json::to_value(state).unwrap()).unwrap();
    store
        .compare_and_swap_checkpoint(claim.lease.clone(), next, Some(0))
        .await
        .unwrap();
    store
        .finish(claim.lease, WorkflowDisposition::Completed)
        .await
        .unwrap();

    let history = store
        .list_checkpoint_history(
            WorkflowTenantId::default(),
            source_id,
            None,
            WorkflowCheckpointHistoryLimit::new(16).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        history
            .iter()
            .map(|revision| revision.revision)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    let command = WorkflowForkCommand::new(source_id, 0, WorkflowForkPolicy::RejectAmbiguous);
    let fork_id = command.fork_checkpoint_id;
    assert_eq!(
        store
            .fork_workflow(WorkflowTenantId::default(), command.clone())
            .await
            .unwrap(),
        WorkflowForkOutcome::Created {
            checkpoint_id: fork_id
        }
    );
    assert_eq!(
        store
            .fork_workflow(WorkflowTenantId::default(), command)
            .await
            .unwrap(),
        WorkflowForkOutcome::Duplicate {
            checkpoint_id: fork_id
        }
    );
    assert_eq!(
        store
            .inspect(WorkflowTenantId::default(), fork_id)
            .await
            .unwrap()
            .lineage,
        Some(WorkflowLineage {
            parent_checkpoint_id: source_id,
            parent_revision: 0,
            policy: WorkflowForkPolicy::RejectAmbiguous,
        })
    );
    store
        .cancel(WorkflowTenantId::default(), fork_id)
        .await
        .unwrap();
}

async fn assert_tenant_budget_ledger(
    first: &PostgresWorkflowStore,
    second: &PostgresWorkflowStore,
    client: &tokio_postgres::Client,
    table: &str,
    lease_duration: LeaseDuration,
) {
    let tenant = WorkflowTenantId::parse("tenant-budget").unwrap();
    first
        .set_tenant_budget_policy(
            tenant.clone(),
            WorkflowTenantBudgetPolicy::new(
                Budget {
                    tokens: Some(100),
                    ..Budget::default()
                },
                Duration::from_secs(60),
                Duration::from_millis(100),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let discovered = first
        .list_tenant_budgets(None, WorkflowTenantListLimit::new(10).unwrap())
        .await
        .unwrap();
    assert_eq!(discovered.as_slice(), std::slice::from_ref(&tenant));
    let envelope = assert_budget_reserve_and_settle(first, second, &tenant, lease_duration).await;
    assert_concurrent_budget_reservation(first, second, client, table, lease_duration, envelope)
        .await;
    assert_budget_recovery(first, second, client, table, lease_duration, tenant).await;
}

async fn assert_budget_reserve_and_settle(
    first: &PostgresWorkflowStore,
    second: &PostgresWorkflowStore,
    tenant: &WorkflowTenantId,
    lease_duration: LeaseDuration,
) -> Budget {
    for name in ["budget-first", "budget-second"] {
        first
            .enqueue(
                WorkflowTask::new(name, 1, json!(null))
                    .unwrap()
                    .with_tenant(tenant.clone()),
            )
            .await
            .unwrap();
    }
    let first_claim = first
        .claim(WorkerId::parse("budget-worker-a").unwrap(), lease_duration)
        .await
        .unwrap()
        .unwrap();
    let second_claim = second
        .claim(WorkerId::parse("budget-worker-b").unwrap(), lease_duration)
        .await
        .unwrap()
        .unwrap();
    let envelope = Budget {
        tokens: Some(60),
        ..Budget::default()
    };
    first
        .reserve_budget(first_claim.lease.clone(), envelope, Usage::default())
        .await
        .unwrap();
    assert_eq!(
        first
            .finish(first_claim.lease.clone(), WorkflowDisposition::Completed)
            .await
            .unwrap_err()
            .kind,
        WorkflowStoreErrorKind::Conflict
    );
    assert_eq!(
        second
            .reserve_budget(second_claim.lease.clone(), envelope, Usage::default())
            .await
            .unwrap_err()
            .kind,
        WorkflowStoreErrorKind::AdmissionDenied
    );
    first
        .settle_budget(
            first_claim.lease.clone(),
            Usage {
                tokens: 40,
                ..Usage::default()
            },
        )
        .await
        .unwrap();
    first
        .finish(first_claim.lease, WorkflowDisposition::Completed)
        .await
        .unwrap();
    second
        .reserve_budget(second_claim.lease.clone(), envelope, Usage::default())
        .await
        .unwrap();
    assert_eq!(
        second
            .settle_budget(
                second_claim.lease.clone(),
                Usage {
                    tokens: 61,
                    ..Usage::default()
                },
            )
            .await
            .unwrap_err()
            .kind,
        WorkflowStoreErrorKind::AdmissionDenied
    );
    second
        .settle_budget(
            second_claim.lease.clone(),
            Usage {
                tokens: 50,
                ..Usage::default()
            },
        )
        .await
        .unwrap();
    second
        .finish(second_claim.lease, WorkflowDisposition::Completed)
        .await
        .unwrap();
    let snapshot = first.inspect_tenant_budget(tenant.clone()).await.unwrap();
    assert_eq!(snapshot.committed.tokens, 90);
    assert_eq!(snapshot.reserved.tokens, 0);
    envelope
}

async fn assert_concurrent_budget_reservation(
    first: &PostgresWorkflowStore,
    second: &PostgresWorkflowStore,
    client: &tokio_postgres::Client,
    table: &str,
    lease_duration: LeaseDuration,
    envelope: Budget,
) {
    let concurrent_tenant = WorkflowTenantId::parse("tenant-budget-concurrent").unwrap();
    first
        .set_tenant_budget_policy(
            concurrent_tenant.clone(),
            WorkflowTenantBudgetPolicy::new(
                Budget {
                    tokens: Some(100),
                    ..Budget::default()
                },
                Duration::from_secs(60),
                Duration::from_secs(1),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    for name in ["budget-race-left", "budget-race-right"] {
        first
            .enqueue(
                WorkflowTask::new(name, 1, json!(null))
                    .unwrap()
                    .with_tenant(concurrent_tenant.clone()),
            )
            .await
            .unwrap();
    }
    let left = first
        .claim(WorkerId::parse("budget-race-a").unwrap(), lease_duration)
        .await
        .unwrap()
        .unwrap();
    let right = second
        .claim(WorkerId::parse("budget-race-b").unwrap(), lease_duration)
        .await
        .unwrap()
        .unwrap();
    let (left_result, right_result) = tokio::join!(
        first.reserve_budget(left.lease, envelope, Usage::default()),
        second.reserve_budget(right.lease, envelope, Usage::default()),
    );
    assert_eq!(
        [&left_result, &right_result]
            .into_iter()
            .filter(|result| result.is_ok())
            .count(),
        1
    );
    assert_eq!(
        [left_result, right_result]
            .into_iter()
            .find_map(Result::err)
            .unwrap()
            .kind,
        WorkflowStoreErrorKind::AdmissionDenied
    );
    client
        .execute(
            &format!(
                "UPDATE {table}_budgets
                 SET expires_at = clock_timestamp() - INTERVAL '1 millisecond'
                 WHERE tenant_id = $1"
            ),
            &[&concurrent_tenant.as_str()],
        )
        .await
        .unwrap();
    first
        .inspect_tenant_budget(concurrent_tenant.clone())
        .await
        .unwrap();
    let audit = first
        .list_tenant_budget_audit(
            concurrent_tenant.clone(),
            None,
            WorkflowBudgetAuditLimit::new(10).unwrap(),
        )
        .await
        .unwrap();
    assert!(audit.iter().any(|event| matches!(
        event.kind,
        WorkflowBudgetAuditKind::Forfeited(WorkflowBudgetForfeitReason::RecoveryExpired)
    )));
    first
        .cancel(concurrent_tenant.clone(), left.task.checkpoint_id)
        .await
        .unwrap();
    first
        .cancel(concurrent_tenant, right.task.checkpoint_id)
        .await
        .unwrap();
}

async fn assert_budget_recovery(
    first: &PostgresWorkflowStore,
    second: &PostgresWorkflowStore,
    client: &tokio_postgres::Client,
    table: &str,
    lease_duration: LeaseDuration,
    tenant: WorkflowTenantId,
) {
    let recovery_task = WorkflowTask::new("budget-recovery", 1, json!(null))
        .unwrap()
        .with_tenant(tenant.clone());
    let recovery_id = recovery_task.checkpoint_id;
    first.enqueue(recovery_task).await.unwrap();
    let original = first
        .claim(
            WorkerId::parse("budget-recovery-a").unwrap(),
            lease_duration,
        )
        .await
        .unwrap()
        .unwrap();
    let recovery_envelope = Budget {
        tokens: Some(10),
        ..Budget::default()
    };
    first
        .reserve_budget(original.lease.clone(), recovery_envelope, Usage::default())
        .await
        .unwrap();
    force_expiration(client, table, recovery_id).await;
    let takeover = second
        .claim(
            WorkerId::parse("budget-recovery-b").unwrap(),
            lease_duration,
        )
        .await
        .unwrap()
        .unwrap();
    second
        .reserve_budget(
            takeover.lease.clone(),
            recovery_envelope,
            Usage {
                tokens: 3,
                ..Usage::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        second.cancel(tenant.clone(), recovery_id).await.unwrap(),
        WorkflowCancelOutcome::Cancelled
    );
    let snapshot = first.inspect_tenant_budget(tenant.clone()).await.unwrap();
    assert_eq!(snapshot.committed.tokens, 100);
    assert_eq!(snapshot.reserved.tokens, 0);
    assert_eq!(snapshot.active_reservations, 0);
    let audit = first
        .list_tenant_budget_audit(
            tenant.clone(),
            None,
            WorkflowBudgetAuditLimit::new(20).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(audit.len(), 10);
    assert_eq!(audit[0].kind, WorkflowBudgetAuditKind::PolicyConfigured);
    assert_eq!(audit[2].kind, WorkflowBudgetAuditKind::AdmissionDenied);
    assert_eq!(audit[5].kind, WorkflowBudgetAuditKind::UsageExceeded);
    assert_eq!(audit[8].kind, WorkflowBudgetAuditKind::Adopted);
    assert!(matches!(
        audit[9].kind,
        WorkflowBudgetAuditKind::Forfeited(WorkflowBudgetForfeitReason::Cancelled)
    ));
    assert_eq!(audit[9].committed.tokens, 100);
    assert_budget_projection_cursor(first, second, client, table, tenant, &audit).await;
}

async fn assert_budget_projection_cursor(
    first: &PostgresWorkflowStore,
    second: &PostgresWorkflowStore,
    client: &tokio_postgres::Client,
    table: &str,
    tenant: WorkflowTenantId,
    audit: &[runifold_workflow::WorkflowBudgetAuditEvent],
) {
    let projection_id = WorkflowBudgetAuditProjectionId::parse("otel-primary").unwrap();
    let initial = first
        .load_or_create_tenant_budget_audit_projection(tenant.clone(), projection_id.clone())
        .await
        .unwrap();
    assert_eq!(initial.sequence(), 0);
    assert_eq!(
        first
            .compact_tenant_budget_audit(tenant.clone(), audit[0].cursor)
            .await
            .unwrap_err()
            .kind,
        WorkflowStoreErrorKind::Conflict
    );
    assert!(
        first
            .advance_tenant_budget_audit_projection(
                tenant.clone(),
                projection_id.clone(),
                initial,
                audit[0].cursor,
            )
            .await
            .unwrap()
    );
    assert!(
        !second
            .advance_tenant_budget_audit_projection(
                tenant.clone(),
                projection_id.clone(),
                initial,
                audit[1].cursor,
            )
            .await
            .unwrap()
    );
    assert_eq!(
        second
            .load_or_create_tenant_budget_audit_projection(tenant.clone(), projection_id.clone(),)
            .await
            .unwrap(),
        audit[0].cursor
    );
    assert_budget_projection_lease(
        first,
        second,
        client,
        table,
        tenant.clone(),
        projection_id,
        [audit[0].cursor, audit[1].cursor],
    )
    .await;
    assert_eq!(
        first
            .compact_tenant_budget_audit(tenant, audit[0].cursor)
            .await
            .unwrap(),
        1
    );
}

async fn assert_budget_projection_lease(
    first: &PostgresWorkflowStore,
    second: &PostgresWorkflowStore,
    client: &tokio_postgres::Client,
    table: &str,
    tenant: WorkflowTenantId,
    projection_id: WorkflowBudgetAuditProjectionId,
    cursors: [runifold_workflow::WorkflowBudgetAuditCursor; 2],
) {
    let first_lease = first
        .claim_tenant_budget_audit_projection(
            tenant.clone(),
            projection_id.clone(),
            WorkerId::parse("projector-a").unwrap(),
            LeaseDuration::new(Duration::from_secs(30)).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first_lease.cursor, cursors[0]);
    assert!(
        second
            .claim_tenant_budget_audit_projection(
                tenant.clone(),
                projection_id.clone(),
                WorkerId::parse("projector-b").unwrap(),
                LeaseDuration::new(Duration::from_secs(30)).unwrap(),
            )
            .await
            .unwrap()
            .is_none()
    );
    client
        .execute(
            &format!(
                "UPDATE {table}_b_audit_projection
                 SET lease_expires_at = clock_timestamp() - INTERVAL '1 millisecond'
                 WHERE tenant_id = $1 AND projection_id = $2"
            ),
            &[&tenant.as_str(), &projection_id.as_str()],
        )
        .await
        .unwrap();
    let takeover = second
        .claim_tenant_budget_audit_projection(
            tenant.clone(),
            projection_id,
            WorkerId::parse("projector-b").unwrap(),
            LeaseDuration::new(Duration::from_secs(30)).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(takeover.fencing_token > first_lease.fencing_token);
    assert_eq!(
        first
            .heartbeat_tenant_budget_audit_projection(
                first_lease,
                LeaseDuration::new(Duration::from_secs(30)).unwrap(),
            )
            .await
            .unwrap_err()
            .kind,
        WorkflowStoreErrorKind::LeaseLost
    );
    let takeover = second
        .advance_tenant_budget_audit_projection_lease(takeover, cursors[1])
        .await
        .unwrap();
    second
        .release_tenant_budget_audit_projection(takeover)
        .await
        .unwrap();
}

async fn assert_concurrent_tenant_limits(
    first: &PostgresWorkflowStore,
    second: &PostgresWorkflowStore,
    lease_duration: LeaseDuration,
) {
    let admission_tenant = WorkflowTenantId::parse("tenant-concurrent-admission").unwrap();
    first
        .set_tenant_policy(
            admission_tenant.clone(),
            WorkflowTenantPolicy::new(1, 1).unwrap(),
        )
        .await
        .unwrap();
    let (left, right) = tokio::join!(
        first.enqueue(
            WorkflowTask::new("admission-left", 1, json!(null))
                .unwrap()
                .with_tenant(admission_tenant.clone())
        ),
        second.enqueue(
            WorkflowTask::new("admission-right", 1, json!(null))
                .unwrap()
                .with_tenant(admission_tenant)
        ),
    );
    assert_eq!(
        [&left, &right]
            .into_iter()
            .filter(|result| result.is_ok())
            .count(),
        1
    );
    let denied = [left, right].into_iter().find_map(Result::err).unwrap();
    assert_eq!(denied.kind, WorkflowStoreErrorKind::AdmissionDenied);
    let admitted = first
        .claim(
            WorkerId::parse("concurrent-admission-cleanup").unwrap(),
            lease_duration,
        )
        .await
        .unwrap()
        .unwrap();
    first
        .finish(admitted.lease, WorkflowDisposition::Completed)
        .await
        .unwrap();

    let lease_tenant = WorkflowTenantId::parse("tenant-concurrent-lease").unwrap();
    first
        .set_tenant_policy(
            lease_tenant.clone(),
            WorkflowTenantPolicy::new(2, 1).unwrap(),
        )
        .await
        .unwrap();
    for name in ["lease-left", "lease-right"] {
        first
            .enqueue(
                WorkflowTask::new(name, 1, json!(null))
                    .unwrap()
                    .with_tenant(lease_tenant.clone()),
            )
            .await
            .unwrap();
    }
    let (left, right) = tokio::join!(
        first.claim(
            WorkerId::parse("concurrent-tenant-worker-a").unwrap(),
            lease_duration,
        ),
        second.claim(
            WorkerId::parse("concurrent-tenant-worker-b").unwrap(),
            lease_duration,
        ),
    );
    let claims = [left.unwrap(), right.unwrap()];
    assert_eq!(claims.iter().filter(|claim| claim.is_some()).count(), 1);
}

async fn assert_tenant_admission(store: &PostgresWorkflowStore, lease_duration: LeaseDuration) {
    let tenant_a = WorkflowTenantId::parse("tenant-a").unwrap();
    let tenant_b = WorkflowTenantId::parse("tenant-b").unwrap();
    store
        .set_tenant_policy(tenant_a.clone(), WorkflowTenantPolicy::new(2, 2).unwrap())
        .await
        .unwrap();
    store
        .set_tenant_policy(tenant_b.clone(), WorkflowTenantPolicy::new(2, 1).unwrap())
        .await
        .unwrap();
    let a_first = WorkflowTask::new("tenant-a-first", 1, json!(null))
        .unwrap()
        .with_tenant(tenant_a.clone())
        .with_priority(100);
    let a_first_id = a_first.checkpoint_id;
    store.enqueue(a_first).await.unwrap();
    store
        .enqueue(
            WorkflowTask::new("tenant-a-second", 1, json!(null))
                .unwrap()
                .with_tenant(tenant_a.clone())
                .with_priority(90),
        )
        .await
        .unwrap();
    let denied = store
        .enqueue(
            WorkflowTask::new("tenant-a-denied", 1, json!(null))
                .unwrap()
                .with_tenant(tenant_a.clone()),
        )
        .await
        .unwrap_err();
    assert_eq!(denied.kind, WorkflowStoreErrorKind::AdmissionDenied);
    store
        .enqueue(
            WorkflowTask::new("tenant-b", 1, json!(null))
                .unwrap()
                .with_tenant(tenant_b.clone())
                .with_priority(1),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .inspect(tenant_b.clone(), a_first_id)
            .await
            .unwrap_err()
            .kind,
        WorkflowStoreErrorKind::TenantMismatch
    );

    let first = store
        .claim(WorkerId::parse("tenant-worker-a").unwrap(), lease_duration)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.task.tenant_id, tenant_a);
    let second = store
        .claim(WorkerId::parse("tenant-worker-b").unwrap(), lease_duration)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.task.tenant_id, tenant_b);
    let third = store
        .claim(WorkerId::parse("tenant-worker-c").unwrap(), lease_duration)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(third.task.tenant_id, tenant_a);

    store
        .finish(first.lease, WorkflowDisposition::Completed)
        .await
        .unwrap();
    store
        .finish(second.lease, WorkflowDisposition::Completed)
        .await
        .unwrap();
    store
        .finish(third.lease, WorkflowDisposition::Completed)
        .await
        .unwrap();
}

async fn assert_signal_governance(
    store: &PostgresWorkflowStore,
    client: &tokio_postgres::Client,
    table: &str,
    lease_duration: LeaseDuration,
) {
    let waiting = WorkflowTask::new("signal-timeout-test", 1, json!(null)).unwrap();
    let waiting_id = waiting.checkpoint_id;
    store.enqueue(waiting).await.unwrap();
    let claim = store
        .claim(
            WorkerId::parse("worker-signal-timeout").unwrap(),
            lease_duration,
        )
        .await
        .unwrap()
        .unwrap();
    store
        .finish(
            claim.lease,
            WorkflowDisposition::Suspend(
                WorkflowWait::signal_or_timeout(
                    WorkflowSignalName::parse("approved").unwrap(),
                    Duration::from_secs(30),
                )
                .unwrap(),
            ),
        )
        .await
        .unwrap();
    client
        .execute(
            &format!(
                "UPDATE {table} SET wake_at = clock_timestamp() - INTERVAL '1 second' \
                 WHERE checkpoint_id = $1"
            ),
            &[&waiting_id.as_uuid()],
        )
        .await
        .unwrap();
    let late = WorkflowSignal::new(
        waiting_id,
        WorkflowSignalName::parse("approved").unwrap(),
        json!({"late": true}),
    )
    .unwrap();
    let late_id = late.signal_id;
    assert_eq!(
        store
            .publish_signal(WorkflowTenantId::default(), late)
            .await
            .unwrap(),
        WorkflowSignalOutcome::DeadLettered
    );
    assert_eq!(
        store
            .inspect_signal(WorkflowTenantId::default(), late_id)
            .await
            .unwrap()
            .state,
        WorkflowSignalState::DeadLettered
    );
    let timed_out = store
        .claim(
            WorkerId::parse("worker-signal-timeout").unwrap(),
            lease_duration,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(timed_out.wake, Some(WorkflowWake::Timeout));
    store
        .finish(timed_out.lease, WorkflowDisposition::Completed)
        .await
        .unwrap();

    assert_signal_cancel_and_compaction(store, client, table, late_id).await;
}

async fn assert_signal_cancel_and_compaction(
    store: &PostgresWorkflowStore,
    client: &tokio_postgres::Client,
    table: &str,
    late_id: runifold_workflow::WorkflowSignalId,
) {
    let cancelled = WorkflowTask::new("signal-cancel-test", 1, json!(null)).unwrap();
    let cancelled_id = cancelled.checkpoint_id;
    store.enqueue(cancelled).await.unwrap();
    let pending = WorkflowSignal::new(
        cancelled_id,
        WorkflowSignalName::parse("future").unwrap(),
        json!({"pending": true}),
    )
    .unwrap();
    let pending_id = pending.signal_id;
    store
        .publish_signal(WorkflowTenantId::default(), pending)
        .await
        .unwrap();
    assert_eq!(
        store
            .cancel(WorkflowTenantId::default(), cancelled_id)
            .await
            .unwrap(),
        WorkflowCancelOutcome::Cancelled
    );
    assert_eq!(
        store
            .inspect_signal(WorkflowTenantId::default(), pending_id)
            .await
            .unwrap()
            .state,
        WorkflowSignalState::DeadLettered
    );

    client
        .execute(
            &format!(
                "UPDATE {table}_signals SET created_at = \
                   clock_timestamp() - INTERVAL '1 hour' \
                 WHERE signal_id IN ($1, $2)"
            ),
            &[
                &late_id.as_checkpoint_id().as_uuid(),
                &pending_id.as_checkpoint_id().as_uuid(),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .compact_signals(
                WorkflowTenantId::default(),
                WorkflowSignalRetention::new(Duration::from_secs(1)).unwrap(),
            )
            .await
            .unwrap(),
        2
    );
}

async fn assert_wait_round_trips(store: &PostgresWorkflowStore, lease_duration: LeaseDuration) {
    assert_signal_round_trip(store, lease_duration).await;
    assert_interrupt_round_trip(store, lease_duration).await;
    assert_checkpoint_time_travel(store, lease_duration).await;
}

async fn assert_signal_round_trip(store: &PostgresWorkflowStore, lease_duration: LeaseDuration) {
    let waiting = WorkflowTask::new("signal-test", 1, json!(null)).unwrap();
    let waiting_id = waiting.checkpoint_id;
    store.enqueue(waiting).await.unwrap();
    let signal = WorkflowSignal::new(
        waiting_id,
        WorkflowSignalName::parse("approved").unwrap(),
        json!({"approved": true}),
    )
    .unwrap();
    assert_eq!(
        store
            .publish_signal(WorkflowTenantId::default(), signal.clone())
            .await
            .unwrap(),
        WorkflowSignalOutcome::Buffered
    );
    assert_eq!(
        store
            .publish_signal(WorkflowTenantId::default(), signal)
            .await
            .unwrap(),
        WorkflowSignalOutcome::Duplicate
    );
    let waiting_claim = store
        .claim(WorkerId::parse("worker-signal").unwrap(), lease_duration)
        .await
        .unwrap()
        .unwrap();
    store
        .finish(
            waiting_claim.lease,
            WorkflowDisposition::Suspend(WorkflowWait::signal(
                WorkflowSignalName::parse("approved").unwrap(),
            )),
        )
        .await
        .unwrap();
    let woken = store
        .claim(WorkerId::parse("worker-signal").unwrap(), lease_duration)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        woken.wake,
        Some(WorkflowWake::Signal { payload, .. })
            if payload == json!({"approved": true})
    ));
    store
        .finish(woken.lease, WorkflowDisposition::Completed)
        .await
        .unwrap();
}

async fn assert_interrupt_round_trip(store: &PostgresWorkflowStore, lease_duration: LeaseDuration) {
    let task = WorkflowTask::new("interrupt-test", 1, json!({"amount": 42})).unwrap();
    let checkpoint_id = task.checkpoint_id;
    store.enqueue(task).await.unwrap();
    let claimed = store
        .claim(WorkerId::parse("worker-interrupt").unwrap(), lease_duration)
        .await
        .unwrap()
        .unwrap();
    let request = WorkflowInterruptRequest::new("Review transfer", json!({"amount": 42})).unwrap();
    store
        .finish(
            claimed.lease,
            WorkflowDisposition::Suspend(WorkflowWait::Interrupt {
                request: request.clone(),
            }),
        )
        .await
        .unwrap();
    let snapshot = store
        .inspect(WorkflowTenantId::default(), checkpoint_id)
        .await
        .unwrap();
    assert_eq!(snapshot.status, WorkflowTaskStatus::Waiting);
    assert_eq!(snapshot.interrupt, Some(request.clone()));

    let command = WorkflowInterruptCommand::new(
        checkpoint_id,
        request.interrupt_id,
        WorkflowInterruptDecision::edit(json!({"amount": 40})).unwrap(),
    )
    .unwrap();
    assert_eq!(
        store
            .decide_interrupt(WorkflowTenantId::default(), command.clone())
            .await
            .unwrap(),
        WorkflowInterruptDecisionOutcome::WokeWorkflow
    );
    assert_eq!(
        store
            .decide_interrupt(WorkflowTenantId::default(), command)
            .await
            .unwrap(),
        WorkflowInterruptDecisionOutcome::Duplicate
    );
    let resumed = store
        .claim(
            WorkerId::parse("worker-interrupt-resume").unwrap(),
            lease_duration,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        resumed.wake,
        Some(WorkflowWake::Signal { payload, .. })
            if serde_json::from_value::<WorkflowInterruptDecision>(payload.clone()).unwrap()
                == WorkflowInterruptDecision::edit(json!({"amount": 40})).unwrap()
    ));
    store
        .finish(resumed.lease, WorkflowDisposition::Completed)
        .await
        .unwrap();
}

async fn force_expiration(
    client: &tokio_postgres::Client,
    table: &str,
    id: runifold_core::CheckpointId,
) {
    client
        .execute(
            &format!(
                "UPDATE {table} SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' \
                 WHERE checkpoint_id = $1"
            ),
            &[&id.as_uuid()],
        )
        .await
        .unwrap();
}
