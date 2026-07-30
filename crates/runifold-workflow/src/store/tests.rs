use std::{
    sync::{
        Barrier,
        atomic::{AtomicU64, Ordering},
    },
    thread,
};

use futures_executor::block_on;
use runifold_core::RunId;
use serde_json::json;

use super::*;
use crate::{
    WorkflowInterruptCommand, WorkflowInterruptDecision, WorkflowInterruptDecisionOutcome,
    WorkflowInterruptRequest, WorkflowSignalName,
};

#[derive(Debug, Default)]
struct ManualClock(AtomicU64);

#[test]
fn tenant_budget_discovery_is_stable_bounded_and_budget_only() {
    let store = InMemoryWorkflowStore::new();
    let policy = WorkflowTenantBudgetPolicy::new(
        Budget {
            tokens: Some(100),
            ..Budget::default()
        },
        Duration::from_secs(60),
        Duration::from_secs(1),
    )
    .unwrap();
    for name in ["tenant-c", "tenant-a", "tenant-b"] {
        block_on(store.set_tenant_budget_policy(tenant(name), policy)).unwrap();
    }
    block_on(store.set_tenant_policy(
        tenant("tenant-policy-only"),
        WorkflowTenantPolicy::default(),
    ))
    .unwrap();

    let first = block_on(store.list_tenant_budgets(None, WorkflowTenantListLimit::new(2).unwrap()))
        .unwrap();
    assert_eq!(
        first
            .iter()
            .map(WorkflowTenantId::as_str)
            .collect::<Vec<_>>(),
        ["tenant-a", "tenant-b"]
    );
    let second = block_on(store.list_tenant_budgets(
        first.last().cloned(),
        WorkflowTenantListLimit::new(2).unwrap(),
    ))
    .unwrap();
    assert_eq!(
        second
            .iter()
            .map(WorkflowTenantId::as_str)
            .collect::<Vec<_>>(),
        ["tenant-c"]
    );
}

impl ManualClock {
    fn advance(&self, millis: u64) {
        self.0.fetch_add(millis, Ordering::SeqCst);
    }
}

impl WorkflowClock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn lease_duration(millis: u64) -> LeaseDuration {
    LeaseDuration::new(Duration::from_millis(millis)).unwrap()
}

fn task(name: &str, priority: i32) -> WorkflowTask {
    WorkflowTask::new(name, 1, json!({"input": name}))
        .unwrap()
        .with_priority(priority)
}

fn tenant(name: &str) -> WorkflowTenantId {
    WorkflowTenantId::parse(name).unwrap()
}

#[test]
fn tenant_admission_rejects_excess_outstanding_work_and_reopens_on_completion() {
    let store = InMemoryWorkflowStore::new();
    let tenant_id = tenant("tenant-a");
    block_on(store.set_tenant_policy(tenant_id.clone(), WorkflowTenantPolicy::new(2, 1).unwrap()))
        .unwrap();
    block_on(store.enqueue(task("first", 1).with_tenant(tenant_id.clone()))).unwrap();
    block_on(store.enqueue(task("second", 1).with_tenant(tenant_id.clone()))).unwrap();

    let denied =
        block_on(store.enqueue(task("third", 1).with_tenant(tenant_id.clone()))).unwrap_err();
    assert_eq!(denied.kind, WorkflowStoreErrorKind::AdmissionDenied);

    let claimed = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(100)))
        .unwrap()
        .unwrap();
    block_on(store.finish(claimed.lease, WorkflowDisposition::Completed)).unwrap();
    block_on(store.enqueue(task("third", 1).with_tenant(tenant_id))).unwrap();
}

#[test]
fn claim_is_fair_between_tenants_and_enforces_each_lease_limit() {
    let store = InMemoryWorkflowStore::new();
    let tenant_a = tenant("tenant-a");
    let tenant_b = tenant("tenant-b");
    block_on(store.set_tenant_policy(tenant_a.clone(), WorkflowTenantPolicy::new(10, 2).unwrap()))
        .unwrap();
    block_on(store.set_tenant_policy(tenant_b.clone(), WorkflowTenantPolicy::new(10, 1).unwrap()))
        .unwrap();
    block_on(store.enqueue(task("a-high", 100).with_tenant(tenant_a.clone()))).unwrap();
    block_on(store.enqueue(task("a-next", 90).with_tenant(tenant_a.clone()))).unwrap();
    block_on(store.enqueue(task("b-low", 1).with_tenant(tenant_b.clone()))).unwrap();

    let first = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(100)))
        .unwrap()
        .unwrap();
    assert_eq!(first.task.tenant_id, tenant_a);
    let second = block_on(store.claim(WorkerId::parse("worker-b").unwrap(), lease_duration(100)))
        .unwrap()
        .unwrap();
    assert_eq!(second.task.tenant_id, tenant_b);
    let third = block_on(store.claim(WorkerId::parse("worker-c").unwrap(), lease_duration(100)))
        .unwrap()
        .unwrap();
    assert_eq!(third.task.workflow, "a-next");
    assert!(
        block_on(store.claim(WorkerId::parse("worker-d").unwrap(), lease_duration(100)))
            .unwrap()
            .is_none()
    );
}

#[test]
fn control_plane_rejects_cross_tenant_resource_access() {
    let store = InMemoryWorkflowStore::new();
    let owner = tenant("tenant-owner");
    let intruder = tenant("tenant-intruder");
    let owner_task = task("owned", 1).with_tenant(owner.clone());
    let id = owner_task.checkpoint_id;
    block_on(store.enqueue(owner_task)).unwrap();

    assert_eq!(
        block_on(store.inspect(intruder.clone(), id))
            .unwrap_err()
            .kind,
        WorkflowStoreErrorKind::TenantMismatch
    );
    assert_eq!(
        block_on(store.cancel(intruder.clone(), id))
            .unwrap_err()
            .kind,
        WorkflowStoreErrorKind::TenantMismatch
    );
    let signal = WorkflowSignal::new(
        id,
        WorkflowSignalName::parse("forbidden").unwrap(),
        json!(null),
    )
    .unwrap();
    assert_eq!(
        block_on(store.publish_signal(intruder, signal))
            .unwrap_err()
            .kind,
        WorkflowStoreErrorKind::TenantMismatch
    );
    assert_eq!(
        block_on(store.inspect(owner, id)).unwrap().status,
        WorkflowTaskStatus::Queued
    );
}

#[test]
fn tenant_budget_reservations_enforce_aggregate_limits_and_settle_usage() {
    let clock = Arc::new(ManualClock::default());
    let store = InMemoryWorkflowStore::with_clock(clock.clone());
    let tenant_id = tenant("tenant-budget");
    let policy = WorkflowTenantBudgetPolicy::new(
        Budget {
            tokens: Some(100),
            ..Budget::default()
        },
        Duration::from_millis(100),
        Duration::from_millis(10),
    )
    .unwrap();
    block_on(store.set_tenant_budget_policy(tenant_id.clone(), policy)).unwrap();
    block_on(store.enqueue(task("first", 1).with_tenant(tenant_id.clone()))).unwrap();
    block_on(store.enqueue(task("second", 1).with_tenant(tenant_id.clone()))).unwrap();
    let first = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(20)))
        .unwrap()
        .unwrap();
    let second = block_on(store.claim(WorkerId::parse("worker-b").unwrap(), lease_duration(20)))
        .unwrap()
        .unwrap();
    let workflow_limit = Budget {
        tokens: Some(60),
        ..Budget::default()
    };
    assert_eq!(
        block_on(store.reserve_budget(first.lease.clone(), workflow_limit, Usage::default(),))
            .unwrap(),
        WorkflowBudgetReservationOutcome::Reserved
    );
    assert_eq!(
        block_on(store.reserve_budget(second.lease.clone(), workflow_limit, Usage::default(),))
            .unwrap_err()
            .kind,
        WorkflowStoreErrorKind::AdmissionDenied
    );

    block_on(store.settle_budget(
        first.lease.clone(),
        Usage {
            tokens: 40,
            ..Usage::default()
        },
    ))
    .unwrap();
    block_on(store.finish(first.lease, WorkflowDisposition::Completed)).unwrap();
    block_on(store.reserve_budget(second.lease.clone(), workflow_limit, Usage::default())).unwrap();
    assert_eq!(
        block_on(store.settle_budget(
            second.lease.clone(),
            Usage {
                tokens: 61,
                ..Usage::default()
            },
        ))
        .unwrap_err()
        .kind,
        WorkflowStoreErrorKind::AdmissionDenied
    );
    assert_eq!(
        block_on(store.cancel(tenant_id.clone(), second.task.checkpoint_id)).unwrap(),
        WorkflowCancelOutcome::Cancelled
    );
    let exhausted = block_on(store.inspect_tenant_budget(tenant_id.clone())).unwrap();
    assert_eq!(exhausted.committed.tokens, 100);
    assert_eq!(exhausted.reserved.tokens, 0);

    clock.advance(100);
    let reset = block_on(store.inspect_tenant_budget(tenant_id.clone())).unwrap();
    assert_eq!(reset.committed, Usage::default());
    assert_budget_audit(&store, tenant_id);
}

#[test]
fn tenant_budget_projection_lease_fences_expired_owner() {
    let clock = Arc::new(ManualClock::default());
    let store = InMemoryWorkflowStore::with_clock(clock.clone());
    let tenant_id = tenant("tenant-projection-lease");
    block_on(
        store.set_tenant_budget_policy(
            tenant_id.clone(),
            WorkflowTenantBudgetPolicy::new(
                Budget {
                    tokens: Some(100),
                    ..Budget::default()
                },
                Duration::from_secs(60),
                Duration::from_secs(1),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let projection_id = WorkflowBudgetAuditProjectionId::parse("otel-primary").unwrap();
    let first = block_on(store.claim_tenant_budget_audit_projection(
        tenant_id.clone(),
        projection_id.clone(),
        WorkerId::parse("projector-a").unwrap(),
        lease_duration(10),
    ))
    .unwrap()
    .unwrap();
    assert!(
        block_on(store.claim_tenant_budget_audit_projection(
            tenant_id.clone(),
            projection_id.clone(),
            WorkerId::parse("projector-b").unwrap(),
            lease_duration(10),
        ))
        .unwrap()
        .is_none()
    );
    let renewed =
        block_on(store.heartbeat_tenant_budget_audit_projection(first.clone(), lease_duration(20)))
            .unwrap();
    clock.advance(21);
    let takeover = block_on(store.claim_tenant_budget_audit_projection(
        tenant_id.clone(),
        projection_id,
        WorkerId::parse("projector-b").unwrap(),
        lease_duration(10),
    ))
    .unwrap()
    .unwrap();
    assert!(takeover.fencing_token > renewed.fencing_token);
    assert_eq!(
        block_on(store.heartbeat_tenant_budget_audit_projection(renewed, lease_duration(10)))
            .unwrap_err()
            .kind,
        WorkflowStoreErrorKind::LeaseLost
    );
    let event = block_on(store.list_tenant_budget_audit(
        tenant_id,
        Some(takeover.cursor),
        WorkflowBudgetAuditLimit::new(1).unwrap(),
    ))
    .unwrap()
    .into_iter()
    .next()
    .unwrap();
    let advanced =
        block_on(store.advance_tenant_budget_audit_projection_lease(takeover, event.cursor))
            .unwrap();
    block_on(store.release_tenant_budget_audit_projection(advanced)).unwrap();
}

fn assert_budget_audit(store: &InMemoryWorkflowStore, tenant_id: WorkflowTenantId) {
    let first_page = block_on(store.list_tenant_budget_audit(
        tenant_id.clone(),
        None,
        WorkflowBudgetAuditLimit::new(3).unwrap(),
    ))
    .unwrap();
    assert_eq!(first_page.len(), 3);
    assert_eq!(
        first_page
            .iter()
            .map(|event| event.kind)
            .collect::<Vec<_>>(),
        [
            WorkflowBudgetAuditKind::PolicyConfigured,
            WorkflowBudgetAuditKind::Reserved,
            WorkflowBudgetAuditKind::AdmissionDenied,
        ]
    );
    let remaining = block_on(store.list_tenant_budget_audit(
        tenant_id.clone(),
        Some(first_page[2].cursor),
        WorkflowBudgetAuditLimit::new(10).unwrap(),
    ))
    .unwrap();
    assert_eq!(remaining.len(), 5);
    assert_eq!(remaining[2].kind, WorkflowBudgetAuditKind::UsageExceeded);
    assert!(matches!(
        remaining[3].kind,
        WorkflowBudgetAuditKind::Forfeited(WorkflowBudgetForfeitReason::Cancelled)
    ));
    assert_eq!(remaining[4].kind, WorkflowBudgetAuditKind::WindowReset);
    let projection_id = WorkflowBudgetAuditProjectionId::parse("otel-primary").unwrap();
    let initial =
        block_on(store.load_or_create_tenant_budget_audit_projection(
            tenant_id.clone(),
            projection_id.clone(),
        ))
        .unwrap();
    assert_eq!(initial, WorkflowBudgetAuditCursor::default());
    assert_eq!(
        block_on(store.compact_tenant_budget_audit(tenant_id.clone(), first_page[0].cursor,))
            .unwrap_err()
            .kind,
        WorkflowStoreErrorKind::Conflict
    );
    assert!(
        block_on(store.advance_tenant_budget_audit_projection(
            tenant_id.clone(),
            projection_id.clone(),
            initial,
            first_page[2].cursor,
        ))
        .unwrap()
    );
    assert!(
        !block_on(store.advance_tenant_budget_audit_projection(
            tenant_id.clone(),
            projection_id.clone(),
            initial,
            remaining[0].cursor,
        ))
        .unwrap()
    );
    assert_eq!(
        block_on(
            store.load_or_create_tenant_budget_audit_projection(tenant_id.clone(), projection_id,)
        )
        .unwrap(),
        first_page[2].cursor
    );
    assert_eq!(
        block_on(store.compact_tenant_budget_audit(tenant_id, first_page[2].cursor)).unwrap(),
        3
    );
}

#[test]
fn tenant_budget_reservation_survives_takeover_then_forfeits_after_grace() {
    let clock = Arc::new(ManualClock::default());
    let store = InMemoryWorkflowStore::with_clock(clock.clone());
    let tenant_id = tenant("tenant-recovery");
    block_on(
        store.set_tenant_budget_policy(
            tenant_id.clone(),
            WorkflowTenantBudgetPolicy::new(
                Budget {
                    tokens: Some(100),
                    ..Budget::default()
                },
                Duration::from_secs(1),
                Duration::from_millis(10),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    block_on(store.enqueue(task("recover", 1).with_tenant(tenant_id.clone()))).unwrap();
    let first = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    let workflow_limit = Budget {
        tokens: Some(80),
        ..Budget::default()
    };
    block_on(store.reserve_budget(first.lease, workflow_limit, Usage::default())).unwrap();

    clock.advance(10);
    let takeover = block_on(store.claim(WorkerId::parse("worker-b").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    block_on(store.reserve_budget(
        takeover.lease.clone(),
        workflow_limit,
        Usage {
            tokens: 20,
            ..Usage::default()
        },
    ))
    .unwrap();
    let adopted = block_on(store.inspect_tenant_budget(tenant_id.clone())).unwrap();
    assert_eq!(adopted.committed.tokens, 20);
    assert_eq!(adopted.reserved.tokens, 60);

    clock.advance(20);
    let forfeited = block_on(store.inspect_tenant_budget(tenant_id)).unwrap();
    assert_eq!(forfeited.committed.tokens, 80);
    assert_eq!(forfeited.reserved.tokens, 0);
}

#[test]
fn claims_highest_priority_and_rejects_duplicate_enqueue() {
    let store = InMemoryWorkflowStore::new();
    let low = task("low", 1);
    let high = task("high", 9);
    block_on(store.enqueue(low)).unwrap();
    block_on(store.enqueue(high.clone())).unwrap();

    let claimed = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(100)))
        .unwrap()
        .unwrap();

    assert_eq!(claimed.task.checkpoint_id, high.checkpoint_id);
    let error = block_on(store.enqueue(high)).unwrap_err();
    assert_eq!(error.kind, WorkflowStoreErrorKind::Conflict);
}

#[test]
fn concurrent_claim_has_exactly_one_winner() {
    let store = InMemoryWorkflowStore::new();
    block_on(store.enqueue(task("contended", 0))).unwrap();
    let barrier = Arc::new(Barrier::new(16));
    let handles = (0..16)
        .map(|index| {
            let store = store.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                block_on(store.claim(
                    WorkerId::parse(format!("worker-{index}")).unwrap(),
                    lease_duration(100),
                ))
                .unwrap()
                .is_some()
            })
        })
        .collect::<Vec<_>>();

    let winners = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .filter(|won| *won)
        .count();

    assert_eq!(winners, 1);
}

#[test]
fn expired_lease_is_reclaimed_with_a_higher_fencing_token() {
    let clock = Arc::new(ManualClock::default());
    let store = InMemoryWorkflowStore::with_clock(clock.clone());
    let task = task("recover", 0);
    block_on(store.enqueue(task)).unwrap();
    let first = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    clock.advance(10);

    let second = block_on(store.claim(WorkerId::parse("worker-b").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();

    assert_eq!(second.lease.fencing_token, first.lease.fencing_token + 1);
    assert_eq!(second.lease.attempt, 2);
    let stale = block_on(store.finish(first.lease, WorkflowDisposition::Completed)).unwrap_err();
    assert_eq!(stale.kind, WorkflowStoreErrorKind::LeaseLost);
}

#[test]
fn heartbeat_and_finish_require_the_current_unexpired_owner() {
    let clock = Arc::new(ManualClock::default());
    let store = InMemoryWorkflowStore::with_clock(clock.clone());
    let task = task("heartbeat", 0);
    let id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();
    let claimed = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    clock.advance(5);
    let renewed = block_on(store.heartbeat(claimed.lease, lease_duration(20))).unwrap();
    assert_eq!(renewed.expires_at_ms, 25);
    block_on(store.finish(renewed, WorkflowDisposition::Completed)).unwrap();

    let snapshot = block_on(store.inspect(WorkflowTenantId::default(), id)).unwrap();
    assert_eq!(snapshot.status, WorkflowTaskStatus::Completed);
    assert_eq!(snapshot.attempts, 1);
}

#[test]
fn retry_delay_prevents_early_reclaim() {
    let clock = Arc::new(ManualClock::default());
    let store = InMemoryWorkflowStore::with_clock(clock.clone());
    block_on(store.enqueue(task("retry", 0))).unwrap();
    let claimed = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    block_on(store.finish(
        claimed.lease,
        WorkflowDisposition::RetryAfter(Duration::from_millis(30)),
    ))
    .unwrap();

    assert!(
        block_on(store.claim(WorkerId::parse("worker-b").unwrap(), lease_duration(10)))
            .unwrap()
            .is_none()
    );
    clock.advance(30);
    assert!(
        block_on(store.claim(WorkerId::parse("worker-b").unwrap(), lease_duration(10)))
            .unwrap()
            .is_some()
    );
}

#[test]
fn checkpoint_cas_is_fenced_across_worker_takeover() {
    let clock = Arc::new(ManualClock::default());
    let store = InMemoryWorkflowStore::with_clock(clock.clone());
    let task = task("checkpoint-fence", 0);
    let id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();
    let first = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    let initial = Checkpoint::initial(id, RunId::new(), "runifold.workflow", 3, json!({}));
    block_on(store.compare_and_swap_checkpoint(first.lease.clone(), initial.clone(), None))
        .unwrap();
    clock.advance(10);
    let second = block_on(store.claim(WorkerId::parse("worker-b").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    let next = initial.next(json!({"worker": "b"})).unwrap();

    let stale = block_on(store.compare_and_swap_checkpoint(
        first.lease,
        next.clone(),
        Some(initial.revision),
    ))
    .unwrap_err();

    assert_eq!(stale.kind, CheckpointErrorKind::Conflict);
    assert_eq!(
        block_on(store.load_checkpoint(second.lease.clone())).unwrap(),
        initial
    );
    block_on(store.compare_and_swap_checkpoint(second.lease, next.clone(), Some(initial.revision)))
        .unwrap();
}

#[test]
fn durable_timer_releases_its_lease_until_store_time_elapses() {
    let clock = Arc::new(ManualClock::default());
    let store = InMemoryWorkflowStore::with_clock(clock.clone());
    let task = task("timer", 0);
    let id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();
    let claimed = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    block_on(store.finish(
        claimed.lease,
        WorkflowDisposition::Suspend(WorkflowWait::timer(Duration::from_millis(25)).unwrap()),
    ))
    .unwrap();

    assert_eq!(
        block_on(store.inspect(WorkflowTenantId::default(), id))
            .unwrap()
            .status,
        WorkflowTaskStatus::Waiting
    );
    assert!(
        block_on(store.claim(WorkerId::parse("worker-b").unwrap(), lease_duration(10)))
            .unwrap()
            .is_none()
    );
    clock.advance(25);
    let woken = block_on(store.claim(WorkerId::parse("worker-b").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    assert_eq!(woken.wake, Some(WorkflowWake::Timer));
}

#[test]
fn signal_is_buffered_idempotently_before_the_wait_is_installed() {
    let store = InMemoryWorkflowStore::new();
    let task = task("signal", 0);
    let id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();
    let name = WorkflowSignalName::parse("approved").unwrap();
    let signal = WorkflowSignal::new(id, name.clone(), json!({"by": "operator"})).unwrap();

    assert_eq!(
        block_on(store.publish_signal(WorkflowTenantId::default(), signal.clone())).unwrap(),
        WorkflowSignalOutcome::Buffered
    );
    assert_eq!(
        block_on(store.publish_signal(WorkflowTenantId::default(), signal)).unwrap(),
        WorkflowSignalOutcome::Duplicate
    );
    let claimed = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    block_on(store.finish(
        claimed.lease,
        WorkflowDisposition::Suspend(WorkflowWait::signal(name)),
    ))
    .unwrap();
    let woken = block_on(store.claim(WorkerId::parse("worker-b").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    assert!(matches!(
        woken.wake,
        Some(WorkflowWake::Signal { payload, .. })
            if payload == json!({"by": "operator"})
    ));
}

#[test]
fn signal_atomically_wakes_an_installed_wait() {
    let store = InMemoryWorkflowStore::new();
    let task = task("signal", 0);
    let id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();
    let name = WorkflowSignalName::parse("payment_received").unwrap();
    let claimed = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    block_on(store.finish(
        claimed.lease,
        WorkflowDisposition::Suspend(WorkflowWait::signal(name.clone())),
    ))
    .unwrap();

    let outcome = block_on(store.publish_signal(
        WorkflowTenantId::default(),
        WorkflowSignal::new(id, name, json!({"payment_id": "pay-7"})).unwrap(),
    ))
    .unwrap();

    assert_eq!(outcome, WorkflowSignalOutcome::WokeWorkflow);
    assert_eq!(
        block_on(store.inspect(WorkflowTenantId::default(), id))
            .unwrap()
            .status,
        WorkflowTaskStatus::Queued
    );
}

#[test]
fn interrupt_is_inspectable_and_decision_is_idempotent() {
    let store = InMemoryWorkflowStore::new();
    let task = task("human-review", 0);
    let checkpoint_id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();
    let claimed = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    let request =
        WorkflowInterruptRequest::new("Review the transfer", json!({"amount": 42})).unwrap();
    block_on(store.finish(
        claimed.lease,
        WorkflowDisposition::Suspend(WorkflowWait::Interrupt {
            request: request.clone(),
        }),
    ))
    .unwrap();

    let snapshot = block_on(store.inspect(WorkflowTenantId::default(), checkpoint_id)).unwrap();
    assert_eq!(snapshot.status, WorkflowTaskStatus::Waiting);
    assert_eq!(snapshot.interrupt, Some(request.clone()));

    let command = WorkflowInterruptCommand::new(
        checkpoint_id,
        request.interrupt_id,
        WorkflowInterruptDecision::edit(json!({"amount": 40})).unwrap(),
    )
    .unwrap();
    assert_eq!(
        block_on(store.decide_interrupt(WorkflowTenantId::default(), command.clone())).unwrap(),
        WorkflowInterruptDecisionOutcome::WokeWorkflow
    );
    assert_eq!(
        block_on(store.decide_interrupt(WorkflowTenantId::default(), command)).unwrap(),
        WorkflowInterruptDecisionOutcome::Duplicate
    );

    let resumed = block_on(store.claim(WorkerId::parse("worker-b").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    let Some(WorkflowWake::Signal { payload, .. }) = resumed.wake else {
        panic!("interrupt decision must resume through a durable signal wake");
    };
    assert_eq!(
        serde_json::from_value::<WorkflowInterruptDecision>(payload).unwrap(),
        WorkflowInterruptDecision::edit(json!({"amount": 40})).unwrap()
    );
}

#[test]
fn interrupt_decision_preserves_tenant_isolation() {
    let store = InMemoryWorkflowStore::new();
    let owner = tenant("owner");
    let task = task("human-review", 0).with_tenant(owner.clone());
    let checkpoint_id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();
    let claimed = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    let request = WorkflowInterruptRequest::new("Approve?", json!({"safe": true})).unwrap();
    block_on(store.finish(
        claimed.lease,
        WorkflowDisposition::Suspend(WorkflowWait::Interrupt {
            request: request.clone(),
        }),
    ))
    .unwrap();
    let command = WorkflowInterruptCommand::new(
        checkpoint_id,
        request.interrupt_id,
        WorkflowInterruptDecision::approve(),
    )
    .unwrap();

    let error = block_on(store.decide_interrupt(tenant("intruder"), command)).unwrap_err();
    assert_eq!(error.kind, WorkflowStoreErrorKind::TenantMismatch);
    assert_eq!(
        block_on(store.inspect(owner, checkpoint_id))
            .unwrap()
            .interrupt,
        Some(request)
    );
}

#[test]
fn signal_or_timeout_uses_one_store_authoritative_winner() {
    let clock = Arc::new(ManualClock::default());
    let store = InMemoryWorkflowStore::with_clock(clock.clone());
    let task = task("timeout", 0);
    block_on(store.enqueue(task)).unwrap();
    let claimed = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    block_on(
        store.finish(
            claimed.lease,
            WorkflowDisposition::Suspend(
                WorkflowWait::signal_or_timeout(
                    WorkflowSignalName::parse("approved").unwrap(),
                    Duration::from_millis(25),
                )
                .unwrap(),
            ),
        ),
    )
    .unwrap();

    clock.advance(25);
    let timed_out = block_on(store.claim(WorkerId::parse("worker-b").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();

    assert_eq!(timed_out.wake, Some(WorkflowWake::Timeout));
}

#[test]
fn late_and_terminal_signals_are_auditable_dead_letters() {
    let clock = Arc::new(ManualClock::default());
    let store = InMemoryWorkflowStore::with_clock(clock.clone());
    let wait_task = task("timeout", 0);
    let id = wait_task.checkpoint_id;
    block_on(store.enqueue(wait_task)).unwrap();
    let claimed = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    block_on(
        store.finish(
            claimed.lease,
            WorkflowDisposition::Suspend(
                WorkflowWait::signal_or_timeout(
                    WorkflowSignalName::parse("approved").unwrap(),
                    Duration::from_millis(25),
                )
                .unwrap(),
            ),
        ),
    )
    .unwrap();
    clock.advance(25);
    let late = WorkflowSignal::new(
        id,
        WorkflowSignalName::parse("approved").unwrap(),
        json!({"by": "operator"}),
    )
    .unwrap();
    let late_id = late.signal_id;

    assert_eq!(
        block_on(store.publish_signal(WorkflowTenantId::default(), late)).unwrap(),
        WorkflowSignalOutcome::DeadLettered
    );
    assert_eq!(
        block_on(store.inspect_signal(WorkflowTenantId::default(), late_id))
            .unwrap()
            .state,
        WorkflowSignalState::DeadLettered
    );
    let timed_out = block_on(store.claim(WorkerId::parse("worker-b").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    block_on(store.finish(timed_out.lease, WorkflowDisposition::Completed)).unwrap();

    let terminal_task = task("terminal", 0);
    let terminal_id = terminal_task.checkpoint_id;
    block_on(store.enqueue(terminal_task)).unwrap();
    let terminal_claim =
        block_on(store.claim(WorkerId::parse("worker-c").unwrap(), lease_duration(10)))
            .unwrap()
            .unwrap();
    block_on(store.finish(terminal_claim.lease, WorkflowDisposition::Completed)).unwrap();
    let terminal_signal = WorkflowSignal::new(
        terminal_id,
        WorkflowSignalName::parse("ignored").unwrap(),
        json!(null),
    )
    .unwrap();
    assert_eq!(
        block_on(store.publish_signal(WorkflowTenantId::default(), terminal_signal)).unwrap(),
        WorkflowSignalOutcome::DeadLettered
    );
}

#[test]
fn signal_compaction_never_removes_pending_delivery() {
    let clock = Arc::new(ManualClock::default());
    let store = InMemoryWorkflowStore::with_clock(clock.clone());
    let consumed_task = task("consumed", 0);
    let consumed_id = consumed_task.checkpoint_id;
    block_on(store.enqueue(consumed_task)).unwrap();
    let consumed = WorkflowSignal::new(
        consumed_id,
        WorkflowSignalName::parse("ready").unwrap(),
        json!({"value": 1}),
    )
    .unwrap();
    let consumed_signal_id = consumed.signal_id;
    block_on(store.publish_signal(WorkflowTenantId::default(), consumed)).unwrap();
    let claimed = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(10)))
        .unwrap()
        .unwrap();
    block_on(store.finish(
        claimed.lease,
        WorkflowDisposition::Suspend(WorkflowWait::signal(
            WorkflowSignalName::parse("ready").unwrap(),
        )),
    ))
    .unwrap();

    let pending_task = task("pending", 0);
    let pending_id = pending_task.checkpoint_id;
    block_on(store.enqueue(pending_task)).unwrap();
    let pending = WorkflowSignal::new(
        pending_id,
        WorkflowSignalName::parse("future").unwrap(),
        json!({"value": 2}),
    )
    .unwrap();
    let pending_signal_id = pending.signal_id;
    block_on(store.publish_signal(WorkflowTenantId::default(), pending)).unwrap();

    let retention = WorkflowSignalRetention::new(Duration::from_millis(10)).unwrap();
    assert_eq!(
        block_on(store.compact_signals(WorkflowTenantId::default(), retention)).unwrap(),
        0
    );
    clock.advance(10);
    assert_eq!(
        block_on(store.compact_signals(WorkflowTenantId::default(), retention)).unwrap(),
        1
    );
    assert_eq!(
        block_on(store.inspect_signal(WorkflowTenantId::default(), pending_signal_id))
            .unwrap()
            .state,
        WorkflowSignalState::Pending
    );
    assert_eq!(
        block_on(store.inspect_signal(WorkflowTenantId::default(), consumed_signal_id))
            .unwrap_err()
            .kind,
        WorkflowStoreErrorKind::NotFound
    );
}

#[test]
fn external_cancel_is_idempotent_and_fences_a_leased_worker() {
    let store = InMemoryWorkflowStore::new();
    let task = task("cancel", 0);
    let id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();
    let claimed = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), lease_duration(100)))
        .unwrap()
        .unwrap();

    assert_eq!(
        block_on(store.cancel(WorkflowTenantId::default(), id)).unwrap(),
        WorkflowCancelOutcome::Cancelled
    );
    assert_eq!(
        block_on(store.cancel(WorkflowTenantId::default(), id)).unwrap(),
        WorkflowCancelOutcome::AlreadyTerminal
    );
    let heartbeat = block_on(store.heartbeat(claimed.lease, lease_duration(100))).unwrap_err();
    assert_eq!(heartbeat.kind, WorkflowStoreErrorKind::LeaseLost);
    assert_eq!(
        block_on(store.inspect(WorkflowTenantId::default(), id))
            .unwrap()
            .status,
        WorkflowTaskStatus::Cancelled
    );
}
