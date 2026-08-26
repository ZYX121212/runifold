//! `SQLite` workflow-store durability and contention contracts.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use runifold_core::{Budget, CapabilitySet, Checkpoint, CheckpointId, RunContext, RunId, Usage};
use runifold_store_sqlite::SqliteWorkflowStore;
use runifold_workflow::{
    LeaseDuration, WorkerId, Workflow, WorkflowBudgetAuditProjectionId,
    WorkflowCheckpointHistoryLimit, WorkflowCheckpointPhase, WorkflowCheckpointState,
    WorkflowDefinition, WorkflowDisposition, WorkflowForkCommand, WorkflowForkOutcome,
    WorkflowForkPolicy, WorkflowInterruptCommand, WorkflowInterruptDecision,
    WorkflowInterruptDecisionOutcome, WorkflowInterruptRequest, WorkflowRegistry,
    WorkflowRemediationCheckpoint, WorkflowRemediationPolicy, WorkflowReviewFuture,
    WorkflowReviewRequest, WorkflowReviewVerdict, WorkflowReviewer, WorkflowStep,
    WorkflowStepFuture, WorkflowStore, WorkflowStoreErrorKind, WorkflowTask, WorkflowTaskStatus,
    WorkflowTenantBudgetPolicy, WorkflowTenantId, WorkflowWait, WorkflowWake, WorkflowWorker,
    WorkflowWorkerOutcome,
};
use serde_json::json;
use uuid::Uuid;

fn database_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("runifold-{name}-{}.sqlite3", Uuid::now_v7()))
}

fn worker(name: &str) -> WorkerId {
    WorkerId::parse(name).expect("test worker identity is valid")
}

fn lease() -> LeaseDuration {
    LeaseDuration::new(Duration::from_secs(30)).expect("test lease is positive")
}

fn crash_lease() -> LeaseDuration {
    LeaseDuration::new(Duration::from_millis(100)).expect("test crash lease is positive")
}

fn task(name: &str) -> WorkflowTask {
    WorkflowTask::new(name, 1, json!({"request": name})).expect("test workflow task is valid")
}

struct CountingGenerator {
    calls: Arc<AtomicUsize>,
}

impl WorkflowStep for CountingGenerator {
    fn execute<'a>(
        &'a self,
        _input: serde_json::Value,
        _run: &'a RunContext,
    ) -> WorkflowStepFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(json!({"text": "unexpected regenerated candidate"})) })
    }
}

struct ApprovePersistedCandidate {
    calls: Arc<AtomicUsize>,
}

impl WorkflowReviewer for ApprovePersistedCandidate {
    fn review<'a>(
        &'a self,
        request: WorkflowReviewRequest,
        _run: &'a RunContext,
    ) -> WorkflowReviewFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            assert_eq!(request.attempt, 1);
            assert_eq!(
                request.original_input,
                json!({"request": "durable-remediation"})
            );
            assert_eq!(request.candidate, json!({"text": "persisted candidate"}));
            Ok(WorkflowReviewVerdict::approve())
        })
    }
}

#[test]
fn missing_tokio_runtime_is_a_typed_storage_error() {
    let store = SqliteWorkflowStore::open_in_memory().expect("in-memory workflow store opens");
    let error = futures_executor::block_on(store.enqueue(task("runtime-required")))
        .expect_err("workflow operation outside Tokio must fail without panicking");
    assert_eq!(error.kind, WorkflowStoreErrorKind::Storage);
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end scenario proves crash takeover, HITL, history, fork, and budget atomicity"
)]
async fn workflow_state_survives_reopen_with_interrupt_history_and_budget_projection() {
    let path = database_path("workflow-reopen");
    let tenant_id = WorkflowTenantId::default();
    let task = task("durable-review");
    let checkpoint_id = task.checkpoint_id;
    let store = SqliteWorkflowStore::open(&path).expect("SQLite workflow store opens");

    store
        .set_tenant_budget_policy(
            tenant_id.clone(),
            WorkflowTenantBudgetPolicy::new(
                Budget {
                    tokens: Some(10_000),
                    ..Budget::default()
                },
                Duration::from_secs(60),
                Duration::from_secs(30),
            )
            .expect("test tenant budget is valid"),
        )
        .await
        .expect("tenant budget persists");
    let projection_id =
        WorkflowBudgetAuditProjectionId::parse("billing").expect("projection id is valid");
    store
        .load_or_create_tenant_budget_audit_projection(tenant_id.clone(), projection_id.clone())
        .await
        .expect("projection persists");

    store.enqueue(task).await.expect("task enqueues");
    let claimed = store
        .claim(worker("worker-a"), crash_lease())
        .await
        .expect("claim succeeds")
        .expect("task is claimable");
    assert_eq!(
        store
            .reserve_budget(
                claimed.lease.clone(),
                Budget {
                    tokens: Some(100),
                    ..Budget::default()
                },
                Usage::default(),
            )
            .await
            .expect("workflow budget reservation persists"),
        runifold_workflow::WorkflowBudgetReservationOutcome::Reserved
    );
    let checkpoint_state = WorkflowCheckpointState {
        workflow: "durable-review".into(),
        workflow_version: 1,
        layout: Vec::new(),
        next_index: 0,
        value: json!({"request": "durable-review"}),
        outputs: BTreeMap::new(),
        usage: Usage::default(),
        phase: WorkflowCheckpointPhase::Ready,
    };
    let checkpoint = Checkpoint::initial(
        checkpoint_id,
        RunId::new(),
        "runifold.workflow",
        3,
        serde_json::to_value(checkpoint_state).expect("workflow checkpoint serializes"),
    );
    store
        .compare_and_swap_checkpoint(claimed.lease.clone(), checkpoint.clone(), None)
        .await
        .expect("checkpoint persists");
    drop(store);

    let reopened = SqliteWorkflowStore::open(&path).expect("workflow store reopens");
    assert_eq!(
        reopened
            .inspect_tenant_budget(tenant_id.clone())
            .await
            .expect("budget reservation survives reopen")
            .reserved
            .tokens,
        100
    );
    tokio::time::sleep(Duration::from_millis(120)).await;
    let recovered = reopened
        .claim(worker("worker-b"), lease())
        .await
        .expect("recovery claim succeeds")
        .expect("expired task is reclaimed");
    assert!(recovered.lease.fencing_token > claimed.lease.fencing_token);
    assert_eq!(
        reopened
            .reserve_budget(
                recovered.lease.clone(),
                Budget {
                    tokens: Some(100),
                    ..Budget::default()
                },
                Usage::default(),
            )
            .await
            .expect("successor adopts durable reservation"),
        runifold_workflow::WorkflowBudgetReservationOutcome::Reserved
    );
    reopened
        .settle_budget(
            recovered.lease.clone(),
            Usage {
                tokens: 20,
                ..Usage::default()
            },
        )
        .await
        .expect("recovered workflow budget settles");
    let request = WorkflowInterruptRequest::new("Approve transfer", json!({"amount": 42}))
        .expect("interrupt request is valid");
    reopened
        .finish(
            recovered.lease,
            WorkflowDisposition::Suspend(WorkflowWait::Interrupt {
                request: request.clone(),
            }),
        )
        .await
        .expect("interrupt wait persists");
    drop(reopened);

    let reopened = SqliteWorkflowStore::open(&path).expect("interrupted store reopens");
    let snapshot = reopened
        .inspect(tenant_id.clone(), checkpoint_id)
        .await
        .expect("workflow remains inspectable");
    assert_eq!(snapshot.status, WorkflowTaskStatus::Waiting);
    assert_eq!(snapshot.interrupt, Some(request.clone()));
    assert_eq!(
        reopened
            .load_or_create_tenant_budget_audit_projection(tenant_id.clone(), projection_id,)
            .await
            .expect("projection survives reopen")
            .sequence(),
        0
    );
    let history = reopened
        .list_checkpoint_history(
            tenant_id.clone(),
            checkpoint_id,
            None,
            WorkflowCheckpointHistoryLimit::new(10).expect("history limit is valid"),
        )
        .await
        .expect("checkpoint history survives reopen");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].revision, checkpoint.revision);

    let command = WorkflowInterruptCommand::new(
        checkpoint_id,
        request.interrupt_id,
        WorkflowInterruptDecision::approve(),
    )
    .expect("interrupt decision is valid");
    assert_eq!(
        reopened
            .decide_interrupt(tenant_id.clone(), command.clone())
            .await
            .expect("interrupt decision persists"),
        WorkflowInterruptDecisionOutcome::WokeWorkflow
    );
    assert_eq!(
        reopened
            .decide_interrupt(tenant_id.clone(), command)
            .await
            .expect("duplicate decision is idempotent"),
        WorkflowInterruptDecisionOutcome::Duplicate
    );
    let resumed = reopened
        .claim(worker("worker-c"), lease())
        .await
        .expect("resumed claim succeeds")
        .expect("interrupt decision wakes workflow");
    assert!(matches!(resumed.wake, Some(WorkflowWake::Signal { .. })));
    assert_eq!(
        reopened
            .load_checkpoint(resumed.lease.clone())
            .await
            .expect("checkpoint remains loadable"),
        checkpoint
    );
    reopened
        .finish(resumed.lease, WorkflowDisposition::Completed)
        .await
        .expect("workflow completes");
    let fork_checkpoint_id = CheckpointId::new();
    assert_eq!(
        reopened
            .fork_workflow(
                tenant_id.clone(),
                WorkflowForkCommand::with_id(
                    fork_checkpoint_id,
                    checkpoint_id,
                    checkpoint.revision,
                    WorkflowForkPolicy::RejectAmbiguous,
                ),
            )
            .await
            .expect("workflow fork persists"),
        WorkflowForkOutcome::Created {
            checkpoint_id: fork_checkpoint_id
        }
    );
    drop(reopened);

    let final_store = SqliteWorkflowStore::open(&path).expect("completed store reopens");
    assert_eq!(
        final_store
            .inspect(tenant_id, checkpoint_id)
            .await
            .expect("completed workflow remains inspectable")
            .status,
        WorkflowTaskStatus::Completed
    );
    assert_eq!(
        final_store
            .inspect(WorkflowTenantId::default(), fork_checkpoint_id)
            .await
            .expect("fork survives reopen")
            .status,
        WorkflowTaskStatus::Queued
    );
    let settled = final_store
        .inspect_tenant_budget(WorkflowTenantId::default())
        .await
        .expect("settled budget survives reopen");
    assert_eq!(settled.committed.tokens, 20);
    assert_eq!(settled.reserved.tokens, 0);
    drop(final_store);
    std::fs::remove_file(path).expect("test database is removable");
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end scenario proves review-ready persistence, reopen, resume, and history"
)]
async fn review_ready_checkpoint_resumes_after_reopen_without_regeneration() {
    const WORKFLOW_CHECKPOINT_SCHEMA_V5: u32 = 5;

    let path = database_path("remediation-review-ready");
    let generator_calls = Arc::new(AtomicUsize::new(0));
    let reviewer_calls = Arc::new(AtomicUsize::new(0));
    let workflow = Workflow::builder("durable-remediation")
        .repairable_step(
            "draft",
            CountingGenerator {
                calls: generator_calls.clone(),
            },
            ApprovePersistedCandidate {
                calls: reviewer_calls.clone(),
            },
            WorkflowRemediationPolicy::new(1),
            CapabilitySet::new(),
            CapabilitySet::new(),
        )
        .build()
        .expect("repairable workflow builds");
    let workflow_task = task("durable-remediation");
    let checkpoint_id = workflow_task.checkpoint_id;
    let draft_step = workflow
        .step_ids()
        .next()
        .expect("repairable workflow contains its draft step")
        .clone();
    let store = SqliteWorkflowStore::open(&path).expect("SQLite workflow store opens");
    store
        .enqueue(workflow_task)
        .await
        .expect("repairable workflow task enqueues");
    let seeded = store
        .claim(worker("checkpoint-seeder"), lease())
        .await
        .expect("checkpoint seed claim succeeds")
        .expect("checkpoint seed task is claimable");
    let original_input = json!({"request": "durable-remediation"});
    let checkpoint = Checkpoint::initial(
        checkpoint_id,
        RunId::new(),
        "runifold.workflow",
        WORKFLOW_CHECKPOINT_SCHEMA_V5,
        serde_json::to_value(WorkflowCheckpointState {
            workflow: workflow.name().to_owned(),
            workflow_version: workflow.version(),
            layout: workflow.step_ids().cloned().collect(),
            next_index: 0,
            value: original_input.clone(),
            outputs: BTreeMap::new(),
            usage: Usage::default(),
            phase: WorkflowCheckpointPhase::Remediating {
                step: draft_step,
                attempt: 1,
                original_input,
                checkpoint: WorkflowRemediationCheckpoint::ReviewReady {
                    candidate: json!({"text": "persisted candidate"}),
                },
            },
        })
        .expect("review-ready checkpoint serializes"),
    );
    store
        .compare_and_swap_checkpoint(seeded.lease.clone(), checkpoint, None)
        .await
        .expect("review-ready checkpoint persists");
    store
        .finish(
            seeded.lease,
            WorkflowDisposition::RetryAfter(Duration::ZERO),
        )
        .await
        .expect("seeded task returns to the queue");
    drop(store);

    let reopened = Arc::new(
        SqliteWorkflowStore::open(&path).expect("SQLite remediation workflow store reopens"),
    );
    let mut registry = WorkflowRegistry::new();
    registry
        .register(WorkflowDefinition::new(
            Arc::new(workflow),
            Budget::default(),
            CapabilitySet::new(),
        ))
        .expect("repairable workflow definition registers");
    let worker = WorkflowWorker::new(
        reopened.clone(),
        registry,
        worker("remediation-worker"),
        lease(),
        Duration::from_secs(1),
    )
    .expect("remediation worker configuration is valid");

    let outcome = worker
        .run_once()
        .await
        .expect("review-ready workflow resumes");

    assert!(matches!(
        outcome,
        WorkflowWorkerOutcome::Completed {
            checkpoint_id: completed_id,
            ref outcome,
        } if completed_id == checkpoint_id
            && outcome.output == json!({"text": "persisted candidate"})
    ));
    assert_eq!(generator_calls.load(Ordering::SeqCst), 0);
    assert_eq!(reviewer_calls.load(Ordering::SeqCst), 1);
    drop(worker);
    drop(reopened);

    let final_store =
        SqliteWorkflowStore::open(&path).expect("completed remediation workflow store reopens");
    assert_eq!(
        final_store
            .inspect(WorkflowTenantId::default(), checkpoint_id)
            .await
            .expect("completed remediation workflow remains inspectable")
            .status,
        WorkflowTaskStatus::Completed
    );
    let history = final_store
        .list_checkpoint_history(
            WorkflowTenantId::default(),
            checkpoint_id,
            None,
            WorkflowCheckpointHistoryLimit::new(16).expect("history limit is valid"),
        )
        .await
        .expect("remediation checkpoint history survives reopen");
    assert!(matches!(
        history.last().map(|revision| &revision.state.phase),
        Some(WorkflowCheckpointPhase::Completed { .. })
    ));
    drop(final_store);
    std::fs::remove_file(path).expect("test remediation database is removable");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn separate_connections_have_exactly_one_claim_winner() {
    let path = Arc::new(database_path("workflow-claim"));
    let checkpoint_id = CheckpointId::new();
    let initial = SqliteWorkflowStore::open(path.as_ref()).expect("SQLite workflow store opens");
    initial
        .enqueue(task("contended").with_checkpoint_id(checkpoint_id))
        .await
        .expect("task enqueues");
    drop(initial);

    let stores = (0..8)
        .map(|_| SqliteWorkflowStore::open(path.as_ref()).expect("contending store opens"))
        .collect::<Vec<_>>();
    let claims = stores
        .into_iter()
        .enumerate()
        .map(|(index, store)| {
            tokio::spawn(async move {
                store
                    .claim(worker(&format!("worker-{index}")), lease())
                    .await
                    .expect("contending claim executes")
                    .is_some()
            })
        })
        .collect::<Vec<_>>();
    let mut winners = 0;
    for claim in claims {
        if claim.await.expect("claim task joins") {
            winners += 1;
        }
    }
    assert_eq!(winners, 1);

    std::fs::remove_file(path.as_ref()).expect("test database is removable");
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_snapshot_format_is_rejected_without_overwrite() {
    let path = database_path("workflow-format");
    let task = task("format-guard");
    let checkpoint_id = task.checkpoint_id;
    let store = SqliteWorkflowStore::open(&path).expect("SQLite workflow store opens");
    store.enqueue(task).await.expect("task enqueues");
    drop(store);

    let connection = rusqlite::Connection::open(&path).expect("test database opens directly");
    connection
        .execute(
            "UPDATE runifold_workflow_state SET format_version = 99 WHERE singleton_id = 1",
            [],
        )
        .expect("test installs a future format marker");
    drop(connection);

    let reopened = SqliteWorkflowStore::open(&path).expect("workflow store connection reopens");
    let error = reopened
        .inspect(WorkflowTenantId::default(), checkpoint_id)
        .await
        .expect_err("future snapshot format must not be treated as empty state");
    assert_eq!(error.kind, WorkflowStoreErrorKind::Storage);
    drop(reopened);

    let connection = rusqlite::Connection::open(&path).expect("test database reopens directly");
    let version = connection
        .query_row(
            "SELECT format_version FROM runifold_workflow_state WHERE singleton_id = 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .expect("format marker remains queryable");
    assert_eq!(version, 99);
    drop(connection);
    std::fs::remove_file(path).expect("test database is removable");
}
