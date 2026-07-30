use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use futures_executor::block_on;
use runifold_core::{Budget, BudgetTracker, CapabilitySet};
use serde_json::{Value, json};

use super::*;
use crate::{
    InMemoryWorkflowStore, WorkflowCheckpointHistoryLimit, WorkflowCheckpointPhase,
    WorkflowCheckpointRevision, WorkflowClock, WorkflowForkCommand, WorkflowForkOutcome,
    WorkflowForkPolicy, WorkflowInterruptCommand, WorkflowInterruptDecision,
    WorkflowInterruptDecisionOutcome, WorkflowLineage, WorkflowSignal, WorkflowSignalName,
    WorkflowSignalOutcome, WorkflowStep, WorkflowStepError, WorkflowStepFuture, WorkflowTaskStatus,
    WorkflowTenantBudgetPolicy, WorkflowTenantId,
};

#[derive(Debug, Default)]
struct ManualClock(AtomicU64);

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

#[derive(Debug)]
struct AdvancingSleeper {
    clock: Arc<ManualClock>,
    advance_ms: u64,
}

impl WorkflowWorkerSleeper for AdvancingSleeper {
    fn sleep(&self, _duration: Duration) -> WorkflowWorkerSleepFuture<'_> {
        Box::pin(async move {
            self.clock.advance(self.advance_ms);
        })
    }
}

#[derive(Debug, Default)]
struct EchoStep;

impl WorkflowStep for EchoStep {
    fn execute<'a>(&'a self, input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        Box::pin(async move { Ok(input) })
    }
}

#[derive(Debug)]
struct IncrementStep {
    calls: Arc<AtomicU64>,
}

impl WorkflowStep for IncrementStep {
    fn execute<'a>(&'a self, input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            input
                .as_u64()
                .map(|value| Value::from(value + 1))
                .ok_or_else(|| WorkflowStepError::InvalidInput("expected unsigned integer".into()))
        })
    }
}

fn assert_history_navigation(
    store: &InMemoryWorkflowStore,
    source_id: CheckpointId,
    history: &[WorkflowCheckpointRevision],
) {
    let first_page = block_on(store.list_checkpoint_history(
        WorkflowTenantId::default(),
        source_id,
        None,
        WorkflowCheckpointHistoryLimit::new(2).unwrap(),
    ))
    .unwrap();
    assert_eq!(
        first_page
            .iter()
            .map(|revision| revision.revision)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    let second_page = block_on(store.list_checkpoint_history(
        WorkflowTenantId::default(),
        source_id,
        first_page.last().map(|revision| revision.revision),
        WorkflowCheckpointHistoryLimit::new(2).unwrap(),
    ))
    .unwrap();
    assert_eq!(
        second_page
            .iter()
            .map(|revision| revision.revision)
            .collect::<Vec<_>>(),
        [2, 3]
    );
    assert_eq!(
        block_on(store.load_checkpoint_revision(WorkflowTenantId::default(), source_id, 2,))
            .unwrap(),
        history[2]
    );
    let isolation_error = block_on(store.list_checkpoint_history(
        WorkflowTenantId::parse("other-tenant").unwrap(),
        source_id,
        None,
        WorkflowCheckpointHistoryLimit::new(2).unwrap(),
    ))
    .unwrap_err();
    assert_eq!(isolation_error.kind, WorkflowStoreErrorKind::TenantMismatch);
}

#[derive(Debug)]
struct TokenConsumingStep(u64);

impl WorkflowStep for TokenConsumingStep {
    fn execute<'a>(&'a self, input: Value, run: &'a RunContext) -> WorkflowStepFuture<'a> {
        let result = run
            .budget()
            .try_consume(Usage {
                tokens: self.0,
                ..Usage::default()
            })
            .map(|_| input)
            .map_err(|error| WorkflowStepError::Execution(error.to_string()));
        Box::pin(async move { result })
    }
}

#[derive(Clone, Debug, Default)]
struct FailOnceStep(Arc<AtomicU64>);

impl WorkflowStep for FailOnceStep {
    fn execute<'a>(&'a self, input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        Box::pin(async move {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(WorkflowStepError::Execution("simulated crash".into()))
            } else {
                Ok(input)
            }
        })
    }
}

#[derive(Debug)]
struct WaitForCancellationStep {
    observed: Arc<AtomicBool>,
}

impl WorkflowStep for WaitForCancellationStep {
    fn execute<'a>(&'a self, _input: Value, run: &'a RunContext) -> WorkflowStepFuture<'a> {
        Box::pin(async move {
            run.cancellation().cancelled().await;
            self.observed.store(true, Ordering::SeqCst);
            Err(WorkflowStepError::Execution("cancelled".into()))
        })
    }
}

#[derive(Debug)]
struct CancelOnSleep {
    shutdown: CancellationToken,
    sleeps: AtomicU64,
}

impl WorkflowWorkerSleeper for CancelOnSleep {
    fn sleep(&self, _duration: Duration) -> WorkflowWorkerSleepFuture<'_> {
        Box::pin(async move {
            self.sleeps.fetch_add(1, Ordering::SeqCst);
            self.shutdown.cancel();
        })
    }
}

#[derive(Debug)]
struct ConcurrentStep {
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl WorkflowStep for ConcurrentStep {
    fn execute<'a>(&'a self, input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        Box::pin(async move {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            Delay::new(Duration::from_millis(10)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(input)
        })
    }
}

#[derive(Debug)]
struct ShutdownThenCompleteStep {
    shutdown: CancellationToken,
    completed: Arc<AtomicBool>,
}

impl WorkflowStep for ShutdownThenCompleteStep {
    fn execute<'a>(&'a self, input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        Box::pin(async move {
            self.shutdown.cancel();
            Delay::new(Duration::from_millis(10)).await;
            self.completed.store(true, Ordering::SeqCst);
            Ok(input)
        })
    }
}

fn duration(millis: u64) -> LeaseDuration {
    LeaseDuration::new(Duration::from_millis(millis)).unwrap()
}

fn definition(workflow: Workflow) -> WorkflowDefinition {
    WorkflowDefinition::new(Arc::new(workflow), Budget::default(), CapabilitySet::new())
}

fn registry(definition: WorkflowDefinition) -> WorkflowRegistry {
    let mut registry = WorkflowRegistry::new();
    registry.register(definition).unwrap();
    registry
}

#[test]
fn worker_claims_executes_checkpoints_and_completes_a_task() {
    let store = Arc::new(InMemoryWorkflowStore::new());
    let workflow = Workflow::builder("echo")
        .step("echo", EchoStep, CapabilitySet::new())
        .build()
        .unwrap();
    let worker = WorkflowWorker::new(
        store.clone(),
        registry(definition(workflow)),
        WorkerId::parse("worker-a").unwrap(),
        duration(100),
        Duration::from_millis(20),
    )
    .unwrap();
    let task = WorkflowTask::new("echo", 1, json!({"value": 7})).unwrap();
    let id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();

    let outcome = block_on(worker.run_once()).unwrap();

    assert!(matches!(
        outcome,
        WorkflowWorkerOutcome::Completed { checkpoint_id, .. } if checkpoint_id == id
    ));
    assert_eq!(
        block_on(store.inspect(WorkflowTenantId::default(), id))
            .unwrap()
            .status,
        WorkflowTaskStatus::Completed
    );
    assert_eq!(
        block_on(worker.run_once()).unwrap(),
        WorkflowWorkerOutcome::Idle
    );
}

#[test]
fn worker_reserves_and_settles_tenant_budget_before_starting_more_work() {
    let store = Arc::new(InMemoryWorkflowStore::new());
    let tenant = WorkflowTenantId::parse("budgeted-tenant").unwrap();
    block_on(
        store.set_tenant_budget_policy(
            tenant.clone(),
            WorkflowTenantBudgetPolicy::new(
                Budget {
                    tokens: Some(10),
                    ..Budget::default()
                },
                Duration::from_secs(60),
                Duration::from_secs(5),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let workflow = Workflow::builder("budgeted")
        .step("consume", TokenConsumingStep(6), CapabilitySet::new())
        .build()
        .unwrap();
    let definition = WorkflowDefinition::new(
        Arc::new(workflow),
        Budget {
            tokens: Some(10),
            ..Budget::default()
        },
        CapabilitySet::new(),
    );
    let worker = WorkflowWorker::new(
        store.clone(),
        registry(definition),
        WorkerId::parse("budget-worker").unwrap(),
        duration(100),
        Duration::from_millis(20),
    )
    .unwrap();
    for _ in 0..2 {
        block_on(
            store.enqueue(
                WorkflowTask::new("budgeted", 1, json!(null))
                    .unwrap()
                    .with_tenant(tenant.clone()),
            ),
        )
        .unwrap();
    }

    assert!(matches!(
        block_on(worker.run_once()).unwrap(),
        WorkflowWorkerOutcome::Completed { .. }
    ));
    assert!(matches!(
        block_on(worker.run_once()).unwrap(),
        WorkflowWorkerOutcome::Retried { .. }
    ));
    let snapshot = block_on(store.inspect_tenant_budget(tenant)).unwrap();
    assert_eq!(snapshot.committed.tokens, 6);
    assert_eq!(snapshot.reserved.tokens, 0);
}

#[test]
fn expired_worker_checkpoint_is_resumed_by_the_next_owner() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(InMemoryWorkflowStore::with_clock(clock.clone()));
    let step = FailOnceStep::default();
    let workflow = Workflow::builder("recover")
        .step("unstable", step.clone(), CapabilitySet::new())
        .build()
        .unwrap();
    let task = WorkflowTask::new("recover", 1, json!({"value": 9})).unwrap();
    let id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();
    let first = block_on(store.claim(WorkerId::parse("worker-a").unwrap(), duration(10)))
        .unwrap()
        .unwrap();
    let checkpoint = WorkflowCheckpoint::distributed(store.clone(), first.lease);
    let run = RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new());
    assert!(block_on(workflow.run_checkpointed(json!({"value": 9}), &run, &checkpoint)).is_err());
    clock.advance(10);
    let worker = WorkflowWorker::new(
        store.clone(),
        registry(
            definition(workflow).with_resume_policy(WorkflowResumePolicy::RetryInterruptedStep),
        ),
        WorkerId::parse("worker-b").unwrap(),
        duration(10),
        Duration::from_millis(5),
    )
    .unwrap();

    let outcome = block_on(worker.run_once()).unwrap();

    assert!(matches!(
        outcome,
        WorkflowWorkerOutcome::Completed { checkpoint_id, .. } if checkpoint_id == id
    ));
    assert_eq!(step.0.load(Ordering::SeqCst), 2);
}

#[test]
fn heartbeat_loss_cancels_inflight_work_before_returning() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(InMemoryWorkflowStore::with_clock(clock.clone()));
    let observed = Arc::new(AtomicBool::new(false));
    let workflow = Workflow::builder("lease-loss")
        .step(
            "wait",
            WaitForCancellationStep {
                observed: observed.clone(),
            },
            CapabilitySet::new(),
        )
        .build()
        .unwrap();
    let worker = WorkflowWorker::new(
        store.clone(),
        registry(definition(workflow)),
        WorkerId::parse("worker-a").unwrap(),
        duration(10),
        Duration::from_millis(5),
    )
    .unwrap()
    .with_sleeper(Arc::new(AdvancingSleeper {
        clock,
        advance_ms: 10,
    }));
    let task = WorkflowTask::new("lease-loss", 1, json!(null)).unwrap();
    let id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();

    let outcome = block_on(worker.run_once()).unwrap();

    assert_eq!(
        outcome,
        WorkflowWorkerOutcome::LeaseLost { checkpoint_id: id }
    );
    assert!(observed.load(Ordering::SeqCst));
}

#[test]
fn missing_definition_is_requeued_without_execution() {
    let store = Arc::new(InMemoryWorkflowStore::new());
    let worker = WorkflowWorker::new(
        store.clone(),
        WorkflowRegistry::new(),
        WorkerId::parse("worker-a").unwrap(),
        duration(100),
        Duration::from_millis(20),
    )
    .unwrap();
    let task = WorkflowTask::new("not-installed", 7, json!(null)).unwrap();
    let id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();

    let outcome = block_on(worker.run_once()).unwrap();

    assert_eq!(
        outcome,
        WorkflowWorkerOutcome::DefinitionUnavailable { checkpoint_id: id }
    );
    assert_eq!(
        block_on(store.inspect(WorkflowTenantId::default(), id))
            .unwrap()
            .status,
        WorkflowTaskStatus::Queued
    );
}

#[test]
fn supervisor_rejects_invalid_limits_and_backoff() {
    assert!(WorkflowSupervisorConfig::new(0).is_err());
    assert!(
        WorkflowSupervisorConfig::new(1)
            .unwrap()
            .with_backoff(Duration::ZERO, Duration::from_secs(1))
            .is_err()
    );
    assert!(
        WorkflowSupervisorConfig::new(1)
            .unwrap()
            .with_backoff(Duration::from_secs(2), Duration::from_secs(1))
            .is_err()
    );
}

#[test]
fn supervisor_backs_off_when_idle_and_stops_without_spinning() {
    let store = Arc::new(InMemoryWorkflowStore::new());
    let worker = Arc::new(
        WorkflowWorker::new(
            store,
            WorkflowRegistry::new(),
            WorkerId::parse("worker-a").unwrap(),
            duration(100),
            Duration::from_millis(20),
        )
        .unwrap(),
    );
    let shutdown = CancellationToken::new();
    let sleeper = Arc::new(CancelOnSleep {
        shutdown: shutdown.clone(),
        sleeps: AtomicU64::new(0),
    });
    let metrics = WorkflowSupervisorMetrics::default();
    let supervisor = WorkflowSupervisor::new(
        worker,
        WorkflowSupervisorConfig::new(1)
            .unwrap()
            .with_backoff(Duration::from_millis(1), Duration::from_millis(4))
            .unwrap(),
    )
    .with_sleeper(sleeper.clone())
    .with_metrics(metrics.clone());

    let report = block_on(supervisor.run(&shutdown));

    assert_eq!(report.idle_polls, 1);
    assert_eq!(sleeper.sleeps.load(Ordering::SeqCst), 1);
    assert_eq!(
        metrics.snapshot(),
        WorkflowSupervisorMetricSnapshot {
            cycles_started: 2,
            peak_active_cycles: 1,
            idle_polls: 1,
            backoffs: 1,
            ..WorkflowSupervisorMetricSnapshot::default()
        }
    );
}

#[test]
fn supervisor_executes_with_bounded_concurrency() {
    let store = Arc::new(InMemoryWorkflowStore::new());
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let workflow = Workflow::builder("concurrent")
        .step(
            "wait",
            ConcurrentStep {
                active: active.clone(),
                peak: peak.clone(),
            },
            CapabilitySet::new(),
        )
        .build()
        .unwrap();
    let worker = Arc::new(
        WorkflowWorker::new(
            store.clone(),
            registry(definition(workflow)),
            WorkerId::parse("worker-a").unwrap(),
            duration(1_000),
            Duration::from_millis(100),
        )
        .unwrap(),
    );
    let mut ids = Vec::new();
    for value in 0..6 {
        let task = WorkflowTask::new("concurrent", 1, json!(value)).unwrap();
        ids.push(task.checkpoint_id);
        block_on(store.enqueue(task)).unwrap();
    }
    let shutdown = CancellationToken::new();
    let sleeper = Arc::new(CancelOnSleep {
        shutdown: shutdown.clone(),
        sleeps: AtomicU64::new(0),
    });
    let metrics = WorkflowSupervisorMetrics::default();
    let supervisor = WorkflowSupervisor::new(
        worker,
        WorkflowSupervisorConfig::new(2)
            .unwrap()
            .with_backoff(Duration::from_millis(1), Duration::from_millis(4))
            .unwrap(),
    )
    .with_sleeper(sleeper)
    .with_metrics(metrics.clone());

    let report = block_on(supervisor.run(&shutdown));

    assert_eq!(report.completed, 6);
    assert_eq!(peak.load(Ordering::SeqCst), 2);
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(metrics.snapshot().peak_active_cycles, 2);
    assert_eq!(metrics.snapshot().active_cycles, 0);
    assert!(ids.into_iter().all(|id| {
        block_on(store.inspect(WorkflowTenantId::default(), id))
            .unwrap()
            .status
            == WorkflowTaskStatus::Completed
    }));
}

#[test]
fn supervisor_shutdown_drains_an_already_started_workflow() {
    let store = Arc::new(InMemoryWorkflowStore::new());
    let shutdown = CancellationToken::new();
    let completed = Arc::new(AtomicBool::new(false));
    let workflow = Workflow::builder("drain")
        .step(
            "finish",
            ShutdownThenCompleteStep {
                shutdown: shutdown.clone(),
                completed: completed.clone(),
            },
            CapabilitySet::new(),
        )
        .build()
        .unwrap();
    let worker = Arc::new(
        WorkflowWorker::new(
            store.clone(),
            registry(definition(workflow)),
            WorkerId::parse("worker-a").unwrap(),
            duration(1_000),
            Duration::from_millis(100),
        )
        .unwrap(),
    );
    let task = WorkflowTask::new("drain", 1, json!(null)).unwrap();
    let id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();
    let supervisor = WorkflowSupervisor::new(worker, WorkflowSupervisorConfig::new(1).unwrap());

    let report = block_on(supervisor.run(&shutdown));

    assert!(completed.load(Ordering::SeqCst));
    assert_eq!(report.completed, 1);
    assert_eq!(
        block_on(store.inspect(WorkflowTenantId::default(), id))
            .unwrap()
            .status,
        WorkflowTaskStatus::Completed
    );
    assert_eq!(supervisor.metrics().snapshot().active_cycles, 0);
}

#[test]
fn worker_suspends_and_resumes_a_durable_timer_without_holding_a_lease() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(InMemoryWorkflowStore::with_clock(clock.clone()));
    let workflow = Workflow::builder("timer")
        .timer("wait", Duration::from_millis(25))
        .step("echo", EchoStep, CapabilitySet::new())
        .build()
        .unwrap();
    let worker = WorkflowWorker::new(
        store.clone(),
        registry(definition(workflow)),
        WorkerId::parse("worker-a").unwrap(),
        duration(100),
        Duration::from_millis(20),
    )
    .unwrap();
    let task = WorkflowTask::new("timer", 1, json!({"value": 7})).unwrap();
    let id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();

    assert_eq!(
        block_on(worker.run_once()).unwrap(),
        WorkflowWorkerOutcome::Suspended { checkpoint_id: id }
    );
    assert_eq!(
        block_on(store.inspect(WorkflowTenantId::default(), id))
            .unwrap()
            .status,
        WorkflowTaskStatus::Waiting
    );
    clock.advance(25);
    assert!(matches!(
        block_on(worker.run_once()).unwrap(),
        WorkflowWorkerOutcome::Completed { checkpoint_id, .. } if checkpoint_id == id
    ));
}

#[test]
fn worker_resumes_a_signal_wait_with_the_durable_payload() {
    let store = Arc::new(InMemoryWorkflowStore::new());
    let workflow = Workflow::builder("approval")
        .wait_for_signal("wait", "approved")
        .step("echo", EchoStep, CapabilitySet::new())
        .build()
        .unwrap();
    let worker = WorkflowWorker::new(
        store.clone(),
        registry(definition(workflow)),
        WorkerId::parse("worker-a").unwrap(),
        duration(100),
        Duration::from_millis(20),
    )
    .unwrap();
    let task = WorkflowTask::new("approval", 1, json!({"request": 7})).unwrap();
    let id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();
    assert_eq!(
        block_on(worker.run_once()).unwrap(),
        WorkflowWorkerOutcome::Suspended { checkpoint_id: id }
    );
    assert_eq!(
        block_on(
            store.publish_signal(
                WorkflowTenantId::default(),
                WorkflowSignal::new(
                    id,
                    WorkflowSignalName::parse("approved").unwrap(),
                    json!({"approved": true}),
                )
                .unwrap(),
            )
        )
        .unwrap(),
        WorkflowSignalOutcome::WokeWorkflow
    );

    let outcome = block_on(worker.run_once()).unwrap();

    assert!(matches!(
        outcome,
        WorkflowWorkerOutcome::Completed {
            checkpoint_id,
            outcome: WorkflowOutcome { output, .. },
        } if checkpoint_id == id && output == json!({"approved": true})
    ));
    assert_eq!(
        block_on(store.inspect(WorkflowTenantId::default(), id))
            .unwrap()
            .attempts,
        2
    );
}

#[test]
fn worker_resumes_a_durable_interrupt_with_the_edited_value() {
    let store = Arc::new(InMemoryWorkflowStore::new());
    let workflow = Workflow::builder("human-review")
        .interrupt("review", "Review the proposed transfer")
        .step("echo", EchoStep, CapabilitySet::new())
        .build()
        .unwrap();
    let worker = WorkflowWorker::new(
        store.clone(),
        registry(definition(workflow)),
        WorkerId::parse("worker-a").unwrap(),
        duration(100),
        Duration::from_millis(20),
    )
    .unwrap();
    let task = WorkflowTask::new("human-review", 1, json!({"amount": 42})).unwrap();
    let id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();
    assert_eq!(
        block_on(worker.run_once()).unwrap(),
        WorkflowWorkerOutcome::Suspended { checkpoint_id: id }
    );
    let snapshot = block_on(store.inspect(WorkflowTenantId::default(), id)).unwrap();
    let request = snapshot.interrupt.expect("interrupt must be inspectable");
    assert_eq!(request.proposal, json!({"amount": 42}));
    assert_eq!(
        block_on(
            store.decide_interrupt(
                WorkflowTenantId::default(),
                WorkflowInterruptCommand::new(
                    id,
                    request.interrupt_id,
                    WorkflowInterruptDecision::edit(json!({"amount": 40})).unwrap(),
                )
                .unwrap(),
            )
        )
        .unwrap(),
        WorkflowInterruptDecisionOutcome::WokeWorkflow
    );

    let outcome = block_on(worker.run_once()).unwrap();

    assert!(matches!(
        outcome,
        WorkflowWorkerOutcome::Completed {
            checkpoint_id,
            outcome: WorkflowOutcome { output, .. },
        } if checkpoint_id == id
            && output == json!({"kind": "edited", "value": {"amount": 40}})
    ));
}

#[test]
fn worker_forks_from_history_without_replaying_committed_steps() {
    let store = Arc::new(InMemoryWorkflowStore::new());
    let calls = Arc::new(AtomicU64::new(0));
    let workflow = Workflow::builder("time-travel")
        .step(
            "first",
            IncrementStep {
                calls: calls.clone(),
            },
            CapabilitySet::new(),
        )
        .step(
            "second",
            IncrementStep {
                calls: calls.clone(),
            },
            CapabilitySet::new(),
        )
        .build()
        .unwrap();
    let worker = WorkflowWorker::new(
        store.clone(),
        registry(definition(workflow)),
        WorkerId::parse("worker-a").unwrap(),
        duration(100),
        Duration::from_millis(20),
    )
    .unwrap();
    let task = WorkflowTask::new("time-travel", 1, json!(0)).unwrap();
    let source_id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();
    assert!(matches!(
        block_on(worker.run_once()).unwrap(),
        WorkflowWorkerOutcome::Completed { checkpoint_id, .. }
            if checkpoint_id == source_id
    ));
    let history = block_on(store.list_checkpoint_history(
        WorkflowTenantId::default(),
        source_id,
        None,
        WorkflowCheckpointHistoryLimit::new(32).unwrap(),
    ))
    .unwrap();
    assert_history_navigation(&store, source_id, &history);
    let after_first = history
        .iter()
        .find(|revision| {
            revision.state.next_index == 1
                && matches!(revision.state.phase, WorkflowCheckpointPhase::Ready)
        })
        .expect("first committed step must have an immutable revision");
    let command = WorkflowForkCommand::new(
        source_id,
        after_first.revision,
        WorkflowForkPolicy::RejectAmbiguous,
    );
    let fork_id = command.fork_checkpoint_id;
    assert_eq!(
        block_on(store.fork_workflow(WorkflowTenantId::default(), command.clone())).unwrap(),
        WorkflowForkOutcome::Created {
            checkpoint_id: fork_id
        }
    );
    assert_eq!(
        block_on(store.fork_workflow(WorkflowTenantId::default(), command)).unwrap(),
        WorkflowForkOutcome::Duplicate {
            checkpoint_id: fork_id
        }
    );

    let outcome = block_on(worker.run_once()).unwrap();

    assert!(matches!(
        outcome,
        WorkflowWorkerOutcome::Completed {
            checkpoint_id,
            outcome: WorkflowOutcome { output, .. },
        } if checkpoint_id == fork_id && output == json!(2)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        block_on(store.inspect(WorkflowTenantId::default(), fork_id))
            .unwrap()
            .lineage,
        Some(WorkflowLineage {
            parent_checkpoint_id: source_id,
            parent_revision: after_first.revision,
            policy: WorkflowForkPolicy::RejectAmbiguous,
        })
    );
}

#[test]
fn worker_commits_a_durable_signal_timeout_as_typed_output() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(InMemoryWorkflowStore::with_clock(clock.clone()));
    let workflow = Workflow::builder("timeout")
        .wait_for_signal_or_timeout("wait", "approved", Duration::from_millis(25))
        .step("echo", EchoStep, CapabilitySet::new())
        .build()
        .unwrap();
    let worker = WorkflowWorker::new(
        store.clone(),
        registry(definition(workflow)),
        WorkerId::parse("worker-a").unwrap(),
        duration(100),
        Duration::from_millis(20),
    )
    .unwrap();
    let task = WorkflowTask::new("timeout", 1, json!({"request": 7})).unwrap();
    let id = task.checkpoint_id;
    block_on(store.enqueue(task)).unwrap();
    assert_eq!(
        block_on(worker.run_once()).unwrap(),
        WorkflowWorkerOutcome::Suspended { checkpoint_id: id }
    );
    clock.advance(25);

    let outcome = block_on(worker.run_once()).unwrap();

    assert!(matches!(
        outcome,
        WorkflowWorkerOutcome::Completed {
            checkpoint_id,
            outcome: WorkflowOutcome { output, .. },
        } if checkpoint_id == id && output == json!({"kind": "timed_out"})
    ));
}
