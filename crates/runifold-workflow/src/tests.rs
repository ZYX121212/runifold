use std::collections::BTreeMap;
use std::future::poll_fn;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::task::Poll;

use runifold_agent::Agent;
use runifold_core::{
    Budget, BudgetTracker, CapabilityDescriptor, CapabilityId, CapabilityKind, CapabilitySet,
    Checkpoint, CheckpointError, CheckpointErrorKind, CheckpointId, CheckpointStore, ChildEvent,
    EffectClass, InMemoryCheckpointStore, InMemoryJournal, RiskLevel, RunContext, RunEventKind,
    Usage,
};
use runifold_model::{ContentPart, FinishReason, ModelRef, ModelStreamEvent};
use runifold_testkit::ScriptedModel;
use serde_json::{Value, json};

use crate::{
    AgentStepOutput, ParallelBranch, PredicateCondition, Workflow, WorkflowBuildError,
    WorkflowError, WorkflowResumePolicy, WorkflowStep, WorkflowStepError, WorkflowStepFuture,
};

struct AddStep {
    amount: i64,
    calls: Arc<AtomicUsize>,
}

impl AddStep {
    fn new(amount: i64, calls: Arc<AtomicUsize>) -> Self {
        Self { amount, calls }
    }
}

impl WorkflowStep for AddStep {
    fn execute<'a>(&'a self, input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            input
                .as_i64()
                .map(|value| Value::from(value + self.amount))
                .ok_or_else(|| WorkflowStepError::InvalidInput("expected an integer".into()))
        })
    }
}

struct FailRevisionOnceStore {
    inner: InMemoryCheckpointStore,
    revision: u64,
    failed: AtomicBool,
}

struct ConcurrentStep {
    output: &'static str,
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
}

impl WorkflowStep for ConcurrentStep {
    fn execute<'a>(&'a self, _input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut yielded = false;
        Box::pin(poll_fn(move |wake| {
            if !yielded {
                yielded = true;
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.maximum.fetch_max(active, Ordering::SeqCst);
                wake.waker().wake_by_ref();
                return Poll::Pending;
            }
            self.active.fetch_sub(1, Ordering::SeqCst);
            Poll::Ready(Ok(Value::String(self.output.into())))
        }))
    }
}

struct CountingValueStep {
    output: &'static str,
    calls: Arc<AtomicUsize>,
}

impl WorkflowStep for CountingValueStep {
    fn execute<'a>(&'a self, _input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(Value::String(self.output.into())) })
    }
}

struct FailingStep;

impl WorkflowStep for FailingStep {
    fn execute<'a>(&'a self, _input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        Box::pin(async {
            Err(WorkflowStepError::Execution(
                "injected parallel failure".into(),
            ))
        })
    }
}

struct CountingFailureStep {
    calls: Arc<AtomicUsize>,
}

impl WorkflowStep for CountingFailureStep {
    fn execute<'a>(&'a self, _input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(WorkflowStepError::Execution("injected race failure".into())) })
    }
}

struct BudgetedValueStep {
    output: &'static str,
    calls: Arc<AtomicUsize>,
}

impl WorkflowStep for BudgetedValueStep {
    fn execute<'a>(&'a self, _input: Value, run: &'a RunContext) -> WorkflowStepFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let result = run
            .budget()
            .try_consume(Usage {
                turns: 1,
                ..Usage::default()
            })
            .map(|_| Value::String(self.output.into()))
            .map_err(|error| WorkflowStepError::Execution(error.to_string()));
        Box::pin(async move { result })
    }
}

struct PendingStep {
    calls: Arc<AtomicUsize>,
}

impl WorkflowStep for PendingStep {
    fn execute<'a>(&'a self, _input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::pending())
    }
}

impl FailRevisionOnceStore {
    fn new(revision: u64) -> Self {
        Self {
            inner: InMemoryCheckpointStore::new(),
            revision,
            failed: AtomicBool::new(false),
        }
    }
}

impl CheckpointStore for FailRevisionOnceStore {
    fn load(&self, id: CheckpointId) -> Result<Checkpoint, CheckpointError> {
        self.inner.load(id)
    }

    fn compare_and_swap(
        &self,
        checkpoint: &Checkpoint,
        expected_revision: Option<u64>,
    ) -> Result<(), CheckpointError> {
        if checkpoint.revision == self.revision && !self.failed.swap(true, Ordering::SeqCst) {
            return Err(CheckpointError::new(
                CheckpointErrorKind::Storage,
                "injected workflow checkpoint interruption",
            ));
        }
        self.inner.compare_and_swap(checkpoint, expected_revision)
    }
}

#[test]
fn sequence_passes_canonical_output_between_steps() {
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let workflow = Workflow::builder("math")
        .step(
            "add_two",
            AddStep::new(2, first_calls.clone()),
            CapabilitySet::new(),
        )
        .step(
            "add_three",
            AddStep::new(3, second_calls.clone()),
            CapabilitySet::new(),
        )
        .build()
        .unwrap();
    let run = root_run(CapabilitySet::new());

    let outcome = futures_executor::block_on(workflow.run(4, &run)).unwrap();

    assert_eq!(outcome.output, json!(9));
    assert_eq!(outcome.steps.len(), 2);
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn branch_executes_exactly_one_selected_step() {
    let true_calls = Arc::new(AtomicUsize::new(0));
    let false_calls = Arc::new(AtomicUsize::new(0));
    let workflow = Workflow::builder("branch")
        .branch(
            "route",
            PredicateCondition::new(|input: &Value| {
                input
                    .as_i64()
                    .map(|value| value > 0)
                    .ok_or_else(|| WorkflowStepError::InvalidInput("expected an integer".into()))
            }),
            AddStep::new(10, true_calls.clone()),
            AddStep::new(-10, false_calls.clone()),
            CapabilitySet::new(),
        )
        .build()
        .unwrap();
    let run = root_run(CapabilitySet::new());

    let outcome = futures_executor::block_on(workflow.run(5, &run)).unwrap();

    assert_eq!(outcome.output, json!(15));
    assert_eq!(true_calls.load(Ordering::SeqCst), 1);
    assert_eq!(false_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn parallel_branches_overlap_and_join_in_stable_key_order() {
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let workflow = Workflow::builder("parallel")
        .parallel(
            "research",
            [
                ParallelBranch::step(
                    "zeta",
                    ConcurrentStep {
                        output: "last-key",
                        active: active.clone(),
                        maximum: maximum.clone(),
                        calls: first_calls.clone(),
                    },
                    CapabilitySet::new(),
                    runifold_core::Usage::default(),
                ),
                ParallelBranch::step(
                    "alpha",
                    ConcurrentStep {
                        output: "first-key",
                        active,
                        maximum: maximum.clone(),
                        calls: second_calls.clone(),
                    },
                    CapabilitySet::new(),
                    runifold_core::Usage::default(),
                ),
            ],
        )
        .build()
        .unwrap();
    let run = root_run(CapabilitySet::new());

    let outcome = futures_executor::block_on(workflow.run("input", &run)).unwrap();

    assert_eq!(
        outcome.output,
        json!({"alpha": "first-key", "zeta": "last-key"})
    );
    assert_eq!(maximum.load(Ordering::SeqCst), 2);
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn parallel_budget_batch_fails_before_any_branch_starts() {
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let workflow = Workflow::builder("parallel-budget")
        .parallel(
            "bounded",
            [
                ParallelBranch::step(
                    "one",
                    CountingValueStep {
                        output: "one",
                        calls: first_calls.clone(),
                    },
                    CapabilitySet::new(),
                    runifold_core::Usage {
                        turns: 2,
                        ..runifold_core::Usage::default()
                    },
                ),
                ParallelBranch::step(
                    "two",
                    CountingValueStep {
                        output: "two",
                        calls: second_calls.clone(),
                    },
                    CapabilitySet::new(),
                    runifold_core::Usage {
                        turns: 2,
                        ..runifold_core::Usage::default()
                    },
                ),
            ],
        )
        .build()
        .unwrap();
    let run = RunContext::root(
        BudgetTracker::new(Budget {
            turns: Some(3),
            ..Budget::default()
        }),
        CapabilitySet::new(),
    );

    let error = futures_executor::block_on(workflow.run("input", &run)).unwrap_err();

    assert!(matches!(error, WorkflowError::Budget(_)));
    assert_eq!(first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(second_calls.load(Ordering::SeqCst), 0);
    assert_eq!(run.budget().usage().turns, 0);
}

#[test]
fn parallel_failure_cancels_unfinished_sibling_runs() {
    let journal = InMemoryJournal::new();
    let workflow = Workflow::builder("parallel-failure")
        .parallel(
            "fanout",
            [
                ParallelBranch::step(
                    "failure",
                    FailingStep,
                    CapabilitySet::new(),
                    runifold_core::Usage::default(),
                ),
                ParallelBranch::step(
                    "sibling",
                    ConcurrentStep {
                        output: "unused",
                        active: Arc::new(AtomicUsize::new(0)),
                        maximum: Arc::new(AtomicUsize::new(0)),
                        calls: Arc::new(AtomicUsize::new(0)),
                    },
                    CapabilitySet::new(),
                    runifold_core::Usage::default(),
                ),
            ],
        )
        .build()
        .unwrap();
    let run = root_run(CapabilitySet::new()).with_journal(Arc::new(journal.clone()));

    let error = futures_executor::block_on(workflow.run("input", &run)).unwrap_err();

    assert!(matches!(error, WorkflowError::ParallelBranch { .. }));
    assert!(journal.events().iter().any(|event| matches!(
        &event.kind,
        RunEventKind::Child(ChildEvent::Cancelled { .. })
    )));
}

#[test]
fn race_returns_the_first_success_and_forfeits_losing_budget() {
    let winner_calls = Arc::new(AtomicUsize::new(0));
    let loser_calls = Arc::new(AtomicUsize::new(0));
    let journal = InMemoryJournal::new();
    let workflow = Workflow::builder("first-success")
        .race(
            "providers",
            [
                ParallelBranch::step(
                    "winner",
                    BudgetedValueStep {
                        output: "winner",
                        calls: winner_calls.clone(),
                    },
                    CapabilitySet::new(),
                    Usage {
                        turns: 1,
                        ..Usage::default()
                    },
                ),
                ParallelBranch::step(
                    "loser",
                    PendingStep {
                        calls: loser_calls.clone(),
                    },
                    CapabilitySet::new(),
                    Usage {
                        turns: 2,
                        ..Usage::default()
                    },
                ),
            ],
        )
        .build()
        .unwrap();
    let run = RunContext::root(
        BudgetTracker::new(Budget {
            turns: Some(3),
            ..Budget::default()
        }),
        CapabilitySet::new(),
    )
    .with_journal(Arc::new(journal.clone()));

    let outcome = futures_executor::block_on(workflow.run("input", &run)).unwrap();

    assert_eq!(outcome.output, json!("winner"));
    assert_eq!(outcome.usage.turns, 3);
    assert_eq!(winner_calls.load(Ordering::SeqCst), 1);
    assert_eq!(loser_calls.load(Ordering::SeqCst), 1);
    assert!(journal.events().iter().any(|event| matches!(
        &event.kind,
        RunEventKind::Child(ChildEvent::Cancelled { .. })
    )));
}

#[test]
fn race_ignores_early_failures_until_a_branch_succeeds() {
    let winner_calls = Arc::new(AtomicUsize::new(0));
    let workflow = Workflow::builder("failure-then-success")
        .race(
            "providers",
            [
                ParallelBranch::step(
                    "failure",
                    FailingStep,
                    CapabilitySet::new(),
                    Usage::default(),
                ),
                ParallelBranch::step(
                    "winner",
                    BudgetedValueStep {
                        output: "winner",
                        calls: winner_calls.clone(),
                    },
                    CapabilitySet::new(),
                    Usage {
                        turns: 1,
                        ..Usage::default()
                    },
                ),
            ],
        )
        .build()
        .unwrap();
    let run = RunContext::root(
        BudgetTracker::new(Budget {
            turns: Some(1),
            ..Budget::default()
        }),
        CapabilitySet::new(),
    );

    let outcome = futures_executor::block_on(workflow.run("input", &run)).unwrap();

    assert_eq!(outcome.output, json!("winner"));
    assert_eq!(winner_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn parent_cancellation_stops_a_race_even_when_steps_ignore_it() {
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let workflow = Workflow::builder("cancel-race")
        .race(
            "providers",
            [
                ParallelBranch::step(
                    "one",
                    PendingStep {
                        calls: first_calls.clone(),
                    },
                    CapabilitySet::new(),
                    Usage {
                        turns: 1,
                        ..Usage::default()
                    },
                ),
                ParallelBranch::step(
                    "two",
                    PendingStep {
                        calls: second_calls.clone(),
                    },
                    CapabilitySet::new(),
                    Usage {
                        turns: 1,
                        ..Usage::default()
                    },
                ),
            ],
        )
        .build()
        .unwrap();
    let run = RunContext::root(
        BudgetTracker::new(Budget {
            turns: Some(2),
            ..Budget::default()
        }),
        CapabilitySet::new(),
    );
    let cancellation = run.cancellation().clone();
    let first_started = first_calls.clone();
    let second_started = second_calls.clone();
    let canceller = std::thread::spawn(move || {
        while first_started.load(Ordering::SeqCst) == 0
            || second_started.load(Ordering::SeqCst) == 0
        {
            std::thread::yield_now();
        }
        cancellation.cancel();
    });

    let error = futures_executor::block_on(workflow.run("input", &run)).unwrap_err();
    canceller.join().unwrap();

    assert!(matches!(error, WorkflowError::Cancelled));
    assert_eq!(run.budget().usage().turns, 2);
}

#[test]
fn race_rejects_write_capabilities_during_build() {
    let write = capability_with_effect("write", EffectClass::IdempotentWrite);
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(write);

    let error = Workflow::builder("unsafe-race")
        .race(
            "providers",
            [
                ParallelBranch::step(
                    "writer",
                    CountingValueStep {
                        output: "writer",
                        calls: Arc::new(AtomicUsize::new(0)),
                    },
                    capabilities,
                    Usage::default(),
                ),
                ParallelBranch::step(
                    "reader",
                    CountingValueStep {
                        output: "reader",
                        calls: Arc::new(AtomicUsize::new(0)),
                    },
                    CapabilitySet::new(),
                    Usage::default(),
                ),
            ],
        )
        .build()
        .unwrap_err();

    assert!(matches!(
        error,
        WorkflowBuildError::UnsafeRaceCapability { .. }
    ));
}

#[test]
fn completed_race_checkpoint_resumes_without_reexecuting_branches() {
    let winner_calls = Arc::new(AtomicUsize::new(0));
    let loser_calls = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(FailRevisionOnceStore::new(3));
    let checkpoint = crate::WorkflowCheckpoint::new(store);
    let workflow = Workflow::builder("recover-race")
        .race(
            "providers",
            [
                ParallelBranch::step(
                    "winner",
                    BudgetedValueStep {
                        output: "winner",
                        calls: winner_calls.clone(),
                    },
                    CapabilitySet::new(),
                    Usage {
                        turns: 1,
                        ..Usage::default()
                    },
                ),
                ParallelBranch::step(
                    "loser",
                    PendingStep {
                        calls: loser_calls.clone(),
                    },
                    CapabilitySet::new(),
                    Usage {
                        turns: 2,
                        ..Usage::default()
                    },
                ),
            ],
        )
        .build()
        .unwrap();
    let run = RunContext::root(
        BudgetTracker::new(Budget {
            turns: Some(3),
            ..Budget::default()
        }),
        CapabilitySet::new(),
    );

    let interrupted =
        futures_executor::block_on(workflow.run_checkpointed("input", &run, &checkpoint))
            .unwrap_err();
    assert!(matches!(interrupted, WorkflowError::Checkpoint(_)));

    let outcome = futures_executor::block_on(workflow.resume(
        &checkpoint,
        &run,
        WorkflowResumePolicy::RejectAmbiguous,
    ))
    .unwrap();

    assert_eq!(outcome.output, json!("winner"));
    assert_eq!(outcome.usage.turns, 3);
    assert_eq!(winner_calls.load(Ordering::SeqCst), 1);
    assert_eq!(loser_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn all_failed_race_resumes_the_aggregate_without_retrying() {
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let checkpoint = crate::WorkflowCheckpoint::new(Arc::new(InMemoryCheckpointStore::new()));
    let workflow = Workflow::builder("failed-race")
        .race(
            "providers",
            [
                ParallelBranch::step(
                    "one",
                    CountingFailureStep {
                        calls: first_calls.clone(),
                    },
                    CapabilitySet::new(),
                    Usage::default(),
                ),
                ParallelBranch::step(
                    "two",
                    CountingFailureStep {
                        calls: second_calls.clone(),
                    },
                    CapabilitySet::new(),
                    Usage::default(),
                ),
            ],
        )
        .build()
        .unwrap();
    let run = root_run(CapabilitySet::new());

    let first = futures_executor::block_on(workflow.run_checkpointed("input", &run, &checkpoint))
        .unwrap_err();
    assert!(matches!(first, WorkflowError::RaceAllFailed { .. }));

    let resumed = futures_executor::block_on(workflow.resume(
        &checkpoint,
        &run,
        WorkflowResumePolicy::RejectAmbiguous,
    ))
    .unwrap_err();

    assert!(matches!(resumed, WorkflowError::RaceAllFailed { .. }));
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn authority_escalation_fails_before_any_step_executes() {
    let calls = Arc::new(AtomicUsize::new(0));
    let capability = capability("restricted");
    let mut requested = CapabilitySet::new();
    requested.grant(capability.clone());
    let workflow = Workflow::builder("authority")
        .step("restricted", AddStep::new(1, calls.clone()), requested)
        .build()
        .unwrap();
    let run = root_run(CapabilitySet::new());

    let error = futures_executor::block_on(workflow.run(0, &run)).unwrap_err();

    assert!(matches!(
        error,
        WorkflowError::AuthorityEscalation { capability: name, .. } if name == "restricted"
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn completed_checkpoint_resumes_without_reexecuting_steps() {
    let calls = Arc::new(AtomicUsize::new(0));
    let workflow = Workflow::builder("durable")
        .step("once", AddStep::new(1, calls.clone()), CapabilitySet::new())
        .build()
        .unwrap();
    let store = Arc::new(InMemoryCheckpointStore::new());
    let checkpoint = crate::WorkflowCheckpoint::new(store);
    let run = root_run(CapabilitySet::new());

    let first =
        futures_executor::block_on(workflow.run_checkpointed(1, &run, &checkpoint)).unwrap();
    let resumed = futures_executor::block_on(workflow.resume(
        &checkpoint,
        &run,
        WorkflowResumePolicy::RejectAmbiguous,
    ))
    .unwrap();

    assert_eq!(first, resumed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn in_flight_step_requires_explicit_retry_authority() {
    let calls = Arc::new(AtomicUsize::new(0));
    let workflow = Workflow::builder("recovery")
        .step(
            "unstable_boundary",
            AddStep::new(1, calls.clone()),
            CapabilitySet::new(),
        )
        .build()
        .unwrap();
    let store = Arc::new(FailRevisionOnceStore::new(2));
    let checkpoint = crate::WorkflowCheckpoint::new(store);
    let run = root_run(CapabilitySet::new());

    let first =
        futures_executor::block_on(workflow.run_checkpointed(1, &run, &checkpoint)).unwrap_err();
    assert!(matches!(first, WorkflowError::Checkpoint(_)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let rejected = futures_executor::block_on(workflow.resume(
        &checkpoint,
        &run,
        WorkflowResumePolicy::RejectAmbiguous,
    ))
    .unwrap_err();
    assert!(matches!(
        rejected,
        WorkflowError::AmbiguousCheckpoint { .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let resumed = futures_executor::block_on(workflow.resume(
        &checkpoint,
        &run,
        WorkflowResumePolicy::RetryInterruptedStep,
    ))
    .unwrap();
    assert_eq!(resumed.output, json!(2));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn inconsistent_checkpoint_state_is_rejected_before_resume() {
    let workflow = Workflow::builder("validated-state")
        .step(
            "step",
            AddStep::new(1, Arc::new(AtomicUsize::new(0))),
            CapabilitySet::new(),
        )
        .build()
        .unwrap();
    let store = Arc::new(InMemoryCheckpointStore::new());
    let checkpoint = crate::WorkflowCheckpoint::new(store.clone());
    let run = root_run(CapabilitySet::new());
    futures_executor::block_on(workflow.run_checkpointed(1, &run, &checkpoint)).unwrap();

    let envelope = store.load(checkpoint.id()).unwrap();
    let mut payload = envelope.payload.clone();
    payload["next_index"] = json!(0);
    let corrupted = envelope.next(payload).unwrap();
    store
        .compare_and_swap(&corrupted, Some(envelope.revision))
        .unwrap();

    let error = futures_executor::block_on(workflow.resume(
        &checkpoint,
        &run,
        WorkflowResumePolicy::RejectAmbiguous,
    ))
    .unwrap_err();

    assert!(matches!(error, WorkflowError::CheckpointIdentityMismatch));
}

#[test]
fn parallel_resume_skips_the_branch_already_persisted_as_completed() {
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let workflow = parallel_counting_workflow(first_calls.clone(), second_calls.clone());
    let store = Arc::new(FailRevisionOnceStore::new(3));
    let checkpoint = crate::WorkflowCheckpoint::new(store);
    let run = root_run(CapabilitySet::new());

    let first = futures_executor::block_on(workflow.run_checkpointed("input", &run, &checkpoint))
        .unwrap_err();
    assert!(matches!(first, WorkflowError::Checkpoint(_)));

    let rejected = futures_executor::block_on(workflow.resume(
        &checkpoint,
        &run,
        WorkflowResumePolicy::RejectAmbiguous,
    ))
    .unwrap_err();
    assert!(matches!(
        rejected,
        WorkflowError::AmbiguousCheckpoint { .. }
    ));

    let resumed = futures_executor::block_on(workflow.resume(
        &checkpoint,
        &run,
        WorkflowResumePolicy::RetryInterruptedStep,
    ))
    .unwrap();

    assert_eq!(resumed.output, json!({"first": "one", "second": "two"}));
    assert_eq!(
        first_calls.load(Ordering::SeqCst) + second_calls.load(Ordering::SeqCst),
        3
    );
    assert!(first_calls.load(Ordering::SeqCst) == 1 || second_calls.load(Ordering::SeqCst) == 1);
}

#[test]
fn fully_completed_parallel_checkpoint_finishes_without_retry_authority() {
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let workflow = parallel_counting_workflow(first_calls.clone(), second_calls.clone());
    let store = Arc::new(FailRevisionOnceStore::new(4));
    let checkpoint = crate::WorkflowCheckpoint::new(store);
    let run = root_run(CapabilitySet::new());

    let first = futures_executor::block_on(workflow.run_checkpointed("input", &run, &checkpoint))
        .unwrap_err();
    assert!(matches!(first, WorkflowError::Checkpoint(_)));

    let resumed = futures_executor::block_on(workflow.resume(
        &checkpoint,
        &run,
        WorkflowResumePolicy::RejectAmbiguous,
    ))
    .unwrap();

    assert_eq!(resumed.output, json!({"first": "one", "second": "two"}));
    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn workflow_records_step_and_child_events() {
    let journal = InMemoryJournal::new();
    let workflow = Workflow::builder("observed")
        .step(
            "step",
            AddStep::new(1, Arc::new(AtomicUsize::new(0))),
            CapabilitySet::new(),
        )
        .build()
        .unwrap();
    let run = root_run(CapabilitySet::new()).with_journal(Arc::new(journal.clone()));

    futures_executor::block_on(workflow.run(1, &run)).unwrap();

    let events = journal.events();
    assert!(events.iter().any(|event| {
        matches!(
            &event.kind,
            runifold_core::RunEventKind::Domain(domain)
                if domain.namespace == "runifold.workflow" && domain.name == "step.started"
        )
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event.kind,
            runifold_core::RunEventKind::Child(runifold_core::ChildEvent::Completed { .. })
        )
    }));
}

#[test]
fn agent_step_runs_inside_the_workflow_boundary() {
    let model = ScriptedModel::new();
    model.enqueue([
        ModelStreamEvent::ResponseStarted {
            id: Some("workflow-agent".into()),
            model: ModelRef::new("test", "scripted"),
        },
        ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::text("planned"),
        },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::Stop,
            provider_metadata: BTreeMap::new(),
        },
    ]);
    let agent = Arc::new(Agent::new(
        "planner",
        Arc::new(model.clone()),
        ModelRef::new("test", "scripted"),
    ));
    let workflow = Workflow::builder("agent-workflow")
        .agent("plan", agent, CapabilitySet::new())
        .build()
        .unwrap();
    let run = root_run(CapabilitySet::new());

    let outcome = futures_executor::block_on(workflow.run("make a plan", &run)).unwrap();
    let agent_output: AgentStepOutput = serde_json::from_value(outcome.output).unwrap();

    assert_eq!(agent_output.input, "planned");
    assert_eq!(
        agent_output.outcome.response.content,
        vec![ContentPart::text("planned")]
    );
    assert_eq!(model.recorded_requests().len(), 1);
}

#[test]
fn agent_outputs_flow_directly_into_following_agents() {
    let planner_model = scripted_text_model("plan", "use a checklist");
    let writer_model = scripted_text_model("write", "finished");
    let planner = Arc::new(Agent::new(
        "planner",
        Arc::new(planner_model.clone()),
        ModelRef::new("test", "planner"),
    ));
    let writer = Arc::new(Agent::new(
        "writer",
        Arc::new(writer_model.clone()),
        ModelRef::new("test", "writer"),
    ));
    let workflow = Workflow::builder("agent-chain")
        .agent("plan", planner, CapabilitySet::new())
        .agent("write", writer, CapabilitySet::new())
        .build()
        .unwrap();
    let run = root_run(CapabilitySet::new());

    let outcome = futures_executor::block_on(workflow.run("start", &run)).unwrap();
    let output: AgentStepOutput = serde_json::from_value(outcome.output).unwrap();

    assert_eq!(output.input, "finished");
    let writer_request = writer_model.recorded_requests().pop().unwrap();
    assert!(matches!(
        &writer_request.messages[0].content[0],
        ContentPart::Text { text } if text == "use a checklist"
    ));
}

fn root_run(capabilities: CapabilitySet) -> RunContext {
    RunContext::root(BudgetTracker::new(Budget::default()), capabilities)
}

fn scripted_text_model(id: &str, text: &str) -> ScriptedModel {
    let model = ScriptedModel::new();
    model.enqueue([
        ModelStreamEvent::ResponseStarted {
            id: Some(id.into()),
            model: ModelRef::new("test", id),
        },
        ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::text(text),
        },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::Stop,
            provider_metadata: BTreeMap::new(),
        },
    ]);
    model
}

fn parallel_counting_workflow(
    first_calls: Arc<AtomicUsize>,
    second_calls: Arc<AtomicUsize>,
) -> Workflow {
    Workflow::builder("parallel-recovery")
        .parallel(
            "fanout",
            [
                ParallelBranch::step(
                    "first",
                    CountingValueStep {
                        output: "one",
                        calls: first_calls,
                    },
                    CapabilitySet::new(),
                    runifold_core::Usage::default(),
                ),
                ParallelBranch::step(
                    "second",
                    CountingValueStep {
                        output: "two",
                        calls: second_calls,
                    },
                    CapabilitySet::new(),
                    runifold_core::Usage::default(),
                ),
            ],
        )
        .build()
        .unwrap()
}

fn capability(name: &str) -> CapabilityDescriptor {
    capability_with_effect(name, EffectClass::Pure)
}

fn capability_with_effect(name: &str, effect: EffectClass) -> CapabilityDescriptor {
    CapabilityDescriptor {
        id: CapabilityId::new(),
        name: name.into(),
        version: "1".into(),
        kind: CapabilityKind::Extension("workflow-test".into()),
        input_schema: json!({}),
        output_schema: json!({}),
        effect,
        risk: RiskLevel::Low,
        metadata: BTreeMap::new(),
    }
}
