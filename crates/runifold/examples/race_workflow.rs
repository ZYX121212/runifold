//! Run a side-effect-safe, budget-bounded first-success race.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::Context;
use runifold::{
    Budget, BudgetTracker, CapabilitySet, ParallelBranch, RunContext, Usage, Workflow,
    WorkflowStep, WorkflowStepError, WorkflowStepFuture,
};
use serde_json::Value;

struct FastAnswer;

impl WorkflowStep for FastAnswer {
    fn execute<'a>(&'a self, _input: Value, run: &'a RunContext) -> WorkflowStepFuture<'a> {
        let result = run
            .budget()
            .try_consume(Usage {
                turns: 1,
                ..Usage::default()
            })
            .map(|_| Value::String("fast answer".into()))
            .map_err(|error| WorkflowStepError::Execution(error.to_string()));
        Box::pin(async move { result })
    }
}

struct SlowAnswer {
    started: Arc<AtomicUsize>,
}

impl WorkflowStep for SlowAnswer {
    fn execute<'a>(&'a self, _input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        self.started.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::pending())
    }
}

fn main() -> anyhow::Result<()> {
    let slow_started = Arc::new(AtomicUsize::new(0));
    let workflow = Workflow::builder("provider-race")
        .race(
            "first_answer",
            [
                ParallelBranch::step(
                    "fast",
                    FastAnswer,
                    CapabilitySet::new(),
                    Usage {
                        turns: 1,
                        ..Usage::default()
                    },
                ),
                ParallelBranch::step(
                    "slow",
                    SlowAnswer {
                        started: slow_started.clone(),
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
        .context("failed to assemble the race workflow")?;
    let run = RunContext::root(
        BudgetTracker::new(Budget {
            turns: Some(3),
            ..Budget::default()
        }),
        CapabilitySet::new(),
    );

    let outcome = futures_executor::block_on(workflow.run("question", &run))
        .context("race workflow failed")?;

    println!("winner: {}", outcome.output);
    println!(
        "started slow branches: {}",
        slow_started.load(Ordering::SeqCst)
    );
    println!("conservative turns charged: {}", outcome.usage.turns);
    Ok(())
}
