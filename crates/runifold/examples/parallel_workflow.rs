//! Run a budget-reserved fan-out/fan-in workflow without a provider.

use anyhow::Context;
use runifold::{
    Budget, BudgetTracker, CapabilitySet, ParallelBranch, RunContext, Usage, Workflow,
    WorkflowStep, WorkflowStepFuture,
};
use serde_json::{Value, json};

struct LabelStep {
    label: &'static str,
}

impl WorkflowStep for LabelStep {
    fn execute<'a>(&'a self, input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        Box::pin(async move {
            Ok(json!({
                "label": self.label,
                "input": input,
            }))
        })
    }
}

fn main() -> anyhow::Result<()> {
    let branch_budget = Usage {
        turns: 1,
        ..Usage::default()
    };
    let workflow = Workflow::builder("parallel-analysis")
        .parallel(
            "analyze",
            [
                ParallelBranch::step(
                    "risk",
                    LabelStep { label: "risk" },
                    CapabilitySet::new(),
                    branch_budget,
                ),
                ParallelBranch::step(
                    "value",
                    LabelStep { label: "value" },
                    CapabilitySet::new(),
                    branch_budget,
                ),
            ],
        )
        .build()
        .context("failed to assemble the parallel workflow")?;
    let run = RunContext::root(
        BudgetTracker::new(Budget {
            turns: Some(2),
            ..Budget::default()
        }),
        CapabilitySet::new(),
    );

    let outcome = futures_executor::block_on(workflow.run("proposal", &run))
        .context("parallel workflow failed")?;

    println!("{}", serde_json::to_string_pretty(&outcome.output)?);
    Ok(())
}
