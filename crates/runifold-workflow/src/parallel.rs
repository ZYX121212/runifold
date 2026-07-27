use std::collections::BTreeMap;

use futures_util::{StreamExt, stream::FuturesUnordered};
use runifold_core::{
    BudgetReservation, CancellationToken, ChildEvent, EventId, RunContext, RunEventKind, RunId,
};
use serde_json::{Map, Value};

use crate::checkpoint::WorkflowCheckpointCursor;
use crate::execution::{record_domain, save_checkpoint};
use crate::workflow::{ParallelBranch, WorkflowNode};
use crate::{
    ParallelBranchCheckpoint, StepId, WorkflowCheckpointPhase, WorkflowCheckpointState,
    WorkflowError, WorkflowStepError,
};

type ActiveBranches = BTreeMap<StepId, (RunId, Option<EventId>, CancellationToken)>;

pub(crate) async fn execute_parallel(
    workflow: &str,
    node: &WorkflowNode,
    branches: &[ParallelBranch],
    state: &mut WorkflowCheckpointState,
    run: &RunContext,
    step_started: Option<EventId>,
    checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
) -> Result<Value, WorkflowError> {
    let pending = prepare_parallel(branches, node, state, run, checkpoint)?;
    let mut futures = FuturesUnordered::new();
    let mut active = BTreeMap::new();
    for (id, branch, reservation) in pending {
        let branch_started = start_branch(workflow, node, &id, run, step_started, &reservation)?;
        let mut child = run.child_reserved(branch.capabilities.clone(), &reservation)?;
        if let Some(event_id) = branch_started {
            child = child.with_cause(event_id);
        }
        run.record(
            RunEventKind::Child(ChildEvent::Started {
                child_run_id: child.run_id(),
            }),
            branch_started,
        )?;
        active.insert(
            id.clone(),
            (child.run_id(), branch_started, child.cancellation().clone()),
        );
        let input = state.value.clone();
        futures.push(async move {
            let result = branch.step.execute(input, &child).await;
            (id, child.run_id(), branch_started, result)
        });
    }

    while let Some((id, child_run_id, branch_started, result)) = futures.next().await {
        active.remove(&id);
        let output = match result {
            Ok(output) => output,
            Err(source) => {
                return record_failure(
                    workflow,
                    node,
                    state,
                    run,
                    checkpoint,
                    &active,
                    BranchFailure {
                        id,
                        child_run_id,
                        branch_started,
                        source,
                    },
                );
            }
        };
        if let Err(error) = record_success(
            workflow,
            node,
            state,
            run,
            checkpoint,
            BranchSuccess {
                id,
                child_run_id,
                branch_started,
                output,
            },
        ) {
            cancel_active(run, workflow, &node.id, &active)?;
            return Err(error);
        }
    }

    completed_output(state, &node.id)
}

fn prepare_parallel<'a>(
    branches: &'a [ParallelBranch],
    node: &WorkflowNode,
    state: &mut WorkflowCheckpointState,
    run: &RunContext,
    checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
) -> Result<Vec<(StepId, &'a ParallelBranch, BudgetReservation)>, WorkflowError> {
    let previous = match &state.phase {
        WorkflowCheckpointPhase::ParallelInFlight {
            step,
            branches: progress,
        } if step == &node.id => progress.clone(),
        _ => BTreeMap::new(),
    };
    let mut progress = BTreeMap::new();
    let mut pending = Vec::new();
    for branch in branches {
        let id = branch_id(branch)?;
        if let Some(ParallelBranchCheckpoint::Completed { output }) = previous.get(&id) {
            progress.insert(
                id,
                ParallelBranchCheckpoint::Completed {
                    output: output.clone(),
                },
            );
        } else {
            progress.insert(id.clone(), ParallelBranchCheckpoint::InFlight);
            pending.push((id, branch));
        }
    }
    let reservations = run
        .budget()
        .try_reserve_batch(pending.iter().map(|(_, branch)| branch.reservation))?;
    state.phase = WorkflowCheckpointPhase::ParallelInFlight {
        step: node.id.clone(),
        branches: progress,
    };
    state.usage = run.budget().usage();
    save_checkpoint(checkpoint, state)?;
    Ok(pending
        .into_iter()
        .zip(reservations)
        .map(|((id, branch), reservation)| (id, branch, reservation))
        .collect())
}

fn start_branch(
    workflow: &str,
    node: &WorkflowNode,
    id: &StepId,
    run: &RunContext,
    step_started: Option<EventId>,
    reservation: &BudgetReservation,
) -> Result<Option<EventId>, WorkflowError> {
    record_domain(
        run,
        "parallel.branch.started",
        serde_json::json!({
            "workflow": workflow,
            "step": node.id,
            "branch": id,
            "reservation": reservation.reserved(),
        }),
        step_started,
    )
}

struct BranchSuccess {
    id: StepId,
    child_run_id: RunId,
    branch_started: Option<EventId>,
    output: Value,
}

fn record_success(
    workflow: &str,
    node: &WorkflowNode,
    state: &mut WorkflowCheckpointState,
    run: &RunContext,
    checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
    success: BranchSuccess,
) -> Result<(), WorkflowError> {
    run.record(
        RunEventKind::Child(ChildEvent::Completed {
            child_run_id: success.child_run_id,
        }),
        success.branch_started,
    )?;
    record_domain(
        run,
        "parallel.branch.completed",
        serde_json::json!({
            "workflow": workflow,
            "step": node.id,
            "branch": success.id,
        }),
        success.branch_started,
    )?;
    set_branch_state(
        state,
        &node.id,
        &success.id,
        ParallelBranchCheckpoint::Completed {
            output: success.output,
        },
    )?;
    state.usage = run.budget().usage();
    save_checkpoint(checkpoint, state)
}

struct BranchFailure {
    id: StepId,
    child_run_id: RunId,
    branch_started: Option<EventId>,
    source: WorkflowStepError,
}

fn record_failure(
    workflow: &str,
    node: &WorkflowNode,
    state: &mut WorkflowCheckpointState,
    run: &RunContext,
    checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
    active: &ActiveBranches,
    failure: BranchFailure,
) -> Result<Value, WorkflowError> {
    cancel_tokens(active);
    run.record(
        RunEventKind::Child(ChildEvent::Failed {
            child_run_id: failure.child_run_id,
        }),
        failure.branch_started,
    )?;
    record_domain(
        run,
        "parallel.branch.failed",
        serde_json::json!({
            "workflow": workflow,
            "step": node.id,
            "branch": failure.id,
        }),
        failure.branch_started,
    )?;
    set_branch_state(
        state,
        &node.id,
        &failure.id,
        ParallelBranchCheckpoint::Failed {
            message: failure.source.to_string(),
        },
    )?;
    cancel_active(run, workflow, &node.id, active)?;
    state.usage = run.budget().usage();
    save_checkpoint(checkpoint, state)?;
    Err(WorkflowError::ParallelBranch {
        step: node.id.clone(),
        branch: failure.id,
        source: Box::new(failure.source),
    })
}

fn branch_id(branch: &ParallelBranch) -> Result<StepId, WorkflowError> {
    StepId::parse(branch.id.clone()).map_err(|_| WorkflowError::CheckpointIdentityMismatch)
}

fn set_branch_state(
    state: &mut WorkflowCheckpointState,
    step: &StepId,
    branch: &StepId,
    branch_state: ParallelBranchCheckpoint,
) -> Result<(), WorkflowError> {
    let WorkflowCheckpointPhase::ParallelInFlight {
        step: active_step,
        branches,
    } = &mut state.phase
    else {
        return Err(WorkflowError::CheckpointIdentityMismatch);
    };
    if active_step != step || !branches.contains_key(branch) {
        return Err(WorkflowError::CheckpointIdentityMismatch);
    }
    branches.insert(branch.clone(), branch_state);
    Ok(())
}

fn completed_output(
    state: &WorkflowCheckpointState,
    step: &StepId,
) -> Result<Value, WorkflowError> {
    let WorkflowCheckpointPhase::ParallelInFlight {
        step: active_step,
        branches,
    } = &state.phase
    else {
        return Err(WorkflowError::CheckpointIdentityMismatch);
    };
    if active_step != step {
        return Err(WorkflowError::CheckpointIdentityMismatch);
    }
    let mut output = Map::new();
    for (id, branch) in branches {
        let ParallelBranchCheckpoint::Completed { output: value } = branch else {
            return Err(WorkflowError::CheckpointIdentityMismatch);
        };
        output.insert(id.to_string(), value.clone());
    }
    Ok(Value::Object(output))
}

fn cancel_active(
    run: &RunContext,
    workflow: &str,
    step: &StepId,
    active: &ActiveBranches,
) -> Result<(), WorkflowError> {
    cancel_tokens(active);
    for (branch, (child_run_id, branch_started, _)) in active {
        run.record(
            RunEventKind::Child(ChildEvent::Cancelled {
                child_run_id: *child_run_id,
            }),
            *branch_started,
        )?;
        record_domain(
            run,
            "parallel.branch.cancelled",
            serde_json::json!({
                "workflow": workflow,
                "step": step,
                "branch": branch,
            }),
            *branch_started,
        )?;
    }
    Ok(())
}

fn cancel_tokens(active: &ActiveBranches) {
    for (_, _, cancellation) in active.values() {
        cancellation.cancel();
    }
}
