use std::{
    collections::BTreeMap,
    future::poll_fn,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

use futures_util::{
    StreamExt,
    future::{Either, select},
    stream::FuturesUnordered,
};
use runifold_core::{
    BudgetReservation, CancellationToken, ChildEvent, EventId, RunContext, RunEventKind, RunId,
};
use serde_json::Value;

use crate::checkpoint::WorkflowCheckpointCursor;
use crate::execution::{record_domain, save_checkpoint};
use crate::workflow::{ParallelBranch, WorkflowNode};
use crate::{
    ParallelBranchCheckpoint, StepId, WorkflowCheckpointPhase, WorkflowCheckpointState,
    WorkflowError, WorkflowStepError,
};

struct ActiveRaceBranch {
    run_id: RunId,
    caused_by: Option<EventId>,
    cancellation: CancellationToken,
    reservation: BudgetReservation,
}

type ActiveRaceBranches = BTreeMap<StepId, ActiveRaceBranch>;

pub(crate) async fn execute_race(
    workflow: &str,
    node: &WorkflowNode,
    branches: &[ParallelBranch],
    state: &mut WorkflowCheckpointState,
    run: &RunContext,
    step_started: Option<EventId>,
    checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
) -> Result<Value, WorkflowError> {
    let pending = prepare_race(branches, node, state, run, checkpoint).await?;
    if pending.is_empty() {
        return terminal_race_state(state, &node.id);
    }

    let mut futures = FuturesUnordered::new();
    let mut active = BTreeMap::new();
    let start_barrier = Arc::new(InitialPollBarrier::new(pending.len()));
    for (id, branch, reservation) in pending {
        let caused_by = start_branch(workflow, node, &id, run, step_started, &reservation)?;
        let mut child = run.child_reserved(branch.capabilities.clone(), &reservation)?;
        if let Some(event_id) = caused_by {
            child = child.with_cause(event_id);
        }
        run.record(
            RunEventKind::Child(ChildEvent::Started {
                child_run_id: child.run_id(),
            }),
            caused_by,
        )?;
        active.insert(
            id.clone(),
            ActiveRaceBranch {
                run_id: child.run_id(),
                caused_by,
                cancellation: child.cancellation().clone(),
                reservation,
            },
        );
        let input = state.value.clone();
        let start_barrier = start_barrier.clone();
        futures.push(async move {
            let result = execute_after_initial_poll(branch, input, &child, &start_barrier).await;
            (id, result)
        });
    }

    loop {
        let cancellation = run.cancellation().clone();
        let next = futures.next();
        let completed = match select(Box::pin(cancellation.cancelled()), Box::pin(next)).await {
            Either::Left(((), pending_next)) => {
                drop(pending_next);
                abandon_active(workflow, node, state, run, &active)?;
                state.usage = run.budget().usage();
                save_checkpoint(checkpoint, state).await?;
                return Err(WorkflowError::Cancelled);
            }
            Either::Right((completed, _)) => completed,
        };
        let Some((id, result)) = completed else {
            break;
        };
        let Some(completed) = active.remove(&id) else {
            abandon_active(workflow, node, state, run, &active)?;
            return Err(WorkflowError::CheckpointIdentityMismatch);
        };
        match result {
            Ok(output) => {
                return record_winner(
                    workflow,
                    node,
                    state,
                    run,
                    checkpoint,
                    &active,
                    RaceWinner {
                        id,
                        branch: completed,
                        output,
                    },
                )
                .await;
            }
            Err(source) => {
                if let Err(error) = record_failure(
                    workflow,
                    node,
                    state,
                    run,
                    checkpoint,
                    &RaceFailure {
                        id,
                        branch: completed,
                        source,
                    },
                )
                .await
                {
                    abandon_active(workflow, node, state, run, &active)?;
                    return Err(error);
                }
            }
        }
    }

    terminal_race_state(state, &node.id)
}

async fn execute_after_initial_poll(
    branch: &ParallelBranch,
    input: Value,
    child: &RunContext,
    barrier: &InitialPollBarrier,
) -> Result<Value, WorkflowStepError> {
    let mut execution = branch.step.execute(input, child);
    let mut arrived = false;
    let mut immediate = None;
    poll_fn(move |context| {
        if !arrived {
            if let Poll::Ready(result) = execution.as_mut().poll(context) {
                immediate = Some(result);
            }
            arrived = true;
            barrier.arrive(context.waker());
        }
        if !barrier.is_open() {
            barrier.register(context.waker());
            return Poll::Pending;
        }
        immediate
            .take()
            .map_or_else(|| execution.as_mut().poll(context), Poll::Ready)
    })
    .await
}

struct InitialPollBarrier {
    state: Mutex<InitialPollBarrierState>,
}

struct InitialPollBarrierState {
    remaining: usize,
    waiters: Vec<Waker>,
}

impl InitialPollBarrier {
    fn new(branches: usize) -> Self {
        Self {
            state: Mutex::new(InitialPollBarrierState {
                remaining: branches,
                waiters: Vec::with_capacity(branches),
            }),
        }
    }

    fn arrive(&self, waker: &Waker) {
        let waiters = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.remaining = state.remaining.saturating_sub(1);
            if state.remaining == 0 {
                std::mem::take(&mut state.waiters)
            } else {
                state.waiters.push(waker.clone());
                Vec::new()
            }
        };
        for waiter in waiters {
            waiter.wake();
        }
    }

    fn register(&self, waker: &Waker) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.remaining != 0 {
            state.waiters.push(waker.clone());
        }
    }

    fn is_open(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remaining
            == 0
    }
}

async fn prepare_race<'a>(
    branches: &'a [ParallelBranch],
    node: &WorkflowNode,
    state: &mut WorkflowCheckpointState,
    run: &RunContext,
    checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
) -> Result<Vec<(StepId, &'a ParallelBranch, BudgetReservation)>, WorkflowError> {
    let previous = match &state.phase {
        WorkflowCheckpointPhase::RaceInFlight {
            step,
            branches: progress,
        } if step == &node.id => progress.clone(),
        _ => BTreeMap::new(),
    };
    if has_winner(&previous) || (!previous.is_empty() && all_failed(&previous)) {
        return Ok(Vec::new());
    }

    let mut progress = BTreeMap::new();
    let mut pending = Vec::new();
    for branch in branches {
        let id = branch_id(branch)?;
        if let Some(failed @ ParallelBranchCheckpoint::Failed { .. }) = previous.get(&id) {
            progress.insert(id, failed.clone());
        } else {
            progress.insert(id.clone(), ParallelBranchCheckpoint::InFlight);
            pending.push((id, branch));
        }
    }
    let reservations = run
        .budget()
        .try_reserve_batch(pending.iter().map(|(_, branch)| branch.reservation))?;
    state.phase = WorkflowCheckpointPhase::RaceInFlight {
        step: node.id.clone(),
        branches: progress,
    };
    state.usage = run.budget().usage();
    save_checkpoint(checkpoint, state).await?;
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
        "race.branch.started",
        serde_json::json!({
            "workflow": workflow,
            "step": node.id,
            "branch": id,
            "reservation": reservation.reserved(),
        }),
        step_started,
    )
}

struct RaceWinner {
    id: StepId,
    branch: ActiveRaceBranch,
    output: Value,
}

async fn record_winner(
    workflow: &str,
    node: &WorkflowNode,
    state: &mut WorkflowCheckpointState,
    run: &RunContext,
    checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
    active: &ActiveRaceBranches,
    winner: RaceWinner,
) -> Result<Value, WorkflowError> {
    set_race_state(
        state,
        &node.id,
        &winner.id,
        ParallelBranchCheckpoint::Completed {
            output: winner.output.clone(),
        },
    )?;
    abandon_active(workflow, node, state, run, active)?;
    run.record(
        RunEventKind::Child(ChildEvent::Completed {
            child_run_id: winner.branch.run_id,
        }),
        winner.branch.caused_by,
    )?;
    record_domain(
        run,
        "race.branch.won",
        serde_json::json!({
            "workflow": workflow,
            "step": node.id,
            "branch": winner.id,
        }),
        winner.branch.caused_by,
    )?;
    state.usage = run.budget().usage();
    save_checkpoint(checkpoint, state).await?;
    Ok(winner.output)
}

struct RaceFailure {
    id: StepId,
    branch: ActiveRaceBranch,
    source: WorkflowStepError,
}

async fn record_failure(
    workflow: &str,
    node: &WorkflowNode,
    state: &mut WorkflowCheckpointState,
    run: &RunContext,
    checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
    failure: &RaceFailure,
) -> Result<(), WorkflowError> {
    set_race_state(
        state,
        &node.id,
        &failure.id,
        ParallelBranchCheckpoint::Failed {
            message: failure.source.to_string(),
        },
    )?;
    run.record(
        RunEventKind::Child(ChildEvent::Failed {
            child_run_id: failure.branch.run_id,
        }),
        failure.branch.caused_by,
    )?;
    record_domain(
        run,
        "race.branch.failed",
        serde_json::json!({
            "workflow": workflow,
            "step": node.id,
            "branch": failure.id,
        }),
        failure.branch.caused_by,
    )?;
    state.usage = run.budget().usage();
    save_checkpoint(checkpoint, state).await
}

fn abandon_active(
    workflow: &str,
    node: &WorkflowNode,
    state: &mut WorkflowCheckpointState,
    run: &RunContext,
    active: &ActiveRaceBranches,
) -> Result<(), WorkflowError> {
    for (id, branch) in active {
        branch.cancellation.cancel();
        branch.reservation.forfeit_remaining()?;
        set_race_state(state, &node.id, id, ParallelBranchCheckpoint::Cancelled)?;
    }
    for (id, branch) in active {
        run.record(
            RunEventKind::Child(ChildEvent::Cancelled {
                child_run_id: branch.run_id,
            }),
            branch.caused_by,
        )?;
        record_domain(
            run,
            "race.branch.cancelled",
            serde_json::json!({
                "workflow": workflow,
                "step": node.id,
                "branch": id,
                "forfeited_reservation": branch.reservation.reserved(),
            }),
            branch.caused_by,
        )?;
    }
    Ok(())
}

fn terminal_race_state(
    state: &WorkflowCheckpointState,
    step: &StepId,
) -> Result<Value, WorkflowError> {
    let WorkflowCheckpointPhase::RaceInFlight {
        step: active_step,
        branches,
    } = &state.phase
    else {
        return Err(WorkflowError::CheckpointIdentityMismatch);
    };
    if active_step != step {
        return Err(WorkflowError::CheckpointIdentityMismatch);
    }
    let mut winners = branches.values().filter_map(|branch| match branch {
        ParallelBranchCheckpoint::Completed { output } => Some(output),
        _ => None,
    });
    if let Some(output) = winners.next() {
        if winners.next().is_some() {
            return Err(WorkflowError::CheckpointIdentityMismatch);
        }
        return Ok(output.clone());
    }
    let failures = branches
        .iter()
        .filter_map(|(id, branch)| match branch {
            ParallelBranchCheckpoint::Failed { message } => Some((id.clone(), message.clone())),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    if failures.len() == branches.len() {
        return Err(WorkflowError::RaceAllFailed {
            step: step.clone(),
            failures,
        });
    }
    Err(WorkflowError::CheckpointIdentityMismatch)
}

fn set_race_state(
    state: &mut WorkflowCheckpointState,
    step: &StepId,
    branch: &StepId,
    branch_state: ParallelBranchCheckpoint,
) -> Result<(), WorkflowError> {
    let WorkflowCheckpointPhase::RaceInFlight {
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

fn has_winner(branches: &BTreeMap<StepId, ParallelBranchCheckpoint>) -> bool {
    branches
        .values()
        .any(|branch| matches!(branch, ParallelBranchCheckpoint::Completed { .. }))
}

fn all_failed(branches: &BTreeMap<StepId, ParallelBranchCheckpoint>) -> bool {
    branches
        .values()
        .all(|branch| matches!(branch, ParallelBranchCheckpoint::Failed { .. }))
}

fn branch_id(branch: &ParallelBranch) -> Result<StepId, WorkflowError> {
    StepId::parse(branch.id.clone()).map_err(|_| WorkflowError::CheckpointIdentityMismatch)
}
