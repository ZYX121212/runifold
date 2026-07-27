use std::{collections::BTreeMap, future::Future, pin::Pin, time::Instant};

use runifold_core::{
    ChildEvent, DomainEvent, EventId, LifecycleEvent, RetrySafety, RunContext, RunError,
    RunErrorKind, RunEventKind, Usage,
};
use serde_json::Value;

use crate::checkpoint::WorkflowCheckpointCursor;
use crate::parallel::execute_parallel;
use crate::race::execute_race;
use crate::workflow::WorkflowNodeKind;
use crate::{
    ParallelBranchCheckpoint, StepId, Workflow, WorkflowCheckpoint, WorkflowCheckpointPhase,
    WorkflowCheckpointState, WorkflowError, WorkflowOutcome, WorkflowResumePolicy,
};

/// A boxed, sendable workflow execution future.
pub type WorkflowFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

impl Workflow {
    /// Executes this workflow from the first node.
    pub fn run<'a>(
        &'a self,
        input: impl Into<Value> + Send + 'a,
        run: &'a RunContext,
    ) -> WorkflowFuture<'a, Result<WorkflowOutcome, WorkflowError>> {
        let state = self.initial_state(input.into(), run.budget().usage());
        Box::pin(async move { self.execute_state(state, run, None).await })
    }

    /// Executes with write-ahead workflow checkpoint persistence.
    pub fn run_checkpointed<'a>(
        &'a self,
        input: impl Into<Value> + Send + 'a,
        run: &'a RunContext,
        checkpoint: &'a WorkflowCheckpoint,
    ) -> WorkflowFuture<'a, Result<WorkflowOutcome, WorkflowError>> {
        Box::pin(async move {
            self.validate_authority(run)?;
            let state = self.initial_state(input.into(), run.budget().usage());
            let mut cursor = WorkflowCheckpointCursor::create(checkpoint, run, &state)?;
            self.execute_state(state, run, Some(&mut cursor)).await
        })
    }

    /// Resumes a persisted workflow execution.
    pub fn resume<'a>(
        &'a self,
        checkpoint: &'a WorkflowCheckpoint,
        run: &'a RunContext,
        policy: WorkflowResumePolicy,
    ) -> WorkflowFuture<'a, Result<WorkflowOutcome, WorkflowError>> {
        Box::pin(async move {
            let (envelope, mut state) = checkpoint.load()?;
            self.validate_checkpoint_identity(&state)?;
            if let Some(outcome) = state.outcome() {
                validate_exact_usage(state.usage, run.budget().usage())?;
                return Ok(outcome);
            }
            match &state.phase {
                WorkflowCheckpointPhase::StepInFlight { step } => {
                    if policy == WorkflowResumePolicy::RejectAmbiguous {
                        return Err(WorkflowError::AmbiguousCheckpoint { step: step.clone() });
                    }
                    validate_usage_floor(state.usage, run.budget().usage())?;
                    state.usage = run.budget().usage();
                    state.phase = WorkflowCheckpointPhase::Ready;
                }
                WorkflowCheckpointPhase::ParallelInFlight { step, branches } => {
                    let all_completed = branches
                        .values()
                        .all(|branch| matches!(branch, ParallelBranchCheckpoint::Completed { .. }));
                    if !all_completed && policy == WorkflowResumePolicy::RejectAmbiguous {
                        return Err(WorkflowError::AmbiguousCheckpoint { step: step.clone() });
                    }
                    if all_completed {
                        validate_exact_usage(state.usage, run.budget().usage())?;
                    } else {
                        validate_usage_floor(state.usage, run.budget().usage())?;
                        state.usage = run.budget().usage();
                    }
                }
                WorkflowCheckpointPhase::RaceInFlight { step, branches } => {
                    let has_winner = branches
                        .values()
                        .any(|branch| matches!(branch, ParallelBranchCheckpoint::Completed { .. }));
                    let all_failed = branches
                        .values()
                        .all(|branch| matches!(branch, ParallelBranchCheckpoint::Failed { .. }));
                    if !has_winner && !all_failed && policy == WorkflowResumePolicy::RejectAmbiguous
                    {
                        return Err(WorkflowError::AmbiguousCheckpoint { step: step.clone() });
                    }
                    if has_winner || all_failed {
                        validate_exact_usage(state.usage, run.budget().usage())?;
                    } else {
                        validate_usage_floor(state.usage, run.budget().usage())?;
                        state.usage = run.budget().usage();
                    }
                }
                WorkflowCheckpointPhase::Ready => {
                    validate_exact_usage(state.usage, run.budget().usage())?;
                }
                WorkflowCheckpointPhase::Completed { .. } => {
                    unreachable!("completed workflow checkpoints return before phase recovery")
                }
            }
            let mut cursor = WorkflowCheckpointCursor::loaded(checkpoint, envelope);
            self.execute_state(state, run, Some(&mut cursor)).await
        })
    }

    fn initial_state(&self, input: Value, usage: Usage) -> WorkflowCheckpointState {
        WorkflowCheckpointState {
            workflow: self.name.clone(),
            workflow_version: self.version,
            layout: self.step_ids().cloned().collect(),
            next_index: 0,
            value: input,
            outputs: BTreeMap::new(),
            usage,
            phase: WorkflowCheckpointPhase::Ready,
        }
    }

    async fn execute_state(
        &self,
        mut state: WorkflowCheckpointState,
        run: &RunContext,
        mut checkpoint: Option<&mut WorkflowCheckpointCursor>,
    ) -> Result<WorkflowOutcome, WorkflowError> {
        self.validate_authority(run)?;
        let started = run
            .record(
                RunEventKind::Lifecycle(LifecycleEvent::Started),
                run.caused_by(),
            )?
            .map(|event| event.meta.event_id);
        let result = self
            .run_loop(&mut state, run, started, &mut checkpoint)
            .await;
        run.record(terminal_event(&self.name, &result), started)?;
        result
    }

    async fn run_loop(
        &self,
        state: &mut WorkflowCheckpointState,
        run: &RunContext,
        caused_by: Option<EventId>,
        checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
    ) -> Result<WorkflowOutcome, WorkflowError> {
        while state.next_index < self.nodes.len() {
            check_lifecycle(run)?;
            let node = &self.nodes[state.next_index];
            let output = match &node.kind {
                WorkflowNodeKind::Parallel(branches) => {
                    self.execute_parallel_node(node, branches, state, run, caused_by, checkpoint)
                        .await?
                }
                WorkflowNodeKind::Race(branches) => {
                    self.execute_race_node(node, branches, state, run, caused_by, checkpoint)
                        .await?
                }
                _ => {
                    self.execute_serial_node(node, state, run, caused_by, checkpoint)
                        .await?
                }
            };
            commit_node(state, &node.id, output, run, checkpoint)?;
        }

        let outcome = WorkflowOutcome {
            output: state.value.clone(),
            steps: state.outputs.clone(),
            usage: run.budget().usage(),
        };
        state.usage = outcome.usage;
        state.phase = WorkflowCheckpointPhase::Completed {
            outcome: outcome.clone(),
        };
        save_checkpoint(checkpoint, state)?;
        Ok(outcome)
    }

    async fn execute_serial_node(
        &self,
        node: &crate::workflow::WorkflowNode,
        state: &mut WorkflowCheckpointState,
        run: &RunContext,
        caused_by: Option<EventId>,
        checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
    ) -> Result<Value, WorkflowError> {
        state.phase = WorkflowCheckpointPhase::StepInFlight {
            step: node.id.clone(),
        };
        state.usage = run.budget().usage();
        save_checkpoint(checkpoint, state)?;

        let step_started = record_domain(
            run,
            "step.started",
            serde_json::json!({
                "workflow": self.name,
                "step": node.id,
                "index": state.next_index,
            }),
            caused_by,
        )?;
        let mut child = run.child(node.capabilities.clone());
        if let Some(event_id) = step_started {
            child = child.with_cause(event_id);
        }
        run.record(
            RunEventKind::Child(ChildEvent::Started {
                child_run_id: child.run_id(),
            }),
            step_started,
        )?;

        let execution = node.execute(state.value.clone(), &child).await;
        let (output, branch) = match execution {
            Ok(result) => result,
            Err(source) => {
                run.record(
                    RunEventKind::Child(ChildEvent::Failed {
                        child_run_id: child.run_id(),
                    }),
                    step_started,
                )?;
                record_domain(
                    run,
                    "step.failed",
                    serde_json::json!({
                        "workflow": self.name,
                        "step": node.id,
                    }),
                    step_started,
                )?;
                return Err(WorkflowError::Step {
                    step: node.id.clone(),
                    source: Box::new(source),
                });
            }
        };

        run.record(
            RunEventKind::Child(ChildEvent::Completed {
                child_run_id: child.run_id(),
            }),
            step_started,
        )?;
        record_domain(
            run,
            "step.completed",
            serde_json::json!({
                "workflow": self.name,
                "step": node.id,
                "branch": branch,
            }),
            step_started,
        )?;

        Ok(output)
    }

    async fn execute_parallel_node(
        &self,
        node: &crate::workflow::WorkflowNode,
        branches: &[crate::ParallelBranch],
        state: &mut WorkflowCheckpointState,
        run: &RunContext,
        caused_by: Option<EventId>,
        checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
    ) -> Result<Value, WorkflowError> {
        let step_started = record_domain(
            run,
            "step.started",
            serde_json::json!({
                "workflow": self.name,
                "step": node.id,
                "index": state.next_index,
                "kind": "parallel",
            }),
            caused_by,
        )?;
        let result = execute_parallel(
            &self.name,
            node,
            branches,
            state,
            run,
            step_started,
            checkpoint,
        )
        .await;
        let output = match result {
            Ok(output) => output,
            Err(error) => {
                record_domain(
                    run,
                    "step.failed",
                    serde_json::json!({
                        "workflow": self.name,
                        "step": node.id,
                    }),
                    step_started,
                )?;
                return Err(error);
            }
        };
        record_domain(
            run,
            "step.completed",
            serde_json::json!({
                "workflow": self.name,
                "step": node.id,
                "kind": "parallel",
            }),
            step_started,
        )?;
        Ok(output)
    }

    async fn execute_race_node(
        &self,
        node: &crate::workflow::WorkflowNode,
        branches: &[crate::ParallelBranch],
        state: &mut WorkflowCheckpointState,
        run: &RunContext,
        caused_by: Option<EventId>,
        checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
    ) -> Result<Value, WorkflowError> {
        let step_started = record_domain(
            run,
            "step.started",
            serde_json::json!({
                "workflow": self.name,
                "step": node.id,
                "index": state.next_index,
                "kind": "race",
            }),
            caused_by,
        )?;
        let result = execute_race(
            &self.name,
            node,
            branches,
            state,
            run,
            step_started,
            checkpoint,
        )
        .await;
        match result {
            Ok(output) => {
                record_domain(
                    run,
                    "step.completed",
                    serde_json::json!({
                        "workflow": self.name,
                        "step": node.id,
                        "kind": "race",
                    }),
                    step_started,
                )?;
                Ok(output)
            }
            Err(error) => {
                record_domain(
                    run,
                    "step.failed",
                    serde_json::json!({
                        "workflow": self.name,
                        "step": node.id,
                        "kind": "race",
                    }),
                    step_started,
                )?;
                Err(error)
            }
        }
    }

    fn validate_authority(&self, run: &RunContext) -> Result<(), WorkflowError> {
        for node in self.nodes.iter() {
            match &node.kind {
                WorkflowNodeKind::Parallel(branches) | WorkflowNodeKind::Race(branches) => {
                    for branch in branches.iter() {
                        if let Some(missing) =
                            branch.capabilities.first_missing_from(run.capabilities())
                        {
                            return Err(WorkflowError::AuthorityEscalation {
                                step: node.id.clone(),
                                capability: missing.name.clone(),
                            });
                        }
                    }
                }
                _ => {
                    if let Some(missing) = node.capabilities.first_missing_from(run.capabilities())
                    {
                        return Err(WorkflowError::AuthorityEscalation {
                            step: node.id.clone(),
                            capability: missing.name.clone(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_checkpoint_identity(
        &self,
        state: &WorkflowCheckpointState,
    ) -> Result<(), WorkflowError> {
        let layout_matches = self.step_ids().eq(state.layout.iter());
        let completed_layout = &state.layout[..state.next_index.min(state.layout.len())];
        let outputs_match = state.outputs.len() == state.next_index
            && completed_layout
                .iter()
                .all(|step| state.outputs.contains_key(step));
        let phase_matches = match &state.phase {
            WorkflowCheckpointPhase::Ready => state.next_index <= self.nodes.len(),
            WorkflowCheckpointPhase::StepInFlight { step } => self
                .nodes
                .get(state.next_index)
                .is_some_and(|node| node.id == *step),
            WorkflowCheckpointPhase::ParallelInFlight { step, branches } => {
                self.nodes.get(state.next_index).is_some_and(|node| {
                    node.id == *step && parallel_layout_matches(&node.kind, branches)
                })
            }
            WorkflowCheckpointPhase::RaceInFlight { step, branches } => self
                .nodes
                .get(state.next_index)
                .is_some_and(|node| node.id == *step && race_layout_matches(&node.kind, branches)),
            WorkflowCheckpointPhase::Completed { outcome } => {
                state.next_index == self.nodes.len()
                    && outcome.output == state.value
                    && outcome.steps == state.outputs
                    && outcome.usage == state.usage
            }
        };
        if state.workflow != self.name
            || state.workflow_version != self.version
            || !layout_matches
            || state.next_index > self.nodes.len()
            || !outputs_match
            || !phase_matches
        {
            return Err(WorkflowError::CheckpointIdentityMismatch);
        }
        Ok(())
    }
}

fn commit_node(
    state: &mut WorkflowCheckpointState,
    step: &StepId,
    output: Value,
    run: &RunContext,
    checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
) -> Result<(), WorkflowError> {
    state.outputs.insert(step.clone(), output.clone());
    state.value = output;
    state.next_index += 1;
    state.usage = run.budget().usage();
    state.phase = WorkflowCheckpointPhase::Ready;
    save_checkpoint(checkpoint, state)
}

fn parallel_layout_matches(
    kind: &WorkflowNodeKind,
    checkpoint_branches: &BTreeMap<StepId, ParallelBranchCheckpoint>,
) -> bool {
    let WorkflowNodeKind::Parallel(branches) = kind else {
        return false;
    };
    branches.len() == checkpoint_branches.len()
        && branches.iter().all(|branch| {
            checkpoint_branches
                .keys()
                .any(|checkpoint| checkpoint.as_str() == branch.id)
        })
}

fn race_layout_matches(
    kind: &WorkflowNodeKind,
    checkpoint_branches: &BTreeMap<StepId, ParallelBranchCheckpoint>,
) -> bool {
    let WorkflowNodeKind::Race(branches) = kind else {
        return false;
    };
    let completed = checkpoint_branches
        .values()
        .filter(|branch| matches!(branch, ParallelBranchCheckpoint::Completed { .. }))
        .count();
    let winner_is_terminal = completed == 0
        || checkpoint_branches.values().all(|branch| {
            matches!(
                branch,
                ParallelBranchCheckpoint::Completed { .. }
                    | ParallelBranchCheckpoint::Failed { .. }
                    | ParallelBranchCheckpoint::Cancelled
            )
        });
    completed <= 1
        && winner_is_terminal
        && branches.len() == checkpoint_branches.len()
        && branches.iter().all(|branch| {
            checkpoint_branches
                .keys()
                .any(|checkpoint| checkpoint.as_str() == branch.id)
        })
}

pub(crate) fn save_checkpoint(
    checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
    state: &WorkflowCheckpointState,
) -> Result<(), WorkflowError> {
    if let Some(checkpoint) = checkpoint.as_deref_mut() {
        checkpoint.save(state)?;
    }
    Ok(())
}

fn check_lifecycle(run: &RunContext) -> Result<(), WorkflowError> {
    if run.cancellation().is_cancelled() {
        return Err(WorkflowError::Cancelled);
    }
    if run
        .deadline()
        .is_some_and(|deadline| deadline <= Instant::now())
    {
        return Err(WorkflowError::DeadlineExceeded);
    }
    Ok(())
}

pub(crate) fn record_domain(
    run: &RunContext,
    name: &str,
    payload: Value,
    caused_by: Option<EventId>,
) -> Result<Option<EventId>, WorkflowError> {
    Ok(run
        .record(
            RunEventKind::Domain(DomainEvent {
                namespace: "runifold.workflow".into(),
                name: name.into(),
                payload,
            }),
            caused_by,
        )?
        .map(|event| event.meta.event_id))
}

fn terminal_event(workflow: &str, result: &Result<WorkflowOutcome, WorkflowError>) -> RunEventKind {
    match result {
        Ok(outcome) => RunEventKind::Lifecycle(LifecycleEvent::Completed {
            output: serde_json::json!({
                "workflow": workflow,
                "steps": outcome.steps.len(),
                "usage": outcome.usage,
            }),
        }),
        Err(WorkflowError::Cancelled) => RunEventKind::Lifecycle(LifecycleEvent::Cancelled),
        Err(error) => RunEventKind::Lifecycle(LifecycleEvent::Failed {
            error: workflow_run_error(error),
        }),
    }
}

fn workflow_run_error(error: &WorkflowError) -> RunError {
    let (kind, retry_safety) = match error {
        WorkflowError::AuthorityEscalation { .. } => {
            (RunErrorKind::CapabilityDenied, RetrySafety::Safe)
        }
        WorkflowError::Cancelled => (RunErrorKind::Cancelled, RetrySafety::Safe),
        WorkflowError::DeadlineExceeded => (RunErrorKind::DeadlineExceeded, RetrySafety::Unknown),
        WorkflowError::Budget(_) => (RunErrorKind::BudgetExceeded, RetrySafety::Safe),
        WorkflowError::Build(_)
        | WorkflowError::CheckpointIdentityMismatch
        | WorkflowError::CheckpointUsageMismatch => (RunErrorKind::InvalidInput, RetrySafety::Safe),
        WorkflowError::AmbiguousCheckpoint { .. }
        | WorkflowError::Step { .. }
        | WorkflowError::ParallelBranch { .. }
        | WorkflowError::RaceAllFailed { .. }
        | WorkflowError::BudgetReservation(_)
        | WorkflowError::Journal(_)
        | WorkflowError::Checkpoint(_) => (RunErrorKind::Invocation, RetrySafety::Unknown),
    };
    RunError {
        kind,
        message: error.to_string(),
        retry_safety,
        metadata: BTreeMap::new(),
    }
}

fn validate_exact_usage(expected: Usage, actual: Usage) -> Result<(), WorkflowError> {
    if expected != actual {
        return Err(WorkflowError::CheckpointUsageMismatch);
    }
    Ok(())
}

fn validate_usage_floor(floor: Usage, actual: Usage) -> Result<(), WorkflowError> {
    let covers = actual.tokens >= floor.tokens
        && actual.cost_microusd >= floor.cost_microusd
        && actual.duration_micros >= floor.duration_micros
        && actual.turns >= floor.turns
        && actual.tool_calls >= floor.tool_calls
        && actual.delegations >= floor.delegations;
    if !covers {
        return Err(WorkflowError::CheckpointUsageMismatch);
    }
    Ok(())
}
