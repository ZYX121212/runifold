use std::{collections::BTreeMap, future::Future, pin::Pin};

use runifold_core::{
    ChildEvent, DomainEvent, EventId, Instant, LifecycleEvent, RetrySafety, RunContext, RunError,
    RunErrorKind, RunEventKind, Usage,
};
use serde_json::Value;

use crate::checkpoint::WorkflowCheckpointCursor;
use crate::parallel::execute_parallel;
use crate::race::execute_race;
use crate::remediation::{execute_repairable_node, prepare_remediation_resume};
use crate::workflow::WorkflowNodeKind;
use crate::{
    ParallelBranchCheckpoint, StepId, Workflow, WorkflowCheckpoint, WorkflowCheckpointPhase,
    WorkflowCheckpointState, WorkflowError, WorkflowInterruptDecision, WorkflowInterruptOutcome,
    WorkflowInterruptRequest, WorkflowOutcome, WorkflowResumePolicy, WorkflowWait,
    WorkflowWaitOutcome, WorkflowWake,
};

/// A boxed, sendable workflow execution future.
#[cfg(not(target_arch = "wasm32"))]
pub type WorkflowFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A boxed workflow execution future on single-threaded WASM.
#[cfg(target_arch = "wasm32")]
pub type WorkflowFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

pub(crate) enum WorkflowExecution {
    Completed(WorkflowOutcome),
    Suspended(WorkflowWait),
}

impl WorkflowExecution {
    fn require_completed(self) -> Result<WorkflowOutcome, WorkflowError> {
        match self {
            Self::Completed(outcome) => Ok(outcome),
            Self::Suspended(_) => Err(WorkflowError::DurableWaitRequiresWorker),
        }
    }
}

impl Workflow {
    /// Executes this workflow from the first node.
    pub fn run<'a>(
        &'a self,
        input: impl Into<Value> + Send + 'a,
        run: &'a RunContext,
    ) -> WorkflowFuture<'a, Result<WorkflowOutcome, WorkflowError>> {
        let state = self.initial_state(input.into(), run.budget().usage());
        Box::pin(async move {
            self.execute_state(state, run, None)
                .await?
                .require_completed()
        })
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
            let mut cursor = WorkflowCheckpointCursor::create(checkpoint, run, &state).await?;
            self.execute_state(state, run, Some(&mut cursor))
                .await?
                .require_completed()
        })
    }

    pub(crate) fn run_checkpointed_controlled<'a>(
        &'a self,
        input: Value,
        run: &'a RunContext,
        checkpoint: &'a WorkflowCheckpoint,
    ) -> WorkflowFuture<'a, Result<WorkflowExecution, WorkflowError>> {
        Box::pin(async move {
            self.validate_authority(run)?;
            let state = self.initial_state(input, run.budget().usage());
            let mut cursor = WorkflowCheckpointCursor::create(checkpoint, run, &state).await?;
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
            self.resume_controlled(checkpoint, run, policy, None)
                .await?
                .require_completed()
        })
    }

    pub(crate) fn resume_controlled<'a>(
        &'a self,
        checkpoint: &'a WorkflowCheckpoint,
        run: &'a RunContext,
        policy: WorkflowResumePolicy,
        wake: Option<WorkflowWake>,
    ) -> WorkflowFuture<'a, Result<WorkflowExecution, WorkflowError>> {
        Box::pin(async move {
            let (envelope, mut state) = checkpoint.load_async().await?;
            self.validate_checkpoint_identity(&state)?;
            if let Some(outcome) = state.outcome() {
                validate_exact_usage(state.usage, run.budget().usage())?;
                return Ok(WorkflowExecution::Completed(outcome));
            }
            let mut waiting_wake = None;
            match &state.phase {
                WorkflowCheckpointPhase::StepInFlight { step } => {
                    if policy == WorkflowResumePolicy::RejectAmbiguous {
                        return Err(WorkflowError::AmbiguousCheckpoint { step: step.clone() });
                    }
                    validate_usage_floor(state.usage, run.budget().usage())?;
                    state.usage = run.budget().usage();
                    state.phase = WorkflowCheckpointPhase::Ready;
                }
                WorkflowCheckpointPhase::Remediating { .. } => {
                    prepare_remediation_resume(&mut state, run, policy)?;
                }
                WorkflowCheckpointPhase::Waiting { wait, .. } => {
                    validate_exact_usage(state.usage, run.budget().usage())?;
                    let wake = wake.ok_or(WorkflowError::DurableWaitRequiresWorker)?;
                    if !wake.matches(wait) {
                        return Err(WorkflowError::WakeMismatch);
                    }
                    waiting_wake = Some((wait.clone(), wake));
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
            if let Some((wait, wake)) = waiting_wake {
                let node = &self.nodes[state.next_index];
                let output = wake_output(&wait, wake, &state.value)?;
                commit_node(&mut state, &node.id, output, run, &mut Some(&mut cursor)).await?;
            }
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
    ) -> Result<WorkflowExecution, WorkflowError> {
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
    ) -> Result<WorkflowExecution, WorkflowError> {
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
                WorkflowNodeKind::Repairable(repairable) => {
                    execute_repairable_node(
                        &self.name, node, repairable, state, run, caused_by, checkpoint,
                    )
                    .await?
                }
                WorkflowNodeKind::Timer(wait)
                | WorkflowNodeKind::Signal(wait)
                | WorkflowNodeKind::SignalOrTimeout(wait) => {
                    state.phase = WorkflowCheckpointPhase::Waiting {
                        step: node.id.clone(),
                        wait: wait.clone(),
                    };
                    state.usage = run.budget().usage();
                    save_checkpoint(checkpoint, state).await?;
                    record_domain(
                        run,
                        "workflow.suspended",
                        serde_json::json!({
                            "workflow": self.name,
                            "step": node.id,
                            "wait": wait,
                        }),
                        caused_by,
                    )?;
                    return Ok(WorkflowExecution::Suspended(wait.clone()));
                }
                WorkflowNodeKind::Interrupt(prompt) => {
                    let wait = WorkflowWait::Interrupt {
                        request: WorkflowInterruptRequest::new(
                            prompt.clone(),
                            state.value.clone(),
                        )?,
                    };
                    state.phase = WorkflowCheckpointPhase::Waiting {
                        step: node.id.clone(),
                        wait: wait.clone(),
                    };
                    state.usage = run.budget().usage();
                    save_checkpoint(checkpoint, state).await?;
                    record_domain(
                        run,
                        "workflow.interrupted",
                        serde_json::json!({
                            "workflow": self.name,
                            "step": node.id,
                            "wait": wait,
                        }),
                        caused_by,
                    )?;
                    return Ok(WorkflowExecution::Suspended(wait));
                }
                _ => {
                    self.execute_serial_node(node, state, run, caused_by, checkpoint)
                        .await?
                }
            };
            commit_node(state, &node.id, output, run, checkpoint).await?;
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
        save_checkpoint(checkpoint, state).await?;
        Ok(WorkflowExecution::Completed(outcome))
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
        save_checkpoint(checkpoint, state).await?;

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
        let mut child = run.child(node.capabilities.clone()).map_err(|error| {
            WorkflowError::AuthorityEscalation {
                step: node.id.clone(),
                capability: error.capability,
            }
        })?;
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
                WorkflowNodeKind::Repairable(repairable) => {
                    if let Some(missing) = node
                        .capabilities
                        .first_missing_from(run.capabilities())
                        .or_else(|| {
                            repairable
                                .reviewer_capabilities
                                .first_missing_from(run.capabilities())
                        })
                    {
                        return Err(WorkflowError::AuthorityEscalation {
                            step: node.id.clone(),
                            capability: missing.name.clone(),
                        });
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
            WorkflowCheckpointPhase::Remediating {
                step,
                attempt,
                original_input,
                checkpoint,
            } => {
                let valid_attempt = *attempt > 0;
                let input_matches = *original_input == state.value;
                let node_matches = self.nodes.get(state.next_index).is_some_and(|node| {
                    node.id == *step && matches!(node.kind, WorkflowNodeKind::Repairable(_))
                });
                let checkpoint_matches = match checkpoint {
                    crate::WorkflowRemediationCheckpoint::GenerationReady { input }
                    | crate::WorkflowRemediationCheckpoint::GenerationInFlight { input } => {
                        *attempt > 1 || *input == *original_input
                    }
                    crate::WorkflowRemediationCheckpoint::ReviewReady { .. }
                    | crate::WorkflowRemediationCheckpoint::ReviewInFlight { .. }
                    | crate::WorkflowRemediationCheckpoint::Approved { .. }
                    | crate::WorkflowRemediationCheckpoint::Rejected { .. }
                    | crate::WorkflowRemediationCheckpoint::Exhausted { .. } => true,
                };
                valid_attempt && input_matches && node_matches && checkpoint_matches
            }
            WorkflowCheckpointPhase::Waiting { step, wait } => {
                self.nodes.get(state.next_index).is_some_and(|node| {
                    if node.id != *step {
                        return false;
                    }
                    matches!(
                        &node.kind,
                        WorkflowNodeKind::Timer(expected)
                            | WorkflowNodeKind::Signal(expected)
                            | WorkflowNodeKind::SignalOrTimeout(expected) if expected == wait
                    ) || matches!(
                        (&node.kind, wait),
                        (
                            WorkflowNodeKind::Interrupt(prompt),
                            WorkflowWait::Interrupt { request }
                        ) if request.prompt == *prompt && request.proposal == state.value
                    )
                })
            }
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

async fn commit_node(
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
    save_checkpoint(checkpoint, state).await
}

fn wake_output(
    wait: &WorkflowWait,
    wake: WorkflowWake,
    current: &Value,
) -> Result<Value, WorkflowError> {
    match (wait, wake) {
        (WorkflowWait::Timer { .. }, WorkflowWake::Timer) => Ok(current.clone()),
        (WorkflowWait::Signal { .. }, WorkflowWake::Signal { payload, .. }) => Ok(payload),
        (
            WorkflowWait::SignalOrTimeout { .. },
            WorkflowWake::Signal {
                signal_id,
                name,
                payload,
            },
        ) => Ok(serde_json::to_value(WorkflowWaitOutcome::Signal {
            signal_id,
            name,
            payload,
        })?),
        (WorkflowWait::SignalOrTimeout { .. }, WorkflowWake::Timeout) => {
            Ok(serde_json::to_value(WorkflowWaitOutcome::TimedOut)?)
        }
        (WorkflowWait::Interrupt { request }, WorkflowWake::Signal { name, payload, .. })
            if name == request.signal_name() =>
        {
            let decision: WorkflowInterruptDecision = serde_json::from_value(payload)?;
            decision.validate()?;
            let outcome = match decision {
                WorkflowInterruptDecision::Approve => WorkflowInterruptOutcome::Approved {
                    value: request.proposal.clone(),
                },
                WorkflowInterruptDecision::Edit { value } => {
                    WorkflowInterruptOutcome::Edited { value }
                }
                WorkflowInterruptDecision::Reject { reason } => {
                    WorkflowInterruptOutcome::Rejected { reason }
                }
            };
            Ok(serde_json::to_value(outcome)?)
        }
        _ => Err(WorkflowError::WakeMismatch),
    }
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

pub(crate) async fn save_checkpoint(
    checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
    state: &WorkflowCheckpointState,
) -> Result<(), WorkflowError> {
    if let Some(checkpoint) = checkpoint.as_deref_mut() {
        checkpoint.save(state).await?;
    }
    Ok(())
}

pub(crate) fn check_lifecycle(run: &RunContext) -> Result<(), WorkflowError> {
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

fn terminal_event(
    workflow: &str,
    result: &Result<WorkflowExecution, WorkflowError>,
) -> RunEventKind {
    match result {
        Ok(WorkflowExecution::Completed(outcome)) => {
            RunEventKind::Lifecycle(LifecycleEvent::Completed {
                output: serde_json::json!({
                    "workflow": workflow,
                    "steps": outcome.steps.len(),
                    "usage": outcome.usage,
                }),
            })
        }
        Ok(WorkflowExecution::Suspended(wait)) => {
            RunEventKind::Lifecycle(LifecycleEvent::Completed {
                output: serde_json::json!({
                    "workflow": workflow,
                    "state": "suspended",
                    "wait": wait,
                }),
            })
        }
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
        WorkflowError::DurableWaitRequiresWorker | WorkflowError::WakeMismatch => {
            (RunErrorKind::InvalidInput, RetrySafety::Safe)
        }
        WorkflowError::Budget(_) => (RunErrorKind::BudgetExceeded, RetrySafety::Safe),
        WorkflowError::Build(_)
        | WorkflowError::Wait(_)
        | WorkflowError::CheckpointIdentityMismatch
        | WorkflowError::CheckpointUsageMismatch => (RunErrorKind::InvalidInput, RetrySafety::Safe),
        WorkflowError::AmbiguousCheckpoint { .. }
        | WorkflowError::Serialization(_)
        | WorkflowError::Step { .. }
        | WorkflowError::Review { .. }
        | WorkflowError::RemediationRejected { .. }
        | WorkflowError::RemediationExhausted { .. }
        | WorkflowError::ParallelBranch { .. }
        | WorkflowError::RaceAllFailed { .. }
        | WorkflowError::ChildRun(_)
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

pub(crate) fn validate_exact_usage(expected: Usage, actual: Usage) -> Result<(), WorkflowError> {
    if expected != actual {
        return Err(WorkflowError::CheckpointUsageMismatch);
    }
    Ok(())
}

pub(crate) fn validate_usage_floor(floor: Usage, actual: Usage) -> Result<(), WorkflowError> {
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
