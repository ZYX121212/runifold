use std::{future::Future, pin::Pin, sync::Arc};

use runifold_core::{CapabilitySet, ChildEvent, EventId, RunContext, RunEventKind};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

use crate::checkpoint::WorkflowCheckpointCursor;
use crate::execution::{
    check_lifecycle, record_domain, save_checkpoint, validate_exact_usage, validate_usage_floor,
};
use crate::workflow::WorkflowNode;
use crate::{
    StepId, WorkflowCheckpointPhase, WorkflowCheckpointState, WorkflowError, WorkflowResumePolicy,
    WorkflowStep,
};

const MAX_REVIEW_REASON_BYTES: usize = 4_096;
const MAX_REVIEW_FEEDBACK_BYTES: usize = 1_048_576;

/// Bounded repair policy for one reviewable workflow step.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowRemediationPolicy {
    max_repairs: u32,
}

impl WorkflowRemediationPolicy {
    /// Creates a policy allowing at most `max_repairs` additional generations.
    pub const fn new(max_repairs: u32) -> Self {
        Self { max_repairs }
    }

    /// Returns the number of additional generations allowed after the first.
    pub const fn max_repairs(self) -> u32 {
        self.max_repairs
    }
}

/// Canonical request presented to an application-owned output reviewer.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkflowReviewRequest {
    /// Stable workflow node under review.
    pub step: StepId,
    /// One-based generation attempt.
    pub attempt: u32,
    /// Original value supplied to the repairable node.
    pub original_input: Value,
    /// Candidate produced by the current generation attempt.
    pub candidate: Value,
}

/// Application-owned decision over one generated candidate.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum WorkflowReviewVerdict {
    /// Accept the candidate as the node's stable output.
    Approve,
    /// Generate another candidate with structured reviewer feedback.
    Repair {
        /// Application-defined findings or repair instructions.
        feedback: Value,
    },
    /// Permanently reject the candidate.
    Reject {
        /// Bounded operator-facing explanation.
        reason: String,
    },
}

impl WorkflowReviewVerdict {
    /// Creates an approval decision.
    pub const fn approve() -> Self {
        Self::Approve
    }

    /// Creates a repair decision with application-defined feedback.
    ///
    /// # Errors
    ///
    /// Rejects feedback whose canonical JSON representation exceeds 1 MiB.
    pub fn repair(feedback: Value) -> Result<Self, WorkflowReviewError> {
        validate_feedback(&feedback)?;
        Ok(Self::Repair { feedback })
    }

    /// Creates a permanent rejection with a safe explanation.
    ///
    /// # Errors
    ///
    /// Rejects blank explanations and values above 4 KiB.
    pub fn reject(reason: impl Into<String>) -> Result<Self, WorkflowReviewError> {
        let reason = reason.into();
        validate_reason(&reason)?;
        Ok(Self::Reject { reason })
    }

    fn validate(&self) -> Result<(), WorkflowReviewError> {
        match self {
            Self::Approve => Ok(()),
            Self::Repair { feedback } => validate_feedback(feedback),
            Self::Reject { reason } => validate_reason(reason),
        }
    }
}

/// Failure returned by an application-owned output reviewer.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkflowReviewError {
    /// Reviewer feedback exceeded the durable checkpoint limit.
    #[error("workflow review feedback exceeds the 1 MiB durable limit")]
    FeedbackTooLarge,
    /// A rejection reason was blank or exceeded its durable limit.
    #[error("workflow review rejection reason must contain 1..=4096 bytes")]
    InvalidRejectionReason,
    /// The reviewer could not evaluate the candidate.
    #[error("workflow review failed: {0}")]
    Execution(String),
}

/// Boxed asynchronous reviewer result.
#[cfg(not(target_arch = "wasm32"))]
pub type WorkflowReviewFuture<'a> =
    Pin<Box<dyn Future<Output = Result<WorkflowReviewVerdict, WorkflowReviewError>> + Send + 'a>>;

/// Boxed asynchronous reviewer result on single-threaded WASM.
#[cfg(target_arch = "wasm32")]
pub type WorkflowReviewFuture<'a> =
    Pin<Box<dyn Future<Output = Result<WorkflowReviewVerdict, WorkflowReviewError>> + 'a>>;

/// Provider-neutral review boundary for a repairable workflow step.
pub trait WorkflowReviewer: Send + Sync {
    /// Reviews one candidate without owning remediation or retry policy.
    fn review<'a>(
        &'a self,
        request: WorkflowReviewRequest,
        run: &'a RunContext,
    ) -> WorkflowReviewFuture<'a>;
}

/// Structured input supplied to the second and later generation attempts.
///
/// `input` is a model-ready textual prompt, so an [`crate::AgentStep`] can
/// consume this value directly. Custom steps can instead inspect the complete
/// original input, previous candidate, feedback, and attempt fields.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkflowRepairInput {
    /// Model-ready prompt containing trusted runtime repair context.
    pub input: String,
    /// Original value supplied to the repairable node.
    pub original_input: Value,
    /// Candidate rejected by the preceding review.
    pub previous_candidate: Value,
    /// Application-defined reviewer findings or repair instructions.
    pub feedback: Value,
    /// One-based generation attempt receiving this input.
    pub attempt: u32,
}

impl WorkflowRepairInput {
    fn new(
        original_input: Value,
        previous_candidate: Value,
        feedback: Value,
        attempt: u32,
    ) -> Self {
        let original_prompt = model_prompt(&original_input);
        let model_candidate = model_visible_candidate(&previous_candidate);
        let instruction = json!({
            "attempt": attempt,
            "previous_candidate": model_candidate,
            "feedback": feedback,
            "instruction": "Produce a corrected candidate that addresses the reviewer feedback. Do not claim the previous candidate was accepted.",
        });
        let input = format!(
            "{original_prompt}\n<runifold_workflow_repair trust=\"runtime\">{instruction}</runifold_workflow_repair>"
        );
        Self {
            input,
            original_input,
            previous_candidate,
            feedback,
            attempt,
        }
    }
}

/// Durable substate of one repairable workflow node.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
#[non_exhaustive]
pub enum WorkflowRemediationCheckpoint {
    /// The next generation input is durable and has not started.
    GenerationReady {
        /// Canonical input for the pending generation.
        input: Value,
    },
    /// A generation may have partially executed.
    GenerationInFlight {
        /// Canonical input supplied to the generation attempt.
        input: Value,
    },
    /// A generated candidate is durable and has not entered review.
    ReviewReady {
        /// Durable candidate awaiting review.
        candidate: Value,
    },
    /// Review of a durable candidate may have partially executed.
    ReviewInFlight {
        /// Durable candidate supplied to the reviewer.
        candidate: Value,
    },
    /// Review accepted a durable candidate before the outer node commit.
    Approved {
        /// Accepted output awaiting the outer node commit.
        output: Value,
    },
    /// Review permanently rejected a candidate.
    Rejected {
        /// Candidate rejected by the reviewer.
        candidate: Value,
        /// Safe reviewer explanation.
        reason: String,
    },
    /// The configured repair limit was exhausted.
    Exhausted {
        /// Last generated candidate.
        candidate: Value,
        /// Feedback that would have required another generation.
        feedback: Value,
    },
}

pub(crate) struct RepairableNode {
    pub(crate) generator: Arc<dyn WorkflowStep>,
    pub(crate) reviewer: Arc<dyn WorkflowReviewer>,
    pub(crate) reviewer_capabilities: CapabilitySet,
    pub(crate) policy: WorkflowRemediationPolicy,
}

pub(crate) fn prepare_remediation_resume(
    state: &mut WorkflowCheckpointState,
    run: &RunContext,
    policy: WorkflowResumePolicy,
) -> Result<(), WorkflowError> {
    let WorkflowCheckpointPhase::Remediating {
        step,
        attempt,
        original_input,
        checkpoint,
    } = state.phase.clone()
    else {
        return Err(WorkflowError::CheckpointIdentityMismatch);
    };
    match checkpoint {
        WorkflowRemediationCheckpoint::GenerationInFlight { input } => {
            require_retry_authority(&step, policy)?;
            validate_usage_floor(state.usage, run.budget().usage())?;
            state.usage = run.budget().usage();
            state.phase = WorkflowCheckpointPhase::Remediating {
                step,
                attempt,
                original_input,
                checkpoint: WorkflowRemediationCheckpoint::GenerationReady { input },
            };
        }
        WorkflowRemediationCheckpoint::ReviewInFlight { candidate } => {
            require_retry_authority(&step, policy)?;
            validate_usage_floor(state.usage, run.budget().usage())?;
            state.usage = run.budget().usage();
            state.phase = WorkflowCheckpointPhase::Remediating {
                step,
                attempt,
                original_input,
                checkpoint: WorkflowRemediationCheckpoint::ReviewReady { candidate },
            };
        }
        WorkflowRemediationCheckpoint::Rejected { reason, .. } => {
            validate_exact_usage(state.usage, run.budget().usage())?;
            return Err(WorkflowError::RemediationRejected {
                step,
                attempts: attempt,
                reason,
            });
        }
        WorkflowRemediationCheckpoint::Exhausted { .. } => {
            validate_exact_usage(state.usage, run.budget().usage())?;
            return Err(WorkflowError::RemediationExhausted {
                step,
                attempts: attempt,
            });
        }
        WorkflowRemediationCheckpoint::GenerationReady { .. }
        | WorkflowRemediationCheckpoint::ReviewReady { .. }
        | WorkflowRemediationCheckpoint::Approved { .. } => {
            validate_exact_usage(state.usage, run.budget().usage())?;
        }
    }
    Ok(())
}

fn require_retry_authority(
    step: &StepId,
    policy: WorkflowResumePolicy,
) -> Result<(), WorkflowError> {
    if policy == WorkflowResumePolicy::RejectAmbiguous {
        Err(WorkflowError::AmbiguousCheckpoint { step: step.clone() })
    } else {
        Ok(())
    }
}

pub(crate) async fn execute_repairable_node(
    workflow: &str,
    node: &WorkflowNode,
    repairable: &RepairableNode,
    state: &mut WorkflowCheckpointState,
    run: &RunContext,
    caused_by: Option<EventId>,
    checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
) -> Result<Value, WorkflowError> {
    let (mut attempt, original_input, mut remediation) = remediation_state(node, state)?;
    loop {
        check_lifecycle(run)?;
        let context = RemediationAttemptContext {
            workflow,
            node,
            repairable,
            run,
            caused_by,
            attempt,
            original_input: &original_input,
        };
        match remediation {
            WorkflowRemediationCheckpoint::GenerationReady { input } => {
                remediation = generate_candidate(&context, state, checkpoint, input).await?;
            }
            WorkflowRemediationCheckpoint::ReviewReady { candidate } => {
                match review_candidate(&context, state, checkpoint, candidate).await? {
                    RemediationAction::Continue {
                        next_attempt,
                        checkpoint,
                    } => {
                        attempt = next_attempt;
                        remediation = checkpoint;
                    }
                    RemediationAction::Complete(output) => return Ok(output),
                }
            }
            WorkflowRemediationCheckpoint::Approved { output } => return Ok(output),
            WorkflowRemediationCheckpoint::Rejected { reason, .. } => {
                return Err(WorkflowError::RemediationRejected {
                    step: node.id.clone(),
                    attempts: attempt,
                    reason,
                });
            }
            WorkflowRemediationCheckpoint::Exhausted { .. } => {
                return Err(WorkflowError::RemediationExhausted {
                    step: node.id.clone(),
                    attempts: attempt,
                });
            }
            WorkflowRemediationCheckpoint::GenerationInFlight { .. }
            | WorkflowRemediationCheckpoint::ReviewInFlight { .. } => {
                return Err(WorkflowError::CheckpointIdentityMismatch);
            }
        }
    }
}

enum RemediationAction {
    Continue {
        next_attempt: u32,
        checkpoint: WorkflowRemediationCheckpoint,
    },
    Complete(Value),
}

struct RemediationAttemptContext<'a> {
    workflow: &'a str,
    node: &'a WorkflowNode,
    repairable: &'a RepairableNode,
    run: &'a RunContext,
    caused_by: Option<EventId>,
    attempt: u32,
    original_input: &'a Value,
}

async fn generate_candidate(
    context: &RemediationAttemptContext<'_>,
    state: &mut WorkflowCheckpointState,
    checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
    input: Value,
) -> Result<WorkflowRemediationCheckpoint, WorkflowError> {
    state.phase = remediation_phase(
        context.node,
        context.attempt,
        context.original_input.clone(),
        WorkflowRemediationCheckpoint::GenerationInFlight {
            input: input.clone(),
        },
    );
    state.usage = context.run.budget().usage();
    save_checkpoint(checkpoint, state).await?;
    let candidate = execute_generation(
        context.workflow,
        context.node,
        context.repairable,
        input,
        context.attempt,
        context.run,
        context.caused_by,
    )
    .await?;
    let remediation = WorkflowRemediationCheckpoint::ReviewReady {
        candidate: candidate.clone(),
    };
    state.phase = remediation_phase(
        context.node,
        context.attempt,
        context.original_input.clone(),
        remediation.clone(),
    );
    state.usage = context.run.budget().usage();
    save_checkpoint(checkpoint, state).await?;
    Ok(remediation)
}

async fn review_candidate(
    context: &RemediationAttemptContext<'_>,
    state: &mut WorkflowCheckpointState,
    checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
    candidate: Value,
) -> Result<RemediationAction, WorkflowError> {
    state.phase = remediation_phase(
        context.node,
        context.attempt,
        context.original_input.clone(),
        WorkflowRemediationCheckpoint::ReviewInFlight {
            candidate: candidate.clone(),
        },
    );
    state.usage = context.run.budget().usage();
    save_checkpoint(checkpoint, state).await?;
    let verdict = execute_review(
        context.workflow,
        context.node,
        context.repairable,
        WorkflowReviewRequest {
            step: context.node.id.clone(),
            attempt: context.attempt,
            original_input: context.original_input.clone(),
            candidate: candidate.clone(),
        },
        context.run,
        context.caused_by,
    )
    .await?;
    apply_review_verdict(context, state, checkpoint, candidate, verdict).await
}

async fn apply_review_verdict(
    context: &RemediationAttemptContext<'_>,
    state: &mut WorkflowCheckpointState,
    checkpoint: &mut Option<&mut WorkflowCheckpointCursor>,
    candidate: Value,
    verdict: WorkflowReviewVerdict,
) -> Result<RemediationAction, WorkflowError> {
    match verdict {
        WorkflowReviewVerdict::Approve => {
            state.phase = remediation_phase(
                context.node,
                context.attempt,
                context.original_input.clone(),
                WorkflowRemediationCheckpoint::Approved {
                    output: candidate.clone(),
                },
            );
            state.usage = context.run.budget().usage();
            save_checkpoint(checkpoint, state).await?;
            Ok(RemediationAction::Complete(candidate))
        }
        WorkflowReviewVerdict::Reject { reason } => {
            state.phase = remediation_phase(
                context.node,
                context.attempt,
                context.original_input.clone(),
                WorkflowRemediationCheckpoint::Rejected {
                    candidate,
                    reason: reason.clone(),
                },
            );
            state.usage = context.run.budget().usage();
            save_checkpoint(checkpoint, state).await?;
            Err(WorkflowError::RemediationRejected {
                step: context.node.id.clone(),
                attempts: context.attempt,
                reason,
            })
        }
        WorkflowReviewVerdict::Repair { feedback } => {
            if context.attempt >= context.repairable.policy.max_repairs().saturating_add(1) {
                state.phase = remediation_phase(
                    context.node,
                    context.attempt,
                    context.original_input.clone(),
                    WorkflowRemediationCheckpoint::Exhausted {
                        candidate,
                        feedback,
                    },
                );
                state.usage = context.run.budget().usage();
                save_checkpoint(checkpoint, state).await?;
                return Err(WorkflowError::RemediationExhausted {
                    step: context.node.id.clone(),
                    attempts: context.attempt,
                });
            }
            let next_attempt = context
                .attempt
                .checked_add(1)
                .ok_or(WorkflowError::CheckpointIdentityMismatch)?;
            let repair_input = WorkflowRepairInput::new(
                context.original_input.clone(),
                candidate,
                feedback,
                next_attempt,
            );
            let remediation = WorkflowRemediationCheckpoint::GenerationReady {
                input: serde_json::to_value(repair_input)?,
            };
            state.phase = remediation_phase(
                context.node,
                next_attempt,
                context.original_input.clone(),
                remediation.clone(),
            );
            state.usage = context.run.budget().usage();
            save_checkpoint(checkpoint, state).await?;
            Ok(RemediationAction::Continue {
                next_attempt,
                checkpoint: remediation,
            })
        }
    }
}

fn remediation_state(
    node: &WorkflowNode,
    state: &WorkflowCheckpointState,
) -> Result<(u32, Value, WorkflowRemediationCheckpoint), WorkflowError> {
    match &state.phase {
        WorkflowCheckpointPhase::Ready => Ok((
            1,
            state.value.clone(),
            WorkflowRemediationCheckpoint::GenerationReady {
                input: state.value.clone(),
            },
        )),
        WorkflowCheckpointPhase::Remediating {
            step,
            attempt,
            original_input,
            checkpoint,
        } if *step == node.id => Ok((*attempt, original_input.clone(), checkpoint.clone())),
        _ => Err(WorkflowError::CheckpointIdentityMismatch),
    }
}

fn remediation_phase(
    node: &WorkflowNode,
    attempt: u32,
    original_input: Value,
    checkpoint: WorkflowRemediationCheckpoint,
) -> WorkflowCheckpointPhase {
    WorkflowCheckpointPhase::Remediating {
        step: node.id.clone(),
        attempt,
        original_input,
        checkpoint,
    }
}

async fn execute_generation(
    workflow: &str,
    node: &WorkflowNode,
    repairable: &RepairableNode,
    input: Value,
    attempt: u32,
    run: &RunContext,
    caused_by: Option<EventId>,
) -> Result<Value, WorkflowError> {
    let started = record_domain(
        run,
        "remediation.generation.started",
        json!({
            "workflow": workflow,
            "step": node.id,
            "attempt": attempt,
        }),
        caused_by,
    )?;
    let mut child = run.child(node.capabilities.clone()).map_err(|error| {
        WorkflowError::AuthorityEscalation {
            step: node.id.clone(),
            capability: error.capability,
        }
    })?;
    if let Some(event_id) = started {
        child = child.with_cause(event_id);
    }
    run.record(
        RunEventKind::Child(ChildEvent::Started {
            child_run_id: child.run_id(),
        }),
        started,
    )?;
    match repairable.generator.execute(input, &child).await {
        Ok(output) => {
            run.record(
                RunEventKind::Child(ChildEvent::Completed {
                    child_run_id: child.run_id(),
                }),
                started,
            )?;
            record_domain(
                run,
                "remediation.generation.completed",
                json!({
                    "workflow": workflow,
                    "step": node.id,
                    "attempt": attempt,
                }),
                started,
            )?;
            Ok(output)
        }
        Err(source) => {
            run.record(
                RunEventKind::Child(ChildEvent::Failed {
                    child_run_id: child.run_id(),
                }),
                started,
            )?;
            Err(WorkflowError::Step {
                step: node.id.clone(),
                source: Box::new(source),
            })
        }
    }
}

async fn execute_review(
    workflow: &str,
    node: &WorkflowNode,
    repairable: &RepairableNode,
    request: WorkflowReviewRequest,
    run: &RunContext,
    caused_by: Option<EventId>,
) -> Result<WorkflowReviewVerdict, WorkflowError> {
    let attempt = request.attempt;
    let started = record_domain(
        run,
        "remediation.review.started",
        json!({
            "workflow": workflow,
            "step": node.id,
            "attempt": attempt,
        }),
        caused_by,
    )?;
    let mut child = run
        .child(repairable.reviewer_capabilities.clone())
        .map_err(|error| WorkflowError::AuthorityEscalation {
            step: node.id.clone(),
            capability: error.capability,
        })?;
    if let Some(event_id) = started {
        child = child.with_cause(event_id);
    }
    run.record(
        RunEventKind::Child(ChildEvent::Started {
            child_run_id: child.run_id(),
        }),
        started,
    )?;
    let verdict = repairable
        .reviewer
        .review(request, &child)
        .await
        .and_then(|verdict| {
            verdict.validate()?;
            Ok(verdict)
        });
    match verdict {
        Ok(verdict) => {
            run.record(
                RunEventKind::Child(ChildEvent::Completed {
                    child_run_id: child.run_id(),
                }),
                started,
            )?;
            record_domain(
                run,
                "remediation.review.completed",
                json!({
                    "workflow": workflow,
                    "step": node.id,
                    "attempt": attempt,
                    "verdict": match &verdict {
                        WorkflowReviewVerdict::Approve => "approve",
                        WorkflowReviewVerdict::Repair { .. } => "repair",
                        WorkflowReviewVerdict::Reject { .. } => "reject",
                    },
                }),
                started,
            )?;
            Ok(verdict)
        }
        Err(source) => {
            run.record(
                RunEventKind::Child(ChildEvent::Failed {
                    child_run_id: child.run_id(),
                }),
                started,
            )?;
            Err(WorkflowError::Review {
                step: node.id.clone(),
                source,
            })
        }
    }
}

fn model_prompt(input: &Value) -> String {
    match input {
        Value::String(prompt) => prompt.clone(),
        Value::Object(object) => object
            .get("input")
            .and_then(Value::as_str)
            .map_or_else(|| input.to_string(), ToOwned::to_owned),
        _ => input.to_string(),
    }
}

fn model_visible_candidate(candidate: &Value) -> Value {
    candidate
        .as_object()
        .and_then(|object| object.get("input"))
        .and_then(Value::as_str)
        .map_or_else(|| candidate.clone(), |text| Value::String(text.to_owned()))
}

fn validate_feedback(feedback: &Value) -> Result<(), WorkflowReviewError> {
    if serde_json::to_vec(feedback).is_ok_and(|encoded| encoded.len() <= MAX_REVIEW_FEEDBACK_BYTES)
    {
        Ok(())
    } else {
        Err(WorkflowReviewError::FeedbackTooLarge)
    }
}

fn validate_reason(reason: &str) -> Result<(), WorkflowReviewError> {
    if reason.trim().is_empty() || reason.len() > MAX_REVIEW_REASON_BYTES {
        Err(WorkflowReviewError::InvalidRejectionReason)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use futures_executor::block_on;
    use runifold_core::{
        Budget, BudgetTracker, CapabilitySet, Checkpoint, CheckpointError, CheckpointErrorKind,
        CheckpointId, CheckpointStore, InMemoryCheckpointStore, RunContext,
    };
    use serde_json::{Value, json};

    use super::{
        WorkflowRemediationCheckpoint, WorkflowRemediationPolicy, WorkflowRepairInput,
        WorkflowReviewError, WorkflowReviewFuture, WorkflowReviewRequest, WorkflowReviewVerdict,
        WorkflowReviewer,
    };
    use crate::{
        Workflow, WorkflowCheckpoint, WorkflowCheckpointPhase, WorkflowCheckpointRevision,
        WorkflowError, WorkflowForkCommand, WorkflowForkPolicy, WorkflowResumePolicy, WorkflowStep,
        WorkflowStepFuture,
    };

    struct CountingGenerator {
        calls: Arc<AtomicUsize>,
        inputs: Arc<Mutex<Vec<Value>>>,
    }

    impl WorkflowStep for CountingGenerator {
        fn execute<'a>(&'a self, input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
            let attempt = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.inputs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(input);
            Box::pin(async move { Ok(Value::String(format!("candidate-{attempt}"))) })
        }
    }

    struct RepairOnceReviewer {
        calls: Arc<AtomicUsize>,
        requests: Arc<Mutex<Vec<WorkflowReviewRequest>>>,
    }

    impl WorkflowReviewer for RepairOnceReviewer {
        fn review<'a>(
            &'a self,
            request: WorkflowReviewRequest,
            _run: &'a RunContext,
        ) -> WorkflowReviewFuture<'a> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let attempt = request.attempt;
            self.requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(request);
            Box::pin(async move {
                if attempt == 1 {
                    WorkflowReviewVerdict::repair(json!({
                        "code": "unsafe_claim",
                        "instruction": "remove the unsupported guarantee",
                    }))
                } else {
                    Ok(WorkflowReviewVerdict::approve())
                }
            })
        }
    }

    struct ApproveReviewer {
        calls: Arc<AtomicUsize>,
    }

    impl WorkflowReviewer for ApproveReviewer {
        fn review<'a>(
            &'a self,
            _request: WorkflowReviewRequest,
            _run: &'a RunContext,
        ) -> WorkflowReviewFuture<'a> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(WorkflowReviewVerdict::approve()) })
        }
    }

    struct AlwaysRepairReviewer {
        calls: Arc<AtomicUsize>,
    }

    impl WorkflowReviewer for AlwaysRepairReviewer {
        fn review<'a>(
            &'a self,
            request: WorkflowReviewRequest,
            _run: &'a RunContext,
        ) -> WorkflowReviewFuture<'a> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(
                async move { WorkflowReviewVerdict::repair(json!({"attempt": request.attempt})) },
            )
        }
    }

    struct RejectReviewer;

    impl WorkflowReviewer for RejectReviewer {
        fn review<'a>(
            &'a self,
            _request: WorkflowReviewRequest,
            _run: &'a RunContext,
        ) -> WorkflowReviewFuture<'a> {
            Box::pin(async { WorkflowReviewVerdict::reject("policy denied the candidate") })
        }
    }

    struct FailRevisionOnceStore {
        inner: InMemoryCheckpointStore,
        revision: u64,
        failed: AtomicBool,
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
                    "injected remediation checkpoint interruption",
                ));
            }
            self.inner.compare_and_swap(checkpoint, expected_revision)
        }
    }

    #[test]
    fn repairable_step_approves_the_first_candidate() {
        let generator_calls = Arc::new(AtomicUsize::new(0));
        let reviewer_calls = Arc::new(AtomicUsize::new(0));
        let workflow = workflow(
            generator_calls.clone(),
            Arc::new(Mutex::new(Vec::new())),
            ApproveReviewer {
                calls: reviewer_calls.clone(),
            },
            WorkflowRemediationPolicy::new(2),
        );
        let run = root_run();

        let outcome = block_on(workflow.run("write a claim", &run)).unwrap();

        assert_eq!(outcome.output, json!("candidate-1"));
        assert_eq!(generator_calls.load(Ordering::SeqCst), 1);
        assert_eq!(reviewer_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn repair_feedback_is_injected_and_the_new_candidate_is_reviewed() {
        let generator_calls = Arc::new(AtomicUsize::new(0));
        let reviewer_calls = Arc::new(AtomicUsize::new(0));
        let inputs = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let workflow = workflow(
            generator_calls.clone(),
            inputs.clone(),
            RepairOnceReviewer {
                calls: reviewer_calls.clone(),
                requests: requests.clone(),
            },
            WorkflowRemediationPolicy::new(2),
        );
        let run = root_run();

        let outcome = block_on(workflow.run("write a claim", &run)).unwrap();

        assert_eq!(outcome.output, json!("candidate-2"));
        assert_eq!(generator_calls.load(Ordering::SeqCst), 2);
        assert_eq!(reviewer_calls.load(Ordering::SeqCst), 2);
        let inputs = inputs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(inputs[0], json!("write a claim"));
        let repair: WorkflowRepairInput = serde_json::from_value(inputs[1].clone()).unwrap();
        assert_eq!(repair.attempt, 2);
        assert_eq!(repair.original_input, json!("write a claim"));
        assert_eq!(repair.previous_candidate, json!("candidate-1"));
        assert_eq!(repair.feedback["code"], "unsafe_claim");
        assert!(repair.input.contains("runifold_workflow_repair"));
        let requests = requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(requests[0].attempt, 1);
        assert_eq!(requests[1].attempt, 2);
    }

    #[test]
    fn exhausted_remediation_is_durable_and_does_not_run_again_on_resume() {
        let generator_calls = Arc::new(AtomicUsize::new(0));
        let reviewer_calls = Arc::new(AtomicUsize::new(0));
        let workflow = workflow(
            generator_calls.clone(),
            Arc::new(Mutex::new(Vec::new())),
            AlwaysRepairReviewer {
                calls: reviewer_calls.clone(),
            },
            WorkflowRemediationPolicy::new(1),
        );
        let store = Arc::new(InMemoryCheckpointStore::new());
        let checkpoint = WorkflowCheckpoint::new(store);
        let run = root_run();

        let error =
            block_on(workflow.run_checkpointed("write a claim", &run, &checkpoint)).unwrap_err();

        assert!(matches!(
            error,
            WorkflowError::RemediationExhausted { attempts: 2, .. }
        ));
        let (_, state) = checkpoint.load().unwrap();
        assert!(matches!(
            state.phase,
            WorkflowCheckpointPhase::Remediating {
                attempt: 2,
                checkpoint: WorkflowRemediationCheckpoint::Exhausted { .. },
                ..
            }
        ));
        let resumed =
            block_on(workflow.resume(&checkpoint, &run, WorkflowResumePolicy::RejectAmbiguous))
                .unwrap_err();
        assert!(matches!(
            resumed,
            WorkflowError::RemediationExhausted { attempts: 2, .. }
        ));
        assert_eq!(generator_calls.load(Ordering::SeqCst), 2);
        assert_eq!(reviewer_calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn durable_review_ready_resume_does_not_repeat_generation() {
        let generator_calls = Arc::new(AtomicUsize::new(0));
        let reviewer_calls = Arc::new(AtomicUsize::new(0));
        let workflow = workflow(
            generator_calls.clone(),
            Arc::new(Mutex::new(Vec::new())),
            ApproveReviewer {
                calls: reviewer_calls.clone(),
            },
            WorkflowRemediationPolicy::new(1),
        );
        let store = Arc::new(FailRevisionOnceStore::new(3));
        let checkpoint = WorkflowCheckpoint::new(store);
        let run = root_run();

        let first =
            block_on(workflow.run_checkpointed("write a claim", &run, &checkpoint)).unwrap_err();
        assert!(matches!(first, WorkflowError::Checkpoint(_)));
        assert_eq!(generator_calls.load(Ordering::SeqCst), 1);
        assert_eq!(reviewer_calls.load(Ordering::SeqCst), 0);

        let outcome =
            block_on(workflow.resume(&checkpoint, &run, WorkflowResumePolicy::RejectAmbiguous))
                .unwrap();

        assert_eq!(outcome.output, json!("candidate-1"));
        assert_eq!(generator_calls.load(Ordering::SeqCst), 1);
        assert_eq!(reviewer_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn in_flight_generation_requires_explicit_retry_authority() {
        let generator_calls = Arc::new(AtomicUsize::new(0));
        let reviewer_calls = Arc::new(AtomicUsize::new(0));
        let workflow = workflow(
            generator_calls.clone(),
            Arc::new(Mutex::new(Vec::new())),
            ApproveReviewer {
                calls: reviewer_calls.clone(),
            },
            WorkflowRemediationPolicy::new(1),
        );
        let store = Arc::new(FailRevisionOnceStore::new(2));
        let checkpoint = WorkflowCheckpoint::new(store);
        let run = root_run();

        let first =
            block_on(workflow.run_checkpointed("write a claim", &run, &checkpoint)).unwrap_err();
        assert!(matches!(first, WorkflowError::Checkpoint(_)));
        let rejected =
            block_on(workflow.resume(&checkpoint, &run, WorkflowResumePolicy::RejectAmbiguous))
                .unwrap_err();
        assert!(matches!(
            rejected,
            WorkflowError::AmbiguousCheckpoint { .. }
        ));
        let (envelope, _) = checkpoint.load().unwrap();
        let rejected_fork = WorkflowForkCommand::new(
            checkpoint.id(),
            envelope.revision,
            WorkflowForkPolicy::RejectAmbiguous,
        )
        .prepare_checkpoint(envelope.clone())
        .unwrap_err();
        assert_eq!(rejected_fork.kind, CheckpointErrorKind::Conflict);
        let retryable_fork = WorkflowForkCommand::new(
            checkpoint.id(),
            envelope.revision,
            WorkflowForkPolicy::RetryInterruptedStep,
        )
        .prepare_checkpoint(envelope)
        .unwrap();
        let revision = WorkflowCheckpointRevision::from_checkpoint(retryable_fork).unwrap();
        assert!(matches!(
            revision.state.phase,
            WorkflowCheckpointPhase::Remediating {
                checkpoint: WorkflowRemediationCheckpoint::GenerationReady { .. },
                ..
            }
        ));

        let outcome = block_on(workflow.resume(
            &checkpoint,
            &run,
            WorkflowResumePolicy::RetryInterruptedStep,
        ))
        .unwrap();

        assert_eq!(outcome.output, json!("candidate-2"));
        assert_eq!(generator_calls.load(Ordering::SeqCst), 2);
        assert_eq!(reviewer_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn in_flight_review_requires_explicit_retry_without_repeating_generation() {
        let generator_calls = Arc::new(AtomicUsize::new(0));
        let reviewer_calls = Arc::new(AtomicUsize::new(0));
        let workflow = workflow(
            generator_calls.clone(),
            Arc::new(Mutex::new(Vec::new())),
            ApproveReviewer {
                calls: reviewer_calls.clone(),
            },
            WorkflowRemediationPolicy::new(1),
        );
        let store = Arc::new(FailRevisionOnceStore::new(4));
        let checkpoint = WorkflowCheckpoint::new(store);
        let run = root_run();

        let first =
            block_on(workflow.run_checkpointed("write a claim", &run, &checkpoint)).unwrap_err();
        assert!(matches!(first, WorkflowError::Checkpoint(_)));
        assert_eq!(generator_calls.load(Ordering::SeqCst), 1);
        assert_eq!(reviewer_calls.load(Ordering::SeqCst), 1);
        let rejected =
            block_on(workflow.resume(&checkpoint, &run, WorkflowResumePolicy::RejectAmbiguous))
                .unwrap_err();
        assert!(matches!(
            rejected,
            WorkflowError::AmbiguousCheckpoint { .. }
        ));

        let outcome = block_on(workflow.resume(
            &checkpoint,
            &run,
            WorkflowResumePolicy::RetryInterruptedStep,
        ))
        .unwrap();

        assert_eq!(outcome.output, json!("candidate-1"));
        assert_eq!(generator_calls.load(Ordering::SeqCst), 1);
        assert_eq!(reviewer_calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn reviewer_rejection_is_a_stable_typed_failure() {
        let workflow = workflow(
            Arc::new(AtomicUsize::new(0)),
            Arc::new(Mutex::new(Vec::new())),
            RejectReviewer,
            WorkflowRemediationPolicy::new(3),
        );

        let error = block_on(workflow.run("write a claim", &root_run())).unwrap_err();

        assert!(matches!(
            error,
            WorkflowError::RemediationRejected {
                attempts: 1,
                reason,
                ..
            } if reason == "policy denied the candidate"
        ));
    }

    #[test]
    fn review_payloads_are_bounded_before_checkpointing() {
        assert!(WorkflowReviewVerdict::reject(" ").is_err());
        assert!(
            WorkflowReviewVerdict::repair(Value::String(
                "x".repeat(super::MAX_REVIEW_FEEDBACK_BYTES)
            ))
            .is_err()
        );
        assert_eq!(
            WorkflowReviewVerdict::reject("x".repeat(super::MAX_REVIEW_REASON_BYTES + 1))
                .unwrap_err(),
            WorkflowReviewError::InvalidRejectionReason
        );
    }

    #[test]
    fn model_repair_prompt_projects_agent_candidate_text_without_full_outcome() {
        let repair = WorkflowRepairInput::new(
            json!("original prompt"),
            json!({
                "input": "visible candidate",
                "outcome": {"provider_metadata": "not-model-visible"},
            }),
            json!({"instruction": "fix it"}),
            2,
        );

        assert!(repair.input.contains("visible candidate"));
        assert!(!repair.input.contains("not-model-visible"));
        assert_eq!(
            repair.previous_candidate["outcome"]["provider_metadata"],
            "not-model-visible"
        );
    }

    fn workflow<R>(
        generator_calls: Arc<AtomicUsize>,
        inputs: Arc<Mutex<Vec<Value>>>,
        reviewer: R,
        policy: WorkflowRemediationPolicy,
    ) -> Workflow
    where
        R: WorkflowReviewer + 'static,
    {
        Workflow::builder("reviewed-generation")
            .repairable_step(
                "draft",
                CountingGenerator {
                    calls: generator_calls,
                    inputs,
                },
                reviewer,
                policy,
                CapabilitySet::new(),
                CapabilitySet::new(),
            )
            .build()
            .unwrap()
    }

    fn root_run() -> RunContext {
        RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new())
    }
}
