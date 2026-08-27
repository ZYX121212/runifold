//! Terminal completion validation and bounded repair diagnostics.

use std::{fmt, marker::PhantomData, sync::Arc};

use runifold_model::{ContentPart, Message, ModelResponse, Role, StructuredOutputErrorKind};
use serde::de::DeserializeOwned;
use serde_json::json;

use super::checkpointing::{AgentProgress, save_checkpoint};
use super::observability::record_domain;
use super::{
    Agent, AgentCheckpointPhase, AgentObserver, AgentOutcome, AgentStreamEvent, EventId,
    RunContext, emit_agent_event,
};
use crate::checkpoint::CheckpointCursor;
use crate::{AgentError, TerminalRequirementFailure, TerminalRequirementFailureKind};

pub(super) trait CompletionValidatorImpl: Send + Sync {
    fn validate(&self, response: &ModelResponse) -> Result<(), TerminalRequirementFailure>;
}

pub(crate) fn failure_error(failure: &TerminalRequirementFailure, attempts: u32) -> AgentError {
    match failure.kind {
        TerminalRequirementFailureKind::EmptyResponse => {
            AgentError::EmptyTerminalResponse { attempts }
        }
        TerminalRequirementFailureKind::MissingStructuredText => {
            AgentError::StructuredOutputUnsatisfied {
                attempts,
                kind: StructuredOutputErrorKind::MissingText,
                line: failure.line,
                column: failure.column,
            }
        }
        TerminalRequirementFailureKind::InvalidStructuredOutput => {
            AgentError::StructuredOutputUnsatisfied {
                attempts,
                kind: StructuredOutputErrorKind::InvalidOutput,
                line: failure.line,
                column: failure.column,
            }
        }
        TerminalRequirementFailureKind::Refusal => AgentError::StructuredOutputUnsatisfied {
            attempts,
            kind: StructuredOutputErrorKind::Refusal,
            line: failure.line,
            column: failure.column,
        },
    }
}

pub(super) fn repair_instruction(failure: &TerminalRequirementFailure) -> String {
    let diagnostic = match failure.kind {
        TerminalRequirementFailureKind::EmptyResponse
        | TerminalRequirementFailureKind::MissingStructuredText => json!({
            "kind": failure.kind,
            "instruction": "Produce the required terminal response now. Do not repeat completed tool calls and do not fabricate tool results."
        }),
        TerminalRequirementFailureKind::InvalidStructuredOutput => json!({
            "kind": failure.kind,
            "line": failure.line,
            "column": failure.column,
            "instruction": "Return only a corrected terminal response that satisfies the original JSON Schema. Do not repeat completed tool calls, change the schema, or invent missing business values."
        }),
        TerminalRequirementFailureKind::Refusal => json!({
            "kind": failure.kind,
            "instruction": "The refusal is terminal and must not be repaired."
        }),
    };
    format!("<runifold_terminal_repair trust=\"runtime\">{diagnostic}</runifold_terminal_repair>")
}

pub(super) fn rejected_candidate_message(response: &ModelResponse) -> Option<Message> {
    if response.content.is_empty() {
        return None;
    }
    let mut message = Message::new(Role::Assistant, response.content.clone()).ok()?;
    message.metadata.insert(
        "runifold.context.transient".into(),
        serde_json::Value::Bool(true),
    );
    message.metadata.insert(
        "runifold.terminal_candidate.rejected".into(),
        serde_json::Value::Bool(true),
    );
    Some(message)
}

pub(super) fn repair_message(failure: &TerminalRequirementFailure) -> Message {
    let mut message = Message::user(repair_instruction(failure));
    message.metadata.insert(
        "runifold.context.transient".into(),
        serde_json::Value::Bool(true),
    );
    message.metadata.insert(
        "runifold.terminal_repair".into(),
        serde_json::Value::Bool(true),
    );
    message
}

pub(super) struct TerminalCompletionContext<'a> {
    pub(super) caused_by: Option<EventId>,
    pub(super) observer: &'a dyn AgentObserver,
    pub(super) persist_terminal_checkpoint: bool,
}

impl Agent {
    pub(super) async fn complete_terminal_candidate(
        &self,
        response: ModelResponse,
        run: &RunContext,
        progress: &mut AgentProgress,
        checkpoint: &mut Option<&mut CheckpointCursor>,
        context: TerminalCompletionContext<'_>,
    ) -> Result<Option<AgentOutcome>, AgentError> {
        let failure = match self.completion_validator.validate(&response) {
            Ok(()) => {
                if self.terminal_review.is_some() {
                    let attempt =
                        progress
                            .terminal_review_repairs
                            .checked_add(1)
                            .ok_or_else(|| {
                                AgentError::Protocol("terminal review counter overflow".into())
                            })?;
                    save_checkpoint(
                        checkpoint,
                        &self.checkpoint_state(
                            progress,
                            run,
                            AgentCheckpointPhase::TerminalReviewReady {
                                response: Box::new(response.clone()),
                                attempt,
                            },
                        ),
                    )?;
                    return self
                        .review_terminal_candidate(
                            response, attempt, run, progress, checkpoint, &context,
                        )
                        .await;
                }
                return self
                    .accept_terminal_candidate(response, run, progress, checkpoint, &context);
            }
            Err(failure) => failure,
        };

        let policy = self.completion_requirement;
        let can_repair =
            failure.repairable(policy) && progress.terminal_repairs < policy.max_terminal_repairs();
        record_domain(
            run,
            "terminal_candidate.rejected",
            json!({
                "agent": self.name,
                "turn": progress.turns,
                "kind": failure.kind,
                "repair_attempts": progress.terminal_repairs,
                "response_id": response.id,
                "finish_reason": response.finish_reason,
                "content_types": content_type_names(&response.content),
            }),
            context.caused_by,
        )?;
        if !can_repair {
            save_checkpoint(
                checkpoint,
                &self.checkpoint_state(
                    progress,
                    run,
                    AgentCheckpointPhase::TerminalRequirementFailed {
                        failure: failure.clone(),
                        attempts: progress.terminal_repairs,
                    },
                ),
            )?;
            return Err(failure_error(&failure, progress.terminal_repairs));
        }

        progress.terminal_repairs = progress
            .terminal_repairs
            .checked_add(1)
            .ok_or_else(|| AgentError::Protocol("terminal repair counter overflow".into()))?;
        if let Some(candidate) = rejected_candidate_message(&response) {
            progress.transcript.push(candidate);
        }
        progress.transcript.push(repair_message(&failure));
        emit_agent_event(
            context.observer,
            AgentStreamEvent::TerminalRepairScheduled {
                attempt: progress.terminal_repairs,
                failure: failure.clone(),
            },
        )
        .await;
        record_domain(
            run,
            "terminal_repair.scheduled",
            json!({
                "agent": self.name,
                "attempt": progress.terminal_repairs,
                "kind": failure.kind,
            }),
            context.caused_by,
        )?;
        save_checkpoint(
            checkpoint,
            &self.checkpoint_state(progress, run, AgentCheckpointPhase::ReadyForTurn),
        )?;
        Ok(None)
    }

    pub(super) fn accept_terminal_candidate(
        &self,
        response: ModelResponse,
        run: &RunContext,
        progress: &mut AgentProgress,
        checkpoint: &mut Option<&mut CheckpointCursor>,
        context: &TerminalCompletionContext<'_>,
    ) -> Result<Option<AgentOutcome>, AgentError> {
        let assistant = Message::new(Role::Assistant, response.content.clone())
            .map_err(|error| AgentError::Protocol(error.to_string()))?;
        progress.transcript.push(assistant);
        if progress.terminal_repairs > 0 {
            record_domain(
                run,
                "completion_requirement.satisfied",
                json!({
                    "agent": self.name,
                    "turn": progress.turns,
                    "repair_attempts": progress.terminal_repairs,
                    "response_id": response.id,
                }),
                context.caused_by,
            )?;
        }
        if context.persist_terminal_checkpoint {
            save_checkpoint(
                checkpoint,
                &self.checkpoint_state(
                    progress,
                    run,
                    AgentCheckpointPhase::Completed {
                        response: Box::new(response.clone()),
                    },
                ),
            )?;
        }
        Ok(Some(progress.clone_outcome(response, run.budget().usage())))
    }
}

fn content_type_names(content: &[ContentPart]) -> Vec<&'static str> {
    content
        .iter()
        .map(|part| match part {
            ContentPart::Text { .. } => "text",
            ContentPart::Image { .. } => "image",
            ContentPart::Audio { .. } => "audio",
            ContentPart::Document { .. } => "document",
            ContentPart::ResourceLink { .. } => "resource_link",
            ContentPart::ToolCall(_) => "tool_call",
            ContentPart::ToolResult(_) => "tool_result",
            ContentPart::Reasoning(_) => "reasoning",
            ContentPart::Refusal { .. } => "refusal",
            ContentPart::Citation(_) => "citation",
            ContentPart::ProviderOpaque(_) => "provider_opaque",
            _ => "unknown",
        })
        .collect()
}

#[derive(Clone)]
pub(crate) struct CompletionValidator(Arc<dyn CompletionValidatorImpl>);

impl CompletionValidator {
    pub(super) fn content() -> Self {
        Self(Arc::new(ContentValidator))
    }

    pub(super) fn structured<T>() -> Self
    where
        T: DeserializeOwned + Send + 'static,
    {
        Self(Arc::new(StructuredValidator::<T>(PhantomData)))
    }

    pub(super) fn validate(
        &self,
        response: &ModelResponse,
    ) -> Result<(), TerminalRequirementFailure> {
        self.0.validate(response)
    }
}

impl fmt::Debug for CompletionValidator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CompletionValidator(..)")
    }
}

struct ContentValidator;

impl CompletionValidatorImpl for ContentValidator {
    fn validate(&self, response: &ModelResponse) -> Result<(), TerminalRequirementFailure> {
        let usable = response.content.iter().any(|part| match part {
            ContentPart::Text { text } => !text.trim().is_empty(),
            ContentPart::Image { .. }
            | ContentPart::Audio { .. }
            | ContentPart::Document { .. }
            | ContentPart::ResourceLink { .. }
            | ContentPart::Refusal { .. } => true,
            _ => false,
        });
        usable.then_some(()).ok_or_else(|| {
            TerminalRequirementFailure::new(TerminalRequirementFailureKind::EmptyResponse)
        })
    }
}

struct StructuredValidator<T>(PhantomData<fn() -> T>);

impl<T> CompletionValidatorImpl for StructuredValidator<T>
where
    T: DeserializeOwned + Send + 'static,
{
    fn validate(&self, response: &ModelResponse) -> Result<(), TerminalRequirementFailure> {
        response.structured::<T>().map(|_| ()).map_err(|error| {
            let kind = match error.kind {
                StructuredOutputErrorKind::MissingText => {
                    TerminalRequirementFailureKind::MissingStructuredText
                }
                StructuredOutputErrorKind::Refusal => TerminalRequirementFailureKind::Refusal,
                StructuredOutputErrorKind::InvalidOutput => {
                    TerminalRequirementFailureKind::InvalidStructuredOutput
                }
                _ => TerminalRequirementFailureKind::InvalidStructuredOutput,
            };
            TerminalRequirementFailure {
                kind,
                line: error.line,
                column: error.column,
            }
        })
    }
}
