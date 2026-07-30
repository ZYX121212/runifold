use std::{fmt, future::Future, pin::Pin, sync::Arc};

use runifold_agent::{Agent, AgentOutcome};
use runifold_core::RunContext;
use runifold_model::ContentPart;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::WorkflowStepError;

/// Stable workflow node identity used by checkpoints and event streams.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct StepId(String);

impl StepId {
    pub(crate) fn parse(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'));
        valid.then_some(Self(value.clone())).ok_or(value)
    }

    /// Returns the stable identifier text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StepId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Boxed asynchronous workflow-step result.
#[cfg(not(target_arch = "wasm32"))]
pub type WorkflowStepFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Value, WorkflowStepError>> + Send + 'a>>;

/// Boxed workflow-step result on single-threaded WASM.
#[cfg(target_arch = "wasm32")]
pub type WorkflowStepFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Value, WorkflowStepError>> + 'a>>;

/// One executable, provider-neutral workflow node.
///
/// External work must be represented by capabilities passed to the workflow
/// builder. The scheduler relies on that declaration to attenuate authority
/// and to reject write-capable branches from first-success races.
pub trait WorkflowStep: Send + Sync {
    /// Executes from canonical JSON input to canonical JSON output.
    fn execute<'a>(&'a self, input: Value, run: &'a RunContext) -> WorkflowStepFuture<'a>;
}

/// Pure, synchronous branch decision over canonical workflow data.
pub trait WorkflowCondition: Send + Sync {
    /// Selects the true or false branch.
    ///
    /// # Errors
    ///
    /// Returns [`WorkflowStepError`] when the canonical input cannot be
    /// evaluated safely.
    fn evaluate(&self, input: &Value) -> Result<bool, WorkflowStepError>;
}

/// Adapts a closure into a durable workflow branch condition.
pub struct PredicateCondition<F> {
    predicate: F,
}

impl<F> PredicateCondition<F> {
    /// Creates a condition from a pure predicate.
    pub const fn new(predicate: F) -> Self {
        Self { predicate }
    }
}

impl<F> WorkflowCondition for PredicateCondition<F>
where
    F: Fn(&Value) -> Result<bool, WorkflowStepError> + Send + Sync,
{
    fn evaluate(&self, input: &Value) -> Result<bool, WorkflowStepError> {
        (self.predicate)(input)
    }
}

impl<F> fmt::Debug for PredicateCondition<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PredicateCondition")
            .finish_non_exhaustive()
    }
}

/// Adapts a Runifold Agent to the canonical workflow step boundary.
#[derive(Clone, Debug)]
pub struct AgentStep {
    agent: Arc<Agent>,
}

impl AgentStep {
    /// Creates an Agent-backed step.
    pub const fn new(agent: Arc<Agent>) -> Self {
        Self { agent }
    }

    /// Returns the wrapped Agent.
    pub const fn agent(&self) -> &Arc<Agent> {
        &self.agent
    }
}

/// Canonical Agent workflow value.
///
/// `input` is the concatenated terminal text consumed automatically by a
/// following Agent step. `outcome` retains the full response and transcript.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AgentStepOutput {
    /// Text forwarded to a following Agent step.
    pub input: String,
    /// Complete canonical Agent result.
    pub outcome: AgentOutcome,
}

impl WorkflowStep for AgentStep {
    fn execute<'a>(&'a self, input: Value, run: &'a RunContext) -> WorkflowStepFuture<'a> {
        Box::pin(async move {
            let prompt = match input {
                Value::String(prompt) => prompt,
                Value::Object(mut object) => object
                    .remove("input")
                    .and_then(|value| value.as_str().map(ToOwned::to_owned))
                    .ok_or_else(|| {
                        WorkflowStepError::InvalidInput(
                            "Agent steps require a string or an object containing string `input`"
                                .into(),
                        )
                    })?,
                _ => {
                    return Err(WorkflowStepError::InvalidInput(
                        "Agent steps require a string or an object containing string `input`"
                            .into(),
                    ));
                }
            };
            let outcome = self.agent.run(prompt, run).await?;
            let input = agent_text(&outcome)?;
            Ok(serde_json::to_value(AgentStepOutput { input, outcome })?)
        })
    }
}

fn agent_text(outcome: &AgentOutcome) -> Result<String, WorkflowStepError> {
    if outcome
        .response
        .content
        .iter()
        .any(|part| matches!(part, ContentPart::Refusal { .. }))
    {
        return Err(WorkflowStepError::InvalidOutput(
            "Agent returned a refusal that cannot be forwarded automatically".into(),
        ));
    }
    let text = outcome
        .response
        .content
        .iter()
        .filter_map(|part| match part {
            ContentPart::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<String>();
    if text.is_empty() {
        return Err(WorkflowStepError::InvalidOutput(
            "Agent returned no terminal text to forward".into(),
        ));
    }
    Ok(text)
}
