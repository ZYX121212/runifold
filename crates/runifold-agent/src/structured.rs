use std::marker::PhantomData;

use runifold_core::RunContext;
use runifold_model::StructuredOutputError;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::{Agent, AgentError, AgentEventStream, AgentFuture, StructuredAgentOutcome};

/// Failure while executing or locally decoding a typed Agent run.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StructuredAgentError {
    /// Canonical Agent execution failed.
    #[error(transparent)]
    Agent(#[from] AgentError),
    /// The terminal response did not satisfy the bound Rust output type.
    #[error(transparent)]
    Output(#[from] StructuredOutputError),
}

/// An Agent whose provider schema and local decoder are bound to the same type.
#[derive(Clone)]
pub struct StructuredAgent<T> {
    agent: Agent,
    output: PhantomData<fn() -> T>,
}

impl<T> StructuredAgent<T> {
    pub(crate) const fn new(agent: Agent) -> Self {
        Self {
            agent,
            output: PhantomData,
        }
    }

    /// Returns the underlying canonical Agent.
    pub const fn agent(&self) -> &Agent {
        &self.agent
    }

    /// Consumes the wrapper and returns the underlying canonical Agent.
    pub fn into_agent(self) -> Agent {
        self.agent
    }

    /// Streams the underlying canonical Agent lifecycle.
    ///
    /// The terminal `Completed` event retains an unparsed
    /// [`crate::AgentOutcome`]. Use [`Self::run`] when the terminal item itself
    /// must be typed.
    pub fn stream<'a>(
        &'a self,
        input: impl Into<String> + Send + 'a,
        run: &'a RunContext,
    ) -> AgentEventStream<'a> {
        self.agent.stream(input, run)
    }
}

impl<T> StructuredAgent<T>
where
    T: DeserializeOwned + Send + 'static,
{
    /// Runs the canonical Agent and locally validates its terminal response.
    ///
    /// # Errors
    ///
    /// Terminal validation and bounded repair happen inside the canonical
    /// Agent loop before a completed checkpoint is committed. The final local
    /// decode remains as a defensive invariant check.
    pub fn run<'a>(
        &'a self,
        input: impl Into<String> + Send + 'a,
        run: &'a RunContext,
    ) -> AgentFuture<'a, Result<StructuredAgentOutcome<T>, StructuredAgentError>> {
        Box::pin(async move {
            let outcome = self.agent.run(input, run).await?;
            Ok(outcome.into_structured()?)
        })
    }
}

impl<T> std::fmt::Debug for StructuredAgent<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("StructuredAgent")
            .field(&self.agent)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use runifold_core::{Budget, BudgetTracker, CapabilitySet, RunContext};
    use runifold_model::{
        ContentPart, FinishReason, ModelRef, ModelStreamEvent, OutputFormat,
        StructuredOutputErrorKind,
    };
    use runifold_testkit::ScriptedModel;
    use schemars::JsonSchema;
    use serde::Deserialize;

    use crate::{Agent, StructuredAgentError};

    #[derive(Debug, Deserialize, Eq, JsonSchema, PartialEq)]
    struct Answer {
        value: u32,
    }

    fn events(text: &str) -> Vec<ModelStreamEvent> {
        vec![
            ModelStreamEvent::ResponseStarted {
                id: Some("response".into()),
                model: ModelRef::new("test", "scripted"),
            },
            ModelStreamEvent::ContentPartCompleted {
                index: 0,
                part: ContentPart::text(text),
            },
            ModelStreamEvent::ResponseCompleted {
                finish_reason: FinishReason::Stop,
                provider_metadata: BTreeMap::new(),
            },
        ]
    }

    fn run() -> RunContext {
        RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new())
    }

    #[test]
    fn typed_agent_uses_one_type_for_schema_and_decode() {
        let model = ScriptedModel::new();
        model.enqueue(events("{\"value\":42}"));
        let agent = Agent::builder(
            "typed",
            Arc::new(model.clone()),
            ModelRef::new("test", "scripted"),
        )
        .build_structured::<Answer>("answer")
        .unwrap();
        let run = run();

        let typed = futures_executor::block_on(agent.run("answer", &run)).unwrap();

        assert_eq!(typed.output, Answer { value: 42 });
        let requests = model.recorded_requests();
        let OutputFormat::JsonSchema { name, strict, .. } = &requests[0].output_format else {
            panic!("expected JSON-schema output");
        };
        assert_eq!(name, "answer");
        assert!(*strict);
    }

    #[test]
    fn typed_agent_fails_the_completion_requirement_before_returning_an_outcome() {
        let model = ScriptedModel::new();
        model.enqueue(events("{\"value\":\"wrong\"}"));
        let agent = Agent::new("typed", Arc::new(model), ModelRef::new("test", "scripted"))
            .into_structured::<Answer>("answer");
        let run = run();

        let error = futures_executor::block_on(agent.run("answer", &run)).unwrap_err();

        assert!(matches!(
            error,
            StructuredAgentError::Agent(crate::AgentError::StructuredOutputUnsatisfied {
                attempts: 0,
                kind: StructuredOutputErrorKind::InvalidOutput,
                ..
            })
        ));
    }
}
