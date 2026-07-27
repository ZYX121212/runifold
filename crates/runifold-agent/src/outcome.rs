use runifold_core::Usage;
use runifold_model::{Message, ModelResponse, StructuredOutputError};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// Successful terminal state of an agent run.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AgentOutcome {
    /// Final model response.
    pub response: ModelResponse,
    /// Complete canonical transcript, including tool calls and results.
    pub transcript: Vec<Message>,
    /// Model turns performed by this agent.
    pub turns: u32,
    /// Tool calls attempted by this agent.
    pub tool_calls: u32,
    /// Successful direct child-agent delegations performed by this agent.
    pub delegations: u32,
    /// Shared run-tree usage snapshot at completion.
    pub usage: Usage,
}

impl AgentOutcome {
    /// Locally validates and decodes the final model response while preserving
    /// the complete canonical outcome.
    ///
    /// # Errors
    ///
    /// Returns [`StructuredOutputError`] when the response is missing textual
    /// output, contains a refusal, or does not deserialize as `T`.
    pub fn into_structured<T>(self) -> Result<StructuredAgentOutcome<T>, StructuredOutputError>
    where
        T: DeserializeOwned,
    {
        let output = self.response.structured()?;
        Ok(StructuredAgentOutcome {
            output,
            outcome: self,
        })
    }
}

/// A locally validated typed value and its complete Agent execution outcome.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct StructuredAgentOutcome<T> {
    /// Deserialized final output.
    pub output: T,
    /// Canonical response, transcript, counters, and usage.
    pub outcome: AgentOutcome,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use runifold_core::Usage;
    use runifold_model::{
        ContentPart, FinishReason, ModelRef, ModelResponse, ModelUsage, StructuredOutputErrorKind,
    };
    use serde::Deserialize;

    use super::AgentOutcome;

    #[derive(Debug, Deserialize, Eq, PartialEq)]
    struct Answer {
        value: u32,
    }

    fn outcome(text: &str) -> AgentOutcome {
        AgentOutcome {
            response: ModelResponse {
                id: Some("response".into()),
                model: ModelRef::new("test", "model"),
                content: vec![ContentPart::text(text)],
                finish_reason: FinishReason::Stop,
                usage: ModelUsage::default(),
                warnings: Vec::new(),
                provider_metadata: BTreeMap::new(),
                provider_events: Vec::new(),
            },
            transcript: Vec::new(),
            turns: 1,
            tool_calls: 0,
            delegations: 0,
            usage: Usage::default(),
        }
    }

    #[test]
    fn typed_outcome_preserves_canonical_execution_metadata() {
        let typed = outcome("{\"value\":42}")
            .into_structured::<Answer>()
            .unwrap();

        assert_eq!(typed.output, Answer { value: 42 });
        assert_eq!(typed.outcome.response.id.as_deref(), Some("response"));
        assert_eq!(typed.outcome.turns, 1);
    }

    #[test]
    fn typed_outcome_rejects_a_shape_mismatch() {
        let error = outcome("{\"value\":\"wrong\"}")
            .into_structured::<Answer>()
            .unwrap_err();

        assert_eq!(error.kind, StructuredOutputErrorKind::InvalidOutput);
    }
}
