use std::collections::BTreeMap;

use runifold_core::Usage;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{ContentPart, ModelRef, ProviderData};

/// Why a model stopped producing output.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum FinishReason {
    /// Natural completion.
    Stop,
    /// Output-token or context limit.
    Length,
    /// The model requested one or more tools.
    ToolCalls,
    /// Provider safety or content filter.
    ContentFilter,
    /// The operation was cancelled.
    Cancelled,
    /// Provider reported an error as a terminal reason.
    Error,
    /// Provider-specific reason retained as text.
    Other(String),
    /// No reliable reason was provided.
    #[default]
    Unknown,
}

/// Detailed usage reported by a model provider.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelUsage {
    /// Input tokens.
    pub input_tokens: u64,
    /// Output tokens.
    pub output_tokens: u64,
    /// Reasoning tokens, when reported separately.
    pub reasoning_tokens: u64,
    /// Input tokens served from provider cache.
    pub cached_input_tokens: u64,
    /// Input tokens written to provider cache.
    pub cache_write_tokens: u64,
    /// Estimated or reported cost in micro-US-dollars.
    pub cost_microusd: u64,
}

impl ModelUsage {
    /// Returns total model tokens without double-counting usage details.
    ///
    /// Reasoning tokens are normally a subset of output tokens, just as cached
    /// tokens are a subset of input tokens.
    pub fn total_tokens(self) -> u64 {
        self.input_tokens.saturating_add(self.output_tokens)
    }
}

impl From<ModelUsage> for Usage {
    fn from(value: ModelUsage) -> Self {
        Self {
            tokens: value.total_tokens(),
            cost_microusd: value.cost_microusd,
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ModelUsage;

    #[test]
    fn token_totals_do_not_double_count_reasoning_details() {
        let usage = ModelUsage {
            input_tokens: 10,
            output_tokens: 8,
            reasoning_tokens: 3,
            cached_input_tokens: 4,
            ..ModelUsage::default()
        };

        assert_eq!(usage.total_tokens(), 18);
    }
}

/// A visible feature degradation or translation warning.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ModelWarning {
    /// Stable warning code.
    pub code: String,
    /// Safe explanation.
    pub message: String,
    /// Namespaced details.
    pub metadata: BTreeMap<String, Value>,
}

/// A complete canonical model response.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ModelResponse {
    /// Provider response identity.
    pub id: Option<String>,
    /// Actual model that produced the response.
    pub model: ModelRef,
    /// Ordered output content.
    pub content: Vec<ContentPart>,
    /// Normalized terminal reason.
    pub finish_reason: FinishReason,
    /// Detailed model usage.
    pub usage: ModelUsage,
    /// Explicit degradation and compatibility warnings.
    pub warnings: Vec<ModelWarning>,
    /// Namespaced response metadata.
    pub provider_metadata: BTreeMap<String, Value>,
    /// Provider stream events retained without normalization.
    pub provider_events: Vec<ProviderData>,
}

impl ModelResponse {
    /// Collects model-visible text in canonical content order.
    ///
    /// Reasoning, refusals, Tool calls, citations, and provider-specific
    /// payloads remain available through [`Self::content`] and are not mixed
    /// into the returned text.
    #[must_use]
    pub fn text(&self) -> String {
        self.content
            .iter()
            .filter_map(|part| match part {
                ContentPart::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }

    /// Consumes the response and collects model-visible text in canonical
    /// content order.
    ///
    /// Use this when the remaining response metadata and provider events are
    /// no longer needed.
    #[must_use]
    pub fn into_text(self) -> String {
        self.content
            .into_iter()
            .filter_map(|part| match part {
                ContentPart::Text { text } => Some(text),
                _ => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod response_tests {
    use super::{FinishReason, ModelResponse, ModelUsage};
    use crate::{ContentPart, ModelRef};
    use std::collections::BTreeMap;

    fn response(content: Vec<ContentPart>) -> ModelResponse {
        ModelResponse {
            id: Some("response-1".into()),
            model: ModelRef::new("test", "scripted"),
            content,
            finish_reason: FinishReason::Stop,
            usage: ModelUsage::default(),
            warnings: Vec::new(),
            provider_metadata: BTreeMap::new(),
            provider_events: Vec::new(),
        }
    }

    #[test]
    fn text_collects_only_model_visible_text_in_order() {
        let response = response(vec![
            ContentPart::text("hello"),
            ContentPart::Refusal {
                text: "not included".into(),
            },
            ContentPart::text(" world"),
        ]);

        assert_eq!(response.text(), "hello world");
        assert_eq!(response.into_text(), "hello world");
    }
}
