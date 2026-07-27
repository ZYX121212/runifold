use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::{ContentPart, ModelResponse};

/// Stable category for local structured-output validation failures.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum StructuredOutputErrorKind {
    /// The response contained no model-visible text.
    MissingText,
    /// The provider returned an explicit refusal instead of structured data.
    Refusal,
    /// The textual response was not valid for the requested Rust type.
    InvalidOutput,
}

/// A local failure while decoding a model response into a Rust type.
///
/// This error never includes the complete model output, which may contain
/// sensitive application data. Line and column are retained for diagnostics.
#[derive(Clone, Debug, Deserialize, Eq, Error, PartialEq, Serialize)]
#[error("{kind:?}: {message}")]
pub struct StructuredOutputError {
    /// Stable failure category.
    pub kind: StructuredOutputErrorKind,
    /// Safe diagnostic message.
    pub message: String,
    /// One-based JSON line, when parsing reached textual input.
    pub line: Option<usize>,
    /// One-based JSON column, when parsing reached textual input.
    pub column: Option<usize>,
}

impl StructuredOutputError {
    fn new(kind: StructuredOutputErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            line: None,
            column: None,
        }
    }
}

impl ModelResponse {
    /// Decodes ordered textual output into a Rust type.
    ///
    /// Reasoning, citations, and opaque provider metadata are deliberately not
    /// mixed into the JSON body. An explicit refusal fails closed.
    ///
    /// # Errors
    ///
    /// Returns [`StructuredOutputError`] when text is absent, the model
    /// refused, or the assembled JSON does not deserialize as `T`.
    pub fn structured<T>(&self) -> Result<T, StructuredOutputError>
    where
        T: DeserializeOwned,
    {
        let mut body = String::new();
        for part in &self.content {
            match part {
                ContentPart::Text { text } => body.push_str(text),
                ContentPart::Refusal { .. } => {
                    return Err(StructuredOutputError::new(
                        StructuredOutputErrorKind::Refusal,
                        "model refused the structured-output request",
                    ));
                }
                _ => {}
            }
        }

        if body.trim().is_empty() {
            return Err(StructuredOutputError::new(
                StructuredOutputErrorKind::MissingText,
                "model response contained no structured-output text",
            ));
        }

        serde_json::from_str(&body).map_err(|error| StructuredOutputError {
            kind: StructuredOutputErrorKind::InvalidOutput,
            message: "structured output did not match the requested Rust type".into(),
            line: Some(error.line()),
            column: Some(error.column()),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::Deserialize;

    use crate::{
        ContentPart, FinishReason, ModelRef, ModelResponse, ModelUsage, ReasoningPart,
        StructuredOutputErrorKind,
    };

    #[derive(Debug, Deserialize, Eq, PartialEq)]
    struct Answer {
        value: u32,
    }

    fn response(content: Vec<ContentPart>) -> ModelResponse {
        ModelResponse {
            id: None,
            model: ModelRef::new("test", "model"),
            content,
            finish_reason: FinishReason::Stop,
            usage: ModelUsage::default(),
            warnings: Vec::new(),
            provider_metadata: BTreeMap::new(),
            provider_events: Vec::new(),
        }
    }

    #[test]
    fn decodes_text_blocks_without_mixing_reasoning() {
        let response = response(vec![
            ContentPart::Reasoning(ReasoningPart {
                text: Some("not JSON".into()),
                signature: None,
                redacted: false,
                provider_data: Vec::new(),
            }),
            ContentPart::text("{\"value\":"),
            ContentPart::text("42}"),
        ]);

        assert_eq!(
            response.structured::<Answer>().unwrap(),
            Answer { value: 42 }
        );
    }

    #[test]
    fn refusal_fails_closed_even_when_text_is_present() {
        let response = response(vec![
            ContentPart::text("{\"value\":42}"),
            ContentPart::Refusal {
                text: "cannot comply".into(),
            },
        ]);

        let error = response.structured::<Answer>().unwrap_err();
        assert_eq!(error.kind, StructuredOutputErrorKind::Refusal);
    }

    #[test]
    fn type_mismatch_has_safe_location_metadata() {
        let response = response(vec![ContentPart::text("{\"value\":\"wrong\"}")]);

        let error = response.structured::<Answer>().unwrap_err();
        assert_eq!(error.kind, StructuredOutputErrorKind::InvalidOutput);
        assert_eq!(error.line, Some(1));
        assert!(error.column.is_some());
    }
}
