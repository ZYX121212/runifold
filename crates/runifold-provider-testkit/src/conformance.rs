//! Provider-neutral acceptance checks over the canonical model boundary.

use runifold_core::RetrySafety;
use runifold_model::{
    ContentPart, Model, ModelCallContext, ModelError, ModelErrorKind, ModelRequest, ModelUsage,
};
use thiserror::Error;

/// One successfully verified provider behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConformanceCheck {
    /// The response retained the configured provider identity.
    ProviderIdentity,
    /// Visible text matched without including reasoning.
    VisibleText,
    /// Reasoning was normalized into canonical reasoning blocks.
    Reasoning,
    /// Detailed token usage matched.
    Usage,
    /// Raw provider events were retained and correctly namespaced.
    ProviderEvents,
    /// A failure had the expected kind, provider, and retry safety.
    ErrorClassification,
}

/// Evidence produced by a conformance run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderConformanceReport {
    provider: String,
    checks: Vec<ConformanceCheck>,
}

impl ProviderConformanceReport {
    /// Returns the provider namespace that was verified.
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Returns the checks completed by this run.
    pub fn checks(&self) -> &[ConformanceCheck] {
        &self.checks
    }
}

/// Expected canonical result of a successful provider invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SuccessContract {
    provider: String,
    visible_text: Option<String>,
    reasoning: Option<String>,
    usage: Option<ModelUsage>,
    require_provider_events: bool,
}

impl SuccessContract {
    /// Starts a contract for one canonical provider namespace.
    pub fn new(provider: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            visible_text: None,
            reasoning: None,
            usage: None,
            require_provider_events: false,
        }
    }

    /// Requires exact model-visible text.
    #[must_use]
    pub fn visible_text(mut self, text: impl Into<String>) -> Self {
        self.visible_text = Some(text.into());
        self
    }

    /// Requires exact concatenated canonical reasoning text.
    #[must_use]
    pub fn reasoning(mut self, reasoning: impl Into<String>) -> Self {
        self.reasoning = Some(reasoning.into());
        self
    }

    /// Requires exact normalized usage.
    #[must_use]
    pub const fn usage(mut self, usage: ModelUsage) -> Self {
        self.usage = Some(usage);
        self
    }

    /// Requires at least one correctly namespaced raw provider event.
    #[must_use]
    pub const fn provider_events(mut self) -> Self {
        self.require_provider_events = true;
        self
    }
}

/// Expected classification of a failed provider invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorContract {
    provider: String,
    kind: ModelErrorKind,
    retry_safety: RetrySafety,
}

impl ErrorContract {
    /// Creates an exact normalized error contract.
    pub fn new(
        provider: impl Into<String>,
        kind: ModelErrorKind,
        retry_safety: RetrySafety,
    ) -> Self {
        Self {
            provider: provider.into(),
            kind,
            retry_safety,
        }
    }
}

/// A provider violated its canonical acceptance contract.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProviderConformanceError {
    /// The provider invocation failed before success checks could run.
    #[error("provider invocation failed during success conformance: {0}")]
    Invocation(#[source] ModelError),
    /// A failure contract unexpectedly produced a response.
    #[error("provider invocation unexpectedly succeeded during error conformance")]
    UnexpectedSuccess,
    /// The canonical response used another provider identity.
    #[error("expected provider `{expected}`, received `{actual}`")]
    ProviderIdentity {
        /// Required provider.
        expected: String,
        /// Actual provider.
        actual: String,
    },
    /// Model-visible text differed.
    #[error("provider visible text did not match the contract")]
    VisibleText,
    /// Canonical reasoning differed.
    #[error("provider reasoning did not match the contract")]
    Reasoning,
    /// Normalized usage differed.
    #[error("provider usage did not match the contract")]
    Usage,
    /// No raw provider event was retained.
    #[error("provider response did not retain a raw provider event")]
    MissingProviderEvent,
    /// A retained raw event used another namespace.
    #[error("raw provider event used `{actual}` instead of `{expected}`")]
    ProviderEventIdentity {
        /// Required provider.
        expected: String,
        /// Actual provider.
        actual: String,
    },
    /// The normalized error category differed.
    #[error("expected error kind {expected:?}, received {actual:?}")]
    ErrorKind {
        /// Required kind.
        expected: ModelErrorKind,
        /// Actual kind.
        actual: ModelErrorKind,
    },
    /// The failed invocation omitted or changed provider identity.
    #[error("expected error provider `{expected}`, received {actual:?}")]
    ErrorProvider {
        /// Required provider.
        expected: String,
        /// Actual provider.
        actual: Option<String>,
    },
    /// Retry safety differed.
    #[error("expected retry safety {expected:?}, received {actual:?}")]
    RetrySafety {
        /// Required classification.
        expected: RetrySafety,
        /// Actual classification.
        actual: RetrySafety,
    },
}

/// Executes and verifies one successful canonical provider invocation.
///
/// # Errors
///
/// Returns [`ProviderConformanceError`] when invocation or any requested
/// acceptance check fails.
pub async fn verify_success(
    model: &dyn Model,
    request: ModelRequest,
    context: ModelCallContext,
    contract: &SuccessContract,
) -> Result<ProviderConformanceReport, ProviderConformanceError> {
    let response = model
        .invoke(request, context)
        .await
        .map_err(ProviderConformanceError::Invocation)?;
    let mut checks = Vec::new();
    if response.model.provider != contract.provider {
        return Err(ProviderConformanceError::ProviderIdentity {
            expected: contract.provider.clone(),
            actual: response.model.provider,
        });
    }
    checks.push(ConformanceCheck::ProviderIdentity);

    if let Some(expected) = &contract.visible_text {
        if response.text() != *expected {
            return Err(ProviderConformanceError::VisibleText);
        }
        checks.push(ConformanceCheck::VisibleText);
    }
    if let Some(expected) = &contract.reasoning {
        let reasoning = response
            .content
            .iter()
            .filter_map(|part| match part {
                ContentPart::Reasoning(reasoning) => reasoning.text.as_deref(),
                _ => None,
            })
            .collect::<String>();
        if reasoning != *expected {
            return Err(ProviderConformanceError::Reasoning);
        }
        checks.push(ConformanceCheck::Reasoning);
    }
    if let Some(expected) = contract.usage {
        if response.usage != expected {
            return Err(ProviderConformanceError::Usage);
        }
        checks.push(ConformanceCheck::Usage);
    }
    if contract.require_provider_events {
        if response.provider_events.is_empty() {
            return Err(ProviderConformanceError::MissingProviderEvent);
        }
        if let Some(event) = response
            .provider_events
            .iter()
            .find(|event| event.provider != contract.provider)
        {
            return Err(ProviderConformanceError::ProviderEventIdentity {
                expected: contract.provider.clone(),
                actual: event.provider.clone(),
            });
        }
        checks.push(ConformanceCheck::ProviderEvents);
    }
    Ok(ProviderConformanceReport {
        provider: contract.provider.clone(),
        checks,
    })
}

/// Executes and verifies one failed canonical provider invocation.
///
/// # Errors
///
/// Returns [`ProviderConformanceError`] when the invocation succeeds or its
/// error classification differs from the contract.
pub async fn verify_error(
    model: &dyn Model,
    request: ModelRequest,
    context: ModelCallContext,
    contract: &ErrorContract,
) -> Result<ProviderConformanceReport, ProviderConformanceError> {
    let Err(error) = model.invoke(request, context).await else {
        return Err(ProviderConformanceError::UnexpectedSuccess);
    };
    if error.kind != contract.kind {
        return Err(ProviderConformanceError::ErrorKind {
            expected: contract.kind.clone(),
            actual: error.kind,
        });
    }
    if error.provider.as_deref() != Some(contract.provider.as_str()) {
        return Err(ProviderConformanceError::ErrorProvider {
            expected: contract.provider.clone(),
            actual: error.provider,
        });
    }
    if error.retry_safety != contract.retry_safety {
        return Err(ProviderConformanceError::RetrySafety {
            expected: contract.retry_safety,
            actual: error.retry_safety,
        });
    }
    Ok(ProviderConformanceReport {
        provider: contract.provider.clone(),
        checks: vec![ConformanceCheck::ErrorClassification],
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use futures_executor::block_on;
    use runifold_model::{
        ContentBlockKind, FinishReason, Message, ModelCapabilities, ModelEventStream, ModelFuture,
        ModelRef, ModelStreamEvent, ProviderEvent,
    };
    use serde_json::json;

    use super::*;

    struct CanonicalModel;

    impl Model for CanonicalModel {
        fn capabilities<'a>(
            &'a self,
            _model: &'a ModelRef,
        ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
            Box::pin(async { Ok(ModelCapabilities::default()) })
        }

        fn stream(
            &self,
            _request: ModelRequest,
            _context: ModelCallContext,
        ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
            let events = vec![
                ModelStreamEvent::ResponseStarted {
                    id: Some("response-1".into()),
                    model: ModelRef::new("test-provider", "test-model"),
                },
                ModelStreamEvent::ContentBlockStarted {
                    index: 0,
                    kind: ContentBlockKind::Reasoning {
                        signature: None,
                        redacted: false,
                    },
                },
                ModelStreamEvent::ReasoningDelta {
                    index: 0,
                    text: "think".into(),
                },
                ModelStreamEvent::ContentBlockCompleted { index: 0 },
                ModelStreamEvent::ContentBlockStarted {
                    index: 1,
                    kind: ContentBlockKind::Text,
                },
                ModelStreamEvent::TextDelta {
                    index: 1,
                    text: "answer".into(),
                },
                ModelStreamEvent::ContentBlockCompleted { index: 1 },
                ModelStreamEvent::UsageUpdated {
                    usage: ModelUsage {
                        input_tokens: 2,
                        output_tokens: 3,
                        reasoning_tokens: 1,
                        ..ModelUsage::default()
                    },
                },
                ModelStreamEvent::Provider {
                    event: ProviderEvent {
                        provider: "test-provider".into(),
                        name: "raw.chunk".into(),
                        payload: json!({"chunk": 1}),
                    },
                },
                ModelStreamEvent::ResponseCompleted {
                    finish_reason: FinishReason::Stop,
                    provider_metadata: BTreeMap::new(),
                },
            ];
            Box::pin(async move {
                Ok(
                    Box::pin(futures_util::stream::iter(events.into_iter().map(Ok)))
                        as ModelEventStream,
                )
            })
        }
    }

    struct FailingModel;

    impl Model for FailingModel {
        fn capabilities<'a>(
            &'a self,
            _model: &'a ModelRef,
        ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
            Box::pin(async { Ok(ModelCapabilities::default()) })
        }

        fn stream(
            &self,
            _request: ModelRequest,
            _context: ModelCallContext,
        ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
            Box::pin(async {
                let mut error = ModelError::local(ModelErrorKind::Provider, "provider unavailable");
                error.provider = Some("test-provider".into());
                error.retry_safety = RetrySafety::Safe;
                Err(error)
            })
        }
    }

    fn request() -> ModelRequest {
        ModelRequest::new(
            ModelRef::new("test-provider", "test-model"),
            Message::user("test"),
        )
    }

    #[test]
    fn verifies_the_full_success_contract() {
        let usage = ModelUsage {
            input_tokens: 2,
            output_tokens: 3,
            reasoning_tokens: 1,
            ..ModelUsage::default()
        };
        let report = block_on(verify_success(
            &CanonicalModel,
            request(),
            ModelCallContext::new(),
            &SuccessContract::new("test-provider")
                .visible_text("answer")
                .reasoning("think")
                .usage(usage)
                .provider_events(),
        ))
        .unwrap();

        assert_eq!(report.checks().len(), 5);
    }

    #[test]
    fn verifies_error_kind_identity_and_retry_safety() {
        let report = block_on(verify_error(
            &FailingModel,
            request(),
            ModelCallContext::new(),
            &ErrorContract::new("test-provider", ModelErrorKind::Provider, RetrySafety::Safe),
        ))
        .unwrap();

        assert_eq!(report.checks(), &[ConformanceCheck::ErrorClassification]);
    }
}
