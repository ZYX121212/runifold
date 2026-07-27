use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    ContentPart, FeaturePolicy, ModelError, ModelErrorKind, ModelRequest, ModelWarning,
    OutputFormat, SupportLevel::Emulated, SupportLevel::Native, SupportLevel::Unknown,
    SupportLevel::Unsupported, ToolChoice,
};

/// How a model or adapter supports a feature.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SupportLevel {
    /// The provider implements the feature directly.
    Native,
    /// The adapter can approximate the feature with visible degradation.
    Emulated,
    /// The feature is known to be unsupported.
    Unsupported,
    /// Support is not known reliably.
    Unknown,
}

/// Support level plus machine-readable constraints.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct FeatureSupport {
    /// Support level.
    pub level: SupportLevel,
    /// Provider- or model-specific constraints.
    pub constraints: BTreeMap<String, Value>,
}

impl FeatureSupport {
    /// Creates feature support without constraints.
    pub fn new(level: SupportLevel) -> Self {
        Self {
            level,
            constraints: BTreeMap::new(),
        }
    }
}

/// Capabilities of a specific model endpoint.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ModelCapabilities {
    /// Streaming output.
    pub streaming: FeatureSupport,
    /// Tool calling.
    pub tools: FeatureSupport,
    /// Parallel tool calls.
    pub parallel_tools: FeatureSupport,
    /// Native structured output.
    pub structured_output: FeatureSupport,
    /// Reasoning or thinking round trips.
    pub reasoning: FeatureSupport,
    /// Image input.
    pub image_input: FeatureSupport,
    /// Audio input.
    pub audio_input: FeatureSupport,
    /// Document input.
    pub document_input: FeatureSupport,
    /// Known context-window limit.
    pub max_context_tokens: Option<u64>,
    /// Namespaced additional capabilities.
    pub extensions: BTreeMap<String, FeatureSupport>,
}

impl Default for ModelCapabilities {
    fn default() -> Self {
        let unknown = || FeatureSupport::new(SupportLevel::Unknown);
        Self {
            streaming: unknown(),
            tools: unknown(),
            parallel_tools: unknown(),
            structured_output: unknown(),
            reasoning: unknown(),
            image_input: unknown(),
            audio_input: unknown(),
            document_input: unknown(),
            max_context_tokens: None,
            extensions: BTreeMap::new(),
        }
    }
}

impl ModelCapabilities {
    /// Validates the features required by a request and returns visible
    /// compatibility warnings.
    ///
    /// Unsupported features always fail because generic middleware cannot
    /// invent a safe degradation. Unknown support is accepted only under
    /// [`FeaturePolicy::BestEffort`]. Emulation is accepted by
    /// [`FeaturePolicy::AllowEmulation`] and [`FeaturePolicy::BestEffort`].
    ///
    /// # Errors
    ///
    /// Returns [`ModelErrorKind::UnsupportedFeature`] when the request's
    /// feature policy cannot accept a required capability.
    pub fn validate_request(
        &self,
        request: &ModelRequest,
        streaming: bool,
    ) -> Result<Vec<ModelWarning>, ModelError> {
        let mut required = Vec::new();
        if streaming {
            required.push(("streaming", &self.streaming));
        }
        let requires_tools = !request.tools.is_empty()
            || matches!(
                request.tool_choice,
                ToolChoice::Required | ToolChoice::Named { .. }
            );
        if requires_tools {
            required.push(("tools", &self.tools));
        }
        if !matches!(request.output_format, OutputFormat::Text) {
            required.push(("structured_output", &self.structured_output));
        }
        for message in &request.messages {
            for part in &message.content {
                match part {
                    ContentPart::Image { .. } => required.push(("image_input", &self.image_input)),
                    ContentPart::Audio { .. } => required.push(("audio_input", &self.audio_input)),
                    ContentPart::Document { .. } => {
                        required.push(("document_input", &self.document_input));
                    }
                    ContentPart::Reasoning(_) => required.push(("reasoning", &self.reasoning)),
                    _ => {}
                }
            }
        }

        required.sort_by_key(|(name, _)| *name);
        required.dedup_by_key(|(name, _)| *name);
        let mut warnings = Vec::new();
        for (name, support) in required {
            assess_feature(name, support, request.feature_policy, &mut warnings)?;
        }
        Ok(warnings)
    }
}

fn assess_feature(
    name: &str,
    support: &FeatureSupport,
    policy: FeaturePolicy,
    warnings: &mut Vec<ModelWarning>,
) -> Result<(), ModelError> {
    match (support.level, policy) {
        (Native, _) => Ok(()),
        (Emulated, FeaturePolicy::AllowEmulation | FeaturePolicy::BestEffort) => {
            warnings.push(compatibility_warning(
                "runifold.feature_emulated",
                name,
                support,
            ));
            Ok(())
        }
        (Unknown, FeaturePolicy::BestEffort) => {
            warnings.push(compatibility_warning(
                "runifold.feature_support_unknown",
                name,
                support,
            ));
            Ok(())
        }
        (Unsupported, _) => Err(unsupported_error(name, "unsupported", support)),
        (Emulated, FeaturePolicy::Strict) => Err(unsupported_error(name, "emulated", support)),
        (Unknown, FeaturePolicy::Strict | FeaturePolicy::AllowEmulation) => {
            Err(unsupported_error(name, "unknown", support))
        }
    }
}

fn compatibility_warning(code: &str, name: &str, support: &FeatureSupport) -> ModelWarning {
    let mut metadata = support.constraints.clone();
    metadata.insert("feature".into(), Value::String(name.into()));
    ModelWarning {
        code: code.into(),
        message: format!("feature `{name}` is not natively supported"),
        metadata,
    }
}

fn unsupported_error(name: &str, level: &str, support: &FeatureSupport) -> ModelError {
    let mut error = ModelError::local(
        ModelErrorKind::UnsupportedFeature,
        format!("required feature `{name}` has support level `{level}`"),
    );
    error
        .metadata
        .insert("feature".into(), Value::String(name.into()));
    error.metadata.insert(
        "constraints".into(),
        Value::Object(support.constraints.clone().into_iter().collect()),
    );
    error
}

#[cfg(test)]
mod tests {
    use crate::{
        FeaturePolicy, FeatureSupport, Message, ModelCapabilities, ModelErrorKind, ModelRef,
        ModelRequest, OutputFormat, SupportLevel,
    };

    #[test]
    fn strict_policy_rejects_unknown_required_support() {
        let request = ModelRequest::new(ModelRef::new("test", "model"), Message::user("hello"));

        let error = ModelCapabilities::default()
            .validate_request(&request, true)
            .unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::UnsupportedFeature);
    }

    #[test]
    fn best_effort_makes_unknown_support_visible() {
        let request = ModelRequest::new(ModelRef::new("test", "model"), Message::user("hello"))
            .feature_policy(FeaturePolicy::BestEffort);

        let warnings = ModelCapabilities::default()
            .validate_request(&request, true)
            .unwrap();

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].code, "runifold.feature_support_unknown");
    }

    #[test]
    fn unsupported_features_fail_even_in_best_effort_mode() {
        let capabilities = ModelCapabilities {
            structured_output: FeatureSupport::new(SupportLevel::Unsupported),
            streaming: FeatureSupport::new(SupportLevel::Native),
            ..ModelCapabilities::default()
        };
        let request = ModelRequest::new(ModelRef::new("test", "model"), Message::user("hello"))
            .output_format(OutputFormat::Json)
            .feature_policy(FeaturePolicy::BestEffort);

        let error = capabilities.validate_request(&request, true).unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::UnsupportedFeature);
        assert_eq!(error.metadata["feature"], "structured_output");
    }
}
