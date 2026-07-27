use std::collections::BTreeMap;

use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{ExtensionMap, Message};

/// A provider-qualified model identity.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ModelRef {
    /// Provider namespace.
    pub provider: String,
    /// Provider model name.
    pub name: String,
}

impl ModelRef {
    /// Creates a model reference.
    pub fn new(provider: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            name: name.into(),
        }
    }
}

/// Behavior when a requested feature is not natively supported.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum FeaturePolicy {
    /// Reject unsupported, unknown, or emulated features.
    #[default]
    Strict,
    /// Permit documented emulation but reject ignored features.
    AllowEmulation,
    /// Permit degradation when it is reported as a warning.
    BestEffort,
}

/// Sampling and output-length options common to providers.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct GenerationOptions {
    /// Sampling temperature.
    pub temperature: Option<f64>,
    /// Nucleus-sampling probability.
    pub top_p: Option<f64>,
    /// Maximum output tokens.
    pub max_output_tokens: Option<u64>,
    /// Optional deterministic seed.
    pub seed: Option<u64>,
    /// Stop sequences.
    pub stop: Vec<String>,
}

/// Desired final-output format.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum OutputFormat {
    /// Unconstrained text.
    #[default]
    Text,
    /// Any valid JSON value.
    Json,
    /// JSON constrained by a schema.
    JsonSchema {
        /// Schema name sent to providers that require one.
        name: String,
        /// JSON Schema.
        schema: Value,
        /// Whether the provider should enforce its strictest mode.
        strict: bool,
    },
}

impl OutputFormat {
    /// Builds a strict JSON-schema format from a Rust type.
    ///
    /// Provider enforcement is only one boundary. Callers should still decode
    /// the response locally with [`crate::ModelResponse::structured`].
    pub fn typed<T>(name: impl Into<String>) -> Self
    where
        T: JsonSchema,
    {
        Self::JsonSchema {
            name: name.into(),
            schema: schema_for!(T).to_value(),
            strict: true,
        }
    }

    /// Builds a JSON-schema format from a Rust type with explicit provider
    /// strictness.
    pub fn typed_with_strictness<T>(name: impl Into<String>, strict: bool) -> Self
    where
        T: JsonSchema,
    {
        Self::JsonSchema {
            name: name.into(),
            schema: schema_for!(T).to_value(),
            strict,
        }
    }
}

/// A model-facing tool definition.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ToolSpec {
    /// Tool name.
    pub name: String,
    /// Model-facing description.
    pub description: String,
    /// JSON Schema for arguments.
    pub input_schema: Value,
    /// Optional JSON Schema for results.
    pub output_schema: Option<Value>,
    /// Namespaced metadata not automatically exposed to a provider.
    pub metadata: ExtensionMap,
}

/// How a model may select tools.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ToolChoice {
    /// The model decides whether to call a tool.
    #[default]
    Auto,
    /// The model must not call tools.
    None,
    /// The model must call at least one tool.
    Required,
    /// The model must call a named tool.
    Named {
        /// Required tool name.
        name: String,
    },
}

/// A complete provider-neutral model request.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ModelRequest {
    /// Selected model.
    pub model: ModelRef,
    /// Ordered conversation messages.
    pub messages: Vec<Message>,
    /// Model-facing tools.
    pub tools: Vec<ToolSpec>,
    /// Tool-selection behavior.
    pub tool_choice: ToolChoice,
    /// Desired final-output format.
    pub output_format: OutputFormat,
    /// Common generation options.
    pub generation: GenerationOptions,
    /// Feature-degradation behavior.
    pub feature_policy: FeaturePolicy,
    /// Typed adapters serialize options into their provider namespace.
    pub provider_options: BTreeMap<String, Value>,
    /// Host-only namespaced metadata.
    pub metadata: ExtensionMap,
}

impl ModelRequest {
    /// Creates a request with one initial message.
    pub fn new(model: ModelRef, message: Message) -> Self {
        Self {
            model,
            messages: vec![message],
            tools: Vec::new(),
            tool_choice: ToolChoice::Auto,
            output_format: OutputFormat::Text,
            generation: GenerationOptions::default(),
            feature_policy: FeaturePolicy::Strict,
            provider_options: BTreeMap::new(),
            metadata: BTreeMap::new(),
        }
    }

    /// Appends a conversation message.
    #[must_use]
    pub fn message(mut self, message: Message) -> Self {
        self.messages.push(message);
        self
    }

    /// Adds a model-facing tool.
    #[must_use]
    pub fn tool(mut self, tool: ToolSpec) -> Self {
        self.tools.push(tool);
        self
    }

    /// Sets the desired output format.
    #[must_use]
    pub fn output_format(mut self, output_format: OutputFormat) -> Self {
        self.output_format = output_format;
        self
    }

    /// Requests strict structured output described by the Rust type `T`.
    #[must_use]
    pub fn structured_output<T>(self, name: impl Into<String>) -> Self
    where
        T: JsonSchema,
    {
        self.output_format(OutputFormat::typed::<T>(name))
    }

    /// Sets the feature-degradation policy.
    #[must_use]
    pub const fn feature_policy(mut self, feature_policy: FeaturePolicy) -> Self {
        self.feature_policy = feature_policy;
        self
    }
}
