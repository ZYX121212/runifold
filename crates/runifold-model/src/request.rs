use std::collections::BTreeMap;

use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{ExtensionMap, Message};

const PROVIDER_TOOLS_METADATA_KEY: &str = "runifold.request.provider_tools.v1";
const RESPONSE_MODE_METADATA_KEY: &str = "runifold.request.response_mode.v1";

/// A provider-qualified model identity.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
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

/// How the provider should deliver a model response.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ResponseMode {
    /// Deliver incremental events when the provider supports streaming.
    #[default]
    Streaming,
    /// Request one complete provider response and normalize it into events.
    Complete,
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

/// A provider-native hosted tool that has no lossless canonical function-tool form.
///
/// Adapters only consume entries matching their provider namespace. The `options`
/// object contains fields beside the wire-level `type`, which is owned by
/// `tool_type` and cannot be overridden.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ProviderToolSpec {
    /// Provider namespace, such as `ark` or `openai`.
    pub provider: String,
    /// Provider wire-level tool type, such as `web_search`.
    pub tool_type: String,
    /// Provider-specific tool configuration excluding `type`.
    pub options: BTreeMap<String, Value>,
}

impl ProviderToolSpec {
    /// Creates a provider-native tool definition.
    ///
    /// # Errors
    ///
    /// Returns [`crate::ModelErrorKind::InvalidRequest`] for blank or unsafe
    /// provider and tool-type tokens.
    pub fn new(
        provider: impl Into<String>,
        tool_type: impl Into<String>,
    ) -> Result<Self, crate::ModelError> {
        let provider = provider.into();
        let tool_type = tool_type.into();
        if !is_provider_token(&provider) {
            return Err(crate::ModelError::local(
                crate::ModelErrorKind::InvalidRequest,
                "provider-native tool provider must be a non-empty ASCII token",
            ));
        }
        if !is_provider_token(&tool_type) {
            return Err(crate::ModelError::local(
                crate::ModelErrorKind::InvalidRequest,
                "provider-native tool type must be a non-empty ASCII token",
            ));
        }
        Ok(Self {
            provider,
            tool_type,
            options: BTreeMap::new(),
        })
    }

    /// Adds one provider-specific option.
    #[must_use]
    pub fn option(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.options.insert(name.into(), value.into());
        self
    }
}

fn is_provider_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
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

    /// Adds a provider-hosted tool.
    #[must_use]
    pub fn provider_tool(mut self, tool: ProviderToolSpec) -> Self {
        let mut tools = self.provider_tools();
        tools.push(tool);
        self.metadata.insert(
            PROVIDER_TOOLS_METADATA_KEY.into(),
            Value::Array(tools.into_iter().map(provider_tool_value).collect()),
        );
        self
    }

    /// Returns provider-hosted tools separately from application function tools.
    #[must_use]
    pub fn provider_tools(&self) -> Vec<ProviderToolSpec> {
        self.metadata
            .get(PROVIDER_TOOLS_METADATA_KEY)
            .and_then(|value| serde_json::from_value(value.clone()).ok())
            .unwrap_or_default()
    }

    /// Replaces common generation controls.
    #[must_use]
    pub fn generation(mut self, generation: GenerationOptions) -> Self {
        self.generation = generation;
        self
    }

    /// Sets response delivery behavior.
    #[must_use]
    pub fn response_mode(mut self, response_mode: ResponseMode) -> Self {
        let value = match response_mode {
            ResponseMode::Streaming => "streaming",
            ResponseMode::Complete => "complete",
        };
        self.metadata.insert(
            RESPONSE_MODE_METADATA_KEY.into(),
            Value::String(value.into()),
        );
        self
    }

    /// Returns the requested response delivery mode.
    #[must_use]
    pub fn selected_response_mode(&self) -> ResponseMode {
        match self
            .metadata
            .get(RESPONSE_MODE_METADATA_KEY)
            .and_then(Value::as_str)
        {
            Some("complete") => ResponseMode::Complete,
            _ => ResponseMode::Streaming,
        }
    }

    /// Adds an adapter-owned namespaced option object.
    #[must_use]
    pub fn provider_option(mut self, provider: impl Into<String>, options: Value) -> Self {
        self.provider_options.insert(provider.into(), options);
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
        self.structured_output_with_strictness::<T>(name, true)
    }

    /// Requests structured output described by `T` with explicit provider
    /// strictness.
    #[must_use]
    pub fn structured_output_with_strictness<T>(self, name: impl Into<String>, strict: bool) -> Self
    where
        T: JsonSchema,
    {
        self.output_format(OutputFormat::typed_with_strictness::<T>(name, strict))
    }

    /// Sets the feature-degradation policy.
    #[must_use]
    pub const fn feature_policy(mut self, feature_policy: FeaturePolicy) -> Self {
        self.feature_policy = feature_policy;
        self
    }
}

fn provider_tool_value(tool: ProviderToolSpec) -> Value {
    Value::Object(
        [
            ("provider".into(), Value::String(tool.provider)),
            ("tool_type".into(), Value::String(tool.tool_type)),
            (
                "options".into(),
                Value::Object(tool.options.into_iter().collect()),
            ),
        ]
        .into_iter()
        .collect(),
    )
}
