use std::sync::Arc;

use runifold_core::CapabilitySet;
use runifold_effect::{EffectExecutor, EffectRecoveryPolicy};
use runifold_model::{FeaturePolicy, Message, Model, ModelRef, OutputFormat};
use runifold_tool::{Tool, ToolRegistrationError};
use schemars::JsonSchema;
use thiserror::Error;

use crate::{
    Agent, AgentConfig, AgentDescriptor, AgentRegistrationError, AgentRoute, GatewayMiddleware,
    StructuredAgent, ToolErrorPolicy,
};

/// Failure while assembling an [`Agent`].
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum AgentBuildError {
    /// Tool registration failed.
    #[error("agent Tool registration failed: {0}")]
    Tool(#[from] ToolRegistrationError),
    /// Child Agent route registration failed.
    #[error("agent route registration failed: {0}")]
    Route(#[from] AgentRegistrationError),
    /// One model-facing name was used by both a Tool and an Agent.
    #[error("callable name `{0}` is registered as both a Tool and an Agent")]
    CallableNameCollision(String),
    /// The Agent name is blank.
    #[error("agent name cannot be empty")]
    EmptyName,
    /// The configured turn limit cannot execute any model turn.
    #[error("max_turns must be greater than zero")]
    ZeroMaxTurns,
}

/// Fluent assembly of one canonical [`Agent`].
///
/// Registration failures are retained and returned by [`Self::build`] so
/// Tool and child Agent calls remain chainable without silently replacing an
/// existing name.
pub struct AgentBuilder {
    agent: Agent,
    error: Option<AgentBuildError>,
}

impl AgentBuilder {
    /// Creates a builder around the same execution path as [`Agent::new`].
    pub fn new(name: impl Into<String>, model: Arc<dyn Model>, model_ref: ModelRef) -> Self {
        Self {
            agent: Agent::new(name, model, model_ref),
            error: None,
        }
    }

    /// Appends a system instruction.
    #[must_use]
    pub fn system(mut self, instruction: impl Into<String>) -> Self {
        self.agent
            .instructions
            .push(Message::system(instruction.into()));
        self
    }

    /// Registers an owned Tool.
    #[must_use]
    pub fn tool<T>(self, tool: T) -> Self
    where
        T: Tool + 'static,
    {
        self.shared_tool(Arc::new(tool))
    }

    /// Registers a shared, type-erased Tool.
    #[must_use]
    pub fn shared_tool(mut self, tool: Arc<dyn Tool>) -> Self {
        if self.error.is_none() {
            if let Err(error) = self.agent.tools.register(tool) {
                self.error = Some(error.into());
            }
        }
        self
    }

    /// Registers a child Agent route with explicit delegated capabilities.
    #[must_use]
    pub fn child(
        mut self,
        descriptor: AgentDescriptor,
        child: Arc<Agent>,
        capabilities: CapabilitySet,
    ) -> Self {
        if self.error.is_none() {
            let route = AgentRoute::new(descriptor, child).with_capabilities(capabilities);
            if let Err(error) = self.agent.agents.register(route) {
                self.error = Some(error.into());
            }
        }
        self
    }

    /// Appends Gateway around-middleware.
    #[must_use]
    pub fn gateway_layer(mut self, middleware: Arc<dyn GatewayMiddleware>) -> Self {
        self.agent.agents.push_middleware(middleware);
        self
    }

    /// Sets the maximum nested Agent delegation depth.
    #[must_use]
    pub fn max_delegation_depth(mut self, max_depth: u32) -> Self {
        self.agent.agents = self.agent.agents.with_max_depth(max_depth);
        self
    }

    /// Sets the local model-turn limit.
    #[must_use]
    pub const fn max_turns(mut self, max_turns: u32) -> Self {
        self.agent.config.max_turns = max_turns;
        self
    }

    /// Sets Tool failure behavior.
    #[must_use]
    pub const fn tool_error_policy(mut self, policy: ToolErrorPolicy) -> Self {
        self.agent.config.tool_error_policy = policy;
        self
    }

    /// Sets provider feature-degradation behavior.
    #[must_use]
    pub const fn feature_policy(mut self, policy: FeaturePolicy) -> Self {
        self.agent.config.feature_policy = policy;
        self
    }

    /// Sets the desired terminal model-output format.
    #[must_use]
    pub fn output_format(mut self, output_format: OutputFormat) -> Self {
        self.agent.output_format = output_format;
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

    /// Replaces all local Agent configuration.
    #[must_use]
    pub const fn config(mut self, config: AgentConfig) -> Self {
        self.agent.config = config;
        self
    }

    /// Shares a write-ahead effect coordinator with this Agent.
    #[must_use]
    pub fn effect_executor(mut self, effects: EffectExecutor) -> Self {
        self.agent.effects = effects;
        self
    }

    /// Sets recovery behavior for ambiguous callable effects.
    #[must_use]
    pub const fn effect_recovery_policy(mut self, policy: EffectRecoveryPolicy) -> Self {
        self.agent.effect_recovery = policy;
        self
    }

    /// Validates registrations and returns the canonical Agent.
    ///
    /// # Errors
    ///
    /// Returns [`AgentBuildError`] for blank identity, invalid turn bounds,
    /// duplicate registrations, or Tool/Agent name collisions.
    pub fn build(self) -> Result<Agent, AgentBuildError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        if self.agent.name.trim().is_empty() {
            return Err(AgentBuildError::EmptyName);
        }
        if self.agent.config.max_turns == 0 {
            return Err(AgentBuildError::ZeroMaxTurns);
        }
        if let Some(collision) = self
            .agent
            .agents
            .model_specs()
            .into_iter()
            .find(|spec| self.agent.tools.contains(&spec.name))
        {
            return Err(AgentBuildError::CallableNameCollision(collision.name));
        }
        Ok(self.agent)
    }

    /// Builds an Agent whose schema and local decoder are bound to `T`.
    ///
    /// This is the preferred terminal builder operation for structured output
    /// because a different decode type cannot be selected later by mistake.
    ///
    /// # Errors
    ///
    /// Returns [`AgentBuildError`] under the same validation rules as
    /// [`Self::build`].
    pub fn build_structured<T>(
        self,
        name: impl Into<String>,
    ) -> Result<StructuredAgent<T>, AgentBuildError>
    where
        T: JsonSchema,
    {
        self.structured_output::<T>(name)
            .build()
            .map(StructuredAgent::new)
    }
}

impl std::fmt::Debug for AgentBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentBuilder")
            .field("agent", &self.agent.name)
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use runifold_core::{CapabilityId, CapabilitySet, EffectClass, RiskLevel};
    use runifold_model::{ModelRef, OutputFormat};
    use runifold_testkit::ScriptedModel;
    use runifold_tool::{Tool, ToolContext, ToolDescriptor, ToolError, ToolFuture, ToolOutput};
    use schemars::JsonSchema;
    use serde::Deserialize;
    use serde_json::json;

    use crate::{Agent, AgentBuildError, AgentDescriptor};

    struct TestTool {
        descriptor: ToolDescriptor,
    }

    #[derive(Deserialize, JsonSchema)]
    struct TypedAnswer {
        value: u32,
    }

    impl TestTool {
        fn named(name: &str) -> Self {
            Self {
                descriptor: ToolDescriptor {
                    id: CapabilityId::new(),
                    name: name.into(),
                    version: "1".into(),
                    description: "test".into(),
                    input_schema: json!({"type": "object"}),
                    output_schema: json!({"type": "object"}),
                    effect: EffectClass::Pure,
                    risk: RiskLevel::Low,
                    metadata: BTreeMap::new(),
                },
            }
        }
    }

    impl Tool for TestTool {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.descriptor
        }

        fn invoke(
            &self,
            input: serde_json::Value,
            _context: ToolContext,
        ) -> ToolFuture<'_, Result<ToolOutput, ToolError>> {
            Box::pin(async move { Ok(ToolOutput::model_visible(input)) })
        }
    }

    #[test]
    fn fluent_builder_assembles_the_canonical_agent() {
        let model = Arc::new(ScriptedModel::new());
        let agent = Agent::builder("worker", model, ModelRef::new("test", "scripted"))
            .system("Be precise")
            .tool(TestTool::named("lookup"))
            .max_turns(4)
            .build()
            .unwrap();

        assert_eq!(agent.name, "worker");
        assert_eq!(agent.instructions.len(), 1);
        assert!(agent.tools.contains("lookup"));
        assert_eq!(agent.config.max_turns, 4);
        assert_eq!(agent.callable_capabilities().len(), 1);
    }

    #[test]
    fn build_rejects_tool_and_agent_name_collisions() {
        let model = Arc::new(ScriptedModel::new());
        let child = Arc::new(Agent::new(
            "child",
            model.clone(),
            ModelRef::new("test", "child"),
        ));
        let error = Agent::builder("parent", model, ModelRef::new("test", "parent"))
            .tool(TestTool::named("search"))
            .child(
                AgentDescriptor::new("search", "delegate search"),
                child,
                CapabilitySet::new(),
            )
            .build()
            .unwrap_err();

        assert_eq!(
            error,
            AgentBuildError::CallableNameCollision("search".into())
        );
    }

    #[test]
    fn builder_derives_a_strict_output_schema_from_a_rust_type() {
        let example = TypedAnswer { value: 7 };
        assert_eq!(example.value, 7);
        let agent = Agent::builder(
            "worker",
            Arc::new(ScriptedModel::new()),
            ModelRef::new("test", "scripted"),
        )
        .structured_output::<TypedAnswer>("typed_answer")
        .build()
        .unwrap();

        let OutputFormat::JsonSchema {
            name,
            schema,
            strict,
        } = agent.output_format
        else {
            panic!("expected JSON-schema output");
        };
        assert_eq!(name, "typed_answer");
        assert!(strict);
        assert_eq!(schema["properties"]["value"]["type"], "integer");
    }
}
