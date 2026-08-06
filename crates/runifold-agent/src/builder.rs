use std::sync::Arc;

use runifold_core::CapabilitySet;
use runifold_effect::{EffectExecutor, EffectRecoveryPolicy};
use runifold_model::{
    ArtifactResolvingModel, ArtifactScope, ArtifactStore, FeaturePolicy, GenerationOptions,
    Message, Model, ModelRef, OutputFormat, ProviderToolSpec, ResponseMode,
};
use runifold_retrieval::{Document, RetrievalError, Retriever};
use runifold_tool::{Tool, ToolRegistrationError};
use schemars::JsonSchema;
use thiserror::Error;

use crate::agent::DynamicContext;
use crate::{
    Agent, AgentConfig, AgentDescriptor, AgentError, AgentFuture, AgentOutcome,
    AgentRegistrationError, AgentRoute, GatewayMiddleware, StructuredAgent, ToolErrorPolicy,
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
    /// A static or dynamic context registration was invalid.
    #[error("agent retrieval configuration failed: {0}")]
    Retrieval(#[from] RetrievalError),
}

/// Failure while building and immediately prompting an Agent.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AgentPromptError {
    /// Fluent Agent assembly failed before model execution.
    #[error("failed to build agent: {0}")]
    Build(#[from] AgentBuildError),
    /// Canonical Agent execution failed.
    #[error("agent prompt failed: {0}")]
    Run(#[from] AgentError),
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

    /// Adds one static document as untrusted user-level context.
    ///
    /// The document is never promoted to a system instruction. Use
    /// [`Self::system`] for trusted application policy.
    #[must_use]
    pub fn context(self, text: impl Into<String>) -> Self {
        let id = format!("static-context-{}", self.agent.context.len() + 1);
        match Document::new(id, text) {
            Ok(document) => self.context_document(document),
            Err(error) => self.with_error(error.into()),
        }
    }

    /// Adds one validated static context document.
    #[must_use]
    pub fn context_document(mut self, document: Document) -> Self {
        if self.error.is_none() {
            self.agent.context.push(document);
        }
        self
    }

    /// Configures reference-only artifact persistence for Tools and resolves
    /// those references only at the final model transport boundary.
    #[must_use]
    pub fn artifacts(mut self, scope: ArtifactScope, store: Arc<dyn ArtifactStore>) -> Self {
        self.agent.model = Arc::new(ArtifactResolvingModel::new(
            self.agent.model.clone(),
            scope.clone(),
            store.clone(),
        ));
        self.agent.tools = self.agent.tools.clone().with_artifact_store(scope, store);
        self
    }

    /// Adds an owned dynamic context source.
    #[must_use]
    pub fn dynamic_context<R>(self, limit: usize, retriever: R) -> Self
    where
        R: Retriever + 'static,
    {
        self.shared_dynamic_context(limit, Arc::new(retriever))
    }

    /// Adds a shared, type-erased dynamic context source.
    #[must_use]
    pub fn shared_dynamic_context(mut self, limit: usize, retriever: Arc<dyn Retriever>) -> Self {
        if self.error.is_none() {
            if limit == 0 {
                self.error = Some(RetrievalError::ZeroLimit.into());
            } else {
                self.agent
                    .dynamic_context
                    .push(DynamicContext { limit, retriever });
            }
        }
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
        if self.error.is_none()
            && let Err(error) = self.agent.tools.register(tool)
        {
            self.error = Some(error.into());
        }
        self
    }

    fn with_error(mut self, error: AgentBuildError) -> Self {
        if self.error.is_none() {
            self.error = Some(error);
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

    /// Adds a provider-hosted tool such as Ark web search.
    #[must_use]
    pub fn provider_tool(mut self, tool: ProviderToolSpec) -> Self {
        self.agent.provider_tools.push(tool);
        self
    }

    /// Replaces common model generation controls for every Agent turn.
    #[must_use]
    pub fn generation(mut self, generation: GenerationOptions) -> Self {
        self.agent.generation = generation;
        self
    }

    /// Sets the sampling temperature for every Agent turn.
    #[must_use]
    pub fn temperature(mut self, temperature: f64) -> Self {
        self.agent.generation.temperature = Some(temperature);
        self
    }

    /// Sets nucleus sampling for every Agent turn.
    #[must_use]
    pub fn top_p(mut self, top_p: f64) -> Self {
        self.agent.generation.top_p = Some(top_p);
        self
    }

    /// Sets the maximum number of output tokens for every Agent turn.
    #[must_use]
    pub fn max_output_tokens(mut self, max_output_tokens: u64) -> Self {
        self.agent.generation.max_output_tokens = Some(max_output_tokens);
        self
    }

    /// Selects streaming or complete response delivery for every Agent turn.
    #[must_use]
    pub const fn response_mode(mut self, response_mode: ResponseMode) -> Self {
        self.agent.response_mode = response_mode;
        self
    }

    /// Adds namespaced provider options to every Agent turn.
    #[must_use]
    pub fn provider_options(
        mut self,
        provider: impl Into<String>,
        options: serde_json::Value,
    ) -> Self {
        self.agent.provider_options.insert(provider.into(), options);
        self
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

    /// Builds the Agent and runs one ergonomic prompt.
    ///
    /// This removes the explicit build step for one-shot usage while retaining
    /// the complete canonical outcome. Use [`Self::build`] when the Agent will
    /// be reused or executed with an explicit runtime context.
    pub fn prompt(
        self,
        input: impl Into<String> + Send + 'static,
    ) -> AgentFuture<'static, Result<AgentOutcome, AgentPromptError>> {
        let input = input.into();
        Box::pin(async move {
            let agent = self.build()?;
            Ok(agent.prompt(input).await?)
        })
    }

    /// Builds the Agent, runs one ergonomic prompt, and returns only
    /// model-visible text.
    ///
    /// This is the shortest path from provider configuration to a text answer.
    /// Use [`Self::prompt`] when transcript, usage, warnings, and provider
    /// events must be preserved.
    pub fn prompt_text(
        self,
        input: impl Into<String> + Send + 'static,
    ) -> AgentFuture<'static, Result<String, AgentPromptError>> {
        let input = input.into();
        Box::pin(async move {
            let agent = self.build()?;
            Ok(agent.prompt_text(input).await?)
        })
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
    use runifold_model::{
        ContentPart, FinishReason, ModelRef, ModelStreamEvent, OutputFormat, ProviderToolSpec,
        ResponseMode,
    };
    use runifold_testkit::ScriptedModel;
    use runifold_tool::{Tool, ToolContext, ToolDescriptor, ToolError, ToolFuture, ToolOutput};
    use schemars::JsonSchema;
    use serde::Deserialize;
    use serde_json::json;

    use crate::{Agent, AgentBuildError, AgentDescriptor, AgentPromptError};

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
    fn builder_retains_generation_provider_and_delivery_controls() {
        let provider_tool = ProviderToolSpec::new("ark", "web_search").unwrap();
        let agent = Agent::builder(
            "researcher",
            Arc::new(ScriptedModel::new()),
            ModelRef::new("ark", "doubao"),
        )
        .temperature(0.2)
        .top_p(0.8)
        .max_output_tokens(4_096)
        .response_mode(ResponseMode::Complete)
        .provider_tool(provider_tool)
        .provider_options("ark", json!({"thinking": {"type": "enabled"}}))
        .build()
        .unwrap();

        assert_eq!(agent.generation.temperature, Some(0.2));
        assert_eq!(agent.generation.top_p, Some(0.8));
        assert_eq!(agent.generation.max_output_tokens, Some(4_096));
        assert_eq!(agent.response_mode, ResponseMode::Complete);
        assert_eq!(agent.provider_tools[0].tool_type, "web_search");
        assert_eq!(agent.provider_options["ark"]["thinking"]["type"], "enabled");
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

    #[test]
    fn builder_prompt_text_is_a_single_use_golden_path() {
        let model = ScriptedModel::new();
        model.enqueue([
            ModelStreamEvent::ResponseStarted {
                id: Some("response-1".into()),
                model: ModelRef::new("test", "scripted"),
            },
            ModelStreamEvent::ContentPartCompleted {
                index: 0,
                part: ContentPart::text("done"),
            },
            ModelStreamEvent::ResponseCompleted {
                finish_reason: FinishReason::Stop,
                provider_metadata: BTreeMap::new(),
            },
        ]);

        let text = futures_executor::block_on(
            Agent::builder("worker", Arc::new(model), ModelRef::new("test", "scripted"))
                .system("Be precise")
                .prompt_text("start"),
        )
        .unwrap();

        assert_eq!(text, "done");
    }

    #[test]
    fn builder_prompt_reports_build_failures_before_model_execution() {
        let error = futures_executor::block_on(
            Agent::builder(
                "",
                Arc::new(ScriptedModel::new()),
                ModelRef::new("test", "scripted"),
            )
            .prompt("start"),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            AgentPromptError::Build(AgentBuildError::EmptyName)
        ));
    }
}
