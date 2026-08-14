use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};

use futures_util::{
    StreamExt,
    future::{Either, select},
};
use runifold_core::{
    Budget, BudgetEvent, BudgetTracker, CapabilitySet, DomainEvent, EffectId, EffectKind,
    EffectRequest, EventId, Instant, InvocationId, LifecycleEvent, RetrySafety, RunContext,
    RunError, RunErrorKind, RunEventKind, Usage,
};
use runifold_effect::{
    EffectExecutionContext, EffectExecutor, EffectFuture, EffectHandler, EffectRecoveryPolicy,
    InMemoryEffectStore,
};
use runifold_model::{
    ContentPart, FeaturePolicy, GenerationOptions, Message, Model, ModelCallContext, ModelError,
    ModelErrorKind, ModelRef, ModelRequest, ModelResponse, ModelStreamAccumulator, OutputFormat,
    ProviderToolSpec, ResponseMode, Role, ToolCall, ToolChoice, ToolResult,
};
use runifold_retrieval::{Document, Retriever};
use runifold_tool::{ToolError, ToolErrorKind, ToolOutput, ToolRegistry};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::checkpoint::CheckpointCursor;
use crate::stream::{AgentObserver, BufferedObserver, NoopObserver, emit_agent_event};
use crate::{
    AgentCheckpoint, AgentCheckpointPhase, AgentCheckpointState, DurableConversationCheckpoint,
    ResumePolicy,
};

const TOOL_RESULT_EXECUTION_ID_METADATA: &str = "runifold.agent.execution_id";
use crate::{
    AgentError, AgentEventStream, AgentGateway, AgentOutcome, AgentStreamEvent, CallableKind,
    CompletionRequirement, GatewayError, GatewayErrorKind, StructuredAgent,
};

mod callable;
mod checkpointing;
pub(crate) mod completion;
mod execution;
mod observability;
mod retrieval;

/// A boxed, sendable future returned by an agent.
#[cfg(not(target_arch = "wasm32"))]
pub type AgentFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A boxed future returned by an agent on single-threaded WASM.
#[cfg(target_arch = "wasm32")]
pub type AgentFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// One dynamic, capability-gated context source.
#[derive(Clone)]
pub(crate) struct DynamicContext {
    pub(crate) limit: usize,
    pub(crate) retriever: Arc<dyn Retriever>,
}

impl std::fmt::Debug for DynamicContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DynamicContext")
            .field("limit", &self.limit)
            .field("retriever", self.retriever.descriptor())
            .finish()
    }
}

/// How the agent handles tool failures that are safe for model recovery.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum ToolErrorPolicy {
    /// Return safe execution failures to the model as failed tool results.
    #[default]
    ReturnToModel,
    /// Stop the agent immediately on every tool error.
    FailFast,
}

/// Local bounds and recovery behavior for an agent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AgentConfig {
    /// Local turn bound in addition to the shared run budget.
    pub max_turns: u32,
    /// Tool failure behavior.
    pub tool_error_policy: ToolErrorPolicy,
    /// Model capability-degradation policy.
    pub feature_policy: FeaturePolicy,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            max_turns: 16,
            tool_error_policy: ToolErrorPolicy::ReturnToModel,
            feature_policy: FeaturePolicy::Strict,
        }
    }
}

/// One configured model-tool agent.
#[derive(Clone)]
pub struct Agent {
    pub(crate) name: String,
    pub(crate) model: Arc<dyn Model>,
    pub(crate) model_ref: ModelRef,
    pub(crate) instructions: Vec<Message>,
    pub(crate) context: Vec<Document>,
    pub(crate) dynamic_context: Vec<DynamicContext>,
    pub(crate) tools: ToolRegistry,
    pub(crate) agents: AgentGateway,
    pub(crate) effects: EffectExecutor,
    pub(crate) effect_recovery: EffectRecoveryPolicy,
    pub(crate) config: AgentConfig,
    pub(crate) min_successful_tool_calls: u32,
    pub(crate) output_format: OutputFormat,
    pub(crate) generation: GenerationOptions,
    pub(crate) response_mode: ResponseMode,
    pub(crate) provider_tools: Vec<ProviderToolSpec>,
    pub(crate) provider_options: BTreeMap<String, serde_json::Value>,
    pub(crate) completion_requirement: CompletionRequirement,
    pub(crate) completion_validator: completion::CompletionValidator,
}

impl Agent {
    /// Starts a fluent builder for an Agent.
    pub fn builder(
        name: impl Into<String>,
        model: Arc<dyn Model>,
        model_ref: ModelRef,
    ) -> crate::AgentBuilder {
        crate::AgentBuilder::new(name, model, model_ref)
    }

    /// Creates an agent without instructions or tools.
    pub fn new(name: impl Into<String>, model: Arc<dyn Model>, model_ref: ModelRef) -> Self {
        Self {
            name: name.into(),
            model,
            model_ref,
            instructions: Vec::new(),
            context: Vec::new(),
            dynamic_context: Vec::new(),
            tools: ToolRegistry::new(),
            agents: AgentGateway::new(),
            effects: EffectExecutor::new(Arc::new(InMemoryEffectStore::new())),
            effect_recovery: EffectRecoveryPolicy::RejectAmbiguous,
            config: AgentConfig::default(),
            min_successful_tool_calls: 0,
            output_format: OutputFormat::Text,
            generation: GenerationOptions::default(),
            response_mode: ResponseMode::Streaming,
            provider_tools: Vec::new(),
            provider_options: BTreeMap::new(),
            completion_requirement: CompletionRequirement::default(),
            completion_validator: completion::CompletionValidator::content(),
        }
    }

    /// Appends a system instruction.
    #[must_use]
    pub fn system(mut self, instruction: impl Into<String>) -> Self {
        self.instructions.push(Message::system(instruction));
        self
    }

    /// Installs the registry whose tools are exposed and executable.
    #[must_use]
    pub fn tools(mut self, tools: ToolRegistry) -> Self {
        self.tools = tools;
        self
    }

    /// Installs the gateway whose child agents are exposed and callable.
    #[must_use]
    pub fn agents(mut self, agents: AgentGateway) -> Self {
        self.agents = agents;
        self
    }

    /// Replaces the write-ahead effect coordinator shared by callables.
    #[must_use]
    pub fn effect_executor(mut self, effects: EffectExecutor) -> Self {
        self.effects = effects;
        self
    }

    /// Sets recovery behavior for ambiguous callable effects.
    #[must_use]
    pub const fn effect_recovery_policy(mut self, policy: EffectRecoveryPolicy) -> Self {
        self.effect_recovery = policy;
        self
    }

    /// Replaces local execution configuration.
    #[must_use]
    pub const fn with_config(mut self, config: AgentConfig) -> Self {
        self.config = config;
        self
    }

    /// Sets terminal validation and bounded repair behavior.
    #[must_use]
    pub const fn completion_requirement(mut self, requirement: CompletionRequirement) -> Self {
        self.completion_requirement = requirement;
        self
    }

    /// Requires this many successful local Tool calls before terminal output.
    ///
    /// Failed Tool results, child-Agent delegations, provider-hosted Tools,
    /// and Tool results from earlier conversation turns do not satisfy this
    /// execution-local completion contract. A value of zero disables it.
    #[must_use]
    pub const fn min_successful_tool_calls(mut self, minimum: u32) -> Self {
        self.min_successful_tool_calls = minimum;
        self
    }

    /// Sets the desired format for the terminal model response.
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

    /// Binds provider schema generation and local decoding to the same type.
    pub fn into_structured<T>(mut self, name: impl Into<String>) -> StructuredAgent<T>
    where
        T: JsonSchema + serde::de::DeserializeOwned + Send + 'static,
    {
        self = self.structured_output::<T>(name);
        self.completion_validator = completion::CompletionValidator::structured::<T>();
        StructuredAgent::new(self)
    }

    /// Returns the stable local agent name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the provider and model identity used by this Agent.
    pub const fn model_ref(&self) -> &ModelRef {
        &self.model_ref
    }

    /// Returns capabilities for every Tool and child Agent exposed by this
    /// Agent.
    ///
    /// The returned set is not granted automatically. Applications explicitly
    /// decide whether to install it on a root or delegated Run.
    pub fn callable_capabilities(&self) -> CapabilitySet {
        let mut capabilities = CapabilitySet::new();
        for spec in self.tools.model_specs() {
            if let Some(descriptor) = self.tools.descriptor(&spec.name) {
                capabilities.grant(descriptor.capability());
            }
        }
        for spec in self.agents.model_specs() {
            if let Some(descriptor) = self.agents.descriptor(&spec.name) {
                capabilities.grant(descriptor.capability());
            }
        }
        for source in &self.dynamic_context {
            capabilities.grant(source.retriever.descriptor().capability());
        }
        capabilities
    }

    /// Creates a root context for the ergonomic prompt surface.
    ///
    /// The context has no hard budget limits and grants only the Tool and child
    /// Agent capabilities explicitly registered on this Agent. Applications
    /// that need deadlines, tighter budgets, narrower authority, durable
    /// journals, or shared run trees should construct a [`RunContext`] and use
    /// [`Self::run`] instead.
    #[must_use]
    pub fn default_run_context(&self) -> RunContext {
        RunContext::root(
            BudgetTracker::new(Budget::default()),
            self.callable_capabilities(),
        )
    }
}

impl std::fmt::Debug for Agent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Agent")
            .field("name", &self.name)
            .field("model_ref", &self.model_ref)
            .field("instructions", &self.instructions)
            .field("context", &self.context)
            .field("dynamic_context", &self.dynamic_context)
            .field("tools", &self.tools)
            .field("agents", &self.agents)
            .field("effects", &self.effects)
            .field("effect_recovery", &self.effect_recovery)
            .field("config", &self.config)
            .field("output_format", &self.output_format)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
