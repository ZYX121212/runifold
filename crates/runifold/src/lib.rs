//! Ergonomic public facade for Runifold.
//!
//! The facade is intentionally small while the runtime kernel is established.

/// Stable runtime-kernel primitives.
pub mod core {
    pub use runifold_core::*;
}

/// Provider-neutral model protocol and streaming primitives.
pub mod model {
    pub use runifold_model::*;
}

/// Write-ahead external-effect coordination.
pub mod effect {
    pub use runifold_effect::*;
}

/// Model Context Protocol client, server, and Tool adapters.
#[cfg(feature = "mcp")]
pub mod mcp {
    pub use runifold_mcp::*;
}

/// OpenTelemetry `GenAI` instrumentation.
#[cfg(feature = "otel")]
pub mod otel {
    pub use runifold_observability_otel::*;
}

/// Durable local persistence backed by `SQLite`.
#[cfg(feature = "sqlite")]
pub mod sqlite {
    pub use runifold_store_sqlite::*;
}

pub use runifold_agent::{
    Agent, AgentBuildError, AgentBuilder, AgentCheckpoint, AgentCheckpointPhase,
    AgentCheckpointState, AgentConfig, AgentDescriptor, AgentError, AgentEventStream, AgentGateway,
    AgentOutcome, AgentRegistrationError, AgentRoute, AgentStreamEvent, CallableKind,
    DelegationRequest, GatewayDecision, GatewayError, GatewayErrorKind, GatewayFuture,
    GatewayMiddleware, GatewayNext, GatewayPolicy, PolicyMiddleware, ResumePolicy, StructuredAgent,
    StructuredAgentError, StructuredAgentOutcome, ToolErrorPolicy,
};
pub use runifold_core::{
    Budget, BudgetExceeded, BudgetReservation, BudgetReservationMismatch, BudgetResource,
    BudgetTracker, CapabilitySet, Checkpoint, CheckpointError, CheckpointErrorKind, CheckpointId,
    CheckpointStore, ChildEvent, DomainEvent, InMemoryCheckpointStore, InMemoryJournal, Journal,
    JournalError, LifecycleEvent, RunContext, RunEvent, RunEventKind, RunRecorder, Usage,
};
pub use runifold_effect::{
    EffectEventPayloadPolicy, EffectExecutionContext, EffectExecutor, EffectExecutorError,
    EffectExecutorErrorKind, EffectFuture, EffectHandler, EffectOutcome, EffectRecord,
    EffectRecoveryPolicy, EffectStatus, EffectStore, InMemoryEffectStore,
};
pub use runifold_macros::tool;
pub use runifold_model::{
    CircuitBreakerConfig, CircuitBreakerConfigError, CircuitState, Message, Model,
    ModelCallContext, ModelEventStream, ModelFallbackPolicy, ModelRef, ModelRequest, ModelResponse,
    ModelRetryPolicy, ModelRetryPolicyError, ModelRoute, ModelRouteHealth, ModelRouter,
    ModelRouterBuildError, ModelRouterBuilder, OutputFormat, RetryJitter, RouterClock,
    RouterSleepFuture, RouterSleeper, StructuredOutputError, StructuredOutputErrorKind,
    SystemRouterClock, SystemRouterSleeper,
};
pub use runifold_tool::{
    FunctionTool, IntoToolError, State, Tool, ToolContext, ToolDescriptor, ToolError,
    ToolErrorKind, ToolOutput, ToolRegistry,
};
pub use runifold_workflow::{
    AgentStep, AgentStepOutput, ParallelBranch, ParallelBranchCheckpoint, PredicateCondition,
    StepId, Workflow, WorkflowBuildError, WorkflowBuilder, WorkflowCheckpoint,
    WorkflowCheckpointPhase, WorkflowCheckpointState, WorkflowCondition, WorkflowError,
    WorkflowFuture, WorkflowOutcome, WorkflowResumePolicy, WorkflowStep, WorkflowStepError,
    WorkflowStepFuture,
};
pub use schemars::{JsonSchema, schema_for};

/// Native Anthropic Messages API adapter.
#[cfg(feature = "anthropic")]
pub mod anthropic {
    use std::sync::Arc;

    use runifold_agent::{Agent, AgentBuilder};
    use runifold_model::ModelRef;

    pub use runifold_provider_anthropic::{
        AnthropicClient, AnthropicConfig, AnthropicConfigError, AnthropicEventDecoder,
    };

    /// Fluent Agent construction from an Anthropic client.
    pub trait AnthropicAgentExt {
        /// Starts an Agent builder using Anthropic's canonical provider identity.
        fn agent(self, name: impl Into<String>, model: impl Into<String>) -> AgentBuilder;
    }

    impl AnthropicAgentExt for AnthropicClient {
        fn agent(self, name: impl Into<String>, model: impl Into<String>) -> AgentBuilder {
            Agent::builder(
                name,
                Arc::new(self),
                ModelRef::new("anthropic", model.into()),
            )
        }
    }
}

/// Native Gemini `GenerateContent` API adapter.
#[cfg(feature = "gemini")]
pub mod gemini {
    use std::sync::Arc;

    use runifold_agent::{Agent, AgentBuilder};
    use runifold_model::ModelRef;

    pub use runifold_provider_gemini::{
        GeminiClient, GeminiConfig, GeminiConfigError, GeminiEventDecoder,
    };

    /// Fluent Agent construction from a Gemini client.
    pub trait GeminiAgentExt {
        /// Starts an Agent builder using Gemini's provider identity.
        fn agent(self, name: impl Into<String>, model: impl Into<String>) -> AgentBuilder;
    }

    impl GeminiAgentExt for GeminiClient {
        fn agent(self, name: impl Into<String>, model: impl Into<String>) -> AgentBuilder {
            Agent::builder(name, Arc::new(self), ModelRef::new("gemini", model.into()))
        }
    }
}

/// Native Ollama chat API adapter.
#[cfg(feature = "ollama")]
pub mod ollama {
    use std::sync::Arc;

    use runifold_agent::{Agent, AgentBuilder};
    use runifold_model::ModelRef;

    pub use runifold_provider_ollama::{
        OllamaChunkDecoder, OllamaClient, OllamaConfig, OllamaConfigError,
    };

    /// Fluent Agent construction from an Ollama client.
    pub trait OllamaAgentExt {
        /// Starts an Agent builder using Ollama's provider identity.
        fn agent(self, name: impl Into<String>, model: impl Into<String>) -> AgentBuilder;
    }

    impl OllamaAgentExt for OllamaClient {
        fn agent(self, name: impl Into<String>, model: impl Into<String>) -> AgentBuilder {
            Agent::builder(name, Arc::new(self), ModelRef::new("ollama", model.into()))
        }
    }
}

/// `OpenAI` Responses API adapter.
#[cfg(feature = "openai")]
pub mod openai {
    use std::sync::Arc;

    use runifold_agent::{Agent, AgentBuilder};
    use runifold_model::ModelRef;

    pub use runifold_provider_openai::{
        OpenAiClient, OpenAiConfig, OpenAiConfigError, OpenAiWireProtocol,
    };

    /// Fluent Agent construction from an OpenAI-compatible client.
    pub trait OpenAiAgentExt {
        /// Starts an Agent builder using this client's provider identity.
        fn agent(self, name: impl Into<String>, model: impl Into<String>) -> AgentBuilder;
    }

    impl OpenAiAgentExt for OpenAiClient {
        fn agent(self, name: impl Into<String>, model: impl Into<String>) -> AgentBuilder {
            let model_ref = ModelRef::new(self.provider(), model);
            Agent::builder(name, Arc::new(self), model_ref)
        }
    }
}

#[cfg(all(test, feature = "anthropic"))]
mod anthropic_tests {
    use crate::anthropic::{AnthropicAgentExt, AnthropicClient, AnthropicConfig};

    #[test]
    fn anthropic_client_builds_an_agent() {
        let agent = AnthropicClient::new(AnthropicConfig::new("key").unwrap())
            .agent("researcher", "claude-test")
            .build()
            .unwrap();

        assert_eq!(agent.model_ref().provider, "anthropic");
        assert_eq!(agent.model_ref().name, "claude-test");
    }
}

#[cfg(all(test, feature = "openai"))]
mod tests {
    use crate::openai::{OpenAiAgentExt, OpenAiClient, OpenAiConfig};

    #[test]
    fn compatible_client_builds_agent_with_its_provider_identity() {
        let agent = OpenAiClient::new(OpenAiConfig::ark("key").unwrap())
            .agent("writer", "doubao-model")
            .build()
            .unwrap();

        assert_eq!(agent.model_ref().provider, "ark");
        assert_eq!(agent.model_ref().name, "doubao-model");
    }
}
