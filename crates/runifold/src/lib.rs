//! Ergonomic public facade for Runifold.
//!
//! The shortest Agent path is provider client construction, fluent assembly,
//! and [`Agent::prompt_text`]. Applications can opt into explicit
//! [`RunContext`] construction when they need tighter authority, budgets,
//! deadlines, observability, or durable execution.

/// Stable runtime-kernel primitives.
pub mod core {
    pub use runifold_core::*;
}

/// Provider-neutral model protocol and streaming primitives.
pub mod model {
    pub use runifold_model::*;
}

/// Provider-neutral embeddings, retrieval, and reference vector indexing.
pub mod retrieval {
    pub use runifold_retrieval::*;
}

/// Qdrant-backed vector retrieval.
#[cfg(feature = "qdrant")]
pub mod qdrant {
    pub use runifold_retrieval_qdrant::*;
}

/// PostgreSQL/pgvector-backed retrieval.
#[cfg(feature = "pgvector")]
pub mod pgvector {
    pub use runifold_retrieval_pgvector::*;
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

/// `PostgreSQL` distributed workflow task control.
#[cfg(feature = "workflow-postgres")]
pub mod postgres {
    pub use runifold_store_postgres::*;
}

/// S3-compatible immutable Task tombstone archive.
#[cfg(feature = "archive-s3")]
pub mod archive_s3;

pub use runifold_agent::{
    Agent, AgentBuildError, AgentBuilder, AgentCheckpoint, AgentCheckpointPhase,
    AgentCheckpointState, AgentConfig, AgentConversationError, AgentConversationOutcome,
    AgentDescriptor, AgentError, AgentEventStream, AgentGateway, AgentOutcome, AgentPromptError,
    AgentRegistrationError, AgentRoute, AgentStreamEvent, AutomaticConversationSummary,
    CallableKind, ConversationAppend, ConversationContextPolicy, ConversationCreateOutcome,
    ConversationId, ConversationSequence, ConversationStore, ConversationStoreError,
    ConversationStoreErrorKind, ConversationStoreFuture, ConversationSummarizer,
    ConversationSummarizerError, ConversationSummarizerFuture, ConversationSummary,
    ConversationSummaryBatch, ConversationSummaryCommit, ConversationSummaryPassLimit,
    ConversationSummaryRequest, ConversationTranscriptEntry, ConversationVersion, ConversationView,
    ConversationWindow, DelegationRequest, DurableConversationCheckpoint,
    DurableConversationCommit, DurableConversationRequest, DurableConversationStore,
    GatewayDecision, GatewayError, GatewayErrorKind, GatewayFuture, GatewayMiddleware, GatewayNext,
    GatewayPolicy, InMemoryConversationStore, MemoryNamespace, PolicyMiddleware, ResumePolicy,
    SemanticMemory, SemanticMemoryId, SemanticMemoryQuery, SemanticMemorySearchOutcome,
    SemanticMemorySource, SemanticMemoryUpsert, SemanticMemoryUpsertOutcome, StructuredAgent,
    StructuredAgentError, StructuredAgentOutcome, ToolErrorPolicy,
};
pub use runifold_core::{
    AuthorityAmplification, Budget, BudgetExceeded, BudgetReservation, BudgetReservationMismatch,
    BudgetResource, BudgetTracker, CancellationToken, CapabilitySet, Checkpoint, CheckpointError,
    CheckpointErrorKind, CheckpointId, CheckpointStore, ChildEvent, ChildRunError, DomainEvent,
    InMemoryCheckpointStore, InMemoryJournal, Journal, JournalError, LifecycleEvent, RunContext,
    RunEvent, RunEventKind, RunRecorder, Usage,
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
    ModelRouterBuildError, ModelRouterBuilder, OutputFormat, ProviderModel, RetryJitter,
    RouterClock, RouterSleepFuture, RouterSleeper, StructuredOutputError,
    StructuredOutputErrorKind, SystemRouterClock, SystemRouterSleeper,
};
pub use runifold_retrieval::{
    Document, DocumentId, Embedding, EmbeddingBatch, EmbeddingFuture, EmbeddingModel,
    EmbeddingRequest, EmbeddingTask, InMemoryVectorIndex, IndexBuildOutcome, RetrievalContext,
    RetrievalError, RetrievalFuture, RetrievalQuery, RetrievalResponse, RetrievedDocument,
    Retriever, RetrieverDescriptor, VectorRecord, VectorRetriever, VectorSearchResponse,
    VectorSearchResult, VectorStore, VectorStoreFuture, VectorUpsertOutcome,
};
pub use runifold_tool::{
    FunctionTool, IntoToolError, State, Tool, ToolContext, ToolDescriptor, ToolError,
    ToolErrorKind, ToolOutput, ToolRegistry,
};
pub use runifold_workflow::{
    AgentStep, AgentStepOutput, ClaimedWorkflow, InMemoryWorkflowStore, LeaseDuration,
    ParallelBranch, ParallelBranchCheckpoint, PredicateCondition, StepId, SystemWorkflowClock,
    SystemWorkflowWorkerSleeper, WorkerId, Workflow, WorkflowBudgetAuditCursor,
    WorkflowBudgetAuditEvent, WorkflowBudgetAuditKind, WorkflowBudgetAuditLimit,
    WorkflowBudgetAuditProjectionId, WorkflowBudgetAuditProjectionLease,
    WorkflowBudgetForfeitReason, WorkflowBudgetReservationOutcome, WorkflowBuildError,
    WorkflowBuilder, WorkflowCancelOutcome, WorkflowCheckpoint, WorkflowCheckpointHistoryLimit,
    WorkflowCheckpointPhase, WorkflowCheckpointRevision, WorkflowCheckpointState, WorkflowClock,
    WorkflowCondition, WorkflowDefinition, WorkflowDisposition, WorkflowError,
    WorkflowFailurePolicy, WorkflowForkCommand, WorkflowForkOutcome, WorkflowForkPolicy,
    WorkflowFuture, WorkflowInterruptCommand, WorkflowInterruptDecision,
    WorkflowInterruptDecisionOutcome, WorkflowInterruptId, WorkflowInterruptOutcome,
    WorkflowInterruptRequest, WorkflowLease, WorkflowLineage, WorkflowOutcome, WorkflowRegistry,
    WorkflowResumePolicy, WorkflowSignal, WorkflowSignalId, WorkflowSignalName,
    WorkflowSignalOutcome, WorkflowSignalRetention, WorkflowSignalSnapshot, WorkflowSignalState,
    WorkflowStep, WorkflowStepError, WorkflowStepFuture, WorkflowStore, WorkflowStoreError,
    WorkflowStoreErrorKind, WorkflowStoreFuture, WorkflowSupervisor, WorkflowSupervisorConfig,
    WorkflowSupervisorMetricSnapshot, WorkflowSupervisorMetrics, WorkflowSupervisorReport,
    WorkflowTask, WorkflowTaskSnapshot, WorkflowTaskStatus, WorkflowTenantBudgetPolicy,
    WorkflowTenantBudgetSnapshot, WorkflowTenantId, WorkflowTenantListLimit, WorkflowTenantPolicy,
    WorkflowWait, WorkflowWaitError, WorkflowWaitOutcome, WorkflowWake, WorkflowWorker,
    WorkflowWorkerError, WorkflowWorkerOutcome, WorkflowWorkerSleepFuture, WorkflowWorkerSleeper,
};
pub use schemars::{JsonSchema, schema_for};

/// Provider-neutral ergonomics inherited by every concrete model adapter.
///
/// A new provider receives Agent construction and the canonical
/// retry/circuit-breaker router path by implementing [`Model`] and
/// [`ProviderModel`]. Budget enforcement, observability, and durable workflow
/// execution remain downstream runtime policies over the resulting Agent or
/// Model rather than provider-specific behavior.
pub trait ProviderModelExt: ProviderModel + Sized + 'static {
    /// Starts an Agent builder using this adapter's canonical provider identity.
    fn agent(self, name: impl Into<String>, model: impl Into<String>) -> AgentBuilder {
        let model_ref = self.model_ref(model);
        Agent::builder(name, std::sync::Arc::new(self), model_ref)
    }

    /// Starts a single-provider router builder.
    ///
    /// Conservative retry and circuit-breaker defaults are preconfigured.
    /// Retrying still occurs only when the adapter marks the failure as safe,
    /// avoiding duplicate charges for ambiguous failures. Applications can
    /// replace either policy before building.
    fn resilient(self, model: impl Into<String>) -> ModelRouterBuilder {
        let target = self.model_ref(model);
        ModelRouter::builder(target.clone())
            .route("primary", std::sync::Arc::new(self), target)
            .retry_policy(ModelRetryPolicy::default())
            .circuit_breaker(CircuitBreakerConfig::default())
    }

    /// Builds the default fully composed runtime for one physical model.
    ///
    /// # Errors
    ///
    /// Returns [`ModelRouterBuildError`] when the model identity is blank.
    fn runtime(self, model: impl Into<String>) -> Result<ProviderRuntime, ModelRouterBuildError> {
        self.resilient(model).build().map(ProviderRuntime::new)
    }
}

impl<T> ProviderModelExt for T where T: ProviderModel + Sized + 'static {}

/// Fully composed execution boundary for one provider-qualified model.
///
/// The runtime applies conservative retry and circuit-breaker defaults over
/// the canonical streaming [`Model`] boundary. Agents built from it inherit
/// budget, capability, cancellation, observability, and durable workflow
/// semantics from Runifold's provider-neutral runtime layers.
///
/// This is long-lived application state, not a per-request builder result.
/// Store one runtime in the application container and clone it for request
/// handlers. Clones share circuit state; calling [`ProviderModelExt::runtime`]
/// again creates an independent runtime with fresh health state.
#[derive(Clone)]
pub struct ProviderRuntime {
    model: std::sync::Arc<dyn Model>,
    router: std::sync::Arc<ModelRouter>,
    model_ref: ModelRef,
}

impl ProviderRuntime {
    fn new(router: ModelRouter) -> Self {
        let model_ref = router.logical_model().clone();
        let router = std::sync::Arc::new(router);
        Self {
            model: router.clone(),
            router,
            model_ref,
        }
    }

    /// Returns the provider-qualified model identity.
    pub const fn model_ref(&self) -> &ModelRef {
        &self.model_ref
    }

    /// Returns current circuit-breaker health for the physical provider route.
    pub fn route_health(&self) -> Vec<ModelRouteHealth> {
        self.router.route_health()
    }

    /// Starts an Agent using the fully composed model runtime.
    pub fn agent(&self, name: impl Into<String>) -> AgentBuilder {
        Agent::builder(name, self.model.clone(), self.model_ref.clone())
    }

    /// Adds OpenTelemetry `GenAI` instrumentation around the complete model
    /// routing, retry, and circuit-breaker boundary.
    #[cfg(feature = "otel")]
    #[must_use]
    pub fn with_otel(mut self) -> Self {
        self.model =
            std::sync::Arc::new(runifold_observability_otel::OtelModel::from_arc(self.model));
        self
    }
}

impl std::fmt::Debug for ProviderRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderRuntime")
            .field("model_ref", &self.model_ref)
            .field("route_health", &self.route_health())
            .finish_non_exhaustive()
    }
}

impl Model for ProviderRuntime {
    fn capabilities<'a>(
        &'a self,
        model: &'a ModelRef,
    ) -> runifold_model::ModelFuture<
        'a,
        Result<runifold_model::ModelCapabilities, runifold_model::ModelError>,
    > {
        self.model.capabilities(model)
    }

    fn stream(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> runifold_model::ModelFuture<'_, Result<ModelEventStream, runifold_model::ModelError>> {
        self.model.stream(request, context)
    }
}

/// Native Anthropic Messages API adapter.
#[cfg(feature = "anthropic")]
pub mod anthropic {
    pub use crate::ProviderModelExt as AnthropicAgentExt;
    pub use runifold_providers::anthropic::{
        AnthropicClient, AnthropicConfig, AnthropicConfigError, AnthropicEventDecoder,
    };
}

/// Native Amazon Bedrock Converse Stream adapter.
#[cfg(feature = "bedrock")]
pub mod bedrock {
    pub use crate::ProviderModelExt as BedrockAgentExt;
    pub use runifold_providers::bedrock::{
        AwsCredentials, AwsRegion, BedrockClient, BedrockConfigError, BedrockEventDecoder,
        BedrockSdkConfig,
    };
}

/// Native Gemini `GenerateContent` API adapter.
#[cfg(feature = "gemini")]
pub mod gemini {
    pub use crate::ProviderModelExt as GeminiAgentExt;
    pub use runifold_providers::gemini::{
        GeminiClient, GeminiConfig, GeminiConfigError, GeminiEmbeddingModel, GeminiEventDecoder,
    };
}

/// Native Ollama chat API adapter.
#[cfg(feature = "ollama")]
pub mod ollama {
    pub use crate::ProviderModelExt as OllamaAgentExt;
    pub use runifold_providers::ollama::{
        OllamaChunkDecoder, OllamaClient, OllamaConfig, OllamaConfigError, OllamaEmbeddingModel,
    };
}

/// `OpenAI` Responses API adapter.
#[cfg(feature = "openai")]
pub mod openai {
    pub use crate::ProviderModelExt as OpenAiAgentExt;
    pub use runifold_providers::openai::{
        AzureOpenAiApiVersion, OPENAI_REALTIME_MAX_AUDIO_CHUNK_BYTES, OpenAiBatch,
        OpenAiBatchEndpoint, OpenAiBatchRequest, OpenAiBatchStatus, OpenAiClient,
        OpenAiCompatibleProfile, OpenAiConfig, OpenAiConfigError, OpenAiControlError,
        OpenAiControlFuture, OpenAiControlPlane, OpenAiEmbeddingModel, OpenAiFile,
        OpenAiFilePurpose, OpenAiFileUpload, OpenAiModelInfo, OpenAiRealtimeAudioChunk,
        OpenAiRealtimeAudioFormat, OpenAiRealtimeCall, OpenAiRealtimeCallRequest,
        OpenAiRealtimeClient, OpenAiRealtimeClientSecret, OpenAiRealtimeClientSecretRequest,
        OpenAiRealtimeCommand, OpenAiRealtimeConnection, OpenAiRealtimeError, OpenAiRealtimeEvent,
        OpenAiRealtimeModality, OpenAiRealtimeReconnectAttempt, OpenAiRealtimeReconnectController,
        OpenAiRealtimeReconnectError, OpenAiRealtimeReconnectEvent,
        OpenAiRealtimeReconnectFailureKind, OpenAiRealtimeReconnectPolicy,
        OpenAiRealtimeReconnectPolicyError, OpenAiRealtimeReconnectStopReason,
        OpenAiRealtimeSdpOffer, OpenAiRealtimeSessionUpdate, OpenAiRealtimeState,
        OpenAiWireProtocol, RealtimeReconnectDisposition,
    };
    #[cfg(target_arch = "wasm32")]
    pub use runifold_providers::openai::{
        OpenAiRealtimeIceServer, OpenAiRealtimeIceTransportPolicy, OpenAiRealtimePendingWebRtc,
        OpenAiRealtimeWebRtcConnectionState, OpenAiRealtimeWebRtcIceState,
        OpenAiRealtimeWebRtcOptions, OpenAiRealtimeWebRtcSession,
    };
}

/// Azure `OpenAI` v1 Responses adapter.
#[cfg(feature = "azure")]
pub mod azure {
    pub use crate::ProviderModelExt as AzureOpenAiAgentExt;
    pub use runifold_providers::openai::{
        AzureOpenAiApiVersion, OpenAiClient as AzureOpenAiClient, OpenAiConfig, OpenAiConfigError,
        OpenAiEmbeddingModel as AzureOpenAiEmbeddingModel,
    };

    /// Creates an Azure `OpenAI` client using the resource API key.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError`] for a blank key or invalid endpoint.
    pub fn api_key_client(
        resource_endpoint: &str,
        api_key: impl Into<String>,
    ) -> Result<AzureOpenAiClient, OpenAiConfigError> {
        OpenAiConfig::azure_api_key(resource_endpoint, api_key).map(AzureOpenAiClient::new)
    }

    /// Creates an Azure `OpenAI` client using an application-provided Entra
    /// bearer token.
    ///
    /// Token acquisition and refresh remain application responsibilities.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError`] for a blank token or invalid endpoint.
    pub fn entra_client(
        resource_endpoint: &str,
        token: impl Into<String>,
    ) -> Result<AzureOpenAiClient, OpenAiConfigError> {
        OpenAiConfig::azure_bearer_token(resource_endpoint, token).map(AzureOpenAiClient::new)
    }
}

macro_rules! compatible_provider_module {
    (
        $(#[$meta:meta])*
        $feature:literal,
        $module:ident,
        $client:ident,
        $agent_ext:ident,
        $profile:ident
    ) => {
        $(#[$meta])*
        #[cfg(feature = $feature)]
        pub mod $module {
            pub use crate::openai::OpenAiAgentExt as $agent_ext;
            pub use runifold_providers::openai::{
                OpenAiClient as $client, OpenAiConfigError, OpenAiEmbeddingModel,
                OpenAiWireProtocol,
            };

            use runifold_providers::openai::OpenAiCompatibleProfile;

            /// Creates a client using the provider's verified public endpoint.
            ///
            /// # Errors
            ///
            /// Returns [`OpenAiConfigError`] when the API key is blank.
            pub fn client(api_key: impl Into<String>) -> Result<$client, OpenAiConfigError> {
                $client::from_profile(OpenAiCompatibleProfile::$profile, api_key)
            }
        }
    };
}

compatible_provider_module!(
    /// Volcengine Ark Responses API adapter.
    "ark",
    ark,
    ArkClient,
    ArkAgentExt,
    Ark
);
compatible_provider_module!(
    /// `DeepSeek` Chat Completions API adapter.
    "deepseek",
    deepseek,
    DeepSeekClient,
    DeepSeekAgentExt,
    DeepSeek
);
compatible_provider_module!(
    /// Groq Chat Completions API adapter.
    "groq",
    groq,
    GroqClient,
    GroqAgentExt,
    Groq
);
compatible_provider_module!(
    /// Mistral Chat Completions API adapter.
    "mistral",
    mistral,
    MistralClient,
    MistralAgentExt,
    Mistral
);
compatible_provider_module!(
    /// `OpenRouter` multi-provider Chat Completions adapter.
    "openrouter",
    openrouter,
    OpenRouterClient,
    OpenRouterAgentExt,
    OpenRouter
);
compatible_provider_module!(
    /// Perplexity Sonar Chat Completions adapter.
    "perplexity",
    perplexity,
    PerplexityClient,
    PerplexityAgentExt,
    Perplexity
);
compatible_provider_module!(
    /// Together AI Chat Completions adapter.
    "together",
    together,
    TogetherClient,
    TogetherAgentExt,
    Together
);
compatible_provider_module!(
    /// `SiliconFlow` Chat Completions adapter.
    "siliconflow",
    siliconflow,
    SiliconFlowClient,
    SiliconFlowAgentExt,
    SiliconFlow
);
compatible_provider_module!(
    /// xAI Chat Completions adapter.
    "xai",
    xai,
    XAiClient,
    XAiAgentExt,
    XAi
);
compatible_provider_module!(
    /// Zhipu AI Chat Completions adapter.
    "zhipu",
    zhipu,
    ZhipuClient,
    ZhipuAgentExt,
    Zhipu
);

/// Alibaba Model Studio OpenAI-compatible adapter.
#[cfg(feature = "qwen")]
pub mod qwen {
    pub use crate::openai::OpenAiAgentExt as QwenAgentExt;
    pub use runifold_providers::openai::{
        OpenAiClient as QwenClient, OpenAiConfigError, OpenAiEmbeddingModel, OpenAiWireProtocol,
    };

    use runifold_providers::openai::OpenAiCompatibleProfile;

    /// Alibaba Model Studio endpoint region.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub enum QwenRegion {
        /// International endpoint hosted in Singapore.
        #[default]
        International,
        /// Mainland China endpoint hosted in Beijing.
        China,
    }

    /// Creates a client using the selected regional endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError`] when the API key is blank.
    pub fn client(
        region: QwenRegion,
        api_key: impl Into<String>,
    ) -> Result<QwenClient, OpenAiConfigError> {
        let profile = match region {
            QwenRegion::International => OpenAiCompatibleProfile::QwenInternational,
            QwenRegion::China => OpenAiCompatibleProfile::QwenChina,
        };
        QwenClient::from_profile(profile, api_key)
    }
}

/// `MiniMax` OpenAI-compatible adapter.
#[cfg(feature = "minimax")]
pub mod minimax {
    pub use crate::openai::OpenAiAgentExt as MiniMaxAgentExt;
    pub use runifold_providers::openai::{
        OpenAiClient as MiniMaxClient, OpenAiConfigError, OpenAiEmbeddingModel, OpenAiWireProtocol,
    };

    use runifold_providers::openai::OpenAiCompatibleProfile;

    /// `MiniMax` endpoint region.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    pub enum MiniMaxRegion {
        /// International endpoint.
        #[default]
        International,
        /// Mainland China endpoint.
        China,
    }

    /// Creates a client using the selected regional endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError`] when the API key is blank.
    pub fn client(
        region: MiniMaxRegion,
        api_key: impl Into<String>,
    ) -> Result<MiniMaxClient, OpenAiConfigError> {
        let profile = match region {
            MiniMaxRegion::International => OpenAiCompatibleProfile::MiniMaxInternational,
            MiniMaxRegion::China => OpenAiCompatibleProfile::MiniMaxChina,
        };
        MiniMaxClient::from_profile(profile, api_key)
    }
}

#[cfg(all(test, feature = "anthropic"))]
mod anthropic_tests {
    use crate::anthropic::{AnthropicAgentExt, AnthropicClient};

    #[test]
    fn anthropic_client_builds_an_agent() {
        let agent = AnthropicClient::from_api_key("key")
            .unwrap()
            .agent("researcher", "claude-test")
            .build()
            .unwrap();

        assert_eq!(agent.model_ref().provider, "anthropic");
        assert_eq!(agent.model_ref().name, "claude-test");
    }
}

#[cfg(all(test, feature = "bedrock"))]
mod bedrock_tests {
    use crate::bedrock::{BedrockAgentExt, BedrockClient};

    #[test]
    fn bedrock_client_builds_an_agent_with_native_identity() {
        let agent = BedrockClient::from_credentials(
            "us-east-1",
            "test-access-key",
            "test-secret-key",
            None,
        )
        .unwrap()
        .agent("researcher", "us.anthropic.claude-test-v1:0")
        .build()
        .unwrap();

        assert_eq!(agent.model_ref().provider, "bedrock");
        assert_eq!(agent.model_ref().name, "us.anthropic.claude-test-v1:0");
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

    #[cfg(feature = "deepseek")]
    #[test]
    fn dedicated_provider_module_preserves_identity() {
        use crate::deepseek::{DeepSeekAgentExt as _, client};

        let agent = client("key")
            .unwrap()
            .agent("reasoner", "deepseek-reasoner")
            .build()
            .unwrap();

        assert_eq!(agent.model_ref().provider, "deepseek");
    }

    #[test]
    fn provider_runtime_composes_resilience_and_agent_identity() {
        let runtime = OpenAiClient::from_api_key("key")
            .unwrap()
            .runtime("gpt-test")
            .unwrap();
        let agent = runtime.agent("assistant").build().unwrap();

        assert_eq!(runtime.model_ref(), agent.model_ref());
        assert_eq!(runtime.route_health().len(), 1);
        assert_eq!(runtime.route_health()[0].route, "primary");
    }

    #[test]
    fn provider_runtime_rejects_an_empty_model_before_execution() {
        let error = OpenAiClient::from_api_key("key")
            .unwrap()
            .runtime(" ")
            .unwrap_err();

        assert_eq!(error, crate::ModelRouterBuildError::EmptyTarget);
    }
}
