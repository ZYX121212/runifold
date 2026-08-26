//! Ergonomic public facade for Runifold.
//!
//! The shortest Agent path is provider client construction, fluent assembly,
//! and `Agent::prompt_text`. Applications can opt into explicit
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
#[cfg(feature = "runtime")]
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
#[cfg(feature = "runtime")]
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

#[cfg(feature = "runtime")]
pub use runifold_agent::{
    Agent, AgentBuildError, AgentBuilder, AgentCheckpoint, AgentCheckpointPhase,
    AgentCheckpointState, AgentConfig, AgentConversationError, AgentConversationOutcome,
    AgentDescriptor, AgentError, AgentEventStream, AgentGateway, AgentOutcome, AgentPromptError,
    AgentRegistrationError, AgentRoute, AgentStreamEvent, AutomaticConversationSummary,
    CallableKind, CompletionRequirement, ConversationAppend, ConversationContextPolicy,
    ConversationCreateOutcome, ConversationId, ConversationSequence, ConversationStore,
    ConversationStoreError, ConversationStoreErrorKind, ConversationStoreFuture,
    ConversationSummarizer, ConversationSummarizerError, ConversationSummarizerFuture,
    ConversationSummary, ConversationSummaryBatch, ConversationSummaryCommit,
    ConversationSummaryPassLimit, ConversationSummaryRequest, ConversationTranscriptEntry,
    ConversationVersion, ConversationView, ConversationWindow, DelegationRequest,
    DurableConversationCheckpoint, DurableConversationCommit, DurableConversationRequest,
    DurableConversationStore, GatewayDecision, GatewayError, GatewayErrorKind, GatewayFuture,
    GatewayMiddleware, GatewayNext, GatewayPolicy, InMemoryConversationStore, MemoryNamespace,
    PolicyMiddleware, ResumePolicy, SemanticMemory, SemanticMemoryId, SemanticMemoryQuery,
    SemanticMemorySearchOutcome, SemanticMemorySource, SemanticMemoryUpsert,
    SemanticMemoryUpsertOutcome, StructuredAgent, StructuredAgentError, StructuredAgentOutcome,
    TerminalRequirementFailure, TerminalRequirementFailureKind, ToolErrorPolicy,
};
pub use runifold_core::{
    AuthorityAmplification, Budget, BudgetExceeded, BudgetReservation, BudgetReservationMismatch,
    BudgetResource, BudgetTracker, CancellationToken, CapabilitySet, Checkpoint, CheckpointError,
    CheckpointErrorKind, CheckpointId, CheckpointStore, ChildEvent, ChildRunError, DomainEvent,
    InMemoryCheckpointStore, InMemoryJournal, Journal, JournalError, LifecycleEvent, RetrySafety,
    RunContext, RunError, RunErrorKind, RunEvent, RunEventKind, RunRecorder, Usage,
};
#[cfg(feature = "runtime")]
pub use runifold_effect::{
    EffectEventPayloadPolicy, EffectExecutionContext, EffectExecutor, EffectExecutorError,
    EffectExecutorErrorKind, EffectFuture, EffectHandler, EffectOutcome, EffectReconciler,
    EffectReconciliation, EffectRecord, EffectRecoveryPolicy, EffectStatus, EffectStore,
    InMemoryEffectStore,
};
#[cfg(feature = "runtime")]
pub use runifold_macros::tool;
pub use runifold_model::{
    Artifact, ArtifactError, ArtifactFuture, ArtifactPage, ArtifactRef, ArtifactResolvingModel,
    ArtifactScope, ArtifactStore, ArtifactWrite, BatchProfile, CapabilityAudit,
    CapabilityAuditEntry, CircuitBreakerConfig, CircuitBreakerConfigError, CircuitState,
    ContentPart, DEFAULT_MAX_ARTIFACT_BYTES, FeaturePolicy, GeneratedImage, GenerationOptions,
    ImageFormat, ImageGenerationModel, ImageGenerationRequest, ImageGenerationResponse,
    InMemoryArtifactStore, InteractiveProfile, MAX_ARTIFACT_IDEMPOTENCY_KEY_BYTES,
    MAX_ARTIFACT_NAME_BYTES, MAX_ARTIFACT_PAGE_SIZE, MediaSource, Message, Model, ModelCallContext,
    ModelCapabilityCatalog, ModelEventStream, ModelFallbackPolicy, ModelRef, ModelRequest,
    ModelResponse, ModelRetryPolicy, ModelRetryPolicyError, ModelRoute, ModelRouteHealth,
    ModelRouter, ModelRouterBuildError, ModelRouterBuilder, OutputFormat, ProductionProfile,
    ProviderModel, ProviderRuntimeProfile, ProviderToolSpec, ResponseMode, RetryJitter,
    RouterClock, RouterSleepFuture, RouterSleeper, RuntimeProfilePreset, SpeechFormat, SpeechModel,
    SpeechRequest, SpeechResponse, StructuredOutputError, StructuredOutputErrorKind, SupportLevel,
    SystemRouterClock, SystemRouterSleeper, TranscriptionModel, TranscriptionRequest,
    TranscriptionResponse,
};
#[cfg(feature = "runtime")]
pub use runifold_retrieval::{
    Document, DocumentId, Embedding, EmbeddingBatch, EmbeddingFuture, EmbeddingModel,
    EmbeddingRequest, EmbeddingTask, HybridRetriever, InMemoryVectorIndex, IndexBuildOutcome,
    ReciprocalRankFusion, RerankRequest, RerankResponse, Reranker, RerankerDescriptor,
    RerankingRetriever, RetrievalContext, RetrievalError, RetrievalFuture, RetrievalQuery,
    RetrievalResponse, RetrievedDocument, Retriever, RetrieverDescriptor, VectorRecord,
    VectorRetriever, VectorSearchResponse, VectorSearchResult, VectorStore, VectorStoreFuture,
    VectorUpsertOutcome,
};
#[cfg(feature = "runtime")]
pub use runifold_tool::{
    FunctionTool, IntoToolError, State, Tool, ToolContext, ToolDescriptor, ToolError,
    ToolErrorKind, ToolLimits, ToolOutput, ToolRegistry,
};
#[cfg(feature = "runtime")]
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
    WorkflowRemediationCheckpoint, WorkflowRemediationPolicy, WorkflowRepairInput,
    WorkflowResumePolicy, WorkflowReviewError, WorkflowReviewFuture, WorkflowReviewRequest,
    WorkflowReviewVerdict, WorkflowReviewer, WorkflowSignal, WorkflowSignalId, WorkflowSignalName,
    WorkflowSignalOutcome, WorkflowSignalRetention, WorkflowSignalSnapshot, WorkflowSignalState,
    WorkflowStep, WorkflowStepError, WorkflowStepFuture, WorkflowStore, WorkflowStoreError,
    WorkflowStoreErrorKind, WorkflowStoreFuture, WorkflowSupervisor, WorkflowSupervisorConfig,
    WorkflowSupervisorMetricSnapshot, WorkflowSupervisorMetrics, WorkflowSupervisorReport,
    WorkflowTask, WorkflowTaskSnapshot, WorkflowTaskStatus, WorkflowTenantBudgetPolicy,
    WorkflowTenantBudgetSnapshot, WorkflowTenantId, WorkflowTenantListLimit, WorkflowTenantPolicy,
    WorkflowWait, WorkflowWaitError, WorkflowWaitOutcome, WorkflowWake, WorkflowWorker,
    WorkflowWorkerError, WorkflowWorkerOutcome, WorkflowWorkerSleepFuture, WorkflowWorkerSleeper,
};
#[cfg(feature = "runtime")]
pub use schemars::{JsonSchema, schema_for};

/// Provider-neutral ergonomics inherited by every concrete model adapter.
///
/// A new provider receives Agent construction and the canonical
/// retry/circuit-breaker router path by implementing [`Model`] and
/// [`ProviderModel`]. Budget enforcement, observability, and durable workflow
/// execution remain downstream runtime policies over the resulting Agent or
/// Model rather than provider-specific behavior.
#[cfg(feature = "runtime")]
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
        let profile = self.runtime_profile();
        self.resilient_with_profile(model, profile)
    }

    /// Starts a single-provider router with an explicitly reviewed profile.
    fn resilient_with_profile(
        self,
        model: impl Into<String>,
        profile: ProviderRuntimeProfile,
    ) -> ModelRouterBuilder {
        let target = self.model_ref(model);
        let retry_policy = profile.selected_retry_policy().clone();
        let circuit_breaker = profile.selected_circuit_breaker().clone();
        ModelRouter::builder(target.clone())
            .route("primary", std::sync::Arc::new(self), target)
            .retry_policy(retry_policy)
            .circuit_breaker(circuit_breaker)
    }

    /// Builds the default fully composed runtime for one physical model.
    ///
    /// # Errors
    ///
    /// Returns [`ModelRouterBuildError`] when the model identity is blank.
    fn runtime(self, model: impl Into<String>) -> Result<ProviderRuntime, ModelRouterBuildError> {
        let profile = self.runtime_profile();
        self.runtime_with_profile(model, profile)
    }

    /// Builds a runtime by layering a standard workload preset over the
    /// adapter's protocol-safe profile.
    ///
    /// # Errors
    ///
    /// Returns [`ModelRouterBuildError`] when the model identity is blank.
    fn runtime_with_preset(
        self,
        model: impl Into<String>,
        preset: impl RuntimeProfilePreset,
    ) -> Result<ProviderRuntime, ModelRouterBuildError> {
        let profile = preset.apply(self.runtime_profile());
        self.runtime_with_profile(model, profile)
    }

    /// Builds a runtime with an explicitly reviewed execution profile.
    ///
    /// # Errors
    ///
    /// Returns [`ModelRouterBuildError`] when the model identity is blank.
    fn runtime_with_profile(
        self,
        model: impl Into<String>,
        profile: ProviderRuntimeProfile,
    ) -> Result<ProviderRuntime, ModelRouterBuildError> {
        let router = self
            .resilient_with_profile(model, profile.clone())
            .build()?;
        Ok(ProviderRuntime::new(router, profile))
    }
}

#[cfg(feature = "runtime")]
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
#[cfg(feature = "runtime")]
#[derive(Clone)]
pub struct ProviderRuntime {
    model: std::sync::Arc<dyn Model>,
    router: std::sync::Arc<ModelRouter>,
    model_ref: ModelRef,
    profile: ProviderRuntimeProfile,
}

#[cfg(feature = "runtime")]
impl ProviderRuntime {
    fn new(router: ModelRouter, profile: ProviderRuntimeProfile) -> Self {
        let model_ref = router.logical_model().clone();
        let router = std::sync::Arc::new(router);
        let model = std::sync::Arc::new(RequestDefaultsModel {
            inner: router.clone(),
            response_mode: profile.selected_response_mode(),
            feature_policy: profile.selected_feature_policy(),
            provider_options: profile.provider_options().clone(),
        });
        Self {
            model,
            router,
            model_ref,
            profile,
        }
    }

    /// Returns the provider-qualified model identity.
    pub const fn model_ref(&self) -> &ModelRef {
        &self.model_ref
    }

    /// Returns the execution profile enforced at the physical model boundary.
    pub const fn profile(&self) -> &ProviderRuntimeProfile {
        &self.profile
    }

    /// Returns current circuit-breaker health for the physical provider route.
    pub fn route_health(&self) -> Vec<ModelRouteHealth> {
        self.router.route_health()
    }

    /// Audits the selected endpoint's declared feature support.
    pub fn capability_audit(
        &self,
    ) -> runifold_model::ModelFuture<'_, Result<CapabilityAudit, runifold_model::ModelError>> {
        Box::pin(async move {
            self.model
                .capabilities(&self.model_ref)
                .await
                .map(|capabilities| capabilities.audit())
        })
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

#[cfg(feature = "runtime")]
impl std::fmt::Debug for ProviderRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderRuntime")
            .field("model_ref", &self.model_ref)
            .field("route_health", &self.route_health())
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "runtime")]
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

#[cfg(feature = "runtime")]
#[derive(Clone)]
struct RequestDefaultsModel {
    inner: std::sync::Arc<dyn Model>,
    response_mode: ResponseMode,
    feature_policy: FeaturePolicy,
    provider_options: std::collections::BTreeMap<String, serde_json::Value>,
}

#[cfg(feature = "runtime")]
impl Model for RequestDefaultsModel {
    fn capabilities<'a>(
        &'a self,
        model: &'a ModelRef,
    ) -> runifold_model::ModelFuture<
        'a,
        Result<runifold_model::ModelCapabilities, runifold_model::ModelError>,
    > {
        self.inner.capabilities(model)
    }

    fn stream(
        &self,
        mut request: ModelRequest,
        context: ModelCallContext,
    ) -> runifold_model::ModelFuture<'_, Result<ModelEventStream, runifold_model::ModelError>> {
        request = request.response_mode(self.response_mode);
        request.feature_policy = self.feature_policy;
        for (provider, policy_options) in &self.provider_options {
            match (request.provider_options.get_mut(provider), policy_options) {
                (
                    Some(serde_json::Value::Object(request_options)),
                    serde_json::Value::Object(policy_options),
                ) => {
                    for (key, value) in policy_options {
                        request_options.insert(key.clone(), value.clone());
                    }
                }
                _ => {
                    request
                        .provider_options
                        .insert(provider.clone(), policy_options.clone());
                }
            }
        }
        self.inner.stream(request, context)
    }
}

#[cfg(all(test, feature = "runtime"))]
mod tests {
    use std::collections::BTreeMap;

    use runifold_testkit::ScriptedModel;

    use crate::{
        BatchProfile, ContentPart, FeaturePolicy, InteractiveProfile, Message, Model,
        ModelCallContext, ModelEventStream, ModelRef, ModelRequest, ProviderModel,
        ProviderModelExt, ProviderRuntimeProfile, ResponseMode,
        model::{FinishReason, ModelFuture, ModelStreamEvent},
    };

    #[derive(Clone, Debug)]
    struct ProfiledModel {
        inner: ScriptedModel,
    }

    impl Model for ProfiledModel {
        fn capabilities<'a>(
            &'a self,
            model: &'a ModelRef,
        ) -> ModelFuture<'a, Result<crate::model::ModelCapabilities, crate::model::ModelError>>
        {
            self.inner.capabilities(model)
        }

        fn stream(
            &self,
            request: ModelRequest,
            context: ModelCallContext,
        ) -> ModelFuture<'_, Result<ModelEventStream, crate::model::ModelError>> {
            self.inner.stream(request, context)
        }
    }

    impl ProviderModel for ProfiledModel {
        fn provider(&self) -> &'static str {
            "profiled"
        }

        fn runtime_profile(&self) -> ProviderRuntimeProfile {
            ProviderRuntimeProfile::conservative()
                .response_mode(ResponseMode::Complete)
                .feature_policy(FeaturePolicy::BestEffort)
                .provider_option(
                    "profiled",
                    serde_json::json!({"parallel_tool_calls": false}),
                )
        }
    }

    #[test]
    fn provider_runtime_applies_adapter_profile_at_the_physical_boundary() {
        let logical = ModelRef::new("profiled", "model");
        let inner = ScriptedModel::new();
        inner.enqueue([
            ModelStreamEvent::ResponseStarted {
                id: Some("response".into()),
                model: logical.clone(),
            },
            ModelStreamEvent::ContentPartCompleted {
                index: 0,
                part: ContentPart::text("ok"),
            },
            ModelStreamEvent::ResponseCompleted {
                finish_reason: FinishReason::Stop,
                provider_metadata: BTreeMap::new(),
            },
        ]);
        let runtime = ProfiledModel {
            inner: inner.clone(),
        }
        .runtime("model")
        .unwrap();

        let request = ModelRequest::new(logical, Message::user("hello")).provider_option(
            "profiled",
            serde_json::json!({
                "parallel_tool_calls": true,
                "deployment_option": "preserved"
            }),
        );
        futures_executor::block_on(runtime.invoke(request, ModelCallContext::new())).unwrap();

        assert_eq!(
            runtime.profile().selected_response_mode(),
            ResponseMode::Complete
        );
        let requests = inner.recorded_requests();
        assert_eq!(requests[0].selected_response_mode(), ResponseMode::Complete);
        assert_eq!(requests[0].feature_policy, FeaturePolicy::BestEffort);
        assert_eq!(
            requests[0].provider_options["profiled"]["parallel_tool_calls"],
            false
        );
        assert_eq!(
            requests[0].provider_options["profiled"]["deployment_option"],
            "preserved"
        );
    }

    #[test]
    fn provider_runtime_rejects_an_empty_model_before_execution() {
        let error = ProfiledModel {
            inner: ScriptedModel::new(),
        }
        .runtime(" ")
        .unwrap_err();

        assert_eq!(error, crate::ModelRouterBuildError::EmptyTarget);
    }

    #[test]
    fn workload_presets_layer_over_adapter_policy() {
        let interactive = ProfiledModel {
            inner: ScriptedModel::new(),
        }
        .runtime_with_preset("model", InteractiveProfile)
        .unwrap();
        let batch = ProfiledModel {
            inner: ScriptedModel::new(),
        }
        .runtime_with_preset("model", BatchProfile)
        .unwrap();

        assert_eq!(
            interactive.profile().selected_response_mode(),
            ResponseMode::Streaming
        );
        assert_eq!(
            batch.profile().selected_response_mode(),
            ResponseMode::Complete
        );
        assert_eq!(
            batch.profile().provider_options()["profiled"]["parallel_tool_calls"],
            false
        );
    }

    #[test]
    fn runtime_exposes_machine_readable_capability_audit() {
        let runtime = ProfiledModel {
            inner: ScriptedModel::new(),
        }
        .runtime("model")
        .unwrap();

        let audit = futures_executor::block_on(runtime.capability_audit()).unwrap();

        assert!(!audit.is_fully_declared());
        assert!(audit.features.iter().any(|entry| {
            entry.feature == "tools" && entry.diagnostic_code == "runifold.capability.unknown"
        }));
    }
}
