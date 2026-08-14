//! Provider-neutral model protocol and streaming primitives.

mod artifact;
mod capability;
mod capability_catalog;
mod circuit;
mod content;
mod error;
mod invocation;
mod media;
mod request;
mod response;
mod retry;
mod router;
mod runtime_profile;
mod stream;
mod structured;

pub use capability::{
    CapabilityAudit, CapabilityAuditEntry, FeatureSupport, ModelCapabilities, SupportLevel,
};
pub use capability_catalog::ModelCapabilityCatalog;
pub use circuit::{
    CircuitBreakerConfig, CircuitBreakerConfigError, CircuitState, ModelRouteHealth, RouterClock,
    SystemRouterClock,
};
pub use content::{
    Citation, ContentPart, MediaSource, Message, ProviderData, ReasoningPart, Role, ToolCall,
    ToolResult,
};
pub use error::{ModelError, ModelErrorKind};
pub use invocation::{Model, ModelCallContext, ModelEventStream, ModelFuture, ProviderModel};
pub use media::{
    GeneratedImage, ImageFormat, ImageGenerationModel, ImageGenerationRequest,
    ImageGenerationResponse, SpeechFormat, SpeechModel, SpeechRequest, SpeechResponse,
    TranscriptionModel, TranscriptionRequest, TranscriptionResponse,
};
pub use request::{
    FeaturePolicy, GenerationOptions, ModelRef, ModelRequest, OutputFormat, ProviderToolSpec,
    ResponseMode, ToolChoice, ToolSpec,
};
pub use response::{FinishReason, ModelResponse, ModelUsage, ModelWarning};
pub use retry::{
    ModelRetryPolicy, ModelRetryPolicyError, RetryJitter, RouterSleepFuture, RouterSleeper,
    SystemRouterSleeper,
};
pub use router::{
    ModelFallbackPolicy, ModelRoute, ModelRouter, ModelRouterBuildError, ModelRouterBuilder,
};
pub use runtime_profile::{
    BatchProfile, InteractiveProfile, ProductionProfile, ProviderRuntimeProfile,
    RuntimeProfilePreset,
};
pub use stream::{ContentBlockKind, ModelStreamAccumulator, ModelStreamEvent, ProviderEvent};
pub use structured::{StructuredOutputError, StructuredOutputErrorKind};

/// Namespaced extension fields.
pub type ExtensionMap = std::collections::BTreeMap<String, serde_json::Value>;
pub use artifact::{
    Artifact, ArtifactError, ArtifactFuture, ArtifactPage, ArtifactRef, ArtifactResolvingModel,
    ArtifactScope, ArtifactStore, ArtifactWrite, DEFAULT_MAX_ARTIFACT_BYTES, InMemoryArtifactStore,
    MAX_ARTIFACT_IDEMPOTENCY_KEY_BYTES, MAX_ARTIFACT_NAME_BYTES, MAX_ARTIFACT_PAGE_SIZE,
};
