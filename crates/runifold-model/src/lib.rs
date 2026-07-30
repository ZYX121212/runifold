//! Provider-neutral model protocol and streaming primitives.

mod capability;
mod circuit;
mod content;
mod error;
mod invocation;
mod request;
mod response;
mod retry;
mod router;
mod stream;
mod structured;

pub use capability::{FeatureSupport, ModelCapabilities, SupportLevel};
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
pub use request::{
    FeaturePolicy, GenerationOptions, ModelRef, ModelRequest, OutputFormat, ToolChoice, ToolSpec,
};
pub use response::{FinishReason, ModelResponse, ModelUsage, ModelWarning};
pub use retry::{
    ModelRetryPolicy, ModelRetryPolicyError, RetryJitter, RouterSleepFuture, RouterSleeper,
    SystemRouterSleeper,
};
pub use router::{
    ModelFallbackPolicy, ModelRoute, ModelRouter, ModelRouterBuildError, ModelRouterBuilder,
};
pub use stream::{ContentBlockKind, ModelStreamAccumulator, ModelStreamEvent, ProviderEvent};
pub use structured::{StructuredOutputError, StructuredOutputErrorKind};

/// Namespaced extension fields.
pub type ExtensionMap = std::collections::BTreeMap<String, serde_json::Value>;
