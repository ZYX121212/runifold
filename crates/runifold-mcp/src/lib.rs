//! Capability-safe Model Context Protocol support for Runifold.
//!
//! The crate provides canonical Tools, Resources, Prompts, and client-owned
//! Sampling over in-process, newline-delimited stdio, and MCP Streamable HTTP
//! transports. HTTP requests are never retried implicitly, and all remote
//! authority remains host-selected.

mod cache;
mod client;
mod completion;
mod content;
mod error;
mod http_auth;
mod http_client;
mod http_headers;
mod http_server;
mod mrtr;
mod pagination;
mod prompt;
mod protocol;
mod remote_tool;
mod resource;
mod sampling;
mod sampling_client;
mod sampling_model;
mod sampling_validation;
mod server;
mod server_response;
mod stdio;
mod subscription;
mod task_client;
mod tasks;
mod transport;
#[cfg(feature = "workflow-tasks")]
mod workflow_tasks;

pub use cache::{
    CacheHint, CacheMode, CacheOperation, CacheScope, CachedResponse, InMemoryResponseCache,
    ResponseCacheStore,
};
pub use client::{McpClient, McpClientConfig, McpProtocolMode};
pub use completion::{
    CompletionDescriptor, CompletionError, CompletionErrorKind, CompletionFuture,
    CompletionProvider, CompletionRegistrationError, CompletionRegistry, FunctionCompletion,
};
pub use content::{
    Annotations, AudienceRole, CompleteParams, CompleteResult, Completion, CompletionArgument,
    CompletionContext, CompletionReference, GetPromptParams, GetPromptResult, Icon,
    ListPromptsParams, ListPromptsResult, ListResourceTemplatesParams, ListResourceTemplatesResult,
    ListResourcesParams, ListResourcesResult, McpPrompt, McpResource, McpResourceTemplate,
    PromptArgument, PromptMessage, PromptRole, ReadResourceParams, ReadResourceResult,
    ResourceContents, ResourceSubscriptionParams,
};
pub use error::{McpError, McpErrorKind};
pub use http_auth::{HttpAuthProvider, HttpAuthorizer, StaticBearerAuth};
pub use http_client::StreamableHttpTransport;
pub use http_server::{
    HttpResponseMode, MCP_METHOD_HEADER, MCP_NAME_HEADER, MCP_PROTOCOL_VERSION_HEADER,
    MCP_SESSION_ID_HEADER, McpHttpServer, McpHttpServerConfig,
};
pub use mrtr::{
    InputRequest, InputRequiredResult, InputResponseFuture, MrtrInputHandler, MrtrToolDecision,
    MrtrToolFuture, MrtrToolGate, MrtrToolRequest,
};
pub use prompt::{
    FunctionPrompt, PromptDescriptor, PromptError, PromptErrorKind, PromptFuture, PromptHandler,
    PromptRegistrationError, PromptRegistry,
};
pub use protocol::{
    CallToolParams, CallToolResult, ClientCapabilities, ClientTaskRequestsCapability,
    ClientTaskSamplingRequestsCapability, ClientTasksCapability, ContentBlock, DiscoverMetadata,
    DiscoverParams, DiscoverResult, DiscoveryCacheScope, Implementation, InitializeParams,
    InitializeResult, JsonRpcError, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse,
    LATEST_PROTOCOL_VERSION, ListToolsParams, ListToolsResult, McpResultType, McpTool,
    PromptsCapability, RequestId, ResourcesCapability, STATELESS_PROTOCOL_VERSION,
    SUPPORTED_PROTOCOL_VERSIONS, SamplingCapability, ServerCapabilities, StatelessRequestMetadata,
    ToolsCapability,
};
pub use remote_tool::{McpRemoteTool, RemoteToolPolicy};
pub use resource::{
    ResourceDescriptor, ResourceError, ResourceErrorKind, ResourceFuture, ResourceHandler,
    ResourceRegistrationError, ResourceRegistry, ResourceTemplateDescriptor,
    ResourceTemplateHandler, StaticTextResource,
};
pub use sampling::{
    CreateMessageParams, CreateMessageResult, IncludeContext, ModelHint, ModelPreferences,
    SamplingApprover, SamplingCallContext, SamplingContent, SamplingContextProvider,
    SamplingDecision, SamplingError, SamplingErrorKind, SamplingFuture, SamplingMessage,
    SamplingPolicy, SamplingProvider, SamplingRole, SamplingService, SamplingStage,
    SamplingToolChoice, SamplingToolChoiceMode,
};
pub use sampling_client::McpSamplingClient;
pub use sampling_model::{
    FixedSamplingModel, ModelSamplingProvider, SamplingModelFeature, SamplingModelRequirements,
    SamplingModelSelector,
};
pub use server::{McpServer, McpSession};
pub use stdio::{StdioTransport, serve_io, serve_stdio};
pub use subscription::{McpSubscription, SubscriptionFilter};
pub use task_client::McpTaskSubscription;
pub(crate) use tasks::CoreTaskWire;
pub use tasks::{
    CallToolOutcome, CreateMessageOutcome, CreateTaskResult, GetTaskResult, McpSamplingTaskBackend,
    McpTask, McpTaskBackend, McpTaskBackendError, McpTaskBackendErrorKind, McpTaskFuture,
    McpTaskTimeError, SAMPLING_TASK_IDEMPOTENCY_KEY, SamplingTaskApprovalClaim,
    SamplingTaskCreation, SamplingTaskOutput, SamplingTaskRequest, SamplingTaskResult,
    SamplingTaskTerminalResult, TASKS_EXTENSION_ID, TaskIdParams, TaskMetadata, TaskStatus,
    ToolTaskRequest, UpdateTaskParams,
};
pub use transport::{
    McpTransport, PeerRequestHandler, ServerNotificationStream, StatelessCancellation,
    TransportFuture,
};
#[cfg(feature = "workflow-tasks")]
pub use workflow_tasks::{
    DefaultWorkflowTaskResultMapper, SamplingTaskIdempotencyNamespace, WorkflowSamplingTaskResult,
    WorkflowSamplingTaskRoute, WorkflowTaskAdapter, WorkflowTaskResultMapper, WorkflowTaskRoute,
};
