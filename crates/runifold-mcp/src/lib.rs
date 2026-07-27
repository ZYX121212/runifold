//! Capability-safe Model Context Protocol support for Runifold.
//!
//! The crate provides canonical Tools, Resources, Prompts, and client-owned
//! Sampling over in-process, newline-delimited stdio, and MCP Streamable HTTP
//! transports. HTTP requests are never retried implicitly, and all remote
//! authority remains host-selected.

mod client;
mod completion;
mod content;
mod error;
mod http_auth;
mod http_client;
mod http_server;
mod pagination;
mod prompt;
mod protocol;
mod remote_tool;
mod resource;
mod sampling;
mod sampling_client;
mod sampling_model;
mod server;
mod server_response;
mod stdio;
mod transport;

pub use client::{McpClient, McpClientConfig};
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
    HttpResponseMode, MCP_PROTOCOL_VERSION_HEADER, MCP_SESSION_ID_HEADER, McpHttpServer,
    McpHttpServerConfig,
};
pub use prompt::{
    FunctionPrompt, PromptDescriptor, PromptError, PromptErrorKind, PromptFuture, PromptHandler,
    PromptRegistrationError, PromptRegistry,
};
pub use protocol::{
    CallToolParams, CallToolResult, ClientCapabilities, ContentBlock, Implementation,
    InitializeParams, InitializeResult, JsonRpcError, JsonRpcNotification, JsonRpcRequest,
    JsonRpcResponse, LATEST_PROTOCOL_VERSION, ListToolsParams, ListToolsResult, McpTool,
    PromptsCapability, RequestId, ResourcesCapability, SamplingCapability, ServerCapabilities,
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
    SamplingApprover, SamplingCallContext, SamplingContent, SamplingDecision, SamplingError,
    SamplingErrorKind, SamplingFuture, SamplingMessage, SamplingPolicy, SamplingProvider,
    SamplingRole, SamplingService, SamplingStage, SamplingToolChoice, SamplingToolChoiceMode,
};
pub use sampling_client::McpSamplingClient;
pub use sampling_model::{FixedSamplingModel, ModelSamplingProvider, SamplingModelSelector};
pub use server::{McpServer, McpSession};
pub use stdio::{StdioTransport, serve_io, serve_stdio};
pub use transport::{McpTransport, PeerRequestHandler, ServerNotificationStream, TransportFuture};
