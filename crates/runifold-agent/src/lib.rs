//! Structured model-tool agent runtime for Runifold.

mod agent;
mod builder;
mod checkpoint;
mod completion;
mod conversation;
mod descriptor;
mod error;
mod gateway;
mod middleware;
mod outcome;
mod stream;
mod structured;
mod terminal_review;

pub use agent::{Agent, AgentConfig, AgentFuture, ToolErrorPolicy};
pub use builder::{AgentBuildError, AgentBuilder, AgentPromptError};
pub use checkpoint::{
    AgentCheckpoint, AgentCheckpointPhase, AgentCheckpointState, DurableConversationCheckpoint,
    ResumePolicy,
};
pub use completion::{
    CompletionRequirement, TerminalRequirementFailure, TerminalRequirementFailureKind,
};
pub use conversation::{
    AgentConversationError, AgentConversationOutcome, AutomaticConversationSummary,
    ConversationAppend, ConversationContextPolicy, ConversationCreateOutcome, ConversationId,
    ConversationSequence, ConversationStore, ConversationStoreError, ConversationStoreErrorKind,
    ConversationStoreFuture, ConversationSummarizer, ConversationSummarizerError,
    ConversationSummarizerFuture, ConversationSummary, ConversationSummaryBatch,
    ConversationSummaryCommit, ConversationSummaryPassLimit, ConversationSummaryRequest,
    ConversationTranscriptEntry, ConversationVersion, ConversationView, ConversationWindow,
    DurableConversationCommit, DurableConversationRequest, DurableConversationStore,
    InMemoryConversationStore, MemoryNamespace, SemanticMemory, SemanticMemoryId,
    SemanticMemoryQuery, SemanticMemorySearchOutcome, SemanticMemorySource, SemanticMemoryUpsert,
    SemanticMemoryUpsertOutcome,
};
pub use descriptor::AgentDescriptor;
pub use error::AgentError;
pub use gateway::{
    AgentGateway, AgentRegistrationError, AgentRoute, GatewayError, GatewayErrorKind,
};
pub use middleware::{
    DelegationRequest, GatewayDecision, GatewayFuture, GatewayMiddleware, GatewayNext,
    GatewayPolicy, PolicyMiddleware,
};
pub use outcome::{AgentOutcome, StructuredAgentOutcome};
pub use stream::{AgentEventStream, AgentStreamEvent, CallableKind};
pub use structured::{StructuredAgent, StructuredAgentError};
pub use terminal_review::{
    CompositeTerminalReviewMode, CompositeTerminalReviewer, TerminalReviewError,
    TerminalReviewFuture, TerminalReviewPolicy, TerminalReviewRequest, TerminalReviewVerdict,
    TerminalReviewVerdictKind, TerminalReviewer, TerminalReviewerDescriptor, TerminalRuleReviewer,
    TurnReviewError, TurnReviewFuture, TurnReviewPolicy, TurnReviewRequest, TurnReviewScope,
    TurnReviewVerdict, TurnReviewer, TurnReviewerDescriptor, TurnRuleReviewer,
};
