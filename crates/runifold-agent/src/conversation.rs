//! Multi-turn conversation, summary, and semantic-memory boundaries.

mod store;
mod summarizer;

pub use store::{
    AgentConversationError, AgentConversationOutcome, ConversationAppend,
    ConversationContextPolicy, ConversationCreateOutcome, ConversationId, ConversationSequence,
    ConversationStore, ConversationStoreError, ConversationStoreErrorKind, ConversationStoreFuture,
    ConversationSummary, ConversationSummaryBatch, ConversationSummaryCommit,
    ConversationTranscriptEntry, ConversationVersion, ConversationView, ConversationWindow,
    InMemoryConversationStore, MemoryNamespace, SemanticMemory, SemanticMemoryId,
    SemanticMemoryQuery, SemanticMemorySearchOutcome, SemanticMemorySource, SemanticMemoryUpsert,
    SemanticMemoryUpsertOutcome,
};
pub(crate) use store::{
    TRANSIENT_CONTEXT_METADATA, is_transient_context, semantic_memory_message, summary_message,
};
pub use summarizer::{
    AutomaticConversationSummary, ConversationSummarizer, ConversationSummarizerError,
    ConversationSummarizerFuture, ConversationSummaryPassLimit, ConversationSummaryRequest,
};
