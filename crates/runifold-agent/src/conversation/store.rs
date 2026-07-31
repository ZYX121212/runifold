//! Conversation persistence domain and in-memory reference adapter.

mod support;

use support::{
    conversation_not_found, namespace_mismatch, normalized_terms, now_ms, require_namespace,
    retrieval_store_error, validate_memory, validate_sources, validate_summary,
    validate_transcript_messages,
};
pub(crate) use support::{is_transient_context, semantic_memory_message, summary_message};

use std::{
    collections::BTreeMap,
    future::Future,
    num::{NonZeroU16, NonZeroU64},
    pin::Pin,
    sync::{Arc, Mutex},
};

use runifold_core::{CheckpointId, Usage};
use runifold_model::Message;
use runifold_retrieval::RetrievalContext;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::{AgentError, AgentOutcome};

const MAX_NAMESPACE_BYTES: usize = 128;
const MAX_SUMMARY_BYTES: usize = 262_144;
const MAX_MEMORY_BYTES: usize = 262_144;
pub(crate) const TRANSIENT_CONTEXT_METADATA: &str = "runifold.context.transient";

/// A boxed asynchronous conversation-store operation.
#[cfg(not(target_arch = "wasm32"))]
pub type ConversationStoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A boxed conversation-store operation on single-threaded WASM.
#[cfg(target_arch = "wasm32")]
pub type ConversationStoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Stable identity of one multi-turn conversation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ConversationId(CheckpointId);

impl ConversationId {
    /// Generates a time-ordered conversation identity.
    pub fn new() -> Self {
        Self(CheckpointId::new())
    }

    /// Reconstructs a conversation identity from durable storage.
    pub const fn from_checkpoint_id(id: CheckpointId) -> Self {
        Self(id)
    }

    /// Returns the UUID-backed durable identity.
    pub const fn as_checkpoint_id(self) -> CheckpointId {
        self.0
    }
}

impl Default for ConversationId {
    fn default() -> Self {
        Self::new()
    }
}

/// Isolation namespace shared by related conversations and semantic memories.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct MemoryNamespace(String);

impl MemoryNamespace {
    /// Validates a portable memory namespace.
    ///
    /// # Errors
    ///
    /// Rejects blank, oversized, or non-portable values.
    pub fn parse(value: impl Into<String>) -> Result<Self, ConversationStoreError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_NAMESPACE_BYTES
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(ConversationStoreError::invalid_input(
                "memory namespace must contain 1..=128 portable ASCII characters",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the validated namespace.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Monotonic append version of one conversation transcript.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ConversationVersion(u64);

impl ConversationVersion {
    /// Creates a version from durable storage.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric version.
    pub const fn get(self) -> u64 {
        self.0
    }

    fn next(self) -> Result<Self, ConversationStoreError> {
        self.0.checked_add(1).map(Self).ok_or_else(|| {
            ConversationStoreError::new(
                ConversationStoreErrorKind::Conflict,
                "conversation version overflow",
            )
        })
    }
}

/// One-based stable position in an append-only transcript.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ConversationSequence(NonZeroU64);

impl ConversationSequence {
    /// Creates a transcript sequence.
    ///
    /// # Errors
    ///
    /// Rejects zero.
    pub fn new(value: u64) -> Result<Self, ConversationStoreError> {
        NonZeroU64::new(value).map(Self).ok_or_else(|| {
            ConversationStoreError::invalid_input("conversation sequence must be positive")
        })
    }

    /// Returns the one-based numeric position.
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Canonical model message stored in the append-only transcript.
///
/// This is conversation data, not an execution-journal event and not semantic
/// memory. System instructions are deliberately rejected from this boundary.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ConversationTranscriptEntry {
    /// Stable position in the conversation.
    pub sequence: ConversationSequence,
    /// Original canonical model message.
    pub message: Message,
}

/// Lossy compression of a prefix of the immutable transcript.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConversationSummary {
    /// Stable summary identity.
    pub summary_id: CheckpointId,
    /// Human- or model-produced summary text.
    pub content: String,
    /// Last transcript entry represented by this summary.
    pub through_sequence: ConversationSequence,
    /// Transcript version observed when the summary was committed.
    pub transcript_version: ConversationVersion,
    /// Store-authoritative creation time.
    pub created_at_ms: u64,
}

/// Bounded context view derived without mutating the transcript.
#[derive(Clone, Debug, PartialEq)]
pub struct ConversationView {
    /// Conversation identity.
    pub conversation_id: ConversationId,
    /// Namespace used for cross-conversation semantic memory.
    pub namespace: MemoryNamespace,
    /// Current transcript append version.
    pub version: ConversationVersion,
    /// Latest monotonic summary, when one exists.
    pub summary: Option<ConversationSummary>,
    /// Older unsummarized entries excluded from the live window.
    pub summary_buffer: Vec<ConversationTranscriptEntry>,
    /// Unsummarized entries still waiting behind the returned summary batch.
    pub summary_backlog: u64,
    /// Most recent model-visible transcript suffix.
    pub window: Vec<ConversationTranscriptEntry>,
}

impl ConversationView {
    /// Returns whether another summary must be committed before bounded Agent execution.
    pub fn requires_summary(&self) -> bool {
        !self.summary_buffer.is_empty()
    }
}

/// Maximum number of recent transcript entries exposed as the live window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConversationWindow(NonZeroU16);

impl ConversationWindow {
    /// Creates a bounded live-message window.
    ///
    /// # Errors
    ///
    /// Rejects zero and values above 4096.
    pub fn new(value: u16) -> Result<Self, ConversationStoreError> {
        NonZeroU16::new(value)
            .filter(|value| value.get() <= 4_096)
            .map(Self)
            .ok_or_else(|| {
                ConversationStoreError::invalid_input("conversation window must be in 1..=4096")
            })
    }

    /// Returns the validated entry limit.
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

/// Maximum older transcript entries returned for one summary operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConversationSummaryBatch(NonZeroU16);

impl ConversationSummaryBatch {
    /// Creates a bounded summary batch.
    ///
    /// # Errors
    ///
    /// Rejects zero and values above 4096.
    pub fn new(value: u16) -> Result<Self, ConversationStoreError> {
        NonZeroU16::new(value)
            .filter(|value| value.get() <= 4_096)
            .map(Self)
            .ok_or_else(|| {
                ConversationStoreError::invalid_input(
                    "conversation summary batch must be in 1..=4096",
                )
            })
    }

    /// Returns the validated entry limit.
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

/// Bounded conversation and cross-session memory context policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConversationContextPolicy {
    /// Recent transcript suffix.
    pub window: ConversationWindow,
    /// Maximum older entries loaded for one summary operation.
    pub summary_batch: ConversationSummaryBatch,
    /// Optional maximum semantic memories retrieved from the namespace.
    pub semantic_memory_limit: Option<NonZeroU16>,
}

impl ConversationContextPolicy {
    /// Creates a transcript-only context policy.
    pub const fn new(window: ConversationWindow) -> Self {
        Self {
            window,
            summary_batch: ConversationSummaryBatch(window.0),
            semantic_memory_limit: None,
        }
    }

    /// Replaces the maximum number of entries loaded for one summary pass.
    #[must_use]
    pub const fn with_summary_batch(mut self, summary_batch: ConversationSummaryBatch) -> Self {
        self.summary_batch = summary_batch;
        self
    }

    /// Enables bounded cross-conversation semantic-memory lookup.
    ///
    /// # Errors
    ///
    /// Rejects zero and values above 256.
    pub fn with_semantic_memory(mut self, limit: u16) -> Result<Self, ConversationStoreError> {
        self.semantic_memory_limit = NonZeroU16::new(limit).filter(|value| value.get() <= 256);
        if self.semantic_memory_limit.is_none() {
            return Err(ConversationStoreError::invalid_input(
                "semantic memory context limit must be in 1..=256",
            ));
        }
        Ok(self)
    }
}

/// Idempotent transcript append with optimistic concurrency control.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ConversationAppend {
    /// Target conversation.
    pub conversation_id: ConversationId,
    /// Version loaded before model execution.
    pub expected_version: ConversationVersion,
    /// Canonical user, assistant, and tool messages to append atomically.
    pub messages: Vec<Message>,
}

/// Request to replace the current summary with a strictly newer prefix.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConversationSummaryCommit {
    /// Target conversation.
    pub conversation_id: ConversationId,
    /// Transcript version used to produce the summary.
    pub expected_version: ConversationVersion,
    /// Last transcript entry represented by the summary.
    pub through_sequence: ConversationSequence,
    /// Replacement summary content.
    pub content: String,
}

/// Stable identity of one explicitly curated semantic memory.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct SemanticMemoryId(CheckpointId);

impl SemanticMemoryId {
    /// Generates a time-ordered semantic-memory identity.
    pub fn new() -> Self {
        Self(CheckpointId::new())
    }

    /// Reconstructs a semantic-memory identity from durable storage.
    pub const fn from_checkpoint_id(id: CheckpointId) -> Self {
        Self(id)
    }

    /// Returns the UUID-backed durable identity.
    pub const fn as_checkpoint_id(self) -> CheckpointId {
        self.0
    }
}

impl Default for SemanticMemoryId {
    fn default() -> Self {
        Self::new()
    }
}

/// Provenance link from semantic memory back to immutable conversation data.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SemanticMemorySource {
    /// Source conversation.
    pub conversation_id: ConversationId,
    /// First supporting transcript entry.
    pub from_sequence: ConversationSequence,
    /// Last supporting transcript entry.
    pub through_sequence: ConversationSequence,
}

/// Cross-conversation semantic fact, preference, or durable user knowledge.
///
/// Semantic memory is never inferred merely by appending transcript messages.
/// Applications must explicitly curate and upsert it with provenance.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SemanticMemory {
    /// Stable memory identity.
    pub memory_id: SemanticMemoryId,
    /// Isolation namespace.
    pub namespace: MemoryNamespace,
    /// Searchable semantic content.
    pub content: String,
    /// Immutable transcript provenance.
    pub sources: Vec<SemanticMemorySource>,
    /// Application-owned structured metadata.
    pub metadata: BTreeMap<String, Value>,
    /// Monotonic update revision.
    pub revision: u64,
    /// Store-authoritative creation time.
    pub created_at_ms: u64,
    /// Store-authoritative last-update time.
    pub updated_at_ms: u64,
}

/// Create-or-CAS command for one semantic memory.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SemanticMemoryUpsert {
    /// Stable memory identity.
    pub memory_id: SemanticMemoryId,
    /// Isolation namespace.
    pub namespace: MemoryNamespace,
    /// Searchable semantic content.
    pub content: String,
    /// Immutable transcript provenance.
    pub sources: Vec<SemanticMemorySource>,
    /// Application metadata.
    pub metadata: BTreeMap<String, Value>,
    /// `None` creates; `Some` requires an exact current revision.
    pub expected_revision: Option<u64>,
}

/// Semantic-memory write plus attributable embedding/storage usage.
#[derive(Clone, Debug, PartialEq)]
pub struct SemanticMemoryUpsertOutcome {
    /// Persisted semantic memory.
    pub memory: SemanticMemory,
    /// Embedding and storage usage attributable to this operation.
    pub usage: Usage,
}

/// Validated semantic-memory lookup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SemanticMemoryQuery {
    /// Isolation namespace.
    pub namespace: MemoryNamespace,
    /// Natural-language lookup text.
    pub text: String,
    /// Maximum returned memories.
    pub limit: NonZeroU16,
}

/// Semantic-memory search results plus attributable embedding/storage usage.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SemanticMemorySearchOutcome {
    /// Memories ordered by descending relevance.
    pub memories: Vec<SemanticMemory>,
    /// Query-embedding and storage usage attributable to this operation.
    pub usage: Usage,
}

impl SemanticMemoryQuery {
    /// Creates a bounded semantic-memory query.
    ///
    /// # Errors
    ///
    /// Rejects blank queries and limits outside 1..=256.
    pub fn new(
        namespace: MemoryNamespace,
        text: impl Into<String>,
        limit: u16,
    ) -> Result<Self, ConversationStoreError> {
        let text = text.into();
        let Some(limit) = NonZeroU16::new(limit).filter(|value| value.get() <= 256) else {
            return Err(ConversationStoreError::invalid_input(
                "semantic memory query requires text and a limit in 1..=256",
            ));
        };
        if text.trim().is_empty() {
            return Err(ConversationStoreError::invalid_input(
                "semantic memory query requires text and a limit in 1..=256",
            ));
        }
        Ok(Self {
            namespace,
            text,
            limit,
        })
    }
}

/// Result of creating an idempotent conversation identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConversationCreateOutcome {
    /// A new empty conversation was created.
    Created,
    /// The identity already exists in the same namespace.
    Duplicate,
}

/// Successful Agent turn committed to one conversation.
#[derive(Clone, Debug, PartialEq)]
pub struct AgentConversationOutcome {
    /// Canonical Agent execution outcome.
    pub outcome: AgentOutcome,
    /// Transcript version after the atomic append.
    pub conversation_version: ConversationVersion,
}

/// Failure while loading, running, or committing one conversational Agent turn.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AgentConversationError {
    /// Conversation or semantic-memory loading failed before execution.
    #[error("conversation store failed: {0}")]
    Store(#[from] ConversationStoreError),
    /// Bounded execution requires the older unsummarized prefix to be summarized.
    #[error(
        "conversation `{conversation_id:?}` requires summarization of {buffered_entries} buffered entries"
    )]
    SummaryRequired {
        /// Conversation requiring compaction.
        conversation_id: ConversationId,
        /// Unsummarized entries outside the configured live window.
        buffered_entries: u64,
    },
    /// Automatic compaction reached its explicit pass limit with work remaining.
    #[error(
        "conversation `{conversation_id:?}` still has {remaining_entries} entries requiring summarization after the configured pass limit"
    )]
    SummaryPassLimitExceeded {
        /// Conversation whose backlog remains.
        conversation_id: ConversationId,
        /// Older unsummarized entries still excluded from the live window.
        remaining_entries: u64,
    },
    /// Automatic summary generation failed before any transcript mutation.
    #[error("conversation summarization failed: {0}")]
    Summarization(#[from] super::ConversationSummarizerError),
    /// Canonical Agent execution failed; no transcript append was attempted.
    #[error("conversational Agent execution failed: {0}")]
    Run(#[source] AgentError),
    /// Model execution succeeded but optimistic transcript commit conflicted.
    #[error("Agent completed but conversation commit failed: {source}")]
    Commit {
        /// Store failure, normally a concurrent-version conflict.
        #[source]
        source: ConversationStoreError,
        /// Preserved model outcome so successful work is never discarded.
        outcome: Box<AgentOutcome>,
    },
}

/// Stable conversation-store failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConversationStoreErrorKind {
    /// Input violated a domain invariant.
    InvalidInput,
    /// The requested resource does not exist.
    NotFound,
    /// A version or create-only precondition failed.
    Conflict,
    /// A resource belongs to another memory namespace.
    NamespaceMismatch,
    /// The backing store failed.
    Storage,
}

/// Typed conversation and semantic-memory persistence failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("{kind:?}: {message}")]
pub struct ConversationStoreError {
    /// Stable failure category.
    pub kind: ConversationStoreErrorKind,
    /// Safe application-facing explanation.
    pub message: String,
}

impl ConversationStoreError {
    /// Creates a normalized store error.
    pub fn new(kind: ConversationStoreErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(ConversationStoreErrorKind::InvalidInput, message)
    }
}

/// Persistence boundary for conversation transcript, summaries, and semantic memory.
///
/// Execution-journal events deliberately do not appear in this trait. They
/// remain owned by [`runifold_core::Journal`].
pub trait ConversationStore: Send + Sync {
    /// Idempotently creates one empty conversation.
    fn create(
        &self,
        conversation_id: ConversationId,
        namespace: MemoryNamespace,
    ) -> ConversationStoreFuture<'_, Result<ConversationCreateOutcome, ConversationStoreError>>;

    /// Loads a bounded view without deleting or rewriting transcript entries.
    fn load_view(
        &self,
        conversation_id: ConversationId,
        namespace: MemoryNamespace,
        window: ConversationWindow,
        summary_batch: ConversationSummaryBatch,
    ) -> ConversationStoreFuture<'_, Result<ConversationView, ConversationStoreError>>;

    /// Lists immutable transcript entries strictly after an optional sequence.
    fn list_transcript(
        &self,
        conversation_id: ConversationId,
        namespace: MemoryNamespace,
        after: Option<ConversationSequence>,
        limit: ConversationWindow,
    ) -> ConversationStoreFuture<'_, Result<Vec<ConversationTranscriptEntry>, ConversationStoreError>>;

    /// Atomically appends canonical messages under the expected version.
    fn append(
        &self,
        namespace: MemoryNamespace,
        command: ConversationAppend,
    ) -> ConversationStoreFuture<'_, Result<ConversationVersion, ConversationStoreError>>;

    /// Monotonically replaces the lossy summary under the expected transcript version.
    fn commit_summary(
        &self,
        namespace: MemoryNamespace,
        command: ConversationSummaryCommit,
    ) -> ConversationStoreFuture<'_, Result<ConversationSummary, ConversationStoreError>>;

    /// Creates or compare-and-swaps one explicitly curated semantic memory.
    fn upsert_memory(
        &self,
        command: SemanticMemoryUpsert,
    ) -> ConversationStoreFuture<'_, Result<SemanticMemory, ConversationStoreError>>;

    /// Writes memory under an explicit cancellation/deadline scope.
    ///
    /// Stores with embedding support should override this method and report
    /// provider and storage usage. The default preserves lexical-store behavior.
    fn upsert_memory_scoped(
        &self,
        command: SemanticMemoryUpsert,
        context: RetrievalContext,
    ) -> ConversationStoreFuture<'_, Result<SemanticMemoryUpsertOutcome, ConversationStoreError>>
    {
        Box::pin(async move {
            context
                .check_live()
                .map_err(|error| retrieval_store_error(&error))?;
            let memory = self.upsert_memory(command).await?;
            Ok(SemanticMemoryUpsertOutcome {
                memory,
                usage: Usage::default(),
            })
        })
    }

    /// Searches semantic memory without reading transcript or journal storage.
    fn search_memory(
        &self,
        query: SemanticMemoryQuery,
    ) -> ConversationStoreFuture<'_, Result<Vec<SemanticMemory>, ConversationStoreError>>;

    /// Searches memory under an explicit cancellation/deadline scope.
    ///
    /// Stores with embedding support should override this method and report
    /// query-embedding and storage usage.
    fn search_memory_scoped(
        &self,
        query: SemanticMemoryQuery,
        context: RetrievalContext,
    ) -> ConversationStoreFuture<'_, Result<SemanticMemorySearchOutcome, ConversationStoreError>>
    {
        Box::pin(async move {
            context
                .check_live()
                .map_err(|error| retrieval_store_error(&error))?;
            let memories = self.search_memory(query).await?;
            Ok(SemanticMemorySearchOutcome {
                memories,
                usage: Usage::default(),
            })
        })
    }
}

/// Deterministic in-memory reference store for tests and ephemeral applications.
#[derive(Clone, Debug, Default)]
pub struct InMemoryConversationStore {
    state: Arc<Mutex<ConversationState>>,
}

#[derive(Debug, Default)]
struct ConversationState {
    conversations: BTreeMap<ConversationId, StoredConversation>,
    memories: BTreeMap<SemanticMemoryId, SemanticMemory>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredConversation {
    namespace: MemoryNamespace,
    version: ConversationVersion,
    transcript: Vec<ConversationTranscriptEntry>,
    summary: Option<ConversationSummary>,
}

const PERSISTENT_SNAPSHOT_VERSION: u32 = 1;

#[derive(Deserialize, Serialize)]
struct PersistentConversationSnapshot {
    version: u32,
    conversations: Vec<(ConversationId, StoredConversation)>,
    memories: Vec<(SemanticMemoryId, SemanticMemory)>,
}

impl InMemoryConversationStore {
    /// Creates an empty ephemeral store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Encodes the complete reference state for a durable adapter.
    #[doc(hidden)]
    pub fn export_persistent_snapshot(&self) -> Result<Vec<u8>, ConversationStoreError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let snapshot = PersistentConversationSnapshot {
            version: PERSISTENT_SNAPSHOT_VERSION,
            conversations: state
                .conversations
                .iter()
                .map(|(id, conversation)| (*id, conversation.clone()))
                .collect(),
            memories: state
                .memories
                .iter()
                .map(|(id, memory)| (*id, memory.clone()))
                .collect(),
        };
        serde_json::to_vec(&snapshot).map_err(|error| {
            ConversationStoreError::new(
                ConversationStoreErrorKind::Storage,
                format!("conversation snapshot encoding failed: {error}"),
            )
        })
    }

    /// Restores the complete reference state for a durable adapter.
    #[doc(hidden)]
    pub fn from_persistent_snapshot(encoded: &[u8]) -> Result<Self, ConversationStoreError> {
        let snapshot: PersistentConversationSnapshot =
            serde_json::from_slice(encoded).map_err(|error| {
                ConversationStoreError::new(
                    ConversationStoreErrorKind::Storage,
                    format!("conversation snapshot decoding failed: {error}"),
                )
            })?;
        if snapshot.version != PERSISTENT_SNAPSHOT_VERSION {
            return Err(ConversationStoreError::new(
                ConversationStoreErrorKind::Storage,
                format!(
                    "unsupported conversation snapshot version {}",
                    snapshot.version
                ),
            ));
        }
        Ok(Self {
            state: Arc::new(Mutex::new(ConversationState {
                conversations: snapshot.conversations.into_iter().collect(),
                memories: snapshot.memories.into_iter().collect(),
            })),
        })
    }
}

impl ConversationStore for InMemoryConversationStore {
    fn create(
        &self,
        conversation_id: ConversationId,
        namespace: MemoryNamespace,
    ) -> ConversationStoreFuture<'_, Result<ConversationCreateOutcome, ConversationStoreError>>
    {
        Box::pin(async move {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(existing) = state.conversations.get(&conversation_id) {
                return if existing.namespace == namespace {
                    Ok(ConversationCreateOutcome::Duplicate)
                } else {
                    Err(namespace_mismatch())
                };
            }
            state.conversations.insert(
                conversation_id,
                StoredConversation {
                    namespace,
                    version: ConversationVersion::default(),
                    transcript: Vec::new(),
                    summary: None,
                },
            );
            Ok(ConversationCreateOutcome::Created)
        })
    }

    fn load_view(
        &self,
        conversation_id: ConversationId,
        namespace: MemoryNamespace,
        window: ConversationWindow,
        summary_batch: ConversationSummaryBatch,
    ) -> ConversationStoreFuture<'_, Result<ConversationView, ConversationStoreError>> {
        Box::pin(async move {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let stored = state
                .conversations
                .get(&conversation_id)
                .ok_or_else(conversation_not_found)?;
            require_namespace(&stored.namespace, &namespace)?;
            let summarized_through = stored
                .summary
                .as_ref()
                .map_or(0, |summary| summary.through_sequence.get());
            let unsummarized = stored
                .transcript
                .iter()
                .filter(|entry| entry.sequence.get() > summarized_through)
                .cloned()
                .collect::<Vec<_>>();
            let window_start = unsummarized.len().saturating_sub(usize::from(window.get()));
            let summary_end = window_start.min(usize::from(summary_batch.get()));
            Ok(ConversationView {
                conversation_id,
                namespace,
                version: stored.version,
                summary: stored.summary.clone(),
                summary_buffer: unsummarized[..summary_end].to_vec(),
                summary_backlog: u64::try_from(window_start.saturating_sub(summary_end))
                    .unwrap_or(u64::MAX),
                window: unsummarized[window_start..].to_vec(),
            })
        })
    }

    fn append(
        &self,
        namespace: MemoryNamespace,
        command: ConversationAppend,
    ) -> ConversationStoreFuture<'_, Result<ConversationVersion, ConversationStoreError>> {
        Box::pin(async move {
            validate_transcript_messages(&command.messages)?;
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let stored = state
                .conversations
                .get_mut(&command.conversation_id)
                .ok_or_else(conversation_not_found)?;
            require_namespace(&stored.namespace, &namespace)?;
            if stored.version != command.expected_version {
                return Err(ConversationStoreError::new(
                    ConversationStoreErrorKind::Conflict,
                    "conversation transcript version precondition failed",
                ));
            }
            let next_version = stored.version.next()?;
            for message in command.messages {
                let sequence = u64::try_from(stored.transcript.len())
                    .ok()
                    .and_then(|value| value.checked_add(1))
                    .and_then(NonZeroU64::new)
                    .map(ConversationSequence)
                    .ok_or_else(|| {
                        ConversationStoreError::new(
                            ConversationStoreErrorKind::Conflict,
                            "conversation transcript sequence overflow",
                        )
                    })?;
                stored
                    .transcript
                    .push(ConversationTranscriptEntry { sequence, message });
            }
            stored.version = next_version;
            Ok(next_version)
        })
    }

    fn list_transcript(
        &self,
        conversation_id: ConversationId,
        namespace: MemoryNamespace,
        after: Option<ConversationSequence>,
        limit: ConversationWindow,
    ) -> ConversationStoreFuture<'_, Result<Vec<ConversationTranscriptEntry>, ConversationStoreError>>
    {
        Box::pin(async move {
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let stored = state
                .conversations
                .get(&conversation_id)
                .ok_or_else(conversation_not_found)?;
            require_namespace(&stored.namespace, &namespace)?;
            let after = after.map_or(0, ConversationSequence::get);
            Ok(stored
                .transcript
                .iter()
                .filter(|entry| entry.sequence.get() > after)
                .take(usize::from(limit.get()))
                .cloned()
                .collect())
        })
    }

    fn commit_summary(
        &self,
        namespace: MemoryNamespace,
        command: ConversationSummaryCommit,
    ) -> ConversationStoreFuture<'_, Result<ConversationSummary, ConversationStoreError>> {
        Box::pin(async move {
            validate_summary(&command.content)?;
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let stored = state
                .conversations
                .get_mut(&command.conversation_id)
                .ok_or_else(conversation_not_found)?;
            require_namespace(&stored.namespace, &namespace)?;
            if stored.version != command.expected_version {
                return Err(ConversationStoreError::new(
                    ConversationStoreErrorKind::Conflict,
                    "conversation summary version precondition failed",
                ));
            }
            let last_sequence = u64::try_from(stored.transcript.len()).unwrap_or(u64::MAX);
            let previous = stored
                .summary
                .as_ref()
                .map_or(0, |summary| summary.through_sequence.get());
            if command.through_sequence.get() <= previous
                || command.through_sequence.get() > last_sequence
            {
                return Err(ConversationStoreError::invalid_input(
                    "conversation summary must cover a newer existing transcript prefix",
                ));
            }
            let summary = ConversationSummary {
                summary_id: CheckpointId::new(),
                content: command.content,
                through_sequence: command.through_sequence,
                transcript_version: stored.version,
                created_at_ms: now_ms(),
            };
            stored.summary = Some(summary.clone());
            Ok(summary)
        })
    }

    fn upsert_memory(
        &self,
        command: SemanticMemoryUpsert,
    ) -> ConversationStoreFuture<'_, Result<SemanticMemory, ConversationStoreError>> {
        Box::pin(async move {
            validate_memory(&command)?;
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let current = state.memories.get(&command.memory_id);
            let revision = match (current, command.expected_revision) {
                (None, None) => 0,
                (Some(current), Some(expected))
                    if current.revision == expected && current.namespace == command.namespace =>
                {
                    expected.checked_add(1).ok_or_else(|| {
                        ConversationStoreError::new(
                            ConversationStoreErrorKind::Conflict,
                            "semantic memory revision overflow",
                        )
                    })?
                }
                (Some(current), _) if current.namespace != command.namespace => {
                    return Err(namespace_mismatch());
                }
                _ => {
                    return Err(ConversationStoreError::new(
                        ConversationStoreErrorKind::Conflict,
                        "semantic memory revision precondition failed",
                    ));
                }
            };
            validate_sources(&state.conversations, &command)?;
            let now = now_ms();
            let created_at_ms = current.map_or(now, |memory| memory.created_at_ms);
            let memory = SemanticMemory {
                memory_id: command.memory_id,
                namespace: command.namespace,
                content: command.content,
                sources: command.sources,
                metadata: command.metadata,
                revision,
                created_at_ms,
                updated_at_ms: now,
            };
            state.memories.insert(memory.memory_id, memory.clone());
            Ok(memory)
        })
    }

    fn search_memory(
        &self,
        query: SemanticMemoryQuery,
    ) -> ConversationStoreFuture<'_, Result<Vec<SemanticMemory>, ConversationStoreError>> {
        Box::pin(async move {
            let query_terms = normalized_terms(&query.text);
            let state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut ranked = state
                .memories
                .values()
                .filter(|memory| memory.namespace == query.namespace)
                .filter_map(|memory| {
                    let terms = normalized_terms(&memory.content);
                    let score = query_terms.intersection(&terms).count();
                    (score > 0).then_some((score, memory))
                })
                .collect::<Vec<_>>();
            ranked.sort_by(|(left_score, left), (right_score, right)| {
                right_score
                    .cmp(left_score)
                    .then_with(|| right.updated_at_ms.cmp(&left.updated_at_ms))
                    .then_with(|| left.memory_id.cmp(&right.memory_id))
            });
            Ok(ranked
                .into_iter()
                .take(usize::from(query.limit.get()))
                .map(|(_, memory)| memory.clone())
                .collect())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use futures_executor::block_on;
    use runifold_model::{ContentPart, Message, Role};

    use super::*;

    fn namespace(value: &str) -> MemoryNamespace {
        MemoryNamespace::parse(value).unwrap()
    }

    fn assistant(text: &str) -> Message {
        Message::new(Role::Assistant, vec![ContentPart::text(text)]).unwrap()
    }

    fn transcript() -> Vec<Message> {
        vec![
            Message::user("u1"),
            assistant("a1"),
            Message::user("u2"),
            assistant("a2"),
            Message::user("u3"),
            assistant("a3"),
        ]
    }

    #[test]
    fn transcript_summary_buffer_and_window_remain_distinct() {
        let store = InMemoryConversationStore::new();
        let conversation_id = ConversationId::new();
        let namespace = namespace("tenant.user");
        block_on(store.create(conversation_id, namespace.clone())).unwrap();
        let version = block_on(store.append(
            namespace.clone(),
            ConversationAppend {
                conversation_id,
                expected_version: ConversationVersion::default(),
                messages: transcript(),
            },
        ))
        .unwrap();
        let view = block_on(store.load_view(
            conversation_id,
            namespace.clone(),
            ConversationWindow::new(2).unwrap(),
            ConversationSummaryBatch::new(4).unwrap(),
        ))
        .unwrap();
        assert_eq!(view.summary_buffer.len(), 4);
        assert_eq!(view.summary_backlog, 0);
        assert_eq!(view.window.len(), 2);
        assert!(view.requires_summary());

        let summary = block_on(store.commit_summary(
            namespace.clone(),
            ConversationSummaryCommit {
                conversation_id,
                expected_version: version,
                through_sequence: ConversationSequence::new(4).unwrap(),
                content: "The first two exchanges".into(),
            },
        ))
        .unwrap();
        let compacted = block_on(store.load_view(
            conversation_id,
            namespace.clone(),
            ConversationWindow::new(2).unwrap(),
            ConversationSummaryBatch::new(4).unwrap(),
        ))
        .unwrap();
        assert_eq!(compacted.summary, Some(summary));
        assert!(compacted.summary_buffer.is_empty());
        assert_eq!(compacted.summary_backlog, 0);
        assert_eq!(compacted.window.len(), 2);

        let immutable = block_on(store.list_transcript(
            conversation_id,
            namespace,
            None,
            ConversationWindow::new(16).unwrap(),
        ))
        .unwrap();
        assert_eq!(immutable.len(), 6);
        assert_eq!(immutable[0].message, Message::user("u1"));
    }

    #[test]
    fn conversation_view_bounds_summary_batch_and_reports_remaining_backlog() {
        let store = InMemoryConversationStore::new();
        let conversation_id = ConversationId::new();
        let namespace = MemoryNamespace::parse("tenant.bounded").unwrap();
        block_on(store.create(conversation_id, namespace.clone())).unwrap();
        let messages = (1..=10)
            .map(|sequence| Message::user(format!("message-{sequence}")))
            .collect();
        block_on(store.append(
            namespace.clone(),
            ConversationAppend {
                conversation_id,
                expected_version: ConversationVersion::default(),
                messages,
            },
        ))
        .unwrap();

        let view = block_on(store.load_view(
            conversation_id,
            namespace,
            ConversationWindow::new(2).unwrap(),
            ConversationSummaryBatch::new(3).unwrap(),
        ))
        .unwrap();

        assert_eq!(
            view.summary_buffer
                .iter()
                .map(|entry| entry.sequence.get())
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(view.summary_backlog, 5);
        assert_eq!(
            view.window
                .iter()
                .map(|entry| entry.sequence.get())
                .collect::<Vec<_>>(),
            vec![9, 10]
        );
    }

    #[test]
    fn transcript_append_is_versioned_and_rejects_system_messages() {
        let store = InMemoryConversationStore::new();
        let conversation_id = ConversationId::new();
        let namespace = namespace("tenant.user");
        block_on(store.create(conversation_id, namespace.clone())).unwrap();
        let version = block_on(store.append(
            namespace.clone(),
            ConversationAppend {
                conversation_id,
                expected_version: ConversationVersion::default(),
                messages: vec![Message::user("hello")],
            },
        ))
        .unwrap();
        assert_eq!(version, ConversationVersion::new(1));

        let conflict = block_on(store.append(
            namespace.clone(),
            ConversationAppend {
                conversation_id,
                expected_version: ConversationVersion::default(),
                messages: vec![Message::user("stale")],
            },
        ))
        .unwrap_err();
        assert_eq!(conflict.kind, ConversationStoreErrorKind::Conflict);

        let invalid = block_on(store.append(
            namespace,
            ConversationAppend {
                conversation_id,
                expected_version: version,
                messages: vec![Message::system("do not persist policy")],
            },
        ))
        .unwrap_err();
        assert_eq!(invalid.kind, ConversationStoreErrorKind::InvalidInput);
    }

    #[test]
    fn semantic_memory_is_explicit_cross_conversation_and_provenanced() {
        let store = InMemoryConversationStore::new();
        let namespace = namespace("tenant.user");
        let source_id = ConversationId::new();
        let other_id = ConversationId::new();
        block_on(store.create(source_id, namespace.clone())).unwrap();
        block_on(store.create(other_id, namespace.clone())).unwrap();
        block_on(store.append(
            namespace.clone(),
            ConversationAppend {
                conversation_id: source_id,
                expected_version: ConversationVersion::default(),
                messages: vec![
                    Message::user("I prefer Rust"),
                    assistant("Preference recorded"),
                ],
            },
        ))
        .unwrap();
        let memory_id = SemanticMemoryId::new();
        let memory = block_on(store.upsert_memory(SemanticMemoryUpsert {
            memory_id,
            namespace: namespace.clone(),
            content: "The user prefers Rust for systems programming".into(),
            sources: vec![SemanticMemorySource {
                conversation_id: source_id,
                from_sequence: ConversationSequence::new(1).unwrap(),
                through_sequence: ConversationSequence::new(2).unwrap(),
            }],
            metadata: BTreeMap::new(),
            expected_revision: None,
        }))
        .unwrap();
        assert_eq!(memory.revision, 0);

        let found = block_on(store.search_memory(
            SemanticMemoryQuery::new(namespace.clone(), "Rust preference", 4).unwrap(),
        ))
        .unwrap();
        assert_eq!(found, vec![memory]);
        assert!(
            block_on(store.list_transcript(
                other_id,
                namespace,
                None,
                ConversationWindow::new(4).unwrap(),
            ))
            .unwrap()
            .is_empty()
        );
    }
}
