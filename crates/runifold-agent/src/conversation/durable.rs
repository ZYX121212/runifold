//! Atomic conversation-and-checkpoint commit boundary.

use runifold_core::{Checkpoint, CheckpointId, CheckpointStore};

use super::{
    ConversationAppend, ConversationContextPolicy, ConversationId, ConversationStore,
    ConversationStoreError, ConversationStoreFuture, ConversationVersion, MemoryNamespace,
};

/// Stable identities and context policy for one new durable Agent turn.
#[derive(Clone, Debug)]
pub struct DurableConversationRequest {
    /// Fresh checkpoint identity used as the turn's idempotency identity.
    pub checkpoint_id: CheckpointId,
    /// Conversation receiving the completed turn.
    pub conversation_id: ConversationId,
    /// Isolation namespace owning the conversation.
    pub namespace: MemoryNamespace,
    /// Bounded context selection policy.
    pub policy: ConversationContextPolicy,
}

/// Final state of one durable conversational Agent turn.
///
/// Implementations must validate both optimistic preconditions and commit the
/// transcript append and checkpoint replacement in one storage transaction.
#[derive(Clone, Debug)]
pub struct DurableConversationCommit {
    /// Namespace owning the conversation.
    pub namespace: MemoryNamespace,
    /// Canonical messages produced by the single Agent execution.
    pub append: ConversationAppend,
    /// Completed checkpoint revision to persist with the transcript.
    pub checkpoint: Checkpoint,
    /// Checkpoint revision observed immediately before the final commit.
    pub expected_checkpoint_revision: u64,
}

/// Store capable of atomically committing conversation and recovery state.
///
/// Merely implementing [`ConversationStore`] and [`CheckpointStore`] is not
/// sufficient: the final writes must share one real transaction.
pub trait DurableConversationStore: ConversationStore + CheckpointStore {
    /// Atomically appends a transcript turn and advances its checkpoint.
    fn commit_durable_turn(
        &self,
        command: DurableConversationCommit,
    ) -> ConversationStoreFuture<'_, Result<ConversationVersion, ConversationStoreError>>;
}
