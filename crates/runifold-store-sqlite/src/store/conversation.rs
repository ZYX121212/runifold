//! Conversation persistence and atomic terminal checkpoint commit.

use std::sync::Arc;

use futures_executor::block_on;
use runifold_agent::{
    ConversationAppend, ConversationCreateOutcome, ConversationId, ConversationSequence,
    ConversationStore, ConversationStoreError, ConversationStoreErrorKind, ConversationStoreFuture,
    ConversationSummary, ConversationSummaryBatch, ConversationSummaryCommit,
    ConversationTranscriptEntry, ConversationVersion, ConversationView, ConversationWindow,
    DurableConversationCommit, DurableConversationStore, InMemoryConversationStore,
    MemoryNamespace, SemanticMemory, SemanticMemoryQuery, SemanticMemoryUpsert,
};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};

use super::{SqliteStore, current_revision, sqlite_revision};

const SNAPSHOT_FORMAT_VERSION: i64 = 1;

impl SqliteStore {
    fn execute_conversation<T, F>(
        &self,
        operation: F,
    ) -> ConversationStoreFuture<'_, Result<T, ConversationStoreError>>
    where
        T: Send + 'static,
        F: FnOnce(&InMemoryConversationStore) -> Result<T, ConversationStoreError> + Send + 'static,
    {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
                storage_error("SQLite conversation operations require a Tokio runtime")
            })?;
            runtime
                .spawn_blocking(move || {
                    let mut connection = connection
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let transaction = connection
                        .transaction_with_behavior(TransactionBehavior::Immediate)
                        .map_err(|error| database_error(&error))?;
                    let state = load_state(&transaction)?;
                    let output = operation(&state)?;
                    save_state(&transaction, &state)?;
                    transaction
                        .commit()
                        .map_err(|error| database_error(&error))?;
                    Ok(output)
                })
                .await
                .map_err(|error| {
                    storage_error(format!("SQLite conversation task failed: {error}"))
                })?
        })
    }
}

impl ConversationStore for SqliteStore {
    fn create(
        &self,
        conversation_id: ConversationId,
        namespace: MemoryNamespace,
    ) -> ConversationStoreFuture<'_, Result<ConversationCreateOutcome, ConversationStoreError>>
    {
        self.execute_conversation(move |store| block_on(store.create(conversation_id, namespace)))
    }

    fn load_view(
        &self,
        conversation_id: ConversationId,
        namespace: MemoryNamespace,
        window: ConversationWindow,
        summary_batch: ConversationSummaryBatch,
    ) -> ConversationStoreFuture<'_, Result<ConversationView, ConversationStoreError>> {
        self.execute_conversation(move |store| {
            block_on(store.load_view(conversation_id, namespace, window, summary_batch))
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
        self.execute_conversation(move |store| {
            block_on(store.list_transcript(conversation_id, namespace, after, limit))
        })
    }

    fn append(
        &self,
        namespace: MemoryNamespace,
        command: ConversationAppend,
    ) -> ConversationStoreFuture<'_, Result<ConversationVersion, ConversationStoreError>> {
        self.execute_conversation(move |store| block_on(store.append(namespace, command)))
    }

    fn commit_summary(
        &self,
        namespace: MemoryNamespace,
        command: ConversationSummaryCommit,
    ) -> ConversationStoreFuture<'_, Result<ConversationSummary, ConversationStoreError>> {
        self.execute_conversation(move |store| block_on(store.commit_summary(namespace, command)))
    }

    fn upsert_memory(
        &self,
        command: SemanticMemoryUpsert,
    ) -> ConversationStoreFuture<'_, Result<SemanticMemory, ConversationStoreError>> {
        self.execute_conversation(move |store| block_on(store.upsert_memory(command)))
    }

    fn search_memory(
        &self,
        query: SemanticMemoryQuery,
    ) -> ConversationStoreFuture<'_, Result<Vec<SemanticMemory>, ConversationStoreError>> {
        self.execute_conversation(move |store| block_on(store.search_memory(query)))
    }
}

impl DurableConversationStore for SqliteStore {
    fn commit_durable_turn(
        &self,
        command: DurableConversationCommit,
    ) -> ConversationStoreFuture<'_, Result<ConversationVersion, ConversationStoreError>> {
        let connection = Arc::clone(&self.connection);
        Box::pin(async move {
            let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
                storage_error("SQLite durable conversation commit requires a Tokio runtime")
            })?;
            runtime
                .spawn_blocking(move || {
                    let mut connection = connection
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let transaction = connection
                        .transaction_with_behavior(TransactionBehavior::Immediate)
                        .map_err(|error| database_error(&error))?;
                    let state = load_state(&transaction)?;
                    let version = block_on(state.append(command.namespace, command.append))?;
                    compare_and_swap_checkpoint(
                        &transaction,
                        &command.checkpoint,
                        command.expected_checkpoint_revision,
                    )?;
                    save_state(&transaction, &state)?;
                    transaction
                        .commit()
                        .map_err(|error| database_error(&error))?;
                    Ok(version)
                })
                .await
                .map_err(|error| {
                    storage_error(format!("SQLite durable commit task failed: {error}"))
                })?
        })
    }
}

fn load_state(
    transaction: &Transaction<'_>,
) -> Result<InMemoryConversationStore, ConversationStoreError> {
    let stored = transaction
        .query_row(
            "SELECT format_version, state_blob
             FROM runifold_conversation_state
             WHERE singleton_id = 1",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()
        .map_err(|error| database_error(&error))?;
    match stored {
        Some((SNAPSHOT_FORMAT_VERSION, encoded)) => {
            InMemoryConversationStore::from_persistent_snapshot(&encoded)
        }
        Some((format_version, _)) => Err(storage_error(format!(
            "unsupported SQLite conversation state format version {format_version}"
        ))),
        None => Ok(InMemoryConversationStore::new()),
    }
}

fn save_state(
    transaction: &Transaction<'_>,
    state: &InMemoryConversationStore,
) -> Result<(), ConversationStoreError> {
    let encoded = state.export_persistent_snapshot()?;
    transaction
        .execute(
            "INSERT INTO runifold_conversation_state (
                 singleton_id, format_version, state_blob, updated_at_ms
             ) VALUES (1, ?1, ?2,
                 CAST(strftime('%s', 'now') AS INTEGER) * 1000
                 + CAST(substr(strftime('%f', 'now'), 4, 3) AS INTEGER))
             ON CONFLICT(singleton_id) DO UPDATE SET
                 format_version = excluded.format_version,
                 state_blob = excluded.state_blob,
                 updated_at_ms = excluded.updated_at_ms",
            params![SNAPSHOT_FORMAT_VERSION, encoded],
        )
        .map_err(|error| database_error(&error))?;
    Ok(())
}

fn compare_and_swap_checkpoint(
    transaction: &Transaction<'_>,
    checkpoint: &runifold_core::Checkpoint,
    expected_revision: u64,
) -> Result<(), ConversationStoreError> {
    let revision = sqlite_revision(checkpoint.revision).map_err(storage_error)?;
    let expected = sqlite_revision(expected_revision).map_err(storage_error)?;
    if revision
        != expected.checked_add(1).ok_or_else(|| {
            conflict_error("checkpoint revision overflow during durable conversation commit")
        })?
    {
        return Err(conflict_error(
            "durable conversation checkpoint revision is not the expected successor",
        ));
    }
    let current = current_revision(
        transaction,
        "runifold_checkpoints",
        "checkpoint_id",
        &checkpoint.id.to_string(),
    )
    .map_err(|error| database_error(&error))?;
    if current != Some(expected) {
        return Err(conflict_error(
            "durable conversation checkpoint revision precondition failed",
        ));
    }
    let record = serde_json::to_string(checkpoint)
        .map_err(|error| storage_error(format!("checkpoint encoding failed: {error}")))?;
    let changed = transaction
        .execute(
            "UPDATE runifold_checkpoints
             SET revision = ?1, record_json = ?2
             WHERE checkpoint_id = ?3 AND revision = ?4",
            params![revision, record, checkpoint.id.to_string(), expected],
        )
        .map_err(|error| database_error(&error))?;
    if changed != 1 {
        return Err(conflict_error(
            "durable conversation checkpoint compare-and-swap failed",
        ));
    }
    Ok(())
}

fn database_error(error: &rusqlite::Error) -> ConversationStoreError {
    storage_error(format!("SQLite conversation operation failed: {error}"))
}

fn storage_error(message: impl Into<String>) -> ConversationStoreError {
    ConversationStoreError::new(ConversationStoreErrorKind::Storage, message)
}

fn conflict_error(message: impl Into<String>) -> ConversationStoreError {
    ConversationStoreError::new(ConversationStoreErrorKind::Conflict, message)
}
