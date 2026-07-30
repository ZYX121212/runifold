//! Bounded transcript loading and conversation conflict diagnosis.

use runifold_agent::{
    ConversationId, ConversationStoreError, ConversationSummaryBatch, ConversationTranscriptEntry,
    ConversationWindow, MemoryNamespace,
};

use super::{
    PostgresConversationStore,
    support::{
        conflict_error, conversation_uuid, decode_transcript_entry, namespace_mismatch, not_found,
        storage_error, to_i64, to_u64,
    },
};

pub(in crate::conversation) struct BoundedTranscript {
    pub(in crate::conversation) summary_buffer: Vec<ConversationTranscriptEntry>,
    pub(in crate::conversation) summary_backlog: u64,
    pub(in crate::conversation) window: Vec<ConversationTranscriptEntry>,
}

impl PostgresConversationStore {
    pub(in crate::conversation) async fn load_bounded_transcript(
        &self,
        conversation_id: ConversationId,
        summarized_through: u64,
        window: ConversationWindow,
        summary_batch: ConversationSummaryBatch,
    ) -> Result<BoundedTranscript, ConversationStoreError> {
        let count_sql = format!(
            "SELECT COUNT(*) AS entry_count FROM {}_transcript \
             WHERE conversation_id = $1 AND sequence > $2",
            self.table
        );
        let count_row = self
            .client
            .query_one(
                &count_sql,
                &[
                    &conversation_uuid(conversation_id),
                    &to_i64(summarized_through)?,
                ],
            )
            .await
            .map_err(storage_error)?;
        let unsummarized_count = to_u64(count_row.get("entry_count"))?;
        let window_count = unsummarized_count.min(u64::from(window.get()));
        let older_count = unsummarized_count.saturating_sub(window_count);
        let summary_count = older_count.min(u64::from(summary_batch.get()));
        let summary_sql = format!(
            "SELECT sequence, message FROM {}_transcript \
             WHERE conversation_id = $1 AND sequence > $2 \
             ORDER BY sequence ASC LIMIT $3",
            self.table
        );
        let summary_buffer = self
            .client
            .query(
                &summary_sql,
                &[
                    &conversation_uuid(conversation_id),
                    &to_i64(summarized_through)?,
                    &to_i64(summary_count)?,
                ],
            )
            .await
            .map_err(storage_error)?
            .iter()
            .map(decode_transcript_entry)
            .collect::<Result<Vec<_>, _>>()?;
        let window_sql = format!(
            "SELECT sequence, message FROM {}_transcript \
             WHERE conversation_id = $1 AND sequence > $2 \
             ORDER BY sequence DESC LIMIT $3",
            self.table
        );
        let mut live_window = self
            .client
            .query(
                &window_sql,
                &[
                    &conversation_uuid(conversation_id),
                    &to_i64(summarized_through)?,
                    &to_i64(window_count)?,
                ],
            )
            .await
            .map_err(storage_error)?
            .iter()
            .map(decode_transcript_entry)
            .collect::<Result<Vec<_>, _>>()?;
        live_window.reverse();
        Ok(BoundedTranscript {
            summary_buffer,
            summary_backlog: older_count.saturating_sub(summary_count),
            window: live_window,
        })
    }

    pub(in crate::conversation) async fn conversation_namespace(
        &self,
        conversation_id: ConversationId,
    ) -> Result<Option<String>, ConversationStoreError> {
        let sql = format!(
            "SELECT namespace FROM {} WHERE conversation_id = $1",
            self.table
        );
        self.client
            .query_opt(&sql, &[&conversation_uuid(conversation_id)])
            .await
            .map_err(storage_error)
            .map(|row| row.map(|row| row.get("namespace")))
    }

    pub(in crate::conversation) async fn diagnose_conversation(
        &self,
        conversation_id: ConversationId,
        namespace: &MemoryNamespace,
        conflict: &'static str,
    ) -> ConversationStoreError {
        match self.conversation_namespace(conversation_id).await {
            Ok(None) => not_found(),
            Ok(Some(actual)) if actual != namespace.as_str() => namespace_mismatch(),
            Ok(Some(_)) => conflict_error(conflict),
            Err(error) => error,
        }
    }
}
