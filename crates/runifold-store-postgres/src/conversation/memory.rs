//! Lexical and vector semantic-memory persistence helpers.

use pgvector::Vector;
use runifold_agent::{
    ConversationStoreError, SemanticMemory, SemanticMemoryQuery, SemanticMemoryUpsert,
};

use super::{
    PostgresConversationStore,
    support::{
        conflict_error, conversation_uuid, decode_memory, encode_error, invalid_input, memory_uuid,
        namespace_mismatch, not_found, storage_error, to_i64, to_u64,
    },
};

impl PostgresConversationStore {
    pub(in crate::conversation) async fn upsert_vector_memory(
        &self,
        command: SemanticMemoryUpsert,
        embedding: Vector,
    ) -> Result<SemanticMemory, ConversationStoreError> {
        let sources = serde_json::to_value(&command.sources).map_err(encode_error)?;
        let metadata = serde_json::to_value(&command.metadata).map_err(encode_error)?;
        let sql = format!(
            r"
            INSERT INTO {table}_memory (
                memory_id, namespace, content, sources, metadata, revision, embedding
            )
            VALUES ($1, $2, $3, $4, $5, 0, $7)
            ON CONFLICT (memory_id) DO UPDATE
            SET content = EXCLUDED.content, sources = EXCLUDED.sources,
                metadata = EXCLUDED.metadata, embedding = EXCLUDED.embedding,
                revision = {table}_memory.revision + 1,
                updated_at = clock_timestamp()
            WHERE {table}_memory.namespace = EXCLUDED.namespace
                AND $6::BIGINT IS NOT NULL
                AND {table}_memory.revision = $6
                AND {table}_memory.revision < 9223372036854775807
            RETURNING memory_id, namespace, content, sources, metadata, revision,
                (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS created_at_ms,
                (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT AS updated_at_ms
            ",
            table = self.table
        );
        let expected = command.expected_revision.map(to_i64).transpose()?;
        let row = self
            .client
            .query_opt(
                &sql,
                &[
                    &memory_uuid(command.memory_id),
                    &command.namespace.as_str(),
                    &command.content,
                    &sources,
                    &metadata,
                    &expected,
                    &embedding,
                ],
            )
            .await
            .map_err(storage_error)?;
        match row {
            Some(row) => decode_memory(&row),
            None => self.diagnose_memory(&command).await,
        }
    }

    pub(in crate::conversation) async fn search_vector_memory(
        &self,
        query: &SemanticMemoryQuery,
        embedding: Vector,
    ) -> Result<Vec<SemanticMemory>, ConversationStoreError> {
        let sql = format!(
            r"
            SELECT memory_id, namespace, content, sources, metadata, revision,
                (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS created_at_ms,
                (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT AS updated_at_ms
            FROM {table}_memory
            WHERE namespace = $1 AND embedding IS NOT NULL
            ORDER BY embedding <=> $2, updated_at DESC, memory_id ASC
            LIMIT $3
            ",
            table = self.table
        );
        self.client
            .query(
                &sql,
                &[
                    &query.namespace.as_str(),
                    &embedding,
                    &i64::from(query.limit.get()),
                ],
            )
            .await
            .map_err(storage_error)?
            .iter()
            .map(decode_memory)
            .collect()
    }

    pub(in crate::conversation) async fn validate_memory_sources(
        &self,
        command: &SemanticMemoryUpsert,
    ) -> Result<(), ConversationStoreError> {
        let sql = format!(
            r"
            SELECT namespace, (
                SELECT COALESCE(MAX(sequence), 0)
                FROM {table}_transcript
                WHERE conversation_id = {table}.conversation_id
            ) AS last_sequence
            FROM {table} WHERE conversation_id = $1
            ",
            table = self.table
        );
        for source in &command.sources {
            let Some(row) = self
                .client
                .query_opt(&sql, &[&conversation_uuid(source.conversation_id)])
                .await
                .map_err(storage_error)?
            else {
                return Err(not_found());
            };
            let namespace: String = row.get("namespace");
            if namespace != command.namespace.as_str() {
                return Err(namespace_mismatch());
            }
            let last_sequence: i64 = row.get("last_sequence");
            if source.through_sequence.get() > to_u64(last_sequence)? {
                return Err(invalid_input(
                    "semantic memory source exceeds the immutable transcript",
                ));
            }
        }
        Ok(())
    }

    pub(in crate::conversation) async fn diagnose_memory(
        &self,
        command: &SemanticMemoryUpsert,
    ) -> Result<SemanticMemory, ConversationStoreError> {
        let sql = format!(
            "SELECT namespace FROM {}_memory WHERE memory_id = $1",
            self.table
        );
        let row = self
            .client
            .query_opt(&sql, &[&memory_uuid(command.memory_id)])
            .await
            .map_err(storage_error)?;
        match row {
            Some(row) if row.get::<_, String>("namespace") != command.namespace.as_str() => {
                Err(namespace_mismatch())
            }
            _ => Err(conflict_error(
                "semantic memory revision precondition failed",
            )),
        }
    }
}
