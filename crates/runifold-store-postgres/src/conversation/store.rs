//! `ConversationStore` persistence implementation.

use std::time::Instant;

use runifold_agent::{
    ConversationAppend, ConversationCreateOutcome, ConversationId, ConversationSequence,
    ConversationStore, ConversationStoreError, ConversationStoreFuture, ConversationSummary,
    ConversationSummaryBatch, ConversationSummaryCommit, ConversationTranscriptEntry,
    ConversationVersion, ConversationView, ConversationWindow, MemoryNamespace, SemanticMemory,
    SemanticMemoryQuery, SemanticMemorySearchOutcome, SemanticMemoryUpsert,
    SemanticMemoryUpsertOutcome,
};
use runifold_core::CheckpointId;
use runifold_retrieval::{EmbeddingRequest, EmbeddingTask, RetrievalContext};

use super::{
    PostgresConversationStore,
    support::{
        combine_usage, conversation_uuid, database_usage, decode_memory, decode_required_summary,
        decode_summary, decode_transcript_entry, decode_version, encode_error, invalid_input,
        memory_uuid, namespace_mismatch, not_found, retrieval_error, storage_error, to_i64,
        to_pgvector, validate_append, validate_memory, validate_summary,
    },
};

impl ConversationStore for PostgresConversationStore {
    fn create(
        &self,
        conversation_id: ConversationId,
        namespace: MemoryNamespace,
    ) -> ConversationStoreFuture<'_, Result<ConversationCreateOutcome, ConversationStoreError>>
    {
        Box::pin(async move {
            let sql = format!(
                r"
                WITH inserted AS (
                    INSERT INTO {} (conversation_id, namespace)
                    VALUES ($1, $2)
                    ON CONFLICT (conversation_id) DO NOTHING
                    RETURNING namespace
                )
                SELECT namespace, TRUE AS created FROM inserted
                UNION ALL
                SELECT namespace, FALSE AS created FROM {}
                WHERE conversation_id = $1 AND NOT EXISTS (SELECT 1 FROM inserted)
                ",
                self.table, self.table
            );
            let row = self
                .client
                .query_one(
                    &sql,
                    &[&conversation_uuid(conversation_id), &namespace.as_str()],
                )
                .await
                .map_err(storage_error)?;
            let actual: String = row.get("namespace");
            if actual != namespace.as_str() {
                return Err(namespace_mismatch());
            }
            Ok(if row.get("created") {
                ConversationCreateOutcome::Created
            } else {
                ConversationCreateOutcome::Duplicate
            })
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
            let metadata_sql = format!(
                r"
                SELECT namespace, version, summary_id, summary_content, summary_through,
                    summary_transcript_version,
                    (EXTRACT(EPOCH FROM summary_created_at) * 1000)::BIGINT
                        AS summary_created_at_ms
                FROM {} WHERE conversation_id = $1
                ",
                self.table
            );
            let Some(row) = self
                .client
                .query_opt(&metadata_sql, &[&conversation_uuid(conversation_id)])
                .await
                .map_err(storage_error)?
            else {
                return Err(not_found());
            };
            let actual: String = row.get("namespace");
            if actual != namespace.as_str() {
                return Err(namespace_mismatch());
            }
            let summary = decode_summary(&row)?;
            let summarized_through = summary
                .as_ref()
                .map_or(0, |summary| summary.through_sequence.get());
            let transcript = self
                .load_bounded_transcript(conversation_id, summarized_through, window, summary_batch)
                .await?;
            Ok(ConversationView {
                conversation_id,
                namespace,
                version: decode_version(row.get("version"))?,
                summary,
                summary_buffer: transcript.summary_buffer,
                summary_backlog: transcript.summary_backlog,
                window: transcript.window,
            })
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
            let Some(actual) = self.conversation_namespace(conversation_id).await? else {
                return Err(not_found());
            };
            if actual != namespace.as_str() {
                return Err(namespace_mismatch());
            }
            let sql = format!(
                r"
                SELECT sequence, message FROM {}_transcript
                WHERE conversation_id = $1 AND sequence > $2
                ORDER BY sequence ASC LIMIT $3
                ",
                self.table
            );
            self.client
                .query(
                    &sql,
                    &[
                        &conversation_uuid(conversation_id),
                        &to_i64(after.map_or(0, ConversationSequence::get))?,
                        &i64::from(limit.get()),
                    ],
                )
                .await
                .map_err(storage_error)?
                .iter()
                .map(decode_transcript_entry)
                .collect()
        })
    }

    fn append(
        &self,
        namespace: MemoryNamespace,
        command: ConversationAppend,
    ) -> ConversationStoreFuture<'_, Result<ConversationVersion, ConversationStoreError>> {
        Box::pin(async move {
            validate_append(&command)?;
            let messages = serde_json::to_value(&command.messages).map_err(encode_error)?;
            let sql = format!(
                r"
                WITH updated AS (
                    UPDATE {table}
                    SET version = version + 1, updated_at = clock_timestamp()
                    WHERE conversation_id = $1 AND namespace = $2
                        AND version = $3 AND version < 9223372036854775807
                    RETURNING conversation_id, version
                ),
                base AS (
                    SELECT COALESCE(MAX(sequence), 0) AS last_sequence
                    FROM {table}_transcript WHERE conversation_id = $1
                ),
                inserted AS (
                    INSERT INTO {table}_transcript (conversation_id, sequence, message)
                    SELECT updated.conversation_id,
                        base.last_sequence + payload.ordinality,
                        payload.message
                    FROM updated CROSS JOIN base
                    CROSS JOIN LATERAL
                        jsonb_array_elements($4::JSONB)
                        WITH ORDINALITY AS payload(message, ordinality)
                    RETURNING sequence
                )
                SELECT updated.version FROM updated
                WHERE (SELECT COUNT(*) FROM inserted) = jsonb_array_length($4::JSONB)
                ",
                table = self.table
            );
            let row = self
                .client
                .query_opt(
                    &sql,
                    &[
                        &conversation_uuid(command.conversation_id),
                        &namespace.as_str(),
                        &to_i64(command.expected_version.get())?,
                        &messages,
                    ],
                )
                .await
                .map_err(storage_error)?;
            match row {
                Some(row) => decode_version(row.get("version")),
                None => Err(self
                    .diagnose_conversation(
                        command.conversation_id,
                        &namespace,
                        "conversation transcript version precondition failed",
                    )
                    .await),
            }
        })
    }

    fn commit_summary(
        &self,
        namespace: MemoryNamespace,
        command: ConversationSummaryCommit,
    ) -> ConversationStoreFuture<'_, Result<ConversationSummary, ConversationStoreError>> {
        Box::pin(async move {
            validate_summary(&command)?;
            let summary_id = CheckpointId::new();
            let sql = format!(
                r"
                UPDATE {table}
                SET summary_id = $4, summary_content = $5, summary_through = $6,
                    summary_transcript_version = version,
                    summary_created_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                WHERE conversation_id = $1 AND namespace = $2 AND version = $3
                    AND $6 > COALESCE(summary_through, 0)
                    AND $6 <= (
                        SELECT COALESCE(MAX(sequence), 0)
                        FROM {table}_transcript WHERE conversation_id = $1
                    )
                RETURNING summary_id, summary_content, summary_through,
                    summary_transcript_version,
                    (EXTRACT(EPOCH FROM summary_created_at) * 1000)::BIGINT
                        AS summary_created_at_ms
                ",
                table = self.table
            );
            let row = self
                .client
                .query_opt(
                    &sql,
                    &[
                        &conversation_uuid(command.conversation_id),
                        &namespace.as_str(),
                        &to_i64(command.expected_version.get())?,
                        &summary_id.as_uuid(),
                        &command.content,
                        &to_i64(command.through_sequence.get())?,
                    ],
                )
                .await
                .map_err(storage_error)?;
            match row {
                Some(row) => decode_required_summary(&row),
                None => Err(self
                    .diagnose_conversation(
                        command.conversation_id,
                        &namespace,
                        "conversation summary precondition failed",
                    )
                    .await),
            }
        })
    }

    fn upsert_memory(
        &self,
        command: SemanticMemoryUpsert,
    ) -> ConversationStoreFuture<'_, Result<SemanticMemory, ConversationStoreError>> {
        if self.semantic_embedder.is_some() {
            return Box::pin(async move {
                self.upsert_memory_scoped(command, RetrievalContext::new())
                    .await
                    .map(|outcome| outcome.memory)
            });
        }
        Box::pin(async move {
            validate_memory(&command)?;
            self.validate_memory_sources(&command).await?;
            let sources = serde_json::to_value(&command.sources).map_err(encode_error)?;
            let metadata = serde_json::to_value(&command.metadata).map_err(encode_error)?;
            let sql = format!(
                r"
                INSERT INTO {table}_memory (
                    memory_id, namespace, content, sources, metadata, revision
                )
                VALUES ($1, $2, $3, $4, $5, 0)
                ON CONFLICT (memory_id) DO UPDATE
                SET content = EXCLUDED.content, sources = EXCLUDED.sources,
                    metadata = EXCLUDED.metadata,
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
                    ],
                )
                .await
                .map_err(storage_error)?;
            match row {
                Some(row) => decode_memory(&row),
                None => self.diagnose_memory(&command).await,
            }
        })
    }

    fn search_memory(
        &self,
        query: SemanticMemoryQuery,
    ) -> ConversationStoreFuture<'_, Result<Vec<SemanticMemory>, ConversationStoreError>> {
        if self.semantic_embedder.is_some() {
            return Box::pin(async move {
                self.search_memory_scoped(query, RetrievalContext::new())
                    .await
                    .map(|outcome| outcome.memories)
            });
        }
        Box::pin(async move {
            let sql = format!(
                r"
                SELECT memory_id, namespace, content, sources, metadata, revision,
                    (EXTRACT(EPOCH FROM created_at) * 1000)::BIGINT AS created_at_ms,
                    (EXTRACT(EPOCH FROM updated_at) * 1000)::BIGINT AS updated_at_ms
                FROM {table}_memory
                WHERE namespace = $1
                    AND to_tsvector('simple', content) @@ plainto_tsquery('simple', $2)
                ORDER BY ts_rank_cd(
                    to_tsvector('simple', content),
                    plainto_tsquery('simple', $2)
                ) DESC, updated_at DESC, memory_id ASC
                LIMIT $3
                ",
                table = self.table
            );
            self.client
                .query(
                    &sql,
                    &[
                        &query.namespace.as_str(),
                        &query.text,
                        &i64::from(query.limit.get()),
                    ],
                )
                .await
                .map_err(storage_error)?
                .iter()
                .map(decode_memory)
                .collect()
        })
    }

    fn upsert_memory_scoped(
        &self,
        command: SemanticMemoryUpsert,
        context: RetrievalContext,
    ) -> ConversationStoreFuture<'_, Result<SemanticMemoryUpsertOutcome, ConversationStoreError>>
    {
        Box::pin(async move {
            context
                .check_live()
                .map_err(|error| retrieval_error(&error))?;
            let Some(embedder) = &self.semantic_embedder else {
                let started = Instant::now();
                let memory = self.upsert_memory(command).await?;
                return Ok(SemanticMemoryUpsertOutcome {
                    memory,
                    usage: database_usage(started),
                });
            };
            validate_memory(&command)?;
            self.validate_memory_sources(&command).await?;
            let batch = embedder
                .embed(
                    EmbeddingRequest::new(
                        vec![command.content.clone()],
                        EmbeddingTask::RetrievalDocument,
                    )
                    .map_err(|error| retrieval_error(&error))?,
                    context.child_attempt(),
                )
                .await
                .map_err(|error| retrieval_error(&error))?
                .validate_count(1)
                .map_err(|error| retrieval_error(&error))?;
            let embedding_usage = batch.usage;
            let embedding = batch
                .embeddings
                .into_iter()
                .next()
                .ok_or_else(|| invalid_input("semantic memory embedding response was empty"))?;
            let started = Instant::now();
            let memory = self
                .upsert_vector_memory(command, to_pgvector(&embedding)?)
                .await?;
            Ok(SemanticMemoryUpsertOutcome {
                memory,
                usage: combine_usage(embedding_usage, database_usage(started))?,
            })
        })
    }

    fn search_memory_scoped(
        &self,
        query: SemanticMemoryQuery,
        context: RetrievalContext,
    ) -> ConversationStoreFuture<'_, Result<SemanticMemorySearchOutcome, ConversationStoreError>>
    {
        Box::pin(async move {
            context
                .check_live()
                .map_err(|error| retrieval_error(&error))?;
            let Some(embedder) = &self.semantic_embedder else {
                let started = Instant::now();
                let memories = self.search_memory(query).await?;
                return Ok(SemanticMemorySearchOutcome {
                    memories,
                    usage: database_usage(started),
                });
            };
            let batch = embedder
                .embed(
                    EmbeddingRequest::new(vec![query.text.clone()], EmbeddingTask::RetrievalQuery)
                        .map_err(|error| retrieval_error(&error))?,
                    context.child_attempt(),
                )
                .await
                .map_err(|error| retrieval_error(&error))?
                .validate_count(1)
                .map_err(|error| retrieval_error(&error))?;
            let embedding_usage = batch.usage;
            let embedding = batch
                .embeddings
                .into_iter()
                .next()
                .ok_or_else(|| invalid_input("semantic memory query embedding was empty"))?;
            let started = Instant::now();
            let memories = self
                .search_vector_memory(&query, to_pgvector(&embedding)?)
                .await?;
            Ok(SemanticMemorySearchOutcome {
                memories,
                usage: combine_usage(embedding_usage, database_usage(started))?,
            })
        })
    }
}
