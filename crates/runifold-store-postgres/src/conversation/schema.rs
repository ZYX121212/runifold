//! Conversation and semantic-memory schema management.

use std::num::NonZeroU32;

use super::{PostgresConversationStore, PostgresConversationStoreError};

impl PostgresConversationStore {
    /// Explicitly creates conversation, transcript, and semantic-memory tables.
    ///
    /// Runtime operations never perform hidden migrations.
    ///
    /// # Errors
    ///
    /// Propagates `PostgreSQL` DDL failures.
    pub async fn ensure_schema(&self) -> Result<(), PostgresConversationStoreError> {
        self.client
            .batch_execute(&Self::schema_sql(&self.table))
            .await?;
        Ok(())
    }

    /// Adds a nullable pgvector column and cosine HNSW index for semantic memory.
    ///
    /// Call this explicitly after [`Self::ensure_schema`] and before enabling
    /// a vector-configured store in production.
    ///
    /// # Errors
    ///
    /// Propagates extension and DDL failures.
    pub async fn ensure_semantic_memory_vector_schema(
        &self,
        dimensions: NonZeroU32,
    ) -> Result<(), PostgresConversationStoreError> {
        self.client
            .batch_execute(&format!(
                "CREATE EXTENSION IF NOT EXISTS vector;\
                 ALTER TABLE {0}_memory ADD COLUMN IF NOT EXISTS \
                    embedding vector({1});\
                 CREATE INDEX IF NOT EXISTS {0}_memory_embedding_hnsw \
                    ON {0}_memory USING hnsw (embedding vector_cosine_ops);",
                self.table,
                dimensions.get()
            ))
            .await?;
        Ok(())
    }

    pub(in crate::conversation) fn schema_sql(table: &str) -> String {
        format!(
            r"
            CREATE TABLE IF NOT EXISTS {table} (
                conversation_id UUID PRIMARY KEY,
                namespace TEXT NOT NULL,
                version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
                summary_id UUID,
                summary_content TEXT,
                summary_through BIGINT CHECK (summary_through > 0),
                summary_transcript_version BIGINT CHECK (summary_transcript_version >= 0),
                summary_created_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                CHECK (
                    (summary_id IS NULL AND summary_content IS NULL
                        AND summary_through IS NULL
                        AND summary_transcript_version IS NULL
                        AND summary_created_at IS NULL)
                    OR
                    (summary_id IS NOT NULL AND summary_content IS NOT NULL
                        AND summary_through IS NOT NULL
                        AND summary_transcript_version IS NOT NULL
                        AND summary_created_at IS NOT NULL)
                )
            );
            CREATE INDEX IF NOT EXISTS {table}_namespace_idx
                ON {table} (namespace, conversation_id);

            CREATE TABLE IF NOT EXISTS {table}_transcript (
                conversation_id UUID NOT NULL REFERENCES {table}(conversation_id)
                    ON DELETE CASCADE,
                sequence BIGINT NOT NULL CHECK (sequence > 0),
                message JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (conversation_id, sequence)
            );

            CREATE TABLE IF NOT EXISTS {table}_memory (
                memory_id UUID PRIMARY KEY,
                namespace TEXT NOT NULL,
                content TEXT NOT NULL,
                sources JSONB NOT NULL,
                metadata JSONB NOT NULL,
                revision BIGINT NOT NULL CHECK (revision >= 0),
                created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
            );
            CREATE INDEX IF NOT EXISTS {table}_memory_namespace_idx
                ON {table}_memory (namespace, updated_at DESC, memory_id);
            CREATE INDEX IF NOT EXISTS {table}_memory_search_idx
                ON {table}_memory USING GIN (to_tsvector('simple', content));

            CREATE TABLE IF NOT EXISTS {table}_checkpoints (
                checkpoint_id UUID PRIMARY KEY,
                revision BIGINT NOT NULL CHECK (revision >= 0),
                record_json JSONB NOT NULL
            );

            CREATE TABLE IF NOT EXISTS {table}_effects (
                effect_id UUID PRIMARY KEY,
                capability_id UUID NOT NULL,
                idempotency_key TEXT,
                revision BIGINT NOT NULL CHECK (revision >= 0),
                record_json JSONB NOT NULL,
                UNIQUE (capability_id, idempotency_key)
            );
            CREATE INDEX IF NOT EXISTS {table}_effects_capability_key
                ON {table}_effects (capability_id, idempotency_key);

            CREATE TABLE IF NOT EXISTS {table}_events (
                event_id UUID PRIMARY KEY,
                run_id UUID NOT NULL,
                sequence BIGINT NOT NULL CHECK (sequence >= 0),
                event_json JSONB NOT NULL,
                UNIQUE (run_id, sequence)
            );
            CREATE INDEX IF NOT EXISTS {table}_events_run_sequence
                ON {table}_events (run_id, sequence);

            CREATE TABLE IF NOT EXISTS {table}_artifacts (
                scope TEXT NOT NULL,
                artifact_id TEXT NOT NULL,
                media_type TEXT NOT NULL,
                size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
                sha256 TEXT NOT NULL,
                name TEXT,
                bytes BYTEA NOT NULL,
                created_at_ms BIGINT NOT NULL CHECK (created_at_ms >= 0),
                expires_at_ms BIGINT CHECK (expires_at_ms >= 0),
                PRIMARY KEY (scope, artifact_id)
            );
            CREATE INDEX IF NOT EXISTS {table}_artifacts_scope_expiry
                ON {table}_artifacts (scope, expires_at_ms, artifact_id);

            CREATE TABLE IF NOT EXISTS {table}_artifact_idempotency (
                scope TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                artifact_id TEXT NOT NULL,
                PRIMARY KEY (scope, idempotency_key),
                FOREIGN KEY (scope, artifact_id)
                    REFERENCES {table}_artifacts(scope, artifact_id) ON DELETE CASCADE
            );
            "
        )
    }
}
