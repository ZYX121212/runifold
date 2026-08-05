//! `PostgreSQL` conversation, summary, and semantic-memory adapter.

mod durable;
mod memory;
mod schema;
mod store;
mod support;
mod transcript;

use support::validate_identifier;

use std::{fmt, sync::Arc};

use runifold_retrieval::EmbeddingModel;
use thiserror::Error;
use tokio_postgres::{Client, NoTls};

use crate::blocking::PostgresBlockingClient;

const MAX_CONTENT_BYTES: usize = 262_144;

/// `PostgreSQL` conversation-store configuration or schema failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PostgresConversationStoreError {
    /// The configured table name is unsafe for SQL interpolation.
    #[error("conversation table must be a portable PostgreSQL identifier of at most 48 bytes")]
    InvalidTable,
    /// `PostgreSQL` connection or explicit schema setup failed.
    #[error("PostgreSQL conversation store operation failed: {0}")]
    Database(#[from] tokio_postgres::Error),
    /// The blocking connection worker could not be joined.
    #[error("PostgreSQL blocking connection task failed: {0}")]
    ConnectionTask(String),
}

/// PostgreSQL-backed append-only conversation and semantic-memory store.
#[derive(Clone)]
pub struct PostgresConversationStore {
    client: Arc<Client>,
    transaction_client: Arc<tokio::sync::Mutex<Client>>,
    blocking: PostgresBlockingClient,
    table: String,
    semantic_embedder: Option<Arc<dyn EmbeddingModel>>,
}

impl fmt::Debug for PostgresConversationStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresConversationStore")
            .field("table", &self.table)
            .field(
                "semantic_embedding",
                &self.semantic_embedder.as_ref().map(|_| "configured"),
            )
            .finish_non_exhaustive()
    }
}

impl PostgresConversationStore {
    /// Connects without creating or changing schema.
    ///
    /// # Errors
    ///
    /// Rejects unsafe table identifiers and propagates connection failures.
    pub async fn connect(
        connection: &str,
        table: &str,
    ) -> Result<Self, PostgresConversationStoreError> {
        validate_identifier(table)?;
        let sync_connection = connection.to_owned();
        let blocking =
            tokio::task::spawn_blocking(move || PostgresBlockingClient::connect(&sync_connection))
                .await
                .map_err(|error| PostgresConversationStoreError::ConnectionTask(error.to_string()))?
                .map_err(PostgresConversationStoreError::ConnectionTask)?;
        let (client, primary_connection) = tokio_postgres::connect(connection, NoTls).await?;
        tokio::spawn(async move {
            let _ = primary_connection.await;
        });
        let (transaction_client, transaction_connection) =
            tokio_postgres::connect(connection, NoTls).await?;
        tokio::spawn(async move {
            let _ = transaction_connection.await;
        });
        Ok(Self {
            client: Arc::new(client),
            transaction_client: Arc::new(tokio::sync::Mutex::new(transaction_client)),
            blocking,
            table: table.to_owned(),
            semantic_embedder: None,
        })
    }

    /// Enables vector semantic-memory writes and searches for scoped operations.
    #[must_use]
    pub fn with_semantic_memory_embedder(mut self, embedder: Arc<dyn EmbeddingModel>) -> Self {
        self.semantic_embedder = Some(embedder);
        self
    }

    pub(crate) const fn blocking(&self) -> &PostgresBlockingClient {
        &self.blocking
    }

    pub(crate) fn table(&self) -> &str {
        &self.table
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_identifiers_are_restricted_before_sql_construction() {
        assert!(validate_identifier("runifold_conversations").is_ok());
        assert!(matches!(
            validate_identifier("bad-name"),
            Err(PostgresConversationStoreError::InvalidTable)
        ));
        assert!(matches!(
            validate_identifier("1bad"),
            Err(PostgresConversationStoreError::InvalidTable)
        ));
    }

    #[test]
    fn schema_preserves_append_only_transcript_and_search_indexes() {
        let schema = PostgresConversationStore::schema_sql("runifold_conversations");

        assert!(schema.contains("PRIMARY KEY (conversation_id, sequence)"));
        assert!(schema.contains("to_tsvector('simple', content)"));
        assert!(!schema.contains("CREATE TRIGGER"));
    }
}
