//! PostgreSQL/pgvector adapter for Runifold's vector-store boundary.

use std::{future::Future, num::NonZeroU32, sync::Arc, time::Instant};

use futures_timer::Delay;
use futures_util::future::{Either, select};
use pgvector::Vector;
use runifold_core::Usage;
use runifold_retrieval::{
    Document, Embedding, RetrievalContext, RetrievalError, VectorRecord, VectorSearchResponse,
    VectorSearchResult, VectorStore, VectorStoreFuture, VectorUpsertOutcome,
};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};

/// Invalid pgvector configuration or connection.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PgVectorError {
    /// The table identifier was empty or unsafe for SQL interpolation.
    #[error("pgvector table must be a simple PostgreSQL identifier")]
    InvalidTable,
    /// `PostgreSQL` connection or setup failed.
    #[error("pgvector database operation failed: {0}")]
    Database(#[from] tokio_postgres::Error),
}

/// PostgreSQL/pgvector persistence bound to one validated table.
#[derive(Clone, Debug)]
pub struct PgVectorStore {
    client: Arc<Mutex<Client>>,
    table: String,
}

impl PgVectorStore {
    /// Connects to `PostgreSQL` without modifying schema.
    ///
    /// # Errors
    ///
    /// Rejects an unsafe table identifier and propagates connection failures.
    pub async fn connect(connection: &str, table: &str) -> Result<Self, PgVectorError> {
        validate_identifier(table)?;
        let (client, connection) = tokio_postgres::connect(connection, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            table: table.into(),
        })
    }

    /// Creates the vector extension and document table explicitly.
    ///
    /// Query and upsert operations never create or migrate schema.
    ///
    /// # Errors
    ///
    /// Propagates `PostgreSQL` extension or DDL failures.
    pub async fn ensure_schema(&self, dimensions: NonZeroU32) -> Result<(), PgVectorError> {
        let client = self.client.lock().await;
        client
            .batch_execute(&format!(
                "CREATE EXTENSION IF NOT EXISTS vector;\
                 CREATE TABLE IF NOT EXISTS {} (\
                   document_id TEXT PRIMARY KEY,\
                   text TEXT NOT NULL,\
                   metadata JSONB NOT NULL,\
                   embedding vector({}) NOT NULL\
                 );",
                self.table,
                dimensions.get()
            ))
            .await?;
        Ok(())
    }

    /// Creates a cosine HNSW index explicitly.
    ///
    /// # Errors
    ///
    /// Propagates `PostgreSQL` DDL failures.
    pub async fn ensure_hnsw_index(&self) -> Result<(), PgVectorError> {
        let client = self.client.lock().await;
        client
            .batch_execute(&format!(
                "CREATE INDEX IF NOT EXISTS {0}_embedding_hnsw \
                 ON {0} USING hnsw (embedding vector_cosine_ops);",
                self.table
            ))
            .await?;
        Ok(())
    }
}

impl VectorStore for PgVectorStore {
    fn upsert(
        &self,
        records: Vec<VectorRecord>,
        context: RetrievalContext,
    ) -> VectorStoreFuture<'_, Result<VectorUpsertOutcome, RetrievalError>> {
        Box::pin(async move {
            context.check_live()?;
            if records.is_empty() {
                return Ok(VectorUpsertOutcome::default());
            }
            let started = Instant::now();
            let statement = format!(
                "INSERT INTO {} (document_id, text, metadata, embedding) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (document_id) DO UPDATE SET \
                   text = EXCLUDED.text, metadata = EXCLUDED.metadata, \
                   embedding = EXCLUDED.embedding",
                self.table
            );
            let records = records
                .into_iter()
                .map(|record| {
                    Ok((
                        record.document.id.to_string(),
                        record.document.text,
                        Value::Object(record.document.metadata.into_iter().collect()),
                        to_pgvector(&record.embedding)?,
                    ))
                })
                .collect::<Result<Vec<_>, RetrievalError>>()?;
            let operation = async {
                let mut client = self.client.lock().await;
                let transaction = client.transaction().await?;
                for (id, text, metadata, vector) in records {
                    transaction
                        .execute(&statement, &[&id, &text, &metadata, &vector])
                        .await?;
                }
                transaction.commit().await
            };
            scoped_database(operation, &context).await?;
            Ok(VectorUpsertOutcome {
                usage: duration_usage(started),
            })
        })
    }

    fn search(
        &self,
        query: Embedding,
        limit: usize,
        context: RetrievalContext,
    ) -> VectorStoreFuture<'_, Result<VectorSearchResponse, RetrievalError>> {
        Box::pin(async move {
            context.check_live()?;
            if limit == 0 {
                return Err(RetrievalError::ZeroLimit);
            }
            let started = Instant::now();
            let vector = to_pgvector(&query)?;
            let limit = i64::try_from(limit)
                .map_err(|_| RetrievalError::provider("pgvector limit exceeds i64"))?;
            let statement = format!(
                "SELECT document_id, text, metadata, \
                   1 - (embedding <=> $1) AS score \
                 FROM {} ORDER BY embedding <=> $1 LIMIT $2",
                self.table
            );
            let operation = async {
                let client = self.client.lock().await;
                client.query(&statement, &[&vector, &limit]).await
            };
            let rows = scoped_database(operation, &context).await?;
            let results = rows
                .into_iter()
                .map(|row| {
                    let id: String = row.get(0);
                    let text: String = row.get(1);
                    let metadata: Value = row.get(2);
                    let score: f64 = row.get(3);
                    let mut document = Document::new(id, text)?;
                    if let Value::Object(metadata) = metadata {
                        document.metadata = metadata.into_iter().collect();
                    }
                    Ok(VectorSearchResult { document, score })
                })
                .collect::<Result<Vec<_>, RetrievalError>>()?;
            Ok(VectorSearchResponse {
                results,
                usage: duration_usage(started),
            })
        })
    }
}

fn validate_identifier(identifier: &str) -> Result<(), PgVectorError> {
    let mut characters = identifier.chars();
    let Some(first) = characters.next() else {
        return Err(PgVectorError::InvalidTable);
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || !characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        return Err(PgVectorError::InvalidTable);
    }
    Ok(())
}

#[allow(clippy::cast_possible_truncation)]
fn to_pgvector(embedding: &Embedding) -> Result<Vector, RetrievalError> {
    let values = embedding
        .values()
        .iter()
        .enumerate()
        .map(|(index, value)| {
            if value.abs() <= f64::from(f32::MAX) {
                let value = *value as f32;
                Ok(value)
            } else {
                Err(RetrievalError::EmbeddingCoordinateOutOfRange { index })
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Vector::from(values))
}

async fn scoped_database<T, F>(
    operation: F,
    context: &RetrievalContext,
) -> Result<T, RetrievalError>
where
    F: Future<Output = Result<T, tokio_postgres::Error>> + Send,
    T: Send,
{
    let timed = async {
        if let Some(remaining) = context.remaining() {
            match select(Box::pin(operation), Box::pin(Delay::new(remaining))).await {
                Either::Left((result, _)) => result,
                Either::Right(_) => return Err(RetrievalError::DeadlineExceeded),
            }
        } else {
            operation.await
        }
        .map_err(|error| RetrievalError::provider(format!("pgvector operation failed: {error}")))
    };
    match select(
        Box::pin(context.cancellation().cancelled()),
        Box::pin(timed),
    )
    .await
    {
        Either::Left(_) => Err(RetrievalError::Cancelled),
        Either::Right((result, _)) => result,
    }
}

fn duration_usage(started: Instant) -> Usage {
    Usage {
        duration_micros: u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        ..Usage::default()
    }
}

#[cfg(test)]
mod tests {
    use super::{PgVectorError, validate_identifier};

    #[test]
    fn table_identifiers_are_restricted_before_sql_construction() {
        assert!(validate_identifier("runifold_documents").is_ok());
        assert!(matches!(
            validate_identifier("docs; DROP TABLE users"),
            Err(PgVectorError::InvalidTable)
        ));
    }
}
