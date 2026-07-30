//! Replaceable vector persistence and retrieval composition.

use std::{future::Future, pin::Pin, sync::Arc};

use runifold_core::Usage;

use crate::{
    Document, Embedding, EmbeddingModel, EmbeddingRequest, EmbeddingTask, RetrievalContext,
    RetrievalError, RetrievalFuture, RetrievalQuery, RetrievalResponse, RetrievedDocument,
    Retriever, RetrieverDescriptor,
};

/// Boxed asynchronous result returned by vector stores.
#[cfg(not(target_arch = "wasm32"))]
pub type VectorStoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Boxed asynchronous vector-store result on single-threaded WASM.
#[cfg(target_arch = "wasm32")]
pub type VectorStoreFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// One document and its validated dense vector.
#[derive(Clone, Debug, PartialEq)]
pub struct VectorRecord {
    /// Application-owned document.
    pub document: Document,
    /// Vector generated for the document text.
    pub embedding: Embedding,
}

/// One ordered vector-search result.
#[derive(Clone, Debug, PartialEq)]
pub struct VectorSearchResult {
    /// Application-owned document reconstructed from storage.
    pub document: Document,
    /// Backend similarity score, larger values first.
    pub score: f64,
}

/// Backend search results and attributable non-embedding usage.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VectorSearchResponse {
    /// Results in descending relevance order.
    pub results: Vec<VectorSearchResult>,
    /// Backend duration or cost, excluding query embedding usage.
    pub usage: Usage,
}

/// Outcome of an explicit vector-store write.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VectorUpsertOutcome {
    /// Backend duration or cost, excluding document embedding usage.
    pub usage: Usage,
}

/// Object-safe vector persistence boundary.
pub trait VectorStore: Send + Sync {
    /// Creates or replaces records by document identity.
    fn upsert(
        &self,
        records: Vec<VectorRecord>,
        context: RetrievalContext,
    ) -> VectorStoreFuture<'_, Result<VectorUpsertOutcome, RetrievalError>>;

    /// Searches by a validated dense vector.
    fn search(
        &self,
        query: Embedding,
        limit: usize,
        context: RetrievalContext,
    ) -> VectorStoreFuture<'_, Result<VectorSearchResponse, RetrievalError>>;
}

/// Provider-neutral composition of an embedding model and vector store.
#[derive(Clone)]
pub struct VectorRetriever {
    descriptor: RetrieverDescriptor,
    embedder: Arc<dyn EmbeddingModel>,
    store: Arc<dyn VectorStore>,
}

impl VectorRetriever {
    /// Creates one capability-addressable retriever.
    pub fn new(
        name: impl Into<String>,
        embedder: Arc<dyn EmbeddingModel>,
        store: Arc<dyn VectorStore>,
    ) -> Self {
        Self {
            descriptor: RetrieverDescriptor::read_only(name),
            embedder,
            store,
        }
    }

    /// Embeds and explicitly upserts documents.
    pub fn index_documents(
        &self,
        documents: Vec<Document>,
        context: RetrievalContext,
    ) -> RetrievalFuture<'_, Result<Usage, RetrievalError>> {
        Box::pin(async move {
            context.check_live()?;
            let inputs = documents
                .iter()
                .map(|document| document.text.clone())
                .collect::<Vec<_>>();
            let batch = self
                .embedder
                .embed(
                    EmbeddingRequest::new(inputs, EmbeddingTask::RetrievalDocument)?,
                    context.child_attempt(),
                )
                .await?
                .validate_count(documents.len())?;
            let embedding_usage = batch.usage;
            let records = documents
                .into_iter()
                .zip(batch.embeddings)
                .map(|(document, embedding)| VectorRecord {
                    document,
                    embedding,
                })
                .collect();
            let stored = self.store.upsert(records, context.child_attempt()).await?;
            combine_usage(embedding_usage, stored.usage)
        })
    }
}

impl Retriever for VectorRetriever {
    fn descriptor(&self) -> &RetrieverDescriptor {
        &self.descriptor
    }

    fn retrieve(
        &self,
        query: RetrievalQuery,
        context: RetrievalContext,
    ) -> RetrievalFuture<'_, Result<RetrievalResponse, RetrievalError>> {
        Box::pin(async move {
            context.check_live()?;
            let mut batch = self
                .embedder
                .embed(
                    EmbeddingRequest::new(vec![query.text], EmbeddingTask::RetrievalQuery)?,
                    context.child_attempt(),
                )
                .await?
                .validate_count(1)?;
            let query_embedding =
                batch
                    .embeddings
                    .pop()
                    .ok_or(RetrievalError::EmbeddingCountMismatch {
                        expected: 1,
                        actual: 0,
                    })?;
            let searched = self
                .store
                .search(query_embedding, query.limit, context.child_attempt())
                .await?;
            let mut results = searched.results;
            results.truncate(query.limit);
            Ok(RetrievalResponse {
                documents: results
                    .into_iter()
                    .map(|result| RetrievedDocument {
                        document: result.document,
                        score: result.score,
                    })
                    .collect(),
                usage: combine_usage(batch.usage, searched.usage)?,
            })
        })
    }
}

impl std::fmt::Debug for VectorRetriever {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VectorRetriever")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

fn combine_usage(left: Usage, right: Usage) -> Result<Usage, RetrievalError> {
    Ok(Usage {
        tokens: left
            .tokens
            .checked_add(right.tokens)
            .ok_or(RetrievalError::UsageOverflow)?,
        cost_microusd: left
            .cost_microusd
            .checked_add(right.cost_microusd)
            .ok_or(RetrievalError::UsageOverflow)?,
        duration_micros: left
            .duration_micros
            .checked_add(right.duration_micros)
            .ok_or(RetrievalError::UsageOverflow)?,
        turns: left
            .turns
            .checked_add(right.turns)
            .ok_or(RetrievalError::UsageOverflow)?,
        tool_calls: left
            .tool_calls
            .checked_add(right.tool_calls)
            .ok_or(RetrievalError::UsageOverflow)?,
        delegations: left
            .delegations
            .checked_add(right.delegations)
            .ok_or(RetrievalError::UsageOverflow)?,
    })
}
