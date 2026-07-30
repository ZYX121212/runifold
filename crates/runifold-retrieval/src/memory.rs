use std::{collections::BTreeSet, sync::Arc};

use runifold_core::Usage;

use crate::{
    Document, Embedding, EmbeddingModel, EmbeddingRequest, EmbeddingTask, RetrievalContext,
    RetrievalError, RetrievalFuture, RetrievalQuery, RetrievalResponse, RetrievedDocument,
    Retriever, RetrieverDescriptor,
};

#[derive(Clone, Debug)]
struct EmbeddedDocument {
    document: Document,
    embedding: Embedding,
}

/// Result of constructing an in-memory vector index.
#[derive(Clone, Debug)]
pub struct IndexBuildOutcome {
    /// Immutable reference index.
    pub index: InMemoryVectorIndex,
    /// Embedding usage attributable to index construction.
    pub usage: Usage,
}

/// Deterministic immutable cosine-similarity reference index.
///
/// This type is intended for tests, small local datasets, and adapter
/// conformance. It is not presented as a vector database.
#[derive(Clone)]
pub struct InMemoryVectorIndex {
    descriptor: RetrieverDescriptor,
    embedder: Arc<dyn EmbeddingModel>,
    entries: Arc<Vec<EmbeddedDocument>>,
    dimensions: usize,
}

impl InMemoryVectorIndex {
    /// Embeds documents and constructs an immutable index.
    ///
    /// # Errors
    ///
    /// Rejects duplicate identities, provider count mismatches, mixed vector
    /// dimensions, cancellation, and deadline expiry.
    pub fn build(
        name: impl Into<String>,
        embedder: Arc<dyn EmbeddingModel>,
        documents: Vec<Document>,
        context: RetrievalContext,
    ) -> RetrievalFuture<'static, Result<IndexBuildOutcome, RetrievalError>> {
        let name = name.into();
        Box::pin(async move {
            context.check_live()?;
            let mut identities = BTreeSet::new();
            for document in &documents {
                if !identities.insert(document.id.clone()) {
                    return Err(RetrievalError::DuplicateDocument(document.id.clone()));
                }
            }
            let inputs = documents
                .iter()
                .map(|document| document.text.clone())
                .collect::<Vec<_>>();
            let request = EmbeddingRequest::new(inputs, EmbeddingTask::RetrievalDocument)?;
            let batch = embedder
                .embed(request, context.child_attempt())
                .await?
                .validate_count(documents.len())?;
            context.check_live()?;

            let dimensions = batch.embeddings.first().map_or(0, Embedding::dimensions);
            for embedding in &batch.embeddings {
                if embedding.dimensions() != dimensions {
                    return Err(RetrievalError::DimensionMismatch {
                        expected: dimensions,
                        actual: embedding.dimensions(),
                    });
                }
            }
            let entries = documents
                .into_iter()
                .zip(batch.embeddings)
                .map(|(document, embedding)| EmbeddedDocument {
                    document,
                    embedding,
                })
                .collect::<Vec<_>>();
            Ok(IndexBuildOutcome {
                index: Self {
                    descriptor: RetrieverDescriptor::read_only(name),
                    embedder,
                    entries: Arc::new(entries),
                    dimensions,
                },
                usage: batch.usage,
            })
        })
    }

    /// Returns the indexed document count.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether the index contains no documents.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the fixed vector dimension, or zero for an empty index.
    pub const fn dimensions(&self) -> usize {
        self.dimensions
    }
}

impl Retriever for InMemoryVectorIndex {
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
            if self.entries.is_empty() {
                return Ok(RetrievalResponse::default());
            }
            let mut batch = self
                .embedder
                .embed(
                    EmbeddingRequest::new(vec![query.text], EmbeddingTask::RetrievalQuery)?,
                    context.child_attempt(),
                )
                .await?
                .validate_count(1)?;
            context.check_live()?;
            let query_embedding =
                batch
                    .embeddings
                    .pop()
                    .ok_or(RetrievalError::EmbeddingCountMismatch {
                        expected: 1,
                        actual: 0,
                    })?;
            if query_embedding.dimensions() != self.dimensions {
                return Err(RetrievalError::DimensionMismatch {
                    expected: self.dimensions,
                    actual: query_embedding.dimensions(),
                });
            }

            let mut documents = self
                .entries
                .iter()
                .map(|entry| RetrievedDocument {
                    document: entry.document.clone(),
                    score: cosine_similarity(&query_embedding, &entry.embedding),
                })
                .collect::<Vec<_>>();
            documents.sort_by(|left, right| {
                right
                    .score
                    .total_cmp(&left.score)
                    .then_with(|| left.document.id.cmp(&right.document.id))
            });
            documents.truncate(query.limit);
            Ok(RetrievalResponse {
                documents,
                usage: batch.usage,
            })
        })
    }
}

impl std::fmt::Debug for InMemoryVectorIndex {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InMemoryVectorIndex")
            .field("descriptor", &self.descriptor)
            .field("documents", &self.entries.len())
            .field("dimensions", &self.dimensions)
            .finish_non_exhaustive()
    }
}

fn cosine_similarity(left: &Embedding, right: &Embedding) -> f64 {
    let dot = left
        .values()
        .iter()
        .zip(right.values())
        .map(|(left, right)| left * right)
        .sum::<f64>();
    dot / (left.squared_norm() * right.squared_norm()).sqrt()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::{EmbeddingBatch, EmbeddingFuture};

    #[derive(Debug)]
    struct KeywordEmbedder;

    impl EmbeddingModel for KeywordEmbedder {
        fn embed(
            &self,
            request: EmbeddingRequest,
            context: RetrievalContext,
        ) -> EmbeddingFuture<'_, Result<EmbeddingBatch, RetrievalError>> {
            Box::pin(async move {
                context.check_live()?;
                let embeddings = request
                    .into_parts()
                    .0
                    .into_iter()
                    .map(|input| {
                        let lower = input.to_lowercase();
                        Embedding::new(vec![
                            if lower.contains("rust") { 1.0 } else { 0.0 },
                            if lower.contains("python") { 1.0 } else { 0.0 },
                            1.0,
                        ])
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(EmbeddingBatch {
                    embeddings,
                    usage: Usage::default(),
                })
            })
        }
    }

    #[derive(Debug)]
    struct TaskRecordingEmbedder {
        tasks: Arc<Mutex<Vec<EmbeddingTask>>>,
    }

    impl EmbeddingModel for TaskRecordingEmbedder {
        fn embed(
            &self,
            request: EmbeddingRequest,
            _context: RetrievalContext,
        ) -> EmbeddingFuture<'_, Result<EmbeddingBatch, RetrievalError>> {
            self.tasks.lock().unwrap().push(request.task());
            Box::pin(async move {
                let embeddings = request
                    .inputs()
                    .iter()
                    .map(|_| Embedding::new(vec![1.0]))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(EmbeddingBatch {
                    embeddings,
                    usage: Usage::default(),
                })
            })
        }
    }

    #[test]
    fn index_returns_stable_similarity_order() {
        let documents = vec![
            Document::new("rust", "Rust ownership and borrowing").unwrap(),
            Document::new("python", "Python dynamic typing").unwrap(),
            Document::new("mixed", "Rust and Python interop").unwrap(),
        ];
        let built = futures_executor::block_on(InMemoryVectorIndex::build(
            "docs",
            Arc::new(KeywordEmbedder),
            documents,
            RetrievalContext::new(),
        ))
        .unwrap();
        let response = futures_executor::block_on(built.index.retrieve(
            RetrievalQuery::new("Rust memory safety", 2).unwrap(),
            RetrievalContext::new(),
        ))
        .unwrap();

        assert_eq!(response.documents.len(), 2);
        assert_eq!(response.documents[0].document.id.as_str(), "rust");
        assert_eq!(response.documents[1].document.id.as_str(), "mixed");
    }

    #[test]
    fn index_distinguishes_document_and_query_embedding_tasks() {
        let tasks = Arc::new(Mutex::new(Vec::new()));
        let embedder = Arc::new(TaskRecordingEmbedder {
            tasks: tasks.clone(),
        });
        let built = futures_executor::block_on(InMemoryVectorIndex::build(
            "docs",
            embedder,
            vec![Document::new("one", "document").unwrap()],
            RetrievalContext::new(),
        ))
        .unwrap();

        futures_executor::block_on(built.index.retrieve(
            RetrievalQuery::new("query", 1).unwrap(),
            RetrievalContext::new(),
        ))
        .unwrap();

        assert_eq!(
            *tasks.lock().unwrap(),
            vec![
                EmbeddingTask::RetrievalDocument,
                EmbeddingTask::RetrievalQuery
            ]
        );
    }

    #[test]
    fn index_rejects_duplicate_documents_before_embedding() {
        let documents = vec![
            Document::new("same", "first").unwrap(),
            Document::new("same", "second").unwrap(),
        ];
        let error = futures_executor::block_on(InMemoryVectorIndex::build(
            "docs",
            Arc::new(KeywordEmbedder),
            documents,
            RetrievalContext::new(),
        ))
        .unwrap_err();

        assert_eq!(
            error,
            RetrievalError::DuplicateDocument(crate::DocumentId::new("same").unwrap())
        );
    }

    #[test]
    fn cancelled_query_stops_before_embedding() {
        let context = RetrievalContext::new();
        context.cancellation().cancel();
        let error = futures_executor::block_on(InMemoryVectorIndex::build(
            "docs",
            Arc::new(KeywordEmbedder),
            vec![Document::new("rust", "Rust").unwrap()],
            context,
        ))
        .unwrap_err();

        assert_eq!(error, RetrievalError::Cancelled);
    }
}
