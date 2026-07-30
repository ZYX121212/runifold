//! Provider-neutral embedding and retrieval contracts for Runifold.
//!
//! This crate defines an edge boundary rather than a vector database or an
//! opinionated RAG architecture. Applications can replace the reference
//! in-memory index without changing Agent execution semantics.

mod context;
mod document;
mod embedding;
mod error;
mod memory;
mod retriever;
mod vector_store;

pub use context::RetrievalContext;
pub use document::{Document, DocumentId};
pub use embedding::{
    Embedding, EmbeddingBatch, EmbeddingFuture, EmbeddingModel, EmbeddingRequest, EmbeddingTask,
};
pub use error::RetrievalError;
pub use memory::{InMemoryVectorIndex, IndexBuildOutcome};
pub use retriever::{
    RetrievalFuture, RetrievalQuery, RetrievalResponse, RetrievedDocument, Retriever,
    RetrieverDescriptor,
};
pub use vector_store::{
    VectorRecord, VectorRetriever, VectorSearchResponse, VectorSearchResult, VectorStore,
    VectorStoreFuture, VectorUpsertOutcome,
};
