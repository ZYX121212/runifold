use thiserror::Error;

use crate::DocumentId;

/// Failure at a provider-neutral embedding or retrieval boundary.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum RetrievalError {
    /// A document identity was empty.
    #[error("document id cannot be empty")]
    EmptyDocumentId,
    /// A document contained no usable text.
    #[error("document `{id}` cannot have empty text")]
    EmptyDocumentText {
        /// Invalid document identity.
        id: DocumentId,
    },
    /// A query contained no usable text.
    #[error("retrieval query cannot be empty")]
    EmptyQuery,
    /// A query requested no results.
    #[error("retrieval limit must be greater than zero")]
    ZeroLimit,
    /// An embedding contained no dimensions.
    #[error("embedding cannot be empty")]
    EmptyEmbedding,
    /// An embedding contained a non-finite coordinate.
    #[error("embedding coordinate {index} must be finite")]
    NonFiniteEmbedding {
        /// Zero-based invalid coordinate.
        index: usize,
    },
    /// An embedding coordinate exceeded a backend numeric representation.
    #[error("embedding coordinate {index} is outside the backend numeric range")]
    EmbeddingCoordinateOutOfRange {
        /// Zero-based invalid coordinate.
        index: usize,
    },
    /// Cosine similarity is undefined for a zero vector.
    #[error("embedding must have a non-zero norm")]
    ZeroNormEmbedding,
    /// Embeddings with different dimensions cannot share an index.
    #[error("embedding dimension mismatch: expected {expected}, received {actual}")]
    DimensionMismatch {
        /// Index or batch dimension.
        expected: usize,
        /// Received dimension.
        actual: usize,
    },
    /// A provider returned a different number of vectors than inputs.
    #[error("embedding count mismatch: expected {expected}, received {actual}")]
    EmbeddingCountMismatch {
        /// Input count.
        expected: usize,
        /// Returned vector count.
        actual: usize,
    },
    /// One item in an embedding request was blank.
    #[error("embedding input {index} cannot be empty")]
    EmptyEmbeddingInput {
        /// Zero-based invalid input index.
        index: usize,
    },
    /// A document identity appeared more than once.
    #[error("duplicate document id `{0}`")]
    DuplicateDocument(DocumentId),
    /// A run did not grant the retriever capability.
    #[error("retriever capability `{name}` is not granted")]
    CapabilityDenied {
        /// Retriever name.
        name: String,
    },
    /// The owning run was cancelled.
    #[error("retrieval was cancelled")]
    Cancelled,
    /// The owning run deadline elapsed.
    #[error("retrieval deadline exceeded")]
    DeadlineExceeded,
    /// An embedding provider or retrieval backend failed.
    #[error("retrieval provider failed: {message}")]
    Provider {
        /// Safe provider diagnostic.
        message: String,
    },
    /// Retrieval usage counters could not be combined without overflow.
    #[error("retrieval usage counter overflow")]
    UsageOverflow,
}

impl RetrievalError {
    /// Creates a provider/backend failure without imposing an adapter error
    /// representation.
    pub fn provider(message: impl Into<String>) -> Self {
        Self::Provider {
            message: message.into(),
        }
    }
}
