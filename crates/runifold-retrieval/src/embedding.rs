use std::{future::Future, pin::Pin};

use runifold_core::Usage;

use crate::{RetrievalContext, RetrievalError};

/// Boxed asynchronous result returned by embedding providers.
#[cfg(not(target_arch = "wasm32"))]
pub type EmbeddingFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Boxed asynchronous embedding result on single-threaded WASM.
#[cfg(target_arch = "wasm32")]
pub type EmbeddingFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Semantic purpose supplied to embedding providers that support task tuning.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum EmbeddingTask {
    /// No task-specific optimization was requested.
    #[default]
    Unspecified,
    /// A search query that will be compared with document embeddings.
    RetrievalQuery,
    /// Corpus content that will be searched by retrieval queries.
    RetrievalDocument,
    /// General semantic similarity comparison.
    SemanticSimilarity,
    /// Classification input.
    Classification,
    /// Clustering input.
    Clustering,
}

/// Validated ordered embedding input with explicit semantic purpose.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EmbeddingRequest {
    inputs: Vec<String>,
    task: EmbeddingTask,
}

impl EmbeddingRequest {
    /// Creates an embedding request.
    ///
    /// An empty batch is valid and permits callers to avoid a provider
    /// round-trip. Individual blank inputs are rejected.
    ///
    /// # Errors
    ///
    /// Returns [`RetrievalError::EmptyEmbeddingInput`] for a blank item.
    pub fn new(inputs: Vec<String>, task: EmbeddingTask) -> Result<Self, RetrievalError> {
        if let Some(index) = inputs.iter().position(|input| input.trim().is_empty()) {
            return Err(RetrievalError::EmptyEmbeddingInput { index });
        }
        Ok(Self { inputs, task })
    }

    /// Returns the ordered input strings.
    pub fn inputs(&self) -> &[String] {
        &self.inputs
    }

    /// Returns the semantic purpose.
    pub const fn task(&self) -> EmbeddingTask {
        self.task
    }

    /// Consumes the request into its provider-adapter parts.
    pub fn into_parts(self) -> (Vec<String>, EmbeddingTask) {
        (self.inputs, self.task)
    }
}

/// Validated dense vector.
#[derive(Clone, Debug, PartialEq)]
pub struct Embedding {
    values: Vec<f64>,
    squared_norm: f64,
}

impl Embedding {
    /// Validates and creates a dense vector.
    ///
    /// # Errors
    ///
    /// Rejects empty, non-finite, and zero-norm vectors.
    pub fn new(values: Vec<f64>) -> Result<Self, RetrievalError> {
        if values.is_empty() {
            return Err(RetrievalError::EmptyEmbedding);
        }
        if let Some(index) = values.iter().position(|value| !value.is_finite()) {
            return Err(RetrievalError::NonFiniteEmbedding { index });
        }
        let squared_norm = values.iter().map(|value| value.powi(2)).sum::<f64>();
        if squared_norm == 0.0 {
            return Err(RetrievalError::ZeroNormEmbedding);
        }
        Ok(Self {
            values,
            squared_norm,
        })
    }

    /// Returns the vector coordinates.
    pub fn values(&self) -> &[f64] {
        &self.values
    }

    /// Returns the number of coordinates.
    pub fn dimensions(&self) -> usize {
        self.values.len()
    }

    pub(crate) fn squared_norm(&self) -> f64 {
        self.squared_norm
    }
}

/// Ordered vectors and usage returned for one embedding batch.
#[derive(Clone, Debug, PartialEq)]
pub struct EmbeddingBatch {
    /// One vector per input in the same order.
    pub embeddings: Vec<Embedding>,
    /// Provider-reported token, cost, and duration usage.
    pub usage: Usage,
}

impl EmbeddingBatch {
    /// Validates the vector count for an input batch.
    ///
    /// # Errors
    ///
    /// Returns [`RetrievalError::EmbeddingCountMismatch`] when provider output
    /// cannot be correlated one-to-one with inputs.
    pub fn validate_count(self, expected: usize) -> Result<Self, RetrievalError> {
        let actual = self.embeddings.len();
        if actual != expected {
            return Err(RetrievalError::EmbeddingCountMismatch { expected, actual });
        }
        Ok(self)
    }
}

/// Object-safe boundary implemented by embedding provider adapters.
pub trait EmbeddingModel: Send + Sync {
    /// Embeds an ordered text batch.
    ///
    /// Implementations must return exactly one vector per input and must
    /// observe cancellation and deadlines from `context`.
    fn embed(
        &self,
        request: EmbeddingRequest,
        context: RetrievalContext,
    ) -> EmbeddingFuture<'_, Result<EmbeddingBatch, RetrievalError>>;
}
