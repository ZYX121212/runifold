use std::{collections::BTreeMap, future::Future, pin::Pin};

use runifold_core::{
    CapabilityDescriptor, CapabilityId, CapabilityKind, EffectClass, RiskLevel, Usage,
};
use serde_json::{Value, json};

use crate::{Document, RetrievalContext, RetrievalError};

/// Boxed asynchronous result returned by retrieval backends.
#[cfg(not(target_arch = "wasm32"))]
pub type RetrievalFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Boxed asynchronous retrieval result on single-threaded WASM.
#[cfg(target_arch = "wasm32")]
pub type RetrievalFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Versioned description and authority identity of one retriever.
#[derive(Clone, Debug, PartialEq)]
pub struct RetrieverDescriptor {
    /// Stable capability identity.
    pub id: CapabilityId,
    /// Operator-visible source name.
    pub name: String,
    /// Semantic contract version.
    pub version: String,
    /// External-effect classification.
    pub effect: EffectClass,
    /// Risk classification.
    pub risk: RiskLevel,
    /// Host-only metadata.
    pub metadata: BTreeMap<String, Value>,
}

impl RetrieverDescriptor {
    /// Creates a read-only retrieval descriptor.
    pub fn read_only(name: impl Into<String>) -> Self {
        Self {
            id: CapabilityId::new(),
            name: name.into(),
            version: "1".into(),
            effect: EffectClass::ReadOnly,
            risk: RiskLevel::Low,
            metadata: BTreeMap::new(),
        }
    }

    /// Converts this descriptor into a grantable runtime capability.
    pub fn capability(&self) -> CapabilityDescriptor {
        CapabilityDescriptor {
            id: self.id,
            name: self.name.clone(),
            version: self.version.clone(),
            kind: CapabilityKind::Extension("runifold.retrieval".into()),
            input_schema: json!({
                "type": "object",
                "required": ["query", "limit"],
                "properties": {
                    "query": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1}
                }
            }),
            output_schema: json!({
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["id", "score"],
                    "properties": {
                        "id": {"type": "string"},
                        "score": {"type": "number"}
                    }
                }
            }),
            effect: self.effect,
            risk: self.risk,
            metadata: self.metadata.clone(),
        }
    }
}

/// Validated semantic retrieval query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetrievalQuery {
    /// Text embedded or otherwise matched by the backend.
    pub text: String,
    /// Maximum number of results.
    pub limit: usize,
}

impl RetrievalQuery {
    /// Validates and creates a query.
    ///
    /// # Errors
    ///
    /// Rejects blank text and a zero result limit.
    pub fn new(text: impl Into<String>, limit: usize) -> Result<Self, RetrievalError> {
        let text = text.into();
        if text.trim().is_empty() {
            return Err(RetrievalError::EmptyQuery);
        }
        if limit == 0 {
            return Err(RetrievalError::ZeroLimit);
        }
        Ok(Self { text, limit })
    }
}

/// One scored retrieval result.
#[derive(Clone, Debug, PartialEq)]
pub struct RetrievedDocument {
    /// Complete host document.
    pub document: Document,
    /// Backend similarity score; larger values rank first.
    pub score: f64,
}

/// Ordered retrieval results and attributable usage.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RetrievalResponse {
    /// Stable descending result order.
    pub documents: Vec<RetrievedDocument>,
    /// Embedding/backend usage attributable to this query.
    pub usage: Usage,
}

/// Object-safe semantic retrieval boundary.
pub trait Retriever: Send + Sync {
    /// Returns the source identity and capability descriptor.
    fn descriptor(&self) -> &RetrieverDescriptor;

    /// Retrieves documents for a validated query.
    fn retrieve(
        &self,
        query: RetrievalQuery,
        context: RetrievalContext,
    ) -> RetrievalFuture<'_, Result<RetrievalResponse, RetrievalError>>;
}
