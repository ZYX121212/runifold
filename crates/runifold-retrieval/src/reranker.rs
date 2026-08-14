//! Provider-neutral reranking and Retriever composition.

use std::{collections::BTreeSet, sync::Arc};

use runifold_core::Usage;

use crate::{
    RetrievalContext, RetrievalError, RetrievalFuture, RetrievalQuery, RetrievalResponse,
    RetrievedDocument, Retriever, RetrieverDescriptor,
};

const MAX_RERANK_CANDIDATES: usize = 1_000;

/// Stable operator identity for one reranker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RerankerDescriptor {
    /// Operator-visible name.
    pub name: String,
    /// Semantic contract version.
    pub version: String,
}

impl RerankerDescriptor {
    /// Creates a version-one descriptor.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: "1".into(),
        }
    }
}

/// Validated reranking input.
#[derive(Clone, Debug, PartialEq)]
pub struct RerankRequest {
    /// Original user query.
    pub query: String,
    /// Candidate documents in first-stage order.
    pub candidates: Vec<RetrievedDocument>,
    /// Maximum final results.
    pub limit: usize,
}

impl RerankRequest {
    /// Creates a bounded reranking request.
    ///
    /// # Errors
    ///
    /// Rejects blank queries, zero limits, excessive candidates, or a limit
    /// larger than the candidate set.
    pub fn new(
        query: impl Into<String>,
        candidates: Vec<RetrievedDocument>,
        limit: usize,
    ) -> Result<Self, RetrievalError> {
        let query = query.into();
        if query.trim().is_empty() {
            return Err(RetrievalError::EmptyQuery);
        }
        if limit == 0 {
            return Err(RetrievalError::ZeroLimit);
        }
        if candidates.len() > MAX_RERANK_CANDIDATES || limit > candidates.len() {
            return Err(RetrievalError::InvalidRerankCandidateLimit);
        }
        Ok(Self {
            query,
            candidates,
            limit,
        })
    }
}

/// Validated reranking output and attributable usage.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RerankResponse {
    /// Final descending result order.
    pub documents: Vec<RetrievedDocument>,
    /// Provider usage attributable to reranking.
    pub usage: Usage,
}

/// Object-safe reranking boundary.
pub trait Reranker: Send + Sync {
    /// Returns stable reranker identity.
    fn descriptor(&self) -> &RerankerDescriptor;

    /// Reranks a validated candidate set.
    fn rerank(
        &self,
        request: RerankRequest,
        context: RetrievalContext,
    ) -> RetrievalFuture<'_, Result<RerankResponse, RetrievalError>>;
}

/// Two-stage Retriever that expands candidates then applies one reranker.
pub struct RerankingRetriever {
    descriptor: RetrieverDescriptor,
    retriever: Arc<dyn Retriever>,
    reranker: Arc<dyn Reranker>,
    candidate_multiplier: usize,
}

impl RerankingRetriever {
    /// Composes first-stage retrieval with a reranker.
    ///
    /// # Errors
    ///
    /// Rejects a zero candidate multiplier.
    pub fn new(
        name: impl Into<String>,
        retriever: Arc<dyn Retriever>,
        reranker: Arc<dyn Reranker>,
        candidate_multiplier: usize,
    ) -> Result<Self, RetrievalError> {
        if candidate_multiplier == 0 {
            return Err(RetrievalError::InvalidRerankCandidateLimit);
        }
        Ok(Self {
            descriptor: RetrieverDescriptor::read_only(name),
            retriever,
            reranker,
            candidate_multiplier,
        })
    }
}

impl Retriever for RerankingRetriever {
    fn descriptor(&self) -> &RetrieverDescriptor {
        &self.descriptor
    }

    fn retrieve(
        &self,
        query: RetrievalQuery,
        context: RetrievalContext,
    ) -> RetrievalFuture<'_, Result<RetrievalResponse, RetrievalError>> {
        Box::pin(async move {
            let candidate_limit = query
                .limit
                .checked_mul(self.candidate_multiplier)
                .filter(|limit| *limit <= MAX_RERANK_CANDIDATES)
                .ok_or(RetrievalError::InvalidRerankCandidateLimit)?;
            let first_stage = self
                .retriever
                .retrieve(
                    RetrievalQuery::new(query.text.clone(), candidate_limit)?,
                    context.clone(),
                )
                .await?;
            if first_stage.documents.is_empty() {
                return Ok(first_stage);
            }
            let allowed = first_stage
                .documents
                .iter()
                .map(|candidate| candidate.document.id.clone())
                .collect::<BTreeSet<_>>();
            let response = self
                .reranker
                .rerank(
                    RerankRequest::new(query.text, first_stage.documents, query.limit)?,
                    context,
                )
                .await?;
            validate_response(&response, &allowed, query.limit)?;
            Ok(RetrievalResponse {
                documents: response.documents,
                usage: add_usage(first_stage.usage, response.usage)?,
            })
        })
    }
}

impl std::fmt::Debug for RerankingRetriever {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RerankingRetriever")
            .field("descriptor", &self.descriptor)
            .field("reranker", self.reranker.descriptor())
            .field("candidate_multiplier", &self.candidate_multiplier)
            .finish_non_exhaustive()
    }
}

fn validate_response(
    response: &RerankResponse,
    allowed: &BTreeSet<crate::DocumentId>,
    limit: usize,
) -> Result<(), RetrievalError> {
    if response.documents.len() > limit {
        return Err(invalid_output("result count exceeds the requested limit"));
    }
    let mut seen = BTreeSet::new();
    for result in &response.documents {
        if !result.score.is_finite() {
            return Err(invalid_output("score must be finite"));
        }
        if !allowed.contains(&result.document.id) {
            return Err(invalid_output("result contains a foreign document"));
        }
        if !seen.insert(result.document.id.clone()) {
            return Err(invalid_output("result contains a duplicate document"));
        }
    }
    Ok(())
}

fn invalid_output(message: &str) -> RetrievalError {
    RetrievalError::InvalidRerankOutput {
        message: message.into(),
    }
}

fn add_usage(left: Usage, right: Usage) -> Result<Usage, RetrievalError> {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use runifold_core::Usage;

    use crate::{
        Document, RetrievalContext, RetrievalError, RetrievalFuture, RetrievalQuery,
        RetrievalResponse, RetrievedDocument, Retriever, RetrieverDescriptor,
    };

    use super::{RerankRequest, RerankResponse, Reranker, RerankerDescriptor, RerankingRetriever};

    struct FixedRetriever {
        descriptor: RetrieverDescriptor,
        documents: Vec<RetrievedDocument>,
    }

    impl Retriever for FixedRetriever {
        fn descriptor(&self) -> &RetrieverDescriptor {
            &self.descriptor
        }

        fn retrieve(
            &self,
            _query: RetrievalQuery,
            _context: RetrievalContext,
        ) -> RetrievalFuture<'_, Result<RetrievalResponse, RetrievalError>> {
            let documents = self.documents.clone();
            Box::pin(async move {
                Ok(RetrievalResponse {
                    documents,
                    usage: Usage {
                        tokens: 2,
                        ..Usage::default()
                    },
                })
            })
        }
    }

    struct ReverseReranker {
        descriptor: RerankerDescriptor,
    }

    impl Reranker for ReverseReranker {
        fn descriptor(&self) -> &RerankerDescriptor {
            &self.descriptor
        }

        fn rerank(
            &self,
            mut request: RerankRequest,
            _context: RetrievalContext,
        ) -> RetrievalFuture<'_, Result<RerankResponse, RetrievalError>> {
            request.candidates.reverse();
            request.candidates.truncate(request.limit);
            Box::pin(async move {
                Ok(RerankResponse {
                    documents: request.candidates,
                    usage: Usage {
                        tokens: 3,
                        ..Usage::default()
                    },
                })
            })
        }
    }

    #[test]
    fn composed_reranker_preserves_ids_order_and_usage() {
        let documents = ["a", "b", "c"]
            .into_iter()
            .enumerate()
            .map(|(index, id)| RetrievedDocument {
                document: Document::new(id, format!("document {id}")).unwrap(),
                score: [1.0, 0.9, 0.8][index],
            })
            .collect();
        let retriever = Arc::new(FixedRetriever {
            descriptor: RetrieverDescriptor::read_only("fixed"),
            documents,
        });
        let reranker = Arc::new(ReverseReranker {
            descriptor: RerankerDescriptor::new("reverse"),
        });
        let composed = RerankingRetriever::new("composed", retriever, reranker, 2).unwrap();

        let response = futures_executor::block_on(composed.retrieve(
            RetrievalQuery::new("query", 2).unwrap(),
            RetrievalContext::new(),
        ))
        .unwrap();

        assert_eq!(response.documents[0].document.id.as_str(), "c");
        assert_eq!(response.documents[1].document.id.as_str(), "b");
        assert_eq!(response.usage.tokens, 5);
    }
}
