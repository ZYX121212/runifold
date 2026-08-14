//! Deterministic weighted reciprocal-rank fusion for independent retrievers.

use std::{collections::BTreeMap, sync::Arc};

use runifold_core::Usage;

use crate::{
    DocumentId, RetrievalContext, RetrievalError, RetrievalFuture, RetrievalQuery,
    RetrievalResponse, RetrievedDocument, Retriever, RetrieverDescriptor,
};

const MAX_HYBRID_CANDIDATES: usize = 1_000;

/// Validated weighted reciprocal-rank fusion policy.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ReciprocalRankFusion {
    /// Weight applied to the first retriever.
    pub first_weight: f64,
    /// Weight applied to the second retriever.
    pub second_weight: f64,
    /// Rank smoothing constant, conventionally `60`.
    pub rank_constant: f64,
    /// Candidate expansion applied before fusion.
    pub candidate_multiplier: usize,
}

impl ReciprocalRankFusion {
    /// Creates a bounded weighted fusion policy.
    ///
    /// # Errors
    ///
    /// Rejects non-finite/non-positive weights or rank constants and a zero
    /// candidate multiplier.
    pub fn new(
        first_weight: f64,
        second_weight: f64,
        rank_constant: f64,
        candidate_multiplier: usize,
    ) -> Result<Self, RetrievalError> {
        if !first_weight.is_finite()
            || first_weight <= 0.0
            || !second_weight.is_finite()
            || second_weight <= 0.0
            || !rank_constant.is_finite()
            || rank_constant <= 0.0
            || candidate_multiplier == 0
        {
            return Err(RetrievalError::InvalidHybridConfiguration);
        }
        Ok(Self {
            first_weight,
            second_weight,
            rank_constant,
            candidate_multiplier,
        })
    }
}

/// Concurrently queries two retrievers and fuses their ranks.
pub struct HybridRetriever {
    descriptor: RetrieverDescriptor,
    first: Arc<dyn Retriever>,
    second: Arc<dyn Retriever>,
    policy: ReciprocalRankFusion,
}

impl HybridRetriever {
    /// Creates a two-source hybrid retriever.
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        first: Arc<dyn Retriever>,
        second: Arc<dyn Retriever>,
        policy: ReciprocalRankFusion,
    ) -> Self {
        Self {
            descriptor: RetrieverDescriptor::read_only(name),
            first,
            second,
            policy,
        }
    }
}

impl Retriever for HybridRetriever {
    fn descriptor(&self) -> &RetrieverDescriptor {
        &self.descriptor
    }

    fn retrieve(
        &self,
        query: RetrievalQuery,
        context: RetrievalContext,
    ) -> RetrievalFuture<'_, Result<RetrievalResponse, RetrievalError>> {
        Box::pin(async move {
            let final_limit = query.limit;
            let candidate_limit = final_limit
                .checked_mul(self.policy.candidate_multiplier)
                .filter(|limit| *limit <= MAX_HYBRID_CANDIDATES)
                .ok_or(RetrievalError::InvalidHybridConfiguration)?;
            let expanded = RetrievalQuery::new(query.text, candidate_limit)?;
            let first = self.first.retrieve(expanded.clone(), context.clone());
            let second = self.second.retrieve(expanded, context);
            let (first, second) = futures_util::try_join!(first, second)?;
            let documents = fuse(first.documents, second.documents, final_limit, self.policy)?;
            Ok(RetrievalResponse {
                documents,
                usage: add_usage(first.usage, second.usage)?,
            })
        })
    }
}

impl std::fmt::Debug for HybridRetriever {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HybridRetriever")
            .field("descriptor", &self.descriptor)
            .field("first", self.first.descriptor())
            .field("second", self.second.descriptor())
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

fn fuse(
    first: Vec<RetrievedDocument>,
    second: Vec<RetrievedDocument>,
    limit: usize,
    policy: ReciprocalRankFusion,
) -> Result<Vec<RetrievedDocument>, RetrievalError> {
    let mut fused = BTreeMap::<DocumentId, FusedDocument>::new();
    accumulate(&mut fused, first, policy.first_weight, policy.rank_constant)?;
    accumulate(
        &mut fused,
        second,
        policy.second_weight,
        policy.rank_constant,
    )?;
    let mut documents = fused.into_values().collect::<Vec<_>>();
    documents.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.first_seen.cmp(&right.first_seen))
            .then_with(|| left.document.document.id.cmp(&right.document.document.id))
    });
    documents.truncate(limit);
    Ok(documents
        .into_iter()
        .map(|entry| RetrievedDocument {
            document: entry.document.document,
            score: entry.score,
        })
        .collect())
}

fn accumulate(
    fused: &mut BTreeMap<DocumentId, FusedDocument>,
    documents: Vec<RetrievedDocument>,
    weight: f64,
    rank_constant: f64,
) -> Result<(), RetrievalError> {
    let source_offset = fused.len();
    let mut source_seen = std::collections::BTreeSet::new();
    for (rank, document) in documents.into_iter().enumerate() {
        let numeric_rank =
            u32::try_from(rank).map_err(|_| RetrievalError::InvalidHybridConfiguration)?;
        let id = document.document.id.clone();
        if !source_seen.insert(id.clone()) {
            return Err(RetrievalError::DuplicateHybridResult(id));
        }
        let score = weight / (rank_constant + f64::from(numeric_rank) + 1.0);
        fused
            .entry(id)
            .and_modify(|entry| entry.score += score)
            .or_insert(FusedDocument {
                document,
                score,
                first_seen: source_offset.saturating_add(rank),
            });
    }
    Ok(())
}

struct FusedDocument {
    document: RetrievedDocument,
    score: f64,
    first_seen: usize,
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
    use super::*;
    use crate::Document;

    struct Fixed {
        descriptor: RetrieverDescriptor,
        ids: Vec<&'static str>,
    }

    impl Retriever for Fixed {
        fn descriptor(&self) -> &RetrieverDescriptor {
            &self.descriptor
        }

        fn retrieve(
            &self,
            _query: RetrievalQuery,
            _context: RetrievalContext,
        ) -> RetrievalFuture<'_, Result<RetrievalResponse, RetrievalError>> {
            let documents = self
                .ids
                .iter()
                .map(|id| RetrievedDocument {
                    document: Document::new(*id, format!("document {id}")).unwrap(),
                    score: 1.0,
                })
                .collect();
            Box::pin(async move {
                Ok(RetrievalResponse {
                    documents,
                    usage: Usage::default(),
                })
            })
        }
    }

    #[test]
    fn common_results_win_and_ties_are_deterministic() {
        let first = Arc::new(Fixed {
            descriptor: RetrieverDescriptor::read_only("lexical"),
            ids: vec!["a", "b", "c"],
        });
        let second = Arc::new(Fixed {
            descriptor: RetrieverDescriptor::read_only("vector"),
            ids: vec!["b", "d", "a"],
        });
        let policy = ReciprocalRankFusion::new(1.0, 1.0, 60.0, 2).unwrap();
        let hybrid = HybridRetriever::new("hybrid", first, second, policy);

        let response = futures_executor::block_on(hybrid.retrieve(
            RetrievalQuery::new("query", 3).unwrap(),
            RetrievalContext::new(),
        ))
        .unwrap();

        let ids = response
            .documents
            .iter()
            .map(|result| result.document.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["b", "a", "d"]);
    }
}
