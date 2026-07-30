//! Deterministic information-retrieval quality metrics.

use std::{collections::BTreeSet, sync::Arc, time::Instant};

use runifold_core::Usage;
use runifold_retrieval::{DocumentId, RetrievalContext, RetrievalError, RetrievalQuery, Retriever};
use thiserror::Error;

/// Invalid dataset or failed retrieval evaluation.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RetrievalEvaluationError {
    /// A case identity was blank.
    #[error("retrieval evaluation case id cannot be empty")]
    EmptyCaseId,
    /// A case had no relevant documents.
    #[error("retrieval evaluation case must contain at least one relevant document")]
    EmptyRelevantDocuments,
    /// A case requested no ranked results.
    #[error("retrieval evaluation cutoff must be greater than zero")]
    ZeroCutoff,
    /// A collection size could not be represented by the metric format.
    #[error("retrieval evaluation collection is too large for metric calculation")]
    CountOutOfRange,
    /// The retriever failed.
    #[error("retrieval evaluation failed: {0}")]
    Retrieval(#[from] RetrievalError),
}

/// One versionable retrieval-quality case.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetrievalEvaluationCase {
    /// Stable case identity.
    pub id: String,
    /// Search query.
    pub query: String,
    /// Ground-truth relevant document identities.
    pub relevant: BTreeSet<DocumentId>,
    /// Ranking cutoff.
    pub cutoff: usize,
}

impl RetrievalEvaluationCase {
    /// Validates one retrieval evaluation case.
    ///
    /// # Errors
    ///
    /// Rejects blank identities or queries, empty relevance sets, and zero
    /// cutoffs.
    pub fn new(
        id: impl Into<String>,
        query: impl Into<String>,
        relevant: impl IntoIterator<Item = DocumentId>,
        cutoff: usize,
    ) -> Result<Self, RetrievalEvaluationError> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(RetrievalEvaluationError::EmptyCaseId);
        }
        if cutoff == 0 {
            return Err(RetrievalEvaluationError::ZeroCutoff);
        }
        let query = query.into();
        RetrievalQuery::new(query.clone(), cutoff)?;
        let relevant = relevant.into_iter().collect::<BTreeSet<_>>();
        if relevant.is_empty() {
            return Err(RetrievalEvaluationError::EmptyRelevantDocuments);
        }
        Ok(Self {
            id,
            query,
            relevant,
            cutoff,
        })
    }
}

/// Metrics for one ranked retrieval case.
#[derive(Clone, Debug, PartialEq)]
pub struct RetrievalCaseMetrics {
    /// Case identity.
    pub id: String,
    /// Precision at the configured cutoff.
    pub precision_at_k: f64,
    /// Recall at the configured cutoff.
    pub recall_at_k: f64,
    /// Reciprocal rank of the first relevant result.
    pub reciprocal_rank: f64,
    /// Normalized discounted cumulative gain.
    pub ndcg_at_k: f64,
    /// Retriever-reported usage.
    pub usage: Usage,
    /// Host-observed end-to-end duration.
    pub elapsed_micros: u64,
}

/// Macro-averaged retrieval-quality report.
#[derive(Clone, Debug, PartialEq)]
pub struct RetrievalEvaluationReport {
    /// Per-case evidence in input order.
    pub cases: Vec<RetrievalCaseMetrics>,
    /// Mean precision at K.
    pub mean_precision_at_k: f64,
    /// Mean recall at K.
    pub mean_recall_at_k: f64,
    /// Mean reciprocal rank.
    pub mean_reciprocal_rank: f64,
    /// Mean normalized discounted cumulative gain.
    pub mean_ndcg_at_k: f64,
    /// Mean host-observed latency.
    pub mean_elapsed_micros: u64,
}

/// Deterministic evaluator for any provider-neutral retriever.
#[derive(Clone)]
pub struct RetrievalEvaluationRunner {
    retriever: Arc<dyn Retriever>,
}

impl RetrievalEvaluationRunner {
    /// Creates an evaluator around one retriever.
    pub fn new(retriever: Arc<dyn Retriever>) -> Self {
        Self { retriever }
    }

    /// Runs cases sequentially to preserve deterministic evidence ordering.
    ///
    /// # Errors
    ///
    /// Fails when any retriever invocation fails.
    pub async fn run(
        &self,
        cases: &[RetrievalEvaluationCase],
    ) -> Result<RetrievalEvaluationReport, RetrievalEvaluationError> {
        let mut metrics = Vec::with_capacity(cases.len());
        for case in cases {
            let started = Instant::now();
            let response = self
                .retriever
                .retrieve(
                    RetrievalQuery::new(case.query.clone(), case.cutoff)?,
                    RetrievalContext::new(),
                )
                .await?;
            let elapsed_micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
            let ranked = response
                .documents
                .iter()
                .take(case.cutoff)
                .map(|result| &result.document.id)
                .collect::<Vec<_>>();
            let hits = ranked
                .iter()
                .filter(|id| case.relevant.contains(*id))
                .count();
            let reciprocal_rank = ranked
                .iter()
                .position(|id| case.relevant.contains(*id))
                .map(|index| count_as_f64(index + 1).map(|rank| 1.0 / rank))
                .transpose()?
                .unwrap_or(0.0);
            let dcg = ranked
                .iter()
                .enumerate()
                .filter(|(_, id)| case.relevant.contains(**id))
                .map(|(index, _)| count_as_f64(index + 2).map(|rank| 1.0 / rank.log2()))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .sum::<f64>();
            let ideal = case.relevant.len().min(case.cutoff);
            let idcg = (0..ideal)
                .map(|index| count_as_f64(index + 2).map(|rank| 1.0 / rank.log2()))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .sum::<f64>();
            metrics.push(RetrievalCaseMetrics {
                id: case.id.clone(),
                precision_at_k: count_as_f64(hits)? / count_as_f64(case.cutoff)?,
                recall_at_k: count_as_f64(hits)? / count_as_f64(case.relevant.len())?,
                reciprocal_rank,
                ndcg_at_k: dcg / idcg,
                usage: response.usage,
                elapsed_micros,
            });
        }
        aggregate(metrics)
    }
}

impl std::fmt::Debug for RetrievalEvaluationRunner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetrievalEvaluationRunner")
            .field("retriever", self.retriever.descriptor())
            .finish()
    }
}

fn aggregate(
    cases: Vec<RetrievalCaseMetrics>,
) -> Result<RetrievalEvaluationReport, RetrievalEvaluationError> {
    if cases.is_empty() {
        return Ok(RetrievalEvaluationReport {
            cases,
            mean_precision_at_k: 0.0,
            mean_recall_at_k: 0.0,
            mean_reciprocal_rank: 0.0,
            mean_ndcg_at_k: 0.0,
            mean_elapsed_micros: 0,
        });
    }
    let count = count_as_f64(cases.len())?;
    let elapsed_total = cases
        .iter()
        .map(|case| u128::from(case.elapsed_micros))
        .sum::<u128>();
    let elapsed_mean = elapsed_total
        / u128::try_from(cases.len()).map_err(|_| RetrievalEvaluationError::CountOutOfRange)?;
    Ok(RetrievalEvaluationReport {
        mean_precision_at_k: cases.iter().map(|case| case.precision_at_k).sum::<f64>() / count,
        mean_recall_at_k: cases.iter().map(|case| case.recall_at_k).sum::<f64>() / count,
        mean_reciprocal_rank: cases.iter().map(|case| case.reciprocal_rank).sum::<f64>() / count,
        mean_ndcg_at_k: cases.iter().map(|case| case.ndcg_at_k).sum::<f64>() / count,
        mean_elapsed_micros: u64::try_from(elapsed_mean)
            .map_err(|_| RetrievalEvaluationError::CountOutOfRange)?,
        cases,
    })
}

fn count_as_f64(value: usize) -> Result<f64, RetrievalEvaluationError> {
    u32::try_from(value)
        .map(f64::from)
        .map_err(|_| RetrievalEvaluationError::CountOutOfRange)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use runifold_retrieval::{
        Document, RetrievalFuture, RetrievalResponse, RetrievedDocument, RetrieverDescriptor,
    };

    use super::*;

    struct RankedRetriever {
        descriptor: RetrieverDescriptor,
    }

    impl Retriever for RankedRetriever {
        fn descriptor(&self) -> &RetrieverDescriptor {
            &self.descriptor
        }

        fn retrieve(
            &self,
            _query: RetrievalQuery,
            _context: RetrievalContext,
        ) -> RetrievalFuture<'_, Result<RetrievalResponse, RetrievalError>> {
            Box::pin(async {
                Ok(RetrievalResponse {
                    documents: vec![
                        RetrievedDocument {
                            document: Document::new("irrelevant", "noise").unwrap(),
                            score: 1.0,
                        },
                        RetrievedDocument {
                            document: Document::new("relevant", "answer").unwrap(),
                            score: 0.9,
                        },
                    ],
                    usage: Usage::default(),
                })
            })
        }
    }

    #[test]
    fn computes_rank_sensitive_metrics_from_stable_evidence() {
        let case = RetrievalEvaluationCase::new(
            "case",
            "query",
            [DocumentId::new("relevant").unwrap()],
            2,
        )
        .unwrap();
        let runner = RetrievalEvaluationRunner::new(Arc::new(RankedRetriever {
            descriptor: RetrieverDescriptor {
                metadata: BTreeMap::new(),
                ..RetrieverDescriptor::read_only("ranked")
            },
        }));

        let report = futures_executor::block_on(runner.run(&[case])).unwrap();

        assert!((report.mean_precision_at_k - 0.5).abs() < f64::EPSILON);
        assert!((report.mean_recall_at_k - 1.0).abs() < f64::EPSILON);
        assert!((report.mean_reciprocal_rank - 0.5).abs() < f64::EPSILON);
        assert!(report.mean_ndcg_at_k > 0.6 && report.mean_ndcg_at_k < 0.7);
    }
}
