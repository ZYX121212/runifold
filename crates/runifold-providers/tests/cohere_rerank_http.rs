//! Cohere v2 Rerank offline HTTP contract.
#![cfg(feature = "cohere")]

use runifold_provider_testkit::{CassetteServer, HttpExchange, ScriptedResponse};
use runifold_providers::cohere::CohereReranker;
use runifold_retrieval::{Document, RerankRequest, Reranker, RetrievalContext, RetrievedDocument};
use serde_json::json;

#[tokio::test]
async fn rerank_maps_indices_to_original_documents() {
    let server = CassetteServer::start(vec![
        HttpExchange::new(
            "POST",
            "/v2/rerank",
            ScriptedResponse::json(
                200,
                &json!({
                    "id":"rerank-test",
                    "results":[
                        {"index":1,"relevance_score":0.9},
                        {"index":0,"relevance_score":0.4}
                    ],
                    "meta":{"billed_units":{"search_units":1}}
                }),
            )
            .unwrap(),
        )
        .with_json_body(json!({
            "model":"rerank-test",
            "query":"capital",
            "documents":["alpha","beta"],
            "top_n":2
        })),
    ])
    .unwrap();
    let reranker =
        CohereReranker::with_base_url("secret-test-token", "rerank-test", &server.base_url())
            .unwrap();
    let candidates = ["alpha", "beta"]
        .into_iter()
        .map(|text| RetrievedDocument {
            document: Document::new(text, text).unwrap(),
            score: 0.0,
        })
        .collect();

    let response = reranker
        .rerank(
            RerankRequest::new("capital", candidates, 2).unwrap(),
            RetrievalContext::new(),
        )
        .await
        .unwrap();

    assert_eq!(response.documents[0].document.id.as_str(), "beta");
    assert!((response.documents[0].score - 0.9).abs() < 1e-12);
    server.assert_finished().unwrap();
    assert_eq!(
        server.observed_requests()[0].headers["authorization"],
        "[REDACTED]"
    );
}
