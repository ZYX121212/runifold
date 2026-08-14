//! Cohere v2 Rerank adapter.

use std::fmt;

use futures_util::{future::Either, pin_mut};
use reqwest::Client;
use runifold_retrieval::{
    RerankRequest, RerankResponse, Reranker, RerankerDescriptor, RetrievalContext, RetrievalError,
    RetrievalFuture,
};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use url::Url;

const DEFAULT_BASE_URL: &str = "https://api.cohere.ai/";
const MAX_RESPONSE_BYTES: u64 = 4 * 1024 * 1024;

/// Invalid Cohere reranker configuration.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum CohereConfigError {
    /// API token is blank.
    #[error("Cohere API token cannot be empty")]
    EmptyApiToken,
    /// Model identity is blank.
    #[error("Cohere rerank model cannot be empty")]
    EmptyModel,
    /// Base URL is invalid or not HTTP(S).
    #[error("invalid Cohere API base URL")]
    InvalidBaseUrl,
}

/// Cohere v2 implementation of Runifold's provider-neutral [`Reranker`].
#[derive(Clone)]
pub struct CohereReranker {
    descriptor: RerankerDescriptor,
    api_token: SecretString,
    model: String,
    endpoint: Url,
    http: Client,
}

impl CohereReranker {
    /// Creates a public Cohere v2 reranker.
    ///
    /// # Errors
    ///
    /// Rejects blank credentials or model identities.
    pub fn new(
        api_token: impl Into<String>,
        model: impl Into<String>,
    ) -> Result<Self, CohereConfigError> {
        Self::with_base_url(api_token, model, DEFAULT_BASE_URL)
    }

    /// Creates a reranker against a custom Cohere-compatible base URL.
    ///
    /// # Errors
    ///
    /// Rejects blank credentials/models and invalid non-HTTP(S) URLs.
    pub fn with_base_url(
        api_token: impl Into<String>,
        model: impl Into<String>,
        base_url: &str,
    ) -> Result<Self, CohereConfigError> {
        let api_token = api_token.into();
        if api_token.trim().is_empty() {
            return Err(CohereConfigError::EmptyApiToken);
        }
        let model = model.into();
        if model.trim().is_empty() {
            return Err(CohereConfigError::EmptyModel);
        }
        let mut base = Url::parse(base_url).map_err(|_| CohereConfigError::InvalidBaseUrl)?;
        if !matches!(base.scheme(), "http" | "https") || base.cannot_be_a_base() {
            return Err(CohereConfigError::InvalidBaseUrl);
        }
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }
        let endpoint = base
            .join("v2/rerank")
            .map_err(|_| CohereConfigError::InvalidBaseUrl)?;
        Ok(Self {
            descriptor: RerankerDescriptor::new(format!("cohere.{model}")),
            api_token: SecretString::from(api_token),
            model,
            endpoint,
            http: Client::new(),
        })
    }

    /// Replaces the HTTP client for explicit transport policy or testing.
    #[must_use]
    pub fn with_http_client(mut self, http: Client) -> Self {
        self.http = http;
        self
    }
}

impl Reranker for CohereReranker {
    fn descriptor(&self) -> &RerankerDescriptor {
        &self.descriptor
    }

    fn rerank(
        &self,
        request: RerankRequest,
        context: RetrievalContext,
    ) -> RetrievalFuture<'_, Result<RerankResponse, RetrievalError>> {
        Box::pin(async move {
            context.check_live()?;
            let documents = request
                .candidates
                .iter()
                .map(|candidate| candidate.document.text.clone())
                .collect::<Vec<_>>();
            let mut builder = self
                .http
                .post(self.endpoint.clone())
                .bearer_auth(self.api_token.expose_secret())
                .header("X-Client-Request-Id", context.invocation_id().to_string())
                .json(&json!({
                    "model": self.model,
                    "query": request.query,
                    "documents": documents,
                    "top_n": request.limit,
                }));
            if let Some(remaining) = context.remaining() {
                builder = builder.timeout(remaining);
            }
            let cancellation = context.cancellation().cancelled();
            let send = builder.send();
            pin_mut!(cancellation, send);
            let response = match futures_util::future::select(cancellation, send).await {
                Either::Left(_) => return Err(RetrievalError::Cancelled),
                Either::Right((result, _)) => result.map_err(|error| transport_error(&error))?,
            };
            if !response.status().is_success() {
                return Err(RetrievalError::provider(format!(
                    "Cohere Rerank returned HTTP {}",
                    response.status()
                )));
            }
            if response
                .content_length()
                .is_some_and(|length| length > MAX_RESPONSE_BYTES)
            {
                return Err(RetrievalError::provider(
                    "Cohere Rerank response exceeds 4 MiB",
                ));
            }
            let cancellation = context.cancellation().cancelled();
            let body = response.bytes();
            pin_mut!(cancellation, body);
            let bytes = match futures_util::future::select(cancellation, body).await {
                Either::Left(_) => return Err(RetrievalError::Cancelled),
                Either::Right((result, _)) => result.map_err(|error| transport_error(&error))?,
            };
            if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_RESPONSE_BYTES {
                return Err(RetrievalError::provider(
                    "Cohere Rerank response exceeds 4 MiB",
                ));
            }
            let payload: CohereResponse = serde_json::from_slice(&bytes)
                .map_err(|_| RetrievalError::provider("Cohere Rerank response is invalid JSON"))?;
            let mut seen = std::collections::BTreeSet::new();
            let mut ranked = Vec::with_capacity(payload.results.len());
            for result in payload.results {
                if result.index >= request.candidates.len()
                    || !result.relevance_score.is_finite()
                    || !seen.insert(result.index)
                {
                    return Err(RetrievalError::provider(
                        "Cohere Rerank response contains an invalid result",
                    ));
                }
                let mut document = request.candidates[result.index].clone();
                document.score = result.relevance_score;
                ranked.push(document);
            }
            if ranked.len() > request.limit {
                return Err(RetrievalError::provider(
                    "Cohere Rerank returned more results than requested",
                ));
            }
            Ok(RerankResponse {
                documents: ranked,
                usage: runifold_core::Usage::default(),
            })
        })
    }
}

impl fmt::Debug for CohereReranker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CohereReranker")
            .field("descriptor", &self.descriptor)
            .field("model", &self.model)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

fn transport_error(error: &reqwest::Error) -> RetrievalError {
    if error.is_timeout() {
        RetrievalError::DeadlineExceeded
    } else {
        RetrievalError::provider("Cohere Rerank transport failed")
    }
}

#[derive(Deserialize)]
struct CohereResponse {
    results: Vec<CohereResult>,
}

#[derive(Deserialize)]
struct CohereResult {
    index: usize,
    relevance_score: f64,
}
