//! Native Ollama `/api/embed` adapter.

use std::num::NonZeroU32;

use futures_util::future::{Either, select};
use reqwest::{Client, Response};
use runifold_core::{CancellationToken, Instant, Usage};
use runifold_retrieval::{
    Embedding, EmbeddingBatch, EmbeddingFuture, EmbeddingModel, EmbeddingRequest, RetrievalContext,
    RetrievalError,
};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{OllamaConfig, OllamaConfigError};

/// Native Ollama embedding adapter bound to one local or hosted model.
#[derive(Clone, Debug)]
pub struct OllamaEmbeddingModel {
    config: OllamaConfig,
    http: Client,
    model: String,
    dimensions: Option<NonZeroU32>,
    truncate: bool,
}

impl OllamaEmbeddingModel {
    pub(crate) fn new(
        config: OllamaConfig,
        http: Client,
        model: impl Into<String>,
    ) -> Result<Self, OllamaConfigError> {
        let model = model.into();
        if model.trim().is_empty() {
            return Err(OllamaConfigError::EmptyEmbeddingModel);
        }
        Ok(Self {
            config,
            http,
            model,
            dimensions: None,
            truncate: false,
        })
    }

    /// Requests a provider-supported output dimension.
    #[must_use]
    pub const fn with_dimensions(mut self, dimensions: NonZeroU32) -> Self {
        self.dimensions = Some(dimensions);
        self
    }

    /// Selects whether Ollama may silently truncate oversized inputs.
    #[must_use]
    pub const fn with_truncate(mut self, truncate: bool) -> Self {
        self.truncate = truncate;
        self
    }

    /// Returns the configured embedding model identity.
    pub fn model(&self) -> &str {
        &self.model
    }
}

impl EmbeddingModel for OllamaEmbeddingModel {
    fn embed(
        &self,
        request: EmbeddingRequest,
        context: RetrievalContext,
    ) -> EmbeddingFuture<'_, Result<EmbeddingBatch, RetrievalError>> {
        Box::pin(async move {
            context.check_live()?;
            let (inputs, _) = request.into_parts();
            if inputs.is_empty() {
                return Ok(EmbeddingBatch {
                    embeddings: Vec::new(),
                    usage: Usage::default(),
                });
            }
            let expected = inputs.len();
            let body = WireRequest {
                model: self.model.clone(),
                input: inputs,
                truncate: self.truncate,
                dimensions: self.dimensions.map(NonZeroU32::get),
            };
            let mut builder = self
                .http
                .post(self.config.embedding_endpoint_url())
                .header("accept", "application/json")
                .header("x-client-request-id", context.invocation_id().to_string())
                .json(&body);
            if let Some(token) = &self.config.bearer_token {
                builder = builder.bearer_auth(token.expose_secret());
            }
            if let Some(remaining) = context.remaining() {
                builder = builder.timeout(remaining);
            }

            let started = Instant::now();
            let cancellation = context.cancellation().clone();
            let deadline = context.deadline();
            let response = send(builder, &cancellation, deadline).await?;
            if !response.status().is_success() {
                return Err(http_error(response, &cancellation, deadline).await?);
            }
            let payload: WireResponse = read_json(response, &cancellation, deadline).await?;
            let embeddings = payload
                .embeddings
                .into_iter()
                .map(Embedding::new)
                .collect::<Result<Vec<_>, _>>()?;
            let provider_duration = payload.total_duration / 1_000;
            EmbeddingBatch {
                embeddings,
                usage: Usage {
                    tokens: payload.prompt_eval_count,
                    duration_micros: if provider_duration == 0 {
                        elapsed_micros(started)
                    } else {
                        provider_duration
                    },
                    ..Usage::default()
                },
            }
            .validate_count(expected)
        })
    }
}

#[derive(Serialize)]
struct WireRequest {
    model: String,
    input: Vec<String>,
    truncate: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    dimensions: Option<u32>,
}

#[derive(Deserialize)]
struct WireResponse {
    embeddings: Vec<Vec<f64>>,
    #[serde(default)]
    total_duration: u64,
    #[serde(default)]
    prompt_eval_count: u64,
}

async fn send(
    builder: reqwest::RequestBuilder,
    cancellation: &CancellationToken,
    deadline: Option<Instant>,
) -> Result<Response, RetrievalError> {
    match select(Box::pin(cancellation.cancelled()), Box::pin(builder.send())).await {
        Either::Left(_) => Err(RetrievalError::Cancelled),
        Either::Right((result, _)) => result.map_err(|error| transport_error(&error, deadline)),
    }
}

async fn read_json<T: serde::de::DeserializeOwned>(
    response: Response,
    cancellation: &CancellationToken,
    deadline: Option<Instant>,
) -> Result<T, RetrievalError> {
    match select(
        Box::pin(cancellation.cancelled()),
        Box::pin(response.json()),
    )
    .await
    {
        Either::Left(_) => Err(RetrievalError::Cancelled),
        Either::Right((result, _)) => result.map_err(|error| {
            if deadline_elapsed(&error, deadline) {
                RetrievalError::DeadlineExceeded
            } else {
                RetrievalError::provider(format!("Ollama embedding response was invalid: {error}"))
            }
        }),
    }
}

async fn http_error(
    response: Response,
    cancellation: &CancellationToken,
    deadline: Option<Instant>,
) -> Result<RetrievalError, RetrievalError> {
    let status = response.status();
    let payload: Value = read_json(response, cancellation, deadline).await?;
    let message = payload.get("error").and_then(Value::as_str).map_or_else(
        || format!("Ollama embeddings returned HTTP {status}"),
        |message| format!("Ollama embeddings failed: {message}"),
    );
    Ok(RetrievalError::provider(message))
}

fn transport_error(error: &reqwest::Error, deadline: Option<Instant>) -> RetrievalError {
    if deadline_elapsed(error, deadline) {
        RetrievalError::DeadlineExceeded
    } else {
        RetrievalError::provider(format!("Ollama embedding transport failed: {error}"))
    }
}

fn deadline_elapsed(error: &reqwest::Error, deadline: Option<Instant>) -> bool {
    error.is_timeout() || deadline.is_some_and(|value| value <= Instant::now())
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}
