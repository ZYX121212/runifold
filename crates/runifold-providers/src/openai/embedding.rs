//! Native OpenAI-compatible embeddings adapter.

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

use super::{OpenAiConfig, OpenAiConfigError};

/// OpenAI-compatible `/embeddings` model bound to one model identity.
#[derive(Clone, Debug)]
pub struct OpenAiEmbeddingModel {
    config: OpenAiConfig,
    http: Client,
    model: String,
    dimensions: Option<NonZeroU32>,
}

impl OpenAiEmbeddingModel {
    pub(crate) fn new(
        config: OpenAiConfig,
        http: Client,
        model: impl Into<String>,
    ) -> Result<Self, OpenAiConfigError> {
        let model = model.into();
        if model.trim().is_empty() {
            return Err(OpenAiConfigError::EmptyEmbeddingModel);
        }
        Ok(Self {
            config,
            http,
            model,
            dimensions: None,
        })
    }

    /// Requests a provider-supported output dimension.
    #[must_use]
    pub const fn with_dimensions(mut self, dimensions: NonZeroU32) -> Self {
        self.dimensions = Some(dimensions);
        self
    }

    /// Returns the configured embedding model identity.
    pub fn model(&self) -> &str {
        &self.model
    }
}

impl EmbeddingModel for OpenAiEmbeddingModel {
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
                input: inputs,
                model: self.model.clone(),
                encoding_format: "float",
                dimensions: self.dimensions.map(NonZeroU32::get),
            };
            let mut builder = self
                .http
                .post(self.config.embedding_endpoint_url())
                .header("accept", "application/json")
                .header("x-client-request-id", context.invocation_id().to_string())
                .json(&body);
            if let Some(api_key) = &self.config.api_key {
                builder = builder.bearer_auth(api_key.expose_secret());
            }
            if let Some(api_key) = &self.config.azure_api_key {
                builder = builder.header("api-key", api_key.expose_secret());
            }
            if let Some(organization) = &self.config.organization {
                builder = builder.header("openai-organization", organization);
            }
            if let Some(project) = &self.config.project {
                builder = builder.header("openai-project", project);
            }
            if let Some(application_url) = &self.config.application_url {
                builder = builder.header("http-referer", application_url.as_str());
            }
            if let Some(application_title) = &self.config.application_title {
                builder = builder.header("x-openrouter-title", application_title);
            }
            if let Some(remaining) = context.remaining() {
                builder = builder.timeout(remaining);
            }

            let started = Instant::now();
            let cancellation = context.cancellation().clone();
            let deadline = context.deadline();
            let response = send(builder, &cancellation, deadline).await?;
            if !response.status().is_success() {
                return Err(
                    http_error(response, &cancellation, &self.config.provider, deadline).await?,
                );
            }
            let payload: WireResponse =
                read_json(response, &cancellation, &self.config.provider, deadline).await?;
            let mut ordered = vec![None; expected];
            for item in payload.data {
                let Some(slot) = ordered.get_mut(item.index) else {
                    return Err(RetrievalError::provider(format!(
                        "{} returned embedding index {} outside batch length {expected}",
                        self.config.provider, item.index
                    )));
                };
                if slot.is_some() {
                    return Err(RetrievalError::provider(format!(
                        "{} returned duplicate embedding index {}",
                        self.config.provider, item.index
                    )));
                }
                *slot = Some(Embedding::new(item.embedding)?);
            }
            let embeddings = ordered
                .into_iter()
                .enumerate()
                .map(|(index, embedding)| {
                    embedding.ok_or_else(|| {
                        RetrievalError::provider(format!(
                            "{} omitted embedding index {index}",
                            self.config.provider
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(EmbeddingBatch {
                embeddings,
                usage: Usage {
                    tokens: payload.usage.prompt_tokens,
                    duration_micros: elapsed_micros(started),
                    ..Usage::default()
                },
            })
        })
    }
}

#[derive(Serialize)]
struct WireRequest {
    input: Vec<String>,
    model: String,
    encoding_format: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    dimensions: Option<u32>,
}

#[derive(Deserialize)]
struct WireResponse {
    data: Vec<WireEmbedding>,
    #[serde(default)]
    usage: WireUsage,
}

#[derive(Deserialize)]
struct WireEmbedding {
    embedding: Vec<f64>,
    index: usize,
}

#[derive(Default, Deserialize)]
struct WireUsage {
    #[serde(default)]
    prompt_tokens: u64,
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
    provider: &str,
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
                RetrievalError::provider(format!(
                    "{provider} embedding response was invalid: {error}"
                ))
            }
        }),
    }
}

async fn http_error(
    response: Response,
    cancellation: &CancellationToken,
    provider: &str,
    deadline: Option<Instant>,
) -> Result<RetrievalError, RetrievalError> {
    let status = response.status();
    let payload: Value = read_json(response, cancellation, provider, deadline).await?;
    let message = payload
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .map_or_else(
            || format!("{provider} embeddings returned HTTP {status}"),
            |message| format!("{provider} embeddings failed: {message}"),
        );
    Ok(RetrievalError::provider(message))
}

fn transport_error(error: &reqwest::Error, deadline: Option<Instant>) -> RetrievalError {
    if deadline_elapsed(error, deadline) {
        RetrievalError::DeadlineExceeded
    } else {
        RetrievalError::provider(format!("embedding transport failed: {error}"))
    }
}

fn deadline_elapsed(error: &reqwest::Error, deadline: Option<Instant>) -> bool {
    error.is_timeout() || deadline.is_some_and(|value| value <= Instant::now())
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}
