//! Native Gemini batch embeddings adapter.

use std::num::NonZeroU32;

use futures_util::future::{Either, select};
use reqwest::{Client, Response};
use runifold_core::{CancellationToken, Instant, Usage};
use runifold_retrieval::{
    Embedding, EmbeddingBatch, EmbeddingFuture, EmbeddingModel, EmbeddingRequest, EmbeddingTask,
    RetrievalContext, RetrievalError,
};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{GeminiConfig, GeminiConfigError};

/// Gemini `batchEmbedContents` adapter bound to one embedding model.
#[derive(Clone, Debug)]
pub struct GeminiEmbeddingModel {
    config: GeminiConfig,
    http: Client,
    model: String,
    dimensions: Option<NonZeroU32>,
    auto_truncate: bool,
}

impl GeminiEmbeddingModel {
    pub(crate) fn new(
        config: GeminiConfig,
        http: Client,
        model: impl Into<String>,
    ) -> Result<Self, GeminiConfigError> {
        let model = model.into();
        if model.trim().is_empty() {
            return Err(GeminiConfigError::EmptyEmbeddingModel);
        }
        Ok(Self {
            config,
            http,
            model: model.strip_prefix("models/").unwrap_or(&model).to_owned(),
            dimensions: None,
            auto_truncate: false,
        })
    }

    /// Requests a reduced output dimension.
    #[must_use]
    pub const fn with_dimensions(mut self, dimensions: NonZeroU32) -> Self {
        self.dimensions = Some(dimensions);
        self
    }

    /// Selects whether Gemini may silently truncate oversized inputs.
    #[must_use]
    pub const fn with_auto_truncate(mut self, auto_truncate: bool) -> Self {
        self.auto_truncate = auto_truncate;
        self
    }

    /// Returns the configured embedding model identity.
    pub fn model(&self) -> &str {
        &self.model
    }
}

impl EmbeddingModel for GeminiEmbeddingModel {
    fn embed(
        &self,
        request: EmbeddingRequest,
        context: RetrievalContext,
    ) -> EmbeddingFuture<'_, Result<EmbeddingBatch, RetrievalError>> {
        Box::pin(async move {
            context.check_live()?;
            let (inputs, task) = request.into_parts();
            if inputs.is_empty() {
                return Ok(EmbeddingBatch {
                    embeddings: Vec::new(),
                    usage: Usage::default(),
                });
            }
            let expected = inputs.len();
            let model = format!("models/{}", self.model);
            let requests = inputs
                .into_iter()
                .map(|text| WireEmbedRequest {
                    model: model.clone(),
                    content: WireContent {
                        parts: vec![WirePart { text }],
                    },
                    config: WireConfig {
                        task_type: task_name(task),
                        auto_truncate: self.auto_truncate,
                        output_dimensionality: self.dimensions.map(NonZeroU32::get),
                    },
                })
                .collect();
            let body = WireBatchRequest { requests };
            let mut builder = self
                .http
                .post(
                    self.config
                        .embedding_endpoint_url(&self.model)
                        .map_err(|error| {
                            RetrievalError::provider(format!(
                                "invalid Gemini embedding endpoint: {error}"
                            ))
                        })?,
                )
                .header("accept", "application/json")
                .header("x-client-request-id", context.invocation_id().to_string())
                .json(&body);
            if let Some(api_key) = &self.config.api_key {
                builder = builder.header("x-goog-api-key", api_key.expose_secret());
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
            let payload: WireBatchResponse = read_json(response, &cancellation, deadline).await?;
            let embeddings = payload
                .embeddings
                .into_iter()
                .map(|embedding| Embedding::new(embedding.values))
                .collect::<Result<Vec<_>, _>>()?;
            EmbeddingBatch {
                embeddings,
                usage: Usage {
                    tokens: payload.usage.prompt_token_count,
                    duration_micros: elapsed_micros(started),
                    ..Usage::default()
                },
            }
            .validate_count(expected)
        })
    }
}

#[derive(Serialize)]
struct WireBatchRequest {
    requests: Vec<WireEmbedRequest>,
}

#[derive(Serialize)]
struct WireEmbedRequest {
    model: String,
    content: WireContent,
    #[serde(rename = "embedContentConfig")]
    config: WireConfig,
}

#[derive(Serialize)]
struct WireContent {
    parts: Vec<WirePart>,
}

#[derive(Serialize)]
struct WirePart {
    text: String,
}

#[derive(Serialize)]
struct WireConfig {
    #[serde(rename = "taskType")]
    task_type: &'static str,
    #[serde(rename = "autoTruncate")]
    auto_truncate: bool,
    #[serde(
        rename = "outputDimensionality",
        skip_serializing_if = "Option::is_none"
    )]
    output_dimensionality: Option<u32>,
}

#[derive(Deserialize)]
struct WireBatchResponse {
    embeddings: Vec<WireEmbedding>,
    #[serde(rename = "usageMetadata", default)]
    usage: WireUsage,
}

#[derive(Deserialize)]
struct WireEmbedding {
    values: Vec<f64>,
}

#[derive(Default, Deserialize)]
struct WireUsage {
    #[serde(rename = "promptTokenCount", default)]
    prompt_token_count: u64,
}

const fn task_name(task: EmbeddingTask) -> &'static str {
    match task {
        EmbeddingTask::RetrievalQuery => "RETRIEVAL_QUERY",
        EmbeddingTask::RetrievalDocument => "RETRIEVAL_DOCUMENT",
        EmbeddingTask::SemanticSimilarity => "SEMANTIC_SIMILARITY",
        EmbeddingTask::Classification => "CLASSIFICATION",
        EmbeddingTask::Clustering => "CLUSTERING",
        _ => "TASK_TYPE_UNSPECIFIED",
    }
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
                RetrievalError::provider(format!("Gemini embedding response was invalid: {error}"))
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
    let message = payload
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .map_or_else(
            || format!("Gemini embeddings returned HTTP {status}"),
            |message| format!("Gemini embeddings failed: {message}"),
        );
    Ok(RetrievalError::provider(message))
}

fn transport_error(error: &reqwest::Error, deadline: Option<Instant>) -> RetrievalError {
    if deadline_elapsed(error, deadline) {
        RetrievalError::DeadlineExceeded
    } else {
        RetrievalError::provider(format!("Gemini embedding transport failed: {error}"))
    }
}

fn deadline_elapsed(error: &reqwest::Error, deadline: Option<Instant>) -> bool {
    error.is_timeout() || deadline.is_some_and(|value| value <= Instant::now())
}

fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}
