//! Qdrant REST adapter for Runifold's provider-neutral vector store.

use std::time::Instant;

use futures_util::future::{Either, select};
use reqwest::{Client, RequestBuilder, Response};
use runifold_core::Usage;
use runifold_retrieval::{
    Document, Embedding, RetrievalContext, RetrievalError, VectorRecord, VectorSearchResponse,
    VectorSearchResult, VectorStore, VectorStoreFuture, VectorUpsertOutcome,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

const DOCUMENT_NAMESPACE: Uuid = Uuid::from_u128(0x15f7_6547_d380_56c9_92ab_8c75_70fb_e984);

/// Invalid Qdrant adapter configuration.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum QdrantConfigError {
    /// The base URL was invalid.
    #[error("invalid Qdrant base URL: {0}")]
    InvalidBaseUrl(#[from] url::ParseError),
    /// The base URL was not hierarchical HTTP(S).
    #[error("Qdrant base URL must be an HTTP(S) URL")]
    InvalidBaseUrlShape,
    /// The API key was blank.
    #[error("Qdrant API key cannot be empty")]
    EmptyApiKey,
    /// The collection identity was blank.
    #[error("Qdrant collection cannot be empty")]
    EmptyCollection,
}

/// Secret-safe Qdrant REST configuration.
#[derive(Clone)]
pub struct QdrantConfig {
    base_url: Url,
    api_key: Option<SecretString>,
}

impl QdrantConfig {
    /// Creates configuration for a local or remote Qdrant REST endpoint.
    ///
    /// # Errors
    ///
    /// Rejects invalid and non-HTTP(S) URLs.
    pub fn new(base_url: &str) -> Result<Self, QdrantConfigError> {
        let mut base_url = Url::parse(base_url)?;
        if !matches!(base_url.scheme(), "http" | "https") || base_url.cannot_be_a_base() {
            return Err(QdrantConfigError::InvalidBaseUrlShape);
        }
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        Ok(Self {
            base_url,
            api_key: None,
        })
    }

    /// Adds Qdrant API-key authentication.
    ///
    /// # Errors
    ///
    /// Rejects a blank API key.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Result<Self, QdrantConfigError> {
        let api_key = api_key.into();
        if api_key.trim().is_empty() {
            return Err(QdrantConfigError::EmptyApiKey);
        }
        self.api_key = Some(SecretString::from(api_key));
        Ok(self)
    }

    fn endpoint(&self, collection: &str, suffix: &[&str]) -> Result<Url, RetrievalError> {
        let mut endpoint = self.base_url.clone();
        let mut segments = endpoint.path_segments_mut().map_err(|()| {
            RetrievalError::provider("Qdrant base URL cannot accept path segments")
        })?;
        segments.pop_if_empty();
        segments.extend(["collections", collection, "points"]);
        segments.extend(suffix.iter().copied());
        drop(segments);
        Ok(endpoint)
    }
}

impl std::fmt::Debug for QdrantConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QdrantConfig")
            .field("base_url", &self.base_url)
            .field("api_key", &self.api_key.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

/// Qdrant-backed vector persistence.
#[derive(Clone, Debug)]
pub struct QdrantVectorStore {
    config: QdrantConfig,
    collection: String,
    http: Client,
}

impl QdrantVectorStore {
    /// Creates a store bound to one collection.
    ///
    /// # Errors
    ///
    /// Rejects a blank collection identity.
    pub fn new(
        config: QdrantConfig,
        collection: impl Into<String>,
    ) -> Result<Self, QdrantConfigError> {
        let collection = collection.into();
        if collection.trim().is_empty() {
            return Err(QdrantConfigError::EmptyCollection);
        }
        Ok(Self {
            config,
            collection,
            http: Client::new(),
        })
    }

    /// Replaces the HTTP transport.
    #[must_use]
    pub fn with_http_client(mut self, http: Client) -> Self {
        self.http = http;
        self
    }

    fn authorize(&self, mut builder: RequestBuilder) -> RequestBuilder {
        if let Some(api_key) = &self.config.api_key {
            builder = builder.header("api-key", api_key.expose_secret());
        }
        builder
    }
}

impl VectorStore for QdrantVectorStore {
    fn upsert(
        &self,
        records: Vec<VectorRecord>,
        context: RetrievalContext,
    ) -> VectorStoreFuture<'_, Result<VectorUpsertOutcome, RetrievalError>> {
        Box::pin(async move {
            context.check_live()?;
            if records.is_empty() {
                return Ok(VectorUpsertOutcome::default());
            }
            let points = records.into_iter().map(WirePoint::from).collect();
            let mut endpoint = self.config.endpoint(&self.collection, &[])?;
            endpoint.query_pairs_mut().append_pair("wait", "true");
            let builder = self.authorize(self.http.put(endpoint).json(&WireUpsert { points }));
            let started = Instant::now();
            send_checked(builder, &context).await?;
            Ok(VectorUpsertOutcome {
                usage: duration_usage(started),
            })
        })
    }

    fn search(
        &self,
        query: Embedding,
        limit: usize,
        context: RetrievalContext,
    ) -> VectorStoreFuture<'_, Result<VectorSearchResponse, RetrievalError>> {
        Box::pin(async move {
            context.check_live()?;
            if limit == 0 {
                return Err(RetrievalError::ZeroLimit);
            }
            let endpoint = self.config.endpoint(&self.collection, &["query"])?;
            let body = WireQuery {
                query: query.values(),
                limit,
                with_payload: true,
                with_vector: false,
            };
            let builder = self.authorize(self.http.post(endpoint).json(&body));
            let started = Instant::now();
            let response: WireQueryResponse = send_json(builder, &context).await?;
            let points = match response.result {
                WireQueryResult::Envelope { points } | WireQueryResult::List(points) => points,
            };
            let results = points
                .into_iter()
                .map(WireScoredPoint::try_into)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(VectorSearchResponse {
                results,
                usage: duration_usage(started),
            })
        })
    }
}

#[derive(Serialize)]
struct WireUpsert {
    points: Vec<WirePoint>,
}

#[derive(Serialize)]
struct WirePoint {
    id: Uuid,
    vector: Vec<f64>,
    payload: Map<String, Value>,
}

impl From<VectorRecord> for WirePoint {
    fn from(record: VectorRecord) -> Self {
        let mut payload = Map::new();
        payload.insert(
            "_runifold_id".into(),
            Value::String(record.document.id.to_string()),
        );
        payload.insert("_runifold_text".into(), Value::String(record.document.text));
        payload.insert(
            "_runifold_metadata".into(),
            Value::Object(record.document.metadata.into_iter().collect()),
        );
        Self {
            id: Uuid::new_v5(&DOCUMENT_NAMESPACE, record.document.id.as_str().as_bytes()),
            vector: record.embedding.values().to_vec(),
            payload,
        }
    }
}

#[derive(Serialize)]
struct WireQuery<'a> {
    query: &'a [f64],
    limit: usize,
    with_payload: bool,
    with_vector: bool,
}

#[derive(Deserialize)]
struct WireQueryResponse {
    result: WireQueryResult,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum WireQueryResult {
    Envelope { points: Vec<WireScoredPoint> },
    List(Vec<WireScoredPoint>),
}

#[derive(Deserialize)]
struct WireScoredPoint {
    score: f64,
    payload: Map<String, Value>,
}

impl TryFrom<WireScoredPoint> for VectorSearchResult {
    type Error = RetrievalError;

    fn try_from(point: WireScoredPoint) -> Result<Self, Self::Error> {
        let id = point
            .payload
            .get("_runifold_id")
            .and_then(Value::as_str)
            .ok_or_else(|| RetrievalError::provider("Qdrant result omitted document id"))?;
        let text = point
            .payload
            .get("_runifold_text")
            .and_then(Value::as_str)
            .ok_or_else(|| RetrievalError::provider("Qdrant result omitted document text"))?;
        let mut document = Document::new(id, text)?;
        if let Some(metadata) = point
            .payload
            .get("_runifold_metadata")
            .and_then(Value::as_object)
        {
            document.metadata = metadata.clone().into_iter().collect();
        }
        Ok(Self {
            document,
            score: point.score,
        })
    }
}

async fn send_checked(
    builder: RequestBuilder,
    context: &RetrievalContext,
) -> Result<(), RetrievalError> {
    let response = send(builder, context).await?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(http_error(response, context).await?)
    }
}

async fn send_json<T: serde::de::DeserializeOwned>(
    builder: RequestBuilder,
    context: &RetrievalContext,
) -> Result<T, RetrievalError> {
    let response = send(builder, context).await?;
    if !response.status().is_success() {
        return Err(http_error(response, context).await?);
    }
    read_json(response, context).await
}

async fn send(
    mut builder: RequestBuilder,
    context: &RetrievalContext,
) -> Result<Response, RetrievalError> {
    if let Some(remaining) = context.remaining() {
        builder = builder.timeout(remaining);
    }
    let cancellation = context.cancellation().clone();
    match select(Box::pin(cancellation.cancelled()), Box::pin(builder.send())).await {
        Either::Left(_) => Err(RetrievalError::Cancelled),
        Either::Right((result, _)) => result.map_err(|error| transport_error(&error)),
    }
}

async fn read_json<T: serde::de::DeserializeOwned>(
    response: Response,
    context: &RetrievalContext,
) -> Result<T, RetrievalError> {
    let cancellation = context.cancellation().clone();
    match select(
        Box::pin(cancellation.cancelled()),
        Box::pin(response.json()),
    )
    .await
    {
        Either::Left(_) => Err(RetrievalError::Cancelled),
        Either::Right((result, _)) => result.map_err(|error| transport_error(&error)),
    }
}

async fn http_error(
    response: Response,
    context: &RetrievalContext,
) -> Result<RetrievalError, RetrievalError> {
    let status = response.status();
    let payload: Value = read_json(response, context).await?;
    let message = payload
        .get("status")
        .and_then(|status| status.get("error"))
        .and_then(Value::as_str)
        .map_or_else(
            || format!("Qdrant returned HTTP {status}"),
            |message| format!("Qdrant failed: {message}"),
        );
    Ok(RetrievalError::provider(message))
}

fn transport_error(error: &reqwest::Error) -> RetrievalError {
    if error.is_timeout() {
        RetrievalError::DeadlineExceeded
    } else {
        RetrievalError::provider(format!("Qdrant transport failed: {error}"))
    }
}

fn duration_usage(started: Instant) -> Usage {
    Usage {
        duration_micros: u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        ..Usage::default()
    }
}
