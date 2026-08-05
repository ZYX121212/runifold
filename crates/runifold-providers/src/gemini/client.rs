//! Gemini runtime client.

use std::collections::BTreeMap;

use eventsource_stream::{EventStreamError, Eventsource};
use futures_util::{
    StreamExt,
    future::{Either, select},
};
use reqwest::{Client, RequestBuilder, Response};
use runifold_core::{CancellationToken, Instant};
use runifold_model::{
    FeatureSupport, Model, ModelCallContext, ModelCapabilities, ModelError, ModelErrorKind,
    ModelEventStream, ModelFuture, ModelRef, ModelRequest, ProviderModel, SupportLevel,
};
use secrecy::ExposeSecret;
use serde_json::Value;

use super::{
    GeminiConfig, GeminiConfigError, GeminiEmbeddingModel, GeminiEventDecoder, encode_request,
};

/// Native Gemini `GenerateContent` client.
#[derive(Clone, Debug)]
pub struct GeminiClient {
    config: GeminiConfig,
    http: Client,
    capabilities: ModelCapabilities,
}

impl GeminiClient {
    /// Creates a Gemini client.
    pub fn new(config: GeminiConfig) -> Self {
        Self {
            config,
            http: Client::new(),
            capabilities: capabilities(),
        }
    }

    /// Creates a `Gemini` client from one API key.
    ///
    /// Use [`Self::new`] for an explicit API base URL.
    ///
    /// # Errors
    ///
    /// Returns [`GeminiConfigError`] when the API key is blank or the built-in
    /// endpoint cannot be constructed.
    pub fn from_api_key(api_key: impl Into<String>) -> Result<Self, GeminiConfigError> {
        GeminiConfig::new(api_key).map(Self::new)
    }

    /// Replaces the HTTP transport.
    #[must_use]
    pub fn with_http_client(mut self, http: Client) -> Self {
        self.http = http;
        self
    }

    /// Binds this transport and credential set to one Gemini embedding model.
    ///
    /// # Errors
    ///
    /// Returns [`GeminiConfigError::EmptyEmbeddingModel`] for a blank model.
    pub fn embedding_model(
        &self,
        model: impl Into<String>,
    ) -> Result<GeminiEmbeddingModel, GeminiConfigError> {
        GeminiEmbeddingModel::new(self.config.clone(), self.http.clone(), model)
    }

    fn builder(
        &self,
        request: &ModelRequest,
        context: &ModelCallContext,
    ) -> Result<RequestBuilder, ModelError> {
        let endpoint = self
            .config
            .endpoint_url(&request.model.name)
            .map_err(|error| invalid(format!("invalid Gemini model endpoint: {error}")))?;
        let body = encode_request(request)?;
        let mut builder = self
            .http
            .post(endpoint)
            .header("accept", "text/event-stream")
            .header("x-client-request-id", context.invocation_id().to_string())
            .json(&body);
        if let Some(api_key) = &self.config.api_key {
            builder = builder.header("x-goog-api-key", api_key.expose_secret());
        }
        if let Some(remaining) = context.remaining() {
            builder = builder.timeout(remaining);
        }
        Ok(builder)
    }
}

impl Model for GeminiClient {
    fn capabilities<'a>(
        &'a self,
        _model: &'a ModelRef,
    ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
        let capabilities = self.capabilities.clone();
        Box::pin(async move { Ok(capabilities) })
    }

    fn stream(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
        Box::pin(async move {
            if !matches!(
                request.selected_response_mode(),
                runifold_model::ResponseMode::Streaming
            ) {
                return Err(ModelError::local(
                    ModelErrorKind::UnsupportedFeature,
                    "Gemini adapter currently requires streaming response mode",
                ));
            }
            if request.model.provider != "gemini" {
                return Err(invalid("Gemini client requires provider `gemini`"));
            }
            let warnings = self.capabilities.validate_request(&request, true)?;
            let cancellation = context.cancellation().clone();
            let deadline = context.deadline();
            let response = send(self.builder(&request, &context)?, &cancellation, deadline).await?;
            if !response.status().is_success() {
                return Err(http_error(response, &cancellation, deadline).await?);
            }
            Ok(stream_response(
                response,
                request.model.name,
                cancellation,
                warnings,
                deadline,
            ))
        })
    }
}

impl ProviderModel for GeminiClient {
    fn provider(&self) -> &'static str {
        "gemini"
    }
}

fn stream_response(
    response: Response,
    model: String,
    cancellation: CancellationToken,
    mut warnings: Vec<runifold_model::ModelWarning>,
    deadline: Option<Instant>,
) -> ModelEventStream {
    let mut source = response.bytes_stream().eventsource();
    let mut decoder = GeminiEventDecoder::new(model);
    Box::pin(async_stream::try_stream! {
        loop {
            let next = match select(Box::pin(cancellation.cancelled()), Box::pin(source.next())).await {
                Either::Left(_) => Err(cancelled())?,
                Either::Right((next, _)) => next,
            };
            let Some(event) = next else {
                decoder.finish()?;
                break;
            };
            let event = event.map_err(|error| match error {
                EventStreamError::Transport(error) => transport(&error, deadline),
                error => protocol(format!("invalid Gemini SSE: {error}")),
            })?;
            let payload: Value = serde_json::from_str(&event.data)
                .map_err(|error| protocol(format!("invalid Gemini SSE JSON: {error}")))?;
            for canonical in decoder.decode(&payload)? {
                let started = matches!(canonical, runifold_model::ModelStreamEvent::ResponseStarted { .. });
                yield canonical;
                if started {
                    for warning in std::mem::take(&mut warnings) {
                        yield runifold_model::ModelStreamEvent::Warning { warning };
                    }
                }
            }
        }
    })
}

async fn send(
    builder: RequestBuilder,
    cancellation: &CancellationToken,
    deadline: Option<Instant>,
) -> Result<Response, ModelError> {
    match select(Box::pin(cancellation.cancelled()), Box::pin(builder.send())).await {
        Either::Left(_) => Err(cancelled()),
        Either::Right((result, _)) => result.map_err(|error| transport(&error, deadline)),
    }
}

async fn http_error(
    response: Response,
    cancellation: &CancellationToken,
    deadline: Option<Instant>,
) -> Result<ModelError, ModelError> {
    let status = response.status();
    let text = match select(
        Box::pin(cancellation.cancelled()),
        Box::pin(response.text()),
    )
    .await
    {
        Either::Left(_) => return Err(cancelled()),
        Either::Right((result, _)) => result.map_err(|error| transport(&error, deadline))?,
    };
    let payload = serde_json::from_str::<Value>(&text).unwrap_or(Value::Null);
    let detail = payload.get("error").unwrap_or(&payload);
    let mut error = ModelError::local(
        ModelErrorKind::Provider,
        detail
            .get("message")
            .and_then(Value::as_str)
            .map_or_else(|| format!("Gemini returned HTTP {status}"), String::from),
    );
    error.provider = Some("gemini".into());
    crate::reliability::classify_status(status, &mut error);
    error
        .metadata
        .insert("http.status".into(), Value::from(status.as_u16()));
    if let Some(value) = detail.get("status") {
        error
            .metadata
            .insert("gemini.error.status".into(), value.clone());
    }
    Ok(error)
}

fn capabilities() -> ModelCapabilities {
    let native = || FeatureSupport::new(SupportLevel::Native);
    let unknown = || FeatureSupport::new(SupportLevel::Unknown);
    ModelCapabilities {
        streaming: native(),
        tools: native(),
        parallel_tools: unknown(),
        structured_output: native(),
        reasoning: native(),
        image_input: native(),
        audio_input: native(),
        document_input: native(),
        max_context_tokens: None,
        extensions: BTreeMap::new(),
    }
}

fn invalid(message: impl Into<String>) -> ModelError {
    ModelError::local(ModelErrorKind::InvalidRequest, message)
}

fn cancelled() -> ModelError {
    with_provider(ModelError::local(
        ModelErrorKind::Cancelled,
        "Gemini invocation was cancelled",
    ))
}

fn protocol(message: impl Into<String>) -> ModelError {
    with_provider(ModelError::local(ModelErrorKind::Protocol, message))
}

fn transport(error: &reqwest::Error, deadline: Option<Instant>) -> ModelError {
    let kind = if error.is_timeout() || deadline.is_some_and(|value| value <= Instant::now()) {
        ModelErrorKind::DeadlineExceeded
    } else {
        ModelErrorKind::Transport
    };
    let mut model_error = with_provider(ModelError::local(
        kind,
        format!("Gemini transport failed: {error}"),
    ));
    crate::reliability::classify_transport(error, &mut model_error);
    model_error
}

fn with_provider(mut error: ModelError) -> ModelError {
    error.provider = Some("gemini".into());
    error
}
