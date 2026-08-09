//! Anthropic runtime client.

use std::collections::BTreeMap;

use eventsource_stream::{EventStreamError, Eventsource};
use futures_util::{
    StreamExt,
    future::{Either, select},
};
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use runifold_core::{CancellationToken, Instant};
use runifold_model::{
    FeatureSupport, Model, ModelCallContext, ModelCapabilities, ModelError, ModelErrorKind,
    ModelEventStream, ModelFuture, ModelRef, ModelRequest, ModelWarning, ProviderModel,
    SupportLevel,
};
use secrecy::ExposeSecret;
use serde_json::Value;

use super::{AnthropicConfig, AnthropicConfigError, AnthropicEventDecoder, encode_request};

/// Native Anthropic Messages API implementation of Runifold's [`Model`] boundary.
#[derive(Clone, Debug)]
pub struct AnthropicClient {
    config: AnthropicConfig,
    http: Client,
    capabilities: ModelCapabilities,
}

impl AnthropicClient {
    /// Creates a client with a pooled Rustls HTTP transport.
    pub fn new(config: AnthropicConfig) -> Self {
        Self {
            config,
            http: Client::new(),
            capabilities: adapter_capabilities(),
        }
    }

    /// Creates an `Anthropic` client from one API key.
    ///
    /// Use [`Self::new`] for explicit API versions, beta features, endpoint
    /// overrides, and output limits.
    ///
    /// # Errors
    ///
    /// Returns [`AnthropicConfigError`] when the API key is blank or the
    /// built-in endpoint cannot be constructed.
    pub fn from_api_key(api_key: impl Into<String>) -> Result<Self, AnthropicConfigError> {
        AnthropicConfig::new(api_key).map(Self::new)
    }

    /// Replaces the HTTP client, primarily for transport policy and testing.
    #[must_use]
    pub fn with_http_client(mut self, http: Client) -> Self {
        self.http = http;
        self
    }

    /// Declares model-specific capabilities known by the application.
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: ModelCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    fn prepare(&self, request: &ModelRequest) -> Result<(Value, Vec<ModelWarning>), ModelError> {
        if !matches!(
            request.selected_response_mode(),
            runifold_model::ResponseMode::Streaming
        ) {
            return Err(ModelError::local(
                ModelErrorKind::UnsupportedFeature,
                "Anthropic adapter currently requires streaming response mode",
            ));
        }
        if request.model.provider != "anthropic" {
            return Err(ModelError::local(
                ModelErrorKind::InvalidRequest,
                format!(
                    "Anthropic client cannot invoke provider `{}`",
                    request.model.provider
                ),
            ));
        }
        let warnings = self.capabilities.validate_request(request, true)?;
        let body = encode_request(request, self.config.default_max_tokens)?;
        Ok((body, warnings))
    }

    fn request_builder(&self, body: &Value, context: &ModelCallContext) -> RequestBuilder {
        let mut builder = self
            .http
            .post(self.config.endpoint_url())
            .header("Accept", "text/event-stream")
            .header("anthropic-version", &self.config.api_version)
            .header("x-client-request-id", context.invocation_id().to_string())
            .json(body);
        if let Some(api_key) = &self.config.api_key {
            builder = builder.header("x-api-key", api_key.expose_secret());
        }
        if !self.config.beta_features.is_empty() {
            builder = builder.header("anthropic-beta", self.config.beta_features.join(","));
        }
        if let Some(remaining) = context.remaining() {
            builder = builder.timeout(remaining);
        }
        builder
    }
}

impl Model for AnthropicClient {
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
            let (body, warnings) = self.prepare(&request)?;
            if context
                .remaining()
                .is_some_and(|remaining| remaining.is_zero())
            {
                return Err(ModelError::local(
                    ModelErrorKind::DeadlineExceeded,
                    "model invocation deadline elapsed before transport start",
                ));
            }
            let cancellation = context.cancellation().clone();
            let deadline = context.deadline();
            let response = send_request(
                self.request_builder(&body, &context),
                &cancellation,
                deadline,
            )
            .await?;
            let status = response.status();
            let request_id = request_id(&response);
            if !status.is_success() {
                let retry_after = retry_after(&response);
                let payload = read_error_body(response, &cancellation, deadline).await?;
                return Err(http_error(status, request_id, &payload, retry_after));
            }
            Ok(event_stream(
                response,
                request_id,
                cancellation,
                warnings,
                deadline,
            ))
        })
    }
}

impl ProviderModel for AnthropicClient {
    fn provider(&self) -> &'static str {
        "anthropic"
    }
}

fn event_stream(
    response: Response,
    request_id: Option<String>,
    cancellation: CancellationToken,
    mut warnings: Vec<ModelWarning>,
    deadline: Option<Instant>,
) -> ModelEventStream {
    let mut source = response.bytes_stream().eventsource();
    let mut decoder = AnthropicEventDecoder::new().with_request_id(request_id);
    Box::pin(async_stream::try_stream! {
        loop {
            let next = match select(
                Box::pin(cancellation.cancelled()),
                Box::pin(source.next()),
            ).await {
                Either::Left(_) => Err(cancelled())?,
                Either::Right((next, _)) => next,
            };
            let Some(event) = next else {
                decoder.finish()?;
                break;
            };
            let event = event.map_err(|error| match error {
                EventStreamError::Transport(error) => transport_error(&error, deadline),
                error => protocol_error(format!("invalid Anthropic SSE frame: {error}")),
            })?;
            let payload: Value = serde_json::from_str(&event.data).map_err(|error| {
                protocol_error(format!("Anthropic SSE data is not valid JSON: {error}"))
            })?;
            for canonical in decoder.decode(payload)? {
                let started = matches!(
                    canonical,
                    runifold_model::ModelStreamEvent::ResponseStarted { .. }
                );
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

async fn send_request(
    builder: RequestBuilder,
    cancellation: &CancellationToken,
    deadline: Option<Instant>,
) -> Result<Response, ModelError> {
    match select(Box::pin(cancellation.cancelled()), Box::pin(builder.send())).await {
        Either::Left(_) => Err(cancelled()),
        Either::Right((result, _)) => result.map_err(|error| transport_error(&error, deadline)),
    }
}

async fn read_error_body(
    response: Response,
    cancellation: &CancellationToken,
    deadline: Option<Instant>,
) -> Result<String, ModelError> {
    match select(
        Box::pin(cancellation.cancelled()),
        Box::pin(response.text()),
    )
    .await
    {
        Either::Left(_) => Err(cancelled()),
        Either::Right((result, _)) => result.map_err(|error| transport_error(&error, deadline)),
    }
}

fn request_id(response: &Response) -> Option<String> {
    response
        .headers()
        .get("request-id")
        .or_else(|| response.headers().get("x-request-id"))
        .and_then(|value| value.to_str().ok())
        .map(String::from)
}

fn retry_after(response: &Response) -> Option<std::time::Duration> {
    let value = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?;
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(std::time::Duration::from_secs(seconds));
    }
    httpdate::parse_http_date(value)
        .ok()?
        .duration_since(std::time::SystemTime::now())
        .ok()
}

fn adapter_capabilities() -> ModelCapabilities {
    let native = || FeatureSupport::new(SupportLevel::Native);
    let unsupported = || FeatureSupport::new(SupportLevel::Unsupported);
    let emulated = || FeatureSupport::new(SupportLevel::Emulated);
    let unknown = || FeatureSupport::new(SupportLevel::Unknown);
    ModelCapabilities {
        streaming: native(),
        tools: native(),
        parallel_tools: unknown(),
        structured_output: unsupported(),
        reasoning: native(),
        image_input: native(),
        audio_input: emulated(),
        document_input: emulated(),
        max_context_tokens: None,
        extensions: BTreeMap::new(),
    }
}

fn cancelled() -> ModelError {
    let mut error = ModelError::local(
        ModelErrorKind::Cancelled,
        "Anthropic invocation was cancelled",
    );
    error.provider = Some("anthropic".into());
    error
}

fn protocol_error(message: impl Into<String>) -> ModelError {
    let mut error = ModelError::local(ModelErrorKind::Protocol, message);
    error.provider = Some("anthropic".into());
    error
}

fn transport_error(error: &reqwest::Error, deadline: Option<Instant>) -> ModelError {
    let kind = if error.is_timeout() || deadline.is_some_and(|value| value <= Instant::now()) {
        ModelErrorKind::DeadlineExceeded
    } else {
        ModelErrorKind::Transport
    };
    let mut model_error = ModelError::local(kind, format!("Anthropic transport failed: {error}"));
    model_error.provider = Some("anthropic".into());
    crate::reliability::classify_transport(error, &mut model_error);
    model_error
}

fn http_error(
    status: StatusCode,
    request_id: Option<String>,
    body: &str,
    retry_after: Option<std::time::Duration>,
) -> ModelError {
    let parsed = serde_json::from_str::<Value>(body).ok();
    let error = parsed
        .as_ref()
        .and_then(|payload| payload.get("error"))
        .unwrap_or(&Value::Null);
    let message = error.get("message").and_then(Value::as_str).map_or_else(
        || format!("Anthropic returned HTTP {status}"),
        std::convert::Into::into,
    );
    let mut model_error = ModelError::local(ModelErrorKind::Provider, message);
    model_error.provider = Some("anthropic".into());
    crate::reliability::classify_status(status, &mut model_error);
    model_error
        .metadata
        .insert("http.status".into(), Value::from(status.as_u16()));
    if let Some(request_id) = request_id {
        model_error
            .metadata
            .insert("anthropic.request_id".into(), Value::String(request_id));
    }
    if let Some(retry_after) = retry_after {
        model_error.metadata.insert(
            "retry.after_ms".into(),
            Value::from(u64::try_from(retry_after.as_millis()).unwrap_or(u64::MAX)),
        );
    }
    if let Some(error_type) = error.get("type") {
        model_error
            .metadata
            .insert("anthropic.error.type".into(), error_type.clone());
    }
    model_error
}

#[cfg(test)]
mod tests {
    use reqwest::StatusCode;
    use runifold_model::{ModelErrorKind, SupportLevel};

    use super::{adapter_capabilities, http_error};

    #[test]
    fn http_errors_keep_retry_and_request_metadata() {
        let error = http_error(
            StatusCode::TOO_MANY_REQUESTS,
            Some("req_123".into()),
            r#"{"error":{"type":"rate_limit_error","message":"slow down"}}"#,
            Some(std::time::Duration::from_secs(2)),
        );

        assert_eq!(error.kind, ModelErrorKind::Provider);
        assert_eq!(error.metadata["anthropic.request_id"], "req_123");
        assert_eq!(error.metadata["anthropic.error.type"], "rate_limit_error");
        assert_eq!(error.metadata["retry.after_ms"], 2_000);
    }

    #[test]
    fn capabilities_only_claim_implemented_features() {
        let capabilities = adapter_capabilities();

        assert_eq!(capabilities.streaming.level, SupportLevel::Native);
        assert_eq!(capabilities.tools.level, SupportLevel::Native);
        assert_eq!(
            capabilities.structured_output.level,
            SupportLevel::Unsupported
        );
        assert_eq!(capabilities.audio_input.level, SupportLevel::Emulated);
        assert_eq!(capabilities.document_input.level, SupportLevel::Emulated);
    }
}
