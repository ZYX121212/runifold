use std::collections::BTreeMap;

use eventsource_stream::{EventStreamError, Eventsource};
use futures_util::{
    StreamExt,
    future::{Either, select},
};
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use runifold_core::CancellationToken;
use runifold_model::{
    FeatureSupport, Model, ModelCallContext, ModelCapabilities, ModelError, ModelErrorKind,
    ModelEventStream, ModelFuture, ModelRef, ModelRequest, ModelWarning, SupportLevel,
};
use secrecy::ExposeSecret;
use serde_json::Value;

use crate::{
    ChatCompletionsDecoder, OpenAiConfig, OpenAiEventDecoder, OpenAiWireProtocol,
    chat::encode_chat_request, encode::encode_request_for,
};

/// `OpenAI` Responses API implementation of Runifold's [`Model`] boundary.
#[derive(Clone, Debug)]
pub struct OpenAiClient {
    config: OpenAiConfig,
    http: Client,
    capabilities: ModelCapabilities,
}

impl OpenAiClient {
    /// Creates a client with a pooled Rustls HTTP transport.
    pub fn new(config: OpenAiConfig) -> Self {
        Self {
            config,
            http: Client::new(),
            capabilities: adapter_capabilities(),
        }
    }

    /// Returns the canonical provider identity configured for this client.
    pub fn provider(&self) -> &str {
        &self.config.provider
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
        if request.model.provider != self.config.provider {
            return Err(ModelError::local(
                ModelErrorKind::InvalidRequest,
                format!(
                    "client for provider `{}` cannot invoke provider `{}`",
                    self.config.provider, request.model.provider
                ),
            ));
        }
        let warnings = self.capabilities.validate_request(request, true)?;
        let body = match self.config.wire_protocol {
            OpenAiWireProtocol::Responses => encode_request_for(request, &self.config.provider)?,
            OpenAiWireProtocol::ChatCompletions => {
                encode_chat_request(request, &self.config.provider)?
            }
        };
        Ok((body, warnings))
    }

    fn request_builder(&self, body: &Value, context: &ModelCallContext) -> RequestBuilder {
        let mut builder = self
            .http
            .post(self.config.endpoint_url())
            .header("Accept", "text/event-stream")
            .header("X-Client-Request-Id", context.invocation_id().to_string())
            .json(body);
        if let Some(api_key) = &self.config.api_key {
            builder = builder.bearer_auth(api_key.expose_secret());
        }
        if let Some(organization) = &self.config.organization {
            builder = builder.header("OpenAI-Organization", organization);
        }
        if let Some(project) = &self.config.project {
            builder = builder.header("OpenAI-Project", project);
        }
        if let Some(remaining) = context.remaining() {
            builder = builder.timeout(remaining);
        }
        builder
    }
}

impl Model for OpenAiClient {
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
            let response = send_request(
                self.request_builder(&body, &context),
                &cancellation,
                &self.config.provider,
            )
            .await?;
            let status = response.status();
            let request_id = request_id(&response);
            if !status.is_success() {
                let retry_after = retry_after(&response);
                let payload =
                    read_error_body(response, &cancellation, &self.config.provider).await?;
                return Err(http_error(
                    status,
                    request_id,
                    &payload,
                    &self.config.provider,
                    retry_after,
                ));
            }
            Ok(event_stream(
                response,
                DecoderConfig {
                    protocol: self.config.wire_protocol,
                    provider: self.config.provider.clone(),
                    request_id,
                },
                cancellation,
                warnings,
            ))
        })
    }
}

struct DecoderConfig {
    protocol: OpenAiWireProtocol,
    provider: String,
    request_id: Option<String>,
}

fn event_stream(
    response: Response,
    config: DecoderConfig,
    cancellation: CancellationToken,
    mut warnings: Vec<ModelWarning>,
) -> ModelEventStream {
    let mut source = response.bytes_stream().eventsource();
    let provider = config.provider.clone();
    let mut decoder = match config.protocol {
        OpenAiWireProtocol::Responses => WireDecoder::Responses(
            OpenAiEventDecoder::for_provider(&config.provider).with_request_id(config.request_id),
        ),
        OpenAiWireProtocol::ChatCompletions => {
            WireDecoder::Chat(ChatCompletionsDecoder::new(&config.provider))
        }
    };
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
                for canonical in decoder.finish()? {
                    yield canonical;
                }
                break;
            };
            let event = event.map_err(|error| match error {
                EventStreamError::Transport(error) => transport_error(&error, &provider),
                error => protocol_error(&provider, format!("invalid SSE frame: {error}")),
            })?;
            if event.data == "[DONE]" {
                for canonical in decoder.finish()? {
                    yield canonical;
                }
                break;
            }
            let payload: Value = serde_json::from_str(&event.data).map_err(|error| {
                protocol_error(&provider, format!("SSE data is not valid JSON: {error}"))
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
    provider: &str,
) -> Result<Response, ModelError> {
    match select(Box::pin(cancellation.cancelled()), Box::pin(builder.send())).await {
        Either::Left(_) => Err(cancelled()),
        Either::Right((result, _)) => result.map_err(|error| transport_error(&error, provider)),
    }
}

async fn read_error_body(
    response: Response,
    cancellation: &CancellationToken,
    provider: &str,
) -> Result<String, ModelError> {
    match select(
        Box::pin(cancellation.cancelled()),
        Box::pin(response.text()),
    )
    .await
    {
        Either::Left(_) => Err(cancelled()),
        Either::Right((result, _)) => result.map_err(|error| transport_error(&error, provider)),
    }
}

fn request_id(response: &Response) -> Option<String> {
    response
        .headers()
        .get("x-request-id")
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

enum WireDecoder {
    Responses(OpenAiEventDecoder),
    Chat(ChatCompletionsDecoder),
}

impl WireDecoder {
    fn decode(
        &mut self,
        payload: Value,
    ) -> Result<Vec<runifold_model::ModelStreamEvent>, ModelError> {
        match self {
            Self::Responses(decoder) => decoder.decode(payload),
            Self::Chat(decoder) => decoder.decode(payload),
        }
    }

    fn finish(&mut self) -> Result<Vec<runifold_model::ModelStreamEvent>, ModelError> {
        match self {
            Self::Responses(_) => Ok(Vec::new()),
            Self::Chat(decoder) => decoder.finish(),
        }
    }
}

fn adapter_capabilities() -> ModelCapabilities {
    let native = || FeatureSupport::new(SupportLevel::Native);
    let unsupported = || FeatureSupport::new(SupportLevel::Unsupported);
    let unknown = || FeatureSupport::new(SupportLevel::Unknown);
    ModelCapabilities {
        streaming: native(),
        tools: unknown(),
        parallel_tools: unknown(),
        structured_output: unknown(),
        reasoning: unknown(),
        image_input: unknown(),
        audio_input: unsupported(),
        document_input: unknown(),
        max_context_tokens: None,
        extensions: BTreeMap::new(),
    }
}

fn cancelled() -> ModelError {
    ModelError::local(ModelErrorKind::Cancelled, "OpenAI invocation was cancelled")
}

fn protocol_error(provider: &str, message: impl Into<String>) -> ModelError {
    let mut error = ModelError::local(ModelErrorKind::Protocol, message);
    error.provider = Some(provider.into());
    error
}

fn transport_error(error: &reqwest::Error, provider: &str) -> ModelError {
    let kind = if error.is_timeout() {
        ModelErrorKind::DeadlineExceeded
    } else {
        ModelErrorKind::Transport
    };
    let mut model_error = ModelError::local(kind, format!("OpenAI transport failed: {error}"));
    model_error.provider = Some(provider.into());
    model_error
}

fn http_error(
    status: StatusCode,
    request_id: Option<String>,
    body: &str,
    provider: &str,
    retry_after: Option<std::time::Duration>,
) -> ModelError {
    let parsed = serde_json::from_str::<Value>(body).ok();
    let error = parsed
        .as_ref()
        .and_then(|payload| payload.get("error"))
        .unwrap_or(&Value::Null);
    let message = error.get("message").and_then(Value::as_str).map_or_else(
        || format!("OpenAI returned HTTP {status}"),
        std::convert::Into::into,
    );
    let mut model_error = ModelError::local(ModelErrorKind::Provider, message);
    model_error.provider = Some(provider.into());
    model_error
        .metadata
        .insert("http.status".into(), Value::from(status.as_u16()));
    if let Some(request_id) = request_id {
        model_error
            .metadata
            .insert("openai.request_id".into(), Value::String(request_id));
    }
    if let Some(retry_after) = retry_after {
        model_error.metadata.insert(
            "retry.after_ms".into(),
            Value::from(u64::try_from(retry_after.as_millis()).unwrap_or(u64::MAX)),
        );
    }
    for field in ["type", "code", "param"] {
        if let Some(value) = error.get(field) {
            model_error
                .metadata
                .insert(format!("openai.error.{field}"), value.clone());
        }
    }
    model_error
}

#[cfg(test)]
mod tests {
    use super::{OpenAiClient, adapter_capabilities, http_error};
    use crate::OpenAiConfig;
    use reqwest::StatusCode;
    use runifold_model::{ModelErrorKind, SupportLevel};

    #[test]
    fn http_errors_keep_request_ids_and_structured_codes() {
        let error = http_error(
            StatusCode::TOO_MANY_REQUESTS,
            Some("req_123".into()),
            r#"{"error":{"message":"rate limited","type":"rate_limit","code":"slow_down"}}"#,
            "openai",
            Some(std::time::Duration::from_secs(2)),
        );

        assert_eq!(error.kind, ModelErrorKind::Provider);
        assert_eq!(error.metadata["openai.request_id"], "req_123");
        assert_eq!(error.metadata["openai.error.code"], "slow_down");
        assert_eq!(error.metadata["retry.after_ms"], 2_000);
    }

    #[test]
    fn protocol_capabilities_do_not_claim_unknown_model_limits() {
        let capabilities = adapter_capabilities();

        assert_eq!(capabilities.streaming.level, SupportLevel::Native);
        assert_eq!(capabilities.tools.level, SupportLevel::Unknown);
        assert_eq!(capabilities.audio_input.level, SupportLevel::Unsupported);
        assert_eq!(capabilities.reasoning.level, SupportLevel::Unknown);
        assert_eq!(capabilities.max_context_tokens, None);
    }

    #[test]
    fn client_exposes_configured_provider_identity() {
        let client = OpenAiClient::new(OpenAiConfig::ark("key").unwrap());

        assert_eq!(client.provider(), "ark");
    }
}
