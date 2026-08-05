//! OpenAI-compatible runtime client.

use std::collections::BTreeMap;

use eventsource_stream::{EventStreamError, Eventsource};
use futures_util::{
    FutureExt, StreamExt,
    future::{Either, select},
    pin_mut,
};
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use runifold_core::{CancellationToken, Instant};
use runifold_model::{
    FeatureSupport, Model, ModelCallContext, ModelCapabilities, ModelError, ModelErrorKind,
    ModelEventStream, ModelFuture, ModelRef, ModelRequest, ModelStreamEvent, ModelWarning,
    ProviderModel, ResponseMode, SupportLevel,
};
use secrecy::ExposeSecret;
use serde_json::Value;

use super::{
    ChatCompletionsDecoder, OpenAiCompatibleProfile, OpenAiConfig, OpenAiConfigError,
    OpenAiControlPlane, OpenAiEmbeddingModel, OpenAiEventDecoder, OpenAiRealtimeClient,
    OpenAiRealtimeError, OpenAiWireProtocol,
    chat::{ChatEvents, encode_chat_request},
    decode::decode_complete_response,
    encode::encode_request_for,
};

const MAX_COMPLETE_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

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
        let capabilities = adapter_capabilities_for(&config);
        Self {
            config,
            http: Client::new(),
            capabilities,
        }
    }

    /// Creates a public `OpenAI` client from one API key.
    ///
    /// Use [`Self::new`] when the application needs an organization, project,
    /// custom endpoint, compatible provider, or explicit wire protocol.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError`] when the API key is blank or the built-in
    /// endpoint cannot be constructed.
    pub fn from_api_key(api_key: impl Into<String>) -> Result<Self, OpenAiConfigError> {
        OpenAiConfig::new(api_key).map(Self::new)
    }

    /// Creates a client from a verified OpenAI-compatible provider profile.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError`] when the API key is blank or the
    /// built-in endpoint cannot be constructed.
    pub fn from_profile(
        profile: OpenAiCompatibleProfile,
        api_key: impl Into<String>,
    ) -> Result<Self, OpenAiConfigError> {
        OpenAiConfig::from_profile(profile, api_key).map(Self::new)
    }

    /// Returns the canonical provider identity configured for this client.
    pub fn provider(&self) -> &str {
        &self.config.provider
    }

    /// Binds this transport and credential set to one embedding model.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiConfigError::EmptyEmbeddingModel`] for a blank model.
    pub fn embedding_model(
        &self,
        model: impl Into<String>,
    ) -> Result<OpenAiEmbeddingModel, OpenAiConfigError> {
        OpenAiEmbeddingModel::new(self.config.clone(), self.http.clone(), model)
    }

    /// Returns the typed model, file, and Batch control plane.
    #[must_use]
    pub fn control_plane(&self) -> OpenAiControlPlane {
        OpenAiControlPlane::new(self.config.clone(), self.http.clone())
    }

    /// Binds this transport configuration to one GA Realtime model.
    ///
    /// # Errors
    ///
    /// Rejects an empty, control-containing, or oversized model identity.
    pub fn realtime(
        &self,
        model: impl Into<String>,
    ) -> Result<OpenAiRealtimeClient, OpenAiRealtimeError> {
        OpenAiRealtimeClient::new(self.config.clone(), model)
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
        let streaming = matches!(request.selected_response_mode(), ResponseMode::Streaming);
        let warnings = self.capabilities.validate_request(request, streaming)?;
        let body = match self.config.wire_protocol {
            OpenAiWireProtocol::Responses => encode_request_for(request, &self.config.provider)?,
            OpenAiWireProtocol::ChatCompletions => {
                if !streaming {
                    return Err(ModelError::local(
                        ModelErrorKind::UnsupportedFeature,
                        "complete response mode is not yet supported by the Chat Completions adapter",
                    ));
                }
                encode_chat_request(request, &self.config.provider)?
            }
        };
        Ok((body, warnings))
    }

    fn request_builder(
        &self,
        body: &Value,
        context: &ModelCallContext,
        response_mode: ResponseMode,
    ) -> RequestBuilder {
        let accept = match response_mode {
            ResponseMode::Complete => "application/json",
            _ => "text/event-stream",
        };
        let mut builder = self
            .http
            .post(self.config.endpoint_url())
            .header("Accept", accept)
            .header("X-Client-Request-Id", context.invocation_id().to_string())
            .json(body);
        if let Some(api_key) = &self.config.api_key {
            builder = builder.bearer_auth(api_key.expose_secret());
        }
        if let Some(api_key) = &self.config.azure_api_key {
            builder = builder.header("api-key", api_key.expose_secret());
        }
        if let Some(organization) = &self.config.organization {
            builder = builder.header("OpenAI-Organization", organization);
        }
        if let Some(project) = &self.config.project {
            builder = builder.header("OpenAI-Project", project);
        }
        if let Some(application_url) = &self.config.application_url {
            builder = builder.header("HTTP-Referer", application_url.as_str());
        }
        if let Some(application_title) = &self.config.application_title {
            builder = builder.header("X-OpenRouter-Title", application_title);
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
            let response_mode = request.selected_response_mode();
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
                self.request_builder(&body, &context, response_mode),
                &cancellation,
                &self.config.provider,
                deadline,
            )
            .await?;
            let status = response.status();
            let request_id = request_id(&response);
            if !status.is_success() {
                let retry_after = retry_after(&response);
                let payload =
                    read_error_body(response, &cancellation, &self.config.provider, deadline)
                        .await?;
                return Err(http_error(
                    status,
                    request_id,
                    &payload,
                    &self.config.provider,
                    retry_after,
                ));
            }
            let decoder_config = DecoderConfig {
                protocol: self.config.wire_protocol,
                provider: self.config.provider.clone(),
                request_id,
                deadline,
            };
            match response_mode {
                ResponseMode::Complete => {
                    let payload = read_complete_body(
                        response,
                        &cancellation,
                        &decoder_config.provider,
                        deadline,
                    )
                    .await?;
                    let events = decode_complete_response(
                        &decoder_config.provider,
                        &payload,
                        decoder_config.request_id,
                    )?;
                    Ok(complete_event_stream(events, warnings))
                }
                _ => Ok(event_stream(
                    response,
                    decoder_config,
                    cancellation,
                    warnings,
                )),
            }
        })
    }
}

fn complete_event_stream(
    events: Vec<ModelStreamEvent>,
    mut warnings: Vec<ModelWarning>,
) -> ModelEventStream {
    let mut normalized = Vec::with_capacity(events.len().saturating_add(warnings.len()));
    for event in events {
        let started = matches!(event, ModelStreamEvent::ResponseStarted { .. });
        normalized.push(Ok(event));
        if started {
            normalized.extend(
                std::mem::take(&mut warnings)
                    .into_iter()
                    .map(|warning| Ok(ModelStreamEvent::Warning { warning })),
            );
        }
    }
    Box::pin(futures_util::stream::iter(normalized))
}

async fn read_complete_body(
    response: Response,
    cancellation: &CancellationToken,
    provider: &str,
    deadline: Option<Instant>,
) -> Result<Value, ModelError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_COMPLETE_RESPONSE_BYTES as u64)
    {
        return Err(protocol_error(
            provider,
            format!("complete response exceeds {MAX_COMPLETE_RESPONSE_BYTES} bytes"),
        ));
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    loop {
        let cancellation_wait = cancellation.cancelled().fuse();
        let next = stream.next().fuse();
        pin_mut!(cancellation_wait, next);
        let chunk = futures_util::select_biased! {
            () = cancellation_wait => return Err(cancelled()),
            chunk = next => chunk,
        };
        let Some(chunk) = chunk else {
            break;
        };
        let chunk = chunk.map_err(|error| transport_error(&error, provider, deadline))?;
        if body.len().saturating_add(chunk.len()) > MAX_COMPLETE_RESPONSE_BYTES {
            return Err(protocol_error(
                provider,
                format!("complete response exceeds {MAX_COMPLETE_RESPONSE_BYTES} bytes"),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|error| {
        protocol_error(
            provider,
            format!("complete response is not valid JSON: {error}"),
        )
    })
}

impl ProviderModel for OpenAiClient {
    fn provider(&self) -> &str {
        self.provider()
    }
}

struct DecoderConfig {
    protocol: OpenAiWireProtocol,
    provider: String,
    request_id: Option<String>,
    deadline: Option<Instant>,
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
        let cancellation_wait = cancellation.cancelled().fuse();
        pin_mut!(cancellation_wait);
        loop {
            let source_next = source.next().fuse();
            pin_mut!(source_next);
            let selected = futures_util::select_biased! {
                () = cancellation_wait => Err(cancelled()),
                next = source_next => Ok(next),
            };
            let next = selected?;
            let Some(event) = next else {
                for canonical in decoder.finish()? {
                    yield canonical;
                }
                break;
            };
            let event = event.map_err(|error| match error {
                EventStreamError::Transport(error) => {
                    transport_error(&error, &provider, config.deadline)
                }
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
                    ModelStreamEvent::ResponseStarted { .. }
                );
                yield canonical;
                if started {
                    for warning in std::mem::take(&mut warnings) {
                        yield ModelStreamEvent::Warning { warning };
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
    deadline: Option<Instant>,
) -> Result<Response, ModelError> {
    let cancellation_wait = cancellation.cancelled();
    let request = builder.send();
    pin_mut!(cancellation_wait, request);
    match select(cancellation_wait, request).await {
        Either::Left(_) => Err(cancelled()),
        Either::Right((result, _)) => {
            result.map_err(|error| transport_error(&error, provider, deadline))
        }
    }
}

async fn read_error_body(
    response: Response,
    cancellation: &CancellationToken,
    provider: &str,
    deadline: Option<Instant>,
) -> Result<String, ModelError> {
    let cancellation_wait = cancellation.cancelled();
    let body = response.text();
    pin_mut!(cancellation_wait, body);
    match select(cancellation_wait, body).await {
        Either::Left(_) => Err(cancelled()),
        Either::Right((result, _)) => {
            result.map_err(|error| transport_error(&error, provider, deadline))
        }
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
    fn decode(&mut self, payload: Value) -> Result<ChatEvents, ModelError> {
        match self {
            Self::Responses(decoder) => decoder
                .decode(payload)
                .map(|events| events.into_iter().collect()),
            Self::Chat(decoder) => decoder.decode_compact(payload),
        }
    }

    fn finish(&mut self) -> Result<ChatEvents, ModelError> {
        match self {
            Self::Responses(_) => Ok(ChatEvents::new()),
            Self::Chat(decoder) => decoder.finish_compact(),
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

fn adapter_capabilities_for(config: &OpenAiConfig) -> ModelCapabilities {
    let mut capabilities = adapter_capabilities();
    if matches!(config.provider.as_str(), "openai" | "ark")
        && config.wire_protocol == OpenAiWireProtocol::Responses
    {
        let native = || FeatureSupport::new(SupportLevel::Native);
        capabilities.tools = native();
        capabilities.structured_output = native();
        capabilities.image_input = native();
        capabilities.document_input = native();
    }
    if config.provider == "ark" && config.wire_protocol == OpenAiWireProtocol::Responses {
        capabilities.reasoning = FeatureSupport::new(SupportLevel::Native);
        capabilities.extensions.insert(
            "ark.web_search".into(),
            FeatureSupport::new(SupportLevel::Native),
        );
        capabilities.extensions.insert(
            "ark.reasoning_generation".into(),
            FeatureSupport::new(SupportLevel::Native),
        );
    }
    capabilities
}

fn cancelled() -> ModelError {
    ModelError::local(ModelErrorKind::Cancelled, "OpenAI invocation was cancelled")
}

fn protocol_error(provider: &str, message: impl Into<String>) -> ModelError {
    let mut error = ModelError::local(ModelErrorKind::Protocol, message);
    error.provider = Some(provider.into());
    error
}

fn transport_error(
    error: &reqwest::Error,
    provider: &str,
    deadline: Option<Instant>,
) -> ModelError {
    let kind = if error.is_timeout() || deadline.is_some_and(|deadline| deadline <= Instant::now())
    {
        ModelErrorKind::DeadlineExceeded
    } else {
        ModelErrorKind::Transport
    };
    let mut model_error = ModelError::local(kind, format!("OpenAI transport failed: {error}"));
    model_error.provider = Some(provider.into());
    crate::reliability::classify_transport(error, &mut model_error);
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
    crate::reliability::classify_status(status, &mut model_error);
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
    use super::super::OpenAiConfig;
    use super::{OpenAiClient, adapter_capabilities, adapter_capabilities_for, http_error};
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

    #[test]
    fn ark_responses_declares_verified_protocol_capabilities() {
        let config = OpenAiConfig::ark("key").unwrap();
        let capabilities = adapter_capabilities_for(&config);

        assert_eq!(capabilities.tools.level, SupportLevel::Native);
        assert_eq!(capabilities.structured_output.level, SupportLevel::Native);
        assert_eq!(capabilities.image_input.level, SupportLevel::Native);
        assert_eq!(capabilities.document_input.level, SupportLevel::Native);
        assert_eq!(
            capabilities.extensions["ark.web_search"].level,
            SupportLevel::Native
        );
        assert_eq!(
            capabilities.extensions["ark.reasoning_generation"].level,
            SupportLevel::Native
        );
        assert_eq!(capabilities.reasoning.level, SupportLevel::Native);
    }

    #[test]
    fn public_openai_responses_declares_implemented_wire_features() {
        let config = OpenAiConfig::new("key").unwrap();
        let capabilities = adapter_capabilities_for(&config);

        assert_eq!(capabilities.tools.level, SupportLevel::Native);
        assert_eq!(capabilities.structured_output.level, SupportLevel::Native);
        assert_eq!(capabilities.image_input.level, SupportLevel::Native);
        assert_eq!(capabilities.document_input.level, SupportLevel::Native);
    }
}
