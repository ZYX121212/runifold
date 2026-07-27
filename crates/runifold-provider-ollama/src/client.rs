use std::collections::BTreeMap;

use futures_util::{
    StreamExt,
    future::{Either, select},
};
use reqwest::{Client, RequestBuilder, Response};
use runifold_core::CancellationToken;
use runifold_model::{
    FeatureSupport, Model, ModelCallContext, ModelCapabilities, ModelError, ModelErrorKind,
    ModelEventStream, ModelFuture, ModelRef, ModelRequest, SupportLevel,
};
use secrecy::ExposeSecret;
use serde_json::Value;

use crate::{OllamaChunkDecoder, OllamaConfig, encode_request};

/// Native Ollama `/api/chat` client.
#[derive(Clone, Debug)]
pub struct OllamaClient {
    config: OllamaConfig,
    http: Client,
    capabilities: ModelCapabilities,
}

impl OllamaClient {
    /// Creates an Ollama client.
    pub fn new(config: OllamaConfig) -> Self {
        Self {
            config,
            http: Client::new(),
            capabilities: capabilities(),
        }
    }

    /// Replaces the HTTP transport.
    #[must_use]
    pub fn with_http_client(mut self, http: Client) -> Self {
        self.http = http;
        self
    }
}

impl Model for OllamaClient {
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
            if request.model.provider != "ollama" {
                return Err(invalid("Ollama client requires provider `ollama`"));
            }
            let warnings = self.capabilities.validate_request(&request, true)?;
            let body = encode_request(&request)?;
            let mut builder = self
                .http
                .post(self.config.endpoint_url())
                .header("accept", "application/x-ndjson")
                .header("x-client-request-id", context.invocation_id().to_string())
                .json(&body);
            if let Some(token) = &self.config.bearer_token {
                builder = builder.bearer_auth(token.expose_secret());
            }
            if let Some(remaining) = context.remaining() {
                builder = builder.timeout(remaining);
            }
            let cancellation = context.cancellation().clone();
            let response = send(builder, &cancellation).await?;
            if !response.status().is_success() {
                return Err(http_error(response, &cancellation).await?);
            }
            Ok(stream_response(
                response,
                request.model.name,
                cancellation,
                warnings,
            ))
        })
    }
}

fn stream_response(
    response: Response,
    model: String,
    cancellation: CancellationToken,
    mut warnings: Vec<runifold_model::ModelWarning>,
) -> ModelEventStream {
    let mut source = response.bytes_stream();
    let mut decoder = OllamaChunkDecoder::new(model);
    Box::pin(async_stream::try_stream! {
        let mut buffer = Vec::new();
        loop {
            let next = match select(Box::pin(cancellation.cancelled()), Box::pin(source.next())).await {
                Either::Left(_) => Err(cancelled())?,
                Either::Right((next, _)) => next,
            };
            match next {
                Some(Ok(bytes)) => {
                    buffer.extend_from_slice(&bytes);
                    while let Some(position) = buffer.iter().position(|byte| *byte == b'\n') {
                        let line = buffer.drain(..=position).collect::<Vec<_>>();
                        let line = &line[..line.len().saturating_sub(1)];
                        if line.iter().all(u8::is_ascii_whitespace) {
                            continue;
                        }
                        for event in decode_line(&mut decoder, line)? {
                            let started = matches!(event, runifold_model::ModelStreamEvent::ResponseStarted { .. });
                            yield event;
                            if started {
                                for warning in std::mem::take(&mut warnings) {
                                    yield runifold_model::ModelStreamEvent::Warning { warning };
                                }
                            }
                        }
                    }
                }
                Some(Err(error)) => Err(transport(&error))?,
                None => {
                    if !buffer.iter().all(u8::is_ascii_whitespace) {
                        for event in decode_line(&mut decoder, &buffer)? {
                            yield event;
                        }
                    }
                    decoder.finish()?;
                    break;
                }
            }
        }
    })
}

fn decode_line(
    decoder: &mut OllamaChunkDecoder,
    line: &[u8],
) -> Result<Vec<runifold_model::ModelStreamEvent>, ModelError> {
    let payload = serde_json::from_slice(line)
        .map_err(|error| protocol(format!("invalid Ollama NDJSON: {error}")))?;
    decoder.decode(&payload)
}

async fn send(
    builder: RequestBuilder,
    cancellation: &CancellationToken,
) -> Result<Response, ModelError> {
    match select(Box::pin(cancellation.cancelled()), Box::pin(builder.send())).await {
        Either::Left(_) => Err(cancelled()),
        Either::Right((result, _)) => result.map_err(|error| transport(&error)),
    }
}

async fn http_error(
    response: Response,
    cancellation: &CancellationToken,
) -> Result<ModelError, ModelError> {
    let status = response.status();
    let text = match select(
        Box::pin(cancellation.cancelled()),
        Box::pin(response.text()),
    )
    .await
    {
        Either::Left(_) => return Err(cancelled()),
        Either::Right((result, _)) => result.map_err(|error| transport(&error))?,
    };
    let payload = serde_json::from_str::<Value>(&text).unwrap_or(Value::Null);
    let message = payload
        .get("error")
        .and_then(Value::as_str)
        .map_or_else(|| format!("Ollama returned HTTP {status}"), String::from);
    let mut error = ModelError::local(ModelErrorKind::Provider, message);
    error.provider = Some("ollama".into());
    error
        .metadata
        .insert("http.status".into(), Value::from(status.as_u16()));
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
        audio_input: FeatureSupport::new(SupportLevel::Unsupported),
        document_input: FeatureSupport::new(SupportLevel::Unsupported),
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
        "Ollama invocation was cancelled",
    ))
}

fn protocol(message: impl Into<String>) -> ModelError {
    with_provider(ModelError::local(ModelErrorKind::Protocol, message))
}

fn transport(error: &reqwest::Error) -> ModelError {
    let kind = if error.is_timeout() {
        ModelErrorKind::DeadlineExceeded
    } else {
        ModelErrorKind::Transport
    };
    with_provider(ModelError::local(
        kind,
        format!("Ollama transport failed: {error}"),
    ))
}

fn with_provider(mut error: ModelError) -> ModelError {
    error.provider = Some("ollama".into());
    error
}
