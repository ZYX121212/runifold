//! Typed OpenAI-compatible control-plane operations.

mod batch;
mod files;
mod realtime;

use std::{future::Future, pin::Pin};

use futures_util::{
    StreamExt,
    future::{Either, select},
};
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use runifold_core::Instant;
use runifold_model::ModelCallContext;
use secrecy::ExposeSecret;
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Value;
use thiserror::Error;
use url::Url;

use super::OpenAiConfig;

pub use batch::{OpenAiBatch, OpenAiBatchEndpoint, OpenAiBatchRequest, OpenAiBatchStatus};
pub use files::{
    OpenAiFile, OpenAiFileDeletion, OpenAiFilePurpose, OpenAiFileStatus, OpenAiFileUpload,
    OpenAiFileWaitPolicy,
};
pub(crate) use realtime::validate_realtime_instructions;
pub use realtime::{OpenAiRealtimeClientSecret, OpenAiRealtimeClientSecretRequest};

const MAX_CONTROL_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// Boxed control-plane result that retains native `Send` guarantees.
#[cfg(not(target_arch = "wasm32"))]
pub type OpenAiControlFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Boxed control-plane result on a single-threaded browser runtime.
#[cfg(target_arch = "wasm32")]
pub type OpenAiControlFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Typed failure from an OpenAI-compatible control-plane operation.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OpenAiControlError {
    /// Local input violated a bounded control-plane invariant.
    #[error("invalid OpenAI control-plane request: {0}")]
    InvalidRequest(String),
    /// The caller cancelled the operation.
    #[error("OpenAI control-plane operation was cancelled")]
    Cancelled,
    /// The operation crossed its explicit deadline.
    #[error("OpenAI control-plane operation exceeded its deadline")]
    DeadlineExceeded,
    /// The HTTP transport failed before a valid Provider response was decoded.
    #[error("OpenAI control-plane transport failed: {0}")]
    Transport(#[source] reqwest::Error),
    /// The Provider rejected the request.
    #[error("OpenAI control-plane request failed with HTTP {status}: {message}")]
    Provider {
        /// HTTP status code.
        status: u16,
        /// Provider-supplied safe diagnostic.
        message: String,
        /// Provider request identity, when exposed.
        request_id: Option<String>,
    },
    /// The Provider returned a successful but malformed representation.
    #[error("invalid OpenAI control-plane response: {0}")]
    Protocol(String),
    /// An uploaded file reached a terminal processing failure.
    #[error("OpenAI file `{file_id}` processing failed with status `{status}`")]
    FileProcessingFailed {
        /// Provider file identity.
        file_id: String,
        /// Provider status token.
        status: String,
    },
}

/// Model metadata returned by `GET /models`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct OpenAiModelInfo {
    /// Stable model identity.
    pub id: String,
    /// Provider creation timestamp, when supplied.
    #[serde(default)]
    pub created: Option<u64>,
    /// Owning organization or Provider namespace, when supplied.
    #[serde(default)]
    pub owned_by: Option<String>,
}

/// Cloneable HTTP control plane bound to one Provider configuration.
#[derive(Clone, Debug)]
pub struct OpenAiControlPlane {
    config: OpenAiConfig,
    http: Client,
}

impl OpenAiControlPlane {
    pub(crate) fn new(config: OpenAiConfig, http: Client) -> Self {
        Self { config, http }
    }

    /// Lists models visible to the configured credential or gateway.
    pub fn list_models(
        &self,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<Vec<OpenAiModelInfo>, OpenAiControlError>> {
        Box::pin(async move {
            let response = self
                .send(self.http.get(self.endpoint("models")), &context)
                .await?;
            let payload: ModelList = self.decode(response, &context).await?;
            if payload.data.iter().any(|model| model.id.trim().is_empty()) {
                return Err(OpenAiControlError::Protocol(
                    "model list contained a blank identity".into(),
                ));
            }
            Ok(payload.data)
        })
    }

    fn endpoint(&self, path: &str) -> Url {
        self.config.control_endpoint_url(path)
    }

    fn request(&self, builder: RequestBuilder, context: &ModelCallContext) -> RequestBuilder {
        let mut builder =
            builder.header("x-client-request-id", context.invocation_id().to_string());
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
        if let Some(remaining) = context.remaining() {
            builder = builder.timeout(remaining);
        }
        builder
    }

    async fn send(
        &self,
        builder: RequestBuilder,
        context: &ModelCallContext,
    ) -> Result<Response, OpenAiControlError> {
        if context
            .remaining()
            .is_some_and(|remaining| remaining.is_zero())
        {
            return Err(OpenAiControlError::DeadlineExceeded);
        }
        let deadline = context.deadline();
        match select(
            Box::pin(context.cancellation().cancelled()),
            Box::pin(self.request(builder, context).send()),
        )
        .await
        {
            Either::Left(_) => Err(OpenAiControlError::Cancelled),
            Either::Right((result, _)) => result.map_err(|error| transport_error(error, deadline)),
        }
    }

    async fn decode<T: DeserializeOwned>(
        &self,
        response: Response,
        context: &ModelCallContext,
    ) -> Result<T, OpenAiControlError> {
        let status = response.status();
        let request_id = request_id(&response);
        let deadline = context.deadline();
        let text = read_bounded_text(response, context, deadline).await?;
        if !status.is_success() {
            return Err(provider_error(status, request_id, &text));
        }
        serde_json::from_str(&text).map_err(|error| {
            OpenAiControlError::Protocol(format!("successful response was not valid JSON: {error}"))
        })
    }
}

#[derive(Deserialize)]
struct ModelList {
    data: Vec<OpenAiModelInfo>,
}

pub(crate) fn validate_id(name: &str, value: String) -> Result<String, OpenAiControlError> {
    if value.is_empty()
        || value.len() > 255
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(OpenAiControlError::InvalidRequest(format!(
            "{name} identity must be a 1..=255 byte ASCII token"
        )));
    }
    Ok(value)
}

fn request_id(response: &Response) -> Option<String> {
    response
        .headers()
        .get("x-request-id")
        .or_else(|| response.headers().get("request-id"))
        .and_then(|value| value.to_str().ok())
        .map(String::from)
}

fn transport_error(error: reqwest::Error, deadline: Option<Instant>) -> OpenAiControlError {
    if error.is_timeout() || deadline.is_some_and(|value| value <= Instant::now()) {
        OpenAiControlError::DeadlineExceeded
    } else {
        OpenAiControlError::Transport(error.without_url())
    }
}

async fn read_bounded_text(
    response: Response,
    context: &ModelCallContext,
    deadline: Option<Instant>,
) -> Result<String, OpenAiControlError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_CONTROL_RESPONSE_BYTES as u64)
    {
        return Err(OpenAiControlError::Protocol(format!(
            "response exceeds the {MAX_CONTROL_RESPONSE_BYTES} byte limit"
        )));
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    loop {
        let next = match select(
            Box::pin(context.cancellation().cancelled()),
            Box::pin(stream.next()),
        )
        .await
        {
            Either::Left(_) => return Err(OpenAiControlError::Cancelled),
            Either::Right((next, _)) => next,
        };
        let Some(chunk) = next else {
            break;
        };
        let chunk = chunk.map_err(|error| transport_error(error, deadline))?;
        if bytes.len().saturating_add(chunk.len()) > MAX_CONTROL_RESPONSE_BYTES {
            return Err(OpenAiControlError::Protocol(format!(
                "response exceeds the {MAX_CONTROL_RESPONSE_BYTES} byte limit"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes)
        .map_err(|error| OpenAiControlError::Protocol(format!("response was not UTF-8: {error}")))
}

fn provider_error(
    status: StatusCode,
    request_id: Option<String>,
    body: &str,
) -> OpenAiControlError {
    let payload = serde_json::from_str::<Value>(body).unwrap_or(Value::Null);
    let detail = payload.get("error").unwrap_or(&payload);
    let message = detail
        .get("message")
        .and_then(Value::as_str)
        .map_or_else(|| format!("Provider returned HTTP {status}"), String::from);
    OpenAiControlError::Provider {
        status: status.as_u16(),
        message,
        request_id,
    }
}
