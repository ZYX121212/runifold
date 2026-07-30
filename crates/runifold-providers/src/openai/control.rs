//! Typed OpenAI-compatible model, file, and Batch control-plane operations.

use std::{collections::BTreeMap, future::Future, pin::Pin};

use futures_util::{
    StreamExt,
    future::{Either, select},
};
use reqwest::{Client, RequestBuilder, Response, StatusCode, multipart};
use runifold_core::Instant;
use runifold_model::ModelCallContext;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use thiserror::Error;
use url::Url;

use super::{
    OpenAiConfig, OpenAiRealtimeCall, OpenAiRealtimeCallRequest, OpenAiRealtimeModality,
    realtime_call::{validate_safety_identifier, validate_sdp},
};

const MAX_UPLOAD_BYTES: usize = 512 * 1024 * 1024;
const MAX_CONTROL_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const MAX_METADATA_ENTRIES: usize = 16;
const MAX_METADATA_KEY_BYTES: usize = 64;
const MAX_METADATA_VALUE_BYTES: usize = 512;
const MAX_REALTIME_INSTRUCTIONS_BYTES: usize = 256 * 1024;

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
}

/// Validated server-side request for a short-lived Realtime client secret.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiRealtimeClientSecretRequest {
    model: String,
    instructions: Option<String>,
    output_modality: Option<OpenAiRealtimeModality>,
    expires_after_seconds: u32,
    safety_identifier: Option<String>,
}

impl OpenAiRealtimeClientSecretRequest {
    /// Creates a request with a ten-minute lifetime.
    ///
    /// # Errors
    ///
    /// Rejects invalid model identifiers.
    pub fn new(model: impl Into<String>) -> Result<Self, OpenAiControlError> {
        Ok(Self {
            model: validate_id("Realtime model", model.into())?,
            instructions: None,
            output_modality: None,
            expires_after_seconds: 600,
            safety_identifier: None,
        })
    }

    /// Applies bounded session instructions to every use of the secret.
    ///
    /// # Errors
    ///
    /// Rejects instructions larger than 256 KiB.
    pub fn with_instructions(
        mut self,
        instructions: impl Into<String>,
    ) -> Result<Self, OpenAiControlError> {
        self.instructions = Some(validate_realtime_instructions(instructions.into())?);
        Ok(self)
    }

    /// Selects the initial output modality attached to the secret.
    #[must_use]
    pub fn with_modality(mut self, modality: OpenAiRealtimeModality) -> Self {
        self.output_modality = Some(modality);
        self
    }

    /// Changes the short-lived credential lifetime.
    ///
    /// # Errors
    ///
    /// Rejects values outside the GA 10-second to 2-hour range.
    pub fn with_expiration_seconds(mut self, seconds: u32) -> Result<Self, OpenAiControlError> {
        if !(10..=7_200).contains(&seconds) {
            return Err(OpenAiControlError::InvalidRequest(
                "Realtime client secret expiration must be between 10 and 7200 seconds".into(),
            ));
        }
        self.expires_after_seconds = seconds;
        Ok(self)
    }

    /// Binds a stable privacy-preserving end-user identifier to the secret.
    ///
    /// # Errors
    ///
    /// Rejects blank, control-containing, or values over 64 bytes.
    pub fn with_safety_identifier(
        mut self,
        identifier: impl Into<String>,
    ) -> Result<Self, OpenAiControlError> {
        let identifier = identifier.into();
        validate_safety_identifier(&identifier)?;
        self.safety_identifier = Some(identifier);
        Ok(self)
    }
}

/// Short-lived Realtime credential and its effective Provider session.
#[derive(Clone)]
pub struct OpenAiRealtimeClientSecret {
    value: SecretString,
    /// Unix timestamp at which the credential expires.
    pub expires_at: u64,
    /// Effective forward-compatible GA session returned by the Provider.
    pub session: Value,
}

impl OpenAiRealtimeClientSecret {
    /// Returns the redacting secret container.
    ///
    /// Accessing plaintext still requires [`ExposeSecret`].
    pub const fn secret(&self) -> &SecretString {
        &self.value
    }
}

impl std::fmt::Debug for OpenAiRealtimeClientSecret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAiRealtimeClientSecret")
            .field("value", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .field("session", &"[REDACTED CONFIG]")
            .finish()
    }
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

/// Validated Provider file purpose.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiFilePurpose(String);

impl OpenAiFilePurpose {
    /// Standard purpose for Batch API JSONL input.
    pub fn batch() -> Self {
        Self("batch".into())
    }

    /// Creates a forward-compatible Provider purpose token.
    ///
    /// # Errors
    ///
    /// Rejects blank, oversized, or non-token values.
    pub fn new(value: impl Into<String>) -> Result<Self, OpenAiControlError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(OpenAiControlError::InvalidRequest(
                "file purpose must be a 1..=64 byte ASCII token".into(),
            ));
        }
        Ok(Self(value))
    }

    /// Returns the Provider wire value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Bounded in-memory file upload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiFileUpload {
    filename: String,
    purpose: OpenAiFilePurpose,
    bytes: Vec<u8>,
}

impl OpenAiFileUpload {
    /// Validates a file upload before opening the transport.
    ///
    /// # Errors
    ///
    /// Rejects unsafe names, empty content, and payloads larger than 512 MiB.
    pub fn new(
        filename: impl Into<String>,
        purpose: OpenAiFilePurpose,
        bytes: Vec<u8>,
    ) -> Result<Self, OpenAiControlError> {
        let filename = filename.into();
        if filename.is_empty()
            || filename.len() > 255
            || filename.contains(['/', '\\'])
            || filename.chars().any(char::is_control)
        {
            return Err(OpenAiControlError::InvalidRequest(
                "filename must be a safe 1..=255 byte basename".into(),
            ));
        }
        if bytes.is_empty() {
            return Err(OpenAiControlError::InvalidRequest(
                "file upload cannot be empty".into(),
            ));
        }
        if bytes.len() > MAX_UPLOAD_BYTES {
            return Err(OpenAiControlError::InvalidRequest(format!(
                "file upload exceeds the {MAX_UPLOAD_BYTES} byte limit"
            )));
        }
        Ok(Self {
            filename,
            purpose,
            bytes,
        })
    }
}

/// File metadata returned by the Provider.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct OpenAiFile {
    /// Stable file identity.
    pub id: String,
    /// Original filename.
    pub filename: String,
    /// Provider file purpose.
    pub purpose: String,
    /// Stored byte count.
    #[serde(default)]
    pub bytes: u64,
    /// Provider creation timestamp.
    #[serde(default)]
    pub created_at: u64,
    /// Provider processing status, when supplied.
    #[serde(default)]
    pub status: Option<String>,
}

/// Supported Batch API request endpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiBatchEndpoint {
    /// Responses API requests.
    Responses,
    /// Chat Completions requests.
    ChatCompletions,
    /// Embedding requests.
    Embeddings,
}

impl OpenAiBatchEndpoint {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Responses => "/v1/responses",
            Self::ChatCompletions => "/v1/chat/completions",
            Self::Embeddings => "/v1/embeddings",
        }
    }
}

/// Validated Batch creation command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiBatchRequest {
    input_file_id: String,
    endpoint: OpenAiBatchEndpoint,
    metadata: BTreeMap<String, String>,
}

impl OpenAiBatchRequest {
    /// Creates a 24-hour Batch command.
    ///
    /// # Errors
    ///
    /// Rejects blank file identities.
    pub fn new(
        input_file_id: impl Into<String>,
        endpoint: OpenAiBatchEndpoint,
    ) -> Result<Self, OpenAiControlError> {
        let input_file_id = validate_id("input file", input_file_id.into())?;
        Ok(Self {
            input_file_id,
            endpoint,
            metadata: BTreeMap::new(),
        })
    }

    /// Adds bounded non-sensitive Batch metadata.
    ///
    /// # Errors
    ///
    /// Rejects too many entries, blank/oversized keys, and oversized values.
    pub fn with_metadata(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, OpenAiControlError> {
        let key = key.into();
        let value = value.into();
        if key.is_empty() || key.len() > MAX_METADATA_KEY_BYTES || key.chars().any(char::is_control)
        {
            return Err(OpenAiControlError::InvalidRequest(
                "metadata key must be a non-empty, bounded header-safe string".into(),
            ));
        }
        if value.len() > MAX_METADATA_VALUE_BYTES || value.chars().any(char::is_control) {
            return Err(OpenAiControlError::InvalidRequest(
                "metadata value must be bounded and contain no control characters".into(),
            ));
        }
        if !self.metadata.contains_key(&key) && self.metadata.len() == MAX_METADATA_ENTRIES {
            return Err(OpenAiControlError::InvalidRequest(format!(
                "Batch metadata cannot exceed {MAX_METADATA_ENTRIES} entries"
            )));
        }
        self.metadata.insert(key, value);
        Ok(self)
    }
}

/// Batch lifecycle state.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiBatchStatus {
    /// Provider validation is pending or running.
    Validating,
    /// Requests are executing.
    InProgress,
    /// Cancellation was requested.
    Cancelling,
    /// Every terminal result is available.
    Completed,
    /// The Batch failed.
    Failed,
    /// The Batch expired.
    Expired,
    /// The Batch was cancelled.
    Cancelled,
    /// A forward-compatible Provider state.
    Unknown(String),
}

impl From<String> for OpenAiBatchStatus {
    fn from(value: String) -> Self {
        match value.as_str() {
            "validating" => Self::Validating,
            "in_progress" => Self::InProgress,
            "cancelling" => Self::Cancelling,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "expired" => Self::Expired,
            "cancelled" => Self::Cancelled,
            _ => Self::Unknown(value),
        }
    }
}

/// Provider Batch representation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiBatch {
    /// Stable Batch identity.
    pub id: String,
    /// Input JSONL file.
    pub input_file_id: String,
    /// Batched API endpoint.
    pub endpoint: String,
    /// Current lifecycle status.
    pub status: OpenAiBatchStatus,
    /// Output file after successful completion.
    pub output_file_id: Option<String>,
    /// Error file after partial or total failure.
    pub error_file_id: Option<String>,
    /// Non-sensitive caller metadata.
    pub metadata: BTreeMap<String, String>,
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

    /// Creates a short-lived credential for a browser or mobile Realtime
    /// connection without exposing the configured long-lived API key.
    pub fn create_realtime_client_secret(
        &self,
        request: OpenAiRealtimeClientSecretRequest,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<OpenAiRealtimeClientSecret, OpenAiControlError>> {
        Box::pin(async move {
            let body = CreateRealtimeClientSecretWire {
                expires_after: RealtimeSecretExpirationWire {
                    anchor: "created_at",
                    seconds: request.expires_after_seconds,
                },
                session: RealtimeSecretSessionWire {
                    session_type: "realtime",
                    model: request.model,
                    instructions: request.instructions,
                    output_modalities: request.output_modality.map(|value| vec![value]),
                },
            };
            let mut builder = self
                .http
                .post(self.endpoint("realtime/client_secrets"))
                .json(&body);
            if let Some(identifier) = request.safety_identifier {
                builder = builder.header("openai-safety-identifier", identifier);
            }
            let response = self.send(builder, &context).await?;
            let wire: RealtimeClientSecretWire = self.decode(response, &context).await?;
            wire.try_into()
        })
    }

    /// Creates a WebRTC call through the server-side unified interface.
    ///
    /// The configured long-lived credential remains on this control plane;
    /// browsers should send their offer to an application-owned endpoint.
    pub fn create_realtime_call(
        &self,
        request: OpenAiRealtimeCallRequest,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<OpenAiRealtimeCall, OpenAiControlError>> {
        Box::pin(async move {
            let session = RealtimeSecretSessionWire {
                session_type: "realtime",
                model: request.model,
                instructions: request.instructions,
                output_modalities: request.output_modality.map(|value| vec![value]),
            };
            let session = serde_json::to_string(&session).map_err(|error| {
                OpenAiControlError::InvalidRequest(format!(
                    "Realtime session configuration could not be encoded: {error}"
                ))
            })?;
            let sdp = multipart::Part::text(request.offer.0)
                .mime_str("application/sdp")
                .map_err(|error| {
                    OpenAiControlError::InvalidRequest(format!(
                        "Realtime SDP part could not be constructed: {error}"
                    ))
                })?;
            let session = multipart::Part::text(session)
                .mime_str("application/json")
                .map_err(|error| {
                    OpenAiControlError::InvalidRequest(format!(
                        "Realtime session part could not be constructed: {error}"
                    ))
                })?;
            let form = multipart::Form::new()
                .part("sdp", sdp)
                .part("session", session);
            let mut builder = self
                .http
                .post(self.endpoint("realtime/calls"))
                .multipart(form);
            if let Some(identifier) = request.safety_identifier {
                builder = builder.header("openai-safety-identifier", identifier);
            }
            let response = self.send(builder, &context).await?;
            self.decode_realtime_call(response, &context).await
        })
    }

    /// Uploads a bounded file using the Provider multipart contract.
    pub fn upload_file(
        &self,
        upload: OpenAiFileUpload,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<OpenAiFile, OpenAiControlError>> {
        Box::pin(async move {
            let part = multipart::Part::bytes(upload.bytes).file_name(upload.filename);
            let form = multipart::Form::new()
                .text("purpose", upload.purpose.0)
                .part("file", part);
            let response = self
                .send(
                    self.http.post(self.endpoint("files")).multipart(form),
                    &context,
                )
                .await?;
            let file: OpenAiFile = self.decode(response, &context).await?;
            validate_file(file)
        })
    }

    /// Creates a 24-hour Batch from an uploaded JSONL file.
    pub fn create_batch(
        &self,
        request: OpenAiBatchRequest,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<OpenAiBatch, OpenAiControlError>> {
        Box::pin(async move {
            let body = CreateBatchWire {
                input_file_id: request.input_file_id,
                endpoint: request.endpoint.as_str(),
                completion_window: "24h",
                metadata: request.metadata,
            };
            let response = self
                .send(
                    self.http.post(self.endpoint("batches")).json(&body),
                    &context,
                )
                .await?;
            self.decode_batch(response, &context).await
        })
    }

    /// Reads one Batch without polling implicitly.
    pub fn get_batch(
        &self,
        batch_id: impl Into<String>,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<OpenAiBatch, OpenAiControlError>> {
        let batch_id = batch_id.into();
        Box::pin(async move {
            let batch_id = validate_id("Batch", batch_id)?;
            let response = self
                .send(
                    self.http.get(self.endpoint(&format!("batches/{batch_id}"))),
                    &context,
                )
                .await?;
            self.decode_batch(response, &context).await
        })
    }

    /// Requests cancellation and returns the Provider's resulting state.
    pub fn cancel_batch(
        &self,
        batch_id: impl Into<String>,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<OpenAiBatch, OpenAiControlError>> {
        let batch_id = batch_id.into();
        Box::pin(async move {
            let batch_id = validate_id("Batch", batch_id)?;
            let response = self
                .send(
                    self.http
                        .post(self.endpoint(&format!("batches/{batch_id}/cancel"))),
                    &context,
                )
                .await?;
            self.decode_batch(response, &context).await
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

    async fn decode_batch(
        &self,
        response: Response,
        context: &ModelCallContext,
    ) -> Result<OpenAiBatch, OpenAiControlError> {
        let wire: BatchWire = self.decode(response, context).await?;
        wire.try_into()
    }

    async fn decode_realtime_call(
        &self,
        response: Response,
        context: &ModelCallContext,
    ) -> Result<OpenAiRealtimeCall, OpenAiControlError> {
        let status = response.status();
        let request_id = request_id(&response);
        let location = response
            .headers()
            .get("location")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let deadline = context.deadline();
        let answer = read_bounded_text(response, context, deadline).await?;
        if !status.is_success() {
            return Err(provider_error(status, request_id, &answer));
        }
        validate_sdp("answer", &answer)
            .map_err(|error| OpenAiControlError::Protocol(error.to_string()))?;
        Ok(OpenAiRealtimeCall { answer, location })
    }
}

#[derive(Deserialize)]
struct ModelList {
    data: Vec<OpenAiModelInfo>,
}

#[derive(Serialize)]
struct CreateBatchWire {
    input_file_id: String,
    endpoint: &'static str,
    completion_window: &'static str,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    metadata: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct CreateRealtimeClientSecretWire {
    expires_after: RealtimeSecretExpirationWire,
    session: RealtimeSecretSessionWire,
}

#[derive(Serialize)]
struct RealtimeSecretExpirationWire {
    anchor: &'static str,
    seconds: u32,
}

#[derive(Serialize)]
struct RealtimeSecretSessionWire {
    #[serde(rename = "type")]
    session_type: &'static str,
    model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_modalities: Option<Vec<OpenAiRealtimeModality>>,
}

#[derive(Deserialize)]
struct RealtimeClientSecretWire {
    value: String,
    expires_at: u64,
    session: Value,
}

impl TryFrom<RealtimeClientSecretWire> for OpenAiRealtimeClientSecret {
    type Error = OpenAiControlError;

    fn try_from(wire: RealtimeClientSecretWire) -> Result<Self, Self::Error> {
        if wire.value.is_empty()
            || wire.value.len() > 4_096
            || wire.value.chars().any(char::is_control)
        {
            return Err(OpenAiControlError::Protocol(
                "Realtime client secret was empty or malformed".into(),
            ));
        }
        if !wire.session.is_object() {
            return Err(OpenAiControlError::Protocol(
                "Realtime client secret response omitted its effective session".into(),
            ));
        }
        Ok(Self {
            value: SecretString::from(wire.value),
            expires_at: wire.expires_at,
            session: wire.session,
        })
    }
}

#[derive(Deserialize)]
struct BatchWire {
    id: String,
    input_file_id: String,
    endpoint: String,
    status: String,
    #[serde(default)]
    output_file_id: Option<String>,
    #[serde(default)]
    error_file_id: Option<String>,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

impl TryFrom<BatchWire> for OpenAiBatch {
    type Error = OpenAiControlError;

    fn try_from(wire: BatchWire) -> Result<Self, Self::Error> {
        Ok(Self {
            id: validate_id("Batch", wire.id)?,
            input_file_id: validate_id("input file", wire.input_file_id)?,
            endpoint: wire.endpoint,
            status: wire.status.into(),
            output_file_id: wire.output_file_id,
            error_file_id: wire.error_file_id,
            metadata: wire.metadata,
        })
    }
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

pub(crate) fn validate_realtime_instructions(value: String) -> Result<String, OpenAiControlError> {
    if value.len() > MAX_REALTIME_INSTRUCTIONS_BYTES {
        return Err(OpenAiControlError::InvalidRequest(format!(
            "Realtime instructions exceed the {MAX_REALTIME_INSTRUCTIONS_BYTES} byte limit"
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

fn validate_file(file: OpenAiFile) -> Result<OpenAiFile, OpenAiControlError> {
    validate_id("file", file.id.clone())?;
    if file.filename.is_empty() || file.purpose.is_empty() {
        return Err(OpenAiControlError::Protocol(
            "file response omitted filename or purpose".into(),
        ));
    }
    Ok(file)
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

#[cfg(test)]
mod tests {
    use super::{
        OpenAiBatchEndpoint, OpenAiBatchRequest, OpenAiBatchStatus, OpenAiControlError,
        OpenAiFilePurpose, OpenAiFileUpload, OpenAiRealtimeClientSecretRequest,
    };

    #[test]
    fn upload_and_batch_inputs_fail_closed_before_transport() {
        assert!(matches!(
            OpenAiFileUpload::new("../secret", OpenAiFilePurpose::batch(), vec![1]),
            Err(OpenAiControlError::InvalidRequest(_))
        ));
        assert!(matches!(
            OpenAiBatchRequest::new("", OpenAiBatchEndpoint::Responses),
            Err(OpenAiControlError::InvalidRequest(_))
        ));
    }

    #[test]
    fn unknown_batch_status_is_preserved() {
        assert_eq!(
            OpenAiBatchStatus::from("pausing".to_owned()),
            OpenAiBatchStatus::Unknown("pausing".to_owned())
        );
    }

    #[test]
    fn realtime_client_secret_request_validates_lifetime() {
        let request = OpenAiRealtimeClientSecretRequest::new("gpt-realtime").unwrap();
        assert!(request.clone().with_expiration_seconds(9).is_err());
        assert!(request.with_expiration_seconds(7_201).is_err());
    }
}
