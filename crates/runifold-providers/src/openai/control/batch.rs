//! OpenAI-compatible Batch API operations.

use std::collections::BTreeMap;

use runifold_model::ModelCallContext;
use serde::{Deserialize, Serialize};

use super::{OpenAiControlError, OpenAiControlFuture, OpenAiControlPlane, validate_id};

const MAX_METADATA_ENTRIES: usize = 16;
const MAX_METADATA_KEY_BYTES: usize = 64;
const MAX_METADATA_VALUE_BYTES: usize = 512;

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

impl OpenAiControlPlane {
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

    async fn decode_batch(
        &self,
        response: reqwest::Response,
        context: &ModelCallContext,
    ) -> Result<OpenAiBatch, OpenAiControlError> {
        let wire: BatchWire = self.decode(response, context).await?;
        wire.try_into()
    }
}

#[derive(Serialize)]
struct CreateBatchWire {
    input_file_id: String,
    endpoint: &'static str,
    completion_window: &'static str,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    metadata: BTreeMap<String, String>,
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

#[cfg(test)]
mod tests {
    use super::{OpenAiBatchEndpoint, OpenAiBatchRequest, OpenAiBatchStatus};

    #[test]
    fn batch_inputs_fail_closed_before_transport() {
        assert!(OpenAiBatchRequest::new("", OpenAiBatchEndpoint::Responses).is_err());
    }

    #[test]
    fn unknown_batch_status_is_preserved() {
        assert_eq!(
            OpenAiBatchStatus::from("pausing".to_owned()),
            OpenAiBatchStatus::Unknown("pausing".to_owned())
        );
    }
}
