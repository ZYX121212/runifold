//! OpenAI-compatible Files API lifecycle operations.

use std::time::Duration;

use futures_util::future::{Either, select};
use reqwest::multipart;
use runifold_core::Instant;
use runifold_model::ModelCallContext;
use serde::Deserialize;

use super::{OpenAiControlError, OpenAiControlFuture, OpenAiControlPlane, validate_id};

const MAX_UPLOAD_BYTES: usize = 512 * 1024 * 1024;

/// Validated Provider file purpose.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenAiFilePurpose(String);

impl OpenAiFilePurpose {
    /// Standard purpose for Batch API JSONL input.
    #[must_use]
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
    #[must_use]
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

/// Provider file-processing lifecycle.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiFileStatus {
    /// The file is still being ingested or processed.
    Processing,
    /// The file can be referenced by model requests.
    Active,
    /// Provider-side processing failed.
    Failed,
    /// A forward-compatible Provider state.
    Unknown(String),
}

impl OpenAiFile {
    /// Normalizes Provider-specific file status tokens.
    #[must_use]
    pub fn lifecycle_status(&self) -> OpenAiFileStatus {
        match self.status.as_deref() {
            Some("active" | "processed" | "completed") => OpenAiFileStatus::Active,
            Some("failed" | "error" | "cancelled") => OpenAiFileStatus::Failed,
            Some("pending" | "processing" | "uploading") | None => OpenAiFileStatus::Processing,
            Some(status) => OpenAiFileStatus::Unknown(status.into()),
        }
    }
}

/// Bounded polling policy for waiting on an uploaded file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpenAiFileWaitPolicy {
    /// Delay between status requests.
    pub poll_interval: Duration,
    /// Maximum total wait, also bounded by the call context deadline.
    pub timeout: Duration,
}

impl OpenAiFileWaitPolicy {
    /// Creates a bounded polling policy.
    ///
    /// # Errors
    ///
    /// Rejects zero polling intervals and timeouts.
    pub fn new(poll_interval: Duration, timeout: Duration) -> Result<Self, OpenAiControlError> {
        if poll_interval.is_zero() || timeout.is_zero() {
            return Err(OpenAiControlError::InvalidRequest(
                "file wait polling interval and timeout must be greater than zero".into(),
            ));
        }
        Ok(Self {
            poll_interval,
            timeout,
        })
    }
}

impl Default for OpenAiFileWaitPolicy {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(500),
            timeout: Duration::from_secs(300),
        }
    }
}

/// Confirmation returned after deleting a provider file.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct OpenAiFileDeletion {
    /// Deleted file identity.
    pub id: String,
    /// Whether the Provider confirmed deletion.
    pub deleted: bool,
}

impl OpenAiControlPlane {
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

    /// Reads current metadata and processing status for one file.
    pub fn get_file(
        &self,
        file_id: impl Into<String>,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<OpenAiFile, OpenAiControlError>> {
        let file_id = file_id.into();
        Box::pin(async move {
            let file_id = validate_id("file", file_id)?;
            let response = self
                .send(
                    self.http.get(self.endpoint(&format!("files/{file_id}"))),
                    &context,
                )
                .await?;
            let file: OpenAiFile = self.decode(response, &context).await?;
            validate_file(file)
        })
    }

    /// Lists files visible to the configured credential.
    pub fn list_files(
        &self,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<Vec<OpenAiFile>, OpenAiControlError>> {
        Box::pin(async move {
            let response = self
                .send(self.http.get(self.endpoint("files")), &context)
                .await?;
            let payload: FileList = self.decode(response, &context).await?;
            payload.data.into_iter().map(validate_file).collect()
        })
    }

    /// Deletes one provider file.
    pub fn delete_file(
        &self,
        file_id: impl Into<String>,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<OpenAiFileDeletion, OpenAiControlError>> {
        let file_id = file_id.into();
        Box::pin(async move {
            let file_id = validate_id("file", file_id)?;
            let response = self
                .send(
                    self.http.delete(self.endpoint(&format!("files/{file_id}"))),
                    &context,
                )
                .await?;
            let deletion: OpenAiFileDeletion = self.decode(response, &context).await?;
            validate_id("file", deletion.id.clone())?;
            Ok(deletion)
        })
    }

    /// Polls until a file becomes usable, fails, is cancelled, or times out.
    pub fn wait_file_active(
        &self,
        file_id: impl Into<String>,
        policy: OpenAiFileWaitPolicy,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<OpenAiFile, OpenAiControlError>> {
        let file_id = file_id.into();
        Box::pin(async move {
            let file_id = validate_id("file", file_id)?;
            let context = context.with_deadline(Instant::now() + policy.timeout);
            loop {
                let file = self.get_file(file_id.clone(), context.clone()).await?;
                match file.lifecycle_status() {
                    OpenAiFileStatus::Active => return Ok(file),
                    OpenAiFileStatus::Failed => {
                        return Err(OpenAiControlError::FileProcessingFailed {
                            file_id,
                            status: file.status.unwrap_or_else(|| "failed".into()),
                        });
                    }
                    OpenAiFileStatus::Processing | OpenAiFileStatus::Unknown(_) => {}
                }
                if context
                    .remaining()
                    .is_some_and(|remaining| remaining.is_zero())
                {
                    return Err(OpenAiControlError::DeadlineExceeded);
                }
                match select(
                    Box::pin(context.cancellation().cancelled()),
                    Box::pin(futures_timer::Delay::new(policy.poll_interval)),
                )
                .await
                {
                    Either::Left(_) => return Err(OpenAiControlError::Cancelled),
                    Either::Right(_) => {}
                }
            }
        })
    }
}

#[derive(Deserialize)]
struct FileList {
    data: Vec<OpenAiFile>,
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

#[cfg(test)]
mod tests {
    use super::{
        OpenAiControlError, OpenAiFile, OpenAiFilePurpose, OpenAiFileStatus, OpenAiFileUpload,
        OpenAiFileWaitPolicy,
    };

    #[test]
    fn upload_inputs_fail_closed_before_transport() {
        assert!(matches!(
            OpenAiFileUpload::new("../secret", OpenAiFilePurpose::batch(), vec![1]),
            Err(OpenAiControlError::InvalidRequest(_))
        ));
    }

    #[test]
    fn file_statuses_are_normalized_without_erasing_unknown_values() {
        let file = |status: Option<&str>| OpenAiFile {
            id: "file_1".into(),
            filename: "report.pdf".into(),
            purpose: "user_data".into(),
            bytes: 1,
            created_at: 0,
            status: status.map(str::to_owned),
        };

        assert_eq!(
            file(Some("active")).lifecycle_status(),
            OpenAiFileStatus::Active
        );
        assert_eq!(
            file(Some("future_state")).lifecycle_status(),
            OpenAiFileStatus::Unknown("future_state".into())
        );
        assert!(
            OpenAiFileWaitPolicy::new(std::time::Duration::ZERO, std::time::Duration::from_secs(1))
                .is_err()
        );
    }
}
