//! S3-compatible immutable Task tombstone archive over pre-signed requests.

mod sigv4;

pub use sigv4::{S3SigV4Credentials, S3SigV4Presigner, S3SigV4PresignerConfig};

use std::{
    future::Future,
    num::NonZeroU32,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use reqwest::{
    Client, Response, StatusCode, Url,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use runifold_workflow::{
    WorkflowTaskStatus, WorkflowTaskTombstoneArchive, WorkflowTaskTombstoneArchiveBatch,
    WorkflowTaskTombstoneArchiveError, WorkflowTaskTombstoneArchiveErrorKind,
    WorkflowTaskTombstoneArchiveFuture, WorkflowTaskTombstoneExportReceipt,
};
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const CHECKSUM_METADATA: &str = "x-amz-meta-runifold-sha256";
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_REQUEST_TIMEOUT: Duration = Duration::from_secs(600);

/// Server-side encryption required for every archived object.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum S3ArchiveEncryption {
    /// S3-managed AES-256 keys.
    Aes256,
    /// A specific KMS key.
    Kms {
        /// KMS key ID or ARN.
        key_id: String,
    },
}

/// S3 Object Lock retention mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum S3ObjectLockMode {
    /// Authorized operators may shorten retention.
    Governance,
    /// Retention cannot be shortened, including by the root account.
    Compliance,
}

/// Immutable Object Lock policy applied at upload time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct S3ObjectLock {
    /// S3 retention enforcement mode.
    pub mode: S3ObjectLockMode,
    /// Whole days added to the local upload time.
    pub retention_days: NonZeroU32,
}

/// Validated S3 archive policy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3TombstoneArchiveConfig {
    bucket: String,
    prefix: String,
    encryption: S3ArchiveEncryption,
    object_lock: Option<S3ObjectLock>,
    request_timeout: Duration,
}

impl S3TombstoneArchiveConfig {
    /// Creates a protected archive configuration.
    ///
    /// # Errors
    ///
    /// Rejects non-portable bucket, prefix, or KMS identities.
    pub fn new(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        encryption: S3ArchiveEncryption,
    ) -> Result<Self, WorkflowTaskTombstoneArchiveError> {
        let bucket = bucket.into();
        if bucket.len() < 3
            || bucket.len() > 63
            || !bucket
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(config_error(
                "S3 bucket must use 3..=63 lowercase DNS bytes",
            ));
        }
        let prefix = prefix.into().trim_matches('/').to_owned();
        if prefix.len() > 128 || prefix.chars().any(char::is_control) {
            return Err(config_error(
                "S3 archive prefix must contain at most 128 printable bytes",
            ));
        }
        if let S3ArchiveEncryption::Kms { key_id } = &encryption {
            if key_id.trim().is_empty()
                || key_id.len() > 512
                || key_id.chars().any(char::is_control)
            {
                return Err(config_error("S3 KMS key ID is invalid"));
            }
        }
        Ok(Self {
            bucket,
            prefix,
            encryption,
            object_lock: None,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        })
    }

    /// Requires S3 Object Lock for every new object.
    #[must_use]
    pub const fn with_object_lock(mut self, object_lock: S3ObjectLock) -> Self {
        self.object_lock = Some(object_lock);
        self
    }

    /// Bounds each PUT and reconciliation HEAD request.
    ///
    /// # Errors
    ///
    /// Rejects zero durations and values above ten minutes.
    pub fn with_request_timeout(
        mut self,
        timeout: Duration,
    ) -> Result<Self, WorkflowTaskTombstoneArchiveError> {
        if timeout.is_zero() || timeout > MAX_REQUEST_TIMEOUT {
            return Err(config_error(
                "S3 archive request timeout must be in 1ns..=600s",
            ));
        }
        self.request_timeout = timeout;
        Ok(self)
    }
}

/// Exact object properties that a pre-signer must authorize.
#[derive(Clone, Debug)]
pub struct S3ArchivePresignRequest {
    /// Bucket selected by archive policy.
    pub bucket: String,
    /// Stable object key derived from the batch ID.
    pub key: String,
    /// Exact headers Runifold will send with PUT.
    pub required_put_headers: HeaderMap,
}

/// Pre-signed PUT and HEAD authority scoped to one object.
#[derive(Clone)]
pub struct S3ArchivePresignedObject {
    /// Single-object conditional PUT URL.
    pub put_url: Url,
    /// Single-object HEAD URL used only for idempotency reconciliation.
    pub head_url: Url,
    /// Additional signer-owned PUT headers.
    pub put_headers: HeaderMap,
    /// Additional signer-owned HEAD headers.
    pub head_headers: HeaderMap,
}

impl std::fmt::Debug for S3ArchivePresignedObject {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3ArchivePresignedObject")
            .field("put_url", &"<redacted-presigned-url>")
            .field("head_url", &"<redacted-presigned-url>")
            .field("put_headers", &"<redacted-signed-headers>")
            .field("head_headers", &"<redacted-signed-headers>")
            .finish()
    }
}

/// Borrowing future returned by an application-owned S3 pre-signer.
pub type S3ArchivePresignFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<S3ArchivePresignedObject, WorkflowTaskTombstoneArchiveError>>
            + Send
            + 'a,
    >,
>;

/// Short-lived authority provider for S3-compatible object operations.
pub trait S3ArchivePresigner: Send + Sync {
    /// Signs one exact PUT and its reconciliation HEAD request.
    fn presign(&self, request: S3ArchivePresignRequest) -> S3ArchivePresignFuture<'_>;
}

/// S3-compatible idempotent tombstone archive.
#[derive(Clone)]
pub struct S3TombstoneArchive<P> {
    put_client: Client,
    reconciliation_client: Client,
    config: S3TombstoneArchiveConfig,
    presigner: Arc<P>,
}

impl<P> S3TombstoneArchive<P>
where
    P: S3ArchivePresigner,
{
    /// Creates an archive using short-lived, application-owned signing.
    pub fn new(config: S3TombstoneArchiveConfig, presigner: Arc<P>) -> Self {
        Self {
            put_client: Client::new(),
            reconciliation_client: Client::new(),
            config,
            presigner,
        }
    }

    async fn store(
        &self,
        batch: WorkflowTaskTombstoneArchiveBatch,
    ) -> Result<WorkflowTaskTombstoneExportReceipt, WorkflowTaskTombstoneArchiveError> {
        let key = object_key(&self.config.prefix, batch.batch_id.as_str());
        let payload = encode_batch(&batch)?;
        let digest = Sha256::digest(&payload);
        let checksum_hex = format!("{digest:x}");
        let checksum_base64 = STANDARD.encode(digest);
        let required =
            required_headers(&self.config, payload.len(), &checksum_hex, &checksum_base64)?;
        let signed = self
            .presigner
            .presign(S3ArchivePresignRequest {
                bucket: self.config.bucket.clone(),
                key: key.clone(),
                required_put_headers: required.clone(),
            })
            .await?;
        let response = self
            .put_client
            .put(signed.put_url)
            .headers(signed.put_headers)
            .headers(required)
            .body(payload)
            .timeout(self.config.request_timeout)
            .send()
            .await;
        match response {
            Ok(response) if response.status().is_success() => {
                receipt(&self.config.bucket, &key, &checksum_hex)
            }
            Ok(response)
                if response.status() == StatusCode::PRECONDITION_FAILED
                    || response.status() == StatusCode::CONFLICT =>
            {
                self.reconcile(signed.head_url, signed.head_headers, &key, &checksum_hex)
                    .await
            }
            Ok(response) => Err(s3_response_error(response).await),
            Err(error) => match self
                .reconcile(signed.head_url, signed.head_headers, &key, &checksum_hex)
                .await
            {
                Ok(receipt) => Ok(receipt),
                Err(_) => Err(classified_error(
                    if error.is_timeout() {
                        WorkflowTaskTombstoneArchiveErrorKind::Timeout
                    } else {
                        WorkflowTaskTombstoneArchiveErrorKind::Ambiguous
                    },
                    "S3 PUT outcome is unknown and reconciliation failed",
                )),
            },
        }
    }

    async fn reconcile(
        &self,
        url: Url,
        headers: HeaderMap,
        key: &str,
        checksum_hex: &str,
    ) -> Result<WorkflowTaskTombstoneExportReceipt, WorkflowTaskTombstoneArchiveError> {
        let response = self
            .reconciliation_client
            .head(url)
            .headers(headers)
            .timeout(self.config.request_timeout)
            .send()
            .await
            .map_err(|error| {
                classified_error(
                    if error.is_timeout() {
                        WorkflowTaskTombstoneArchiveErrorKind::Timeout
                    } else {
                        WorkflowTaskTombstoneArchiveErrorKind::Unavailable
                    },
                    "S3 HEAD reconciliation transport failed",
                )
            })?;
        if !response.status().is_success() {
            return Err(classified_error(
                WorkflowTaskTombstoneArchiveErrorKind::Unavailable,
                format!(
                    "S3 HEAD reconciliation failed with status {}",
                    response.status().as_u16()
                ),
            ));
        }
        if response
            .headers()
            .get(CHECKSUM_METADATA)
            .and_then(|value| value.to_str().ok())
            != Some(checksum_hex)
        {
            return Err(classified_error(
                WorkflowTaskTombstoneArchiveErrorKind::Integrity,
                "S3 object exists without matching checksum",
            ));
        }
        receipt(&self.config.bucket, key, checksum_hex)
    }
}

impl<P> WorkflowTaskTombstoneArchive for S3TombstoneArchive<P>
where
    P: S3ArchivePresigner + 'static,
{
    fn archive(
        &self,
        batch: WorkflowTaskTombstoneArchiveBatch,
    ) -> WorkflowTaskTombstoneArchiveFuture<'_> {
        Box::pin(async move { self.store(batch).await })
    }
}

impl<P> std::fmt::Debug for S3TombstoneArchive<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3TombstoneArchive")
            .field("config", &self.config)
            .field("presigner", &"<s3-presigner>")
            .finish_non_exhaustive()
    }
}

async fn s3_response_error(mut response: Response) -> WorkflowTaskTombstoneArchiveError {
    const MAX_ERROR_BYTES: usize = 4_096;

    let status = response.status().as_u16();
    let mut body = Vec::new();
    while body.len() < MAX_ERROR_BYTES {
        let Ok(Some(chunk)) = response.chunk().await else {
            break;
        };
        let remaining = MAX_ERROR_BYTES - body.len();
        body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }
    let body = String::from_utf8_lossy(&body);
    let code = safe_xml_token(&body, "Code");
    let header = safe_xml_token(&body, "Header");
    let detail = match (code, header) {
        (Some(code), Some(header)) => format!(" ({code}, unsupported header {header})"),
        (Some(code), None) => format!(" ({code})"),
        (None, _) => String::new(),
    };
    classified_error(
        response_status_kind(response.status()),
        format!("S3 conditional PUT failed with status {status}{detail}"),
    )
}

fn response_status_kind(status: StatusCode) -> WorkflowTaskTombstoneArchiveErrorKind {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            WorkflowTaskTombstoneArchiveErrorKind::Authorization
        }
        StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT => {
            WorkflowTaskTombstoneArchiveErrorKind::Timeout
        }
        StatusCode::TOO_MANY_REQUESTS => WorkflowTaskTombstoneArchiveErrorKind::Unavailable,
        _ if status.is_server_error() => WorkflowTaskTombstoneArchiveErrorKind::Unavailable,
        _ => WorkflowTaskTombstoneArchiveErrorKind::Other,
    }
}

fn safe_xml_token<'a>(body: &'a str, field: &str) -> Option<&'a str> {
    let start = format!("<{field}>");
    let end = format!("</{field}>");
    let value = body.split_once(&start)?.1.split_once(&end)?.0;
    (!value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')))
    .then_some(value)
}

fn required_headers(
    config: &S3TombstoneArchiveConfig,
    length: usize,
    checksum_hex: &str,
    checksum_base64: &str,
) -> Result<HeaderMap, WorkflowTaskTombstoneArchiveError> {
    let mut headers = HeaderMap::new();
    insert(&mut headers, "if-none-match", "*")?;
    insert(&mut headers, "content-type", "application/json")?;
    insert(&mut headers, "content-length", &length.to_string())?;
    insert(&mut headers, "x-amz-checksum-sha256", checksum_base64)?;
    insert(&mut headers, CHECKSUM_METADATA, checksum_hex)?;
    match &config.encryption {
        S3ArchiveEncryption::Aes256 => {
            insert(&mut headers, "x-amz-server-side-encryption", "AES256")?;
        }
        S3ArchiveEncryption::Kms { key_id } => {
            insert(&mut headers, "x-amz-server-side-encryption", "aws:kms")?;
            insert(
                &mut headers,
                "x-amz-server-side-encryption-aws-kms-key-id",
                key_id,
            )?;
        }
    }
    if let Some(lock) = config.object_lock {
        let mode = match lock.mode {
            S3ObjectLockMode::Governance => "GOVERNANCE",
            S3ObjectLockMode::Compliance => "COMPLIANCE",
        };
        insert(&mut headers, "x-amz-object-lock-mode", mode)?;
        insert(
            &mut headers,
            "x-amz-object-lock-retain-until-date",
            &retention_deadline(lock.retention_days)?,
        )?;
    }
    Ok(headers)
}

fn retention_deadline(days: NonZeroU32) -> Result<String, WorkflowTaskTombstoneArchiveError> {
    let seconds = u64::from(days.get())
        .checked_mul(86_400)
        .and_then(|value| {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()?
                .as_secs()
                .checked_add(value)
        })
        .ok_or_else(|| archive_error("S3 Object Lock deadline overflowed"))?;
    let seconds =
        i64::try_from(seconds).map_err(|_| archive_error("S3 Object Lock deadline overflowed"))?;
    OffsetDateTime::from_unix_timestamp(seconds)
        .map_err(|_| archive_error("S3 Object Lock deadline is invalid"))?
        .format(&Rfc3339)
        .map_err(|_| archive_error("S3 Object Lock deadline formatting failed"))
}

fn encode_batch(
    batch: &WorkflowTaskTombstoneArchiveBatch,
) -> Result<Vec<u8>, WorkflowTaskTombstoneArchiveError> {
    let tombstones = batch
        .tombstones
        .iter()
        .map(|item| {
            serde_json::json!({
                "checkpoint_id": item.checkpoint_id.as_uuid().to_string(),
                "created_at_ms": item.created_at_ms,
                "cursor": item.cursor.get(),
                "deleted_at_ms": item.deleted_at_ms,
                "final_status": status_name(item.final_status),
                "tenant_id": item.tenant_id.as_str(),
                "terminal_at_ms": item.terminal_at_ms,
                "workflow": item.workflow,
                "workflow_version": item.workflow_version,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&serde_json::json!({
        "batch_id": batch.batch_id.as_str(),
        "schema": "runifold.task-tombstones.v1",
        "tenant_id": batch.tenant_id.as_str(),
        "tombstones": tombstones,
    }))
    .map_err(|error| archive_error(format!("tombstone archive encoding failed: {error}")))
}

fn status_name(status: WorkflowTaskStatus) -> &'static str {
    match status {
        WorkflowTaskStatus::Queued => "queued",
        WorkflowTaskStatus::Leased => "leased",
        WorkflowTaskStatus::Waiting => "waiting",
        WorkflowTaskStatus::Completed => "completed",
        WorkflowTaskStatus::Failed => "failed",
        WorkflowTaskStatus::Cancelled => "cancelled",
        _ => "unknown",
    }
}

fn object_key(prefix: &str, batch_id: &str) -> String {
    if prefix.is_empty() {
        format!("{batch_id}.json")
    } else {
        format!("{prefix}/{batch_id}.json")
    }
}

fn receipt(
    bucket: &str,
    key: &str,
    checksum: &str,
) -> Result<WorkflowTaskTombstoneExportReceipt, WorkflowTaskTombstoneArchiveError> {
    WorkflowTaskTombstoneExportReceipt::parse(format!("s3:{bucket}:{key}:sha256:{checksum}"))
        .map_err(|_| archive_error("S3 archive receipt exceeded its portable bound"))
}

fn insert(
    headers: &mut HeaderMap,
    name: &'static str,
    value: &str,
) -> Result<(), WorkflowTaskTombstoneArchiveError> {
    let name = HeaderName::from_static(name);
    let value = HeaderValue::from_str(value)
        .map_err(|_| config_error("S3 archive header contains invalid bytes"))?;
    headers.insert(name, value);
    Ok(())
}

fn config_error(message: impl Into<String>) -> WorkflowTaskTombstoneArchiveError {
    classified_error(
        WorkflowTaskTombstoneArchiveErrorKind::Configuration,
        message,
    )
}

fn archive_error(message: impl Into<String>) -> WorkflowTaskTombstoneArchiveError {
    WorkflowTaskTombstoneArchiveError::new(message)
}

fn classified_error(
    kind: WorkflowTaskTombstoneArchiveErrorKind,
    message: impl Into<String>,
) -> WorkflowTaskTombstoneArchiveError {
    WorkflowTaskTombstoneArchiveError::with_kind(kind, message)
}

#[cfg(test)]
mod tests {
    use std::{
        fmt::Write as _,
        io::{Read, Write},
        net::TcpListener,
        thread,
        time::{Duration, Instant},
    };

    use runifold_core::CheckpointId;
    use runifold_workflow::{
        WorkflowTaskTombstone, WorkflowTaskTombstoneArchiveBatchId, WorkflowTaskTombstoneCursor,
        WorkflowTenantId,
    };

    use super::*;

    #[tokio::test]
    async fn real_http_put_and_conflict_head_replay_are_idempotent() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let server = thread::spawn(move || {
            let (stream, first) = read_request(&listener);
            let checksum = header(&first, CHECKSUM_METADATA).to_owned();
            assert!(first.starts_with("PUT /runifold-archive/tombstones/"));
            assert!(first.contains("X-Amz-Signature="));
            assert_eq!(header(&first, "if-none-match"), "*");
            assert_eq!(header(&first, "x-amz-server-side-encryption"), "AES256");
            respond(stream, "200 OK", &[]);

            let (stream, replay) = read_request(&listener);
            assert!(replay.starts_with("PUT /runifold-archive/"));
            respond(stream, "412 Precondition Failed", &[]);

            let (stream, head) = read_request(&listener);
            assert!(head.starts_with("HEAD /runifold-archive/"));
            respond(stream, "200 OK", &[(CHECKSUM_METADATA, checksum.as_str())]);
        });
        let signer = S3SigV4Presigner::new(
            S3SigV4PresignerConfig::new(endpoint, "us-east-1", 60, true).unwrap(),
            S3SigV4Credentials::new("ACCESS", "SECRET", Some("TOKEN".into())).unwrap(),
        );
        let archive = S3TombstoneArchive::new(
            S3TombstoneArchiveConfig::new(
                "runifold-archive",
                "tombstones",
                S3ArchiveEncryption::Aes256,
            )
            .unwrap(),
            Arc::new(signer),
        );
        let batch = batch();
        let (first, replay) = tokio::join!(archive.archive(batch.clone()), archive.archive(batch));
        let first = first.unwrap();
        let replay = replay.unwrap();
        assert_eq!(first, replay);
        server.join().unwrap();
    }

    #[tokio::test]
    async fn request_timeout_bounds_put_and_reconciliation() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let server = thread::spawn(move || {
            let (put, request) = read_request(&listener);
            assert!(request.starts_with("PUT "));
            let (head, request) = read_request(&listener);
            assert!(request.starts_with("HEAD "));
            thread::sleep(Duration::from_millis(100));
            drop((put, head));
        });
        let signer = S3SigV4Presigner::new(
            S3SigV4PresignerConfig::new(endpoint, "us-east-1", 60, true).unwrap(),
            S3SigV4Credentials::new("ACCESS", "SECRET", None).unwrap(),
        );
        let config = S3TombstoneArchiveConfig::new(
            "runifold-archive",
            "tombstones",
            S3ArchiveEncryption::Aes256,
        )
        .unwrap()
        .with_request_timeout(Duration::from_millis(20))
        .unwrap();
        let archive = S3TombstoneArchive::new(config, Arc::new(signer));

        let started = Instant::now();
        let error = archive.archive(batch()).await.unwrap_err();

        assert_eq!(error.kind(), WorkflowTaskTombstoneArchiveErrorKind::Timeout);
        assert!(started.elapsed() < Duration::from_secs(1));
        server.join().unwrap();
    }

    #[test]
    fn s3_error_details_are_bounded_to_safe_tokens() {
        let body = "<Error><Code>NotImplemented</Code><Header>If-None-Match</Header></Error>";
        assert_eq!(safe_xml_token(body, "Code"), Some("NotImplemented"));
        assert_eq!(safe_xml_token(body, "Header"), Some("If-None-Match"));
        assert_eq!(safe_xml_token("<Code>secret value</Code>", "Code"), None);
        assert_eq!(safe_xml_token("<Code>../../secret</Code>", "Code"), None);
        assert_eq!(
            response_status_kind(StatusCode::FORBIDDEN),
            WorkflowTaskTombstoneArchiveErrorKind::Authorization
        );
        assert_eq!(
            response_status_kind(StatusCode::SERVICE_UNAVAILABLE),
            WorkflowTaskTombstoneArchiveErrorKind::Unavailable
        );
    }

    #[test]
    fn request_timeout_policy_rejects_unbounded_values() {
        let config = S3TombstoneArchiveConfig::new(
            "runifold-archive",
            "tombstones",
            S3ArchiveEncryption::Aes256,
        )
        .unwrap();
        assert_eq!(
            config
                .clone()
                .with_request_timeout(Duration::ZERO)
                .unwrap_err()
                .kind(),
            WorkflowTaskTombstoneArchiveErrorKind::Configuration
        );
        assert_eq!(
            config
                .with_request_timeout(Duration::from_secs(601))
                .unwrap_err()
                .kind(),
            WorkflowTaskTombstoneArchiveErrorKind::Configuration
        );
    }

    fn batch() -> WorkflowTaskTombstoneArchiveBatch {
        let tenant_id = WorkflowTenantId::parse("tenant-a").unwrap();
        WorkflowTaskTombstoneArchiveBatch {
            batch_id: WorkflowTaskTombstoneArchiveBatchId::parse("tenant-a:1:1").unwrap(),
            tenant_id: tenant_id.clone(),
            tombstones: vec![WorkflowTaskTombstone {
                cursor: WorkflowTaskTombstoneCursor::new(1),
                checkpoint_id: CheckpointId::new(),
                tenant_id,
                workflow: "archive-test".into(),
                workflow_version: 1,
                final_status: WorkflowTaskStatus::Completed,
                created_at_ms: 1,
                terminal_at_ms: 2,
                deleted_at_ms: 3,
            }],
        }
    }

    fn read_request(listener: &TcpListener) -> (std::net::TcpStream, String) {
        let (mut stream, _) = listener.accept().unwrap();
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            let count = stream.read(&mut buffer).unwrap();
            if count == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..count]);
            let text = String::from_utf8_lossy(&bytes);
            let Some(header_end) = text.find("\r\n\r\n") else {
                continue;
            };
            let length = header(&text, "content-length")
                .parse::<usize>()
                .unwrap_or_default();
            if bytes.len() >= header_end + 4 + length {
                break;
            }
        }
        (stream, String::from_utf8(bytes).unwrap())
    }

    fn header<'a>(request: &'a str, name: &str) -> &'a str {
        request
            .lines()
            .find_map(|line| {
                let (candidate, value) = line.split_once(':')?;
                candidate.eq_ignore_ascii_case(name).then_some(value.trim())
            })
            .unwrap_or("")
    }

    fn respond(mut stream: std::net::TcpStream, status: &str, headers: &[(&str, &str)]) {
        let mut response =
            format!("HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: 0\r\n");
        for (name, value) in headers {
            write!(response, "{name}: {value}\r\n").unwrap();
        }
        response.push_str("\r\n");
        stream.write_all(response.as_bytes()).unwrap();
    }
}
