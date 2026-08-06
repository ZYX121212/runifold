use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    ContentPart, MediaSource, Model, ModelCallContext, ModelCapabilities, ModelError,
    ModelErrorKind, ModelEventStream, ModelFuture, ModelRef, ModelRequest, ProviderModel,
};

/// Maximum artifact size accepted by the reference store and resolver.
pub const DEFAULT_MAX_ARTIFACT_BYTES: usize = 16 * 1024 * 1024;
/// Maximum number of artifact references returned or deleted per operation.
pub const MAX_ARTIFACT_PAGE_SIZE: u32 = 1_000;
/// Maximum UTF-8 byte length of an artifact idempotency key.
pub const MAX_ARTIFACT_IDEMPOTENCY_KEY_BYTES: usize = 512;
/// Maximum UTF-8 byte length of an artifact display name.
pub const MAX_ARTIFACT_NAME_BYTES: usize = 256;

/// Validated tenant or application isolation boundary for artifacts.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ArtifactScope(String);

impl ArtifactScope {
    /// Parses a bounded, storage-safe scope.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactError::InvalidInput`] when the value is empty, too
    /// long, or contains characters that are unsafe for storage boundaries.
    pub fn parse(value: impl Into<String>) -> Result<Self, ArtifactError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':')
            })
        {
            return Err(ArtifactError::InvalidInput(
                "artifact scope must contain 1..=128 safe ASCII characters".into(),
            ));
        }
        Ok(Self(value))
    }

    /// Returns the validated scope text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ArtifactScope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

/// A stable, integrity-bound reference to binary artifact content.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactRef {
    /// Tenant or application isolation boundary.
    pub scope: ArtifactScope,
    /// Store-owned stable identity.
    pub artifact_id: String,
    /// Trusted MIME type associated with the stored bytes.
    pub media_type: String,
    /// Raw content length.
    pub size_bytes: u64,
    /// Lowercase SHA-256 digest.
    pub sha256: String,
    /// Optional neutral display name.
    pub name: Option<String>,
    /// Store-assigned creation time in Unix milliseconds.
    pub created_at_unix_ms: u64,
    /// Optional expiration time in Unix milliseconds.
    pub expires_at_unix_ms: Option<u64>,
}

impl ArtifactRef {
    /// Converts this reference into canonical lazy media.
    pub fn media_source(&self) -> MediaSource {
        MediaSource::Artifact {
            reference: self.clone(),
        }
    }
}

/// One bounded page of artifact references in stable identity order.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ArtifactPage {
    /// References visible in the requested scope.
    pub items: Vec<ArtifactRef>,
    /// Exclusive cursor for the next page.
    pub next_cursor: Option<String>,
}

/// Bytes loaded from an artifact store together with verified metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Artifact {
    /// Integrity-bound reference.
    pub reference: ArtifactRef,
    /// Raw bytes.
    pub bytes: Vec<u8>,
}

/// Idempotent artifact creation request.
#[derive(Clone, Debug)]
pub struct ArtifactWrite {
    /// Tenant or application isolation boundary.
    scope: ArtifactScope,
    /// Stable key reused across retries of one logical write.
    idempotency_key: String,
    /// Trusted MIME type.
    media_type: String,
    /// Optional neutral display name.
    name: Option<String>,
    /// Raw bytes.
    bytes: Vec<u8>,
    /// Optional retention deadline.
    expires_at_unix_ms: Option<u64>,
}

impl ArtifactWrite {
    /// Creates a validated write request.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactError`] for blank identities, invalid MIME values, or
    /// empty content.
    pub fn new(
        scope: ArtifactScope,
        idempotency_key: impl Into<String>,
        media_type: impl Into<String>,
        bytes: Vec<u8>,
    ) -> Result<Self, ArtifactError> {
        let idempotency_key = idempotency_key.into();
        let media_type = media_type.into();
        if idempotency_key.trim().is_empty()
            || idempotency_key.len() > MAX_ARTIFACT_IDEMPOTENCY_KEY_BYTES
            || idempotency_key.chars().any(char::is_control)
        {
            return Err(ArtifactError::InvalidInput(format!(
                "artifact idempotency key must contain 1..={MAX_ARTIFACT_IDEMPOTENCY_KEY_BYTES} non-control UTF-8 bytes"
            )));
        }
        if !valid_media_type(&media_type) {
            return Err(ArtifactError::InvalidInput(
                "artifact MIME type is invalid".into(),
            ));
        }
        if bytes.is_empty() {
            return Err(ArtifactError::InvalidInput(
                "artifact content cannot be empty".into(),
            ));
        }
        if !content_matches_media_type(&media_type, &bytes) {
            return Err(ArtifactError::InvalidInput(
                "artifact bytes do not match the declared MIME type".into(),
            ));
        }
        Ok(Self {
            scope,
            idempotency_key,
            media_type,
            name: None,
            bytes,
            expires_at_unix_ms: None,
        })
    }

    /// Sets an absolute retention deadline portable across durable stores.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactError::InvalidInput`] when the timestamp exceeds the
    /// signed 64-bit millisecond representation shared by `SQLite` and
    /// `PostgreSQL`.
    pub fn with_expires_at_unix_ms(
        mut self,
        expires_at_unix_ms: u64,
    ) -> Result<Self, ArtifactError> {
        if expires_at_unix_ms > i64::MAX as u64 {
            return Err(ArtifactError::InvalidInput(
                "artifact expiration exceeds the portable i64 millisecond range".into(),
            ));
        }
        self.expires_at_unix_ms = Some(expires_at_unix_ms);
        Ok(self)
    }

    /// Returns the isolation scope.
    pub const fn scope(&self) -> &ArtifactScope {
        &self.scope
    }

    /// Sets a bounded neutral display name.
    ///
    /// # Errors
    ///
    /// Returns [`ArtifactError::InvalidInput`] for a blank, oversized, or
    /// control-character-containing name.
    pub fn with_name(mut self, name: impl Into<String>) -> Result<Self, ArtifactError> {
        let name = name.into();
        if name.trim().is_empty()
            || name.len() > MAX_ARTIFACT_NAME_BYTES
            || name.chars().any(char::is_control)
        {
            return Err(ArtifactError::InvalidInput(format!(
                "artifact name must contain 1..={MAX_ARTIFACT_NAME_BYTES} non-control UTF-8 bytes"
            )));
        }
        self.name = Some(name);
        Ok(self)
    }

    /// Returns the stable idempotency key.
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    /// Returns the trusted MIME type.
    pub fn media_type(&self) -> &str {
        &self.media_type
    }

    /// Returns the optional display name.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns the raw artifact bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the optional retention deadline.
    pub const fn expires_at_unix_ms(&self) -> Option<u64> {
        self.expires_at_unix_ms
    }

    /// Returns whether a stored reference represents this exact immutable
    /// write, excluding only the store-assigned creation time.
    pub fn matches_immutable_reference(&self, reference: &ArtifactRef) -> bool {
        reference.scope == self.scope
            && reference.artifact_id == artifact_identity(&self.media_type, &self.bytes)
            && reference.media_type == self.media_type
            && reference.size_bytes == self.bytes.len() as u64
            && reference.sha256 == sha256(&self.bytes)
            && reference.name == self.name
            && reference.expires_at_unix_ms == self.expires_at_unix_ms
    }
}

/// Typed artifact persistence or integrity failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ArtifactError {
    /// Caller supplied an invalid request.
    #[error("invalid artifact input: {0}")]
    InvalidInput(String),
    /// Artifact identity was not found.
    #[error("artifact `{0}` was not found")]
    NotFound(String),
    /// One idempotency key was reused for different content.
    #[error("artifact idempotency conflict for `{0}`")]
    IdempotencyConflict(String),
    /// The same content address already exists with different immutable metadata.
    #[error("artifact metadata conflict for `{0}`")]
    MetadataConflict(String),
    /// Stored content no longer matches its integrity metadata.
    #[error("artifact integrity check failed for `{0}`")]
    Integrity(String),
    /// Artifact exists but is past its retention deadline.
    #[error("artifact `{0}` has expired")]
    Expired(String),
    /// Backend storage failed.
    #[error("artifact storage failed: {0}")]
    Storage(String),
}

/// A boxed artifact-store future.
#[cfg(not(target_arch = "wasm32"))]
pub type ArtifactFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A boxed artifact-store future for single-threaded WASM.
#[cfg(target_arch = "wasm32")]
pub type ArtifactFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Binary artifact persistence boundary.
pub trait ArtifactStore: Send + Sync {
    /// Idempotently writes content and returns its integrity-bound reference.
    fn put(&self, write: ArtifactWrite) -> ArtifactFuture<'_, Result<ArtifactRef, ArtifactError>>;

    /// Loads and verifies one artifact.
    fn get(&self, reference: &ArtifactRef) -> ArtifactFuture<'_, Result<Artifact, ArtifactError>>;

    /// Lists one stable, bounded page inside one scope.
    fn list(
        &self,
        scope: &ArtifactScope,
        after: Option<&str>,
        limit: u32,
    ) -> ArtifactFuture<'_, Result<ArtifactPage, ArtifactError>>;

    /// Idempotently deletes one artifact and its idempotency records.
    fn delete(
        &self,
        scope: &ArtifactScope,
        artifact_id: &str,
    ) -> ArtifactFuture<'_, Result<bool, ArtifactError>>;

    /// Deletes at most `limit` expired artifacts in one scope.
    fn purge_expired(
        &self,
        scope: &ArtifactScope,
        now_unix_ms: u64,
        limit: u32,
    ) -> ArtifactFuture<'_, Result<u32, ArtifactError>>;
}

impl<T> ArtifactStore for Arc<T>
where
    T: ArtifactStore + ?Sized,
{
    fn put(&self, write: ArtifactWrite) -> ArtifactFuture<'_, Result<ArtifactRef, ArtifactError>> {
        (**self).put(write)
    }

    fn get(&self, reference: &ArtifactRef) -> ArtifactFuture<'_, Result<Artifact, ArtifactError>> {
        (**self).get(reference)
    }

    fn list(
        &self,
        scope: &ArtifactScope,
        after: Option<&str>,
        limit: u32,
    ) -> ArtifactFuture<'_, Result<ArtifactPage, ArtifactError>> {
        (**self).list(scope, after, limit)
    }

    fn delete(
        &self,
        scope: &ArtifactScope,
        artifact_id: &str,
    ) -> ArtifactFuture<'_, Result<bool, ArtifactError>> {
        (**self).delete(scope, artifact_id)
    }

    fn purge_expired(
        &self,
        scope: &ArtifactScope,
        now_unix_ms: u64,
        limit: u32,
    ) -> ArtifactFuture<'_, Result<u32, ArtifactError>> {
        (**self).purge_expired(scope, now_unix_ms, limit)
    }
}

/// Cloneable bounded in-memory artifact store for tests and local processes.
#[derive(Clone, Debug)]
pub struct InMemoryArtifactStore {
    state: Arc<Mutex<MemoryState>>,
    max_artifact_bytes: usize,
}

#[derive(Debug, Default)]
struct MemoryState {
    artifacts: BTreeMap<(ArtifactScope, String), Artifact>,
    idempotency: BTreeMap<(ArtifactScope, String), String>,
}

impl InMemoryArtifactStore {
    /// Creates a store with the default 16 MiB per-artifact limit.
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(MemoryState::default())),
            max_artifact_bytes: DEFAULT_MAX_ARTIFACT_BYTES,
        }
    }

    /// Replaces the per-artifact byte limit.
    #[must_use]
    pub const fn with_max_artifact_bytes(mut self, limit: usize) -> Self {
        self.max_artifact_bytes = limit;
        self
    }
}

impl Default for InMemoryArtifactStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ArtifactStore for InMemoryArtifactStore {
    fn put(&self, write: ArtifactWrite) -> ArtifactFuture<'_, Result<ArtifactRef, ArtifactError>> {
        Box::pin(async move {
            if write.bytes.len() > self.max_artifact_bytes {
                return Err(ArtifactError::InvalidInput(format!(
                    "artifact is {} bytes and exceeds the {}-byte limit",
                    write.bytes.len(),
                    self.max_artifact_bytes
                )));
            }
            let digest = sha256(&write.bytes);
            let artifact_id = artifact_identity(&write.media_type, &write.bytes);
            let scope = write.scope.clone();
            let artifact_key = (scope.clone(), artifact_id.clone());
            let idempotency_key = (scope.clone(), write.idempotency_key.clone());
            let mut state = self
                .state
                .lock()
                .map_err(|_| ArtifactError::Storage("artifact store lock is poisoned".into()))?;
            if let Some(existing_id) = state.idempotency.get(&idempotency_key) {
                if existing_id != &artifact_id {
                    return Err(ArtifactError::IdempotencyConflict(write.idempotency_key));
                }
                let reference = state
                    .artifacts
                    .get(&(scope, existing_id.clone()))
                    .map(|artifact| artifact.reference.clone())
                    .ok_or_else(|| ArtifactError::Integrity(existing_id.clone()))?;
                if !write.matches_immutable_reference(&reference) {
                    return Err(ArtifactError::IdempotencyConflict(write.idempotency_key));
                }
                return Ok(reference);
            }
            if let Some(existing) = state.artifacts.get(&artifact_key) {
                if !write.matches_immutable_reference(&existing.reference) {
                    return Err(ArtifactError::MetadataConflict(artifact_id));
                }
                let reference = existing.reference.clone();
                state.idempotency.insert(idempotency_key, artifact_id);
                return Ok(reference);
            }
            let reference = ArtifactRef {
                scope,
                artifact_id: artifact_id.clone(),
                media_type: write.media_type,
                size_bytes: write.bytes.len() as u64,
                sha256: digest,
                name: write.name,
                created_at_unix_ms: unix_time_ms()?,
                expires_at_unix_ms: write.expires_at_unix_ms,
            };
            state.artifacts.insert(
                artifact_key,
                Artifact {
                    reference: reference.clone(),
                    bytes: write.bytes,
                },
            );
            state.idempotency.insert(idempotency_key, artifact_id);
            Ok(reference)
        })
    }

    fn get(&self, reference: &ArtifactRef) -> ArtifactFuture<'_, Result<Artifact, ArtifactError>> {
        let reference = reference.clone();
        Box::pin(async move {
            let artifact = self
                .state
                .lock()
                .map_err(|_| ArtifactError::Storage("artifact store lock is poisoned".into()))?
                .artifacts
                .get(&(reference.scope.clone(), reference.artifact_id.clone()))
                .cloned()
                .ok_or_else(|| ArtifactError::NotFound(reference.artifact_id.clone()))?;
            verify_artifact(&artifact)?;
            verify_reference(&reference, &artifact.reference)?;
            ensure_not_expired(&artifact.reference, unix_time_ms()?)?;
            Ok(artifact)
        })
    }

    fn list(
        &self,
        scope: &ArtifactScope,
        after: Option<&str>,
        limit: u32,
    ) -> ArtifactFuture<'_, Result<ArtifactPage, ArtifactError>> {
        let scope = scope.clone();
        let after = after.map(str::to_owned);
        Box::pin(async move {
            validate_page_limit(limit)?;
            let state = self
                .state
                .lock()
                .map_err(|_| ArtifactError::Storage("artifact store lock is poisoned".into()))?;
            let mut items = state
                .artifacts
                .iter()
                .filter(|((item_scope, id), _)| {
                    item_scope == &scope && after.as_ref().is_none_or(|cursor| id > cursor)
                })
                .map(|(_, artifact)| artifact.reference.clone())
                .take(limit as usize + 1)
                .collect::<Vec<_>>();
            let next_cursor = if items.len() > limit as usize {
                items.pop();
                items.last().map(|item| item.artifact_id.clone())
            } else {
                None
            };
            Ok(ArtifactPage { items, next_cursor })
        })
    }

    fn delete(
        &self,
        scope: &ArtifactScope,
        artifact_id: &str,
    ) -> ArtifactFuture<'_, Result<bool, ArtifactError>> {
        let scope = scope.clone();
        let artifact_id = artifact_id.to_owned();
        Box::pin(async move {
            let mut state = self
                .state
                .lock()
                .map_err(|_| ArtifactError::Storage("artifact store lock is poisoned".into()))?;
            let removed = state
                .artifacts
                .remove(&(scope.clone(), artifact_id.clone()))
                .is_some();
            if removed {
                state
                    .idempotency
                    .retain(|(item_scope, _), id| item_scope != &scope || id != &artifact_id);
            }
            Ok(removed)
        })
    }

    fn purge_expired(
        &self,
        scope: &ArtifactScope,
        now_unix_ms: u64,
        limit: u32,
    ) -> ArtifactFuture<'_, Result<u32, ArtifactError>> {
        let scope = scope.clone();
        Box::pin(async move {
            validate_page_limit(limit)?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| ArtifactError::Storage("artifact store lock is poisoned".into()))?;
            let ids = state
                .artifacts
                .iter()
                .filter(|((item_scope, _), artifact)| {
                    item_scope == &scope
                        && artifact
                            .reference
                            .expires_at_unix_ms
                            .is_some_and(|expires| expires <= now_unix_ms)
                })
                .map(|((_, id), _)| id.clone())
                .take(limit as usize)
                .collect::<Vec<_>>();
            for id in &ids {
                state.artifacts.remove(&(scope.clone(), id.clone()));
                state.idempotency.retain(|(item_scope, _), artifact_id| {
                    item_scope != &scope || artifact_id != id
                });
            }
            u32::try_from(ids.len())
                .map_err(|_| ArtifactError::Storage("purge count exceeds u32".into()))
        })
    }
}

/// Model decorator that resolves lazy artifact references immediately before
/// provider transport while keeping transcripts and checkpoints reference-only.
#[derive(Clone, Debug)]
pub struct ArtifactResolvingModel<M, S> {
    inner: M,
    scope: ArtifactScope,
    store: S,
    max_artifact_bytes: usize,
}

impl<M, S> ArtifactResolvingModel<M, S> {
    /// Wraps a model with an artifact store.
    pub fn new(inner: M, scope: ArtifactScope, store: S) -> Self {
        Self {
            inner,
            scope,
            store,
            max_artifact_bytes: DEFAULT_MAX_ARTIFACT_BYTES,
        }
    }

    /// Replaces the maximum resolved artifact size.
    #[must_use]
    pub const fn with_max_artifact_bytes(mut self, limit: usize) -> Self {
        self.max_artifact_bytes = limit;
        self
    }
}

impl<M, S> Model for ArtifactResolvingModel<M, S>
where
    M: Model,
    S: ArtifactStore,
{
    fn capabilities<'a>(
        &'a self,
        model: &'a ModelRef,
    ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
        self.inner.capabilities(model)
    }

    fn stream(
        &self,
        mut request: ModelRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
        Box::pin(async move {
            resolve_request_artifacts(
                &mut request,
                &self.scope,
                &self.store,
                self.max_artifact_bytes,
            )
            .await?;
            self.inner.stream(request, context).await
        })
    }
}

impl<M, S> ProviderModel for ArtifactResolvingModel<M, S>
where
    M: ProviderModel,
    S: ArtifactStore,
{
    fn provider(&self) -> &str {
        self.inner.provider()
    }
}

async fn resolve_request_artifacts<S: ArtifactStore>(
    request: &mut ModelRequest,
    scope: &ArtifactScope,
    store: &S,
    max_artifact_bytes: usize,
) -> Result<(), ModelError> {
    for message in &mut request.messages {
        for part in &mut message.content {
            resolve_content_artifacts(part, scope, store, max_artifact_bytes).await?;
        }
    }
    Ok(())
}

async fn resolve_content_artifacts<S: ArtifactStore>(
    part: &mut ContentPart,
    scope: &ArtifactScope,
    store: &S,
    max_artifact_bytes: usize,
) -> Result<(), ModelError> {
    match part {
        ContentPart::Image { source }
        | ContentPart::Audio { source }
        | ContentPart::Document { source, .. } => {
            resolve_source(source, scope, store, max_artifact_bytes).await
        }
        ContentPart::ToolResult(result) => {
            for content in &mut result.content {
                Box::pin(resolve_content_artifacts(
                    content,
                    scope,
                    store,
                    max_artifact_bytes,
                ))
                .await?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

async fn resolve_source<S: ArtifactStore>(
    source: &mut MediaSource,
    scope: &ArtifactScope,
    store: &S,
    max_artifact_bytes: usize,
) -> Result<(), ModelError> {
    let MediaSource::Artifact { reference } = source else {
        return Ok(());
    };
    if &reference.scope != scope {
        return Err(ModelError::local(
            ModelErrorKind::InvalidRequest,
            "artifact reference belongs to a different scope",
        ));
    }
    let artifact = store
        .get(reference)
        .await
        .map_err(|error| artifact_model_error(&error))?;
    if artifact.bytes.len() > max_artifact_bytes {
        return Err(ModelError::local(
            ModelErrorKind::InvalidRequest,
            format!(
                "artifact `{}` is {} bytes and exceeds the {max_artifact_bytes}-byte resolution limit",
                reference.artifact_id,
                artifact.bytes.len()
            ),
        ));
    }
    *source = MediaSource::Base64 {
        media_type: artifact.reference.media_type,
        data: STANDARD.encode(artifact.bytes),
    };
    Ok(())
}

fn verify_artifact(artifact: &Artifact) -> Result<(), ArtifactError> {
    if artifact.reference.size_bytes != artifact.bytes.len() as u64
        || artifact.reference.sha256 != sha256(&artifact.bytes)
        || artifact.reference.artifact_id
            != artifact_identity(&artifact.reference.media_type, &artifact.bytes)
    {
        return Err(ArtifactError::Integrity(
            artifact.reference.artifact_id.clone(),
        ));
    }
    Ok(())
}

fn verify_reference(expected: &ArtifactRef, actual: &ArtifactRef) -> Result<(), ArtifactError> {
    if expected != actual {
        return Err(ArtifactError::Integrity(expected.artifact_id.clone()));
    }
    Ok(())
}

fn ensure_not_expired(reference: &ArtifactRef, now_unix_ms: u64) -> Result<(), ArtifactError> {
    if reference
        .expires_at_unix_ms
        .is_some_and(|expires| expires <= now_unix_ms)
    {
        return Err(ArtifactError::Expired(reference.artifact_id.clone()));
    }
    Ok(())
}

fn validate_page_limit(limit: u32) -> Result<(), ArtifactError> {
    if limit == 0 || limit > MAX_ARTIFACT_PAGE_SIZE {
        return Err(ArtifactError::InvalidInput(format!(
            "artifact page limit must be between 1 and {MAX_ARTIFACT_PAGE_SIZE}"
        )));
    }
    Ok(())
}

fn unix_time_ms() -> Result<u64, ArtifactError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ArtifactError::Storage(error.to_string()))?;
    u64::try_from(elapsed.as_millis())
        .map_err(|_| ArtifactError::Storage("system time exceeds u64 milliseconds".into()))
}

fn sha256(bytes: &[u8]) -> String {
    hex_digest(Sha256::digest(bytes))
}

fn artifact_identity(media_type: &str, bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(media_type.as_bytes());
    digest.update([0]);
    digest.update(bytes);
    format!("sha256:{}", hex_digest(digest.finalize()))
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = digest.as_ref();
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn valid_media_type(value: &str) -> bool {
    let Some((kind, subtype)) = value.split_once('/') else {
        return false;
    };
    valid_media_token(kind) && valid_media_token(subtype)
}

fn valid_media_token(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

fn content_matches_media_type(media_type: &str, bytes: &[u8]) -> bool {
    match media_type {
        "image/png" => bytes.starts_with(b"\x89PNG\r\n\x1a\n"),
        "image/jpeg" => bytes.starts_with(&[0xff, 0xd8, 0xff]),
        "image/gif" => bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a"),
        "image/webp" => bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP",
        "audio/wav" => bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WAVE",
        "audio/mpeg" => {
            bytes.starts_with(b"ID3")
                || bytes
                    .get(..2)
                    .is_some_and(|prefix| prefix[0] == 0xff && prefix[1] & 0xe0 == 0xe0)
        }
        "audio/ogg" => bytes.starts_with(b"OggS"),
        "application/pdf" => bytes.starts_with(b"%PDF-"),
        value if value.starts_with("text/") => std::str::from_utf8(bytes).is_ok(),
        _ => true,
    }
}

fn artifact_model_error(error: &ArtifactError) -> ModelError {
    let kind = match error {
        ArtifactError::NotFound(_)
        | ArtifactError::InvalidInput(_)
        | ArtifactError::Expired(_)
        | ArtifactError::IdempotencyConflict(_)
        | ArtifactError::MetadataConflict(_) => ModelErrorKind::InvalidRequest,
        ArtifactError::Integrity(_) | ArtifactError::Storage(_) => ModelErrorKind::Protocol,
    };
    ModelError::local(kind, error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use futures_executor::block_on;

    use super::*;

    const PNG: &[u8] = b"\x89PNG\r\n\x1a\npng";

    fn scope() -> ArtifactScope {
        ArtifactScope::parse("tenant.test").unwrap()
    }

    #[test]
    fn in_memory_store_is_content_addressed_idempotent_and_integrity_bound() {
        let store = InMemoryArtifactStore::new();
        let first =
            block_on(store.put(
                ArtifactWrite::new(scope(), "turn-1:image", "image/png", PNG.to_vec()).unwrap(),
            ))
            .unwrap();
        let replay =
            block_on(store.put(
                ArtifactWrite::new(scope(), "turn-1:image", "image/png", PNG.to_vec()).unwrap(),
            ))
            .unwrap();
        assert_eq!(first, replay);
        assert_eq!(block_on(store.get(&first)).unwrap().bytes, PNG);

        let conflict = block_on(
            store.put(
                ArtifactWrite::new(
                    scope(),
                    "turn-1:image",
                    "image/png",
                    b"\x89PNG\r\n\x1a\nother".to_vec(),
                )
                .unwrap(),
            ),
        )
        .unwrap_err();
        assert!(matches!(conflict, ArtifactError::IdempotencyConflict(_)));
    }

    #[test]
    fn request_resolution_keeps_storage_references_outside_provider_transport() {
        let store = InMemoryArtifactStore::new();
        let reference =
            block_on(store.put(
                ArtifactWrite::new(scope(), "turn-2:image", "image/png", PNG.to_vec()).unwrap(),
            ))
            .unwrap();
        let result = crate::ToolResult {
            call_id: "call-1".into(),
            name: Some("render".into()),
            content: vec![ContentPart::Image {
                source: reference.media_source(),
            }],
            structured_content: None,
            is_error: false,
            metadata: BTreeMap::new(),
        };
        let message =
            crate::Message::new(crate::Role::Tool, vec![ContentPart::ToolResult(result)]).unwrap();
        let mut request = ModelRequest::new(ModelRef::new("test", "vision"), message);

        block_on(resolve_request_artifacts(
            &mut request,
            &scope(),
            &store,
            DEFAULT_MAX_ARTIFACT_BYTES,
        ))
        .unwrap();

        let ContentPart::ToolResult(result) = &request.messages[0].content[0] else {
            panic!("tool result must remain canonical");
        };
        assert!(matches!(
            &result.content[0],
            ContentPart::Image {
                source: MediaSource::Base64 { media_type, data }
            } if media_type == "image/png" && data == &STANDARD.encode(PNG)
        ));
    }

    #[test]
    fn rejects_known_media_with_mismatched_magic_bytes() {
        let error = ArtifactWrite::new(scope(), "turn-3:image", "image/png", b"not-png".to_vec())
            .unwrap_err();

        assert!(matches!(error, ArtifactError::InvalidInput(_)));
    }

    #[test]
    fn deserialization_and_write_metadata_cannot_bypass_validation() {
        assert!(serde_json::from_str::<ArtifactScope>("\"../tenant\"").is_err());
        assert!(
            ArtifactWrite::new(
                scope(),
                "x".repeat(MAX_ARTIFACT_IDEMPOTENCY_KEY_BYTES + 1),
                "text/plain",
                b"text".to_vec(),
            )
            .is_err()
        );
        assert!(
            ArtifactWrite::new(scope(), "key", "text/plain", b"text".to_vec())
                .unwrap()
                .with_name("bad\nname")
                .is_err()
        );
        assert!(ArtifactWrite::new(scope(), "key", "image//png", b"text".to_vec()).is_err());
        assert!(
            ArtifactWrite::new(scope(), "key", "text/plain", b"text".to_vec())
                .unwrap()
                .with_expires_at_unix_ms(u64::MAX)
                .is_err()
        );
    }

    #[test]
    fn immutable_metadata_is_bound_to_content_and_idempotency() {
        let store = InMemoryArtifactStore::new();
        let original = ArtifactWrite::new(scope(), "first", "text/plain", b"same".to_vec())
            .unwrap()
            .with_name("original")
            .unwrap();
        let reference = block_on(store.put(original)).unwrap();

        let replay_with_changed_expiry =
            ArtifactWrite::new(scope(), "first", "text/plain", b"same".to_vec())
                .unwrap()
                .with_name("original")
                .unwrap()
                .with_expires_at_unix_ms(i64::MAX as u64)
                .unwrap();
        assert!(matches!(
            block_on(store.put(replay_with_changed_expiry)),
            Err(ArtifactError::IdempotencyConflict(_))
        ));

        let alias_with_changed_name =
            ArtifactWrite::new(scope(), "second", "text/plain", b"same".to_vec())
                .unwrap()
                .with_name("changed")
                .unwrap();
        assert!(matches!(
            block_on(store.put(alias_with_changed_name)),
            Err(ArtifactError::MetadataConflict(_))
        ));
        assert_eq!(
            block_on(store.get(&reference))
                .unwrap()
                .reference
                .name
                .as_deref(),
            Some("original")
        );
    }

    #[test]
    fn scopes_pagination_expiration_and_deletion_are_enforced() {
        let store = InMemoryArtifactStore::new();
        let left = scope();
        let right = ArtifactScope::parse("tenant.other").unwrap();
        let expired = block_on(
            store.put(
                ArtifactWrite::new(left.clone(), "expired", "image/png", PNG.to_vec())
                    .unwrap()
                    .with_expires_at_unix_ms(1)
                    .unwrap(),
            ),
        )
        .unwrap();
        let active = block_on(store.put(
            ArtifactWrite::new(left.clone(), "active", "text/plain", b"active".to_vec()).unwrap(),
        ))
        .unwrap();
        let isolated = block_on(store.put(
            ArtifactWrite::new(right.clone(), "active", "text/plain", b"other".to_vec()).unwrap(),
        ))
        .unwrap();

        assert!(matches!(
            block_on(store.get(&expired)),
            Err(ArtifactError::Expired(_))
        ));
        assert_eq!(block_on(store.list(&left, None, 1)).unwrap().items.len(), 1);
        assert_eq!(
            block_on(store.list(&right, None, 10)).unwrap().items,
            [isolated]
        );
        assert_eq!(
            block_on(store.purge_expired(&left, u64::MAX, 10)).unwrap(),
            1
        );
        assert!(block_on(store.delete(&left, &active.artifact_id)).unwrap());
        assert!(!block_on(store.delete(&left, &active.artifact_id)).unwrap());
    }
}
