use runifold_model::{
    Artifact, ArtifactError, ArtifactFuture, ArtifactPage, ArtifactRef, ArtifactScope,
    ArtifactStore, ArtifactWrite, DEFAULT_MAX_ARTIFACT_BYTES, MAX_ARTIFACT_PAGE_SIZE,
};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

use super::PostgresConversationStore;

impl ArtifactStore for PostgresConversationStore {
    fn put(&self, write: ArtifactWrite) -> ArtifactFuture<'_, Result<ArtifactRef, ArtifactError>> {
        Box::pin(async move {
            if write.bytes().len() > DEFAULT_MAX_ARTIFACT_BYTES {
                return Err(ArtifactError::InvalidInput(format!(
                    "artifact is {} bytes and exceeds the {}-byte limit",
                    write.bytes().len(),
                    DEFAULT_MAX_ARTIFACT_BYTES
                )));
            }
            let digest = sha256(write.bytes());
            let artifact_id = artifact_identity(write.media_type(), write.bytes());
            let size_bytes = i64::try_from(write.bytes().len()).map_err(|_| {
                ArtifactError::InvalidInput("artifact size exceeds PostgreSQL BIGINT".into())
            })?;
            let created_at_ms = i64_value(unix_time_ms()?, "creation time")?;
            let expires_at_ms = write
                .expires_at_unix_ms()
                .map(|value| i64_value(value, "expiration time"))
                .transpose()?;
            let artifacts = format!("{}_artifacts", self.table);
            let idempotency = format!("{}_artifact_idempotency", self.table);
            let mut client = self.transaction_client.lock().await;
            let transaction = client
                .transaction()
                .await
                .map_err(|error| storage(&error))?;
            if let Some(reference) =
                load_replay_reference(&transaction, &artifacts, &idempotency, &write, &artifact_id)
                    .await?
            {
                return Ok(reference);
            }
            transaction
                .execute(
                    &format!(
                        "INSERT INTO {artifacts}
                         (scope, artifact_id, media_type, size_bytes, sha256, name, bytes,
                          created_at_ms, expires_at_ms)
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                         ON CONFLICT (scope, artifact_id) DO NOTHING"
                    ),
                    &[
                        &write.scope().as_str(),
                        &artifact_id,
                        &write.media_type(),
                        &size_bytes,
                        &digest,
                        &write.name(),
                        &write.bytes(),
                        &created_at_ms,
                        &expires_at_ms,
                    ],
                )
                .await
                .map_err(|error| storage(&error))?;
            let reference =
                load_reference(&transaction, &artifacts, write.scope(), &artifact_id).await?;
            if !write.matches_immutable_reference(&reference) {
                return Err(ArtifactError::MetadataConflict(artifact_id));
            }
            transaction
                .execute(
                    &format!(
                        "INSERT INTO {idempotency} (scope, idempotency_key, artifact_id)
                         VALUES ($1, $2, $3)
                         ON CONFLICT (scope, idempotency_key) DO NOTHING"
                    ),
                    &[
                        &write.scope().as_str(),
                        &write.idempotency_key(),
                        &artifact_id,
                    ],
                )
                .await
                .map_err(|error| storage(&error))?;
            let selected = transaction
                .query_one(
                    &format!(
                        "SELECT artifact_id FROM {idempotency}
                         WHERE scope = $1 AND idempotency_key = $2"
                    ),
                    &[&write.scope().as_str(), &write.idempotency_key()],
                )
                .await
                .map_err(|error| storage(&error))?
                .get::<_, String>("artifact_id");
            if selected != artifact_id {
                return Err(ArtifactError::IdempotencyConflict(
                    write.idempotency_key().into(),
                ));
            }
            transaction
                .commit()
                .await
                .map_err(|error| storage(&error))?;
            Ok(reference)
        })
    }

    fn get(&self, reference: &ArtifactRef) -> ArtifactFuture<'_, Result<Artifact, ArtifactError>> {
        let reference = reference.clone();
        Box::pin(async move {
            let sql = format!(
                "SELECT media_type, size_bytes, sha256, name, bytes,
                        created_at_ms, expires_at_ms
                 FROM {}_artifacts WHERE scope = $1 AND artifact_id = $2",
                self.table
            );
            let row = self
                .client
                .query_opt(&sql, &[&reference.scope.as_str(), &reference.artifact_id])
                .await
                .map_err(|error| storage(&error))?
                .ok_or_else(|| ArtifactError::NotFound(reference.artifact_id.clone()))?;
            let size = decode_size(row.get("size_bytes"), &reference.artifact_id)?;
            let artifact = Artifact {
                reference: ArtifactRef {
                    scope: reference.scope.clone(),
                    artifact_id: reference.artifact_id.clone(),
                    media_type: row.get("media_type"),
                    size_bytes: size,
                    sha256: row.get("sha256"),
                    name: row.get("name"),
                    created_at_unix_ms: decode_time(
                        row.get("created_at_ms"),
                        &reference.artifact_id,
                    )?,
                    expires_at_unix_ms: row
                        .get::<_, Option<i64>>("expires_at_ms")
                        .map(|value| decode_time(value, &reference.artifact_id))
                        .transpose()?,
                },
                bytes: row.get("bytes"),
            };
            verify(&artifact)?;
            if artifact.reference != reference {
                return Err(ArtifactError::Integrity(reference.artifact_id));
            }
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
        let after = after.unwrap_or_default().to_owned();
        Box::pin(async move {
            validate_limit(limit)?;
            let sql = format!(
                "SELECT artifact_id, media_type, size_bytes, sha256, name,
                        created_at_ms, expires_at_ms
                 FROM {}_artifacts
                 WHERE scope = $1 AND artifact_id > $2
                 ORDER BY artifact_id LIMIT $3",
                self.table
            );
            let fetch = i64::from(limit) + 1;
            let rows = self
                .client
                .query(&sql, &[&scope.as_str(), &after, &fetch])
                .await
                .map_err(|error| storage(&error))?;
            let mut items = rows
                .iter()
                .map(|row| decode_reference(row, &scope))
                .collect::<Result<Vec<_>, _>>()?;
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
            let artifacts = format!("{}_artifacts", self.table);
            let idempotency = format!("{}_artifact_idempotency", self.table);
            let mut client = self.transaction_client.lock().await;
            let transaction = client
                .transaction()
                .await
                .map_err(|error| storage(&error))?;
            transaction
                .execute(
                    &format!("DELETE FROM {idempotency} WHERE scope = $1 AND artifact_id = $2"),
                    &[&scope.as_str(), &artifact_id],
                )
                .await
                .map_err(|error| storage(&error))?;
            let removed = transaction
                .execute(
                    &format!("DELETE FROM {artifacts} WHERE scope = $1 AND artifact_id = $2"),
                    &[&scope.as_str(), &artifact_id],
                )
                .await
                .map_err(|error| storage(&error))?
                > 0;
            transaction
                .commit()
                .await
                .map_err(|error| storage(&error))?;
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
            validate_limit(limit)?;
            let now = i64_value(now_unix_ms, "purge time")?;
            let artifacts = format!("{}_artifacts", self.table);
            let sql = format!(
                "DELETE FROM {artifacts} WHERE (scope, artifact_id) IN (
                    SELECT scope, artifact_id FROM {artifacts}
                    WHERE scope = $1 AND expires_at_ms IS NOT NULL AND expires_at_ms <= $2
                    ORDER BY expires_at_ms, artifact_id LIMIT $3
                 )"
            );
            let removed = self
                .client
                .execute(&sql, &[&scope.as_str(), &now, &i64::from(limit)])
                .await
                .map_err(|error| storage(&error))?;
            u32::try_from(removed)
                .map_err(|_| ArtifactError::Storage("purge count exceeds u32".into()))
        })
    }
}

async fn load_replay_reference(
    transaction: &tokio_postgres::Transaction<'_>,
    artifacts: &str,
    idempotency: &str,
    write: &ArtifactWrite,
    artifact_id: &str,
) -> Result<Option<ArtifactRef>, ArtifactError> {
    let existing = transaction
        .query_opt(
            &format!(
                "SELECT artifact_id FROM {idempotency}
                 WHERE scope = $1 AND idempotency_key = $2"
            ),
            &[&write.scope().as_str(), &write.idempotency_key()],
        )
        .await
        .map_err(|error| storage(&error))?;
    let Some(existing) = existing else {
        return Ok(None);
    };
    let existing = existing.get::<_, String>("artifact_id");
    if existing != artifact_id {
        return Err(ArtifactError::IdempotencyConflict(
            write.idempotency_key().into(),
        ));
    }
    let reference = load_reference(transaction, artifacts, write.scope(), &existing).await?;
    if !write.matches_immutable_reference(&reference) {
        return Err(ArtifactError::IdempotencyConflict(
            write.idempotency_key().into(),
        ));
    }
    Ok(Some(reference))
}

async fn load_reference(
    transaction: &tokio_postgres::Transaction<'_>,
    table: &str,
    scope: &ArtifactScope,
    artifact_id: &str,
) -> Result<ArtifactRef, ArtifactError> {
    let row = transaction
        .query_one(
            &format!(
                "SELECT artifact_id, media_type, size_bytes, sha256, name,
                        created_at_ms, expires_at_ms
                 FROM {table} WHERE scope = $1 AND artifact_id = $2"
            ),
            &[&scope.as_str(), &artifact_id],
        )
        .await
        .map_err(|error| storage(&error))?;
    decode_reference(&row, scope)
}

fn decode_reference(
    row: &tokio_postgres::Row,
    scope: &ArtifactScope,
) -> Result<ArtifactRef, ArtifactError> {
    let artifact_id: String = row.get("artifact_id");
    Ok(ArtifactRef {
        scope: scope.clone(),
        artifact_id: artifact_id.clone(),
        media_type: row.get("media_type"),
        size_bytes: decode_size(row.get("size_bytes"), &artifact_id)?,
        sha256: row.get("sha256"),
        name: row.get("name"),
        created_at_unix_ms: decode_time(row.get("created_at_ms"), &artifact_id)?,
        expires_at_unix_ms: row
            .get::<_, Option<i64>>("expires_at_ms")
            .map(|value| decode_time(value, &artifact_id))
            .transpose()?,
    })
}

fn verify(artifact: &Artifact) -> Result<(), ArtifactError> {
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

fn decode_size(value: i64, artifact_id: &str) -> Result<u64, ArtifactError> {
    u64::try_from(value).map_err(|_| ArtifactError::Integrity(artifact_id.into()))
}

fn decode_time(value: i64, artifact_id: &str) -> Result<u64, ArtifactError> {
    u64::try_from(value).map_err(|_| ArtifactError::Integrity(artifact_id.into()))
}

fn i64_value(value: u64, label: &str) -> Result<i64, ArtifactError> {
    i64::try_from(value)
        .map_err(|_| ArtifactError::InvalidInput(format!("artifact {label} exceeds i64")))
}

fn unix_time_ms() -> Result<u64, ArtifactError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ArtifactError::Storage(error.to_string()))?;
    u64::try_from(elapsed.as_millis())
        .map_err(|_| ArtifactError::Storage("system time exceeds u64 milliseconds".into()))
}

fn ensure_not_expired(reference: &ArtifactRef, now: u64) -> Result<(), ArtifactError> {
    if reference
        .expires_at_unix_ms
        .is_some_and(|expires| expires <= now)
    {
        return Err(ArtifactError::Expired(reference.artifact_id.clone()));
    }
    Ok(())
}

fn validate_limit(limit: u32) -> Result<(), ArtifactError> {
    if limit == 0 || limit > MAX_ARTIFACT_PAGE_SIZE {
        return Err(ArtifactError::InvalidInput(format!(
            "artifact page limit must be between 1 and {MAX_ARTIFACT_PAGE_SIZE}"
        )));
    }
    Ok(())
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

fn storage(error: &tokio_postgres::Error) -> ArtifactError {
    ArtifactError::Storage(error.to_string())
}
