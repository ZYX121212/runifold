use runifold_model::{
    Artifact, ArtifactError, ArtifactFuture, ArtifactPage, ArtifactRef, ArtifactScope,
    ArtifactStore, ArtifactWrite, DEFAULT_MAX_ARTIFACT_BYTES, MAX_ARTIFACT_PAGE_SIZE,
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

use super::SqliteStore;

impl ArtifactStore for SqliteStore {
    fn put(&self, write: ArtifactWrite) -> ArtifactFuture<'_, Result<ArtifactRef, ArtifactError>> {
        let store = self.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || store.put_artifact_blocking(&write))
                .await
                .map_err(|error| ArtifactError::Storage(error.to_string()))?
        })
    }

    fn get(&self, reference: &ArtifactRef) -> ArtifactFuture<'_, Result<Artifact, ArtifactError>> {
        let store = self.clone();
        let reference = reference.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || store.get_artifact_blocking(&reference))
                .await
                .map_err(|error| ArtifactError::Storage(error.to_string()))?
        })
    }

    fn list(
        &self,
        scope: &ArtifactScope,
        after: Option<&str>,
        limit: u32,
    ) -> ArtifactFuture<'_, Result<ArtifactPage, ArtifactError>> {
        let store = self.clone();
        let scope = scope.clone();
        let after = after.map(str::to_owned);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                store.list_artifacts_blocking(&scope, after.as_deref(), limit)
            })
            .await
            .map_err(|error| ArtifactError::Storage(error.to_string()))?
        })
    }

    fn delete(
        &self,
        scope: &ArtifactScope,
        artifact_id: &str,
    ) -> ArtifactFuture<'_, Result<bool, ArtifactError>> {
        let store = self.clone();
        let scope = scope.clone();
        let artifact_id = artifact_id.to_owned();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                store.delete_artifact_blocking(&scope, &artifact_id)
            })
            .await
            .map_err(|error| ArtifactError::Storage(error.to_string()))?
        })
    }

    fn purge_expired(
        &self,
        scope: &ArtifactScope,
        now_unix_ms: u64,
        limit: u32,
    ) -> ArtifactFuture<'_, Result<u32, ArtifactError>> {
        let store = self.clone();
        let scope = scope.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                store.purge_artifacts_blocking(&scope, now_unix_ms, limit)
            })
            .await
            .map_err(|error| ArtifactError::Storage(error.to_string()))?
        })
    }
}

impl SqliteStore {
    fn put_artifact_blocking(&self, write: &ArtifactWrite) -> Result<ArtifactRef, ArtifactError> {
        if write.bytes().len() > DEFAULT_MAX_ARTIFACT_BYTES {
            return Err(ArtifactError::InvalidInput(format!(
                "artifact is {} bytes and exceeds the {}-byte limit",
                write.bytes().len(),
                DEFAULT_MAX_ARTIFACT_BYTES
            )));
        }
        let digest = sha256(write.bytes());
        let artifact_id = artifact_identity(write.media_type(), write.bytes());
        let size_bytes = i64::try_from(write.bytes().len())
            .map_err(|_| ArtifactError::InvalidInput("artifact size exceeds SQLite i64".into()))?;
        let created_at_ms = i64_value(unix_time_ms()?, "creation time")?;
        let expires_at_ms = write
            .expires_at_unix_ms()
            .map(|value| i64_value(value, "expiration time"))
            .transpose()?;
        let mut connection = self.lock();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| storage(&error))?;
        let existing = transaction
            .query_row(
                "SELECT artifact_id FROM runifold_artifact_idempotency
                 WHERE scope = ?1 AND idempotency_key = ?2",
                params![write.scope().as_str(), write.idempotency_key()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| storage(&error))?;
        if let Some(existing) = existing {
            if existing != artifact_id {
                return Err(ArtifactError::IdempotencyConflict(
                    write.idempotency_key().into(),
                ));
            }
            let reference = load_reference(&transaction, write.scope(), &existing)?;
            if !write.matches_immutable_reference(&reference) {
                return Err(ArtifactError::IdempotencyConflict(
                    write.idempotency_key().into(),
                ));
            }
            return Ok(reference);
        }
        transaction
            .execute(
                "INSERT OR IGNORE INTO runifold_artifacts
                 (scope, artifact_id, media_type, size_bytes, sha256, name, bytes,
                  created_at_ms, expires_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    write.scope().as_str(),
                    artifact_id,
                    write.media_type(),
                    size_bytes,
                    digest,
                    write.name(),
                    write.bytes(),
                    created_at_ms,
                    expires_at_ms
                ],
            )
            .map_err(|error| storage(&error))?;
        let reference = load_reference(&transaction, write.scope(), &artifact_id)?;
        if !write.matches_immutable_reference(&reference) {
            return Err(ArtifactError::MetadataConflict(artifact_id));
        }
        transaction
            .execute(
                "INSERT INTO runifold_artifact_idempotency (scope, idempotency_key, artifact_id)
                 VALUES (?1, ?2, ?3)",
                params![write.scope().as_str(), write.idempotency_key(), artifact_id],
            )
            .map_err(|error| storage(&error))?;
        transaction.commit().map_err(|error| storage(&error))?;
        Ok(reference)
    }

    fn get_artifact_blocking(&self, expected: &ArtifactRef) -> Result<Artifact, ArtifactError> {
        let connection = self.lock();
        let artifact = connection
            .query_row(
                "SELECT media_type, size_bytes, sha256, name, bytes,
                        created_at_ms, expires_at_ms
                 FROM runifold_artifacts WHERE scope = ?1 AND artifact_id = ?2",
                params![expected.scope.as_str(), expected.artifact_id],
                |row| {
                    Ok(Artifact {
                        reference: ArtifactRef {
                            scope: expected.scope.clone(),
                            artifact_id: expected.artifact_id.clone(),
                            media_type: row.get(0)?,
                            size_bytes: u64::try_from(row.get::<_, i64>(1)?).map_err(|error| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    1,
                                    rusqlite::types::Type::Integer,
                                    Box::new(error),
                                )
                            })?,
                            sha256: row.get(2)?,
                            name: row.get(3)?,
                            created_at_unix_ms: unsigned_i64(row.get(5)?, 5)?,
                            expires_at_unix_ms: row
                                .get::<_, Option<i64>>(6)?
                                .map(|value| unsigned_i64(value, 6))
                                .transpose()?,
                        },
                        bytes: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(|error| storage(&error))?
            .ok_or_else(|| ArtifactError::NotFound(expected.artifact_id.clone()))?;
        if artifact.reference.size_bytes != artifact.bytes.len() as u64
            || artifact.reference.sha256 != sha256(&artifact.bytes)
            || artifact.reference.artifact_id
                != artifact_identity(&artifact.reference.media_type, &artifact.bytes)
        {
            return Err(ArtifactError::Integrity(expected.artifact_id.clone()));
        }
        if artifact.reference != *expected {
            return Err(ArtifactError::Integrity(expected.artifact_id.clone()));
        }
        ensure_not_expired(&artifact.reference, unix_time_ms()?)?;
        Ok(artifact)
    }

    fn list_artifacts_blocking(
        &self,
        scope: &ArtifactScope,
        after: Option<&str>,
        limit: u32,
    ) -> Result<ArtifactPage, ArtifactError> {
        validate_limit(limit)?;
        let fetch = i64::from(limit) + 1;
        let connection = self.lock();
        let mut statement = connection
            .prepare(
                "SELECT artifact_id, media_type, size_bytes, sha256, name,
                        created_at_ms, expires_at_ms
                 FROM runifold_artifacts
                 WHERE scope = ?1 AND artifact_id > ?2
                 ORDER BY artifact_id LIMIT ?3",
            )
            .map_err(|error| storage(&error))?;
        let rows = statement
            .query_map(params![scope.as_str(), after.unwrap_or(""), fetch], |row| {
                Ok(ArtifactRef {
                    scope: scope.clone(),
                    artifact_id: row.get(0)?,
                    media_type: row.get(1)?,
                    size_bytes: unsigned_i64(row.get(2)?, 2)?,
                    sha256: row.get(3)?,
                    name: row.get(4)?,
                    created_at_unix_ms: unsigned_i64(row.get(5)?, 5)?,
                    expires_at_unix_ms: row
                        .get::<_, Option<i64>>(6)?
                        .map(|value| unsigned_i64(value, 6))
                        .transpose()?,
                })
            })
            .map_err(|error| storage(&error))?;
        let mut items = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| storage(&error))?;
        let next_cursor = if items.len() > limit as usize {
            items.pop();
            items.last().map(|item| item.artifact_id.clone())
        } else {
            None
        };
        Ok(ArtifactPage { items, next_cursor })
    }

    fn delete_artifact_blocking(
        &self,
        scope: &ArtifactScope,
        artifact_id: &str,
    ) -> Result<bool, ArtifactError> {
        let mut connection = self.lock();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| storage(&error))?;
        transaction
            .execute(
                "DELETE FROM runifold_artifact_idempotency
                 WHERE scope = ?1 AND artifact_id = ?2",
                params![scope.as_str(), artifact_id],
            )
            .map_err(|error| storage(&error))?;
        let removed = transaction
            .execute(
                "DELETE FROM runifold_artifacts WHERE scope = ?1 AND artifact_id = ?2",
                params![scope.as_str(), artifact_id],
            )
            .map_err(|error| storage(&error))?
            > 0;
        transaction.commit().map_err(|error| storage(&error))?;
        Ok(removed)
    }

    fn purge_artifacts_blocking(
        &self,
        scope: &ArtifactScope,
        now_unix_ms: u64,
        limit: u32,
    ) -> Result<u32, ArtifactError> {
        validate_limit(limit)?;
        let now = i64_value(now_unix_ms, "purge time")?;
        let mut connection = self.lock();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| storage(&error))?;
        let ids = {
            let mut statement = transaction
                .prepare(
                    "SELECT artifact_id FROM runifold_artifacts
                     WHERE scope = ?1 AND expires_at_ms IS NOT NULL AND expires_at_ms <= ?2
                     ORDER BY expires_at_ms, artifact_id LIMIT ?3",
                )
                .map_err(|error| storage(&error))?;
            statement
                .query_map(params![scope.as_str(), now, i64::from(limit)], |row| {
                    row.get::<_, String>(0)
                })
                .map_err(|error| storage(&error))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| storage(&error))?
        };
        for id in &ids {
            transaction
                .execute(
                    "DELETE FROM runifold_artifacts WHERE scope = ?1 AND artifact_id = ?2",
                    params![scope.as_str(), id],
                )
                .map_err(|error| storage(&error))?;
        }
        transaction.commit().map_err(|error| storage(&error))?;
        u32::try_from(ids.len())
            .map_err(|_| ArtifactError::Storage("purge count exceeds u32".into()))
    }
}

fn load_reference(
    transaction: &rusqlite::Transaction<'_>,
    scope: &ArtifactScope,
    artifact_id: &str,
) -> Result<ArtifactRef, ArtifactError> {
    transaction
        .query_row(
            "SELECT media_type, size_bytes, sha256, name, created_at_ms, expires_at_ms
             FROM runifold_artifacts WHERE scope = ?1 AND artifact_id = ?2",
            params![scope.as_str(), artifact_id],
            |row| {
                Ok(ArtifactRef {
                    scope: scope.clone(),
                    artifact_id: artifact_id.into(),
                    media_type: row.get(0)?,
                    size_bytes: u64::try_from(row.get::<_, i64>(1)?).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Integer,
                            Box::new(error),
                        )
                    })?,
                    sha256: row.get(2)?,
                    name: row.get(3)?,
                    created_at_unix_ms: unsigned_i64(row.get(4)?, 4)?,
                    expires_at_unix_ms: row
                        .get::<_, Option<i64>>(5)?
                        .map(|value| unsigned_i64(value, 5))
                        .transpose()?,
                })
            },
        )
        .map_err(|error| storage(&error))
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

fn storage(error: &rusqlite::Error) -> ArtifactError {
    ArtifactError::Storage(error.to_string())
}

fn unsigned_i64(value: i64, column: usize) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            column,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sqlite_artifacts_survive_idempotent_replay() {
        let store = SqliteStore::open_in_memory().unwrap();
        let scope = ArtifactScope::parse("tenant.test").unwrap();
        let other_scope = ArtifactScope::parse("tenant.other").unwrap();
        let png = b"\x89PNG\r\n\x1a\npng";
        let write =
            ArtifactWrite::new(scope.clone(), "turn:image", "image/png", png.to_vec()).unwrap();
        let first = store.put(write.clone()).await.unwrap();
        assert_eq!(store.put(write).await.unwrap(), first);
        assert_eq!(store.get(&first).await.unwrap().bytes, png);
        let changed_replay =
            ArtifactWrite::new(scope.clone(), "turn:image", "image/png", png.to_vec())
                .unwrap()
                .with_expires_at_unix_ms(i64::MAX as u64)
                .unwrap();
        assert!(matches!(
            store.put(changed_replay).await,
            Err(ArtifactError::IdempotencyConflict(_))
        ));
        let changed_alias =
            ArtifactWrite::new(scope.clone(), "turn:alias", "image/png", png.to_vec())
                .unwrap()
                .with_name("different")
                .unwrap();
        assert!(matches!(
            store.put(changed_alias).await,
            Err(ArtifactError::MetadataConflict(_))
        ));
        let isolated = store
            .put(
                ArtifactWrite::new(other_scope.clone(), "turn:image", "image/png", png.to_vec())
                    .unwrap(),
            )
            .await
            .unwrap();
        let expired = store
            .put(
                ArtifactWrite::new(
                    scope.clone(),
                    "turn:expired",
                    "text/plain",
                    b"expired".to_vec(),
                )
                .unwrap()
                .with_expires_at_unix_ms(1)
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(matches!(
            store.get(&expired).await,
            Err(ArtifactError::Expired(_))
        ));
        assert_eq!(
            store
                .purge_expired(&scope, i64::MAX as u64, 10)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            store.list(&scope, None, 10).await.unwrap().items.as_slice(),
            std::slice::from_ref(&first)
        );
        assert!(store.delete(&scope, &first.artifact_id).await.unwrap());
        assert!(!store.delete(&scope, &first.artifact_id).await.unwrap());
        assert_eq!(store.get(&isolated).await.unwrap().bytes, png);
    }
}
