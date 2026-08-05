//! `PostgreSQL` checkpoint persistence and atomic durable conversation commits.

use runifold_agent::{
    ConversationStoreError, ConversationStoreFuture, ConversationVersion,
    DurableConversationCommit, DurableConversationStore,
};
use runifold_core::{
    Checkpoint, CheckpointError, CheckpointErrorKind, CheckpointId, CheckpointStore,
};
use serde_json::Value;

use super::{
    PostgresConversationStore,
    support::{
        conflict_error, conversation_uuid, decode_version, encode_error, storage_error, to_i64,
        validate_append,
    },
};

impl CheckpointStore for PostgresConversationStore {
    fn load(&self, id: CheckpointId) -> Result<Checkpoint, CheckpointError> {
        let sql = format!(
            "SELECT record_json FROM {}_checkpoints WHERE checkpoint_id = $1",
            self.table
        );
        let row = self
            .blocking()
            .execute(move |client| client.query_opt(&sql, &[&id.as_uuid()]))
            .map_err(|_| checkpoint_worker())?
            .map_err(checkpoint_storage)?
            .ok_or_else(|| checkpoint_not_found(id))?;
        serde_json::from_value(row.get::<_, Value>("record_json")).map_err(|_| {
            CheckpointError::new(
                CheckpointErrorKind::InvalidPayload,
                "PostgreSQL checkpoint payload is invalid",
            )
        })
    }

    fn compare_and_swap(
        &self,
        checkpoint: &Checkpoint,
        expected_revision: Option<u64>,
    ) -> Result<(), CheckpointError> {
        let revision = checkpoint_i64(checkpoint.revision)?;
        let expected = expected_revision.map(checkpoint_i64).transpose()?;
        let record = serde_json::to_value(checkpoint).map_err(|_| {
            CheckpointError::new(
                CheckpointErrorKind::InvalidPayload,
                "checkpoint could not be encoded",
            )
        })?;
        let table = format!("{}_checkpoints", self.table);
        let checkpoint_id = checkpoint.id;
        self.blocking()
            .execute(move |client| -> Result<(), CheckpointError> {
                let mut transaction = client.transaction().map_err(checkpoint_storage)?;
                match expected {
                    None if revision == 0 => {
                        let changed = transaction
                            .execute(
                                &format!(
                                    "INSERT INTO {table} (checkpoint_id, revision, record_json) \
                                     VALUES ($1, $2, $3) \
                                     ON CONFLICT (checkpoint_id) DO NOTHING"
                                ),
                                &[&checkpoint_id.as_uuid(), &revision, &record],
                            )
                            .map_err(checkpoint_storage)?;
                        if changed != 1 {
                            return Err(checkpoint_conflict(checkpoint_id));
                        }
                    }
                    Some(expected)
                        if expected.checked_add(1).is_some_and(|next| revision == next) =>
                    {
                        let changed = transaction
                            .execute(
                                &format!(
                                    "UPDATE {table} SET revision = $1, record_json = $2 \
                                     WHERE checkpoint_id = $3 AND revision = $4"
                                ),
                                &[&revision, &record, &checkpoint_id.as_uuid(), &expected],
                            )
                            .map_err(checkpoint_storage)?;
                        if changed != 1 {
                            let exists = transaction
                                .query_opt(
                                    &format!("SELECT 1 FROM {table} WHERE checkpoint_id = $1"),
                                    &[&checkpoint_id.as_uuid()],
                                )
                                .map_err(checkpoint_storage)?
                                .is_some();
                            return Err(if exists {
                                checkpoint_conflict(checkpoint_id)
                            } else {
                                checkpoint_not_found(checkpoint_id)
                            });
                        }
                    }
                    _ => return Err(checkpoint_conflict(checkpoint_id)),
                }
                transaction.commit().map_err(checkpoint_storage)
            })
            .map_err(|_| checkpoint_worker())?
    }
}

impl DurableConversationStore for PostgresConversationStore {
    fn commit_durable_turn(
        &self,
        command: DurableConversationCommit,
    ) -> ConversationStoreFuture<'_, Result<ConversationVersion, ConversationStoreError>> {
        Box::pin(async move {
            validate_append(&command.append)?;
            let expected_checkpoint = to_i64(command.expected_checkpoint_revision)?;
            let checkpoint_revision = to_i64(command.checkpoint.revision)?;
            if expected_checkpoint
                .checked_add(1)
                .is_none_or(|next| next != checkpoint_revision)
            {
                return Err(conflict_error(
                    "durable conversation checkpoint is not the expected successor",
                ));
            }
            let messages = serde_json::to_value(&command.append.messages).map_err(encode_error)?;
            let checkpoint = serde_json::to_value(&command.checkpoint).map_err(encode_error)?;
            let mut client = self.transaction_client.lock().await;
            let transaction = client.transaction().await.map_err(storage_error)?;
            let conversation_sql = format!(
                "UPDATE {table} SET version = version + 1, updated_at = clock_timestamp() \
                 WHERE conversation_id = $1 AND namespace = $2 AND version = $3 \
                   AND version < 9223372036854775807 RETURNING version",
                table = self.table
            );
            let Some(version_row) = transaction
                .query_opt(
                    &conversation_sql,
                    &[
                        &conversation_uuid(command.append.conversation_id),
                        &command.namespace.as_str(),
                        &to_i64(command.append.expected_version.get())?,
                    ],
                )
                .await
                .map_err(storage_error)?
            else {
                return Err(conflict_error(
                    "durable conversation transcript version precondition failed",
                ));
            };
            let transcript_sql = format!(
                "WITH base AS (\
                     SELECT COALESCE(MAX(sequence), 0) AS last_sequence \
                     FROM {table}_transcript WHERE conversation_id = $1\
                 ) \
                 INSERT INTO {table}_transcript (conversation_id, sequence, message) \
                 SELECT $1, base.last_sequence + payload.ordinality, payload.message \
                 FROM base CROSS JOIN LATERAL \
                    jsonb_array_elements($2::JSONB) WITH ORDINALITY AS payload(message, ordinality)",
                table = self.table
            );
            let inserted = transaction
                .execute(
                    &transcript_sql,
                    &[
                        &conversation_uuid(command.append.conversation_id),
                        &messages,
                    ],
                )
                .await
                .map_err(storage_error)?;
            if inserted != u64::try_from(command.append.messages.len()).unwrap_or(u64::MAX) {
                return Err(conflict_error(
                    "durable conversation transcript append was incomplete",
                ));
            }
            let checkpoint_sql = format!(
                "UPDATE {table}_checkpoints SET revision = $1, record_json = $2 \
                 WHERE checkpoint_id = $3 AND revision = $4",
                table = self.table
            );
            let changed = transaction
                .execute(
                    &checkpoint_sql,
                    &[
                        &checkpoint_revision,
                        &checkpoint,
                        &command.checkpoint.id.as_uuid(),
                        &expected_checkpoint,
                    ],
                )
                .await
                .map_err(storage_error)?;
            if changed != 1 {
                return Err(conflict_error(
                    "durable conversation checkpoint precondition failed",
                ));
            }
            transaction.commit().await.map_err(storage_error)?;
            decode_version(version_row.get("version"))
        })
    }
}

fn checkpoint_i64(value: u64) -> Result<i64, CheckpointError> {
    i64::try_from(value).map_err(|_| {
        CheckpointError::new(
            CheckpointErrorKind::InvalidPayload,
            "checkpoint revision exceeds PostgreSQL BIGINT",
        )
    })
}

fn checkpoint_storage(_error: postgres::Error) -> CheckpointError {
    CheckpointError::new(
        CheckpointErrorKind::Storage,
        "PostgreSQL checkpoint operation failed",
    )
}

fn checkpoint_worker() -> CheckpointError {
    CheckpointError::new(
        CheckpointErrorKind::Storage,
        "PostgreSQL checkpoint worker is unavailable",
    )
}

fn checkpoint_conflict(id: CheckpointId) -> CheckpointError {
    CheckpointError::new(
        CheckpointErrorKind::Conflict,
        format!("checkpoint `{id}` revision precondition failed"),
    )
}

fn checkpoint_not_found(id: CheckpointId) -> CheckpointError {
    CheckpointError::new(
        CheckpointErrorKind::NotFound,
        format!("checkpoint `{id}` does not exist"),
    )
}
