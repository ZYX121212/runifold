//! `PostgreSQL` write-ahead external-effect persistence.

use postgres::error::SqlState;
use runifold_core::{CapabilityId, EffectId};
use runifold_effect::{EffectExecutorError, EffectExecutorErrorKind, EffectRecord, EffectStore};
use serde_json::Value;

use crate::PostgresConversationStore;

impl EffectStore for PostgresConversationStore {
    fn load(&self, id: EffectId) -> Result<Option<EffectRecord>, EffectExecutorError> {
        let sql = format!(
            "SELECT record_json FROM {}_effects WHERE effect_id = $1",
            self.table()
        );
        self.blocking()
            .execute(move |client| client.query_opt(&sql, &[&id.as_uuid()]))
            .map_err(|_| effect_worker())?
            .map_err(effect_storage)?
            .map(|row| decode_record(row.get("record_json")))
            .transpose()
    }

    fn find_by_idempotency(
        &self,
        capability_id: CapabilityId,
        key: &str,
    ) -> Result<Option<EffectRecord>, EffectExecutorError> {
        let sql = format!(
            "SELECT record_json FROM {}_effects \
             WHERE capability_id = $1 AND idempotency_key = $2",
            self.table()
        );
        let key = key.to_owned();
        self.blocking()
            .execute(move |client| client.query_opt(&sql, &[&capability_id.as_uuid(), &key]))
            .map_err(|_| effect_worker())?
            .map_err(effect_storage)?
            .map(|row| decode_record(row.get("record_json")))
            .transpose()
    }

    fn compare_and_swap(
        &self,
        record: &EffectRecord,
        expected_revision: Option<u64>,
    ) -> Result<(), EffectExecutorError> {
        let revision = effect_i64(record.revision)?;
        let expected = expected_revision.map(effect_i64).transpose()?;
        let encoded = serde_json::to_value(record).map_err(|_| effect_protocol())?;
        let effect_id = record.request.effect_id.as_uuid();
        let capability_id = record.request.capability_id.as_uuid();
        let idempotency_key = record.request.idempotency_key.clone();
        let table = format!("{}_effects", self.table());
        self.blocking()
            .execute(move |client| -> Result<(), EffectExecutorError> {
                let mut transaction = client.transaction().map_err(effect_storage)?;
                if let Some(key) = idempotency_key.as_deref() {
                    let owner = transaction
                        .query_opt(
                            &format!(
                                "SELECT effect_id FROM {table} \
                                 WHERE capability_id = $1 AND idempotency_key = $2"
                            ),
                            &[&capability_id, &key],
                        )
                        .map_err(effect_storage)?
                        .map(|row| row.get::<_, uuid::Uuid>("effect_id"));
                    if owner.is_some_and(|owner| owner != effect_id) {
                        return Err(effect_idempotency_conflict());
                    }
                }

                match expected {
                    None if revision == 0 => {
                        let changed = transaction
                            .execute(
                                &format!(
                                    "INSERT INTO {table} \
                                     (effect_id, capability_id, idempotency_key, revision, record_json) \
                                     VALUES ($1, $2, $3, $4, $5) \
                                     ON CONFLICT (effect_id) DO NOTHING"
                                ),
                                &[
                                    &effect_id,
                                    &capability_id,
                                    &idempotency_key,
                                    &revision,
                                    &encoded,
                                ],
                            )
                            .map_err(effect_write_error)?;
                        if changed != 1 {
                            return Err(effect_conflict());
                        }
                    }
                    Some(expected)
                        if expected
                            .checked_add(1)
                            .is_some_and(|next| revision == next) =>
                    {
                        let changed = transaction
                            .execute(
                                &format!(
                                    "UPDATE {table} SET capability_id = $1, idempotency_key = $2, \
                                        revision = $3, record_json = $4 \
                                     WHERE effect_id = $5 AND revision = $6"
                                ),
                                &[
                                    &capability_id,
                                    &idempotency_key,
                                    &revision,
                                    &encoded,
                                    &effect_id,
                                    &expected,
                                ],
                            )
                            .map_err(effect_write_error)?;
                        if changed != 1 {
                            return Err(effect_conflict());
                        }
                    }
                    _ => return Err(effect_conflict()),
                }
                transaction.commit().map_err(effect_storage)
            })
            .map_err(|_| effect_worker())?
    }
}

fn decode_record(value: Value) -> Result<EffectRecord, EffectExecutorError> {
    serde_json::from_value(value).map_err(|_| effect_protocol())
}

fn effect_i64(value: u64) -> Result<i64, EffectExecutorError> {
    i64::try_from(value).map_err(|_| {
        EffectExecutorError::new(
            EffectExecutorErrorKind::Protocol,
            "effect revision exceeds PostgreSQL BIGINT",
        )
    })
}

fn effect_storage(_error: postgres::Error) -> EffectExecutorError {
    EffectExecutorError::new(
        EffectExecutorErrorKind::Store,
        "PostgreSQL effect store operation failed",
    )
}

fn effect_worker() -> EffectExecutorError {
    EffectExecutorError::new(
        EffectExecutorErrorKind::Store,
        "PostgreSQL effect store worker is unavailable",
    )
}

fn effect_write_error(error: postgres::Error) -> EffectExecutorError {
    if error.code() == Some(&SqlState::UNIQUE_VIOLATION) {
        effect_idempotency_conflict()
    } else {
        effect_storage(error)
    }
}

fn effect_protocol() -> EffectExecutorError {
    EffectExecutorError::new(
        EffectExecutorErrorKind::Protocol,
        "PostgreSQL effect record is invalid",
    )
}

fn effect_conflict() -> EffectExecutorError {
    EffectExecutorError::new(
        EffectExecutorErrorKind::Store,
        "effect record revision precondition failed",
    )
}

fn effect_idempotency_conflict() -> EffectExecutorError {
    EffectExecutorError::new(
        EffectExecutorErrorKind::IdempotencyConflict,
        "idempotency key already belongs to another effect",
    )
}
