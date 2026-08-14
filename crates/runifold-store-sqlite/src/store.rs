use std::{
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use runifold_core::{
    CapabilityId, Checkpoint, CheckpointError, CheckpointErrorKind, CheckpointId, CheckpointStore,
    EffectId, Journal, JournalError, RunEvent, RunId,
};
use runifold_effect::{EffectExecutorError, EffectExecutorErrorKind, EffectRecord, EffectStore};
use runifold_ops::{
    RunEventCursor, RunEventPage, RunEventPageSize, RunEventSource, RunEventSourceError,
};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use thiserror::Error;

mod artifact;
mod conversation;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS runifold_checkpoints (
    checkpoint_id TEXT PRIMARY KEY NOT NULL,
    revision      INTEGER NOT NULL CHECK (revision >= 0),
    record_json   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runifold_effects (
    effect_id       TEXT PRIMARY KEY NOT NULL,
    capability_id   TEXT NOT NULL,
    idempotency_key TEXT,
    revision        INTEGER NOT NULL CHECK (revision >= 0),
    record_json     TEXT NOT NULL,
    UNIQUE (capability_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS runifold_effects_capability_key
    ON runifold_effects (capability_id, idempotency_key);

CREATE TABLE IF NOT EXISTS runifold_events (
    event_id   TEXT PRIMARY KEY NOT NULL,
    run_id     TEXT NOT NULL,
    sequence   INTEGER NOT NULL CHECK (sequence >= 0),
    event_json TEXT NOT NULL,
    UNIQUE (run_id, sequence)
);

CREATE INDEX IF NOT EXISTS runifold_events_run_sequence
    ON runifold_events (run_id, sequence);

CREATE TABLE IF NOT EXISTS runifold_conversation_state (
    singleton_id   INTEGER PRIMARY KEY NOT NULL CHECK (singleton_id = 1),
    format_version INTEGER NOT NULL,
    state_blob     BLOB NOT NULL,
    updated_at_ms  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS runifold_artifacts (
    scope       TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    media_type  TEXT NOT NULL,
    size_bytes  INTEGER NOT NULL CHECK (size_bytes >= 0),
    sha256      TEXT NOT NULL,
    name        TEXT,
    bytes       BLOB NOT NULL,
    created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
    expires_at_ms INTEGER CHECK (expires_at_ms >= 0),
    PRIMARY KEY (scope, artifact_id)
);
CREATE INDEX IF NOT EXISTS runifold_artifacts_scope_expiry
    ON runifold_artifacts (scope, expires_at_ms, artifact_id);

CREATE TABLE IF NOT EXISTS runifold_artifact_idempotency (
    scope           TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    artifact_id     TEXT NOT NULL,
    PRIMARY KEY (scope, idempotency_key),
    FOREIGN KEY (scope, artifact_id) REFERENCES runifold_artifacts(scope, artifact_id)
        ON DELETE CASCADE
);

PRAGMA user_version = 2;
";

/// Failure while opening, initializing, or directly querying a `SQLite` store.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SqliteStoreError {
    /// `SQLite` rejected an operation.
    #[error("sqlite operation failed: {0}")]
    Database(#[from] rusqlite::Error),
    /// Persisted JSON did not satisfy its canonical Rust representation.
    #[error("sqlite JSON decoding failed: {0}")]
    Json(#[from] serde_json::Error),
}

/// Cloneable SQLite-backed effect, checkpoint, and journal store.
///
/// Clones share one connection and serialize operations at the connection
/// boundary. `SQLite` transactions still provide persistence and CAS guarantees
/// across separately opened store instances and processes.
#[derive(Clone)]
pub struct SqliteStore {
    connection: Arc<Mutex<Connection>>,
}

impl SqliteStore {
    /// Opens or creates a file-backed store and initializes its schema.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteStoreError`] when the database cannot be opened or
    /// initialized.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SqliteStoreError> {
        Self::from_connection(Connection::open(path)?)
    }

    /// Opens an existing database without creating schema or permitting writes.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteStoreError`] when the database cannot be opened.
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self, SqliteStoreError> {
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// Creates a process-local `SQLite` database, primarily for tests.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteStoreError`] when `SQLite` initialization fails.
    pub fn open_in_memory() -> Result<Self, SqliteStoreError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, SqliteStoreError> {
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", true)?;
        connection.execute_batch(SCHEMA)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// Loads all journal events for one Run in sequence order.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteStoreError`] when querying or decoding fails.
    pub fn events(&self, run_id: RunId) -> Result<Vec<RunEvent>, SqliteStoreError> {
        let connection = self.lock();
        let mut statement = connection.prepare(
            "SELECT event_json
             FROM runifold_events
             WHERE run_id = ?1
             ORDER BY sequence ASC",
        )?;
        let rows = statement.query_map([run_id.to_string()], |row| row.get::<_, String>(0))?;
        decode_rows(rows)
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        self.connection
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl std::fmt::Debug for SqliteStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqliteStore")
            .finish_non_exhaustive()
    }
}

impl CheckpointStore for SqliteStore {
    fn load(&self, id: CheckpointId) -> Result<Checkpoint, CheckpointError> {
        let connection = self.lock();
        let record = connection
            .query_row(
                "SELECT record_json
                 FROM runifold_checkpoints
                 WHERE checkpoint_id = ?1",
                [id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| checkpoint_storage(&error))?;
        let record = record.ok_or_else(|| {
            CheckpointError::new(
                CheckpointErrorKind::NotFound,
                format!("checkpoint `{id}` does not exist"),
            )
        })?;
        serde_json::from_str(&record).map_err(|error| {
            CheckpointError::new(CheckpointErrorKind::InvalidPayload, error.to_string())
        })
    }

    fn compare_and_swap(
        &self,
        checkpoint: &Checkpoint,
        expected_revision: Option<u64>,
    ) -> Result<(), CheckpointError> {
        let revision = sqlite_revision(checkpoint.revision).map_err(checkpoint_invalid)?;
        let expected = expected_revision
            .map(sqlite_revision)
            .transpose()
            .map_err(checkpoint_invalid)?;
        let record = serde_json::to_string(checkpoint).map_err(|error| {
            CheckpointError::new(CheckpointErrorKind::InvalidPayload, error.to_string())
        })?;
        let mut connection = self.lock();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| checkpoint_storage(&error))?;
        let current = current_revision(
            &transaction,
            "runifold_checkpoints",
            "checkpoint_id",
            &checkpoint.id.to_string(),
        )
        .map_err(|error| checkpoint_storage(&error))?;

        match (current, expected) {
            (None, None) if revision == 0 => {
                transaction
                    .execute(
                        "INSERT INTO runifold_checkpoints
                         (checkpoint_id, revision, record_json)
                         VALUES (?1, ?2, ?3)",
                        params![checkpoint.id.to_string(), revision, record],
                    )
                    .map_err(|error| checkpoint_storage(&error))?;
            }
            (Some(current), Some(expected))
                if current == expected
                    && expected.checked_add(1).is_some_and(|next| revision == next) =>
            {
                let changed = transaction
                    .execute(
                        "UPDATE runifold_checkpoints
                         SET revision = ?1, record_json = ?2
                         WHERE checkpoint_id = ?3 AND revision = ?4",
                        params![revision, record, checkpoint.id.to_string(), expected],
                    )
                    .map_err(|error| checkpoint_storage(&error))?;
                if changed != 1 {
                    return Err(checkpoint_conflict(checkpoint.id));
                }
            }
            (None, Some(_)) => {
                return Err(CheckpointError::new(
                    CheckpointErrorKind::NotFound,
                    format!("checkpoint `{}` does not exist", checkpoint.id),
                ));
            }
            _ => return Err(checkpoint_conflict(checkpoint.id)),
        }
        transaction
            .commit()
            .map_err(|error| checkpoint_storage(&error))
    }
}

impl EffectStore for SqliteStore {
    fn load(&self, id: EffectId) -> Result<Option<EffectRecord>, EffectExecutorError> {
        let connection = self.lock();
        let record = connection
            .query_row(
                "SELECT record_json FROM runifold_effects WHERE effect_id = ?1",
                [id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| effect_storage(&error))?;
        record
            .map(|record| serde_json::from_str(&record).map_err(|error| effect_protocol(&error)))
            .transpose()
    }

    fn find_by_idempotency(
        &self,
        capability_id: CapabilityId,
        key: &str,
    ) -> Result<Option<EffectRecord>, EffectExecutorError> {
        let connection = self.lock();
        let record = connection
            .query_row(
                "SELECT record_json
                 FROM runifold_effects
                 WHERE capability_id = ?1 AND idempotency_key = ?2",
                params![capability_id.to_string(), key],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|error| effect_storage(&error))?;
        record
            .map(|record| serde_json::from_str(&record).map_err(|error| effect_protocol(&error)))
            .transpose()
    }

    fn compare_and_swap(
        &self,
        record: &EffectRecord,
        expected_revision: Option<u64>,
    ) -> Result<(), EffectExecutorError> {
        let revision = sqlite_revision(record.revision).map_err(effect_store_message)?;
        let expected = expected_revision
            .map(sqlite_revision)
            .transpose()
            .map_err(effect_store_message)?;
        let json = serde_json::to_string(record).map_err(|error| effect_protocol(&error))?;
        let effect_id = record.request.effect_id.to_string();
        let capability_id = record.request.capability_id.to_string();
        let idempotency_key = record.request.idempotency_key.as_deref();
        let mut connection = self.lock();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| effect_storage(&error))?;
        let current = current_revision(&transaction, "runifold_effects", "effect_id", &effect_id)
            .map_err(|error| effect_storage(&error))?;

        if let Some(key) = idempotency_key {
            let owner = transaction
                .query_row(
                    "SELECT effect_id
                     FROM runifold_effects
                     WHERE capability_id = ?1 AND idempotency_key = ?2",
                    params![capability_id, key],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(|error| effect_storage(&error))?;
            if owner.is_some_and(|owner| owner != effect_id) {
                return Err(EffectExecutorError::new(
                    EffectExecutorErrorKind::IdempotencyConflict,
                    "idempotency key already belongs to another effect",
                ));
            }
        }

        match (current, expected) {
            (None, None) if revision == 0 => {
                transaction
                    .execute(
                        "INSERT INTO runifold_effects
                         (effect_id, capability_id, idempotency_key, revision, record_json)
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![effect_id, capability_id, idempotency_key, revision, json],
                    )
                    .map_err(|error| effect_storage(&error))?;
            }
            (Some(current), Some(expected))
                if current == expected
                    && expected.checked_add(1).is_some_and(|next| revision == next) =>
            {
                let changed = transaction
                    .execute(
                        "UPDATE runifold_effects
                         SET capability_id = ?1, idempotency_key = ?2,
                             revision = ?3, record_json = ?4
                         WHERE effect_id = ?5 AND revision = ?6",
                        params![
                            capability_id,
                            idempotency_key,
                            revision,
                            json,
                            effect_id,
                            expected
                        ],
                    )
                    .map_err(|error| effect_storage(&error))?;
                if changed != 1 {
                    return Err(effect_conflict());
                }
            }
            _ => return Err(effect_conflict()),
        }
        transaction.commit().map_err(|error| effect_storage(&error))
    }
}

impl Journal for SqliteStore {
    fn record(&self, event: &RunEvent) -> Result<(), JournalError> {
        let sequence =
            sqlite_revision(event.meta.sequence).map_err(|error| journal_message(&error))?;
        let json = serde_json::to_string(event).map_err(|error| journal_message(&error))?;
        self.lock()
            .execute(
                "INSERT INTO runifold_events
                 (event_id, run_id, sequence, event_json)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    event.meta.event_id.to_string(),
                    event.meta.run_id.to_string(),
                    sequence,
                    json
                ],
            )
            .map_err(|error| journal_message(&error))?;
        Ok(())
    }
}

impl RunEventSource for SqliteStore {
    fn event_page(
        &self,
        run_id: RunId,
        after: Option<RunEventCursor>,
        limit: RunEventPageSize,
    ) -> Result<RunEventPage, RunEventSourceError> {
        let after = after.map_or(-1, |cursor| {
            i64::try_from(cursor.sequence()).unwrap_or(i64::MAX)
        });
        let query_limit = limit
            .get()
            .checked_add(1)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or_else(|| RunEventSourceError::storage("event page limit overflow"))?;
        let connection = self.lock();
        let mut statement = connection
            .prepare(
                "SELECT sequence, event_json
                 FROM runifold_events
                 WHERE run_id = ?1 AND sequence > ?2
                 ORDER BY sequence ASC
                 LIMIT ?3",
            )
            .map_err(|error| RunEventSourceError::storage(error.to_string()))?;
        let rows = statement
            .query_map(params![run_id.to_string(), after, query_limit], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|error| RunEventSourceError::storage(error.to_string()))?;
        let mut events = rows
            .map(|row| {
                let (stored_sequence, json) =
                    row.map_err(|error| RunEventSourceError::storage(error.to_string()))?;
                let event: RunEvent = serde_json::from_str(&json)
                    .map_err(|error| RunEventSourceError::corrupt_data(error.to_string()))?;
                if event.meta.run_id != run_id
                    || i64::try_from(event.meta.sequence).ok() != Some(stored_sequence)
                {
                    return Err(RunEventSourceError::corrupt_data(
                        "event index does not match its canonical envelope",
                    ));
                }
                Ok(event)
            })
            .collect::<Result<Vec<RunEvent>, RunEventSourceError>>()?;
        let has_more = events.len() > limit.get();
        if has_more {
            events.truncate(limit.get());
        }
        let next = has_more
            .then(|| {
                events
                    .last()
                    .map(|event| RunEventCursor::after(event.meta.sequence))
            })
            .flatten();
        Ok(RunEventPage { events, next })
    }
}

fn current_revision(
    transaction: &Transaction<'_>,
    table: &str,
    id_column: &str,
    id: &str,
) -> rusqlite::Result<Option<i64>> {
    let sql = format!("SELECT revision FROM {table} WHERE {id_column} = ?1");
    transaction
        .query_row(&sql, [id], |row| row.get(0))
        .optional()
}

fn sqlite_revision(value: u64) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| "revision exceeds SQLite integer range".into())
}

fn checkpoint_invalid(message: String) -> CheckpointError {
    CheckpointError::new(CheckpointErrorKind::InvalidPayload, message)
}

fn checkpoint_storage(error: &rusqlite::Error) -> CheckpointError {
    CheckpointError::new(CheckpointErrorKind::Storage, error.to_string())
}

fn checkpoint_conflict(id: CheckpointId) -> CheckpointError {
    CheckpointError::new(
        CheckpointErrorKind::Conflict,
        format!("checkpoint `{id}` revision precondition failed"),
    )
}

fn effect_storage(error: &rusqlite::Error) -> EffectExecutorError {
    effect_store_message(error.to_string())
}

fn effect_store_message(message: String) -> EffectExecutorError {
    EffectExecutorError::new(EffectExecutorErrorKind::Store, message)
}

fn effect_protocol(error: &serde_json::Error) -> EffectExecutorError {
    EffectExecutorError::new(EffectExecutorErrorKind::Protocol, error.to_string())
}

fn effect_conflict() -> EffectExecutorError {
    EffectExecutorError::new(
        EffectExecutorErrorKind::Store,
        "effect record revision precondition failed",
    )
}

fn journal_message(error: &impl ToString) -> JournalError {
    JournalError {
        message: error.to_string(),
    }
}

fn decode_rows(
    rows: impl Iterator<Item = rusqlite::Result<String>>,
) -> Result<Vec<RunEvent>, SqliteStoreError> {
    rows.map(|row| {
        let json = row?;
        Ok(serde_json::from_str(&json)?)
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use runifold_core::{
        CapabilityId, Checkpoint, CheckpointErrorKind, CheckpointId, CheckpointStore, DomainEvent,
        EffectClass, EffectId, EffectKind, EffectRequest, EventFactory, InvocationId, Journal,
        LifecycleEvent, RunEvent, RunEventKind, RunId,
    };
    use runifold_effect::{EffectExecutorErrorKind, EffectRecord, EffectStatus, EffectStore};
    use runifold_ops::{RunEventCursor, RunEventPageSize, RunEventSource, RunEventSourceErrorKind};
    use rusqlite::Connection;
    use serde_json::json;
    use uuid::Uuid;

    use super::{SqliteStore, SqliteStoreError};

    #[test]
    fn checkpoint_survives_reopen_and_rejects_stale_revision() {
        let path = temporary_database_path();
        let checkpoint = Checkpoint::initial(
            CheckpointId::new(),
            RunId::new(),
            "test",
            1,
            json!({"step": 1}),
        );
        {
            let store = SqliteStore::open(&path).unwrap();
            CheckpointStore::compare_and_swap(&store, &checkpoint, None).unwrap();
        }
        let store = SqliteStore::open(&path).unwrap();
        assert_eq!(
            CheckpointStore::load(&store, checkpoint.id).unwrap(),
            checkpoint
        );

        let next = checkpoint.next(json!({"step": 2})).unwrap();
        CheckpointStore::compare_and_swap(&store, &next, Some(0)).unwrap();
        let stale = checkpoint.next(json!({"step": 3})).unwrap();
        let error = CheckpointStore::compare_and_swap(&store, &stale, Some(0)).unwrap_err();
        assert_eq!(error.kind, CheckpointErrorKind::Conflict);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn effect_survives_reopen_and_preserves_idempotency_index() {
        let path = temporary_database_path();
        let capability_id = CapabilityId::new();
        let request = effect_request(capability_id, "stable-key");
        let completed = EffectRecord {
            revision: 2,
            request: request.clone(),
            status: EffectStatus::Completed {
                output: json!({"ok": true}),
            },
        };
        {
            let store = SqliteStore::open(&path).unwrap();
            EffectStore::compare_and_swap(&store, &EffectRecord::prepared(request.clone()), None)
                .unwrap();
            let started = EffectRecord {
                revision: 1,
                request: request.clone(),
                status: EffectStatus::Started,
            };
            EffectStore::compare_and_swap(&store, &started, Some(0)).unwrap();
            EffectStore::compare_and_swap(&store, &completed, Some(1)).unwrap();
        }

        let store = SqliteStore::open(&path).unwrap();
        assert_eq!(
            store
                .find_by_idempotency(capability_id, "stable-key")
                .unwrap(),
            Some(completed)
        );

        let conflicting = EffectRecord::prepared(effect_request(capability_id, "stable-key"));
        let error = EffectStore::compare_and_swap(&store, &conflicting, None).unwrap_err();
        assert_eq!(error.kind, EffectExecutorErrorKind::IdempotencyConflict);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn journal_round_trips_events_in_run_sequence_order() {
        let store = SqliteStore::open_in_memory().unwrap();
        let run_id = RunId::new();
        let factory = EventFactory::new(run_id, None);
        let first = factory.emit(RunEventKind::Lifecycle(LifecycleEvent::Started), None);
        let second = factory.emit(
            RunEventKind::Lifecycle(LifecycleEvent::Completed {
                output: json!("done"),
            }),
            Some(first.meta.event_id),
        );

        store.record(&first).unwrap();
        store.record(&second).unwrap();

        assert_eq!(store.events(run_id).unwrap(), vec![first, second]);
    }

    #[test]
    fn read_only_event_source_pages_without_mutating_schema() {
        let path = temporary_database_path();
        let run_id = RunId::new();
        let factory = EventFactory::new(run_id, None);
        let first = factory.emit(RunEventKind::Lifecycle(LifecycleEvent::Started), None);
        let second = factory.emit(
            RunEventKind::Domain(DomainEvent {
                namespace: "test".into(),
                name: "middle".into(),
                payload: json!({}),
            }),
            Some(first.meta.event_id),
        );
        let third = factory.emit(
            RunEventKind::Lifecycle(LifecycleEvent::Completed { output: json!({}) }),
            Some(second.meta.event_id),
        );
        {
            let writable = SqliteStore::open(&path).unwrap();
            for event in [&first, &second, &third] {
                writable.record(event).unwrap();
            }
        }

        let read_only = SqliteStore::open_read_only(&path).unwrap();
        let size = RunEventPageSize::new(2).unwrap();
        let first_page = read_only.event_page(run_id, None, size).unwrap();
        assert_eq!(first_page.events, vec![first, second]);
        assert_eq!(first_page.next, Some(RunEventCursor::after(1)));
        let final_page = read_only.event_page(run_id, first_page.next, size).unwrap();
        assert_eq!(final_page.events, vec![third]);
        assert_eq!(final_page.next, None);

        drop(read_only);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn event_source_rejects_index_envelope_mismatch() {
        let path = temporary_database_path();
        let run_id = RunId::new();
        let factory = EventFactory::new(run_id, None);
        let first = factory.emit(RunEventKind::Lifecycle(LifecycleEvent::Started), None);
        let mismatched = factory.emit(
            RunEventKind::Lifecycle(LifecycleEvent::Completed { output: json!({}) }),
            Some(first.meta.event_id),
        );
        {
            let store = SqliteStore::open(&path).unwrap();
            store.record(&first).unwrap();
        }
        let connection = Connection::open(&path).unwrap();
        connection
            .execute(
                "UPDATE runifold_events SET event_json = ?1 WHERE event_id = ?2",
                rusqlite::params![
                    serde_json::to_string(&mismatched).unwrap(),
                    first.meta.event_id.to_string()
                ],
            )
            .unwrap();
        drop(connection);

        let read_only = SqliteStore::open_read_only(&path).unwrap();
        let error = read_only
            .event_page(run_id, None, RunEventPageSize::new(1).unwrap())
            .unwrap_err();
        assert_eq!(error.kind, RunEventSourceErrorKind::CorruptData);

        drop(read_only);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn direct_store_error_preserves_json_source() {
        use std::error::Error as _;

        let error: SqliteStoreError = serde_json::from_str::<RunEvent>("{").unwrap_err().into();

        assert!(matches!(error, SqliteStoreError::Json(_)));
        assert!(error.source().is_some());
    }

    fn effect_request(capability_id: CapabilityId, key: &str) -> EffectRequest {
        EffectRequest {
            effect_id: EffectId::new(),
            invocation_id: InvocationId::new(),
            kind: EffectKind::Tool,
            capability_id,
            input: json!({"value": 1}),
            effect_class: EffectClass::IdempotentWrite,
            idempotency_key: Some(key.into()),
        }
    }

    fn temporary_database_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("runifold-{}.sqlite3", Uuid::now_v7()))
    }
}
