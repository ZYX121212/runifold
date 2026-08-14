//! `PostgreSQL` canonical run-event journal and operational reader.

use runifold_core::{Journal, JournalError, RunEvent, RunId};
use runifold_ops::{
    RunEventCursor, RunEventPage, RunEventPageSize, RunEventSource, RunEventSourceError,
};
use serde_json::Value;

use crate::PostgresConversationStore;

impl Journal for PostgresConversationStore {
    fn record(&self, event: &RunEvent) -> Result<(), JournalError> {
        let table = format!("{}_events", self.table());
        let event_id = event.meta.event_id.as_uuid();
        let run_id = event.meta.run_id.as_uuid();
        let sequence = i64::try_from(event.meta.sequence).map_err(|_| JournalError {
            message: "event sequence exceeds PostgreSQL BIGINT".into(),
        })?;
        let encoded = serde_json::to_value(event).map_err(|_| JournalError {
            message: "canonical run event could not be encoded".into(),
        })?;
        self.blocking()
            .execute(move |client| {
                client.execute(
                    &format!(
                        "INSERT INTO {table} (event_id, run_id, sequence, event_json) \
                         VALUES ($1, $2, $3, $4)"
                    ),
                    &[&event_id, &run_id, &sequence, &encoded],
                )
            })
            .map_err(|_| JournalError {
                message: "PostgreSQL journal worker is unavailable".into(),
            })?
            .map(|_| ())
            .map_err(|_| JournalError {
                message: "PostgreSQL journal write failed".into(),
            })
    }
}

impl RunEventSource for PostgresConversationStore {
    fn event_page(
        &self,
        run_id: RunId,
        after: Option<RunEventCursor>,
        limit: RunEventPageSize,
    ) -> Result<RunEventPage, RunEventSourceError> {
        let table = format!("{}_events", self.table());
        let run_id = run_id.as_uuid();
        let after = after.map_or(-1, |cursor| {
            i64::try_from(cursor.sequence()).unwrap_or(i64::MAX)
        });
        let query_limit = limit
            .get()
            .checked_add(1)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or_else(|| RunEventSourceError::storage("event page limit overflow"))?;
        let rows = self
            .blocking()
            .execute(move |client| {
                client.query(
                    &format!(
                        "SELECT sequence, event_json FROM {table} \
                         WHERE run_id = $1 AND sequence > $2 \
                         ORDER BY sequence ASC LIMIT $3"
                    ),
                    &[&run_id, &after, &query_limit],
                )
            })
            .map_err(|_| RunEventSourceError::storage("PostgreSQL journal worker unavailable"))?
            .map_err(|_| RunEventSourceError::storage("PostgreSQL journal query failed"))?;
        let mut events = rows
            .into_iter()
            .map(|row| {
                let stored_sequence: i64 = row.get("sequence");
                let event = decode_event(row.get("event_json"))?;
                if event.meta.run_id != RunId::from_uuid(run_id)
                    || i64::try_from(event.meta.sequence).ok() != Some(stored_sequence)
                {
                    return Err(RunEventSourceError::corrupt_data(
                        "event index does not match its canonical envelope",
                    ));
                }
                Ok(event)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let has_more = events.len() > limit.get();
        if has_more {
            events.truncate(limit.get());
        }
        let next = if has_more {
            events
                .last()
                .map(|event| RunEventCursor::after(event.meta.sequence))
        } else {
            None
        };
        Ok(RunEventPage { events, next })
    }
}

fn decode_event(value: Value) -> Result<RunEvent, RunEventSourceError> {
    serde_json::from_value(value)
        .map_err(|_| RunEventSourceError::corrupt_data("persisted run event is invalid"))
}
