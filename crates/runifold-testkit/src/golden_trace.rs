//! Stable execution traces with nondeterministic identities removed.

use runifold_core::RunEvent;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Versioned, serializable execution trace for behavioral regression tests.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GoldenTrace {
    /// Normalization contract version.
    pub schema_version: u32,
    /// Canonical events with generated identities and wall-clock values scrubbed.
    pub events: Vec<Value>,
}

impl GoldenTrace {
    /// Current normalization contract version.
    pub const SCHEMA_VERSION: u32 = 1;

    /// Normalizes canonical runtime events for deterministic comparison.
    ///
    /// # Errors
    ///
    /// Returns [`serde_json::Error`] only if a public event cannot be encoded.
    pub fn from_events(events: &[RunEvent]) -> Result<Self, serde_json::Error> {
        let events = events
            .iter()
            .map(serde_json::to_value)
            .map(|value| value.map(normalize_value))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            schema_version: Self::SCHEMA_VERSION,
            events,
        })
    }

    /// Compares two normalized traces and reports the first divergent event.
    ///
    /// # Errors
    ///
    /// Returns [`GoldenTraceMismatch`] for a schema or event difference.
    pub fn assert_matches(&self, expected: &Self) -> Result<(), GoldenTraceMismatch> {
        if self.schema_version != expected.schema_version {
            return Err(GoldenTraceMismatch::SchemaVersion {
                expected: expected.schema_version,
                actual: self.schema_version,
            });
        }
        let limit = self.events.len().max(expected.events.len());
        for index in 0..limit {
            let actual = self.events.get(index);
            let expected = expected.events.get(index);
            if actual != expected {
                return Err(GoldenTraceMismatch::Event {
                    index,
                    expected: expected.cloned(),
                    actual: actual.cloned(),
                });
            }
        }
        Ok(())
    }
}

fn normalize_value(mut value: Value) -> Value {
    normalize_at(None, &mut value);
    value
}

fn normalize_at(key: Option<&str>, value: &mut Value) {
    if key.is_some_and(is_nondeterministic_key) {
        *value = Value::String("<normalized>".into());
        return;
    }
    match value {
        Value::Array(values) => {
            for value in values {
                normalize_at(None, value);
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                normalize_at(Some(key), value);
            }
        }
        _ => {}
    }
}

fn is_nondeterministic_key(key: &str) -> bool {
    key == "timestamp_ms"
        || key == "event_id"
        || key == "run_id"
        || key == "parent_run_id"
        || key == "caused_by"
        || key == "invocation_id"
        || key == "effect_id"
        || key == "child_run_id"
}

/// Typed golden-trace comparison failure.
#[derive(Clone, Debug, Error, PartialEq)]
#[non_exhaustive]
pub enum GoldenTraceMismatch {
    /// The normalization contracts differ.
    #[error("golden trace schema mismatch: expected {expected}, got {actual}")]
    SchemaVersion {
        /// Expected version.
        expected: u32,
        /// Actual version.
        actual: u32,
    },
    /// One event differs or is missing.
    #[error("golden trace diverged at event {index}")]
    Event {
        /// Zero-based event index.
        index: usize,
        /// Expected event, or `None` when the actual trace has an extra event.
        expected: Option<Value>,
        /// Actual event, or `None` when the actual trace ended early.
        actual: Option<Value>,
    },
}

#[cfg(test)]
mod tests {
    use runifold_core::Budget;

    use crate::RunScenario;

    use super::{GoldenTrace, GoldenTraceMismatch};

    #[test]
    fn generated_ids_and_timestamps_do_not_destabilize_golden_traces() {
        let first = RunScenario::new(Budget::default());
        let first_started = first.start();
        first.complete(serde_json::json!({"ok": true}), &first_started);
        let second = RunScenario::new(Budget::default());
        let second_started = second.start();
        second.complete(serde_json::json!({"ok": true}), &second_started);

        let actual = GoldenTrace::from_events(&first.recorded_events()).unwrap();
        let expected = GoldenTrace::from_events(&second.recorded_events()).unwrap();

        actual.assert_matches(&expected).unwrap();
    }

    #[test]
    fn divergent_event_payload_reports_the_exact_index() {
        let first = RunScenario::new(Budget::default());
        let first_started = first.start();
        first.complete(serde_json::json!({"ok": true}), &first_started);
        let second = RunScenario::new(Budget::default());
        let second_started = second.start();
        second.complete(serde_json::json!({"ok": false}), &second_started);

        let actual = GoldenTrace::from_events(&first.recorded_events()).unwrap();
        let expected = GoldenTrace::from_events(&second.recorded_events()).unwrap();
        let error = actual.assert_matches(&expected).unwrap_err();

        assert!(matches!(error, GoldenTraceMismatch::Event { index: 1, .. }));
    }
}
