//! Stable, read-only operational views over canonical Runifold artifacts.

use std::collections::BTreeSet;

use runifold_core::{
    Budget, BudgetEvent, LifecycleEvent, RunError, RunEvent, RunEventKind, RunId, Usage,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Largest event page accepted by the stable operational query API.
pub const MAX_EVENT_PAGE_SIZE: usize = 1_000;

/// Exclusive position in one run's canonical event sequence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct RunEventCursor(u64);

impl RunEventCursor {
    /// Creates a cursor positioned after `sequence`.
    #[must_use]
    pub const fn after(sequence: u64) -> Self {
        Self(sequence)
    }

    /// Returns the last sequence already observed by the caller.
    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.0
    }
}

/// Validated number of events requested from an operational source.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct RunEventPageSize(usize);

impl RunEventPageSize {
    /// Creates a bounded page size.
    ///
    /// # Errors
    ///
    /// Rejects zero and values above [`MAX_EVENT_PAGE_SIZE`].
    pub const fn new(value: usize) -> Result<Self, RunEventQueryError> {
        if value == 0 || value > MAX_EVENT_PAGE_SIZE {
            Err(RunEventQueryError::InvalidPageSize { value })
        } else {
            Ok(Self(value))
        }
    }

    /// Returns the validated page size.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

/// One ordered page read from a durable canonical journal.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RunEventPage {
    /// Canonical events ordered by ascending sequence.
    pub events: Vec<RunEvent>,
    /// Cursor for the next page, absent when the source is exhausted.
    pub next: Option<RunEventCursor>,
}

/// Stable read-only boundary implemented by durable journal adapters.
pub trait RunEventSource: Send + Sync {
    /// Reads canonical events after an exclusive sequence cursor.
    ///
    /// # Errors
    ///
    /// Returns a typed source error when storage access or decoding fails.
    fn event_page(
        &self,
        run_id: RunId,
        after: Option<RunEventCursor>,
        limit: RunEventPageSize,
    ) -> Result<RunEventPage, RunEventSourceError>;
}

/// Stable operational source failure category.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RunEventSourceErrorKind {
    /// Durable storage could not complete the query.
    Storage,
    /// Persisted data was not a canonical `RunEvent`.
    CorruptData,
}

/// Failure while reading canonical events from a durable source.
#[derive(Clone, Debug, Error, Deserialize, Eq, PartialEq, Serialize)]
#[error("run event source {kind:?}: {message}")]
pub struct RunEventSourceError {
    /// Stable error category suitable for application mapping.
    pub kind: RunEventSourceErrorKind,
    /// Redacted diagnostic message.
    pub message: String,
}

impl RunEventSourceError {
    /// Creates a storage failure.
    #[must_use]
    pub fn storage(message: impl Into<String>) -> Self {
        Self {
            kind: RunEventSourceErrorKind::Storage,
            message: message.into(),
        }
    }

    /// Creates a persisted-data decoding failure.
    #[must_use]
    pub fn corrupt_data(message: impl Into<String>) -> Self {
        Self {
            kind: RunEventSourceErrorKind::CorruptData,
            message: message.into(),
        }
    }
}

/// Invalid operational event query.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum RunEventQueryError {
    /// The requested page size is outside the supported bounds.
    #[error("event page size {value} must be between 1 and {MAX_EVENT_PAGE_SIZE}")]
    InvalidPageSize {
        /// Rejected page size.
        value: usize,
    },
}

/// Current terminal state inferred from canonical lifecycle events.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RunStatus {
    /// A start exists without a terminal lifecycle event.
    Running,
    /// The run completed successfully.
    Completed,
    /// The run failed with a typed error.
    Failed,
    /// The run was cancelled.
    Cancelled,
}

/// Read-only operational summary of one run event stream.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RunInspection {
    /// Inspection schema version.
    pub schema_version: u32,
    /// Inspected run identity.
    pub run_id: RunId,
    /// Inferred lifecycle state.
    pub status: RunStatus,
    /// Number of canonical events.
    pub event_count: usize,
    /// Last observed monotonic sequence.
    pub last_sequence: u64,
    /// Latest cumulative usage snapshot.
    pub usage: Usage,
    /// Terminal typed error, when the run failed.
    pub error: Option<RunError>,
    /// Domain event names in first-observed order.
    pub domain_events: Vec<String>,
}

impl RunInspection {
    /// Current inspection contract version.
    pub const SCHEMA_VERSION: u32 = 1;

    /// Validates and summarizes one canonical run history.
    ///
    /// # Errors
    ///
    /// Rejects empty, mixed-run, non-monotonic, causally invalid, or
    /// multiply-terminal histories.
    pub fn inspect(events: &[RunEvent]) -> Result<Self, InspectionError> {
        let first = events.first().ok_or(InspectionError::Empty)?;
        let run_id = first.meta.run_id;
        let mut seen = BTreeSet::new();
        let mut usage = Usage::default();
        let mut terminal = None;
        let mut error = None;
        let mut domain_events = Vec::new();
        for (index, event) in events.iter().enumerate() {
            if event.meta.run_id != run_id {
                return Err(InspectionError::MixedRun { index });
            }
            let expected = u64::try_from(index).unwrap_or(u64::MAX);
            if event.meta.sequence != expected {
                return Err(InspectionError::Sequence {
                    index,
                    expected,
                    actual: event.meta.sequence,
                });
            }
            if event
                .meta
                .caused_by
                .is_some_and(|cause| !seen.contains(&cause))
            {
                return Err(InspectionError::UnknownCause { index });
            }
            seen.insert(event.meta.event_id);
            match &event.kind {
                RunEventKind::Budget(BudgetEvent::Updated { usage: current }) => usage = *current,
                RunEventKind::Domain(domain) => {
                    domain_events.push(format!("{}.{}", domain.namespace, domain.name));
                }
                RunEventKind::Lifecycle(LifecycleEvent::Completed { .. }) => {
                    set_terminal(&mut terminal, RunStatus::Completed, index)?;
                }
                RunEventKind::Lifecycle(LifecycleEvent::Failed { error: failure }) => {
                    set_terminal(&mut terminal, RunStatus::Failed, index)?;
                    error = Some(failure.clone());
                }
                RunEventKind::Lifecycle(LifecycleEvent::Cancelled) => {
                    set_terminal(&mut terminal, RunStatus::Cancelled, index)?;
                }
                _ => {}
            }
        }
        Ok(Self {
            schema_version: Self::SCHEMA_VERSION,
            run_id,
            status: terminal.unwrap_or(RunStatus::Running),
            event_count: events.len(),
            last_sequence: events.last().map_or(0, |event| event.meta.sequence),
            usage,
            error,
            domain_events,
        })
    }
}

fn set_terminal(
    terminal: &mut Option<RunStatus>,
    status: RunStatus,
    index: usize,
) -> Result<(), InspectionError> {
    if terminal.replace(status).is_some() {
        Err(InspectionError::MultipleTerminalEvents { index })
    } else {
        Ok(())
    }
}

/// One budget dimension and its remaining headroom.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BudgetDimension {
    /// Stable dimension name.
    pub name: String,
    /// Configured limit, or `None` when unbounded.
    pub limit: Option<u64>,
    /// Observed usage in the same unit.
    pub used: u64,
    /// Remaining capacity, or `None` when unbounded.
    pub remaining: Option<u64>,
    /// Whether observed usage exceeds the configured limit.
    pub exceeded: bool,
}

/// Machine-readable explanation of budget consumption.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BudgetExplanation {
    /// One entry per canonical budget dimension.
    pub dimensions: Vec<BudgetDimension>,
}

impl BudgetExplanation {
    /// Computes remaining headroom without mutating runtime accounting.
    #[must_use]
    pub fn new(budget: Budget, usage: Usage) -> Self {
        let duration_limit = budget
            .duration
            .map(|value| u64::try_from(value.as_micros()).unwrap_or(u64::MAX));
        Self {
            dimensions: vec![
                dimension("tokens", budget.tokens, usage.tokens),
                dimension("cost_microusd", budget.cost_microusd, usage.cost_microusd),
                dimension("duration_micros", duration_limit, usage.duration_micros),
                dimension("turns", budget.turns, usage.turns),
                dimension("tool_calls", budget.tool_calls, usage.tool_calls),
                dimension("delegations", budget.delegations, usage.delegations),
            ],
        }
    }
}

fn dimension(name: &str, limit: Option<u64>, used: u64) -> BudgetDimension {
    BudgetDimension {
        name: name.into(),
        limit,
        used,
        remaining: limit.map(|limit| limit.saturating_sub(used)),
        exceeded: limit.is_some_and(|limit| used > limit),
    }
}

/// Kind of structural JSON checkpoint change.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointChangeKind {
    /// A path exists only in the newer checkpoint.
    Added,
    /// A path exists only in the older checkpoint.
    Removed,
    /// Both checkpoints contain a different scalar or container kind.
    Changed,
}

/// One value-free checkpoint change safe for operator output.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CheckpointChange {
    /// JSON Pointer identifying the changed value.
    pub path: String,
    /// Structural change kind.
    pub kind: CheckpointChangeKind,
}

/// Computes a bounded, value-free structural checkpoint diff.
#[must_use]
pub fn diff_checkpoints(before: &Value, after: &Value) -> Vec<CheckpointChange> {
    let mut changes = Vec::new();
    diff_at("", before, after, &mut changes);
    changes.truncate(1_024);
    changes
}

fn diff_at(path: &str, before: &Value, after: &Value, changes: &mut Vec<CheckpointChange>) {
    if changes.len() >= 1_024 || before == after {
        return;
    }
    match (before, after) {
        (Value::Object(before), Value::Object(after)) => {
            let keys = before.keys().chain(after.keys()).collect::<BTreeSet<_>>();
            for key in keys {
                let child = format!("{path}/{}", key.replace('~', "~0").replace('/', "~1"));
                match (before.get(key), after.get(key)) {
                    (Some(left), Some(right)) => diff_at(&child, left, right, changes),
                    (None, Some(_)) => changes.push(CheckpointChange {
                        path: child,
                        kind: CheckpointChangeKind::Added,
                    }),
                    (Some(_), None) => changes.push(CheckpointChange {
                        path: child,
                        kind: CheckpointChangeKind::Removed,
                    }),
                    (None, None) => {}
                }
            }
        }
        _ => changes.push(CheckpointChange {
            path: if path.is_empty() {
                "/".into()
            } else {
                path.into()
            },
            kind: CheckpointChangeKind::Changed,
        }),
    }
}

/// Typed run-inspection failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum InspectionError {
    /// No events were supplied.
    #[error("run history is empty")]
    Empty,
    /// An event belongs to another run.
    #[error("event {index} belongs to a different run")]
    MixedRun {
        /// Zero-based event index.
        index: usize,
    },
    /// Sequence numbers are not contiguous.
    #[error("event {index} has sequence {actual}; expected {expected}")]
    Sequence {
        /// Zero-based event index.
        index: usize,
        /// Required canonical sequence.
        expected: u64,
        /// Observed sequence.
        actual: u64,
    },
    /// A causal parent did not precede its event.
    #[error("event {index} references an unknown or future cause")]
    UnknownCause {
        /// Zero-based event index.
        index: usize,
    },
    /// More than one terminal lifecycle event was present.
    #[error("event {index} adds a second terminal lifecycle state")]
    MultipleTerminalEvents {
        /// Zero-based event index.
        index: usize,
    },
}

#[cfg(test)]
mod tests {
    use runifold_core::{Budget, RunEvent, Usage};

    use super::{
        BudgetExplanation, CheckpointChangeKind, RunEventPageSize, RunEventQueryError,
        RunInspection, RunStatus, diff_checkpoints,
    };

    #[test]
    fn inspection_validates_and_summarizes_canonical_history() {
        let scenario = runifold_test_fixture();
        let inspection = RunInspection::inspect(&scenario).unwrap();

        assert_eq!(inspection.status, RunStatus::Completed);
        assert_eq!(inspection.event_count, 2);
    }

    #[test]
    fn budget_explanation_is_saturating_and_marks_excess() {
        let explanation = BudgetExplanation::new(
            Budget {
                tokens: Some(10),
                ..Budget::default()
            },
            Usage {
                tokens: 12,
                ..Usage::default()
            },
        );
        let tokens = &explanation.dimensions[0];
        assert_eq!(tokens.remaining, Some(0));
        assert!(tokens.exceeded);
    }

    #[test]
    fn checkpoint_diff_reports_paths_without_values() {
        let changes = diff_checkpoints(
            &serde_json::json!({"secret": "old", "keep": 1}),
            &serde_json::json!({"secret": "new", "add": true}),
        );
        assert!(changes.iter().any(|change| {
            change.path == "/secret" && change.kind == CheckpointChangeKind::Changed
        }));
        assert!(!serde_json::to_string(&changes).unwrap().contains("old"));
    }

    #[test]
    fn event_page_size_enforces_public_query_bounds() {
        assert_eq!(RunEventPageSize::new(1).unwrap().get(), 1);
        assert!(matches!(
            RunEventPageSize::new(0),
            Err(RunEventQueryError::InvalidPageSize { value: 0 })
        ));
        assert!(RunEventPageSize::new(1_001).is_err());
    }

    fn runifold_test_fixture() -> Vec<RunEvent> {
        use runifold_core::{
            BudgetTracker, CapabilitySet, EventFactory, LifecycleEvent, RunContext, RunEventKind,
        };
        let run = RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new());
        let factory = EventFactory::new(run.run_id(), None);
        let started = factory.emit(RunEventKind::Lifecycle(LifecycleEvent::Started), None);
        let completed = factory.emit(
            RunEventKind::Lifecycle(LifecycleEvent::Completed {
                output: serde_json::json!({"ok": true}),
            }),
            Some(started.meta.event_id),
        );
        vec![started, completed]
    }
}
