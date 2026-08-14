//! Read-only operational CLI for Runifold execution artifacts and journals.

use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use runifold_core::{Budget, RunEvent, RunId, Usage};
use runifold_ops::{
    BudgetExplanation, RunEventPageSize, RunEventSource, RunInspection, diff_checkpoints,
};
use runifold_store_postgres::PostgresConversationStore;
use runifold_store_sqlite::SqliteStore;
use serde_json::Value;

const DEFAULT_POSTGRES_TABLE: &str = "runifold_conversations";
const MAX_LOADED_EVENTS: usize = 100_000;
const MAX_EVENT_EXPORT_BYTES: usize = 64 * 1024 * 1024;
const MAX_JSON_INPUT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "runifold", about = "Inspect Runifold execution evidence")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inspect, tail, or replay canonical run events.
    Run {
        #[command(subcommand)]
        command: RunCommand,
    },
    /// Compare redacted checkpoint structure.
    Checkpoint {
        #[command(subcommand)]
        command: CheckpointCommand,
    },
    /// Explain budget headroom.
    Budget {
        #[command(subcommand)]
        command: BudgetCommand,
    },
    /// Validate run evidence and report operational health.
    Doctor {
        #[command(flatten)]
        input: EventInput,
    },
}

#[derive(Debug, Subcommand)]
enum RunCommand {
    /// Summarize one canonical run history.
    Inspect {
        #[command(flatten)]
        input: EventInput,
    },
    /// Print the last canonical events.
    Tail {
        #[command(flatten)]
        input: EventInput,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Validate and emit a side-effect-free replay evidence bundle.
    Replay {
        #[command(flatten)]
        input: EventInput,
        #[arg(long)]
        output: PathBuf,
    },
}

/// Exactly one of `events`, `sqlite`, or `postgres` must be supplied.
#[derive(Args, Debug)]
#[group(skip)]
struct EventInput {
    /// Exported JSON array containing canonical `RunEvent` values.
    #[arg(long, conflicts_with_all = ["sqlite", "postgres"])]
    events: Option<PathBuf>,
    /// Existing `SQLite` journal opened strictly read-only.
    #[arg(long, conflicts_with_all = ["events", "postgres"])]
    sqlite: Option<PathBuf>,
    /// `PostgreSQL` connection string; no schema changes are performed.
    #[arg(
        long,
        env = "RUNIFOLD_POSTGRES_URL",
        conflicts_with_all = ["events", "sqlite"]
    )]
    postgres: Option<String>,
    /// Run to query when using a durable journal.
    #[arg(long)]
    run_id: Option<RunId>,
    /// `PostgreSQL` conversation table prefix.
    #[arg(long, default_value = DEFAULT_POSTGRES_TABLE)]
    table: String,
}

impl EventInput {
    async fn load(&self) -> Result<Vec<RunEvent>> {
        if let Some(path) = &self.events {
            if self.run_id.is_some() {
                bail!("--run-id is only valid with --sqlite or --postgres");
            }
            return read_event_export(path);
        }
        if self.sqlite.is_none() && self.postgres.is_none() {
            bail!("one event source is required");
        }
        let run_id = self
            .run_id
            .context("--run-id is required with --sqlite or --postgres")?;
        if let Some(path) = &self.sqlite {
            let store = SqliteStore::open_read_only(path)
                .with_context(|| format!("failed to open {} read-only", path.display()))?;
            return load_source(&store, run_id);
        }
        let connection = self
            .postgres
            .as_deref()
            .context("one event source is required")?;
        let store = PostgresConversationStore::connect(connection, &self.table)
            .await
            .context("failed to connect to PostgreSQL journal")?;
        load_source(&store, run_id)
    }
}

#[derive(Debug, Subcommand)]
enum CheckpointCommand {
    /// Report changed JSON Pointers without exposing values.
    Diff { before: PathBuf, after: PathBuf },
}

#[derive(Debug, Subcommand)]
enum BudgetCommand {
    /// Explain remaining capacity from exported Budget and Usage JSON.
    Explain { budget: PathBuf, usage: PathBuf },
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Run { command } => match command {
            RunCommand::Inspect { input } => {
                let events = input.load().await?;
                print_json(&RunInspection::inspect(&events)?)?;
            }
            RunCommand::Tail { input, limit } => {
                let events = input.load().await?;
                let start = events.len().saturating_sub(limit);
                print_json(&events[start..])?;
            }
            RunCommand::Replay { input, output } => {
                let events = input.load().await?;
                let inspection = RunInspection::inspect(&events)?;
                let bundle = serde_json::json!({
                    "schema_version": 1,
                    "mode": "side_effect_free_evidence",
                    "inspection": inspection,
                    "events": events,
                });
                write_json_atomic(&output, &bundle)?;
            }
        },
        Command::Checkpoint { command } => match command {
            CheckpointCommand::Diff { before, after } => {
                let before: Value = read_json(&before)?;
                let after: Value = read_json(&after)?;
                print_json(&diff_checkpoints(&before, &after))?;
            }
        },
        Command::Budget { command } => match command {
            BudgetCommand::Explain { budget, usage } => {
                let budget: Budget = read_json(&budget)?;
                let usage: Usage = read_json(&usage)?;
                print_json(&BudgetExplanation::new(budget, usage))?;
            }
        },
        Command::Doctor { input } => {
            let events = input.load().await?;
            let inspection = RunInspection::inspect(&events)?;
            print_json(&serde_json::json!({
                "healthy": true,
                "checks": ["canonical_sequence", "causal_links", "single_terminal_state"],
                "inspection": inspection,
            }))?;
        }
    }
    Ok(())
}

fn load_source(source: &dyn RunEventSource, run_id: RunId) -> Result<Vec<RunEvent>> {
    load_source_bounded(source, run_id, MAX_LOADED_EVENTS)
}

fn load_source_bounded(
    source: &dyn RunEventSource,
    run_id: RunId,
    max_events: usize,
) -> Result<Vec<RunEvent>> {
    let page_size = RunEventPageSize::new(1_000)?;
    let mut events = Vec::new();
    let mut cursor = None;
    loop {
        let page = source.event_page(run_id, cursor, page_size)?;
        if page.events.len() > max_events.saturating_sub(events.len()) {
            bail!("run exceeds the CLI safety limit of {max_events} events");
        }
        events.extend(page.events);
        match page.next {
            Some(next) if cursor.is_none_or(|current| next.sequence() > current.sequence()) => {
                cursor = Some(next);
            }
            Some(_) => bail!("event source returned a non-advancing cursor"),
            None => return Ok(events),
        }
    }
}

fn read_json<T>(path: &Path) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    read_json_bounded(path, MAX_JSON_INPUT_BYTES)
}

fn read_event_export(path: &Path) -> Result<Vec<RunEvent>> {
    let events: Vec<RunEvent> = read_json_bounded(path, MAX_EVENT_EXPORT_BYTES)?;
    validate_event_count(events.len(), MAX_LOADED_EVENTS)?;
    Ok(events)
}

fn validate_event_count(event_count: usize, max_events: usize) -> Result<()> {
    if event_count > max_events {
        bail!("run exceeds the CLI safety limit of {max_events} events");
    }
    Ok(())
}

fn read_json_bounded<T>(path: &Path, max_bytes: usize) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let read_limit = u64::try_from(max_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut bytes = Vec::new();
    BufReader::new(file)
        .take(read_limit)
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read {}", path.display()))?;
    if bytes.len() > max_bytes {
        bail!(
            "JSON input {} exceeds the safety limit of {max_bytes} bytes",
            path.display()
        );
    }
    serde_json::from_slice(&bytes).with_context(|| format!("invalid JSON in {}", path.display()))
}

fn print_json(value: &(impl serde::Serialize + ?Sized)) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn write_json_atomic(path: &Path, value: &(impl serde::Serialize + ?Sized)) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    serde_json::from_slice::<Value>(&bytes).context("generated replay bundle is invalid JSON")?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("replay output must have a UTF-8 file name")?;
    let temporary_path = parent.join(format!(".{file_name}.{}.tmp", RunId::new()));
    let mut temporary = TemporaryOutput::new(temporary_path);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temporary.path())
        .with_context(|| format!("failed to create temporary output for {}", path.display()))?;
    file.write_all(&bytes)
        .with_context(|| format!("failed to write temporary output for {}", path.display()))?;
    file.flush()
        .with_context(|| format!("failed to flush temporary output for {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to sync temporary output for {}", path.display()))?;
    drop(file);
    std::fs::rename(temporary.path(), path)
        .with_context(|| format!("failed to atomically replace {}", path.display()))?;
    temporary.commit();
    sync_parent_directory(parent)?;
    Ok(())
}

#[cfg(unix)]
fn sync_parent_directory(parent: &Path) -> Result<()> {
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("failed to sync output directory {}", parent.display()))
}

#[cfg(not(unix))]
fn sync_parent_directory(_parent: &Path) -> Result<()> {
    Ok(())
}

struct TemporaryOutput {
    path: PathBuf,
    committed: bool,
}

impl TemporaryOutput {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            committed: false,
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for TemporaryOutput {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use runifold_core::{EventFactory, LifecycleEvent, RunEventKind};
    use runifold_ops::{
        RunEventCursor, RunEventPage, RunEventSourceError, RunEventSourceErrorKind,
    };
    use serde_json::json;

    use super::*;

    struct FixedSource {
        events: Vec<RunEvent>,
        repeated_cursor: bool,
    }

    impl RunEventSource for FixedSource {
        fn event_page(
            &self,
            _run_id: RunId,
            after: Option<RunEventCursor>,
            limit: RunEventPageSize,
        ) -> std::result::Result<RunEventPage, RunEventSourceError> {
            if self.repeated_cursor {
                return Ok(RunEventPage {
                    events: Vec::new(),
                    next: Some(after.unwrap_or_else(|| RunEventCursor::after(0))),
                });
            }
            let start = after.map_or(0, |cursor| {
                usize::try_from(cursor.sequence())
                    .unwrap_or(usize::MAX)
                    .saturating_add(1)
            });
            if start >= self.events.len() {
                return Ok(RunEventPage {
                    events: Vec::new(),
                    next: None,
                });
            }
            let end = start.saturating_add(limit.get()).min(self.events.len());
            let events = self.events[start..end].to_vec();
            let next = (end < self.events.len())
                .then(|| RunEventCursor::after(self.events[end - 1].meta.sequence));
            Ok(RunEventPage { events, next })
        }
    }

    #[test]
    fn parser_rejects_multiple_event_sources() {
        let error = Cli::try_parse_from([
            "runifold",
            "run",
            "inspect",
            "--events",
            "events.json",
            "--sqlite",
            "events.sqlite3",
        ])
        .unwrap_err();
        assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[tokio::test]
    async fn event_input_rejects_missing_source_before_run_id() {
        let input = EventInput {
            events: None,
            sqlite: None,
            postgres: None,
            run_id: None,
            table: DEFAULT_POSTGRES_TABLE.into(),
        };
        assert_eq!(
            input.load().await.unwrap_err().to_string(),
            "one event source is required"
        );
    }

    #[test]
    fn bounded_loading_rejects_large_runs_before_extending_the_buffer() {
        let run_id = RunId::new();
        let factory = EventFactory::new(run_id, None);
        let events = vec![
            factory.emit(RunEventKind::Lifecycle(LifecycleEvent::Started), None),
            factory.emit(
                RunEventKind::Lifecycle(LifecycleEvent::Completed { output: json!({}) }),
                None,
            ),
        ];
        let source = FixedSource {
            events,
            repeated_cursor: false,
        };
        let error = load_source_bounded(&source, run_id, 1).unwrap_err();
        assert_eq!(
            error.to_string(),
            "run exceeds the CLI safety limit of 1 events"
        );
    }

    #[test]
    fn bounded_loading_rejects_non_advancing_sources() {
        let source = FixedSource {
            events: Vec::new(),
            repeated_cursor: true,
        };
        let error = load_source_bounded(&source, RunId::new(), 1).unwrap_err();
        assert_eq!(
            error.to_string(),
            "event source returned a non-advancing cursor"
        );
    }

    #[test]
    fn unknown_run_loads_no_events_and_fails_inspection() {
        let source = FixedSource {
            events: Vec::new(),
            repeated_cursor: false,
        };
        let events = load_source_bounded(&source, RunId::new(), 1).unwrap();
        assert!(events.is_empty());
        assert!(RunInspection::inspect(&events).is_err());
    }

    #[test]
    fn source_errors_keep_their_typed_category() {
        let error = RunEventSourceError::corrupt_data("invalid envelope");
        assert_eq!(error.kind, RunEventSourceErrorKind::CorruptData);
    }

    #[test]
    fn bounded_json_read_stops_at_the_byte_limit() {
        let path = std::env::temp_dir().join(format!("runifold-cli-{}.json", RunId::new()));
        std::fs::write(&path, b"[null]").unwrap();
        let error = read_json_bounded::<Value>(&path, 4).unwrap_err();
        assert!(error.to_string().contains("exceeds the safety limit"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn exported_event_count_uses_the_same_safety_limit() {
        assert!(validate_event_count(MAX_LOADED_EVENTS, MAX_LOADED_EVENTS).is_ok());
        let error = validate_event_count(MAX_LOADED_EVENTS + 1, MAX_LOADED_EVENTS).unwrap_err();
        assert!(error.to_string().contains("100000 events"));
    }

    #[test]
    fn atomic_json_write_replaces_complete_files_and_cleans_failed_temps() {
        let parent = std::env::temp_dir().join(format!("runifold-cli-{}", RunId::new()));
        std::fs::create_dir(&parent).unwrap();
        let output = parent.join("replay.json");
        std::fs::write(&output, b"stale").unwrap();
        write_json_atomic(&output, &serde_json::json!({"complete": true})).unwrap();
        let value: Value = serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
        assert_eq!(value, serde_json::json!({"complete": true}));

        let blocked = parent.join("blocked.json");
        std::fs::create_dir(&blocked).unwrap();
        assert!(write_json_atomic(&blocked, &serde_json::json!({})).is_err());
        assert!(
            std::fs::read_dir(&parent)
                .unwrap()
                .filter_map(Result::ok)
                .all(|entry| !entry.file_name().to_string_lossy().ends_with(".tmp"))
        );

        std::fs::remove_dir_all(parent).unwrap();
    }
}
