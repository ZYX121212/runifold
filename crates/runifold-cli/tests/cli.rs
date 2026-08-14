//! End-to-end tests for the read-only operational CLI.

use std::{fs, path::PathBuf, process::Command};

use runifold_core::{EventFactory, LifecycleEvent, RunEventKind, RunId};
use runifold_store_sqlite::SqliteStore;
use serde_json::json;

#[test]
fn inspect_reads_a_complete_exported_run() {
    let path = temporary_path("json");
    let run_id = RunId::new();
    let factory = EventFactory::new(run_id, None);
    let started = factory.emit(RunEventKind::Lifecycle(LifecycleEvent::Started), None);
    let completed = factory.emit(
        RunEventKind::Lifecycle(LifecycleEvent::Completed {
            output: json!({"answer": 42}),
        }),
        Some(started.meta.event_id),
    );
    fs::write(
        &path,
        serde_json::to_vec(&vec![started, completed]).unwrap(),
    )
    .unwrap();

    let output = runifold()
        .args(["run", "inspect", "--events"])
        .arg(&path)
        .output()
        .unwrap();

    assert!(output.status.success(), "{}", stderr(&output));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["status"], "completed");
    assert_eq!(value["event_count"], 2);
    fs::remove_file(path).unwrap();
}

#[test]
fn inspect_rejects_empty_and_invalid_exports() {
    for contents in [b"[]".as_slice(), b"{".as_slice()] {
        let path = temporary_path("json");
        fs::write(&path, contents).unwrap();
        let output = runifold()
            .args(["run", "inspect", "--events"])
            .arg(&path)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(!output.stderr.is_empty());
        fs::remove_file(path).unwrap();
    }
}

#[test]
fn inspect_unknown_sqlite_run_is_read_only_and_fails_cleanly() {
    let path = temporary_path("sqlite3");
    drop(SqliteStore::open(&path).unwrap());
    let output = runifold()
        .args(["run", "inspect", "--sqlite"])
        .arg(&path)
        .args(["--run-id", &RunId::new().to_string()])
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(!output.stderr.is_empty());
    assert!(path.exists());
    fs::remove_file(path).unwrap();
}

#[test]
fn replay_atomically_writes_a_complete_bundle() {
    let events = temporary_path("json");
    let output = temporary_path("replay.json");
    let run_id = RunId::new();
    let factory = EventFactory::new(run_id, None);
    let started = factory.emit(RunEventKind::Lifecycle(LifecycleEvent::Started), None);
    let completed = factory.emit(
        RunEventKind::Lifecycle(LifecycleEvent::Completed { output: json!({}) }),
        Some(started.meta.event_id),
    );
    fs::write(
        &events,
        serde_json::to_vec(&vec![started, completed]).unwrap(),
    )
    .unwrap();

    let command = runifold()
        .args(["run", "replay", "--events"])
        .arg(&events)
        .arg("--output")
        .arg(&output)
        .output()
        .unwrap();
    assert!(command.status.success(), "{}", stderr(&command));
    let bundle: serde_json::Value = serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
    assert_eq!(bundle["mode"], "side_effect_free_evidence");
    assert_eq!(bundle["events"].as_array().unwrap().len(), 2);

    fs::remove_file(events).unwrap();
    fs::remove_file(output).unwrap();
}

fn runifold() -> Command {
    Command::new(env!("CARGO_BIN_EXE_runifold"))
}

fn temporary_path(extension: &str) -> PathBuf {
    std::env::temp_dir().join(format!("runifold-cli-{}.{}", RunId::new(), extension))
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}
