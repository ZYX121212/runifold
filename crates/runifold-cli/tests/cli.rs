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
    let mut command = Command::new(env!("CARGO_BIN_EXE_runifold"));
    command.env_remove("RUNIFOLD_POSTGRES_URL");
    command
}

fn temporary_path(extension: &str) -> PathBuf {
    std::env::temp_dir().join(format!("runifold-cli-{}.{}", RunId::new(), extension))
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn ai_recipe_works_outside_the_source_repository() {
    let directory = temporary_path("project");
    fs::create_dir(&directory).unwrap();
    let output = runifold()
        .current_dir(&directory)
        .args(["ai", "recipe", "add-tool", "--json"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", stderr(&output));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["knowledge_version"], env!("CARGO_PKG_VERSION"));
    assert!(value["document"].as_str().unwrap().contains("async fn add"));
    assert!(
        value["document"]
            .as_str()
            .unwrap()
            .contains("https://github.com/")
    );
    assert!(!value["document"].as_str().unwrap().contains("../../../"));
    fs::remove_dir(directory).unwrap();
}

#[test]
fn ai_explain_resolves_runtime_codes_and_existing_aliases() {
    for (input, expected) in [
        ("RF-TOOL-003", "RF-TOOL-003"),
        ("runifold.capability_denied", "RF-RUN-002"),
        ("RF-EFFECT-003", "RF-EFFECT-003"),
    ] {
        let output = runifold()
            .args(["ai", "explain", input, "--json"])
            .output()
            .unwrap();
        assert!(output.status.success(), "{}", stderr(&output));
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["diagnostic"]["code"], expected);
        assert!(
            !value["diagnostic"]["canonical_fix"]
                .as_str()
                .unwrap()
                .is_empty()
        );
    }
    for args in [
        ["ai", "recipe", "missing", "--json"],
        ["ai", "explain", "RF-UNKNOWN", "--json"],
    ] {
        let output = runifold().args(args).output().unwrap();
        assert!(!output.status.success());
    }
}

#[test]
fn ai_doctor_inspects_renamed_optional_dependencies_without_claiming_resolved_features() {
    let directory = temporary_path("project with spaces");
    fs::create_dir_all(directory.join("src")).unwrap();
    fs::create_dir_all(directory.join("runtime/src")).unwrap();
    fs::write(directory.join("src/lib.rs"), "").unwrap();
    fs::write(directory.join("runtime/src/lib.rs"), "").unwrap();
    fs::write(directory.join("Cargo.toml"), r#"
[package]
name = "sample-app"
version = "0.1.0"
edition = "2024"
[workspace]
[dependencies]
rf = { package = "runifold", version = "=0.10.0", path = "runtime", optional = true, default-features = false, features = ["runtime"] }
"#).unwrap();
    fs::write(
        directory.join("runtime/Cargo.toml"),
        r#"
[package]
name = "runifold"
version = "0.10.0"
edition = "2024"
[features]
runtime = []
"#,
    )
    .unwrap();
    let manifest = directory.join("Cargo.toml");
    let before = fs::read(&manifest).unwrap();
    let output = runifold()
        .args(["doctor", "--ai", "--json", "--manifest-path"])
        .arg(&manifest)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stdout)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let declaration = &value["declarations"][0];
    assert_eq!(declaration["dependency"], "runifold");
    assert_eq!(declaration["import_name"], "rf");
    assert_eq!(declaration["optional"], true);
    assert_eq!(declaration["default_features"], false);
    assert_eq!(declaration["declared_features"], json!(["runtime"]));
    assert!(value["not_checked"].as_array().unwrap().len() >= 4);
    assert_eq!(before, fs::read(manifest).unwrap());
    assert!(
        !directory.join("Cargo.lock").exists(),
        "inspection created a lockfile"
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn ai_doctor_reports_missing_and_invalid_manifests_as_structured_failures() {
    let path = temporary_path("Cargo.toml");
    for contents in [None, Some("[package\ninvalid")] {
        if let Some(contents) = contents {
            fs::write(&path, contents).unwrap();
        }
        let output = runifold()
            .args(["doctor", "--ai", "--json", "--manifest-path"])
            .arg(&path)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["healthy"], false);
        assert_eq!(value["issues"][0]["code"], "RF-AI-CARGO-001");
    }
    fs::remove_file(path).unwrap();
}

#[test]
fn ai_doctor_rejects_mixed_operational_arguments() {
    for source in ["--events", "--sqlite", "--postgres"] {
        let output = runifold()
            .args(["doctor", "--ai", source, "unused"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
    }
}

#[test]
fn operational_doctor_keeps_its_json_contract() {
    let path = temporary_path("json");
    let factory = EventFactory::new(RunId::new(), None);
    let start = factory.emit(RunEventKind::Lifecycle(LifecycleEvent::Started), None);
    let finish = factory.emit(
        RunEventKind::Lifecycle(LifecycleEvent::Completed { output: json!({}) }),
        Some(start.meta.event_id),
    );
    fs::write(&path, serde_json::to_vec(&[start, finish]).unwrap()).unwrap();
    let output = runifold()
        .args(["doctor", "--json", "--events"])
        .arg(&path)
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", stderr(&output));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["healthy"], true);
    assert_eq!(value["inspection"]["event_count"], 2);
    assert!(value.get("mode").is_none());
    fs::remove_file(path).unwrap();
}
