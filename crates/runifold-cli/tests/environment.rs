//! Resolved versions, local vendor content, feature selection and privacy.
use serde_json::Value;
use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
};

struct Project(PathBuf);
impl Project {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "runifold-private-environment-{}",
            runifold_core::RunId::new()
        ));
        fs::create_dir_all(root.join("src")).unwrap();
        fs::create_dir_all(root.join("vendor/engine/src")).unwrap();
        fs::write(root.join("Cargo.toml"), "[package]\nname='environment-fixture'\nversion='1.2.3'\nedition='2021'\n[workspace]\n[features]\ndefault=[]\nextra=[]\n[dependencies]\nengine={package='runifold-model',path='vendor/engine'}\n").unwrap();
        fs::write(root.join("src/lib.rs"), "pub fn app() {}\n").unwrap();
        fs::write(root.join("vendor/engine/Cargo.toml"), "[package]\nname='runifold-model'\nversion='1.2.3'\nedition='2021'\n[features]\ndefault=[]\nextra=[]\n").unwrap();
        fs::write(
            root.join("vendor/engine/src/lib.rs"),
            "pub fn engine() {}\n",
        )
        .unwrap();
        let output = Command::new("cargo")
            .args(["generate-lockfile", "--offline", "--manifest-path"])
            .arg(root.join("Cargo.toml"))
            .output()
            .unwrap();
        assert!(output.status.success());
        Self(root)
    }
    fn inspect(&self, flags: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_runifold"))
            .args(["ai", "environment", "--json", "--manifest-path"])
            .arg(self.0.join("Cargo.toml"))
            .args(flags)
            .output()
            .unwrap()
    }
    fn snapshot(&self, flags: &[&str]) -> Value {
        let output = self.inspect(flags);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        serde_json::from_slice(&output.stdout).unwrap()
    }
}
impl Drop for Project {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn vendor_source_and_patch_changes_are_visible_without_changing_locked_version() {
    let project = Project::new();
    let original = project.snapshot(&[]);
    assert_eq!(original, project.snapshot(&[]));
    fs::write(
        project.0.join("vendor/engine/src/lib.rs"),
        "pub fn engine() -> bool { true }\n",
    )
    .unwrap();
    let modified = project.snapshot(&[]);
    assert_ne!(
        original["fingerprint_sha256"],
        modified["fingerprint_sha256"]
    );
    assert_eq!(original["lock_sha256"], modified["lock_sha256"]);
    let engine = modified["packages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|package| package["name"] == "runifold-model")
        .unwrap();
    assert_eq!(engine["version"], "1.2.3");
    assert_eq!(engine["source_kind"], "path");
    fs::write(
        project.0.join("vendor/engine/PATCH.md"),
        "local diagnostic patch\n",
    )
    .unwrap();
    assert_ne!(
        modified["fingerprint_sha256"],
        project.snapshot(&[])["fingerprint_sha256"]
    );
}

#[test]
fn snapshots_are_location_independent_and_do_not_collect_env_credentials() {
    let first = Project::new();
    let second = Project::new();
    let before = first.snapshot(&[]);
    fs::write(first.0.join(".env"), "TOKEN=do-not-emit-private-secret\n").unwrap();
    let after = first.snapshot(&[]);
    assert_eq!(before["fingerprint_sha256"], after["fingerprint_sha256"]);
    assert_eq!(
        before["fingerprint_sha256"],
        second.snapshot(&[])["fingerprint_sha256"]
    );
    let text = after.to_string();
    assert!(!text.contains("do-not-emit-private-secret"));
    assert!(!text.contains("runifold-private-environment"));
}

#[test]
fn resolved_features_and_renamed_dependency_edges_are_recorded() {
    let project = Project::new();
    let default = project.snapshot(&[]);
    let configured = project.snapshot(&["--features", "extra,engine/extra"]);
    assert_ne!(configured["compiler_host"].as_str().unwrap(), "");
    assert_eq!(
        configured["compiler_details_sha256"]
            .as_str()
            .unwrap()
            .len(),
        64
    );
    assert_ne!(
        default["fingerprint_sha256"],
        configured["fingerprint_sha256"]
    );
    let packages = configured["packages"].as_array().unwrap();
    let engine = packages
        .iter()
        .find(|package| package["name"] == "runifold-model")
        .unwrap();
    assert!(
        engine["resolved_features"]
            .as_array()
            .unwrap()
            .contains(&Value::from("extra"))
    );
    let app = packages
        .iter()
        .find(|package| package["name"] == "environment-fixture")
        .unwrap();
    assert_eq!(app["dependencies"][0]["import_name"], "engine");
    assert_eq!(
        app["dependencies"][0]["identity_sha256"],
        engine["identity_sha256"]
    );
}

#[test]
fn missing_lock_and_invalid_manifests_fail_without_creating_state_or_echoing_input() {
    let project = Project::new();
    fs::remove_file(project.0.join("Cargo.lock")).unwrap();
    let output = project.inspect(&[]);
    assert!(!output.status.success());
    assert!(!project.0.join("Cargo.lock").exists());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["healthy"], false);
    assert_eq!(report["code"], "RF-AI-ENVIRONMENT-001");
    fs::write(
        project.0.join("Cargo.toml"),
        "private-secret invalid manifest",
    )
    .unwrap();
    let output = project.inspect(&[]);
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("private-secret"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("private-secret"));
}

#[test]
fn deleted_tracked_source_is_a_change_and_not_an_incomplete_manifest() {
    let project = Project::new();
    fs::write(project.0.join("src/obsolete.rs"), "pub fn obsolete() {}\n").unwrap();
    assert!(
        Command::new("git")
            .args(["init", "-q"])
            .arg(&project.0)
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new("git")
            .arg("-C")
            .arg(&project.0)
            .args(["add", "."])
            .status()
            .unwrap()
            .success()
    );
    let before = project.snapshot(&[]);
    fs::remove_file(project.0.join("src/obsolete.rs")).unwrap();
    assert_ne!(
        before["fingerprint_sha256"],
        project.snapshot(&[])["fingerprint_sha256"]
    );
}

#[test]
fn custom_target_paths_do_not_enter_diagnostic_reports() {
    let project = Project::new();
    let output = project.inspect(&["--target", "/private/do-not-emit-platform.json"]);
    assert!(!output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["healthy"], false);
    assert!(!String::from_utf8_lossy(&output.stdout).contains("do-not-emit"));
}

#[cfg(unix)]
#[test]
fn symlinked_source_is_rejected_without_reading_its_target() {
    let project = Project::new();
    fs::remove_file(project.0.join("vendor/engine/src/lib.rs")).unwrap();
    std::os::unix::fs::symlink("/etc/hosts", project.0.join("vendor/engine/src/lib.rs")).unwrap();
    let output = project.inspect(&[]);
    assert!(!output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["healthy"], false);
    assert!(report.get("fingerprint_sha256").is_none());
}
