//! Versioned task context and evidence-based, offline Cargo diagnostics.

use std::{collections::BTreeMap, path::Path, process::Command};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const KNOWLEDGE: &str = include_str!("ai-knowledge.json");

#[derive(Debug, Deserialize)]
struct Knowledge {
    manifest: Value,
    documents: BTreeMap<String, String>,
    api_index: Value,
}

fn knowledge() -> Result<Knowledge> {
    serde_json::from_str(KNOWLEDGE).context("bundled AI knowledge is invalid")
}

pub(crate) fn context(task: Option<&str>, json: bool) -> Result<()> {
    let knowledge = knowledge()?;
    let version = knowledge.manifest["version"]
        .as_str()
        .context("bundled knowledge has no version")?;
    let base = format!("https://github.com/ZYX121212/runifold/blob/v{version}");
    if let Some(task) = task {
        let Some(document) = knowledge.documents.get(task) else {
            if json {
                super::print_json(
                    &serde_json::json!({"schema_version": 1, "healthy": false, "code": "RF-AI-TASK-001", "available_tasks": knowledge.documents.keys().collect::<Vec<_>>(), "recommended_action": "Choose a task from runifold ai context."}),
                )?;
            }
            bail!(
                "unknown task `{task}`; choose one of: {}",
                knowledge
                    .documents
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        };
        let recipe = &knowledge.manifest["recipes"][task];
        if json {
            super::print_json(&serde_json::json!({
                "schema_version": 1,
                "knowledge_version": version,
                "task": task,
                "recipe": recipe,
                "document": document.replace("../../../", &format!("{base}/")),
                "source_base_url": base,
                "recommended_api": knowledge.manifest["recommended_api"],
                "laws": knowledge.manifest["laws"],
                "api_index": recipe["uses"].as_array().into_iter().flatten()
                    .filter_map(|symbol| symbol.as_str().and_then(|name| knowledge.api_index.get(name).map(|entry| (name, entry))))
                    .collect::<BTreeMap<_, _>>(),
            }))?;
        } else {
            println!("{}", document.replace("../../../", &format!("{base}/")));
        }
    } else if json {
        let recipes = knowledge.manifest["recipes"]
            .as_object()
            .context("missing recipes")?;
        let task_index = recipes.iter().map(|(id, recipe)| (id, serde_json::json!({
            "title": recipe["title"], "audience": recipe["audience"], "api": recipe["api"],
        }))).collect::<BTreeMap<_, _>>();
        super::print_json(&serde_json::json!({
            "schema_version": 1, "knowledge_version": version,
            "canonical_paths": knowledge.manifest["canonical_paths"],
            "recommended_api": knowledge.manifest["recommended_api"],
            "architecture": knowledge.manifest["architecture"],
            "recipes": task_index,
            "manifest_path": "runifold.ai.json",
            "source_base_url": base,
            "next": ["runifold ai recipe <task> --json", "runifold ai explain <code> --json", "runifold ai check --json"],
        }))?;
    } else {
        println!("Runifold {version} AI context\nAvailable tasks:");
        for (task, _) in knowledge.documents {
            println!("  {task}");
        }
        println!("\nUse: runifold context --task <task> [--json]");
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
struct Metadata {
    packages: Vec<Package>,
}

#[derive(Debug, Deserialize)]
struct Package {
    name: String,
    version: String,
    dependencies: Vec<Dependency>,
}

#[derive(Debug, Deserialize)]
struct Dependency {
    name: String,
    rename: Option<String>,
    req: String,
    kind: Option<String>,
    optional: bool,
    uses_default_features: bool,
    features: Vec<String>,
    target: Option<String>,
}

#[derive(Debug, Serialize)]
struct Declaration {
    package: String,
    dependency: String,
    import_name: String,
    version_requirement: String,
    optional: bool,
    default_features: bool,
    declared_features: Vec<String>,
    target: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum Severity {
    Error,
    Warning,
}

#[derive(Debug, Serialize)]
struct Issue {
    code: &'static str,
    severity: Severity,
    message: String,
    evidence: String,
    recommended_action: String,
}

#[derive(Debug, Serialize)]
struct Report {
    schema_version: u32,
    mode: &'static str,
    knowledge_version: String,
    healthy: bool,
    checks: Vec<&'static str>,
    project_packages: Vec<String>,
    declarations: Vec<Declaration>,
    issues: Vec<Issue>,
    recommended_actions: Vec<String>,
    recommended_api: Value,
    relevant_docs: BTreeMap<String, String>,
    validation_commands: Vec<&'static str>,
    not_checked: Vec<&'static str>,
}

impl Report {
    fn issue(
        &mut self,
        code: &'static str,
        severity: Severity,
        message: impl Into<String>,
        evidence: impl Into<String>,
        action: impl Into<String>,
    ) {
        self.healthy &= !matches!(severity, Severity::Error);
        let action = action.into();
        if !self.recommended_actions.contains(&action) {
            self.recommended_actions.push(action.clone());
        }
        self.issues.push(Issue {
            code,
            severity,
            message: message.into(),
            evidence: evidence.into(),
            recommended_action: action,
        });
    }
}

/// Returns false for failed checks after printing the complete report.
pub(crate) fn doctor(manifest_path: &Path, json: bool) -> Result<bool> {
    let knowledge = knowledge()?;
    let mut report = Report {
        schema_version: 1,
        mode: "ai_development",
        knowledge_version: env!("CARGO_PKG_VERSION").into(),
        healthy: true,
        checks: Vec::new(),
        project_packages: Vec::new(),
        declarations: Vec::new(),
        issues: Vec::new(),
        recommended_actions: Vec::new(),
        recommended_api: knowledge.manifest["recommended_api"].clone(),
        relevant_docs: BTreeMap::new(),
        validation_commands: vec![
            "cargo check --locked",
            "cargo test --locked",
            "cargo clippy --all-targets --locked -- -D warnings",
        ],
        not_checked: vec![
            "Dependency requirements are declarations, not resolved Cargo.lock versions.",
            "Declared features are not resolved features; optional and target dependencies may be inactive.",
            "Runtime Tool registration, authority, effect classes and store injection are not inspected.",
            "External recovery safety and live provider capabilities are not checked.",
            "Suggested validation commands run in the selected project directory and are not executed.",
        ],
    };
    for (task, recipe) in knowledge.manifest["recipes"]
        .as_object()
        .context("bundled recipe index is invalid")?
    {
        if let Some(path) = recipe["path"].as_str() {
            report.relevant_docs.insert(
                task.clone(),
                format!(
                    "https://github.com/ZYX121212/runifold/blob/v{}/{path}",
                    report.knowledge_version
                ),
            );
        }
    }
    match cargo_metadata(manifest_path) {
        Ok(metadata) => inspect(metadata, &knowledge, &mut report),
        Err(error) => report.issue(
            "RF-AI-CARGO-001",
            Severity::Error,
            "Cannot inspect the selected Cargo project.",
            error.to_string(),
            "Verify --manifest-path and Cargo installation; run cargo metadata --offline --locked --no-deps --format-version 1 in the selected project to inspect Cargo diagnostics.",
        ),
    }
    if json {
        super::print_json(&report)?;
    } else {
        println!("Runifold {} AI doctor", report.knowledge_version);
        println!("Checks passed: {}", report.healthy);
        println!("Project packages: {}", report.project_packages.join(", "));
        for declaration in &report.declarations {
            println!(
                "  {} -> {} {} (declared features: {}; defaults: {}; optional: {})",
                declaration.package,
                declaration.dependency,
                declaration.version_requirement,
                declaration.declared_features.join(", "),
                declaration.default_features,
                declaration.optional,
            );
        }
        println!("Recommended API: ProviderRuntime -> AgentBuilder -> Agent");
        for issue in &report.issues {
            println!(
                "{}: {}\n  {}",
                issue.code, issue.message, issue.recommended_action
            );
        }
        println!("\nValidation (run in selected project):");
        for command in &report.validation_commands {
            println!("  {command}");
        }
        println!("\nRelevant docs:");
        for (task, url) in &report.relevant_docs {
            println!("  {task}: {url}");
        }
        println!("\nNot checked:");
        for limitation in &report.not_checked {
            println!("  {limitation}");
        }
    }
    Ok(report.healthy)
}

fn cargo_metadata(path: &Path) -> Result<Metadata> {
    if !path.is_file() {
        bail!("the selected manifest is not a file");
    }
    let output = Command::new("cargo")
        .args([
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--offline",
            "--locked",
            "--manifest-path",
        ])
        .arg(path)
        .output()
        .context("could not start cargo metadata")?;
    if !output.status.success() {
        // Cargo stderr may include private registry URLs or application paths.
        // Keep the machine report value-free; give an explicit reproduction command.
        bail!("cargo metadata did not succeed; no project configuration was inferred");
    }
    serde_json::from_slice(&output.stdout).context("cargo metadata returned an invalid response")
}

fn inspect(metadata: Metadata, knowledge: &Knowledge, report: &mut Report) {
    report.checks.push("cargo_metadata_offline_locked");
    let mut found = false;
    for package in metadata.packages {
        report.project_packages.push(package.name.clone());
        if knowledge.manifest["crates"].get(&package.name).is_some() {
            found = true;
            check_version(&package.name, &package.version, report);
        }
        for dependency in package
            .dependencies
            .into_iter()
            .filter(|dep| dep.kind.is_none())
        {
            if dependency.name != "runifold" && !dependency.name.starts_with("runifold-") {
                continue;
            }
            found = true;
            check_version(&dependency.name, &dependency.req, report);
            if let Some(features) =
                knowledge.manifest["crates"][&dependency.name]["features"].as_object()
                && same_release_family(&dependency.req, &report.knowledge_version) == Some(true)
            {
                for feature in &dependency.features {
                    if !features.contains_key(feature) && !feature.contains('/') {
                        report.issue(
                            "RF-AI-FEATURE-001",
                            Severity::Warning,
                            format!("Feature `{feature}` is absent from the bundled {} feature map.", dependency.name),
                            format!("{} declares {} with {feature}", package.name, dependency.name),
                            "Check the actual dependency version/source and its Cargo features; the local knowledge may predate this feature.",
                        );
                    }
                }
            }
            report.declarations.push(Declaration {
                package: package.name.clone(),
                import_name: dependency.rename.unwrap_or_else(|| dependency.name.clone()),
                dependency: dependency.name,
                version_requirement: dependency.req,
                optional: dependency.optional,
                default_features: dependency.uses_default_features,
                declared_features: dependency.features,
                target: dependency.target,
            });
        }
    }
    report.project_packages.sort();
    report.declarations.sort_by(|left, right| {
        (&left.package, &left.dependency, &left.target).cmp(&(
            &right.package,
            &right.dependency,
            &right.target,
        ))
    });
    report.checks.push("runifold_declarations");
    if !found {
        report.issue(
            "RF-AI-CARGO-002",
            Severity::Error,
            "No Runifold workspace package or normal dependency declaration was detected.",
            "Inspected workspace packages and their normal dependencies; dev/build-only dependencies do not establish an application integration.",
            "Select the application's Cargo.toml or add the Runifold dependency required by your task recipe.",
        );
    }
}

fn check_version(name: &str, requirement: &str, report: &mut Report) {
    if same_release_family(requirement, &report.knowledge_version) == Some(false) {
        report.issue(
            "RF-AI-VERSION-001",
            Severity::Warning,
            format!("{name} does not target the bundled knowledge's release family."),
            format!("requirement: {requirement}; knowledge: {}", report.knowledge_version),
            "Use documentation and a CLI matching the application's Runifold release; do not silently upgrade the application.",
        );
    }
}

// Only compare unambiguous numeric requirements (^, ~, = or bare). Ranges,
// wildcards and prereleases remain unknown rather than guessed compatible.
fn same_release_family(requirement: &str, version: &str) -> Option<bool> {
    fn family(value: &str) -> Option<(u64, u64)> {
        let value = value.trim().trim_start_matches(['^', '~', '=']).trim();
        let pieces = value.split('.').collect::<Vec<_>>();
        if !(2..=3).contains(&pieces.len())
            || pieces
                .iter()
                .any(|part| part.is_empty() || !part.chars().all(|c| c.is_ascii_digit()))
        {
            return None;
        }
        Some((pieces[0].parse().ok()?, pieces[1].parse().ok()?))
    }
    Some(family(requirement)? == family(version)?)
}

pub(crate) fn explain(code: &str, json: bool) -> Result<()> {
    let knowledge = knowledge()?;
    let diagnostics = knowledge.manifest["diagnostics"]
        .as_array()
        .context("missing diagnostics")?;
    let diagnostic = diagnostics.iter().find(|item| {
        item["code"].as_str() == Some(code)
            || item["aliases"]
                .as_array()
                .is_some_and(|aliases| aliases.iter().any(|alias| alias.as_str() == Some(code)))
    });
    let Some(diagnostic) = diagnostic else {
        if json {
            super::print_json(
                &serde_json::json!({"schema_version": 1, "healthy": false, "code": "RF-AI-DIAGNOSTIC-001", "requested_code": code, "recommended_action": "Use a documented code from the matching CLI knowledge version."}),
            )?;
        }
        bail!("unknown diagnostic `{code}`");
    };
    if json {
        super::print_json(
            &serde_json::json!({"schema_version": 1, "knowledge_version": knowledge.manifest["version"], "diagnostic": diagnostic}),
        )?;
    } else {
        println!(
            "{}: {}",
            diagnostic["code"].as_str().unwrap_or(code),
            diagnostic["title"].as_str().unwrap_or("Diagnostic")
        );
        println!("{}", diagnostic["meaning"].as_str().unwrap_or(""));
        println!(
            "Canonical fix: {}",
            diagnostic["canonical_fix"].as_str().unwrap_or("")
        );
        println!(
            "Recipe: runifold ai recipe {}",
            diagnostic["recipe"].as_str().unwrap_or("")
        );
        println!("Causes: {}", diagnostic["causes"]);
        println!("Validation: {}", diagnostic["validation"]);
    }
    Ok(())
}

pub(crate) fn check(root: &Path, base: &str, execute: bool, json: bool) -> Result<bool> {
    let mut command = Command::new("python3");
    command
        .args(["-c", include_str!("ai-check.py"), "--root"])
        .arg(root)
        .args(["--base", base]);
    if execute {
        command.arg("--execute");
    }
    if json {
        command.arg("--json");
    }
    Ok(command
        .status()
        .context("ai check requires Python 3.11+ and Git")?
        .success())
}

#[cfg(test)]
mod tests {
    use super::same_release_family;

    #[test]
    fn version_ranges_and_prereleases_are_not_guessed() {
        assert_eq!(same_release_family("^0.10.0", "0.10.0"), Some(true));
        assert_eq!(same_release_family("=0.9.1", "0.10.0"), Some(false));
        for requirement in [">=0.9, <0.11", "*", "0.10.*", "0.10.0-beta", "0.10.1.2"] {
            assert_eq!(same_release_family(requirement, "0.10.0"), None);
        }
    }
}
