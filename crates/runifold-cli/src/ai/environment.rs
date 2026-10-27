//! Offline, content-based identity for the selected Cargo dependency graph.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_FILES: usize = 100_000;
const MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;

pub(crate) struct EnvironmentOptions {
    pub(crate) manifest_path: PathBuf,
    pub(crate) features: Vec<String>,
    pub(crate) all_features: bool,
    pub(crate) no_default_features: bool,
    pub(crate) target: Option<String>,
}

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    resolve: Option<Resolution>,
    workspace_root: PathBuf,
}

#[derive(Deserialize)]
struct Package {
    id: String,
    name: String,
    version: String,
    source: Option<String>,
    manifest_path: PathBuf,
}

#[derive(Deserialize)]
struct Resolution {
    nodes: Vec<Node>,
}

#[derive(Deserialize)]
struct Node {
    id: String,
    features: Vec<String>,
    deps: Vec<Dependency>,
}

#[derive(Deserialize)]
struct Dependency {
    name: String,
    pkg: String,
}

#[derive(Serialize)]
struct PackageIdentity {
    name: String,
    version: String,
    source_kind: &'static str,
    source_identity_sha256: Option<String>,
    registry_checksum: Option<String>,
    content_sha256: String,
    source_files: usize,
    file_selection: &'static str,
    resolved_features: Vec<String>,
}

#[derive(Serialize)]
struct ResolvedPackage {
    identity_sha256: String,
    #[serde(flatten)]
    identity: PackageIdentity,
    dependencies: Vec<ResolvedDependency>,
}

#[derive(Serialize, PartialEq, Eq, PartialOrd, Ord)]
struct ResolvedDependency {
    import_name: String,
    identity_sha256: String,
}

#[derive(Serialize)]
struct EnvironmentSnapshot {
    schema_version: u32,
    mode: &'static str,
    cli_version: &'static str,
    fingerprint_sha256: String,
    lock_sha256: String,
    workspace_manifest_sha256: String,
    rustc_version: String,
    compiler_host: String,
    compiler_details_sha256: String,
    requested_features: Vec<String>,
    all_features: bool,
    no_default_features: bool,
    target_filter: Option<String>,
    packages: Vec<ResolvedPackage>,
    not_established: Vec<&'static str>,
}

pub(crate) fn environment(options: &EnvironmentOptions, json: bool) -> Result<bool> {
    match capture(options) {
        Ok(report) => {
            if json {
                super::super::print_json(&report)?;
            } else {
                println!(
                    "Resolved environment SHA-256: {}",
                    report.fingerprint_sha256
                );
                println!(
                    "{} packages; offline, locked, source contents verified",
                    report.packages.len()
                );
                println!(
                    "This describes the selected Cargo graph, not a running binary or remote service."
                );
            }
            Ok(true)
        }
        Err(error) => {
            // Only controlled outer messages, never Cargo stderr, raw source
            // identities, TOML snippets, credential values or filesystem errors.
            super::super::print_json(&serde_json::json!({
                "schema_version": 1, "mode": "resolved_build_environment", "healthy": false,
                "code": "RF-AI-ENVIRONMENT-001", "message": error.to_string(),
                "recommended_action": "Use an existing locked Cargo project with cached dependencies, readable source files and no source changes during inspection. Resolve dependencies separately if offline metadata is unavailable."
            }))?;
            Ok(false)
        }
    }
}

fn metadata(options: &EnvironmentOptions) -> Result<Metadata> {
    if options.target.as_ref().is_some_and(|target| {
        target.is_empty()
            || target.len() > 128
            || !target
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    }) {
        bail!(
            "target filter must be a bounded platform identifier; custom target paths are not supported"
        );
    }
    let mut command = Command::new("cargo");
    command
        .args([
            "metadata",
            "--offline",
            "--locked",
            "--format-version",
            "1",
            "--manifest-path",
        ])
        .arg(&options.manifest_path);
    if options.all_features {
        command.arg("--all-features");
    }
    if options.no_default_features {
        command.arg("--no-default-features");
    }
    if !options.features.is_empty() {
        command.arg("--features").arg(options.features.join(","));
    }
    if let Some(target) = &options.target {
        command.arg("--filter-platform").arg(target);
    }
    let output = command
        .output()
        .context("cannot start offline Cargo metadata")?;
    if !output.status.success() {
        bail!("offline locked Cargo resolution failed; no environment was inferred");
    }
    serde_json::from_slice(&output.stdout).context("Cargo metadata was not a supported JSON graph")
}

fn capture(options: &EnvironmentOptions) -> Result<EnvironmentSnapshot> {
    let metadata = metadata(options)?;
    let lock_path = metadata.workspace_root.join("Cargo.lock");
    let manifest_path = metadata.workspace_root.join("Cargo.toml");
    let lock_bytes = fs::read(&lock_path).context("a readable Cargo.lock is required")?;
    let manifest_bytes = fs::read(&manifest_path).context("cannot read workspace manifest")?;
    let resolution = metadata
        .resolve
        .context("Cargo did not provide a resolved graph")?;
    if resolution.nodes.len() > 4096 {
        bail!("resolved graph exceeds the 4096-package inspection bound");
    }
    let nodes: BTreeMap<_, _> = resolution
        .nodes
        .into_iter()
        .map(|node| (node.id.clone(), node))
        .collect();
    let ResolvedGraph { packages, roots } = package_graph(metadata.packages, &nodes)?;
    // Re-read all selected bytes and file sets so a concurrent edit cannot be
    // accepted as a coherent manifest, even in an uncommitted vendor tree.
    let mut verification_budget = SourceBudget::default();
    for (root, expected) in roots {
        if source_identity(&root, &mut verification_budget)?.0 != expected {
            bail!("package source changed during inspection");
        }
    }
    if fs::read(&lock_path).context("cannot recheck Cargo.lock")? != lock_bytes
        || fs::read(&manifest_path).context("cannot recheck workspace manifest")? != manifest_bytes
    {
        bail!("workspace resolution inputs changed during inspection");
    }
    let compiler = compiler_identity()?;
    let mut requested_features = options.features.clone();
    requested_features.sort();
    requested_features.dedup();
    let mut report = EnvironmentSnapshot {
        schema_version: 1,
        mode: "resolved_build_environment",
        cli_version: env!("CARGO_PKG_VERSION"),
        fingerprint_sha256: String::new(),
        lock_sha256: digest(&lock_bytes),
        workspace_manifest_sha256: digest(&manifest_bytes),
        rustc_version: compiler.version,
        compiler_host: compiler.host,
        compiler_details_sha256: compiler.details_sha256,
        requested_features,
        all_features: options.all_features,
        no_default_features: options.no_default_features,
        target_filter: options.target.clone(),
        packages,
        not_established: vec![
            "Cargo metadata features describe its resolved graph, not proof of features linked into an existing binary.",
            "Source identities and lock contents are fingerprints; raw registry/Git URLs, local paths and source contents are excluded from output.",
            "Ignored local files, external build-script inputs, environment flags, Cargo configuration and runtime policy are not captured.",
            "This is not a runtime configuration snapshot, recovery contract, vendor-service validation or release acceptance.",
        ],
    };
    report.fingerprint_sha256 =
        digest(&serde_json::to_vec(&report).context("cannot encode environment snapshot")?);
    Ok(report)
}

struct CompilerIdentity {
    version: String,
    host: String,
    details_sha256: String,
}

fn compiler_identity() -> Result<CompilerIdentity> {
    let output = Command::new("rustc")
        .args(["--version", "--verbose"])
        .output()
        .context("cannot inspect Rust compiler")?;
    if !output.status.success() || output.stdout.len() > 4096 {
        bail!("Rust compiler version inspection failed");
    }
    let details =
        std::str::from_utf8(&output.stdout).context("Rust compiler version is not UTF-8")?;
    let version = details
        .lines()
        .next()
        .filter(|line| line.starts_with("rustc ") && line.len() <= 256)
        .context("Rust compiler version response is invalid")?;
    let host = details
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .filter(|host| {
            !host.is_empty()
                && host.len() <= 128
                && host
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
        .context("Rust compiler host response is invalid")?;
    Ok(CompilerIdentity {
        version: version.into(),
        host: host.into(),
        details_sha256: digest(&output.stdout),
    })
}

struct ResolvedGraph {
    packages: Vec<ResolvedPackage>,
    roots: Vec<(PathBuf, String)>,
}

fn package_graph(inputs: Vec<Package>, nodes: &BTreeMap<String, Node>) -> Result<ResolvedGraph> {
    let mut packages = Vec::new();
    let mut identities = BTreeMap::new();
    let mut roots = Vec::new();
    let mut budget = SourceBudget::default();
    for package in inputs {
        let Some(node) = nodes.get(&package.id) else {
            continue;
        };
        let root = package
            .manifest_path
            .parent()
            .context("package manifest has no directory")?
            .to_path_buf();
        let (content_sha256, source_files, file_selection) = source_identity(&root, &mut budget)?;
        let mut features = node.features.clone();
        features.sort();
        features.dedup();
        let identity = PackageIdentity {
            name: package.name.clone(),
            version: package.version.clone(),
            source_kind: package.source.as_deref().map_or("path", |source| {
                if source.starts_with("git+") {
                    "git"
                } else {
                    "registry"
                }
            }),
            source_identity_sha256: package
                .source
                .as_ref()
                .map(|source| digest(source.as_bytes())),
            registry_checksum: registry_checksum(&root)?,
            content_sha256,
            source_files,
            file_selection,
            resolved_features: features,
        };
        let identity_sha256 =
            digest(&serde_json::to_vec(&identity).context("cannot encode package identity")?);
        identities.insert(package.id.clone(), identity_sha256.clone());
        roots.push((root, identity.content_sha256.clone()));
        packages.push((
            package.id,
            ResolvedPackage {
                identity_sha256,
                identity,
                dependencies: Vec::new(),
            },
        ));
    }
    for (id, package) in &mut packages {
        let node = nodes.get(id).context("resolved package node is missing")?;
        for dependency in &node.deps {
            package.dependencies.push(ResolvedDependency {
                import_name: dependency.name.clone(),
                identity_sha256: identities
                    .get(&dependency.pkg)
                    .context("resolved dependency identity is missing")?
                    .clone(),
            });
        }
        package.dependencies.sort();
        package.dependencies.dedup();
    }
    let mut packages: Vec<_> = packages.into_iter().map(|(_, package)| package).collect();
    packages.sort_by(|left, right| left.identity_sha256.cmp(&right.identity_sha256));
    Ok(ResolvedGraph { packages, roots })
}

fn registry_checksum(root: &Path) -> Result<Option<String>> {
    let path = root.join(".cargo-checksum.json");
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path).context("cannot read package checksum provenance")?;
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).context("package checksum provenance is invalid")?;
    Ok(value
        .get("package")
        .and_then(serde_json::Value::as_str)
        .filter(|checksum| {
            checksum.len() == 64 && checksum.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
        .map(str::to_owned))
}

#[derive(Default)]
struct SourceBudget {
    files: usize,
    bytes: u64,
}

fn digest(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    bytes
        .iter()
        .flat_map(|byte| {
            [
                char::from(DIGITS[usize::from(byte >> 4)]),
                char::from(DIGITS[usize::from(byte & 15)]),
            ]
        })
        .collect()
}

fn source_identity(
    root: &Path,
    budget: &mut SourceBudget,
) -> Result<(String, usize, &'static str)> {
    let (files, selection) = selected_files(root)?;
    let count = files.len();
    if count > MAX_FILES {
        bail!("source file selection exceeds inspection bounds");
    }
    let mut hash = Sha256::new();
    for relative in files {
        budget.files += 1;
        if budget.files > MAX_FILES {
            bail!("source file count exceeds inspection bounds");
        }
        validate_relative(&relative)?;
        let path = root.join(&relative);
        ensure_source_path(root, &path)?;
        let before =
            fs::symlink_metadata(&path).context("cannot inspect a selected source file")?;
        if !before.is_file() {
            bail!("selected source contains a symlink or non-regular file");
        }
        budget.bytes = budget
            .bytes
            .checked_add(before.len())
            .context("source byte count overflow")?;
        if budget.bytes > MAX_BYTES {
            bail!("source bytes exceed the 2 GiB inspection bound");
        }
        let mut file = File::open(&path).context("cannot read a selected source file")?;
        let mut content_hash = Sha256::new();
        let mut buffer = [0_u8; 16 * 1024];
        let mut read = 0_u64;
        loop {
            let size = file
                .read(&mut buffer)
                .context("cannot hash a selected source file")?;
            if size == 0 {
                break;
            }
            read += u64::try_from(size).context("source read size overflow")?;
            if read > before.len() {
                bail!("source file grew during inspection");
            }
            content_hash.update(&buffer[..size]);
        }
        let after = fs::symlink_metadata(&path).context("cannot recheck a selected source file")?;
        if read != before.len()
            || after.len() != before.len()
            || after.modified().ok() != before.modified().ok()
        {
            bail!("source file changed during inspection");
        }
        let name = relative
            .iter()
            .map(|part| part.to_str().context("source file names must be UTF-8"))
            .collect::<Result<Vec<_>>>()?
            .join("/");
        hash.update(
            u64::try_from(name.len())
                .context("source name length overflow")?
                .to_le_bytes(),
        );
        hash.update(name.as_bytes());
        hash.update(before.len().to_le_bytes());
        hash.update(content_hash.finalize());
    }
    Ok((hex(&hash.finalize()), count, selection))
}

fn validate_relative(path: &Path) -> Result<()> {
    if path.is_absolute()
        || path.components().any(|part| {
            !matches!(
                part,
                std::path::Component::Normal(_) | std::path::Component::CurDir
            )
        })
    {
        bail!("source file selection contains an unsafe relative path");
    }
    Ok(())
}

fn selected_files(root: &Path) -> Result<(BTreeSet<PathBuf>, &'static str)> {
    let mut files = BTreeSet::new();
    let checksum_manifest = root.join(".cargo-checksum.json");
    let selection = if checksum_manifest.is_file() {
        // Also walk added files: trusting only the vendor checksum list would
        // miss a new source file consumed by an unchanged build script.
        collect_files(root, root, &mut files)?;
        "package_tree"
    } else {
        let output = Command::new("git")
            .arg("-C")
            .arg(root)
            .args([
                "ls-files",
                "--cached",
                "--others",
                "--exclude-standard",
                "-z",
                "--",
                ".",
            ])
            .output()
            .context("cannot inspect local source selection")?;
        if output.status.success() && !output.stdout.is_empty() {
            let names =
                std::str::from_utf8(&output.stdout).context("source file names must be UTF-8")?;
            for name in names.split('\0').filter(|name| !name.is_empty()) {
                let relative = PathBuf::from(name);
                validate_relative(&relative)?;
                if private_source_file(&relative) {
                    continue;
                }
                match fs::symlink_metadata(root.join(&relative)) {
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(_) => bail!("cannot inspect a selected source entry"),
                }
                files.insert(relative);
            }
            "git_tracked_and_nonignored"
        } else {
            collect_files(root, root, &mut files)?;
            "package_tree"
        }
    };
    files.insert(PathBuf::from("Cargo.toml"));
    Ok((files, selection))
}

fn private_source_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            matches!(name, ".env" | "credentials.toml") || name.starts_with(".env.")
        })
}

fn ensure_source_path(root: &Path, path: &Path) -> Result<()> {
    let root = root
        .canonicalize()
        .context("cannot resolve package source root")?;
    let resolved = path
        .canonicalize()
        .context("cannot resolve selected source file")?;
    if !resolved.starts_with(root) {
        bail!("source file resolves outside its package");
    }
    Ok(())
}

fn collect_files(root: &Path, directory: &Path, files: &mut BTreeSet<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(directory).context("cannot list package source files")? {
        let entry = entry.context("cannot list a package source entry")?;
        let name = entry.file_name();
        if matches!(
            name.to_str(),
            Some("target" | ".git" | "node_modules" | ".env" | "credentials.toml")
        ) || name.to_str().is_some_and(|name| name.starts_with(".env."))
        {
            continue;
        }
        let kind = entry
            .file_type()
            .context("cannot inspect a package source entry")?;
        if kind.is_symlink() {
            bail!("package source contains a symlink; resolve it before fingerprinting");
        }
        if kind.is_dir() {
            collect_files(root, &entry.path(), files)?;
        } else if kind.is_file() {
            files.insert(
                entry
                    .path()
                    .strip_prefix(root)
                    .context("source entry escaped its package")?
                    .to_path_buf(),
            );
            if files.len() > MAX_FILES {
                bail!("source file selection exceeds inspection bounds");
            }
        } else {
            bail!("package source contains a non-regular file");
        }
    }
    Ok(())
}
