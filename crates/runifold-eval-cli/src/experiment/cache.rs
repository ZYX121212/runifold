use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, ensure};
use runifold_core::RunId;
use runifold_testkit::{EvaluationDataset, EvaluationReport};
use serde::Serialize;

use super::{EXPERIMENT_SCHEMA_VERSION, ExperimentScorer, Shard};
use crate::dataset;

#[derive(Serialize)]
struct CacheFingerprint<'a> {
    schema_version: u32,
    dataset: &'a EvaluationDataset,
    candidate_version: &'a str,
    base_seed: u64,
    shard: Option<Shard>,
    scorer: &'a ExperimentScorer,
    command: &'a [String],
    timeout_ms: u64,
    max_output_bytes: usize,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn fingerprint(
    dataset: &EvaluationDataset,
    candidate_version: &str,
    base_seed: u64,
    shard: Option<Shard>,
    scorer: &ExperimentScorer,
    command: &[OsString],
    timeout_ms: u64,
    max_output_bytes: usize,
) -> Result<String> {
    let command = utf8_command(command)?;
    let value = serde_json::to_vec(&CacheFingerprint {
        schema_version: EXPERIMENT_SCHEMA_VERSION,
        dataset,
        candidate_version,
        base_seed,
        shard,
        scorer,
        command: &command,
        timeout_ms,
        max_output_bytes,
    })
    .context("failed to serialize experiment cache identity")?;
    Ok(blake3::hash(&value).to_hex().to_string())
}

pub(super) async fn load_sample(
    root: &Path,
    fingerprint: &str,
    sample_index: usize,
    dataset: &EvaluationDataset,
    candidate_version: &str,
) -> Result<Option<EvaluationReport>> {
    load_validated(
        &sample_path(root, fingerprint, sample_index),
        dataset,
        candidate_version,
    )
    .await
}

pub(super) async fn store_sample(
    root: &Path,
    fingerprint: &str,
    sample_index: usize,
    report: &EvaluationReport,
) -> Result<()> {
    store_atomic(
        &sample_path(root, fingerprint, sample_index),
        sample_index,
        report,
    )
    .await
}

pub(super) async fn load_case(
    root: &Path,
    fingerprint: &str,
    sample_index: usize,
    dataset: &EvaluationDataset,
    candidate_version: &str,
) -> Result<Option<EvaluationReport>> {
    let path = case_path(root, fingerprint, sample_index, dataset)?;
    load_validated(&path, dataset, candidate_version).await
}

pub(super) async fn store_case(
    root: &Path,
    fingerprint: &str,
    sample_index: usize,
    dataset: &EvaluationDataset,
    report: &EvaluationReport,
) -> Result<()> {
    let path = case_path(root, fingerprint, sample_index, dataset)?;
    store_atomic(&path, sample_index, report).await
}

async fn load_validated(
    path: &Path,
    dataset: &EvaluationDataset,
    candidate_version: &str,
) -> Result<Option<EvaluationReport>> {
    match tokio::fs::read(path).await {
        Ok(bytes) => {
            let report = serde_json::from_slice::<EvaluationReport>(&bytes)
                .with_context(|| format!("invalid cached evaluation {}", path.display()))?;
            report.validate().with_context(|| {
                format!("cached evaluation invariants failed {}", path.display())
            })?;
            ensure!(
                report.dataset_name == dataset.name()
                    && report.dataset_version == dataset.version()
                    && report.candidate_version == candidate_version,
                "cached evaluation identity mismatch at {}",
                path.display()
            );
            let expected = dataset
                .cases()
                .iter()
                .map(|case| case.id().as_str())
                .collect::<std::collections::BTreeSet<_>>();
            let actual = report
                .cases
                .iter()
                .map(|case| case.case_id.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            ensure!(
                actual == expected,
                "cached evaluation cases mismatch at {}",
                path.display()
            );
            Ok(Some(report))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => {
            Err(error).with_context(|| format!("failed to read cache {}", path.display()))
        }
    }
}

async fn store_atomic(path: &Path, sample_index: usize, report: &EvaluationReport) -> Result<()> {
    let parent = path.parent().context("cache path has no parent")?;
    tokio::fs::create_dir_all(parent)
        .await
        .with_context(|| format!("failed to create cache directory {}", parent.display()))?;
    let temporary = parent.join(format!(".checkpoint-{sample_index}-{}.tmp", RunId::new()));
    dataset::write(&temporary, report.to_json_pretty()?.as_bytes()).await?;
    tokio::fs::rename(&temporary, path)
        .await
        .with_context(|| format!("failed to commit cache {}", path.display()))
}

fn sample_path(root: &Path, fingerprint: &str, sample_index: usize) -> PathBuf {
    root.join(fingerprint)
        .join(format!("sample-{sample_index}.json"))
}

fn case_path(
    root: &Path,
    fingerprint: &str,
    sample_index: usize,
    dataset: &EvaluationDataset,
) -> Result<PathBuf> {
    ensure!(
        dataset.cases().len() == 1,
        "case cache requires exactly one dataset case"
    );
    let case_id = dataset
        .cases()
        .first()
        .map_or("", |case| case.id().as_str());
    let digest = blake3::hash(case_id.as_bytes()).to_hex();
    Ok(root
        .join(fingerprint)
        .join(format!("sample-{sample_index}"))
        .join(format!("case-{digest}.json")))
}

fn utf8_command(command: &[OsString]) -> Result<Vec<String>> {
    command
        .iter()
        .map(|part| {
            part.to_str()
                .map(str::to_owned)
                .context("experiment Candidate command must be valid UTF-8")
        })
        .collect()
}
