use std::path::Path;

use anyhow::{Context, Result, bail};
use runifold_testkit::{EvaluationCase, EvaluationDataset, EvaluationReport};

pub(crate) async fn load_jsonl(
    path: &Path,
    name: &str,
    version: &str,
) -> Result<EvaluationDataset> {
    let content = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read dataset {}", path.display()))?;
    let mut cases = Vec::new();
    for (index, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let case = serde_json::from_str::<EvaluationCase>(line)
            .with_context(|| format!("invalid dataset JSONL at line {}", index + 1))?;
        cases.push(case);
    }
    EvaluationDataset::new(name, version, cases).context("dataset invariants failed")
}

pub(crate) async fn load_report(path: &Path) -> Result<EvaluationReport> {
    let content = tokio::fs::read(path)
        .await
        .with_context(|| format!("failed to read report {}", path.display()))?;
    let report = serde_json::from_slice::<EvaluationReport>(&content)
        .with_context(|| format!("invalid report JSON {}", path.display()))?;
    report
        .validate()
        .with_context(|| format!("report invariants failed for {}", path.display()))?;
    Ok(report)
}

pub(crate) async fn write(path: &Path, content: &[u8]) -> Result<()> {
    let Some(parent) = path.parent() else {
        bail!("output path {} has no parent", path.display());
    };
    if !parent.as_os_str().is_empty() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    tokio::fs::write(path, content)
        .await
        .with_context(|| format!("failed to write {}", path.display()))
}
