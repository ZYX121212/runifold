//! CLI and CI quality gates for Runifold evaluations.

mod candidate;
mod dataset;
mod experiment;
mod render;

use std::{ffi::OsString, num::NonZeroUsize, path::PathBuf, process::ExitCode, time::Duration};

use anyhow::{Context, Result};
use candidate::ProcessCandidate;
use clap::{Args, Parser, Subcommand, ValueEnum};
use runifold_testkit::{
    EvaluationReport, EvaluationRunner, JsonExactMatchScorer, RegressionComparison,
    RegressionPolicy, TokenOverlapScorer,
};

const QUALITY_GATE_FAILURE: u8 = 2;

#[derive(Debug, Parser)]
#[command(name = "runifold-eval", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Executes a Candidate command for every JSONL case.
    Run(RunArgs),
    /// Compares existing candidate and baseline reports.
    Compare(CompareArgs),
    /// Repeats a seeded evaluation with checkpoints and confidence statistics.
    Experiment(experiment::ExperimentArgs),
    /// Merges every shard of one completed experiment.
    Merge(experiment::MergeArgs),
}

#[derive(Debug, Args)]
struct RunArgs {
    /// JSONL dataset path.
    #[arg(long)]
    dataset: PathBuf,
    /// Stable dataset name.
    #[arg(long)]
    dataset_name: String,
    /// Immutable dataset version.
    #[arg(long)]
    dataset_version: String,
    /// Candidate model, prompt, Agent, or application version.
    #[arg(long)]
    candidate_version: String,
    /// Output JSON report.
    #[arg(long)]
    output: PathBuf,
    /// Optional baseline JSON report.
    #[arg(long)]
    baseline: Option<PathBuf>,
    /// Optional `JUnit` XML output.
    #[arg(long)]
    junit: Option<PathBuf>,
    /// Optional Markdown summary output.
    #[arg(long)]
    markdown: Option<PathBuf>,
    /// Concurrent Candidate processes.
    #[arg(long, default_value = "4")]
    concurrency: NonZeroUsize,
    /// Per-case Candidate timeout in milliseconds.
    #[arg(long, default_value_t = 60_000)]
    timeout_ms: u64,
    /// Maximum Candidate stdout bytes per case.
    #[arg(long, default_value_t = 1_048_576)]
    max_output_bytes: usize,
    /// Built-in scorer.
    #[arg(long, value_enum, default_value_t = ScorerKind::Exact)]
    scorer: ScorerKind,
    /// Per-case threshold for the token-overlap scorer.
    #[arg(long, default_value_t = 1.0)]
    score_threshold: f64,
    /// Maximum mean-score drop from baseline.
    #[arg(long, default_value_t = 0.02)]
    max_mean_drop: f64,
    /// Maximum pass-rate drop from baseline.
    #[arg(long, default_value_t = 0.05)]
    max_pass_rate_drop: f64,
    /// Maximum execution-success drop from baseline.
    #[arg(long, default_value_t = 0.0)]
    max_execution_drop: f64,
    /// Candidate executable and arguments.
    #[arg(required = true, last = true)]
    candidate_command: Vec<OsString>,
}

#[derive(Debug, Args)]
struct CompareArgs {
    /// Candidate JSON report.
    #[arg(long)]
    candidate: PathBuf,
    /// Baseline JSON report.
    #[arg(long)]
    baseline: PathBuf,
    /// Optional `JUnit` XML output.
    #[arg(long)]
    junit: Option<PathBuf>,
    /// Optional Markdown summary output.
    #[arg(long)]
    markdown: Option<PathBuf>,
    /// Maximum mean-score drop from baseline.
    #[arg(long, default_value_t = 0.02)]
    max_mean_drop: f64,
    /// Maximum pass-rate drop from baseline.
    #[arg(long, default_value_t = 0.05)]
    max_pass_rate_drop: f64,
    /// Maximum execution-success drop from baseline.
    #[arg(long, default_value_t = 0.0)]
    max_execution_drop: f64,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum ScorerKind {
    Exact,
    TokenOverlap,
}

#[tokio::main]
async fn main() -> ExitCode {
    match execute(Cli::parse()).await {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(QUALITY_GATE_FAILURE),
        Err(error) => {
            eprintln!("{error:#}");
            ExitCode::FAILURE
        }
    }
}

async fn execute(cli: Cli) -> Result<bool> {
    match cli.command {
        Command::Run(args) => run(args).await,
        Command::Compare(args) => compare(args).await,
        Command::Experiment(args) => experiment::run(args).await,
        Command::Merge(args) => experiment::merge(args).await,
    }
}

async fn run(args: RunArgs) -> Result<bool> {
    let dataset =
        dataset::load_jsonl(&args.dataset, &args.dataset_name, &args.dataset_version).await?;
    let candidate = ProcessCandidate::new(
        args.candidate_command,
        Duration::from_millis(args.timeout_ms),
        args.max_output_bytes,
    )
    .context("invalid Candidate process configuration")?;
    let runner = EvaluationRunner::new(candidate).with_concurrency(args.concurrency);
    let report = match args.scorer {
        ScorerKind::Exact => {
            runner
                .with_scorer(JsonExactMatchScorer)
                .run(&dataset, args.candidate_version)
                .await?
        }
        ScorerKind::TokenOverlap => {
            runner
                .with_scorer(TokenOverlapScorer::new(
                    "token_overlap",
                    args.score_threshold,
                )?)
                .run(&dataset, args.candidate_version)
                .await?
        }
    };
    let comparison = compare_baseline(
        &report,
        args.baseline.as_ref(),
        policy(
            args.max_mean_drop,
            args.max_pass_rate_drop,
            args.max_execution_drop,
        )?,
    )
    .await?;
    write_outputs(
        &report,
        comparison.as_ref(),
        Some(&args.output),
        args.junit.as_ref(),
        args.markdown.as_ref(),
    )
    .await?;
    Ok(render::absolute_passed(&report)
        && comparison
            .as_ref()
            .is_none_or(|comparison| comparison.passed))
}

async fn compare(args: CompareArgs) -> Result<bool> {
    let candidate = dataset::load_report(&args.candidate).await?;
    let baseline = dataset::load_report(&args.baseline).await?;
    let comparison = candidate.compare(
        &baseline,
        &policy(
            args.max_mean_drop,
            args.max_pass_rate_drop,
            args.max_execution_drop,
        )?,
    )?;
    write_outputs(
        &candidate,
        Some(&comparison),
        None,
        args.junit.as_ref(),
        args.markdown.as_ref(),
    )
    .await?;
    Ok(render::absolute_passed(&candidate) && comparison.passed)
}

fn policy(mean: f64, pass_rate: f64, execution: f64) -> Result<RegressionPolicy> {
    RegressionPolicy::new(mean, pass_rate, execution).context("invalid regression policy")
}

async fn compare_baseline(
    report: &EvaluationReport,
    baseline: Option<&PathBuf>,
    policy: RegressionPolicy,
) -> Result<Option<RegressionComparison>> {
    let Some(path) = baseline else {
        return Ok(None);
    };
    let baseline = dataset::load_report(path).await?;
    Ok(Some(report.compare(&baseline, &policy)?))
}

async fn write_outputs(
    report: &EvaluationReport,
    comparison: Option<&RegressionComparison>,
    json: Option<&PathBuf>,
    junit: Option<&PathBuf>,
    markdown: Option<&PathBuf>,
) -> Result<()> {
    if let Some(path) = json {
        dataset::write(path, report.to_json_pretty()?.as_bytes()).await?;
    }
    if let Some(path) = junit {
        dataset::write(path, render::junit(report, comparison).as_bytes()).await?;
    }
    if let Some(path) = markdown {
        dataset::write(path, render::markdown(report, comparison).as_bytes()).await?;
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use std::{ffi::OsString, num::NonZeroUsize, time::Duration};

    use runifold_core::RunId;

    use super::{Cli, Command, RunArgs, ScorerKind, execute};

    #[tokio::test]
    async fn run_executes_candidate_without_disclosing_reference_and_writes_ci_reports() {
        let root = std::env::temp_dir().join(format!("runifold-eval-cli-{}", RunId::new()));
        tokio::fs::create_dir_all(&root).await.unwrap();
        let dataset = root.join("dataset.jsonl");
        let output = root.join("report.json");
        let junit = root.join("report.xml");
        let markdown = root.join("report.md");
        tokio::fs::write(
            &dataset,
            r#"{"id":"one","input":"question","expected":"answer","tags":["smoke"]}"#,
        )
        .await
        .unwrap();
        let script = r#"payload=$(cat); case "$payload" in *expected*) exit 9;; esac; printf '{"output":"answer"}'"#;
        let passed = execute(Cli {
            command: Command::Run(RunArgs {
                dataset,
                dataset_name: "answers".into(),
                dataset_version: "1".into(),
                candidate_version: "candidate".into(),
                output: output.clone(),
                baseline: None,
                junit: Some(junit.clone()),
                markdown: Some(markdown.clone()),
                concurrency: NonZeroUsize::MIN,
                timeout_ms: u64::try_from(Duration::from_secs(1).as_millis()).unwrap(),
                max_output_bytes: 1024,
                scorer: ScorerKind::Exact,
                score_threshold: 1.0,
                max_mean_drop: 0.0,
                max_pass_rate_drop: 0.0,
                max_execution_drop: 0.0,
                candidate_command: vec![
                    OsString::from("sh"),
                    OsString::from("-c"),
                    OsString::from(script),
                ],
            }),
        })
        .await
        .unwrap();

        assert!(passed);
        assert!(
            tokio::fs::read_to_string(output)
                .await
                .unwrap()
                .contains("\"candidate_version\": \"candidate\"")
        );
        assert!(
            tokio::fs::read_to_string(junit)
                .await
                .unwrap()
                .contains("<testsuite")
        );
        assert!(
            tokio::fs::read_to_string(markdown)
                .await
                .unwrap()
                .contains("Absolute gate: **PASS**")
        );
        tokio::fs::remove_dir_all(root).await.unwrap();
    }
}
