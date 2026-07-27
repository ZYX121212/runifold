use std::{collections::BTreeSet, ffi::OsString, num::NonZeroUsize, time::Duration};

use runifold_core::RunId;
use runifold_testkit::{
    EvaluationCase, EvaluationCaseId, EvaluationCaseResult, EvaluationDataset, EvaluationReport,
    EvaluationScore, EvaluationScoreSummary,
};

use super::{
    ExperimentArgs, ExperimentReport, ExperimentSample, ExperimentScorer, Shard, cache,
    confidence_interval, run, select_shard, stable_bucket,
};
use crate::ScorerKind;

#[test]
fn deterministic_shards_are_disjoint_and_complete() {
    let cases = (0..20)
        .map(|index| EvaluationCase::new(format!("case-{index}"), serde_json::json!(index)))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let dataset = EvaluationDataset::new("data", "1", cases).unwrap();
    let mut selected = BTreeSet::new();
    for index in 0..4 {
        let shard = select_shard(&dataset, Some(Shard { index, count: 4 })).unwrap();
        for case in shard.cases() {
            assert!(selected.insert(case.id().as_str().to_owned()));
            assert_eq!(stable_bucket(case.id().as_str(), 4), index);
        }
    }
    assert_eq!(selected.len(), dataset.cases().len());
}

#[test]
fn confidence_interval_uses_sample_evidence() {
    let (_, lower, upper) = confidence_interval(&[0.8, 0.9, 1.0], 0.9);

    assert!(lower.is_some_and(|value| value < 0.9));
    assert!(upper.is_some_and(|value| value > 0.9));
}

#[test]
fn experiment_validation_rejects_forged_statistics() {
    let sample = sample_report("one", 1.0);
    let dataset = EvaluationDataset::new(
        "data",
        "1",
        vec![
            EvaluationCase::new("one", serde_json::json!("input"))
                .unwrap()
                .with_expected(serde_json::json!("output")),
        ],
    )
    .unwrap();
    let mut report = ExperimentReport::new(
        &dataset,
        "candidate".into(),
        42,
        None,
        ExperimentScorer {
            name: "json_exact_match".into(),
            threshold: 1.0,
        },
        1,
        vec![ExperimentSample {
            index: 0,
            report: sample,
        }],
    )
    .unwrap();
    report.statistics[0].mean = 0.0;

    assert!(report.validate().is_err());
}

#[test]
fn merge_requires_complete_shards_and_rebuilds_evidence() {
    let case_ids = [0, 1].map(|bucket| {
        (0..1_000)
            .map(|index| format!("case-{index}"))
            .find(|id| stable_bucket(id, 2) == bucket)
            .unwrap()
    });
    let reports = case_ids
        .iter()
        .enumerate()
        .map(|(index, case_id)| {
            let dataset = EvaluationDataset::new(
                "data",
                "1",
                vec![
                    EvaluationCase::new(case_id, serde_json::json!("input"))
                        .unwrap()
                        .with_expected(serde_json::json!("output")),
                ],
            )
            .unwrap();
            ExperimentReport::new(
                &dataset,
                "candidate".into(),
                42,
                Some(Shard { index, count: 2 }),
                ExperimentScorer {
                    name: "json_exact_match".into(),
                    threshold: 1.0,
                },
                1,
                vec![ExperimentSample {
                    index: 0,
                    report: sample_report(case_id, 1.0),
                }],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();

    assert!(ExperimentReport::merge(vec![reports[0].clone()]).is_err());
    let merged = ExperimentReport::merge(reports).unwrap();
    assert!(merged.shard.is_none());
    assert_eq!(merged.case_ids.len(), 2);
    assert_eq!(merged.samples[0].report.cases.len(), 2);
    assert!((merged.statistics[0].mean - 1.0).abs() < 1e-12);
}

#[cfg(unix)]
#[tokio::test]
async fn experiment_resumes_from_validated_sample_cache() {
    let root = std::env::temp_dir().join(format!("runifold-experiment-{}", RunId::new()));
    tokio::fs::create_dir_all(&root).await.unwrap();
    let dataset = root.join("dataset.jsonl");
    let counter = root.join("counter");
    let cache = root.join("cache");
    let first_output = root.join("first.json");
    let second_output = root.join("second.json");
    tokio::fs::write(
        &dataset,
        r#"{"id":"one","input":"question","expected":"answer","tags":[]}"#,
    )
    .await
    .unwrap();
    let script = r#"payload=$(cat)
case "$payload" in *sample_index*seed*) ;; *) exit 7;; esac
n=$(cat "$1" 2>/dev/null || printf 0)
[ "$n" -ge 2 ] && exit 8
n=$((n + 1))
printf %s "$n" > "$1"
printf '{"output":"answer","input_tokens":10,"output_tokens":5,"cost_usd":0.001}'"#;
    let args = |output| ExperimentArgs {
        dataset: dataset.clone(),
        dataset_name: "answers".into(),
        dataset_version: "1".into(),
        candidate_version: "candidate".into(),
        output,
        junit: None,
        markdown: None,
        samples: NonZeroUsize::new(2).unwrap(),
        seed: 42,
        shard_index: 0,
        shard_count: NonZeroUsize::MIN,
        cache_dir: Some(cache.clone()),
        concurrency: NonZeroUsize::MIN,
        timeout_ms: u64::try_from(Duration::from_secs(1).as_millis()).unwrap(),
        max_output_bytes: 1024,
        scorer: ScorerKind::Exact,
        score_threshold: 1.0,
        min_confidence_lower_bound: Some(1.0),
        max_flaky_case_rate: 0.0,
        max_p95_latency_ms: Some(1_000.0),
        max_total_tokens: Some(30),
        max_total_cost_usd: Some(0.002),
        candidate_command: vec![
            OsString::from("sh"),
            OsString::from("-c"),
            OsString::from(script),
            OsString::from("runifold-candidate"),
            counter.clone().into_os_string(),
        ],
    };

    assert!(run(args(first_output)).await.unwrap());
    assert!(run(args(second_output)).await.unwrap());
    assert_eq!(tokio::fs::read_to_string(counter).await.unwrap(), "2");

    tokio::fs::remove_dir_all(root).await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn experiment_resumes_at_case_granularity() {
    let root = std::env::temp_dir().join(format!("runifold-case-cache-{}", RunId::new()));
    tokio::fs::create_dir_all(&root).await.unwrap();
    let dataset_path = root.join("dataset.jsonl");
    let counter = root.join("counter");
    let cache_root = root.join("cache");
    let output = root.join("experiment.json");
    tokio::fs::write(
        &dataset_path,
        concat!(
            "{\"id\":\"one\",\"input\":\"q1\",\"expected\":\"answer\",\"tags\":[]}\n",
            "{\"id\":\"two\",\"input\":\"q2\",\"expected\":\"answer\",\"tags\":[]}"
        ),
    )
    .await
    .unwrap();
    let cases = [("one", "q1"), ("two", "q2")]
        .map(|(id, input)| {
            EvaluationCase::new(id, serde_json::json!(input))
                .unwrap()
                .with_expected(serde_json::json!("answer"))
        })
        .to_vec();
    let dataset = EvaluationDataset::new("data", "1", cases).unwrap();
    let first_case = EvaluationDataset::new("data", "1", vec![dataset.cases()[0].clone()]).unwrap();
    let script = r#"cat >/dev/null
n=$(cat "$1" 2>/dev/null || printf 0)
n=$((n + 1))
printf %s "$n" > "$1"
printf '{"output":"answer"}'"#;
    let command = vec![
        OsString::from("sh"),
        OsString::from("-c"),
        OsString::from(script),
        OsString::from("runifold-candidate"),
        counter.clone().into_os_string(),
    ];
    let scorer = ExperimentScorer {
        name: "json_exact_match".into(),
        threshold: 1.0,
    };
    let fingerprint = cache::fingerprint(
        &dataset,
        "candidate",
        0,
        None,
        &scorer,
        &command,
        1_000,
        1_024,
    )
    .unwrap();
    cache::store_case(
        &cache_root,
        &fingerprint,
        0,
        &first_case,
        &sample_report("one", 1.0),
    )
    .await
    .unwrap();

    let passed = run(ExperimentArgs {
        dataset: dataset_path,
        dataset_name: "data".into(),
        dataset_version: "1".into(),
        candidate_version: "candidate".into(),
        output,
        junit: None,
        markdown: None,
        samples: NonZeroUsize::MIN,
        seed: 0,
        shard_index: 0,
        shard_count: NonZeroUsize::MIN,
        cache_dir: Some(cache_root),
        concurrency: NonZeroUsize::new(2).unwrap(),
        timeout_ms: 1_000,
        max_output_bytes: 1_024,
        scorer: ScorerKind::Exact,
        score_threshold: 1.0,
        min_confidence_lower_bound: None,
        max_flaky_case_rate: 0.0,
        max_p95_latency_ms: None,
        max_total_tokens: None,
        max_total_cost_usd: None,
        candidate_command: command,
    })
    .await
    .unwrap();

    assert!(passed);
    assert_eq!(tokio::fs::read_to_string(counter).await.unwrap(), "1");
    tokio::fs::remove_dir_all(root).await.unwrap();
}

fn sample_report(case_id: &str, score: f64) -> EvaluationReport {
    EvaluationReport {
        dataset_name: "data".into(),
        dataset_version: "1".into(),
        candidate_version: "candidate".into(),
        execution_success_rate: 1.0,
        cases: vec![EvaluationCaseResult {
            case_id: EvaluationCaseId::new(case_id).unwrap(),
            run_id: None,
            metrics: None,
            scores: vec![EvaluationScore {
                name: "json_exact_match".into(),
                value: score,
                threshold: 1.0,
                passed: score >= 1.0,
                rationale: None,
            }],
            failures: Vec::new(),
        }],
        summaries: vec![EvaluationScoreSummary {
            name: "json_exact_match".into(),
            scored_cases: 1,
            total_cases: 1,
            mean: score,
            pass_rate: f64::from(score >= 1.0),
        }],
    }
}
