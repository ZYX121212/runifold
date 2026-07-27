use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    process::Stdio,
    time::{Duration, Instant},
};

use runifold_core::RunId;
use runifold_testkit::{
    EvaluationCase, EvaluationError, EvaluationFuture, EvaluationMetrics, EvaluationOutput,
    EvaluationTarget,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    time::timeout,
};

#[derive(Clone, Debug)]
pub(crate) struct ProcessCandidate {
    command: Vec<OsString>,
    timeout: Duration,
    max_output_bytes: usize,
    sample_context: Option<SampleContext>,
}

#[derive(Clone, Copy, Debug)]
struct SampleContext {
    index: usize,
    base_seed: u64,
}

impl ProcessCandidate {
    pub(crate) fn new(
        command: Vec<OsString>,
        timeout: Duration,
        max_output_bytes: usize,
    ) -> Result<Self, EvaluationError> {
        if command.is_empty() {
            return Err(EvaluationError::Target {
                message: "candidate command must not be empty".into(),
            });
        }
        if max_output_bytes == 0 || max_output_bytes == usize::MAX {
            return Err(EvaluationError::Target {
                message: "candidate maximum output bytes must be between 1 and usize::MAX - 1"
                    .into(),
            });
        }
        Ok(Self {
            command,
            timeout,
            max_output_bytes,
            sample_context: None,
        })
    }

    #[must_use]
    pub(crate) const fn with_sample_context(mut self, index: usize, base_seed: u64) -> Self {
        self.sample_context = Some(SampleContext { index, base_seed });
        self
    }
}

impl EvaluationTarget for ProcessCandidate {
    fn execute(
        &self,
        case: EvaluationCase,
    ) -> EvaluationFuture<Result<EvaluationOutput, EvaluationError>> {
        let command = self.command.clone();
        let timeout_duration = self.timeout;
        let max_output_bytes = self.max_output_bytes;
        let sample_context = self.sample_context;
        Box::pin(async move {
            let request = CandidateRequest {
                case_id: case.id().as_str(),
                input: case.input(),
                tags: case.tags(),
                sample_index: sample_context.map(|context| context.index),
                seed: sample_context
                    .map(|context| case_seed(context.base_seed, context.index, case.id().as_str())),
            };
            let request = serde_json::to_vec(&request)
                .map_err(|_| target_error("candidate request could not be serialized"))?;
            let started = Instant::now();
            let mut child = Command::new(&command[0])
                .args(&command[1..])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .spawn()
                .map_err(|_| target_error("candidate process could not be started"))?;
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| target_error("candidate process stdin was not available"))?;
            stdin
                .write_all(&request)
                .await
                .map_err(|_| target_error("candidate request could not be written"))?;
            stdin
                .shutdown()
                .await
                .map_err(|_| target_error("candidate stdin could not be closed"))?;
            drop(stdin);
            let stdout = child
                .stdout
                .take()
                .ok_or_else(|| target_error("candidate process stdout was not available"))?;
            let execution = async move {
                let read = async move {
                    let mut bytes = Vec::new();
                    stdout
                        .take(u64::try_from(max_output_bytes + 1).unwrap_or(u64::MAX))
                        .read_to_end(&mut bytes)
                        .await?;
                    Ok::<_, std::io::Error>(bytes)
                };
                let (status, bytes) = tokio::try_join!(child.wait(), read)?;
                Ok::<_, std::io::Error>((status, bytes))
            };
            let (status, bytes) = timeout(timeout_duration, execution)
                .await
                .map_err(|_| target_error("candidate process exceeded its deadline"))?
                .map_err(|_| target_error("candidate process execution failed"))?;
            if !status.success() {
                return Err(target_error("candidate process exited unsuccessfully"));
            }
            if bytes.len() > max_output_bytes {
                return Err(target_error("candidate response exceeded its output limit"));
            }
            let response = serde_json::from_slice::<CandidateResponse>(&bytes)
                .map_err(|_| target_error("candidate response was not valid protocol JSON"))?;
            let mut metrics = EvaluationMetrics::new(started.elapsed().as_secs_f64() * 1_000.0)
                .map_err(|_| target_error("candidate duration could not be represented"))?;
            match (response.input_tokens, response.output_tokens) {
                (Some(input), Some(output)) => {
                    metrics = metrics.with_tokens(input, output);
                }
                (None, None) => {}
                _ => {
                    return Err(target_error(
                        "candidate response must provide both input_tokens and output_tokens",
                    ));
                }
            }
            if let Some(cost_usd) = response.cost_usd {
                metrics = metrics
                    .with_cost_usd(cost_usd)
                    .map_err(|_| target_error("candidate response cost was invalid"))?;
            }
            let mut output = EvaluationOutput::new(response.output).with_metrics(metrics);
            if let Some(run_id) = response.run_id {
                output = output.with_run_id(run_id);
            }
            for (key, value) in response.metadata {
                output = output.with_metadata(key, value)?;
            }
            Ok(output)
        })
    }
}

#[derive(Serialize)]
struct CandidateRequest<'a> {
    case_id: &'a str,
    input: &'a Value,
    tags: &'a BTreeSet<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sample_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    seed: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CandidateResponse {
    output: Value,
    #[serde(default)]
    run_id: Option<RunId>,
    #[serde(default)]
    metadata: BTreeMap<String, Value>,
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
    #[serde(default)]
    cost_usd: Option<f64>,
}

fn target_error(message: &str) -> EvaluationError {
    EvaluationError::Target {
        message: message.to_owned(),
    }
}

fn case_seed(base_seed: u64, sample_index: usize, case_id: &str) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&base_seed.to_le_bytes());
    hasher.update(
        &u64::try_from(sample_index)
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    hasher.update(case_id.as_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    u64::from_le_bytes(bytes)
}

#[cfg(all(test, unix))]
mod tests {
    use std::{ffi::OsString, time::Duration};

    use runifold_testkit::{EvaluationCase, EvaluationTarget};

    use super::{ProcessCandidate, case_seed};

    #[tokio::test]
    async fn deadline_terminates_a_slow_candidate() {
        let candidate = ProcessCandidate::new(
            vec![
                OsString::from("sh"),
                OsString::from("-c"),
                OsString::from("cat >/dev/null; sleep 1"),
            ],
            Duration::from_millis(10),
            1024,
        )
        .unwrap();
        let case = EvaluationCase::new("slow", serde_json::json!("input")).unwrap();

        let error = candidate.execute(case).await.unwrap_err();

        assert!(error.to_string().contains("exceeded its deadline"));
    }

    #[tokio::test]
    async fn candidate_usage_is_validated_and_persisted_as_metrics() {
        let candidate = ProcessCandidate::new(
            vec![
                OsString::from("sh"),
                OsString::from("-c"),
                OsString::from(
                    r#"cat >/dev/null; printf '{"output":"ok","input_tokens":7,"output_tokens":3,"cost_usd":0.01}'"#,
                ),
            ],
            Duration::from_secs(1),
            1024,
        )
        .unwrap();
        let case = EvaluationCase::new("usage", serde_json::json!("input")).unwrap();

        let output = candidate.execute(case).await.unwrap();
        let metrics = output.metrics().unwrap();

        assert_eq!(metrics.input_tokens, Some(7));
        assert_eq!(metrics.output_tokens, Some(3));
        assert!((metrics.cost_usd.unwrap() - 0.01).abs() < 1e-12);
        assert!(metrics.duration_ms >= 0.0);
    }

    #[test]
    fn case_seed_is_stable_and_changes_across_samples_and_cases() {
        let first = case_seed(42, 0, "one");

        assert_eq!(first, case_seed(42, 0, "one"));
        assert_ne!(first, case_seed(42, 1, "one"));
        assert_ne!(first, case_seed(42, 0, "two"));
    }
}
