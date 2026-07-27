# RFC 0036: Evaluation CLI and CI Quality Gates

- Status: implemented
- Scope: `runifold-eval-cli`
- Binary: `runifold-eval`

## Decision

Runifold provides a separate CLI crate for executing offline quality
evaluations and enforcing them in CI. Keeping the executable separate prevents
`clap`, Tokio process support, and application-level error handling from
entering runtime or testkit library dependency graphs.

The CLI has two commands:

- `run` loads JSONL cases, executes one external Candidate process per case,
  scores outputs, optionally compares a baseline, and writes reports;
- `compare` validates and compares existing candidate and baseline JSON
  reports without executing a Candidate.

Exit status is part of the stable CI contract:

- `0`: execution completed and all absolute and relative quality gates passed;
- `1`: CLI configuration, dataset/report IO, or artifact validation failed;
- `2`: evaluation completed but a target, scorer, absolute-quality, or
  regression gate failed.

## JSONL dataset

Each non-empty line is one serialized `EvaluationCase`:

```json
{"id":"refund-policy","input":{"question":"Can I refund?"},"expected":{"decision":"yes"},"tags":["policy"]}
```

Dataset name and version are command arguments. The CLI reconstructs
`EvaluationDataset`, so empty datasets, duplicate IDs, empty identities, and
invalid tags fail before Candidate execution.

## Candidate process protocol

The command after `--` is executed directly without a shell. Each case gets an
isolated process. The CLI writes exactly one JSON object to stdin:

```json
{"case_id":"refund-policy","input":{"question":"Can I refund?"},"tags":["policy"]}
```

The reference answer is deliberately excluded, so a Candidate cannot pass by
reading the answer from its protocol input.

The Candidate must write one JSON object to stdout:

```json
{"output":{"decision":"yes"},"run_id":"019...","metadata":{},"input_tokens":120,"output_tokens":18,"cost_usd":0.00042}
```

`run_id`, `metadata`, token usage, and cost are optional. Token fields must
occur together. The host measures duration independently. stderr is not copied
into evaluation reports. Arguments are not shell-expanded. Every process has a
deadline, kill-on-drop behavior, and a bounded stdout size. Non-zero exit,
timeout, oversized output, invalid JSON, unknown response fields, and invalid
metrics or metadata all become isolated target failures.

## Scoring and gates

`--scorer exact` uses strict JSON equality. `--scorer token-overlap` uses the
deterministic scorer and `--score-threshold`. Absolute gating fails closed when
a case has no scores, any target/scorer failure, or any score below threshold.

When `--baseline` is present, dataset identity must match and the CLI also
applies maximum mean, pass-rate, and execution-success drops. The same relative
gate is available independently through `compare`.

## Reports

`run` always writes the output-free canonical JSON report. Optional outputs
are:

- JUnit XML, with one testcase per dataset case and one baseline testcase;
- Markdown containing aggregate scores and regression deltas.

JUnit and Markdown contain only report fields. Raw inputs, references,
Candidate outputs, prompts, transcripts, and process stderr remain excluded.

## Dependency boundary

```text
runifold-eval-cli -> runifold-testkit -> runifold-model + runifold-core
```

No runtime crate depends on the CLI.
