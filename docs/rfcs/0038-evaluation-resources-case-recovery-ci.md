# RFC 0038: Resource-Aware Evaluation and Case Recovery

- Status: implemented
- Scope: `runifold-testkit`, `runifold-eval`, GitHub Actions
- Depends on: RFC 0036, RFC 0037

## Resource evidence

Every successful external Candidate invocation records host-observed wall-clock
duration. A Candidate may also return Provider usage:

```json
{
  "output": {"decision": "yes"},
  "input_tokens": 120,
  "output_tokens": 18,
  "cost_usd": 0.00042
}
```

`input_tokens` and `output_tokens` must occur together. `cost_usd` must be
finite and non-negative. These fields, Run ID, and scorer results are persisted
without retaining prompts, references, Candidate outputs, transcripts, or
stderr.

The canonical per-case report uses optional metrics so reports and Sample
caches from RFC 0036 and RFC 0037 remain readable. Reports loaded from external
storage validate metric ranges and reject metrics attached to a target
failure.

## Experiment resource statistics

Experiment reports recompute the following evidence from every Case and Sample:

- mean, p50, p95, and maximum host latency;
- total input and output tokens;
- total Candidate-reported cost;
- expected and observed counts for latency, token, and cost evidence.

Stored aggregates cannot override their per-case evidence. Shard merge rebuilds
resource statistics after combining Case results.

## Budget gates

`experiment` and `merge` accept:

- `--max-p95-latency-ms`;
- `--max-total-tokens`;
- `--max-total-cost-usd`.

A configured budget fails closed if any required observation is missing.
Token totals use checked integer arithmetic. Cost totals must remain finite.
The decision is reflected consistently in the process exit code, JUnit, and
Markdown reports.

## Case-level recovery

The cache retains the RFC 0037 full-Sample fast path. When no complete Sample
exists, each Case is independently looked up under:

```text
fingerprint / sample index / hash(case ID)
```

Missing Cases execute concurrently. Each completed one-case canonical report
is validated and atomically renamed into place before the next process can be
lost to interruption. Once all Cases exist, the CLI rebuilds and stores the
full Sample report.

The outer cache fingerprint still covers complete selected dataset content,
Candidate version and command, scorer configuration, seed, shard, deadline,
and output bound. A Case filename uses a BLAKE3 digest rather than raw user
input, preventing path traversal.

## Reusable GitHub Actions workflow

`.github/workflows/runifold-evaluation.yml` is a same-repository reusable
workflow triggered with `workflow_call`. It:

1. installs stable Rust and builds the locked evaluation CLI;
2. restores a coarse GitHub Actions cache while relying on Runifold's internal
   fingerprint for semantic isolation;
3. runs confidence, flakiness, latency, token, and cost gates;
4. always saves a new immutable checkpoint cache;
5. publishes Markdown to the job summary and uploads JSON, JUnit, and Markdown.

Third-party Actions are pinned to commit SHAs. The Candidate command is
explicitly a trusted workflow input and runs behind an explicit Bash boundary.
Provider credentials are optional workflow secrets and are never command
inputs.

Caller example:

```yaml
jobs:
  evaluate:
    uses: ./.github/workflows/runifold-evaluation.yml
    with:
      dataset: evals/support.jsonl
      dataset-name: support
      dataset-version: 2026-07-27
      candidate-version: prompt-v4
      candidate-build-command: cargo build --locked --release -p my-eval-candidate
      candidate-command: ./target/release/my-eval-candidate
      samples: 10
      min-confidence-lower-bound: "0.85"
      max-flaky-case-rate: "0.02"
      max-p95-latency-ms: "3000"
      max-total-tokens: "250000"
      max-total-cost-usd: "10.00"
    secrets:
      openai-api-key: ${{ secrets.OPENAI_API_KEY }}
```
