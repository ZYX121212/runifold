# RFC 0037: Reproducible Evaluation Experiments

- Status: implemented
- Scope: `runifold-eval experiment`, `runifold-eval merge`
- Depends on: RFC 0035, RFC 0036

## Problem

A single evaluation run cannot distinguish a stable improvement from model
sampling noise. Large datasets also need parallel execution and crash recovery
without silently mixing results from different Candidates or configurations.

## Experiment identity

`experiment` repeats every selected case with a stable `sample_index`. It
derives a per-case `seed` from:

```text
base seed + sample index + case ID
```

The Candidate request adds `sample_index` and `seed` but still excludes the
reference answer. A Candidate that supports deterministic sampling should pass
the seed to its Provider. Candidates may ignore it, in which case the report
still measures their observed variance.

## Statistical evidence

The experiment artifact embeds every output-free canonical
`EvaluationReport`. Aggregate statistics are recomputed from those samples:

- mean score and pass rate;
- sample standard deviation;
- two-sided 95% Student's t confidence interval;
- flaky-case rate, where a case changes its overall pass/fail decision.

One sample has no confidence interval. Therefore
`--min-confidence-lower-bound` fails closed unless at least two complete samples
produce the scorer.

Loaded and merged artifacts validate all nested reports, identities, case
membership, statistics, and flaky rates. Stored aggregate values cannot
override their sample evidence.

## Checkpoints and cache isolation

`--cache-dir` writes an output-free report after every completed Case and a
complete report after every finished Sample. Interrupted experiments resume at
Case granularity while retaining the complete-Sample fast path.

The cache namespace is a BLAKE3 fingerprint of:

- schema version and complete selected dataset content;
- Candidate version and UTF-8 executable arguments;
- base seed and shard identity;
- scorer and threshold;
- timeout and output limits.

A cache hit is accepted only after the report and exact case membership are
validated. Corrupt or contradictory entries fail explicitly instead of being
ignored. Concurrency does not participate in the fingerprint because it must
not change Candidate semantics.

## Deterministic sharding

Cases are assigned with a stable BLAKE3 hash of the Case ID:

```text
bucket = hash(case_id) mod shard_count
```

Each shard is independently resumable. `merge` requires exactly one report for
every shard index and rejects missing, duplicate, mixed-identity, or
overlapping inputs. It rebuilds every sample report and all statistics from
per-case evidence.

## CI gates

An experiment passes only when:

1. every nested absolute evaluation gate passes;
2. flaky-case rate is at most `--max-flaky-case-rate`;
3. every scorer's 95% lower bound meets
   `--min-confidence-lower-bound`, when configured.

JSON, JUnit, and Markdown outputs carry the same decision. Exit codes retain
RFC 0036 semantics: `0` passes, `1` is an invalid configuration or artifact,
and `2` is a quality-gate failure.

## Example

```bash
runifold-eval experiment \
  --dataset evals/support.jsonl \
  --dataset-name support \
  --dataset-version 2026-07-27 \
  --candidate-version prompt-v3 \
  --samples 10 \
  --seed 42 \
  --cache-dir .runifold/eval-cache \
  --min-confidence-lower-bound 0.85 \
  --max-flaky-case-rate 0.02 \
  --output artifacts/experiment.json \
  --junit artifacts/experiment.xml \
  --markdown artifacts/experiment.md \
  -- ./target/release/my-eval-candidate
```

For distributed execution, run the same command with a shared
`--shard-count`, distinct `--shard-index`, and distinct output paths. Merge all
completed outputs before applying the final gate:

```bash
runifold-eval merge \
  --inputs artifacts/shard-0.json artifacts/shard-1.json \
  --output artifacts/experiment.json \
  --min-confidence-lower-bound 0.85 \
  --max-flaky-case-rate 0.02
```
