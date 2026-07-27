# RFC 0035: Agent Quality Evaluation and Regression Gates

- Status: implemented
- Scope: `runifold-testkit`

## Decision

Runifold treats quality evaluation as a separate offline signal rather than an
online operational metric. `runifold-testkit` provides:

- versioned, duplicate-free datasets;
- an asynchronous system-under-evaluation boundary;
- asynchronous composable scorers;
- bounded concurrent case execution with deterministic report order;
- optional Run IDs that correlate a case result with its OpenTelemetry trace;
- output-free JSON reports;
- relative baseline comparison across mean score, pass rate, and target
  execution success.

This layer remains Provider-, Agent-, and OpenTelemetry-independent. An
application adapts an Agent, Workflow, model, or complete service with an
`EvaluationTarget` closure. The closure owns its case and future, so it can
create an isolated `RunContext`, invoke async infrastructure, and return the
resulting `RunId` without requiring `async_trait`.

Evaluation currently remains in `runifold-testkit`. A separate `runifold-eval`
crate is justified only when persistent datasets, distributed workers, or
independent release cadence create a real package boundary.

## Dataset identity

Every dataset has a non-empty name, version, and at least one case. Case IDs
must be unique. Baseline and candidate reports are comparable only when both
dataset name and version match exactly. Changing a rubric, reference answer,
or case population therefore requires a new dataset version.

Cases contain arbitrary JSON input, an optional reference answer, and stable
tags. Raw cases are never copied into `EvaluationReport`.

## Target and trace correlation

`EvaluationTarget` accepts an owned case and returns `EvaluationOutput`.
Outputs contain the scorer-visible JSON value, optional metadata, and an
optional `RunId`. The Run ID is retained in the per-case report so an operator
can jump from a failed quality case to the causal Agent/Model/Tool trace.

A successful target does not need a Run ID. Missing correlation reduces
debuggability but does not turn a valid output into an execution failure.
Target failures are isolated to their case and do not cancel unrelated cases.

## Scoring

Scores are finite values from zero through one. Each scorer has a stable,
unique name and a validated per-case threshold. Missing scores count against
dataset-wide pass rate because the denominator is every case, not only
successfully scored cases.

`JsonExactMatchScorer` covers deterministic structured outputs. `FnScorer`
adapts semantic similarity, policy, task-specific, human, or model-judge
scorers without coupling the testkit to a Provider.

`TokenOverlapScorer` supplies deterministic case-folded Sørensen-Dice overlap
for string answers. It is deliberately described as lexical rather than
semantic. `JsonRuleScorer` combines weighted existence, equality, substring,
and numeric-range rules for structured outputs. Rule weights, score thresholds,
and numeric bounds are validated before execution.

`ModelJudgeScorer` accepts any canonical `Model`, so OpenAI, Ark, Qwen,
Anthropic, Gemini, Ollama, Router, or a custom adapter can judge the same
dataset. The request uses a strict JSON Schema and the response is decoded and
range-checked locally. Input, reference, and candidate values are serialized as
an untrusted JSON payload; only the versioned rubric is placed in the system
instruction. Provider error messages are not copied into evaluation reports.

Model judges must use structured output, a separate budget, explicit rubric
versioning, and adversarial cases for prompt injection. Judge rationales can
contain evaluated content and should be retained only when the evaluation
owner explicitly accepts that disclosure.

## Reports and privacy

Reports contain:

- dataset and candidate versions;
- target execution-success rate;
- case ID and optional Run ID;
- normalized scores, thresholds, pass decisions, and optional rationales;
- safe target/scorer failures;
- deterministic per-scorer aggregates.

They do not contain raw inputs, reference answers, target outputs, prompts, or
transcripts. This makes CI artifacts safer by default. A target or scorer can
still place sensitive text in an error or rationale, so those fields remain
explicit evaluator-owned disclosure surfaces.

## Regression policy

`RegressionPolicy` bounds the allowed decrease in:

- mean score;
- dataset-wide pass rate;
- target execution-success rate.

A comparison fails closed when a baseline score is missing from the candidate.
New candidate-only scores do not affect an older baseline. Teams should combine
relative regression checks with explicit absolute requirements; a candidate
that matches a poor baseline is not necessarily acceptable.

## Artifact repository

`EvaluationRepository` separates artifact persistence from evaluation
execution. `FileEvaluationRepository` stores versioned datasets and candidate
reports as validated JSON:

- identity components are hex encoded before path construction;
- writes use a same-directory temporary file and an atomic no-replace hard
  link;
- saving identical bytes is idempotent;
- changing content under an existing dataset/candidate version is a typed
  conflict rather than an overwrite;
- loaded datasets and reports are validated again before use.

This immutable policy makes a version a content promise and prevents concurrent
CI jobs from silently replacing a baseline. Database or object-store adapters
can implement the same narrow repository trait.

## Dependency boundary

```text
runifold-testkit -> runifold-model + runifold-core
```

No runtime crate depends on evaluation. Production execution therefore pays no
evaluation dependency, latency, storage, or cardinality cost.
