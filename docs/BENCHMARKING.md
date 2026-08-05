# Provider benchmarking

Runifold's benchmark contract measures execution evidence without coupling the
runner to a particular framework. `ModelBenchmarkTarget` adapts Runifold's
canonical `Model` boundary, while another framework can implement
`BenchmarkTarget` and use the same scheduler, percentile calculation, report
schema, and regression gate.

## Measured evidence

Each report contains:

- successful and failed invocation counts;
- normalized failure categories;
- success rate;
- total measured wall time and throughput;
- successful total-latency min, p50, p95, p99, and max;
- successful time-to-first-output min, p50, p95, p99, and max;
- successful responses that produced no model output;
- configured concurrency and reproducibility metadata.

TTFT starts immediately before the framework invocation and stops on the first
canonical text, reasoning, Tool-argument, refusal, or complete content output.
Connection setup, request encoding, signing, routing, and provider queueing are
therefore included. Heartbeats, response-start markers, usage, warnings, and
opaque provider events do not count as model output.

## Runifold example

```rust,ignore
use std::{num::NonZeroUsize, sync::Arc};
use runifold_provider_testkit::{
    BenchmarkPlan, ModelBenchmarkTarget, benchmark,
};

let target = Arc::new(ModelBenchmarkTarget::new(
    Arc::new(model),
    request,
));
let plan = BenchmarkPlan::new(NonZeroUsize::new(1_000).unwrap())
    .with_warmup(50)
    .with_concurrency(NonZeroUsize::new(32).unwrap())
    .with_environment("framework.version", env!("CARGO_PKG_VERSION"))
    .with_environment("rust.version", "1.88.0")
    .with_environment("target", "aarch64-apple-darwin")
    .with_environment("provider", "bedrock")
    .with_environment("model", "model-or-inference-profile-id");

let report = benchmark("runifold", target, plan).await?;
let json = serde_json::to_string_pretty(&report)?;
```

Application and benchmark binaries may use `anyhow` to attach file and process
context. The reusable library contract retains typed `thiserror` errors.

## Cross-framework adapter

A Rig comparison adapter implements only one interface:

```rust,ignore
impl BenchmarkTarget for RigTarget {
    fn execute(&self) -> BenchmarkFuture<'_> {
        Box::pin(async move {
            // Invoke Rig, measure first model output and completion, validate
            // the expected result, then construct BenchmarkInvocation.
        })
    }
}
```

The adapter must include request construction and response decoding in the same
timing boundary as `ModelBenchmarkTarget`. It must classify an invalid or
unexpected response as a failure rather than reporting a fast success.

The repository includes a complete, standalone Rig 0.40 adapter and executor:

```console
cargo run --release --manifest-path benchmarks/rig-compare/Cargo.toml
```

It sends both implementations through independent copies of the same loopback
OpenAI Chat Completions SSE cassette, applies the same warmup, concurrency, and
reporting contract, and verifies every captured request's model, streaming
flag, user message, and absence of Tools. Rig is isolated in its own unpublished
workspace and lockfile, so its dependency graph and Rust-version requirements
do not affect Runifold's packages.

The workload is configurable with `RUNIFOLD_BENCH_ROUNDS`,
`RUNIFOLD_BENCH_RUNS`, `RUNIFOLD_BENCH_WARMUP`, and
`RUNIFOLD_BENCH_CONCURRENCY`. The default ten paired rounds automatically
alternate framework order; `RUNIFOLD_BENCH_ORDER` selects the starting order.
Reports are written under
`benchmarks/rig-compare/target/benchmark-reports/`. Each artifact retains every
raw round plus paired medians, favorable-round counts, and deterministic
paired-bootstrap 95% confidence intervals. `RUNIFOLD_BENCH_ENFORCE=1` gates on
aggregate non-regression rather than a noisy individual round.

The ten-round default is a fast development signal. Evidence used for
optimization or public comparison should use at least 20 paired rounds and
1,000 measured requests per framework per round, repeat with the opposite
starting order, and retain both artifacts.

The scheduled `Reproducible Rig comparison` workflow executes that 20×1000
profile with aggregate non-regression enforcement and retains the raw rounds
and confidence intervals for 90 days. This is public reproducibility evidence;
it becomes independent evidence only when an unaffiliated maintainer runs the
same locked contract and publishes their artifact.

## Regression gates

`compare_benchmarks` evaluates:

- maximum absolute success-rate drop;
- maximum relative throughput drop;
- maximum relative p95 TTFT increase;
- maximum relative p95 total-latency increase.

Policies are explicit and validated. Missing candidate latency evidence fails
when the baseline contains it. When neither side produces visible model output,
the corresponding TTFT gate is not applicable and passes.

## Fair comparison rules

Run comparisons only when all of the following are identical:

1. release-mode compilation and allocator configuration;
2. machine or isolated runner class;
3. Rust version, target, and async runtime;
4. model revision, endpoint region, credentials class, and provider account;
5. canonical conversation, Tools, output limits, and timeout;
6. warmup count, measured count, concurrency, and connection reuse;
7. telemetry and payload-capture settings;
8. retry policy and maximum attempts.

Alternate candidate order between repetitions and retain every raw JSON report.
Do not publish a single-run winner. Report medians across repeated benchmark
runs, confidence intervals when possible, and both success and latency
evidence. Provider-network benchmarks establish end-to-end behavior but cannot
isolate framework CPU overhead; use a loopback cassette benchmark for that
separate question.
