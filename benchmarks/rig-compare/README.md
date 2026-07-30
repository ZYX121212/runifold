# Runifold versus Rig

This standalone, unpublished release-mode tool compares both frameworks through
the same `runifold-provider-testkit` benchmark contract and loopback provider
cassette. Its independent workspace and lockfile prevent Rig's dependencies
from changing Runifold's public dependency graph or MSRV.

Run the default comparison:

```shell
cargo run --release --manifest-path benchmarks/rig-compare/Cargo.toml
```

Tune the bounded workload with environment variables:

```shell
RUNIFOLD_BENCH_ROUNDS=20 \
RUNIFOLD_BENCH_RUNS=1000 \
RUNIFOLD_BENCH_WARMUP=100 \
RUNIFOLD_BENCH_CONCURRENCY=32 \
cargo run --release --manifest-path benchmarks/rig-compare/Cargo.toml
```

The default run executes ten paired rounds and alternates framework order.
`RUNIFOLD_BENCH_ORDER` selects only the first round's order:

```shell
RUNIFOLD_BENCH_ORDER=runifold-first cargo run --release --manifest-path benchmarks/rig-compare/Cargo.toml
RUNIFOLD_BENCH_ORDER=rig-first cargo run --release --manifest-path benchmarks/rig-compare/Cargo.toml
```

For evidence intended to guide optimization or publication, use at least 20
paired rounds and 1,000 measured requests per framework per round:

```shell
RUNIFOLD_BENCH_ROUNDS=20 \
RUNIFOLD_BENCH_RUNS=1000 \
RUNIFOLD_BENCH_WARMUP=100 \
cargo run --release --manifest-path benchmarks/rig-compare/Cargo.toml
```

The executable validates the semantic request shape produced by both
frameworks, prints the aggregate paired result, and writes aggregate evidence
plus every raw round to a timestamped
`target/benchmark-reports/<unix-nanoseconds>/comparison.json` artifact. The
aggregate contains framework medians, paired relative-delta medians,
deterministic paired-bootstrap 95% confidence intervals, favorable-round
counts, a non-regression decision, and a stricter statistically supported
outperformance decision. Set `RUNIFOLD_BENCH_ENFORCE=1` to return a non-zero
exit status when the aggregate confidence interval exceeds the declared
regression allowance relative to Rig.

This loopback test isolates client-side serialization, HTTP transport, SSE
decoding, stream lifecycle, and scheduler overhead. It is not evidence about
real-provider latency, Agent quality, ecosystem breadth, or an overall
framework winner.
