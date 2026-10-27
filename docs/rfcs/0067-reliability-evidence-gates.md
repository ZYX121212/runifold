# RFC 0067: Reliability evidence gates

## Status

Implemented for the MinIO WORM archive.

## Decision

Runifold separates feature availability from production verification. A
production reliability claim requires a repeatable external-boundary test, a
mandatory CI gate, pinned infrastructure, fail-closed assertions, and a
machine-readable artifact.

The MinIO gate runs bounded concurrent conditional writes and repeated
post-commit response-loss recovery. It emits a versioned JSON report only after
every assertion succeeds. Missing connection configuration, an invalid stress
count, a missing evidence file, or a failed artifact upload fails the job.

## Data minimization

Evidence records only revision and environment identity, operation counts,
elapsed time, and result. Credentials, signed URLs, tenant and object
identities, payloads, and provider error bodies are excluded by construction.

## Scope

This RFC establishes the evidence contract and its first implementation. It
does not claim real AWS IAM/KMS validation, multi-hour soak coverage, WASM
runtime compatibility, or independent benchmark reproduction. Those remain
explicit gaps in the public reliability matrix until their own mandatory gates
exist.

## Repeated runtime and database evidence

The soak runner binds the real checkout revision and a tracked/nonignored source
SHA-256 before/after execution. Unknown revisions, source changes, failed test
summaries, incomplete suite iterations, missing identities and runs shorter than
requested fail closed and cannot preserve an earlier passed report. Duration
uses a monotonic clock. Log projection is bounded and retains test identifiers
and numeric summaries, never panic payloads or raw logs.

Schema 3 distinguishes `short_run` from `release` profiles. A completed 5-minute
check can pass its own scope, but `release_eligible` remains false. Release
eligibility additionally requires the requested duration to be at least 180
minutes, actual elapsed time at least that duration, completed suites and a
matching source identity. This is a scoped repeated-fixture result, not proof of
one production process or remote vendor billing over three hours.

Resource observations accept both standalone JSON and bounded serial-libtest
test-name prefixes; both forms retain the same numeric and memory limits.

The repeated request stability boundary drives 16896 requests in one isolated
Linux/macOS test process, with opening failures, queued cancellation, committed
cancellation and premature EOF. Each round must return queues and local capacity
to zero, preserve unknown/unsafe retry classifications, avoid additional physical
attempts and stay within an 8 MiB resident-memory growth bound after warmup.
The soak also repeats real PostgreSQL restart/durability, SQLite recovery,
Effect reconciliation, scheduler and HTTP/WebSocket/continuation fault suites.
PostgreSQL fixtures select explicit ports outside default outbound ephemeral
ranges. Only typed Docker start rejections that explicitly identify an occupied
port permit bounded reprovisioning, with owned-container cleanup. Readiness,
transport and SQL failures, and test assertions, remain failures. Real held-port
tests verify successful reselection, the four-attempt bound, and cleanup.
Tests of the evidence writer use synthetic timestamps only to test eligibility;
they are never themselves soak evidence.
