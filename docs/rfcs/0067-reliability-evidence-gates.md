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
