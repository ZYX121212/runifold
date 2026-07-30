# RFC 0065: Real MinIO WORM verification

## Status

Implemented in CI.

## Decision

Runifold verifies its native S3 archive against a real, pinned MinIO server in
a dedicated CI job. The job creates a versioned bucket with Object Lock enabled
before running the ignored integration test. A CI-only static KMS key enables
the SSE-S3 path; production deployments still require their own managed KMS.
The job does not rely on optional credentials and cannot silently skip when its
environment is incomplete.

The test races two writers for the same immutable archive batch and requires
both calls to converge on one receipt. It then reconstructs the signer and
archive client to prove that reconciliation does not depend on process memory.
A different payload under the same batch identity must fail closed.

Finally, a separately signed HEAD request verifies the committed object's
COMPLIANCE retention mode, retention deadline, Runifold checksum metadata, and
version identifier. These assertions cover the storage semantics required for
durable tombstone purge evidence rather than only HTTP request shape.

Conditional PUT and reconciliation HEAD traffic use separate HTTP connection
pools. Some S3-compatible servers can reject a conditional upload before fully
consuming its body, so reusing that connection for the authoritative HEAD can
turn a valid conflict into a transport failure.

## Operational boundary

The MinIO job validates S3-compatible behavior and Runifold's own SigV4
implementation. AWS-specific IAM, KMS key policy, lifecycle replication, and
regional outage behavior remain deployment acceptance responsibilities and
require separate AWS integration credentials.

## Local execution

Start a lock-enabled MinIO bucket, then run:

```bash
RUNIFOLD_MINIO_ENDPOINT=http://127.0.0.1:9000 \
RUNIFOLD_MINIO_ACCESS_KEY=runifold-ci \
RUNIFOLD_MINIO_SECRET_KEY=runifold-ci-secret \
RUNIFOLD_MINIO_BUCKET=runifold-archive \
cargo test -p runifold \
  --features archive-s3 \
  --test live_minio \
  lock_checksum_concurrency_and_reconstruction_survive_real_minio \
  -- --ignored --exact --nocapture
```
