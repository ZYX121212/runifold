# RFC 0066: S3 archive fault recovery

## Status

Implemented behind `archive-s3`.

## Problem

An object store write has a commit ambiguity window: the service may durably
commit an object while the client loses the response. Treating this as a
definite failure can duplicate effects, while treating it as success without
evidence can allow tombstone deletion before archive durability is proven.
Unbounded network operations can also hold a governance workflow forever.

## Decision

Each S3 PUT and reconciliation HEAD request has an explicit timeout. The
default is 30 seconds; callers may select a non-zero duration up to ten
minutes. A PUT transport failure never triggers a second PUT inside the same
archive call. Runifold instead uses the independently signed HEAD authority and
accepts success only when the stored checksum metadata exactly matches the
stable batch payload.

Archive failures expose a stable low-cardinality kind:

- `Configuration`
- `Authorization`
- `Timeout`
- `Unavailable`
- `Integrity`
- `Ambiguous`
- `Other`

Messages remain bounded and must not contain pre-signed URLs, credentials, raw
provider bodies, tenant identities, or object payloads. Provider XML error
detail is limited to small token-shaped code and header fields.

## Fault verification

The mandatory MinIO test places a transparent TCP proxy between Runifold and
the real object store. The proxy forwards a complete signed PUT, waits until
MinIO returns success, and then closes the client connection without forwarding
the response. It forwards the following signed HEAD normally.

The archive call must return a receipt derived from the committed checksum. A
new directly connected archive instance must replay the same receipt. This
proves recovery from response loss using remote durable state rather than
process memory or an unsafe blind retry.

## Limits

The request timeout bounds HTTP PUT and HEAD operations. Custom pre-signers are
application code and remain responsible for bounding their own credential or
signing I/O. Whole-workflow cancellation and deadlines remain governed by the
durable workflow runtime.
