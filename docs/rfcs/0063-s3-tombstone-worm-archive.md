# RFC 0063: S3-compatible WORM tombstone archive

## Status

Implemented behind the `archive-s3` feature.

## Decision

Runifold uses application-owned pre-signing rather than holding long-lived AWS
credentials. The pre-signer receives the exact bucket, stable object key, and
required headers, then returns short-lived PUT and HEAD URLs. This supports AWS
S3, MinIO, Ceph, and compatible object stores without coupling the workflow
domain crate to a cloud SDK.

Each object contains canonical JSON and uses:

- a stable key derived from the archive batch ID;
- `If-None-Match: *` conditional creation;
- SHA-256 checksum request and durable checksum metadata;
- required AES-256 or KMS server-side encryption;
- optional GOVERNANCE or COMPLIANCE Object Lock retention.

On duplicate or ambiguous PUT, Runifold performs only a pre-signed HEAD. It
accepts success when the stored checksum metadata exactly matches the payload;
otherwise it fails closed and does not advance the PostgreSQL export watermark.

## Security boundary

The application credential layer can grant authority for one object and two
operations. Runifold never logs or stores signing credentials or pre-signed
URLs. The archive receipt contains only bucket, object key, and checksum.

## Verification

A real TCP HTTP cassette verifies conditional PUT headers and replay through a
412 response followed by checksum-confirming HEAD.
