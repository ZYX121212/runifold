# RFC 0064: Native S3 SigV4 pre-signer

## Status

Implemented behind `archive-s3`.

## Decision

Runifold provides a lightweight native Signature Version 4 pre-signer instead
of requiring the AWS SDK. It supports AWS S3 virtual-host addressing and
path-style custom endpoints such as MinIO.

Configuration includes endpoint, region, expiry, addressing mode, access key,
secret key, and an optional temporary session token. Credentials are validated,
stored in secret containers, and redacted from `Debug`. Generated URLs and
signed headers are also redacted.

PUT signatures bind every Runifold-required checksum, encryption, conditional
creation, and Object Lock header. HEAD signatures authorize only reconciliation.
Expiration is bounded by S3's seven-day maximum.

## Construction

```rust
use std::sync::Arc;
use reqwest::Url;
use runifold::archive_s3::{
    S3ArchiveEncryption, S3SigV4Credentials, S3SigV4Presigner,
    S3SigV4PresignerConfig, S3TombstoneArchive, S3TombstoneArchiveConfig,
};

let signer = S3SigV4Presigner::new(
    S3SigV4PresignerConfig::new(
        Url::parse("https://s3.example.internal")?,
        "us-east-1",
        300,
        true,
    )?,
    S3SigV4Credentials::new(access_key, secret_key, session_token)?,
);
let archive = S3TombstoneArchive::new(
    S3TombstoneArchiveConfig::new(
        "runifold-archive",
        "task-tombstones",
        S3ArchiveEncryption::Aes256,
    )?,
    Arc::new(signer),
);
```

## Verification

Deterministic signing tests cover path encoding, temporary tokens, signed-header
sets, and disclosure resistance. The HTTP archive cassette uses the native
signer and races two identical archive calls; one creates the object and the
other reconciles the checksum after a conditional-write conflict.
