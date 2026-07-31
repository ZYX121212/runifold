# RFC 0039: Release integrity and supply-chain gates

- Status: Accepted
- Date: 2026-07-27

## Context

Runifold is a graph of public crates rather than one binary. A source-tree test
alone does not prove that normalized Cargo manifests resolve, packaged files are
complete, the declared MSRV works, public APIs remain compatible, or released
dependencies satisfy security policy.

Publishing is irreversible at the version level, so artifact creation and
registry mutation need different authorization boundaries.

## Decision

Runifold adopts a synchronized workspace version and the following release
invariants:

1. Every crate inherits the same version, MSRV, README, license, repository, and
   crates.io-only publication policy.
2. CI checks current stable Rust and the declared Rust 1.88 MSRV.
3. Cargo packages and recompiles the complete workspace through its temporary
   local registry before release.
4. Public APIs are compared with the latest release tag using
   `cargo-semver-checks`.
5. RustSec advisories, license allowlists, and dependency sources are enforced.
6. Every release includes CycloneDX SBOMs and SHA-256 checksums.
7. A tag may create GitHub artifacts, but crates.io publication requires a
   separate protected, manually confirmed workflow.
8. Publication follows a dependency-first order and is restartable.

## Consequences

Release latency increases because packaged crates and MSRV builds are compiled
independently. In exchange, the system detects failures at the same boundaries
users consume: registry manifests, public APIs, supported compilers, dependency
provenance, and downloadable artifacts.

The initial `v0.1.0` release has no SemVer baseline. Starting with the next
release, the latest version tag is mandatory as the compatibility baseline.
