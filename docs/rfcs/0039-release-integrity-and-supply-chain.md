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

When accepting frozen copies alongside another checkout, use a fresh target
directory for each candidate. Benchmarks, MSRV and synthetic SemVer workspaces
use separate output trees;
sharing mutable Cargo artifacts across frozen source copies can execute stale
binaries even when source fingerprints match. Source identity alone does not
authenticate an already-built executable. Record executed artifact digests when
binding a local acceptance run to its compiled outputs.

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
release, the latest prior version tag reachable from the tested commit is the
default compatibility baseline. Tags on other histories are excluded; an
explicit `SEMVER_BASELINE` still selects a deliberate comparison.
Public enum extensions append variants so existing discriminants and Serde
variant indices are preserved. The release SemVer gate checks this contract.

## Consumer environment identity

`runifold ai environment --manifest-path Cargo.toml --json` inspects the
locked, offline Cargo graph rather than declaration ranges. It records resolved
package versions, features, renamed dependency edges, hashed source identities,
available package checksum provenance, actual package source digests, the
workspace manifest/lock digests and compiler version. A version-preserving vendor
source or PATCH.md change changes the identity. Relocating identical path
packages does not. Rooted SHA-256 identities replace raw Git/registry URLs and
absolute paths in the report; source contents are never emitted.

Source selection uses packaged source trees or Git tracked/nonignored files,
with a bounded tree fallback for non-Git path dependencies. Private environment
and credential files are excluded. Symlinks/nonregular files, path escapes,
incoherent source edits, missing locks and unavailable offline dependencies fail
the inspection rather than produce a partial fingerprint. Selection is bounded
to 4096 resolved packages, 100000 files and 2 GiB per hashing pass. It rereads
sources before returning, so normal concurrent edits fail closed.

The report explicitly distinguishes graph features from features linked into an
existing binary. Ignored files, external build-script inputs and compilation
environment flags are outside its scope. Runtime policy is separately available
through `ProviderRuntimeProfile::diagnostic_snapshot`; neither snapshot implies
live provider compatibility, migration safety or release acceptance.
