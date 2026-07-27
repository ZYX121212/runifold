# Releasing Runifold

Runifold publishes one synchronized version across all workspace crates. A
release has two deliberately separate phases: creating verifiable GitHub
artifacts, then publishing immutable versions to crates.io.

## Compatibility policy

- Rust 1.85 is the minimum supported Rust version (MSRV).
- All public crates use Semantic Versioning.
- Before 1.0, a breaking public API change requires a minor release.
- After 1.0, a breaking public API change requires a major release.
- `cargo-semver-checks` is a release gate after the first release tag exists.
  Its output supplements review; it cannot prove all Rust API compatibility.

## Prepare a release

1. Move relevant entries from `Unreleased` in `CHANGELOG.md` to the new version.
2. Set the shared version in the root `Cargo.toml` and update internal
   dependency requirements together.
3. Run `scripts/release-check.sh`.
4. Commit the release, create a signed `vX.Y.Z` tag, and push it.

The tag workflow checks formatting, linting, tests, documentation, MSRV, all
packaged crate contents, RustSec advisories, license/source policy, and tag to
manifest version equality. It creates:

- one `.crate` archive per workspace member;
- one CycloneDX JSON SBOM per workspace member;
- `SHA256SUMS` covering every archive and SBOM.

These files are attached to the GitHub Release. A failed check creates no
release.

## Publish crates.io packages

Configure a protected GitHub environment named `crates-io`, require reviewer
approval, and store a scoped crates.io token as `CARGO_REGISTRY_TOKEN`.

After inspecting the GitHub Release assets, manually run **Publish crates.io**:

- `tag`: the existing release tag, such as `v0.1.0`;
- `confirmation`: the exact manifest version without `v`, such as `0.1.0`.

The workflow checks out that tag, packages the whole workspace again, and calls
`scripts/publish-crates.sh --execute`. The script publishes dependencies before
their consumers, waits for each version to become visible in crates.io, and
skips versions already present so an interrupted run can safely resume.

`scripts/publish-crates.sh --list` prints the audited order without changing
external state. Never publish from a branch or a dirty worktree.

## Failure recovery

- GitHub Release build failed: fix the release commit and use a new version/tag.
  Do not move a tag after consumers may have observed it.
- Publication stopped mid-workspace: rerun the same manual workflow. Existing
  crate versions are skipped, and publication resumes at the first missing one.
- A crate is incorrect after publication: yank it if necessary, fix forward,
  and release a new patch version. crates.io versions cannot be overwritten.
