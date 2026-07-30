#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

msrv="$(cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].rust_version')"
if [[ -z "$msrv" || "$msrv" == "null" ]]; then
    echo "workspace packages must declare rust-version" >&2
    exit 1
fi
msrv_toolchain="$msrv"
if [[ "$msrv" =~ ^[0-9]+\.[0-9]+$ ]]; then
    msrv_toolchain="${msrv}.0"
fi

versions="$(
    cargo metadata --no-deps --format-version 1 |
        jq -r '.packages[].version' |
        sort -u
)"
if [[ "$(printf '%s\n' "$versions" | wc -l | tr -d ' ')" != "1" ]]; then
    echo "workspace packages must share one release version:" >&2
    printf '%s\n' "$versions" >&2
    exit 1
fi

version="$(printf '%s\n' "$versions" | head -n 1)"
release_tag="${RELEASE_TAG:-${GITHUB_REF_NAME:-}}"
if [[ -n "$release_tag" && "$release_tag" != "v$version" ]]; then
    echo "tag $release_tag does not match workspace version v$version" >&2
    exit 1
fi

echo "Checking Runifold v$version with MSRV Rust $msrv_toolchain"
cargo fmt --all -- --check
cargo test --workspace --all-targets --locked
cargo check --workspace --all-targets --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked

escaped_msrv_toolchain="${msrv_toolchain//./\\.}"
if ! rustup toolchain list | awk '{print $1}' |
    rg -q "^${escaped_msrv_toolchain}(-|$)"; then
    rustup toolchain install "$msrv_toolchain" --profile minimal
fi
cargo "+$msrv_toolchain" check --workspace --all-targets --all-features --locked

scripts/semver-check.sh
scripts/publish-crates.sh --list >/dev/null
cargo package --workspace --allow-dirty --locked --no-verify

package_count="$(find target/package -maxdepth 1 -name "*-$version.crate" | wc -l | tr -d ' ')"
workspace_count="$(
    cargo metadata --no-deps --format-version 1 |
        jq '.workspace_members | length'
)"
if [[ "$package_count" != "$workspace_count" ]]; then
    echo "expected $workspace_count packages, found $package_count" >&2
    exit 1
fi

echo "Release verification passed for $workspace_count crates."
