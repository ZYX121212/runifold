#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

baseline="${SEMVER_BASELINE:-$(git tag --list 'v*' --sort=-version:refname | head -n 1)}"
if [[ -z "$baseline" ]]; then
    echo "No prior release tag exists; SemVer comparison starts after v0.1.0."
    exit 0
fi

git rev-parse --verify "${baseline}^{commit}" >/dev/null
echo "Checking public APIs against $baseline"

baseline_root="$(mktemp -d)"
current_metadata="$(mktemp)"
baseline_metadata="$(mktemp)"
cleanup() {
    rm -rf "$baseline_root"
    rm -f "$current_metadata" "$baseline_metadata"
}
trap cleanup EXIT

git archive "$baseline" | tar -x -C "$baseline_root"
cargo metadata --no-deps --format-version 1 >"$current_metadata"
cargo metadata \
    --no-deps \
    --format-version 1 \
    --manifest-path "$baseline_root/Cargo.toml" \
    >"$baseline_metadata"

new_packages=()
exclusions=()
while IFS= read -r package; do
    if [[ -n "$package" ]]; then
        new_packages+=("$package")
        exclusions+=(--exclude "$package")
    fi
done < <(
    comm -23 \
        <(jq -r '.packages[].name' "$current_metadata" | sort) \
        <(jq -r '.packages[].name' "$baseline_metadata" | sort)
)

if ((${#new_packages[@]} > 0)); then
    echo "Skipping new packages without a $baseline baseline:"
    printf '  %s\n' "${new_packages[@]}"
fi

cargo semver-checks \
    --workspace \
    --all-features \
    --baseline-rev "$baseline" \
    "${exclusions[@]}"
