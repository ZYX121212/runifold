#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

version="$(
    cargo metadata --no-deps --format-version 1 |
        jq -r '.packages[0].version'
)"
release_dir="$repo_root/target/release-assets"
generated_sboms=()
for manifest in crates/*/Cargo.toml; do
    crate_dir="$(dirname "$manifest")"
    crate_name="$(basename "$crate_dir")"
    generated_sboms+=("$crate_dir/$crate_name.cdx.json")
done
cleanup_generated_sboms() {
    rm -f "${generated_sboms[@]}"
}
trap cleanup_generated_sboms EXIT
cleanup_generated_sboms

if [[ -z "${SOURCE_DATE_EPOCH:-}" ]]; then
    SOURCE_DATE_EPOCH="$(git log -1 --format=%ct 2>/dev/null || printf '0')"
fi
export SOURCE_DATE_EPOCH

mkdir -p "$release_dir/crates" "$release_dir/sbom"
find "$release_dir" -type f -delete

# `release-check.sh` performs the isolated package compilation. This pass only
# materializes the exact archives that become release assets.
cargo package --workspace --allow-dirty --locked --no-verify
cp target/package/*-"$version".crate "$release_dir/crates/"

cargo cyclonedx --format json --spec-version 1.5 --all-features --target all
while IFS= read -r manifest; do
    crate_dir="$(dirname "$manifest")"
    crate_name="$(basename "$crate_dir")"
    cp "$crate_dir/$crate_name.cdx.json" "$release_dir/sbom/$crate_name-$version.cdx.json"
done < <(find crates -mindepth 2 -maxdepth 2 -name Cargo.toml | sort)

(
    cd "$release_dir"
    if command -v sha256sum >/dev/null 2>&1; then
        find crates sbom -type f -print0 | sort -z | xargs -0 sha256sum > SHA256SUMS
    else
        find crates sbom -type f -print0 | sort -z | xargs -0 shasum -a 256 > SHA256SUMS
    fi
)

echo "Release assets are available in $release_dir"
