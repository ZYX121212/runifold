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
cargo semver-checks --workspace --all-features --baseline-rev "$baseline"
