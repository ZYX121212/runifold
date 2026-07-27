#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

publish_order=(
    runifold-core
    runifold-macros
    runifold-provider-testkit
    runifold-effect
    runifold-model
    runifold-tool
    runifold-testkit
    runifold-provider-anthropic
    runifold-provider-gemini
    runifold-provider-ollama
    runifold-provider-openai
    runifold-observability-otel
    runifold-agent
    runifold-mcp
    runifold-store-sqlite
    runifold-workflow
    runifold-eval-cli
    runifold
)

mode="${1:---list}"
metadata="$(cargo metadata --no-deps --format-version 1)"
version="$(jq -r '.packages[0].version' <<<"$metadata")"
workspace_crates="$(
    printf '%s' "$metadata" |
        jq -r '.packages[].name' |
        sort
)"
ordered_crates="$(printf '%s\n' "${publish_order[@]}" | sort)"
if [[ "$workspace_crates" != "$ordered_crates" ]]; then
    echo "publish order does not exactly match workspace crates" >&2
    diff <(printf '%s\n' "$workspace_crates") <(printf '%s\n' "$ordered_crates") || true
    exit 1
fi

published=""
for crate_name in "${publish_order[@]}"; do
    while IFS= read -r dependency; do
        [[ -z "$dependency" ]] && continue
        if ! rg -qx "$dependency" <<<"$published"; then
            echo "$crate_name appears before internal dependency $dependency" >&2
            exit 1
        fi
    done < <(
        jq -r --arg crate "$crate_name" '
            .packages[]
            | select(.name == $crate)
            | .dependencies[]
            | select(.kind == null and .path != null)
            | .name
        ' <<<"$metadata"
    )
    published="${published}${crate_name}"$'\n'
done

if [[ "$mode" == "--list" ]]; then
    printf '%s\n' "${publish_order[@]}"
    exit 0
fi
if [[ "$mode" != "--execute" ]]; then
    echo "usage: $0 [--list|--execute]" >&2
    exit 2
fi
if [[ "${CONFIRM_RUNIFOLD_PUBLISH:-}" != "$version" ]]; then
    echo "set CONFIRM_RUNIFOLD_PUBLISH=$version to publish" >&2
    exit 1
fi
if [[ -n "$(git status --porcelain)" ]]; then
    echo "refusing to publish from a dirty worktree" >&2
    exit 1
fi
if [[ "$(git tag --points-at HEAD --list "v$version")" != "v$version" ]]; then
    echo "HEAD must be tagged v$version" >&2
    exit 1
fi

for crate_name in "${publish_order[@]}"; do
    if curl --fail --silent --show-error \
        --header "User-Agent: runifold-release-bot (https://github.com/runifold/runifold)" \
        "https://crates.io/api/v1/crates/$crate_name/$version" >/dev/null 2>&1; then
        echo "$crate_name@$version already exists; skipping"
        continue
    fi

    cargo publish --package "$crate_name" --registry crates-io --locked

    for attempt in {1..60}; do
        if cargo info "$crate_name@$version" --registry crates-io >/dev/null 2>&1; then
            break
        fi
        if [[ "$attempt" == "60" ]]; then
            echo "timed out waiting for $crate_name@$version in crates.io" >&2
            exit 1
        fi
        sleep 5
    done
done

echo "Published Runifold v$version in dependency order."
