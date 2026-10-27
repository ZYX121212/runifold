#!/usr/bin/env bash
# Build the real storage fixture from immutable upstream source commits.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output="$repo_root/target/minio-fixture"
temporary="$(mktemp -d)"
trap 'rm -rf "$temporary"' EXIT
mkdir -p "$output"

for component in minio mc; do
    if [[ "$component" == minio ]]; then
        revision="${RUNIFOLD_MINIO_SERVER_SOURCE:?supply the pinned MinIO server commit}"
    else
        revision="${RUNIFOLD_MINIO_CLIENT_SOURCE:?supply the pinned MinIO client commit}"
    fi
    if [[ ! "$revision" =~ ^[0-9a-f]{40}$ ]]; then
        echo "fixture revisions must be full source commit IDs" >&2
        exit 1
    fi
    source_root="$temporary/$component"
    git init --quiet "$source_root"
    git -C "$source_root" fetch --quiet --depth=1 "https://github.com/minio/$component.git" "$revision"
    git -C "$source_root" checkout --quiet --detach FETCH_HEAD
    test "$(git -C "$source_root" rev-parse HEAD)" = "$revision"
    (
        cd "$source_root"
        CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o "$output/$component" .
    )
    cp "$source_root/LICENSE" "$output/LICENSE-$component"
done

cat >"$output/Dockerfile" <<'DOCKERFILE'
FROM scratch
COPY minio /minio
COPY LICENSE-minio /LICENSE
ENTRYPOINT ["/minio"]
DOCKERFILE
docker build --tag runifold-ci-minio:source "$output"
docker image inspect runifold-ci-minio:source --format '{{.Id}}' >"$output/server-image-id"
sha256sum "$output/mc" | cut -d ' ' -f 1 >"$output/client-sha256"
