# Testing

Run the complete suite with:

```bash
cargo test --workspace
```

The ignored live OpenAI Realtime canary is selected explicitly:

```bash
RUNIFOLD_LIVE_OPENAI_REALTIME_MODEL=gpt-realtime-2.1 \
RUNIFOLD_LIVE_OPENAI_REALTIME_TTL_SECONDS=60 \
RUNIFOLD_LIVE_EVIDENCE_PATH=target/reliability-evidence/openai-realtime-live.json \
cargo test -p runifold-providers \
  --features openai \
  --test openai_realtime_live \
  -- --ignored
```

`OPENAI_API_KEY` must already exist in the process environment; do not place
it in the command line. The canary makes exactly two client-secret requests,
performs no model inference, prints no secret, and persists only redacted
assertions. GitHub users should prefer the manual `Live OpenAI Realtime
canary` workflow, which also scans the artifact for credential-shaped values.

The ignored Ark canary verifies strict JSON Schema, hosted `web_search` mixed
with a function tool, and streamed plus complete Responses delivery:

```bash
RUNIFOLD_LIVE_ARK_MODEL=doubao-seed-2-0-lite-260428 \
RUNIFOLD_LIVE_EVIDENCE_PATH=target/reliability-evidence/ark-live.json \
cargo test -p runifold-providers \
  --features openai \
  --test ark_live \
  -- --ignored
```

`ARK_API_KEY` must already exist in the process environment. Prefer the manual
`Live Ark Responses canary` workflow; it fails when the repository secret is
missing and uploads only model identity and pass/fail assertions, never model
content or credentials.

PostgreSQL integration tests do not silently skip. By default they use
Testcontainers to start disposable `pgvector/pgvector:pg16` databases, so a
working Docker-compatible daemon is required. Containers and their writable
layers are removed automatically when each test binary finishes.

Externally managed databases remain supported:

```bash
RUNIFOLD_TEST_POSTGRES_URL=postgres://... \
RUNIFOLD_TEST_PGVECTOR_URL=postgres://... \
cargo test -p runifold-store-postgres
```

`RUNIFOLD_TEST_POSTGRES_URL` covers conversation and workflow persistence.
`RUNIFOLD_TEST_PGVECTOR_URL` covers semantic-memory vector operations. An
explicitly configured variable must contain a non-empty URL; invalid or
unreachable endpoints fail the test.

The database restart test always owns its disposable container. It commits a
transcript, stops PostgreSQL, requires the stale client to return a typed
storage failure within five seconds, restarts the same writable layer,
rediscovers the mapped host port, reconnects, and verifies the committed
transcript.

SQLite crash-recovery tests use a parent/child synchronization marker and then
call `Child::kill` from the parent at the exact durable boundary. The child does
not execute a normal shutdown path. Separate tests prove that a completed Tool
effect is replayed once and that an expired workflow lease adopts and settles
the crashed budget reservation with a higher fencing token.

The MCP Task recovery test also always owns its database. It durably creates a
workflow-backed Task, verifies that a stopped database becomes a bounded
JSON-RPC storage failure, restarts the same writable layer, constructs a fresh
store, adapter, server, and client, and recovers solely from `taskId`. Eight
concurrent Task subscriptions must observe the recovered working state and the
same cancelled terminal state; duplicate cancellation must remain
idempotent. The same test then verifies active-Task retention protection,
exclusive cleanup ownership, expired-lease takeover, stale fencing rejection,
atomic tombstone-plus-delete, idempotent empty retry, audit pagination, and
post-cleanup MCP not-found behavior. It also verifies database-clock cleanup
heartbeat, premature-takeover prevention, keyset discovery across three
tenants, bounded supervisor concurrency, automatic cleanup, and health totals.
The same disposable database then exercises tombstone governance: archive
watermark monotonicity, hold exclusion, four-eyes approval, a late legal-hold
race, lease takeover, stale fencing, idempotent purge, and durable evidence.
The authorized facade is tested for deny-before-I/O behavior, policy-backend
outage, principal-derived audit identity, foreign-lease rejection, stable
archive batch replay, and idempotent export confirmation.
It also covers durable approval-inbox discovery, expired reviewer takeover,
stale-token fencing, principal-bound decisions, durable rejection reasons, and
successful independent approval.

S3-compatible immutable tombstone archives have a dedicated, mandatory CI job
against pinned MinIO server and client images. The job creates an Object
Lock-enabled bucket, configures a CI-only static KMS key for SSE-S3, and runs
the otherwise ignored `live_minio` test. It races same-batch writers,
reconstructs the archive client, rejects a conflicting payload, and verifies
checksum metadata, COMPLIANCE retention, and object versioning through a
separately signed HEAD request. A transparent TCP fault proxy also allows
MinIO to commit an encrypted locked object and then discards the successful
PUT response; the same archive call must recover through HEAD and a direct
client reconstruction must replay the identical receipt. The four
`RUNIFOLD_MINIO_*` variables are required; missing configuration is a test
failure rather than a skip.

The CI job runs 32 four-writer conditional-create iterations and eight
commit-success/response-loss recoveries. A successful run writes
`target/reliability-evidence/minio-worm.json` and uploads it as a 30-day CI
artifact. The report contains only the revision, pinned image identities,
bounded operation counts, elapsed time, and pass result. It contains no
credentials, tenant IDs, object keys, payloads, URLs, or provider error bodies.
Set `RUNIFOLD_MINIO_STRESS_ITERATIONS` to `1..=1000` for a larger local run.

The provider-neutral facade must compile for `wasm32-unknown-unknown` on the
declared Rust 1.88 MSRV, and core security semantics must execute under the
pinned `wasm-bindgen` Node runner:

```bash
rustup target add wasm32-unknown-unknown --toolchain 1.88.0
cargo install wasm-bindgen-cli --version 0.2.126 --locked
cargo +1.88.0 check -p runifold \
  --target wasm32-unknown-unknown \
  --no-default-features \
  --locked
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
  cargo +1.88.0 test -p runifold-core \
    --target wasm32-unknown-unknown \
    --test wasm_edge \
    --locked
```

CI records successful build and runtime identities in
`target/reliability-evidence/wasm-edge.json`. This gate covers the
provider-neutral kernel.

The browser Provider gate additionally starts the CORS-enabled cassette in
`scripts/browser-provider-server.py` and executes
`wasm_browser_provider` through a matching pinned Chrome Headless Shell and
ChromeDriver. It verifies OpenAI-compatible, Anthropic, Gemini and Ollama
Agent streaming, OpenAI-compatible/Gemini/Ollama embeddings, fragmented SSE
and NDJSON, OpenAI-compatible model discovery, bounded multipart upload,
Batch create/inspect/cancel, in-flight cancellation, browser deadlines, 429
classification, OpenAI GA Realtime WebSocket text and bounded PCM24
input/output/transcript lifecycles, short-lived Realtime client-secret
creation/redaction, real WebRTC offer/answer and `oai-events` exchange,
fake-device microphone capture, remote-audio attachment, bounded data-channel
overflow behavior, local RFC 5389 STUN and server-reflexive candidates,
Peer/ICE connectivity state, phase-aware reconnect classification, TURN
credential redaction, two relay-only peers crossing a digest-pinned coturn
container, ICE disconnection after that container is stopped, automatic
reconnect factory rotation with fresh-credential intent, container restart,
new relay-only Peer allocation, old-session event isolation, status-aware
Gateway retry, pending-Peer cleanup, and the absence of every upstream
Provider credential header.
CI writes
`target/reliability-evidence/wasm-browser-provider.json`.

Long-lived Provider keys must never be supplied to downloadable browser
artifacts. See [Browser and edge deployment](EDGE.md) for the verified
application-gateway boundary.

Before merging persistence changes, run:

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo +1.88.0 check --workspace --all-targets --all-features
```

CI also verifies the zero-feature contracts for `runifold` and
`runifold-providers`, every native Provider feature in isolation, and the
facade's native and OpenAI-compatible Provider feature groups. A test target
that imports an optional Provider must declare the corresponding crate-level
`cfg(feature = "...")` gate.

Release verification validates the internal dependency order and assembles
every `.crate` archive with `--no-verify`. This is required for an unpublished
workspace version because downstream archives cannot resolve that version
from crates.io until their internal dependencies have been published. The
ordered publish script performs Cargo's normal package verification after each
dependency becomes available.
