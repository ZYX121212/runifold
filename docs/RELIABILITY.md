# Reliability evidence

Runifold treats a production claim as verified only when a repeatable test
exercises the relevant external boundary and runs in CI. Unit tests and mocked
responses are valuable, but they are not labelled as production verification.

## Current matrix

| Capability | Evidence | CI status |
| --- | --- | --- |
| Provider stream parsing and cancellation | Real loopback HTTP/SSE/NDJSON/binary EventStream cassettes | Mandatory |
| Provider concurrency isolation | Repeated concurrent loopback requests | Mandatory |
| PostgreSQL workflow and conversation persistence | Disposable real PostgreSQL/pgvector containers | Mandatory |
| PostgreSQL outage and restart recovery | Stop/restart of the same writable container layer | Mandatory |
| SQLite forced process-kill recovery | Parent kills a synchronized child after the durable effect boundary; completed work replays once | Mandatory |
| External Effect outcome reconciliation | Uncertain handler failures remain `Started`; completed, proven-not-executed, unresolved, and lookup-failure branches are fail-closed contracts | Mandatory |
| SQLite WorkflowStore recovery | Forced worker kill, fenced lease takeover, budget adoption, HITL, history, and fork survive reopen | Mandatory |
| SQLite concurrent workflow claim | Independent connections produce exactly one fenced winner | Mandatory |
| SQLite durable Agent conversation | Transcript and terminal checkpoint commit atomically; reopen resumes without another model call | Mandatory |
| PostgreSQL durable Agent conversation and Effect store | Real PostgreSQL transaction proves transcript/checkpoint atomicity, rollback on stale checkpoint, reconnect replay, Effect CAS, and idempotency conflict | Mandatory |
| S3 conditional immutable creation | Real pinned MinIO with Object Lock and SSE-S3 | Mandatory |
| S3 post-commit response loss | Transparent TCP fault proxy plus checksum HEAD reconciliation | Mandatory |
| S3 concurrent idempotency | 32 rounds with four writers for one batch per round | Mandatory |
| WASM provider-neutral facade build | Rust 1.88 `wasm32-unknown-unknown` compile of `runifold` without platform adapters | Mandatory |
| WASM edge kernel runtime | Node execution of identity, authority, cancellation, and budget semantics | Mandatory |
| WASM browser Agents, embeddings, OpenAI control plane, and Realtime WebSocket/WebRTC | Pinned headless Chrome with OpenAI, Anthropic, Gemini and Ollama protocols, real CORS, Fetch/WebSocket/WebRTC, fragmented streams, embeddings, model listing, multipart upload, Batch lifecycle, Realtime text/audio/media, local STUN, digest-pinned coturn relay-only peers, coturn-stop ICE partition, Peer/ICE state, reconnect safety, cancellation, deadline, and 429 cassettes | Mandatory |
| OpenAI Realtime ephemeral credential rotation | Two live `/v1/realtime/client_secrets` requests, distinct secret/session assertions, bounded TTL, and credential-free evidence | Manual opt-in |
| Ark strict structured Responses, hosted/function tools, and delivery modes | Two live Ark requests plus content-free evidence | Manual opt-in |
| AWS S3 IAM/KMS integration | Not yet verified | Planned |
| Multi-hour network/database soak | Scheduled three-hour repetition of real PostgreSQL, Effect, and Provider fault suites with credential-free evidence | Scheduled |
| Browser Service Workers | Not yet verified | Planned |
| Reproducible Rig framework benchmark | Scheduled 20×1000 paired rounds, alternating order, raw artifacts, bootstrap confidence intervals, and non-regression enforcement | Scheduled |
| Independent third-party reproduction | Requires an external maintainer to run and attest the public benchmark contract | External evidence required |

## MinIO evidence artifact

The `MinIO WORM archive` CI job writes
`target/reliability-evidence/minio-worm.json`. The report schema contains:

- schema and suite identity;
- pass result and source revision;
- exact pinned MinIO server and client image identities;
- configured stress iterations;
- concurrent request and response-loss recovery counts;
- total elapsed milliseconds.

The artifact intentionally excludes credentials, pre-signed URLs, tenant IDs,
object keys, archived payloads, and remote error bodies. CI retains the report
for 30 days.

The default CI profile performs 32 iterations, producing 128 concurrent
conditional PUT attempts and eight post-commit response-loss recoveries. Local
runs may select `1..=1000` iterations:

```bash
RUNIFOLD_MINIO_STRESS_ITERATIONS=100 \
RUNIFOLD_EVIDENCE_PATH=target/reliability-evidence/minio-worm.json \
cargo test -p runifold \
  --features archive-s3 \
  --test live_minio \
  lock_checksum_concurrency_and_reconstruction_survive_real_minio \
  -- --ignored --exact --nocapture
```

The remaining `RUNIFOLD_MINIO_*` connection variables are documented in
[Testing](TESTING.md).

## WASM edge evidence artifact

The `WASM edge runtime` CI job writes
`target/reliability-evidence/wasm-edge.json`. It cross-compiles the
provider-neutral `runifold` facade with no default features using the declared
Rust 1.88 MSRV, then executes `runifold-core` safety semantics through the
pinned `wasm-bindgen-test-runner` in Node.

The executable smoke test covers:

- UUID v7 Run identity generation through the JavaScript randomness source;
- child Run identity and authority attenuation;
- hierarchical cancellation propagation;
- atomic budget rejection without partial accounting.

The artifact records the revision, target, Rust compiler, Node runtime,
test-runner version, package boundary, and assertion names. It contains no
credentials, prompts, model data, URLs, or host paths.

## Browser Provider evidence artifact

The same CI job writes
`target/reliability-evidence/wasm-browser-provider.json` after executing the
OpenAI-compatible, Anthropic, Gemini and Ollama Agent paths,
OpenAI-compatible/Gemini/Ollama embeddings, and the OpenAI-compatible model,
file and Batch control plane, and OpenAI GA Realtime WebSocket/WebRTC in pinned
headless Chrome. A real local server exercises CORS preflight, fragmented SSE
and NDJSON, ordered embedding batches, multipart upload, Batch
create/inspect/cancel, Realtime handshake, text/audio/media lifecycle, local
RFC 5389 STUN, a pending response body, deadline abort and HTTP 429.

The browser cassette accepts only no-credential application-gateway paths and
rejects `Authorization`, `api-key`, `x-api-key` and `x-goog-api-key`. The
artifact records browser, WebDriver, Rust and test-runner identities plus fixed
assertion names. It excludes credentials, headers, prompts, responses, URLs
and host paths.

This evidence applies to the listed HTTP protocols and Realtime
WebSocket/WebRTC, including a real TURN relay, container-stop partition, and
container restart followed by a fresh relay-only Peer, old-session event
isolation, and the WASM automatic reconnect controller. Service Worker
execution remains planned.

## Live OpenAI Realtime evidence

`.github/workflows/live-openai-realtime.yml` is manual-only and requires the
repository secret `OPENAI_API_KEY`. It mints two ephemeral client secrets
using the selected Realtime model and a 10–7200 second TTL, verifies that both
the credential material and effective session identity rotate, and writes
`target/reliability-evidence/openai-realtime-live.json`.

The artifact contains only the model, requested TTL, source revision, fixed
assertion names, and pass result. The test compares secret material only in
memory. The workflow fails if the artifact contains an `ek_...` or `sk-...`
shape. Standard API keys remain server-side, as required by the
[official WebRTC guide](https://developers.openai.com/api/docs/guides/realtime-webrtc#creating-an-ephemeral-token).

This live gate is deliberately not part of pull-request CI: it requires
explicit dispatch and external API authority. A missing key is a hard failure
after dispatch rather than a skipped test.

## Claim policy

A feature is not moved to `Mandatory` from prose, mocked tests, or one local
success. It requires a bounded reproducible command, pinned dependencies,
fail-closed assertions, a CI gate, and enough artifact metadata to identify the
tested revision and environment.
