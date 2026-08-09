# Changelog

All notable changes to Runifold are documented here. This project follows
[Semantic Versioning](https://semver.org/); while the public API is below 1.0,
breaking changes require a minor-version increment.

## [Unreleased]

## [0.5.1] - 2026-08-09

- Make durable MCP `tasks/update` review decisions provably idempotent: reject unknown or mismatched response keys, accept exact retries while the decision signal is retained, reject conflicting retries, surface stale/dead-lettered decisions, and fail closed after signal compaction removes the evidence needed to verify a replay.

- Reject malformed Base64 at both provider-neutral rich-content projection and
  native OpenAI, Anthropic, Gemini, and Ollama encoding boundaries instead of
  forwarding invalid inline media to Provider APIs.
- Reject zero MCP Task retention metadata consistently with task-creation
  policy, closing a client/server validation mismatch.
- Reject blank, control-character-bearing, and oversized MCP Task identities
  and input keys before invoking durable backends.
- Validate native Provider media URLs and MIME types, including Gemini's
  required MIME field, and reject relative, credential-bearing, fragmented,
  unsupported-scheme, or oversized references before transport.

## [0.5.0] - 2026-08-09

- Completed provider-neutral rich Tool-result projection across OpenAI,
  Anthropic, Gemini, Bedrock, and Ollama, with explicit lossless extension
  envelopes or typed rejection where a provider cannot represent a modality.
- Completed MCP Sampling context inclusion and Tool-enabled Sampling with
  bounded media validation, balanced Tool history, negotiated capabilities,
  cancellation, budget rollback, and exact terminal JSON-RPC errors.
- Added official durable task-augmented `sampling/createMessage` integration
  over `WorkflowStore`, including stable private-namespaced idempotency,
  persisted request binding, SQLite restart recovery, and approved-result
  replay.
- Added cross-instance response-approval leases using store-authoritative time,
  expiry takeover, stale-owner fencing, and compaction-protected durable
  control records for in-memory, SQLite, and PostgreSQL workflow stores.
- Expanded regression coverage for concurrent creation, reconnect, crash
  recovery, strict validation, rich media, approval non-bypass, signal
  retention, and no-default-features builds; updated provider and MCP RFCs to
  document remaining external-side-effect idempotency boundaries.

This release contains pre-1.0 breaking API changes in MCP Sampling and Tasks.

## [0.4.1] - 2026-08-06

- Allowed the OSI-approved MIT-0 license used by the `borrow-or-share`
  transitive dependency so the crates.io supply-chain gate accepts the
  verified release graph.
- Reissued the 0.4.0 rich Tool-result and Artifact release payload after its
  GitHub preflight stopped before creating a release or publishing any crate.

## [0.4.0] - 2026-08-06

- Replaced scalar Tool results with ordered rich content, separately validated
  structured output, application-error status, and host-only metadata across
  Agent, MCP, Provider, journal, and OpenTelemetry boundaries.
- Added scope-bound content-addressed artifacts with integrity verification,
  idempotent writes, bounded pagination, expiry purging, deletion, and durable
  SQLite/PostgreSQL adapters. Artifact bytes remain references in durable
  state and are resolved only at the Provider transport boundary.
- Bound scope deserialization, MIME values, idempotency keys, names, retention
  timestamps, and immutable metadata. Concurrent PostgreSQL writers now
  converge on one idempotency record without surfacing a storage race.
- Added canonical bounded binary stream events and native generated-media
  decoding for Gemini and OpenAI Responses. Provider observability payloads
  redact duplicated Base64 media.
- Added native Bedrock image and document Tool-result projection. Unsupported
  protocol combinations, including Bedrock audio and Ollama rich media, remain
  explicit errors rather than lossy stringification.

This release contains pre-1.0 breaking API changes.

## [0.3.2] - 2026-08-05

- Added a lightweight facade mode selected with `--no-default-features`, while
  preserving the complete Agent/Effect/Retrieval/Tool/Workflow runtime as the
  compatible default feature set.
- Completed the Ark Responses surface for strict tools, JSON Schema,
  reasoning, image/document input, native web search, complete and streamed
  delivery, Provider file references, and typed Files/Batch/Realtime control
  operations.
- Added PostgreSQL checkpoint and Effect CAS persistence plus an atomic
  `DurableConversationStore` commit covering transcript and terminal
  checkpoint state in one transaction. Real-container tests prove reconnect,
  idempotency conflicts, stale-checkpoint rollback, and transcript atomicity.
- Kept uncertain external-effect handler failures in `Started` state so remote
  reconciliation can resolve post-commit response loss without converting an
  ambiguous write into a false terminal failure.
- Added scheduled three-hour reliability soak and locked Rig comparison
  workflows with credential-free artifacts and aggregate non-regression gates.

## [0.3.1] - 2026-08-01

- Added stable identity reconstruction for every UUID-backed core identifier
  through `FromStr`, plus `AgentDescriptor::with_id`. The default constructor
  remains intentionally ephemeral and is now documented and regression-tested.
- Clarified that `ModelRouter` and `ProviderRuntime` are long-lived runtime
  state: clones share circuit-breaker state, while rebuilding either value
  creates a fresh circuit. A clone-sharing regression test now protects the
  contract.
- Strengthened SQLite crash-recovery evidence by synchronizing parent and child
  processes and terminating the child with `Child::kill` after the durable
  boundary. Completed effects, workflow leases, and reserved budgets recover
  without duplicate execution.
- Corrected SQLite documentation: `SqliteWorkflowStore` is a complete durable
  local `WorkflowStore`, and `SqliteStore` supports atomic durable Agent turns.
  Documented the native SQLite compatibility boundary with SQLx 0.9 and 0.8.
- Explicitly documented that PostgreSQL conversation and workflow adapters do
  not yet provide the combined atomic `DurableConversationStore` transaction.

## [0.3.0] - 2026-07-31

- `SqliteStore` now implements `ConversationStore` and the new
  `DurableConversationStore` boundary. `Agent::run_durable_conversation` and
  `resume_durable_conversation` use write-ahead Agent checkpoints, then commit
  the canonical transcript append and terminal checkpoint in one SQLite
  transaction. Reopen, idempotent completed resume, and rollback-on-conflict
  contracts are covered by tests.
- Added `SqliteWorkflowStore`, a complete durable local `WorkflowStore` using
  SQLite-authoritative time, immediate transactions, fenced lease takeover,
  persistent tenant budgets and projections, signals/HITL, cancellation,
  checkpoint history, and fork/replay state. Synchronous SQLite work runs on a
  Tokio blocking boundary; reopen, reservation takeover, concurrent claim,
  history/fork, and format-forward-safety contracts are covered by tests.

## [0.2.0] - 2026-07-31

- Raised the MSRV to Rust 1.88 and upgraded the Bedrock, time, and
  Testcontainers dependency families to remove vulnerable or unmaintained TLS,
  archive, and date-time implementations. Bedrock now selects only the modern
  AWS HTTPS client instead of also enabling its legacy Rustls transport.
- Split the MCP client, MCP server, Streamable HTTP adapter, durable workflow
  store/worker, and deterministic evaluation internals into focused private
  modules without changing their public APIs.
- Hardened the real HTTP cassette harness so an expected client timeout or
  cancellation cannot prevent later scripted exchanges from running, removing
  an order-dependent control-plane reliability test failure. A client closing
  after the complete scripted body also no longer turns the final HTTP chunk
  terminator into a false concurrent-test failure.
- Added typed OpenAI GA Realtime WebSocket sessions on native Rust and WASM,
  including validated session/text/audio/response commands, PCM24/PCMU/PCMA
  formats, bounded Base64 audio and output transcripts, lossless unknown
  events, strict response lifecycle state, bounded frames and browser receive
  queues, cancellation/deadlines, redacted short-lived client-secret creation,
  credential-free browser Gateway enforcement, and explicit ambiguous
  reconnect classification for in-flight responses.
- Added OpenAI GA Realtime WebRTC for browsers with microphone capture, remote
  autoplay attachment, a bounded `oai-events` transport reusing the canonical
  typed lifecycle, ephemeral-secret and credential-free Gateway negotiation,
  server-side multipart `/realtime/calls`, privacy-preserving safety
  identifiers, validated STUN/TURN and relay-only policy, redacted TURN
  credentials, observable Peer/ICE connectivity, phase-aware reconnect
  safety, pinned coturn relay-only connectivity, container-stop network
  partition coverage, and real pinned-Chrome SDP/media/backpressure/STUN
  coverage.
- Added a safety-first OpenAI Realtime reconnect controller with validated
  bounded exponential backoff, deterministic per-invocation jitter,
  cancellation and deadline truncation, fresh credential/SDP factory
  invocations, redacted observability events, exact attempt exhaustion, and
  mandatory stop on ambiguous in-flight responses or permanent failures.
  Browser Gateway recovery now recreates the Peer, SDP, data channel and media
  resources per attempt, retries only transient 408/429/5xx exchange statuses,
  and explicitly closes pending resources after every failed exchange.
- Added a manual-only live OpenAI Realtime canary that uses the official
  client-secret endpoint twice, verifies distinct `ek_` credentials and
  effective sessions without printing them, validates requested expiration,
  supplies the server-side safety identifier, rejects credential-shaped
  evidence, and uploads only redacted pass assertions.
- Added the stateless MCP Tasks extension with explicit capability negotiation,
  polymorphic Tool outcomes, `tasks/get`, `tasks/update`, `tasks/cancel`,
  deadline-bounded client polling, Task input handling, required Streamable
  HTTP routing headers, and an optional adapter that projects Runifold durable
  workflows without introducing a second task state machine.
- Added capability-gated `notifications/tasks` subscriptions with exact Task-ID
  filters, bounded IDs per stream, configurable refresh cadence, initial and
  reconnect snapshots, unchanged-state suppression, terminal detachment, and
  a typed client Task stream.
- Added Task lifecycle governance with strict RFC 3339 timestamp ordering,
  overflow-safe absolute retention, a typed `TaskExpired` client failure, and
  configurable minimum/maximum polling intervals that bound hostile server
  hints without misusing retention as workflow execution timeout.
- Added disposable PostgreSQL MCP Task fault injection covering durable create
  read-back, bounded outage failure, database restart and adapter
  reconstruction from `taskId`, eight concurrent state subscribers,
  idempotent cancellation, and convergent terminal snapshots.
- Added an optional fenced terminal Task retention control plane with
  tenant-scoped cleanup leases, monotonic takeover tokens, bounded
  `SKIP LOCKED` batches, atomic tombstone-plus-delete PostgreSQL CTEs,
  cursor-paginated immutable audit, active-Task protection, and stale-owner
  rejection.
- Added dynamically sharded terminal Task cleanup supervision with keyset
  tenant discovery, database-clock lease heartbeat, bounded per-tenant work
  and global concurrency, failure isolation, lock-free health snapshots, and
  identity-free OpenTelemetry cleanup metrics.
- Added optional Task tombstone lifecycle governance with auditable legal
  holds, monotonic external-export receipts, bounded prepared purge sets,
  independent four-eyes approval, late-hold revalidation, fenced crash
  takeover, atomic detailed deletion, and durable aggregate purge evidence.
- Added a fail-closed Task governance facade with tenant-scoped permissions,
  pluggable asynchronous authorization, authenticated-principal audit
  identity, lease-owner binding, idempotent archive batch delivery and crash
  replay, plus identity-free OpenTelemetry governance outcomes.
- Added a durable tenant purge-approval inbox with bounded discovery,
  independently claimed reviewer leases, fencing-token timeout recovery,
  durable rejection reasons, principal-bound decisions, and low-cardinality
  OpenTelemetry operations.
- Split the PostgreSQL tombstone adapter into private hold/export, approval,
  purge/evidence, and shared decoding boundaries without changing its public
  store contract or atomic SQL behavior.
- Added an optional S3-compatible tombstone archive with pre-signed least
  authority, stable conditional object creation, SHA-256 reconciliation,
  SSE/KMS protection, Object Lock retention, and real HTTP replay tests.
- Added a native, SDK-independent S3 SigV4 pre-signer with temporary-token
  support, custom/path-style endpoints, signed protection headers, complete
  credential redaction, and concurrent same-batch reconciliation tests.
- Added a mandatory real-MinIO CI gate for immutable tombstone archives,
  covering concurrent creation, process reconstruction, checksum conflict,
  COMPLIANCE Object Lock retention, and versioned-object verification without
  environment-gated test skips. PUT and reconciliation HEAD connection pools
  are isolated so early conditional responses cannot poison recovery.
- Added bounded S3 archive PUT/HEAD requests, stable configuration,
  authorization, timeout, unavailable, integrity, and ambiguous failure
  classes, plus real MinIO fault injection that drops a successful PUT
  response after commit and requires automatic checksum reconciliation.
- Added a mandatory MinIO reliability evidence gate with 32 four-writer
  conditional-create rounds, repeated post-commit response loss, pinned
  environment identities, bounded machine-readable JSON, and retained CI
  artifacts. A public reliability matrix distinguishes verified behavior from
  planned AWS, WASM, soak, and independent benchmark evidence.
- Added a mandatory Rust 1.88 WASM edge gate that compiles the provider-neutral
  facade for `wasm32-unknown-unknown`, executes UUID identity, authority
  attenuation, hierarchical cancellation, and atomic budget semantics under a
  pinned Node test runner, and retains a non-sensitive machine-readable CI
  artifact.
- Added a mandatory pinned-Chrome browser gate for OpenAI-compatible,
  Anthropic, Gemini and Ollama Agent streaming plus native embedding paths,
  OpenAI GA Realtime text/audio WebSocket and client-secret control plane,
  with real CORS, Fetch, fragmented SSE/NDJSON, credential-free gateway
  enforcement, in-flight cancellation, monotonic deadlines and retry-safe
  HTTP 429 classification. Browser futures retain honest single-threaded
  semantics, native futures remain `Send`, and the verified
  application-gateway path rejects bundled Authorization credentials.
- Added a typed OpenAI-compatible control plane for model discovery, bounded
  multipart file upload, and Batch create/inspect/cancel. Every operation
  preserves cancellation, monotonic deadlines, Provider diagnostics and the
  credential-free browser Gateway boundary.
- Workflow task inspection now exposes store-authoritative creation/update
  timestamps, definition identity, version, and safe terminal failure detail
  consistently across in-memory and PostgreSQL stores.
- Added transport-independent MCP response caching for the 2026-07-28
  cacheable operations, including explicit server TTL/scope policy, bounded
  client TTLs, private authorization partitions, opt-in public sharing,
  per-call use/refresh/bypass modes, independent pagination entries, exact
  Resource keys, and notification-driven invalidation.
- Added MCP 2026-07-28 Multi Round-Trip Request support with bounded
  whole-operation deadlines, fresh request IDs per attempt, opaque
  `requestState` echoing, host-controlled Sampling/Elicitation/Roots input
  resolution, per-request capability enforcement, and stateless Tool
  preflight gates that execute the canonical Tool only after input completion.
- Added modern `subscriptions/listen` over POST/SSE, stdio, and in-process
  transports with explicit filters, first-message acknowledgment, request-ID
  correlation, concurrent subscriptions, drop cancellation, exact Resource
  authorization, and no HTTP session allocation.
- Completed MCP 2026-07-28 schema-driven Tool parameter headers. Streamable
  HTTP clients compile nested `x-mcp-header` declarations, exclude malformed
  tools, safely encode `Mcp-Param-*` values, and cache rules for concurrent
  calls; servers independently validate header/body equality and return the
  protocol-defined `HeaderMismatch` response before Tool execution.
- Consolidated first-party HTTP model integrations into the feature-gated
  `runifold-providers` crate. OpenAI-compatible, Anthropic, Gemini, and Ollama
  retain their facade module paths while heavyweight future SDK integrations
  remain eligible for separate companion crates.
- Added Azure OpenAI v1 Responses support with resource API-key and
  application-provided Entra bearer authentication, optional preview
  selection, canonical provider identity, and real HTTP conformance coverage.
- Added a native Amazon Bedrock Converse Stream adapter backed by the AWS SDK,
  with `SigV4`, temporary-credential support, canonical Tools and reasoning,
  detailed usage, strict event lifecycle validation, cancellation, deadlines,
  and one Runifold-owned retry authority.
- Added real loopback HTTP Bedrock binary EventStream cassettes covering
  `SigV4`, temporary-credential redaction, fragmented frames, truncated
  streams, deadlines, and concurrent invocation isolation.
- Added a framework-neutral Provider benchmark contract with bounded
  concurrency, warmup, TTFT and total-latency distributions, throughput,
  normalized failure counts, reproducibility metadata, JSON reports, and
  explicit baseline regression policies.
- Added a standalone release-mode Rig 0.40 comparison executor using equivalent
  loopback OpenAI SSE cassettes, captured-request validation, alternating
  paired rounds, deterministic bootstrap confidence intervals, aggregate
  evidence gates, and timestamped raw JSON artifacts without affecting the
  main workspace dependency graph.
- Optimized the common Provider hot path by removing boxed cancellation races,
  retaining one cancellation listener across SSE chunks, fast-pathing
  text-only capability validation, directly encoding single-text messages,
  keeping common Chat decoder events inline, omitting redundant automatic Tool
  choice, and unifying the workspace on `reqwest` 0.13 without raising the Rust
  1.88 MSRV.
- `RunContext::child` now rejects capabilities absent from the parent instead
  of relying on each orchestration layer to enforce attenuation.
- `RunContext::child_reserved` now validates both capability authority and
  reservation ownership through the typed `ChildRunError` boundary.
- Added an asynchronous `WorkflowStore` control-plane contract with atomic
  claims, authoritative leases, heartbeats, delayed retries, and fencing.
- Added an optional PostgreSQL workflow-store adapter using database time and
  `FOR UPDATE SKIP LOCKED` task selection.
- Distributed workflow checkpoints now use lease-fenced asynchronous CAS, so
  a superseded worker cannot overwrite recovery progress.
- Added a definition registry and one-task worker runtime with automatic
  heartbeat supervision, lease-loss cancellation, checkpoint resume, and
  explicit failure policy.
- Added a production worker supervisor with bounded concurrency, exponential
  idle/error backoff, graceful shutdown draining, and lock-free operational
  metric snapshots.
- Added durable timer and external-signal workflow nodes with lease-free waits,
  store-authoritative wakeup, buffered idempotent signals, fenced recovery, and
  PostgreSQL persistence.
- Added durable signal-or-timeout races with typed outcomes, idempotent external
  cancellation, payload-safe signal inspection, dead-letter lifecycle, and
  retention compaction that never deletes pending delivery.
- Added durable human-in-the-loop interrupts with checkpointed prompts and
  proposals, inspectable pending state, typed approve/edit/reject decisions,
  idempotent delivery, tenant isolation, and PostgreSQL crash recovery.
- Added immutable workflow checkpoint history with bounded pagination, exact
  revision inspection, idempotent fork/replay, explicit ambiguous-step retry
  authority, durable lineage, isolated branch identities, and PostgreSQL
  history capture.
- Added a typed `ConversationStore` boundary with append-only canonical
  transcripts, optimistic turn commits, monotonic summaries, explicit
  summary-buffer backpressure, bounded windows, provenance-required
  cross-conversation semantic memory, and an in-memory reference adapter.
- Agents can now run bounded conversational turns that load prior context,
  inject summaries and semantic memory only as untrusted transient data,
  preserve successful outcomes on commit conflicts, and never mix retrieval
  context or execution-journal events into the durable transcript.
- Added a PostgreSQL `ConversationStore` with explicit schema setup, atomic
  transcript compare-and-swap, monotonic summaries, namespace isolation,
  provenance-validated semantic memory, lexical search, and reconnect/concurrent
  writer integration coverage against a real PostgreSQL container.
- Replaced silently environment-gated PostgreSQL integration tests with shared
  disposable pgvector containers, while retaining explicit URL overrides, and
  added bounded database-stop/restart fault injection that verifies typed
  outage failure and committed transcript recovery.
- Added `ConversationSummarizer`, an Agent-backed automatic summary path, and
  `AutomaticConversationSummary`. Summary generation shares the caller's
  cancellation, deadline, budget, authority, and journal context; transcript
  data is explicitly untrusted and stale summary commits fail by CAS.
- Conversation views now bound summary batches independently from live
  windows, expose the unloaded `summary_backlog`, and enforce an explicit
  automatic summary pass limit before the main model can run.
- PostgreSQL semantic memory can now use an explicitly configured embedding
  model, native pgvector storage, cosine HNSW search, atomic memory/vector
  upserts, retrieval-scoped cancellation and deadlines, and attributable
  embedding/database usage. The lexical path remains available without an
  embedder.
- Split the PostgreSQL conversation adapter into focused schema,
  transcript/summary, semantic-memory, trait-persistence, and codec/validation
  modules, reducing its public configuration facade from 926 to 107 lines
  without changing the `ConversationStore` API.
- Replaced the monolithic PostgreSQL crate root with a seven-line stable
  adapter facade and moved workflow budget representation/conversion plus
  validation and error normalization into focused internal modules without
  changing the public `PostgresWorkflowStore` API.
- Moved PostgreSQL workflow row decoding, task and budget snapshots, signal
  state mapping, and durable fork storage DTOs into a dedicated internal codec
  module.
- Split PostgreSQL workflow SQL into focused internal modules for core schema,
  tenant-budget tables and database functions, and claim/heartbeat/signal
  runtime statements, leaving a five-line SQL module facade.
- Isolated lease-fenced claim, heartbeat, suspend, retry, and terminal finish
  transitions in a dedicated workflow lifecycle module while retaining the
  object-safe `WorkflowStore` facade.
- Isolated durable signal publication, idempotent replay, cancellation,
  inspection, dead-lettering, retention compaction, and the signal-backed HITL
  delivery path in a dedicated internal module.
- Isolated workflow inspection, checkpoint history and revision lookup,
  fork/replay preparation, durable lineage, checkpoint loading, and
  lease-fenced compare-and-swap in a dedicated internal extension trait,
  bringing the PostgreSQL workflow orchestration root below 1,000 lines.
- Isolated PostgreSQL tenant-budget policy, audit, projection leases,
  reservation, and settlement in a dedicated internal control-plane module,
  reducing the workflow orchestration root to a 477-line queue facade.
- Fixed PostgreSQL signal suspension JSON typing and serialized same-tenant
  claim admission with transaction advisory locks, closing a real concurrent
  lease-limit race while retaining cross-tenant claim parallelism.
- Added explicit workflow tenant identities, tenant-scoped control-plane
  operations, atomic outstanding/concurrent admission limits, and fair
  cross-tenant claims for both in-memory and PostgreSQL stores.
- Added persistent tenant budget ledgers with pre-execution envelope
  reservation, actual-usage settlement, fenced crash takeover, conservative
  expiry/cancellation forfeiture, and atomic PostgreSQL concurrency control.
- Added cursor-paginated durable tenant-budget audit facts plus low-cardinality
  OpenTelemetry decision, resource, utilization, and reservation-age metrics,
  with Prometheus alerts and a Grafana budget panel.
- Added bounded restart-safe budget audit projection with named durable
  cursors, monotonic compare-and-set, explicit at-least-once delivery, and
  retention guards that protect the slowest registered projection.
- Added database-clock projection leases with heartbeats, fencing-token
  takeover, bounded continuous supervision, cancellation-safe release, and
  low-cardinality projection lease-loss telemetry.
- Split budget metrics, bounded projection, and lease supervision into focused
  modules, and added lock-free live supervisor health snapshots for readiness
  and control-plane inspection.
- Added stable paginated budget-tenant discovery and a deterministic sharded
  multi-tenant projection coordinator with bounded concurrency, dynamic
  rescans, isolated tenant failures, and lease-safe rebalancing.
- Added one-shot `AgentBuilder::prompt` and `prompt_text` golden paths that
  preserve typed build and runtime failures.
- Added automatic least-authority root contexts for ergonomic Agent prompts.
- Added canonical text extraction on model responses and Agent outcomes.
- Provider clients now expose concise validated constructors for common
  credentials and local endpoints without conflating configuration values
  with runtime clients.
- Added provider-neutral embedding and retrieval contracts plus a deterministic
  in-memory cosine index for tests, small datasets, and adapter conformance.
- Agents now support static and capability-gated dynamic context as explicitly
  untrusted data, with usage accounting, events, deadlines, cancellation, and
  checkpoint-safe recovery that never repeats a completed lookup.
- Added native batched embedding adapters for OpenAI-compatible endpoints,
  Gemini, and Ollama, including task-aware retrieval tuning, optional output
  dimensions, usage attribution, cancellation, deadlines, and real HTTP
  cassette coverage.
- Added a replaceable `VectorStore` boundary and `VectorRetriever` composition,
  plus optional Qdrant REST and PostgreSQL/pgvector adapters.
- Added deterministic retrieval evaluation with Precision@K, Recall@K, MRR,
  nDCG, usage evidence, and observed latency.

## [0.1.0] - 2026-07-27

- Initial typed runtime for model calls, tools, agents, durable workflows,
  native providers, MCP, persistence, evaluation, and OpenTelemetry.
- Release engineering gates for MSRV, public API compatibility, dependency
  security policy, CycloneDX SBOMs, package verification, and controlled
  crates.io publication.

[Unreleased]: https://github.com/ZYX121212/runifold/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ZYX121212/runifold/releases/tag/v0.1.0
