# Changelog

All notable changes to Runifold are documented here. This project follows
[Semantic Versioning](https://semver.org/); while the public API is below 1.0,
breaking changes require a minor-version increment.

## [Unreleased]

## [0.8.1] - 2026-08-26

### Added

- Added explicit provider strictness variants for typed model requests and
  structured Agents, preserving local typed validation and bounded repair when
  a compatible endpoint requires non-strict provider enforcement.

### Fixed

- Compiled Schemars-generated schemas into the OpenAI-compatible strict wire
  subset before local preflight and transport. The adapter now removes root
  `$schema` metadata and numeric representation hints, closes object schemas,
  requires declared properties, and applies the same behavior to Responses,
  Chat Completions, and strict function Tools without mutating canonical
  requests.

## [0.8.0] - 2026-08-26

### Added

- Added review-gated workflow generation with application-owned reviewers,
  bounded feedback-driven repair, independently attenuated generation/review
  capabilities, durable per-substage checkpoints, conservative in-flight
  recovery, and stable rejection/exhaustion failures.

### Changed

- Workflow checkpoint writers now emit schema v5. Readers remain compatible
  with schemas v3 and v4, but v0.7 workers cannot read checkpoints after a
  v0.8 worker advances them; drain old workers before upgrading.

## [0.7.2] - 2026-08-14

### Added

- Added an explicit OpenAI-versus-compatible Chat Completions dialect so the
  public OpenAI endpoint uses `max_completion_tokens` and requests streaming
  usage without imposing those fields on third-party compatible servers.
- Added an independent Responses lifecycle dialect: public OpenAI requires
  sequence numbers and explicit terminal statuses, while compatible endpoints
  validate those fields when present without guessing unsupported guarantees.
- Added atomic Chat Completions Complete decoding with the same canonical
  lifecycle, usage, refusal, reasoning, Tool-call, and finish validation as the
  streaming adapter.

### Fixed

- Corrected Responses image-tool streaming to read generated media from the
  completed output item, detect PNG/JPEG/WebP media types, and redact raw image
  payloads from provider events.
- Preserved nested `response.failed` diagnostics, normalized Chat refusal
  deltas, bounded SSE events and HTTP error bodies, and made function-tool
  strict mode an explicit locally validated contract instead of forcing it on
  arbitrary JSON Schemas.
- Preserved Responses Programmatic Tool Calling `caller` correlation across
  decoding, local Tool execution, and request replay.
- Hardened configuration URLs, headers, provider identity, and redacted debug
  output; retained status-derived failure classification for truncated HTTP
  error bodies; and made configured-provider metadata namespaces stable.
- Validated strict function and structured-output schemas recursively before
  transport, including supported keywords and formats, value types, closed
  objects, arrays, local references, nesting, and aggregate limits.
- Validated inline image base64, decoded size, supported media type, and file
  signature before sending native OpenAI image input.
- Encoded current public OpenAI Chat instructions with the `developer` role,
  preserved typed refusals, and rejected ambiguous multi-choice responses.

## [0.7.1] - 2026-08-14

### Fixed

- Stabilized the OpenAI media cancellation/deadline wire test by cancelling
  only after the cassette has observed the complete request and by reserving
  connection-establishment time separately from the delayed response body.

## [0.7.0] - 2026-08-14

### Added

- Added provider-neutral `ProviderRuntimeProfile` defaults. Concrete adapters
  publish their reviewed delivery, feature, retry, circuit, and request-option
  policy through `ProviderModel::runtime_profile`; `ProviderModelExt::runtime`
  applies it automatically, while `runtime_with_profile` is the explicit
  override boundary.
- Consolidated concrete Provider ownership in `runifold-providers`. OpenAI-
  compatible brands are named modules under the single `openai` protocol
  feature instead of facade or per-brand Cargo features, and Realtime-only
  transports are isolated behind `openai-realtime`.
- Added Hugging Face Inference Providers and explicit application-owned vLLM,
  llama.cpp, and llamafile constructors under the existing `openai` protocol
  feature, without adding brand Cargo features or hidden local-process policy.
- Exposed stable `AgentError::run_error_kind`, `retry_safety`, and
  `to_run_error` normalization so applications can consume the same failure
  taxonomy as Runifold observability without duplicating internal matches.
- Added `ProductionProfile`, `InteractiveProfile`, and `BatchProfile` as
  workload overlays that preserve Provider-owned safety settings, plus
  serializable capability audits and stable diagnostic codes/recommendations
  for one-shot failures.
- Productized deterministic fault scenarios, Model disconnects, named Tool
  failure injection, runtime reconstruction, and normalized golden traces in
  `runifold-testkit`.
- Added the read-only `runifold-ops` inspection model and `runifold` operations
  CLI for exported run inspection/tailing, side-effect-free replay evidence,
  value-free checkpoint diffs, budget explanation, and doctor checks.
- Added the provider-neutral `Reranker` and `RerankingRetriever` boundaries,
  plus the `runifold-retrieval-text` companion crate for bounded UTF-8 loading
  and stable Unicode/provenance-aware chunking.
- Added durable SQLite/PostgreSQL canonical journal paging and direct `run_id`
  CLI inspection with strictly read-only SQLite access.
- Added provider-neutral image generation, speech, and transcription tasks with
  bounded OpenAI adapters and offline wire-contract tests.
- Added exact per-model capability catalogs and typed OpenAI hosted-tool
  constructors for web/file search, Code Interpreter, image generation, and
  remote MCP.
- Added concurrent weighted reciprocal-rank hybrid retrieval, Markdown/JSON
  Lines ingestion, and a native Cohere v2 Rerank adapter behind `cohere`.

### Fixed

- Made `ResponseMode::Complete` validate the full canonical response before a
  route commits. Malformed Tool arguments and other terminal protocol failures
  can now participate in explicitly authorized retry and single-route circuit
  breaking without exposing partial output or executing a Tool.

## [0.6.1] - 2026-08-13

### Fixed

- Added the required `status: "completed"` field when replaying assistant
  messages through the Responses API, preventing OpenAI-compatible endpoints
  from rejecting repaired or continued conversations with a missing message
  status.
- Accepted `status: "incomplete"` message items only inside an incomplete
  terminal Responses envelope, preserving partial text and its canonical
  finish reason so Agent terminal repair can evaluate it instead of losing the
  invocation to a Provider protocol error. Incomplete responses containing
  function calls remain non-executable.

## [0.6.0] - 2026-08-12

### Fixed

- Preserved Responses `function_call` item identity and status across complete
  and streaming decode, canonical accumulation, checkpoint serialization, and
  subsequent request replay. Replayed calls now include `status: "completed"`
  for OpenAI-compatible endpoints, including Ark/Doubao.
- Treated `response.function_call_arguments.done` and
  `response.output_item.done` as authoritative completion events, so providers
  that omit argument deltas no longer produce an accidental empty `{}` call.
- Retained completed provider-native Responses items such as Ark web-search and
  reasoning calls as replayable opaque input items. Mixed native/function-tool
  Agent turns no longer discard provider context before the next model call.
- Rejected queued, in-progress, cancelled, incomplete, truncated, and
  status-mismatched Responses function calls before they can reach an Agent
  tool boundary. Agents now execute calls only from an explicit canonical
  `ToolCalls` terminal response.
- Made Responses streaming EOF fail closed unless `response.created` and one
  matching terminal event were observed with no unfinished function calls.
  Events arriving after completion are rejected instead of being silently
  appended.
- Applied the same post-terminal lifecycle guard to Anthropic, Gemini, and
  Ollama stream decoders, and normalized a provider `Stop` containing complete
  tool content to the canonical `ToolCalls` reason without hiding unknown or
  explicit failure reasons.
- Enforced Provider stream identity and lifecycle invariants: OpenAI Responses
  sequence numbers, complete message status and closed content blocks;
  Chat-Completions response/model and fragmented Tool identity; Gemini
  response/model identity; and Ollama model identity now fail closed on drift.
- Preserved Gemini thought signatures in both native GenerateContent and
  OpenAI-compatible `extra_content` history replay, including signed non-Tool
  parts. This prevents mandatory Gemini 3 signatures from disappearing between
  a Tool call and its result turn.
- Preserved Anthropic server Tool blocks for exact replay and continued
  `pause_turn` responses within the Agent's existing budgets. Known content
  blocks now retain non-empty start payloads, and terminal stop reasons cannot
  be duplicated.
- Classified OpenRouter-compatible errors embedded in successful HTTP streams
  as Provider failures instead of partial success, and rejected late content
  after the terminal Chat-Completions chunk while retaining usage-only tails.
- Corrected the built-in Perplexity Sonar endpoint from the retired
  `/chat/completions` path to the documented `/v1/sonar` path.
- Encoded Chat-Completions assistant Tool-call messages with protocol-correct
  `content: null` instead of an empty content array, and made Bedrock reject
  unsupported start/stop-only content blocks rather than inventing empty text.
- Bound Responses terminal envelopes to the response ID and model announced by
  `response.created`, rejecting cross-response stream contamination.
- Rejected blank model identities consistently at every HTTP Provider client
  boundary before constructing a URL or opening transport.

## [0.5.5] - 2026-08-12

- Added `CompletionRequirement` for Agent-level bounded recovery from empty
  provider terminal responses and locally invalid structured output, with all
  repair turns charged to the existing turn, Token, cost, duration, deadline,
  and cancellation budgets.
- Moved typed structured-output validation before terminal checkpoint and
  durable conversation commit; repair state and exhausted failures now survive
  checkpoint recovery without replaying completed Tool effects.
- Added safe terminal-repair events and improved Tool input-schema diagnostics
  with the Tool name, input and schema JSON Pointers, validation keyword,
  bounded scalar value, and allowed enum values.

## [0.5.4] - 2026-08-11

- Added `Agent::min_successful_tool_calls` and the matching fluent builder API,
  making successful local Tool use a fail-closed completion contract rather
  than a prompt-only convention.
- Dynamically use `ToolChoice::Required` until the execution-local minimum is
  satisfied, then restore `Auto`; failed results, delegations, provider-hosted
  Tools, and earlier conversation turns cannot satisfy the requirement.
- Reject impossible shared Tool-call budgets before model execution and retain
  correct requirement state across checkpoint effect replay without extending
  the public checkpoint schema.

## [0.5.3] - 2026-08-11

- Added first-class rich typed functions through `FunctionTool::new_rich` and
  `#[runifold::tool(output = "rich")]`, allowing ordinary async Tool handlers
  to return canonical images, audio, documents, resources, structured content,
  and application-error results without manually implementing `Tool`.
- Kept the existing JSON `FunctionTool::new` path source-compatible, exposed
  optional rich structured-output schema validation, and re-exported
  `ContentPart` from the facade for complete media construction ergonomics.

## [0.5.2] - 2026-08-10

- Isolated PostgreSQL tenant lease-limit concurrency verification from the
  multi-scenario workflow test, preventing unrelated globally claimable tasks
  from producing false failures during long reliability soaks.
- Added a mandatory, reproducible Rich Tool and durable Artifact CI gate with
  credential-free evidence across validation, Agent, MCP Sampling, native
  Provider projections, SQLite lifecycle semantics, and real PostgreSQL
  concurrent idempotency, expiration, and deletion.
- Updated the Rig comparison workspace to resolve the exact Runifold 0.5.2
  development packages.

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
