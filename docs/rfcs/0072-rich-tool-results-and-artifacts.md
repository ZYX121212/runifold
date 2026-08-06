# RFC 0072: Rich Tool results and durable artifacts

## Status

Implemented. PostgreSQL artifact evidence is part of the real database suite;
the change remains unreleased until the next version is published.

## Problem

A Tool result is not necessarily JSON text. Screenshots, generated audio,
documents, resource links, structured data, application errors, and private
host metadata have different consumers and lifetime requirements. Flattening
them into one string loses media type, ordering, error semantics, and the
ability to keep large bytes out of transcripts and checkpoints.

## Canonical result

`ToolOutput` now separates four concerns:

- `content`: ordered model-visible `ContentPart` values;
- `structured_content`: the value checked against the Tool output schema;
- `metadata`: namespaced host data that is not promoted into model text;
- `is_error`: an application-level Tool result that the model may recover
  from, distinct from a runtime `ToolError`.

`model_visible` remains an explicit disclosure gate. An Agent fails closed if
a host-only result reaches its model-call path. Tool schemas compile at
registration, and invocation input, structured output, and serialized result
sizes are bounded before the next model call.

## Artifact boundary

`ArtifactStore` stores binary content behind a stable, content-addressed
`ArtifactRef`. Every reference binds the MIME type, size, and SHA-256 digest.
Writes require an idempotency key; replay with different content is a conflict.
The key and the content address are also bound to immutable display and expiry
metadata, so retry races cannot silently rewrite an existing reference. Scope
deserialization, MIME values, names, keys, and portable retention timestamps
are validated before storage. Known media types are checked against their file
signature before storage.

Tools obtain the configured store through `ToolContext::artifact_store` and
return `MediaSource::Artifact` references. `ArtifactResolvingModel` loads and
verifies bytes only immediately before Provider transport. Conversation and
checkpoint state therefore retain references instead of Base64 payloads.
The in-memory, SQLite, and PostgreSQL stores implement the same contract.

Artifact resolution is deliberately not a public download service. Every
operation requires a validated `ArtifactScope`; references bind that scope and
cannot be resolved by an Agent configured for another scope. Stores expose
bounded cursor pagination, idempotent deletion, expiry timestamps, and bounded
expiry purging. Authorization before choosing a scope, encryption at rest,
malware inspection, and signed delivery URLs remain application or
storage-adapter responsibilities.

## Protocol projection

MCP preserves text, image, audio, embedded resources, resource links,
structured content, metadata, and application-error status. Provider adapters
project only representations their wire protocol accepts. Unsupported rich
parts fail explicitly; they are never silently serialized to debug JSON.

OpenAI Responses and Gemini use native multimodal Tool-result forms. Anthropic
uses native text/image/resource forms. Bedrock preserves native JSON, image,
and document Tool results; its current SDK protocol has no audio Tool-result
variant. Text-only Chat Completions and Ollama reject unsupported media.

## Streaming and observability

The canonical stream supports bounded, independently Base64-encoded binary
chunks for image, audio, and document blocks. The accumulator refuses a block
that would exceed the artifact-size limit.

Journal and OpenTelemetry events contain only result shape: content, media,
and artifact counts plus structured/error flags. They do not copy Tool-result
text, binary bytes, Base64, artifact identifiers, or host metadata values.

## Compatibility

This is a breaking Tool API change. Code using `ToolOutput { value, ... }`
must use `ToolOutput::model_visible(value)` or construct rich `content` and
`structured_content`. The change is intended for the next semver-minor release
while Runifold remains pre-1.0. Artifact metadata builders such as
`with_name` and `with_expires_at_unix_ms` are fallible and must be propagated
or handled before calling the store.

## Evidence

Unit and integration tests cover schema compilation, bounded Tool I/O,
application errors, binary-stream accumulation, Agent preservation, MCP
round-trip behavior, Provider projections, artifact idempotency and integrity,
SQLite reopen-safe storage, PostgreSQL replay/conflict behavior, and redacted
OpenTelemetry attributes.
