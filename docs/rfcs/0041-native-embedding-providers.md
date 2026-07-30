# RFC 0041: Native embedding providers

- Status: Accepted
- Scope: `runifold-retrieval`,
  `runifold-providers::{openai, gemini, ollama}`, `runifold`

## Motivation

A provider-neutral retrieval contract is useful only when applications can
connect it to native embedding APIs without rebuilding transport, credential,
deadline, cancellation, error, and usage handling. The adapter must remain an
edge concern and must not make retrieval core depend on provider wire types.

## Decision

`EmbeddingRequest` carries an ordered text batch and an `EmbeddingTask`.
`InMemoryVectorIndex` uses `RetrievalDocument` while building a corpus and
`RetrievalQuery` while searching it. Providers that support task tuning map
those values natively; providers without task-specific fields ignore the hint
without changing vector ordering.

Each existing provider client exposes `embedding_model(model)`. The returned
adapter owns a clone of the validated client transport and secret-safe
configuration. Configuration values themselves never perform network calls.

- OpenAI-compatible clients call `POST /embeddings`, request float vectors,
  restore response items by their explicit index, and account prompt tokens.
- Gemini calls `:batchEmbedContents`, maps task types into
  `embedContentConfig`, preserves response order, and accounts prompt tokens.
- Ollama calls `POST /api/embed`, sends native input arrays, and accounts
  prompt tokens plus provider-reported duration.

All adapters reject blank model names and invalid vectors. Empty batches
return locally without opening a connection. Optional output dimensions use
`NonZeroU32`. Gemini and Ollama expose truncation controls, with silent
truncation disabled by default.

The retrieval lifecycle context controls request identity, deadline, and
cancellation. Transport timeouts remain `DeadlineExceeded`; cancellation
remains `Cancelled`; protocol and provider failures become safe
`RetrievalError::Provider` diagnostics that never include input text or
credentials.

## Verification

Real loopback HTTP cassette tests validate endpoint paths, batch bodies,
response ordering, task mapping, usage, structured failures, body timeouts,
and credential redaction. They do not require live provider credentials.

## Consequences

OpenAI-compatible providers such as Ark, Qwen, and custom endpoints can use
the embeddings adapter when their endpoint implements the compatible
`/embeddings` contract. This RFC does not add an Anthropic embedding adapter;
applications can combine an Anthropic generation client with any
provider-neutral `EmbeddingModel`.
