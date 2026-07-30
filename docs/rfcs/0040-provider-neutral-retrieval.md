# RFC 0040: Provider-neutral retrieval

- Status: Accepted
- Scope: `runifold-retrieval`, `runifold-agent`, `runifold`

## Motivation

Agents need grounding without making the runtime own a vector database or an
opinionated RAG pipeline. Retrieval is external read authority: it may cross a
network boundary, consume provider tokens, fail, time out, and return hostile
text. Those semantics belong in the same run, capability, budget, event, and
recovery model as other Agent work.

## Decision

`runifold-retrieval` defines object-safe `EmbeddingModel` and `Retriever`
boundaries, validated documents, embeddings and queries, attributable usage,
and lifecycle context carrying invocation identity, cancellation, and
deadlines. `InMemoryVectorIndex` is a deterministic cosine-similarity reference
implementation for tests and small local datasets, not a production vector
database.

An Agent can attach static documents with `context` and dynamic sources with
`dynamic_context`. The ergonomic `prompt` path grants registered retriever
capabilities. The explicit `run` path requires the host to grant each
`RetrieverDescriptor` capability.

Retrieved text is inserted as a user-role message immediately before the
original user request. It is delimited, identified by application-owned
document IDs, and explicitly labelled untrusted data. Retrieval can never
create or modify system instructions.

Each lookup records started, completed, denied, or failed domain events.
Provider-reported usage is charged to the owning run. The runtime applies the
run deadline and cancellation even when an adapter fails to do so.

For checkpointed execution, successfully prepared context is persisted before
the first model request. Resume reuses that transcript and never repeats the
completed lookup. This avoids nondeterministic context changes and duplicate
external cost.

## Consequences

Applications can replace local memory with Qdrant, PostgreSQL/pgvector,
Elasticsearch, a keyword engine, or a hosted retrieval API without changing
Agent execution semantics. Provider embedding adapters and storage adapters
remain edge crates. Conversation history, semantic memory, and the execution
journal remain separate concepts.
