# RFC 0042: Vector stores and retrieval evaluation

- Status: Accepted
- Scope: `runifold-retrieval`, `runifold-retrieval-qdrant`,
  `runifold-retrieval-pgvector`, `runifold-testkit`, `runifold`

## Motivation

Embedding providers do not define persistence, nearest-neighbor search, or
retrieval quality. Binding Agent code directly to one database prevents
replacement and makes relevance regressions invisible.

## Decision

`VectorStore` is an object-safe persistence boundary over validated
`VectorRecord` values. It exposes explicit upsert and vector search operations.
`VectorRetriever` composes any `EmbeddingModel` with any `VectorStore`, using
document-task embeddings for writes and query-task embeddings for reads.
Embedding and backend usage are combined with checked arithmetic.

Database adapters remain optional edge crates:

- `runifold-retrieval-qdrant` uses the Qdrant REST API. Arbitrary Runifold
  document identities map to deterministic UUID v5 point IDs, while the
  original identity, text, and metadata remain in reserved payload fields.
- `runifold-retrieval-pgvector` uses parameterized PostgreSQL statements and
  restricts interpolated table names to simple identifiers. Extension, table,
  and HNSW creation are explicit setup calls and never occur during retrieval.

Both adapters apply retrieval cancellation and deadlines. Query results are
bounded again at the provider-neutral composition layer even if a backend
returns too many records.

`runifold-testkit` evaluates any `Retriever` sequentially against stable
relevance judgments. It reports per-case and macro-averaged Precision@K,
Recall@K, reciprocal rank, normalized discounted cumulative gain, usage, and
host-observed latency. It stores no hidden model judgment and preserves case
ordering for reproducibility.

## Verification

Qdrant uses a real loopback HTTP cassette that validates endpoint paths,
payload identity, query bodies, response reconstruction, and credential
redaction. pgvector includes identifier-injection tests and an integration test
that starts a disposable `pgvector/pgvector:pg16` database automatically.
`RUNIFOLD_TEST_PGVECTOR_URL` remains available when CI or a developer wants to
target an externally managed database explicitly.

## Consequences

Agents depend only on `Retriever`, so applications can move between the
reference in-memory index, Qdrant, pgvector, or a future backend without
changing Agent execution. The runtime still does not own a vector database or
an opinionated chunking pipeline.
