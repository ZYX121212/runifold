# RFC 0055: MCP response caching

- Status: implemented
- Scope: `runifold-mcp`
- Protocol baseline: MCP `2026-07-28`

## Decision

Runifold caches only the protocol-defined cacheable operations:
`server/discover`, `tools/list`, `prompts/list`, `resources/list`,
`resources/templates/list`, and `resources/read`. The cache sits above
`McpTransport`, so in-process, stdio, and Streamable HTTP clients have
identical semantics.

Modern servers always emit `ttlMs` and `cacheScope`. The default is
`0` and `private`, which is immediately stale and therefore preserves existing
behavior. Legacy results omit both fields. A server opts into freshness per
operation with `McpServer::with_cache_hint`.

## Client policy

`McpClientConfig` installs a bounded in-memory cache by default and accepts a
custom thread-safe `ResponseCacheStore`. The client caps every server-provided
TTL; the default maximum is one hour. Missing, malformed, zero, future-dated,
or expired metadata fails closed as a cache miss.

Every key contains:

- an application-selected trusted endpoint namespace;
- the exact MCP method and serialized parameters;
- either `public`, or `private` plus an authorization partition.

The generated default namespace and private partition are unique to one client
configuration. Consequently, no data is shared accidentally. Applications
must explicitly reuse both a store and endpoint namespace to share public
entries. They must reuse a private partition only for the same authorization
context.

Per-call `CacheMode` has three values:

- `Use` returns a fresh hit and otherwise fetches and stores;
- `Refresh` skips lookup but updates storage;
- `Bypass` performs neither lookup nor storage.

## Pagination and invalidation

Every pagination cursor is part of the key, so pages have independent
freshness. A list request rejected by the peer invalidates every cached page
for that operation; callers can restart traversal from the beginning without
combining two server snapshots.

`notifications/tools/list_changed` and
`notifications/prompts/list_changed` invalidate every corresponding page.
`notifications/resources/list_changed` invalidates Resource and Resource
Template list pages. `notifications/resources/updated` invalidates only the
exact `resources/read` URI. Invalidation is applied before either legacy
notification streams or modern filtered subscriptions yield the event.

TTL expiry is lazy. It does not create polling, background requests, or hidden
retries. Tool calls and all other effectful operations are never cached.
An MRTR completion produced after `inputResponses` or `requestState` is also
never cached because those inputs are deliberately absent from the base key.

## Security

Self-reported server identity is not a cache namespace or authorization input.
Private results are never looked up outside their configured authorization
partition. Public sharing is an explicit host decision. Server TTLs cannot
extend beyond the client's bound, and malformed cache metadata is equivalent
to `0/private`.

## Verification

The conformance suite proves use, refresh, and bypass behavior; notification
invalidation; public sharing only under an explicit endpoint namespace; and
private isolation between authorization partitions. Unit tests cover missing
hints and all-page invalidation.
