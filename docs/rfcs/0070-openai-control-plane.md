# RFC 0070: OpenAI-compatible model, file, and Batch control plane

## Status

Implemented.

## Decision

`OpenAiClient::control_plane()` returns a cloneable `OpenAiControlPlane` that
shares the client's validated endpoint, pooled HTTP transport and credential
policy. It exposes model discovery, bounded file upload, Batch creation,
single-read inspection and cancellation.

The API uses typed inputs and outputs:

- `OpenAiFileUpload` validates a basename, non-empty body and a 512 MiB limit
  before opening the transport;
- `OpenAiFilePurpose` validates a bounded forward-compatible token;
- `OpenAiBatchEndpoint` prevents arbitrary Batch request targets;
- `OpenAiBatchRequest` bounds metadata count, key size and value size;
- `OpenAiBatchStatus::Unknown` preserves future Provider lifecycle states.

The library does not poll implicitly. Durable workflows can checkpoint the
returned Batch identity and schedule explicit `get_batch` calls according to
their own deadline, budget and retry policy.

## Failure semantics

Every operation receives `ModelCallContext`. Pre-existing cancellation and
elapsed deadlines fail before transport. Cancellation races both request and
response-body work. Browser Fetch aborts are classified as
`DeadlineExceeded` when the monotonic deadline has elapsed.

Provider rejection retains the HTTP status and exposed request identity.
Errors never include uploaded bytes or credentials. Successful malformed JSON
is a protocol error rather than partial success.

## Browser credential boundary

The verified browser path uses `OpenAiConfig::custom` and an
application-controlled gateway. The Chrome cassette rejects `Authorization`
and Provider API-key headers. Model listing, multipart upload and Batch
create/inspect/cancel therefore execute without a long-lived Provider secret
inside WASM or JavaScript.

## Evidence

Native loopback cassettes validate exact methods, paths, JSON bodies,
multipart fields, deadline behavior and Provider diagnostics. The mandatory
pinned-Chrome gate runs the complete model → file → Batch lifecycle through
real CORS and Fetch and records fixed assertions in the browser reliability
artifact.

## Non-goals

This layer does not add automatic Batch polling, file persistence, Realtime,
audio, image generation or Service Worker lifecycle management.
