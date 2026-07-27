# RFC 0028: Provider transport reliability

- Status: implemented
- Scope: provider adapters and `runifold-provider-testkit`

## Decision

Provider reliability is verified through real loopback HTTP rather than only
decoder unit tests. The cassette server supports two modes:

1. ordered exchanges for exact protocol scenarios;
2. repeated concurrent exchanges for connection-pool and isolation stress.

Repeated mode creates one handler per accepted connection and records accepted
requests, completed responses, and maximum simultaneous handlers. It does not
serialize responses behind a global mutex.

## Concurrency contract

Provider clients are cloneable handles over pooled HTTP transports. Concurrent
invocations must not share decoder state, content buffers, cancellation state,
or response metadata.

Normal workspace tests execute:

- 16 concurrent OpenAI Responses streams;
- 32 concurrent Gemini SSE streams;
- 32 concurrent Ollama NDJSON streams.

Every invocation reconstructs an independent terminal response. The cassette
must observe all requests and a maximum in-flight count greater than one.
These bounded tests are deterministic enough for CI while exercising actual
socket concurrency.

## Timeout classification

An invocation deadline applies to response bodies, not only connection
establishment. A timeout from the underlying HTTP byte stream is classified as
`DeadlineExceeded`.

SSE parsing previously flattened transport errors into `Protocol`. OpenAI,
Anthropic, and Gemini now inspect the event-stream error boundary:

- underlying `reqwest` timeout becomes `DeadlineExceeded`;
- other underlying HTTP failures become `Transport`;
- invalid UTF-8 or SSE grammar remains `Protocol`.

Ollama performs the equivalent classification directly on its NDJSON byte
stream.

## Offline and truncation behavior

Connection refusal before a response is a `Transport` error. A response body
ending without its provider terminal marker never becomes a successful partial
response:

- Anthropic requires `message_stop`;
- Gemini requires candidate `finishReason`;
- Ollama requires `done: true`;
- OpenAI requires its terminal response event.

A transport may report an abrupt socket close as either transport failure or a
clean body end. Both paths fail closed because the protocol decoder also
requires a terminal marker.

## Provider crash recovery boundary

Runifold does not pretend that an interrupted remote generation can be resumed
from arbitrary token output. Once a stream has emitted a canonical event, the
router commit point forbids transparent fallback or retry because that could
duplicate visible output and provider charges.

Recovery remains safe at two separate boundaries:

- failures before the stream commit point may use the existing explicit retry
  and fallback policy;
- durable Agent, Tool, and Workflow effects resume from checkpoints without
  re-executing completed effects.

Mid-stream provider crashes therefore fail the current model invocation while
preserving the higher-level durable execution contract.
