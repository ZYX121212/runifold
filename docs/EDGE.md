# Browser and edge deployment

Runifold's provider-neutral runtime plus OpenAI-compatible, Anthropic, Gemini
and Ollama Agent paths are verified on `wasm32-unknown-unknown`. OpenAI,
Gemini and Ollama embeddings are verified in the same browser runtime. The
browser transport uses Fetch, ReadableStream-backed SSE or NDJSON,
AbortController-backed cancellation and request timeouts through `reqwest`.

## Credential boundary

Do not place a long-lived Provider API key in browser source, JavaScript,
WASM, local storage, or a downloadable configuration file. Obfuscation does
not create a security boundary.

Browser applications should call an application-controlled gateway:

```rust,ignore
use runifold::{
    ProviderModelExt,
    openai::{OpenAiClient, OpenAiConfig, OpenAiWireProtocol},
};

let config = OpenAiConfig::custom(
    "application-gateway",
    "https://app.example.com/api/llm/",
    OpenAiWireProtocol::Responses,
)?;
let agent = OpenAiClient::new(config)
    .agent("browser-assistant", "application-selected-model")
    .system("Answer precisely.");

let answer = agent.prompt_text("Explain capability attenuation.").await?;
```

`OpenAiConfig::custom` sends no `Authorization` or Provider key. The gateway
owns Provider authentication, user authorization, quotas, audit, model
allowlists and abuse controls. Prefer a same-origin endpoint. If a
cross-origin gateway is unavoidable, allow only the application origin and
the required methods and headers.

Native Anthropic and Gemini protocols expose equivalent credential-free
gateway constructors:

```rust,ignore
let anthropic = AnthropicConfig::gateway(
    "https://app.example.com/api/anthropic/v1/",
)?;
let gemini = GeminiConfig::gateway(
    "https://app.example.com/api/gemini/v1beta/",
)?;
```

`OllamaConfig::new` is credential-free unless the application explicitly adds
a bearer token. A browser deployment must not add that token to a downloadable
artifact.

An explicitly short-lived, audience-restricted token can be supplied by an
application only when the upstream protocol documents browser use. It must not
be treated as equivalent to a long-lived Provider key.

## Browser policy

The application Content Security Policy should restrict `connect-src` to the
gateway. Cross-origin deployments need to allow `content-type` and
`x-client-request-id`, and expose response headers such as `retry-after` and
`x-request-id` if applications need those diagnostics.

Runifold classifies a deadline-triggered browser AbortError as
`DeadlineExceeded`, explicit caller cancellation as `Cancelled`, and HTTP 429
as a retry-safe Provider rejection. Generic browser network failures remain
retry-ambiguous because Fetch does not prove whether the server accepted a
request.

## Verified scope

The mandatory Chrome gate verifies:

- Agents consuming fragmented OpenAI Responses SSE, Anthropic Messages SSE,
  Gemini GenerateContent SSE and Ollama NDJSON through browser Fetch;
- ordered OpenAI-compatible, Gemini and Ollama embedding batches;
- OpenAI-compatible model discovery, bounded multipart file upload, and typed
  Batch create/inspect/cancel operations;
- OpenAI GA Realtime text sessions through a real browser WebSocket, including
  session creation, updates, user items, response lifecycle and text deltas;
- OpenAI GA Realtime audio and WebRTC through real browser peers, including
  microphone media, playback attachment, SDP/ICE, a local RFC 5389 STUN
  responder, typed Peer/ICE state, reconnect safety and bounded data-channel
  overflow;
- relay-only connectivity through digest-pinned coturn, followed by a real
  container-stop network partition and observable ICE disconnection;
- the automatic reconnect controller running in WASM, rotating its connection
  factory on every bounded attempt and emitting redacted lifecycle events;
- coturn stop/restart recovery with a newly allocated relay-only Peer and
  isolation proving queued events from the lost session cannot cross into the
  replacement session;
- CORS preflight and ReadableStream delivery across the native protocols;
- absence of browser `Authorization`, `api-key`, `x-api-key` and
  `x-goog-api-key`;
- cancellation while a response body is pending;
- monotonic deadline enforcement and classification;
- HTTP 429 retry safety and exposed `Retry-After`.

Service Worker execution remains outside this browser claim until it has its
own executable gate.
