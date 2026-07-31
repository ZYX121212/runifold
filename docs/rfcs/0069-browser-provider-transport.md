# RFC 0069: Browser Provider transport

## Status

Implemented for the OpenAI-compatible Responses Agent path.

## Decision

Runifold retains provider-native adapters for OpenAI-compatible, Anthropic,
Gemini and Ollama protocols. On native targets their futures and streams
remain `Send`; on `wasm32` the public asynchronous aliases reflect the
browser's single-threaded Promise and ReadableStream execution model.
No unsafe or synthetic `Send` wrapper is used around browser transport state.

The runtime exports one portable monotonic `Instant`: native targets use
`std::time::Instant`, while `wasm32` uses `web_time::Instant`. Public deadline
semantics are unchanged.

## Credential policy

The verified browser configuration is an application-controlled gateway
created with `OpenAiConfig::custom`, `AnthropicConfig::gateway`,
`GeminiConfig::gateway`, or credential-free `OllamaConfig::new`. The cassette
rejects `Authorization`, `api-key`, `x-api-key` and `x-goog-api-key`, proving
that the tested paths do not bundle Provider secrets.

Long-lived Provider keys in downloadable browser artifacts are unsupported as
a production security architecture. Provider authentication, user
authorization, quotas and model policy belong at the gateway boundary.

## Failure semantics

Browser Fetch reports an AbortError rather than a native socket timeout. The
adapter therefore combines the transport error with the monotonic invocation
deadline. An elapsed deadline maps to `DeadlineExceeded`; explicit Runifold
cancellation remains `Cancelled`.

Fetch cannot prove whether a generic failed request reached the server, so
browser transport failures are not automatically marked retry-safe. HTTP 429
remains explicitly safe and preserves exposed retry metadata.

## Evidence

CI pins Rust 1.88, `wasm-bindgen` 0.2.126, Chrome Headless Shell
150.0.7871.124 and the matching ChromeDriver. It runs a real CORS-enabled
fragmented SSE/NDJSON and WebSocket server and nine asynchronous tests inside
Chrome.

The resulting JSON records only toolchain identities, protocol, feature set,
credential policy and assertion names. It excludes prompts, response bodies,
headers, credentials, URLs and host paths.

## Non-claims

This RFC does not verify Bedrock's native SDK, Realtime audio/WebRTC, Service
Workers or arbitrary third-party CORS behavior. Those capabilities require
separate browser gates. Model discovery, Files and Batch are specified by RFC
0070; Realtime text WebSocket is specified by RFC 0071.
