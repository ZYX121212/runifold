# RFC 0027: Native Gemini and Ollama providers

- Status: implemented
- Scope: `runifold-provider-gemini`, `runifold-provider-ollama`, `runifold-model`

## Decision

Gemini and Ollama use their native protocols. Neither adapter translates
through OpenAI compatibility:

- Gemini calls `models/{model}:streamGenerateContent?alt=sse`;
- Ollama calls `/api/chat` and incrementally frames NDJSON.

Both implement the provider-neutral `Model` boundary and reuse the offline
real-HTTP cassette from RFC 0026.

## Canonical tool-result identity

The original canonical `ToolResult` retained only `call_id`. Gemini
`functionResponse` and Ollama tool messages also need the function name.
Guessing from prior transcript state inside each provider would make requests
order-dependent and complicate checkpoint recovery.

`ToolResult` therefore adds:

```rust
pub name: Option<String>
```

The Agent executor writes the invoked tool name. The field has a Serde default,
so checkpoints created before this RFC continue to deserialize. Providers
which correlate only by call ID ignore it.

## Gemini

The adapter supports native:

- system instructions and user/model contents;
- text, inline media, file media, and signed thought round trips;
- function declarations, calls, and responses;
- JSON mode and JSON Schema response configuration;
- temperature, top-p, output limit, and stop sequences;
- usage metadata and finish-reason normalization;
- prompt blocks and structured HTTP errors.

The SSE stream must end with a candidate `finishReason`. Additional candidates
and unknown parts remain observable as provider events rather than being
silently merged into candidate zero.

## Ollama

The adapter supports native:

- local daemon and custom hosted endpoint configuration;
- optional bearer authentication with secret-safe debug output;
- system, user, assistant, and tool chat messages;
- inline base64 images;
- model thinking and text deltas;
- function tools and tool calls;
- JSON and JSON Schema output formats;
- model options including seed, token limit, temperature, top-p, and stops;
- prompt/evaluation token usage and duration metadata.

NDJSON framing is independent from HTTP chunk boundaries. A JSON object may be
split across transport chunks, and multiple objects may arrive together.
Successful completion requires an explicit `done: true` object. Mid-stream
`error` objects fail the invocation.

Ollama currently has no portable native equivalent for forced or named tool
choice, so strict requests for those modes fail locally.

## Verification

Offline real-HTTP tests cover Gemini text, usage, function calls, structured
errors, and API-key redaction. Ollama tests cover split NDJSON frames,
thinking, text, usage, tool calls, and mid-stream runner failure.
