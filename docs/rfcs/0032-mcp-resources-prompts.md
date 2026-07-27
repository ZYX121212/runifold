# RFC 0032: Capability-safe MCP Resources and Prompts

- Status: implemented
- Scope: `runifold-core`, `runifold-mcp`
- Protocol baseline: MCP `2025-11-25`

## Decision

Runifold implements MCP Resources and Prompts as one context-discovery layer
over the existing lifecycle and transport boundary.

Resources are application-controlled context identified by absolute URIs.
Prompts are user-controlled message templates. Neither surface grants model
authority, invokes a model, or automatically mutates a model request.

The implemented methods are:

- `resources/list`;
- `resources/read`;
- `prompts/list`;
- `prompts/get`.

Resource templates, pagination, subscriptions, list-change notifications,
completion, Sampling, Elicitation, and Tasks are not advertised by this slice.

## Domain boundaries

`ResourceRegistry` stores object-safe `ResourceHandler` implementations in
deterministic URI order. Each `ResourceDescriptor` owns a stable
`CapabilityId`, protocol metadata, semantic version, host-selected risk, and
host-only metadata. Reading is always classified as `EffectClass::ReadOnly`.

`PromptRegistry` stores object-safe `PromptHandler` implementations in
deterministic name order. Each `PromptDescriptor` is a grantable
`CapabilityKind::Prompt` with an input schema derived from declared string
arguments. Rendering is classified as pure by default, while the host retains
an explicit risk classification.

`StaticTextResource` covers immutable documentation and configuration.
`FunctionPrompt` covers short, non-blocking Rust rendering closures. Handlers
that perform asynchronous work implement the object-safe traits directly.

## Authority

Listing filters entries against the server `RunContext`. Read and render repeat
the capability check immediately before execution and create a child run
containing exactly the selected Resource or Prompt capability.

Unknown and unauthorized resources both return JSON-RPC resource-not-found.
Unknown and unauthorized prompts both return invalid-prompt-name. This keeps
discovery from becoming an authority side channel.

The server advertises `resources` or `prompts` only when the corresponding
registry is configured. It advertises neither subscription nor list-change
support.

## Validation and limits

Resource descriptors require absolute, parseable URIs. Every returned content
part must retain the requested URI. Binary content is decoded before leaving
the server to prove valid base64 and enforce the decoded-size limit. Empty,
mismatched, malformed, and oversized outputs fail closed.

Prompt registration rejects blank and duplicate argument names. Rendering
rejects missing required arguments and undeclared arguments before invoking
the handler. Empty message lists, excessive message counts, and oversized
serialized results fail closed.

Arguments and rendered content are protocol values, not trusted instructions.
Applications decide whether and where to expose returned content to users or
models.

## Client behavior

`McpClient` verifies negotiated capabilities before issuing Resource or Prompt
requests. This produces a local protocol failure instead of sending a method
the server did not advertise.

List methods currently reject a returned continuation cursor. This is
deliberate: the client does not silently return an incomplete discovery set.
Pagination will be added as an explicit stream or page API.

Prompt retrieval returns `GetPromptResult` to the host. It never inserts
messages into an Agent transcript automatically, preserving user control and
making prompt-injection review an application decision.

## Verification

The conformance suite runs the same operations over:

- the in-process transport;
- multiplexed stdio;
- real loopback Streamable HTTP.

Tests cover capability-filtered discovery, concealed unauthorized reads,
required and unknown prompt arguments, URI validation, decoded resource size,
invalid base64, and concurrent Resource and Prompt requests.
