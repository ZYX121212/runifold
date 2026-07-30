# RFC 0068: WASM edge compatibility gate

## Status

Implemented.

## Decision

Runifold defines its first portable edge boundary as the provider-neutral
facade with no default features. The mandatory gate compiles `runifold` for
`wasm32-unknown-unknown` on Rust 1.85 and executes kernel safety semantics in
Node through a version-pinned `wasm-bindgen` runner.

This boundary includes core identity, cancellation, capability, budget,
effect, model, retrieval, Tool, Agent, and Workflow types. It excludes
platform adapters whose contracts require native sockets, files, threads,
databases, telemetry exporters, or server transports.

## Runtime assertions

Compilation alone is insufficient because target-specific randomness and
synchronization behavior can link successfully but fail when executed. The
edge smoke test therefore creates UUID v7 Run identities, creates an
authority-attenuated child, propagates cancellation, and verifies that a
budget rejection leaves committed usage unchanged.

The UUID dependency enables its JavaScript randomness source at the shared
workspace boundary. Its WASM-only transitive dependencies do not add a native
runtime path.

## Evidence

CI writes a bounded JSON report only after both the facade build and executable
smoke test pass. The report identifies the source revision, compiler, target,
Node runtime, test runner, package boundary, and assertion names. Missing
evidence or artifact upload fails the job.

The report excludes source paths, environment variables, credentials, model
inputs, URLs, and application data.

## Non-claims

This RFC alone does not establish browser compatibility for Provider HTTP,
streaming, WebSocket, filesystem, database, MCP server, or OpenTelemetry
adapters. RFC 0069 subsequently verifies the OpenAI-compatible Responses Agent
path in Chrome. Every remaining adapter still requires a target-specific
contract and executable browser or edge-platform gate.
