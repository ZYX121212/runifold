# Runifold project charter

## Mission

Runifold provides the smallest trustworthy runtime foundation on which Rust
applications can call models, execute tools, run agents, and compose workflows.

It optimizes for correctness, explicit semantics, production observability,
testability, and long-term evolvability rather than provider count or built-in
application features.

## Product boundary

Runifold owns:

- execution identity, parent-child relationships, and causality;
- event delivery and optional durable journals;
- cancellation, deadlines, resource budgets, and structured concurrency;
- capability grants, effect classification, and policy interception points;
- lossless model content and streaming semantics;
- local and remote invocation contracts;
- deterministic and model-directed composition primitives;
- conformance fixtures and offline testing tools.

Runifold does not own:

- a vector database or opinionated RAG architecture;
- a prompt marketplace or collection of business agents;
- a proprietary model protocol;
- a mandatory memory backend;
- a GUI, hosted control plane, or proxy server;
- hidden retries, hidden context sharing, or hidden permission inheritance.

## Architectural laws

1. Core crates never depend on provider, MCP, A2A, OpenTelemetry, or transport
   wire types.
2. Provider- or protocol-specific information that cannot be normalized is
   retained as explicitly namespaced opaque data.
3. A child run cannot outlive its parent without an explicit detach operation.
4. Child runs receive capabilities and budgets by explicit grant, never by
   ambient inheritance.
5. Visible output and external side effects change retry safety and must be
   represented in runtime state.
6. Conversation history, execution journals, and semantic memory are separate
   concepts.
7. Simple APIs wrap the precise execution path; they never implement a second
   runtime.
8. Public boundaries are versioned, and fast-moving edge crates can evolve
   independently of the kernel.

## Success criteria

Runifold succeeds when a user can:

- test an agent without network access;
- reconstruct why every model, tool, and child-agent invocation happened;
- cancel a root run and observe all descendants stop;
- enforce a total token, cost, time, turn, and delegation budget;
- switch local and remote transports without changing agent logic;
- use typed local composition and schema-validated remote composition;
- inspect every feature downgrade instead of discovering silent data loss.

