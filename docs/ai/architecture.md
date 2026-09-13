# Architecture and extension map

The [manifest](../../runifold.manifest.json) is the generated inventory of every
crate, its dependencies, features, entry file and ownership. The [root guide](../../AGENTS.md)
provides the compact layer map; local crate guides narrow the scope.

## Canonical Agent path

`ProviderRuntime::agent` / `Agent::builder` → `Agent` →
`agent/execution.rs` → model invocation → turn review → `agent/callable.rs`
(ToolRegistry or AgentGateway, coordinated by EffectExecutor) → local completion
validation / terminal review → outcome. Streaming observes this same execution.

Within `crates/runifold-agent/src/`:

- `builder.rs`: fluent registration and build validation.
- `agent/execution.rs`: loop scheduling and model turns.
- `agent/callable.rs`: Tools, delegation, concurrency and effect integration.
- `agent/review.rs`, `terminal_review.rs`: review gates and public reviewer contracts.
- `completion.rs`, `structured.rs`: terminal requirements and typed decoding.
- `conversation/session.rs` and `conversation/session/`: durable session admission,
  request replay and summary/recovery coordination.
- `stream.rs`: public stream lifecycle; `agent/observability.rs`: observation.

For a change to Tool behavior, inspect both the registry boundary and the Agent
callable integration. For storage changes, preserve the owning contract before
changing SQLite/PostgreSQL implementations. For a provider, start at its feature
module under `runifold-providers`; canonical IR belongs to `runifold-model`.
