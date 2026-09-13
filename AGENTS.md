# Runifold Agent Guide

Runifold is a typed, observable, cancellable and budget-aware Rust runtime for
models, Tools, Agents and durable workflows. AI-native developer experience is
part of the product: make correct use and correct modification easy to discover,
execute and verify. Treat recurring AI mistakes as DX defects to investigate.

## Start here

- Building an application: use [task recipes](docs/ai/README.md).
- Modifying the framework: read this file, then the affected crate's `AGENTS.md`.
- Machine-readable architecture, features, recipes and checks:
  [runifold.ai.json](runifold.ai.json).
- Source of architectural policy: [project charter](docs/CHARTER.md).
- Current public types: crate exports and rustdoc. RFCs explain intent and history;
  older snippets can predate current APIs. Do not copy them without checking.

## Architecture map

| Layer | Crates | Boundary |
| --- | --- | --- |
| Public facade | runifold | Recommended public imports and ProviderRuntime; no second runtime |
| Kernel | runifold-core | Identity, authority, budgets, cancellation, events, checkpoints |
| Model | runifold-model | Neutral content, streams, invocation, routing and retry |
| Execution | runifold-agent, runifold-tool, runifold-effect, runifold-workflow | Canonical orchestration, registry, write-ahead effects, durable steps |
| Code generation | runifold-macros | Typed Tool expansion into the existing tool runtime |
| Providers / protocols | runifold-providers, runifold-mcp | Wire adaptation at the edge |
| Retrieval | runifold-retrieval, runifold-retrieval-text, runifold-retrieval-qdrant, runifold-retrieval-pgvector | Neutral contracts and separate adapters |
| Persistence | runifold-store-sqlite, runifold-store-postgres | Store implementations, not execution policy |
| Observability / operations | runifold-observability-otel, runifold-ops, runifold-cli | Projection, inspection and development diagnostics |
| Testing | runifold-testkit, runifold-provider-testkit, runifold-eval-cli | Scripted models, wire cassettes and evaluations |

The manifest lists each crate's ownership, exclusions, actual workspace dependency
kinds and Cargo features. See [architecture](docs/ai/architecture.md) for entry files.

## Architectural laws

1. **RF-LAW-001:** Provider, MCP and transport wire types stay out of neutral core contracts.
2. **RF-LAW-003:** Child capabilities and budgets are granted explicitly and cannot amplify the
   parent's authority. Preserve structured cancellation and deadline propagation.
3. **RF-LAW-002:** Simple and advanced APIs share the canonical execution path.
4. **RF-LAW-004:** Visible output and external effects change retry safety. Never hide retries
   or automatically repeat an ambiguous non-idempotent write.
5. **RF-LAW-005:** Conversation history, execution journals and semantic memory remain separate.
6. Preserve canonical content and namespaced provider data; do not flatten rich
   Tool results into strings or silently discard unsupported semantics.
7. Persisted execution identity, version, lease/fencing and commit boundaries are
   contracts. Test recovery rather than inferring it from successful execution.

## Common tasks

| Task | Start / verify |
| --- | --- |
| Create an application Agent | [create-agent](docs/ai/recipes/create-agent.md); long-lived ProviderRuntime |
| Add a Tool | [add-tool](docs/ai/recipes/add-tool.md); macro, descriptor, registry, safe errors |
| Add a Provider | [add-provider](docs/ai/recipes/add-provider.md), then [test-provider](docs/ai/recipes/test-provider.md); feature isolation and real loopback fixtures |
| Change Agent execution | `crates/runifold-agent/src/agent/execution.rs`; callable/review/completion siblings; run and stream must agree |
| Change callable dispatch | `crates/runifold-agent/src/agent/callable.rs`; preserve capability preflight, Effect identity and Tool/Gateway errors |
| Add public API | Owning crate, its `lib.rs`, facade re-export if appropriate, rustdoc, public-surface regression test, changelog and relevant RFC |
| Add durable state | Store contract first, backend implementations next; replay, conflict, crash and migration tests |
| Add a test | Local invariant near implementation; cross-crate public API in `tests/`; provider protocol with provider-testkit |
| Change AI knowledge | Edit `docs/ai/catalog.json` / source; regenerate with `python3 scripts/ai-knowledge.py --write` |

## Public API and error rules

Prefer `ProviderModelExt -> ProviderRuntime -> AgentBuilder -> Agent` for hosted
application Agents. `Agent::builder` / `Agent::new` are appropriate for explicit
model injection and offline tests. They use the same execution machinery.
Describe context lifetime and authority in rustdoc's first sentence.

Use typed structs/enums and named errors at library boundaries. Preserve stable
error category and retry safety; add a concrete corrective action when known.
Keep credentials, prompts and private payloads out of ordinary error formatting.
Tool application errors must explicitly implement `IntoToolError`.
Do not fix a failing example by widening permissions or weakening validation.
Do not break a public API in a patch release merely to shorten a recipe.

## Required validation

Run the affected crate guide's tests and the regression test for the changed
boundary. Format with `cargo fmt --all -- --check`; run applicable Clippy with
`--all-targets --all-features --locked -- -D warnings`.
For public docs run `RUSTDOCFLAGS='-D warnings' cargo doc --workspace --all-features --no-deps --locked`.
For AI knowledge run `python3 scripts/ai-knowledge.py --check` and the recipes'
listed compilation/tests. The CI AI gate executes these commands automatically.

Full tests include real PostgreSQL containers. Read [testing](docs/ai/testing.md)
and [CI](.github/workflows/ci.yml) before choosing a full gate. Report any unavailable
Docker, target, credential or toolchain prerequisites honestly. Live vendor tests
remain opt-in; offline recipe checks need no vendor credentials.

## Do not

- Introduce a separate AI execution engine, hidden Tool bypass or alternate Agent loop.
- Invent methods, Cargo features or macro signatures from naming conventions.
- Make doc claims unsupported by a current example or test.
- Hand-edit generated guides, manifest, recipe excerpts or the CLI knowledge bundle.
- Copy a version from a prompt into metadata: derive it from Cargo.toml.
- Revert unrelated local changes or modify credentials to make checks pass.

## Documentation maintenance

Keep task instructions concise and specific. AI and human documentation use the
same code and semantic sources. A behavior change updates its regression test,
relevant RFC and CHANGELOG. Review whether it also changes the preferred API,
recipe, diagnostic or crate ownership. See [AI DX contract](docs/rfcs/0074-ai-native-developer-experience.md).
