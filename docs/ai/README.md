# AI-native developer experience

Runifold's AI development contract is: understand the system, use its recommended
API, modify the correct boundary, and verify the result. Humans and coding agents
use the same underlying semantics and executable examples.

## Choose a task

1. [Create an Agent](recipes/create-agent.md)
2. [Add a typed Tool](recipes/add-tool.md)
3. [Structured output](recipes/structured-output.md)
4. [Stream an Agent](recipes/stream-agent.md)
5. [Conversation history](recipes/conversation.md) and [durable requests](recipes/durable-conversation.md)
6. [Add a Provider](recipes/add-provider.md) — contributor
7. [Add a reviewer](recipes/add-reviewer.md)
8. [Recoverable effects](recipes/add-effect.md)
9. [Workflow steps](recipes/add-workflow-step.md)
10. [Test a Provider](recipes/test-provider.md) — contributor
11. [Add retrieval](recipes/add-retriever.md)
12. [Modify Agent execution](recipes/modify-agent-loop.md) — contributor
13. [Change public API](recipes/public-api-change.md) — contributor

## Understand and diagnose

- [Architecture](architecture.md), [API map](api-map.md), [invariants](invariants.md)
- [Common errors](common-errors.md), [testing](testing.md)
- [Machine-readable manifest](../../runifold.manifest.json)
- [Framework contribution guide](../../AGENTS.md)

From an installed matching-version CLI, or using
`cargo run -p runifold-cli --` in this workspace:

```sh
runifold ai context --json
runifold ai recipe add-tool --json
runifold ai explain RF-TOOL-003 --json
runifold ai check --json
runifold doctor --ai --json --manifest-path /path/to/app/Cargo.toml
```

`context` contains the bundled recipe text so an application does not need a
Runifold checkout. Its source/document links point to the CLI release tag. Verify
the reported knowledge version against the application's dependency requirements.
`doctor --ai` inspects Cargo declarations offline, emits evidence and corrective
actions, and explains what it cannot establish. It never runs suggested repairs.
See the [CLI contract](../rfcs/0074-ai-native-developer-experience.md) for exit codes
and the distinction between declared features and runtime configuration.

## Maintain knowledge

Edit semantic descriptions in `catalog.json` and actual example/test source.
Run `python3 scripts/ai-knowledge.py --write`, then `--check`. Cargo supplies
version, workspace topology, dependency kinds and feature declarations. CI checks
regeneration, local links and runs the distinct recipe validation commands.
Generated Rust excerpts intentionally retain their source context; follow the full
source link for imports and helper definitions.

## Knowledge priority and enforcement

Current source/compiler establishes actual signatures; machine architectural rules
constrain acceptable dependencies. Read the nearest scoped guide, root guide,
current recipe, rustdoc and then RFC rationale. A mismatch is a design-drift signal,
not permission to restore an obsolete API blindly.

The canonical machine index is `runifold.ai.json`; `runifold.manifest.json` is a
generated compatibility view. The schema lives in `ai/schemas/`. Source content
fingerprints establish freshness without self-invalidating HEAD hashes. API indexing
checks curated public source declarations and a facade compile contract; it is not
an exhaustive rustdoc JSON index of every Rust symbol.

Copilot, Cursor and Claude entry files are generated pointers to canonical guides.
The root and docs `llms.txt` files are publication inputs, with versioned Markdown
source links. No live documentation-site deployment is implied by generating them.
