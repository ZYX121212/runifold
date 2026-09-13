# RFC 0074: AI-native development contract

- Status: implemented (evaluation infrastructure; tool trials and live-site deployment are separate evidence)
- Scope: documentation, knowledge generation, diagnostics, CLI, CI and evaluations

## Decision

Runifold exposes a versioned development interface for coding agents: bootstrap
instructions, scoped guides, task recipes, machine indexes, stable diagnostics,
affected-boundary validation and reproducible evaluations. These reuse runtime
semantics and never create another Agent engine.

One canonical set of explicit semantic facts plus Cargo/source/test metadata
produces the machine manifest, scoped guidance, recipe excerpts and agent-specific
adapters. Generated output is checked in for discovery and packaged into the CLI
so application developers can retrieve recipes without cloning the repository.

Knowledge is loaded progressively: root guide, owning crate guide, one recipe,
then API/rationale references. Source/compiler describe current implementation;
architecture checks constrain acceptable implementation. When these disagree,
report design drift and consult current release/RFC history, rather than silently
rewriting code to match an old RFC.

## CLI contract

`runifold ai context [--json]` provides a compact task/architecture index.
`runifold ai recipe <task> [--json]` returns the bundled task document and evidence.
`runifold ai explain <code> [--json]` resolves stable diagnostics into causes,
corrective actions and validation. `runifold ai check [--execute] [--json]` selects
validation from changed files and reverse workspace dependencies and runs machine
architecture rules. Execution is explicit, uses argument vectors without a shell,
and records command status. Semantic invariants remain review/test obligations.

`runifold context [--task <id>] [--json]` remains a convenience alias.
`runifold doctor --ai [--json] [--manifest-path <Cargo.toml>]` inspects Cargo
workspace packages and normal dependency declarations. It uses
`cargo metadata --offline --locked --no-deps --format-version 1`, reads no provider
credentials, and runs no suggested repair. JSON schema version is numeric 1.

Doctor exit 0 means its bounded checks passed (warnings may remain). Exit 1 means
a failed check, including unavailable/invalid Cargo metadata or no Runifold
integration. Argument errors use Clap's exit 2. Operational `doctor --events`,
`--sqlite` and `--postgres` retain their existing behavior and JSON format; they
cannot be mixed with AI mode. `--json` is also accepted for operational doctor.

Declared feature lists are not resolved feature sets. Optional and target-specific
dependencies may be inactive. Requirement comparisons recognize only explicit
numeric release families; ranges and prereleases are unknown. Missing features
in bundled knowledge are warnings, because a newer patch or local fork may add
features. Runtime Tool registration, store injection, write classification and
external recovery cannot be inferred from Cargo declarations and are not checked.

## Freshness and compatibility

Generate metadata from actual workspace Cargo configuration and extract code from
compiled source. Content fingerprints bind the relevant knowledge inputs. Do not
require an embedded current HEAD hash: committing generated files changes HEAD and
would otherwise make every new commit stale. No passing verification timestamp is
invented by generation; CI execution is separate evidence.

Existing error Display and serialization remain compatible. Additive
`diagnostic_code()` accessors expose RF identifiers; existing run error codes keep
their current values and are recognized as explanation aliases. RF codes describe
library/runtime diagnostics, not custom rustc compiler diagnostics.

## Validation and measurement

CI verifies regenerated artifacts, schema/links, architecture rules, public symbol
references and source fingerprints, and executes recipe checks. The AI evaluation
suite stores tasks, immutable baseline/patch references, command evidence and
explicit human-review grades. Unrun tasks and unavailable tools have no score.
Compilation alone cannot certify architectural correctness or claim compatibility
percentages for Codex, Claude Code, Cursor or Copilot.

## Boundaries

Instructions are guidance; authority enforcement, effect safety, types and tests
remain the enforcement mechanisms. Static dependency rules only establish dependency
isolation, not absence of every possible wire-shaped type or hidden retry. Website
Markdown and llms indexes are generated publication inputs; a live site requires
its own deployment and verification.
