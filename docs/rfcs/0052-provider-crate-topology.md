# RFC 0052: Provider crate topology

Status: implemented

## Problem

Creating one crate for every HTTP model provider makes workspace membership,
publication ordering, shared transport maintenance, and dependency wiring grow
linearly with provider count. OpenAI-compatible, Anthropic, Gemini, and Ollama
use the same portable HTTP stack and release version, so their previous crate
boundaries did not correspond to independent dependency or lifecycle
boundaries.

## Decision

First-party providers live in the `runifold-providers` crate when their entire
dependency graph can remain behind an independent Cargo feature and they share
the public `Model + ProviderModel` integration contract. This includes the
portable HTTP adapters and selected SDK-backed adapters such as Amazon
Bedrock. Each provider remains a public module behind an independent feature:

- `openai`;
- `anthropic`;
- `gemini`;
- `ollama`;
- `bedrock`.

The `runifold` facade forwards its existing features to the matching provider
feature and preserves application-facing paths such as `runifold::openai` and
`runifold::anthropic`.

Provider protocol code remains separated by module. Each module owns its
configuration, client, request encoder, streaming decoder, embedding adapter,
and protocol tests. Consolidation shares the compilation and publication
boundary; it does not merge wire protocols or error types.

## Companion-crate threshold

A provider receives a separate companion crate only when feature gating is not
enough and at least one of these conditions applies:

1. it requires an incompatible platform, toolchain, runtime, or license;
2. it introduces a distinct runtime such as local inference;
3. it needs an independently versioned public API or release lifecycle;
4. its dependencies cannot be completely excluded from consumers that do not
   enable its feature;
5. isolating its dependency graph materially reduces consumers' build cost in
   a way Cargo feature isolation cannot.

Expected examples include local Candle or accelerator runtimes and integrations
with incompatible native system dependencies. A heavyweight SDK alone is not a
crate boundary when it is fully optional. A provider brand by itself is never
a crate boundary.

## Compatibility

This is a pre-1.0 package-topology change. Direct dependencies on the former
`runifold-provider-openai`, `runifold-provider-anthropic`,
`runifold-provider-gemini`, and `runifold-provider-ollama` packages migrate to
`runifold-providers` with the corresponding feature. Applications using the
`runifold` facade keep the same module paths.

## Invariants

1. Enabling one facade provider feature enables only its provider module.
2. Provider-specific wire types never leak into `runifold-model`.
3. A protocol failure retains its provider-specific typed error.
4. Common transport dependencies are declared once.
5. Every provider keeps deterministic unit and real-HTTP cassette coverage.
6. New HTTP providers default to a module, not a crate.
7. Optional SDK dependencies never enter builds that omit their provider
   feature.
8. SDK-backed adapters preserve the same canonical runtime contract and do not
   introduce a second hidden retry authority.
