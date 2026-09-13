# Recommended API map

| Task | Preferred entry | Scope |
| --- | --- | --- |
| Provider-backed application Agent | `ProviderModelExt`, `ProviderRuntime::agent` | Keep runtime as shared application state |
| Explicit/mock Model injection | `Agent::builder` | Produces the same canonical Agent |
| One-shot text | `AgentBuilder::prompt_text` / `Agent::prompt_text` | Ergonomic root authority comes from registered callables |
| Application-owned limits | `Agent::run(input, &RunContext)` | Supply explicit budget, authority and lifecycle |
| Typed Tool | `#[runifold::tool]`, generated `name_tool()`, `.tool(...)` | Input struct and ToolContext; optional host State |
| Typed output | `build_structured::<T>` | Same type controls schema and local decode |
| Streaming | `Agent::stream` | Poll lifecycle events; handle failure and terminal completion |
| Durable requests | `AgentSession` | Persistent admission and stable request replay |
| Model-only use | `runifold_model::Model` | Lower-level invocation; no Agent Tool loop |

## Context names

- `RunContext`: shared run-tree identity, limits, authority, cancellation and events.
- `ModelCallContext`: control and observation for one model invocation.
- `ToolContext`: context delivered to one typed Tool invocation.
- `EffectExecutionContext`: context for a low-level external effect handler.

These scopes differ; follow their actual constructors and methods. Do not invent
an interchangeable `AgentContext` or `ExecutionContext`.

Recipes link to current exports and compiled examples. Historical RFCs can describe
superseded convenience APIs; current source and the versioned recipes take precedence
for executable signatures.
