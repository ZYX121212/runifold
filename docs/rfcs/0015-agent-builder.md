# RFC 0015: Canonical fluent Agent construction

- Status: implemented
- Scope: `runifold-agent`, `runifold`, `runifold-provider-openai`

## Summary

Runifold exposes a fluent `AgentBuilder` without introducing a second
execution implementation. `build()` produces the same `Agent` used by the
precise registry-based API, so convenience and correctness cannot diverge.

## Construction

The provider-neutral entry point is:

```text
Agent::builder(name, model, model_ref)
```

It supports ordered system instructions, owned or shared Tools, explicitly
authorized child Agent routes, Gateway middleware, delegation depth, Agent
configuration, write-ahead EffectExecutor injection, and Effect recovery
policy.

Registration errors are retained by the builder and returned from `build()`.
Duplicate names are never replaced silently. `build()` also rejects blank
Agent identity, zero turns, and a Tool/Agent model-facing name collision.

## Provider convenience

With the facade `openai` feature, `OpenAiAgentExt` starts the same builder
directly from `OpenAiClient`:

```text
client.agent(agent_name, model_name)
```

The ModelRef provider identity comes from `OpenAiConfig`. Therefore OpenAI,
Ark, Qwen, and custom OpenAI-compatible endpoints share one fluent surface
without pretending they are the same provider.

## Explicit authority

`Agent::callable_capabilities()` returns descriptors for all Tools and child
Agent routes exposed by that Agent. It does not mutate a Run or grant ambient
authority. The application explicitly chooses whether to install that set on
a root or delegated Run.

This reduces registry boilerplate while preserving the rule that capabilities
are granted deliberately at execution boundaries.

## Invariants

1. Builder-created Agents use the canonical Agent loop.
2. Convenience never weakens capability checks.
3. Duplicate or colliding callable names fail before model execution.
4. Provider identity is retained separately from wire compatibility.
5. No credential is read implicitly.

## Deferred decisions

- typed Tool derive and function adapters;
- a higher-level session API combining Run options and persistence;
- provider-specific convenience surfaces outside OpenAI-compatible protocols;
- stable configuration serialization.
