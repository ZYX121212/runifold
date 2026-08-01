# RFC 0008: Agent delegation gateway

- Status: Accepted for initial implementation
- Scope: `runifold-agent`, `runifold-core`

## Summary

An agent may expose other agents to its model as callable delegation routes.
The model protocol uses the same callable representation as tools, but the
runtime keeps the two mechanisms distinct:

- tools consume the `tool_calls` budget and execute through `ToolRegistry`;
- agents consume the `delegations` budget and execute through `AgentGateway`.

This separation preserves provider compatibility without erasing authority,
lineage, accounting, or policy semantics.

## Route contract

Each `AgentRoute` binds:

- an `AgentDescriptor` with stable capability identity;
- a model-facing callable name and description;
- a child `Agent`;
- the exact capability set requested for the child run.

`AgentDescriptor::new` creates a fresh identity for ephemeral routes. A route
whose grants, policies, or audit history survive process restart must load its
`CapabilityId` from durable application configuration and apply it with
`AgentDescriptor::with_id`. Route names are presentation, not authorization
identity.

The descriptor becomes `CapabilityKind::Agent`, not
`CapabilityKind::Tool`. Its canonical model-facing input is:

```json
{"input": "task delegated to the child"}
```

Successful results preserve the child's canonical response content and local
turn, tool-call, and delegation counters.

## Invocation order

Before starting child work, a gateway:

1. rejects pre-existing cancellation or an elapsed deadline;
2. resolves the named route;
3. verifies that the parent holds the route's Agent capability;
4. verifies that every requested child capability is already held by the
   parent;
5. enforces the delegation-depth limit;
6. atomically consumes one shared delegation unit;
7. creates a child `RunContext`;
8. invokes the child Agent.

Checks that can reject authority or bounds occur before the child model is
invoked.

## Authority attenuation

A route declares the exact capabilities its child receives. The gateway
requires this set to be a subset of the parent's capability set. The child does
not ambiently inherit capabilities or metadata.

This gives delegation a monotonic authority invariant:

```text
child authority ⊆ parent authority
```

Possession of an Agent route does not implicitly grant any of the child's
tools, resources, or further Agent routes.

## Structured concurrency

The child Run:

- receives a new Run ID;
- records the caller as its parent;
- retains the same root Run ID;
- shares the run-tree budget tracker;
- receives a descendant cancellation token;
- cannot extend the parent's deadline.

Delegation depth and the selected route name are added as namespaced child
metadata. Recursive or cyclic topologies remain bounded even when assembled by
applications.

## Error policy

The parent model may recover from malformed delegation input or ordinary child
failure. The following gateway failures are hard runtime failures and are
never converted into model-visible tool results:

- capability denial;
- authority escalation;
- depth exhaustion;
- budget exhaustion;
- cancellation;
- deadline expiration.

## Initial invariants

1. A model-facing name cannot simultaneously identify a local tool and an
   Agent route.
2. Route capability is checked by stable identity, not by name.
3. Child authority can only stay equal or shrink.
4. Delegation budget is consumed before child execution.
5. Rejected authority, lifecycle, depth, or budget checks perform no child
   model work.
6. Tool-call and delegation accounting remain distinct.
7. Child cancellation and deadlines are descendants of the parent Run.
8. Provider-specific types never enter the gateway.

## Deferred decisions

- production middleware implementations for audit, approval, and rate limits;
- concurrent fan-out and deterministic join semantics;
- delegation result projection and context compaction;
- remote Agent-to-Agent transports;
- durable child-run event journaling;
- retry, fallback, and circuit-breaker policy;
- ergonomic high-level graph and builder APIs.
