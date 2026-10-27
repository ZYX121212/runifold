# RFC 0001: Runtime kernel

- Status: Accepted for initial implementation
- Scope: `runifold-core`

## Summary

Runifold models all executable work as hierarchical runs. Runs emit causal
events, request external effects, consume explicit budgets, and may only access
granted capabilities.

The initial kernel deliberately has no model-provider or agent-loop dependency.

## Core vocabulary

### Run

A run is one bounded execution with an identity, root identity, optional
parent, cancellation token, deadline, budget tracker, metadata, and explicit
capability set.

### Event

An event is an immutable fact with a unique ID, per-run sequence number,
timestamp, run identity, and optional causal event ID. Lifecycle, effect,
child-run, budget, and domain-specific facts share the same envelope.

### Effect

An effect describes work outside the current state transition: invoking a
model, tool, agent, approval mechanism, timer, or extension. The description
includes its capability, input, effect class, and optional idempotency key.

### Capability

A capability is an explicitly granted reference to a model, tool, agent,
resource, or extension. Descriptors carry versioned input/output schemas,
effect class, and risk metadata.

Child grants select a subset of the parent's complete descriptors. Identity
alone is insufficient: a child cannot retain an ID while changing its name,
version, kind, schemas, effect, risk or metadata. `CapabilitySet::is_subset_of`
and `first_missing_from` both enforce this contract, including reserved child
creation. Put child-specific execution metadata in `RunContext::metadata_mut`
rather than rewriting a granted descriptor. This is exact contract matching,
not inference of arbitrary JSON Schema or application-policy containment.

## Initial invariants

1. Run, event, effect, and capability IDs are globally unique UUIDv7 values.
2. Event sequence numbers are monotonic within a run.
3. Parent cancellation is visible to every descendant cancellation token.
4. Cancelling a child does not cancel its parent or siblings.
5. A child context retains the root ID and receives a new run ID.
6. A child receives only the capability set explicitly passed at creation.
7. A child deadline cannot exceed its parent deadline.
8. Budget consumption is atomic: a rejected update changes no counters.
9. Core errors are structured and expose retry safety.
10. Extension events and metadata are namespaced.
11. A child grant preserves its parent's complete capability descriptor.

The facade's `authority_contract` integration test exercises ordinary and
reserved child creation, Gateway delegation and Workflow steps. A reclassified
write is rejected before child work or delegation budget consumption. Core
tests cover every descriptor field, including policy metadata.

## Deferred decisions

- durable journal storage and checkpoint protocol;
- reservation semantics for concurrent child budgets;
- typed/dynamic invocation trait boundary;
- model content intermediate representation;
- effect executor and policy middleware interface;
- detach semantics for intentionally independent runs.

These are deferred until their behavior can be demonstrated with the scripted
runtime rather than guessed from provider APIs.
