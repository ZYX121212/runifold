# Invariants

The [project charter](../CHARTER.md) is the policy source. The root guide and
manifest expose a compact operational view of it.

Before changing execution, verify:

- Authority: child capabilities/budgets cannot exceed explicit parent grants.
- Execution: convenience and streaming use the same canonical Agent path.
- Recovery: stable identity binds the same work; completed effects replay;
  ambiguous writes are not silently retried.
- Completion: local validation and required review precede successful commit.
- Persistence: emitted text is not proof of a committed conversation.
- Content: retain rich content and explicit provider extensions without lossy flattening.
- Privacy: diagnostics report safe categories and actions, not secret payloads.

Use the relevant regression test from the recipes or [reliability matrix](../RELIABILITY.md).
An invariant is not verified merely because a type compiles.
