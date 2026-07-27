# RFC 0023: Durable Workflow and Agent Orchestration

## Status

Implemented for sequential, conditional, and durable parallel execution.

## Problem

Agent delegation alone does not define a durable application workflow.
Applications need stable node identity, explicit authority, canonical data
boundaries, causal events, and conservative recovery across multiple Agents
and host-defined steps.

Treating orchestration as fluent syntax over unrelated futures is
insufficient. A process may stop after external work completes but before its
result is persisted. Silent replay can duplicate model charges or side
effects.

## Crate boundary

`runifold-workflow` depends on `runifold-core` and `runifold-agent`. Neither
the runtime kernel nor Agent loop depends on workflow orchestration. The root
`runifold` facade re-exports the stable workflow API.

This direction keeps the Agent usable independently and prevents orchestration
policy from leaking into model, Tool, or Effect contracts.

## Canonical execution boundary

`WorkflowStep` receives and returns `serde_json::Value`. This is the durable
intermediate representation used in checkpoints and completed-node outputs.
It allows heterogeneous nodes without serializing executable Rust closures or
trait objects.

Typed adapters may validate Rust input and output types above this boundary.
They must not change the persisted representation without a workflow version
change.

Every node has a stable `StepId`. Identifiers are limited to ASCII letters,
digits, `_`, `-`, and `.`, with a maximum length of 128 bytes. Checkpoints
store the complete ordered layout.

## Definition identity

A workflow definition is identified by:

- stable workflow name;
- non-zero caller-managed version;
- ordered list of `StepId` values.

Resume rejects a checkpoint when any of these differ. Changing code behind a
step while preserving its identifier requires incrementing the workflow
version.

## Authority and child runs

Every node declares its exact `CapabilitySet`. Before any workflow node
executes, the scheduler verifies that all requested capabilities are a subset
of the parent run's authority.

Each node receives a child `RunContext`:

- budget remains shared with the run tree;
- cancellation and deadline remain hierarchical;
- only the node's explicit capabilities are granted;
- parent-child and workflow-domain events retain causal links.

An Agent is adapted through `AgentStep`; it does not receive ambient parent
authority automatically.

## Sequential and conditional nodes

Sequential nodes consume the previous node's canonical output. The initial
node consumes the workflow input.

A conditional node evaluates a pure `WorkflowCondition` and executes exactly
one of two configured steps. The selected branch is recorded in the node's
completion event. Both alternatives share the node identity and capability
grant because only one durable node boundary is crossed.

## Checkpoint protocol

`WorkflowCheckpointState` stores:

- workflow identity and ordered layout;
- next node index;
- canonical value for the next node;
- outputs of every completed node;
- shared usage snapshot;
- execution phase.

The phases are:

- `Ready`;
- `StepInFlight { step }`;
- `Completed { outcome }`.

The write-ahead sequence for every node is:

1. persist `StepInFlight`;
2. create the capability-attenuated child run;
3. execute the selected step;
4. persist its output, increment the next index, and return to `Ready`;
5. after the final node, persist `Completed`.

Completed recovery returns the stored outcome without executing any node.

## Conservative recovery

`WorkflowResumePolicy::RejectAmbiguous` is the default. It rejects a
`StepInFlight` checkpoint because model cost, output, or an external effect may
already exist.

`RetryInterruptedStep` is explicit caller authority to replay only the
interrupted node from its last stable input. Completed earlier nodes are not
executed again. Usage may increase after an interrupted attempt but can never
be restored below the checkpoint snapshot.

This protocol provides durable at-least-once node execution with explicit
ambiguity. It does not claim exactly-once external execution.

## Parallelism

Fail-fast fan-out/fan-in, atomic budget reservation, per-branch persisted
state, deterministic joins, and structured sibling cancellation are specified
in [RFC 0024](0024-budget-reservations-and-parallel-workflows.md).

Side-effect-safe first-success race is specified in
[RFC 0025](0025-safe-first-success-race.md). Races that admit write-capable or
unknown effects remain intentionally unsupported.

## Initial invariants

1. Invalid or duplicate step identities fail before execution.
2. Authority amplification fails before the first node executes.
3. Every node runs with an explicit child capability set.
4. Completed node outputs are never recomputed during stable recovery.
5. In-flight replay always requires explicit caller authority.
6. Workflow name, version, and ordered layout must match on resume.
7. Stable recovery never decreases shared usage.
8. Conditional execution invokes exactly one branch.
9. Checkpoint writes use the existing compare-and-swap storage contract.
