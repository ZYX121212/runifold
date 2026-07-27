# RFC 0013: Agent callable write-ahead effects

- Status: implemented
- Scope: `runifold-agent`, `runifold-effect`, `runifold-tool`

## Summary

Every Tool invocation and Agent delegation issued by the Agent loop executes
through `EffectExecutor`. This closes the recovery gap between turn-level
checkpoints and individual external actions: a checkpoint retry can replay a
completed callable result without executing its handler again.

## Stable execution identity

An ordinary Agent run uses the root Run ID as its execution identity. A
checkpointed run uses the checkpoint ID, and that identity is persisted in
`AgentCheckpointState` for every resume.

Each model-emitted callable position derives this idempotency key:

```text
{execution_id}:agent:{agent_name}:turn:{turn}:call:{call_index}
```

The key identifies a logical position, while `EffectExecutor` also compares
the capability, effect kind, effect class, and canonical input. If a retried
model call emits different work at the same position, execution fails with an
idempotency conflict rather than returning an unrelated result.

Agent names must therefore remain stable within one logical execution.

## Tool integration

The Tool descriptor supplies the capability ID and effect class. Capability
validation still occurs before the effect is persisted. The handler invokes
the registry only after a durable `Started` record exists.

Both successful `ToolOutput` and structured `ToolError` values are persisted
inside a result envelope. Replaying a completed failure preserves the existing
Agent tool-error policy without executing the Tool again.

The logical Tool-call budget and local counter are consumed when the Agent
processes the model call, including a checkpoint retry. They measure logical
work observed by the Agent, not physical handler executions.

## Delegation integration

The Agent route descriptor supplies the capability ID. Delegation uses an
Agent effect and invokes `AgentGateway` inside the effect handler, retaining
Gateway middleware, authority attenuation, depth checks, child Runs, and
delegation budget enforcement.

Successful child outcomes are converted to model-visible Tool output.
Structured `GatewayError` values are persisted and replayed. Gateway
delegation budget is consumed only when the handler actually delegates; a
completed replay does not create another child Run.

## Recovery behavior

The Agent configures one `EffectRecoveryPolicy` for its callables:

- `Completed` returns the stored envelope without handler execution;
- `Started` is rejected by default as ambiguous;
- explicit safe retry remains constrained by effect class and idempotency;
- a conflicting request at the same stable key is always rejected.

The Agent owns an in-memory executor by default for convenient local use.
Applications requiring recovery across Agent instances or process restarts
must inject an executor backed by the same durable `EffectStore`.

## Compatibility

Capability denial and descriptor lookup happen before effect execution so
public Tool and Gateway error categories remain stable. Model-visible failure
behavior is unchanged. The migration changes execution coordination and
recovery guarantees, not the provider-neutral Agent protocol.

## Invariants

1. A callable handler never starts before a durable `Started` record.
2. A completed callable is not physically executed twice when its store is
   available.
3. Stable keys cannot replay different work.
4. Tool and Gateway structured failures are recoverable results.
5. Unsafe ambiguous effects are never retried implicitly.
6. Cross-process recovery requires a shared durable EffectStore.

## Deferred decisions

- Durable SQLite and PostgreSQL EffectStore implementations.
- Handler-specific reconciliation for ambiguous remote effects.
- Distributed leases, ownership, heartbeats, and attempt identities.
- Transactional coordination between checkpoints, effects, and journals.
