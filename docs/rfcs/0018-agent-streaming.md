# RFC 0018: Backpressured Agent streaming

- Status: implemented
- Scope: `runifold-agent`, `runifold-model`, `runifold`

## Summary

`Agent::stream(input, run)` drives the same canonical Agent state machine as
`Agent::run`. It exposes real-time model, callable, accounting, and terminal
events without implementing a second model-tool loop.

The returned `AgentEventStream` borrows the Agent and RunContext. Execution
advances only while the caller polls the stream.

## Event protocol

`AgentStreamEvent` contains:

- `Started`;
- `TurnStarted`;
- `Model { turn, event: ModelStreamEvent }`;
- `CallableStarted`;
- `CallableCompleted`;
- `UsageUpdated`;
- `Completed { outcome }`.

`CallableKind` distinguishes a local Tool from a child Agent delegation.
Started events include the canonical model-emitted ToolCall. Completed events
include identity and success state but do not duplicate Tool output bodies.
Tool results remain available in the canonical transcript and subsequent
model request.

The Model variant retains every provider-neutral event accepted by
`ModelStreamAccumulator`, including text, reasoning, Tool argument, refusal,
usage, warning, heartbeat, provider-extension, and terminal events.

## One execution path

The Agent loop receives an internal observer. Normal run, checkpointed run,
and resume use a no-op observer. Streaming uses a buffered observer. Model
stream collection always uses the same strict ModelStreamAccumulator that
constructs the terminal ModelResponse.

Therefore:

- streamed and non-streamed Agent outcomes share transcript construction;
- malformed model event order fails identically;
- Tool and delegation execution use the existing EffectExecutor path;
- budgets, cancellation, checkpoints, journals, and capability checks are
  unchanged.

## Backpressure

After publishing each visible event, the streaming observer yields once.
`AgentEventStream::poll_next` returns that event before polling execution
again.

This creates one poll boundary per event even when a provider stream is
immediately ready. A slow caller slows the Agent rather than allowing the
observer queue to grow with the entire response.

The no-op observer does not yield, so ordinary `run()` has no artificial
event pacing.

## Usage

UsageUpdated contains the latest shared run-tree Usage after:

- turn budget consumption;
- model usage accounting;
- local Tool-call budget consumption;
- Agent delegation when the Gateway changed shared usage.

Provider-reported intermediate model usage remains visible separately inside
ModelStreamEvent.

## Completion and failure

Successful execution emits Completed with the full AgentOutcome and then ends
the stream. Execution failure yields `Err(AgentError)` after already accepted
events have been delivered.

Dropping the stream drops the in-flight execution future. If an external
effect had reached Started, its write-ahead record remains ambiguous and
normal Effect recovery rules apply.

## Invariants

1. `run` and `stream` use one Agent loop.
2. Only Model events accepted by ModelStreamAccumulator are emitted.
3. Every visible streaming event creates a backpressure boundary.
4. Tool output bodies are not duplicated into callable lifecycle events.
5. Completed carries the same canonical AgentOutcome returned by run.
6. Dropping a stream cannot mark an ambiguous external effect completed or
   failed without evidence.

## Deferred decisions

- checkpointed and resumed streaming entry points;
- child Agent event flattening versus nested stream envelopes;
- resumable stream cursors;
- bounded cross-task channels for spawned execution;
- explicit stream-drop cancellation policy;
- convenience adapters for Server-Sent Events and WebSockets.
