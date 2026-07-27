# RFC 0029: OpenTelemetry GenAI observability

- Status: implemented
- Scope: `runifold-observability-otel`

## Decision

OpenTelemetry is an optional edge adapter rather than a runtime-kernel
dependency. The adapter has two independent decorators:

- `OtelModel` wraps the provider-neutral `Model` boundary and emits GenAI
  inference-client spans plus duration and token-usage metrics;
- `OtelJournal` projects durable Runifold events into Agent, Tool, delegation,
  and Workflow spans.

Applications may enable only one decorator. Using both gives end-to-end
execution and inference signals without teaching the core, model, Agent, or
Workflow crates about an exporter or telemetry SDK.

`OtelRuntime` is the preferred construction boundary when both decorators are
used. It owns a private Run correlation registry and creates Model and Journal
decorators that share it. A model call carrying `ModelCallContext::run_id`
therefore becomes a child of the corresponding Agent or Workflow Run span.
Child Run roots use `EventMeta::parent_run_id` when the parent remains active.

The registry contains OpenTelemetry contexts only inside the optional adapter;
`runifold-core` and `runifold-model` retain no OpenTelemetry dependency.
Standalone `OtelModel` and `OtelJournal` constructors remain available when
cross-boundary correlation is not required.

## Model signals

Every model stream creates a `CLIENT` span named
`{gen_ai.operation.name} {gen_ai.request.model}`. It records:

- Runifold Run and model-invocation correlation identities on spans;
- `gen_ai.operation.name` and `gen_ai.provider.name`;
- requested and actual model identities;
- streaming and generation parameters;
- normalized finish reasons;
- input, output, reasoning, and cache token details;
- normalized `error.type` and a terminal exception event.

The adapter records `gen_ai.client.operation.duration` in seconds and
`gen_ai.client.token.usage` in tokens. The original canonical stream is passed
through unchanged. Opening errors, mid-stream errors, and abandoned streams all
close their span exactly once.

Run and invocation identities are deliberately excluded from metric
attributes. They are useful for trace lookup but would create unbounded metric
cardinality.

Gemini uses the semantic-convention provider identity `gcp.gemini` and the
`generate_content` operation. Other chat-style adapters use their configured
provider identity and the `chat` operation.

## Agent and orchestration signals

`OtelJournal` records the wrapped journal first. Only accepted durable events
may affect telemetry.

The event projection creates:

- one `invoke_agent` root per Agent run;
- one `agent.turn` child for each bounded Model/Tool iteration;
- one `execute_tool` child per Tool call;
- one `invoke_agent` child per delegated Agent call;
- one `invoke_workflow` root per Workflow run;
- one Runifold workflow-step child per step.

Model, Tool, and delegation spans are parented to the active Turn. When an
Agent delegation creates a child `RunContext`, the `ChildEvent::Started` fact
binds the child Agent root to the active delegation span. Other child Runs
fall back to the active parent Run span.

Terminal lifecycle events close outstanding children and their root. Failed and
cancelled operations receive a normalized error status. Duplicate starts while
an operation is active and unmatched terminal events are ignored. The decorator
does not claim durable deduplication when an entire historical event log is
replayed; hosts should instrument live journal writes or enforce their own
export checkpoint.

## Operational Agent metrics

The Journal projection also emits metrics intended for dashboards, alerts, and
capacity planning:

- `runifold.agent.operation.duration` and `runifold.agent.turn.duration`;
- `runifold.agent.turns`, `runifold.agent.tool.calls`, and
  `runifold.agent.delegations`;
- `runifold.agent.cost`, reported in US dollars from attributed
  `usage.cost_microusd`;
- `runifold.agent.errors` and `runifold.agent.budget.exhaustions`.

Completed-run counts and cost come from the canonical Agent terminal event
rather than reconstructing usage from spans. A failed or cancelled run still
records end-to-end duration and an error count. Budget exhaustion is also
recorded separately so operators can distinguish resource-policy pressure from
provider or application failures.

These instruments use only bounded `status` and normalized `error.type`
attributes. Agent names, Run identities, Tool names, invocation identities,
and delegation targets remain trace-only. Evaluation quality is intentionally
not inferred from operational telemetry; quality scores belong to a separate
evaluation signal with an explicit dataset and rubric.

Duration and cost histograms carry recommended explicit bucket boundaries as
instrument advice. This produces useful Prometheus quantiles without requiring
the optional adapter to depend on `opentelemetry_sdk`. A host may override the
advice with SDK Views for a workload-specific latency distribution.

## Routing and streaming signals

The Model decorator recognizes lossless `runifold.router/route.selected`
Provider events. The logical Model span records the selected route, attempt
identity, whether fallback occurred, and the number of prior failures. Every
prior failure is projected as a safe span event containing route, target,
failure category, attempt number, and retry-safety classification. Provider
error messages are not exported.

The adapter also records:

- `runifold.model.time_to_first_chunk`;
- `runifold.model.route.selections`;
- `runifold.model.route.failures`.

Route metrics use only bounded status and failure dimensions. Route,
invocation, Run, and request identities remain trace-only to avoid unbounded
metric cardinality.

## MCP Sampling signals

Scoped Sampling writes redacted `sampling.started`, `sampling.completed`, and
`sampling.failed` durable events. The projection creates one
`mcp.sampling.create_message` span beneath the active Agent Turn.

Sampling failures carry a stable stage such as `request_review`,
`model_execution`, `response_validation`, or `response_review`. Prompt and
response content are never included in these lifecycle events. A durable event
failure is surfaced as a typed MCP observability error rather than allowing an
unobserved scoped Sampling call.

The projection emits `runifold.mcp.sampling.requests`,
`runifold.mcp.sampling.duration`, and `runifold.mcp.sampling.failures`.
Sampling metric attributes are restricted to bounded `status`, normalized
`error.type`, and normalized stage values. Unknown extension values collapse
to `_OTHER`; request, call, and Run identities remain trace-only.

## SLO assets

The crate embeds version-matched Prometheus recording/alert rules and an
importable Grafana dashboard through `slo::PROMETHEUS_RULES` and
`slo::GRAFANA_DASHBOARD`. The default objectives are 99% Agent success, Agent
P95 duration at or below 30 seconds, 99% MCP Sampling success, and Agent budget
exhaustion below 2%. Error-budget alerts use paired short and long windows to
reduce noise.

The templates assume default OpenTelemetry Prometheus name and unit
translation. Exporter-specific renaming remains a deployment concern. The
operational runbook is maintained in
[`docs/operations-slo.md`](../operations-slo.md).

## Privacy

Prompt content, system instructions, model output, Tool definitions, exception
messages, Tool arguments, and Tool results are sensitive. They are not exported
by default.

`ContentCapture::Messages` opts into message and output capture.
`ContentCapture::MessagesAndTools` additionally captures Tool definitions.
Exception messages require the separate `with_error_messages(true)` opt-in.
This separation prevents enabling content capture from silently exporting
operational error text.

Runifold event payloads used by `OtelJournal` contain stable names and call
identities, not Tool arguments or results. The adapter does not serialize
arbitrary lifecycle output or error messages.

## Failure and dependency boundaries

Telemetry is fail-open after durable event acceptance: OpenTelemetry span and
metric APIs do not alter runtime outcomes. A wrapped journal failure remains a
typed `JournalError` and no corresponding telemetry state is created.

The dependency direction is:

```text
runifold-observability-otel -> runifold-model + runifold-core
```

No kernel or execution crate depends on OpenTelemetry. The facade exposes the
adapter only behind the optional `otel` feature.
