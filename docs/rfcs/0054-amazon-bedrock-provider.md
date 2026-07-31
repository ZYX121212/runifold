# RFC 0054: Native Amazon Bedrock provider

Status: implemented

## Problem

Amazon Bedrock exposes multiple model families behind one managed runtime.
Treating it as an OpenAI-compatible endpoint would erase its native Converse
content blocks, Tool lifecycle, reasoning fields, usage details, and AWS
authentication semantics. Implementing `SigV4` manually would also duplicate a
security-sensitive protocol already owned by the AWS SDK.

## Decision

The `bedrock` feature in `runifold-providers` implements the native
`ConverseStream` operation through `aws-sdk-bedrockruntime`.

The adapter:

1. translates canonical messages, system instructions, Tools, Tool results,
   inference options, and additional model fields into typed SDK inputs;
2. translates text, Tool arguments, reasoning text and signatures, usage,
   finish reasons, and unknown native events into the canonical stream;
3. rejects unsupported or lossy translations before transport;
4. validates the streaming content-block and terminal-event lifecycle;
5. checks cancellation and deadlines while opening and receiving the stream;
6. implements `Model + ProviderModel`, automatically inheriting Runifold's
   Agent, routing, budget, observability, and durable-workflow layers.

## Credential and retry ownership

Runifold does not discover AWS credentials implicitly. Applications may load
the AWS standard credential chain and convert its shared SDK configuration into
the exported `BedrockSdkConfig`, or explicitly provide short-lived credentials
and a session token.

`BedrockClient::new` rebuilds the service configuration with SDK retries
disabled. Runifold's router is therefore the sole retry and circuit-breaker
authority, so retry attempts remain visible to budget accounting,
observability, and idempotency policy. `from_sdk_client` preserves an
application-owned SDK client as an explicit escape hatch; its caller owns the
risk of layered retries.

## Capability contract

Converse Stream is a provider-wide native capability. Tools, reasoning,
multimodal inputs, structured output, and context limits vary across Bedrock
model families and inference profiles, so the adapter does not guess them.
Applications attach verified model-family capabilities through
`with_capabilities`.

The model name is passed to the SDK as the Converse `modelId`. It can therefore
be any model ID or ARN form supported by that operation, including inference
profiles and provisioned throughput.

## Dependency and MSRV policy

The AWS SDK and Smithy types are fully optional and enter the graph only with
the `bedrock` feature. Their versions are constrained to a release family
validated by the workspace Rust 1.88 gate. Upgrades are deliberate because an
unbounded Smithy dependency update can raise the effective compiler floor.

This integration remains a module rather than a provider-specific crate
because feature isolation removes its dependency cost for all other users and
its public contract shares the same release lifecycle.

## Verification

Deterministic tests construct real SDK input and streaming event types. They
cover request translation, Tool and reasoning event accumulation, detailed
usage, finish reasons, terminal ordering, and configuration validation.

The offline cassette additionally executes the AWS SDK over a real loopback
HTTP socket. It validates `SigV4` request construction, session-token
redaction, binary EventStream CRC framing, arbitrary HTTP fragmentation,
truncated streams, deadlines, and concurrent invocation isolation.

This remains an offline protocol test rather than a live AWS-account test. A
future opt-in suite may validate account policy, regional availability, real
service throttling, and vendor-side behavior without placing credentials in
default CI.

## Invariants

1. No credentials, prompts, Tool arguments, or model output enter default
   telemetry attributes.
2. Unsupported canonical input fails before a billable request.
3. A response completes only after all available metadata has been consumed.
4. Post-visible-output failures remain unsafe for automatic replay.
5. Unknown native events are retained through provider-event escape hatches.
6. Exactly one retry authority is active in the default runtime path.
