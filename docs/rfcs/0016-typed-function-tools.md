# RFC 0016: Typed asynchronous function Tools

- Status: implemented
- Scope: `runifold-tool`, `runifold-macros`, `runifold`

## Summary

Runifold can expose a typed asynchronous Rust function through the canonical
Tool boundary. Rust types define the wire contract:

- `DeserializeOwned + JsonSchema` for input;
- `Serialize + JsonSchema` for successful output;
- `Result<Output, ToolError>` for execution.

The runtime adapter is `FunctionTool<Input, Output, Handler>`. The
`#[runifold::tool]` attribute generates a constructor around that adapter.

Rich functions use `FunctionTool::new_rich` or `#[runifold::tool(output =
"rich")]` and return the canonical `ToolOutput` directly. Images, audio,
documents, resources, structured content, application-error status, and
host-only metadata therefore retain their meaning instead of being serialized
into an ordinary JSON string. This is an explicit mode so existing typed JSON
functions keep their generated output schema and inference behavior.

## FunctionTool

`FunctionTool::new(name, description, handler)` generates complete input and
output JSON Schemas with Schemars. Its builder configures:

- stable capability ID;
- semantic version;
- effect class;
- risk level;
- host-only metadata.

`FunctionTool::new_rich` retains the same builders. Its default output schema
is permissive because rich presentation content is heterogeneous; applications
that attach typed `structured_content` can replace it with
`.output_schema(schema)` and receive the same registry validation as ordinary
typed output.

The default effect is Pure and the default risk is Low. Any function that
reads or changes external state must override the effect classification.

At invocation:

1. model input is deserialized into the declared Rust input type;
2. invalid input returns `ToolErrorKind::InvalidInput` without calling the
   handler;
3. the async handler receives owned typed input and `ToolContext`;
4. successful output is serialized into the canonical ToolOutput;
5. serialization failure returns `ToolErrorKind::InvalidOutput`.

The resulting FunctionTool implements the same object-safe `Tool` trait as a
manual implementation. Registry capability checks, cancellation, deadlines,
Agent write-ahead effects, and recovery therefore remain unchanged.

## Attribute macro

The attribute accepts:

- `description` (required);
- `name` (defaults to the Rust function name);
- `version` (defaults to `1`);
- `effect` (defaults to `pure`);
- `risk` (defaults to `low`).

The annotated function must be async and non-generic. It accepts either
`(Input, ToolContext)` or `(State<Service>, Input, ToolContext)`, and returns
`Result<Output, Error>` where Error implements `IntoToolError`.

The optional `output` attribute accepts `json` (the default) or `rich`. Rich
functions must return `Result<ToolOutput, Error>`; the generated constructor
uses `FunctionTool::new_rich` and never reserializes the canonical output.

For a function named `weather`, the macro retains the original function and
generates `weather_tool() -> impl Tool`. This constructor can be passed
directly to `AgentBuilder::tool`.

Effect strings are:

- `pure`;
- `read_only`;
- `idempotent_write`;
- `non_idempotent_write`;
- `destructive`;
- `unknown`.

Risk strings are `low`, `medium`, `high`, and `critical`. Unknown or duplicate
attribute keys fail at compile time.

## Schema stability

Generated schemas are part of the Tool capability contract but their exact
document shape follows Schemars. Applications requiring a long-lived external
contract should pin dependency versions, assign an explicit Tool version, and
review schema changes before release.

## Invariants

1. Typed Tools execute through the canonical Tool trait.
2. Invalid input cannot reach the handler.
3. Tool output is never exposed before successful serialization.
4. Effect and risk classification remain explicit policy data.
5. Macro expansion does not bypass capability or EffectExecutor checks.

## Deferred decisions

- borrowed inputs and streaming Tool outputs (completed rich output is
  supported, incremental Tool output is not);
- methods;
- compile-fail UI fixtures for every diagnostic;
- configurable constructor naming and crate-path resolution.
