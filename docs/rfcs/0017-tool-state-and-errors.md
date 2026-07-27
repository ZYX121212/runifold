# RFC 0017: Tool state injection and safe application errors

- Status: implemented
- Scope: `runifold-tool`, `runifold-macros`, `runifold`

## Summary

Typed Tool handlers may receive shared host application state and return
domain-specific errors without changing the model-facing contract.

The supported stateful signature is:

```text
async fn handler(
    state: State<Service>,
    input: Input,
    context: ToolContext,
) -> Result<Output, ApplicationError>
```

The generated constructor accepts `Arc<Service>`.

## State boundary

`State<T>` is a cloneable wrapper around `Arc<T>` with `Deref<Target = T>`.
It supports owned construction and wrapping an existing shared allocation.

State is captured by the local FunctionTool handler. It is not:

- included in the generated JSON Schema;
- accepted from model arguments;
- serialized into messages or transcripts;
- copied into EffectRequest input;
- persisted in checkpoints or EffectRecords.

The application owns state lifetime, internal concurrency, connection pools,
and shutdown behavior. Runifold only clones the Arc for one invocation.

## Application error boundary

Application errors implement:

```text
IntoToolError::into_tool_error(self) -> ToolError
```

The implementation explicitly chooses:

- normalized ToolErrorKind;
- safe human-readable message;
- RetrySafety;
- safe namespaced metadata.

Runifold deliberately provides no blanket conversion from `Error` or
`Display`. Error strings frequently contain request bodies, SQL fragments,
filesystem paths, credentials, or other internal data. Automatic conversion
would make accidental disclosure the default.

`ToolError` implements `IntoToolError` as identity, preserving the original
two-argument typed Tool signature.

## Macro expansion

For a stateful function named `search`, `#[runifold::tool]` generates:

```text
search_tool(state: Arc<Service>) -> impl Tool
```

The state is wrapped once, cloned for each invocation, and passed to the
original async function. The function's error is mapped with IntoToolError
before it reaches FunctionTool.

The two-argument `(Input, ToolContext)` signature uses the same explicit error
conversion and remains source-compatible for ToolError.

## Invariants

1. Host state never becomes model-controlled input.
2. Host state is absent from durable Effect identity and payloads.
3. Application errors cannot cross the Tool boundary without an explicit
   safe conversion.
4. State injection does not bypass capability, cancellation, deadline,
   budget, or write-ahead Effect checks.
5. ToolError retains identity conversion.

## Deferred decisions

- multiple independently typed state arguments;
- request-scoped state supplied by RunContext extensions;
- derive support for declarative error mappings;
- secret-typed metadata with enforced redaction;
- streaming Tool outputs.
