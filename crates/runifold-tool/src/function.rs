use std::{future::Future, marker::PhantomData};

use runifold_core::{CapabilityId, EffectClass, RiskLevel};
use schemars::{JsonSchema, schema_for};
use serde::{Serialize, de::DeserializeOwned};

use crate::{Tool, ToolContext, ToolDescriptor, ToolError, ToolErrorKind, ToolFuture, ToolOutput};

/// A typed asynchronous Rust function exposed through the canonical Tool
/// boundary.
pub struct FunctionTool<Input, Output, Handler> {
    descriptor: ToolDescriptor,
    handler: Handler,
    types: PhantomData<fn(Input) -> Output>,
}

impl<Input, Output, Handler> FunctionTool<Input, Output, Handler>
where
    Input: JsonSchema,
    Output: JsonSchema,
{
    /// Creates a typed Tool with generated input and output JSON Schemas.
    ///
    /// The default effect is [`EffectClass::Pure`] and the default risk is
    /// [`RiskLevel::Low`]. Callers must explicitly override these values for
    /// functions that read or modify external state.
    pub fn new(name: impl Into<String>, description: impl Into<String>, handler: Handler) -> Self {
        Self {
            descriptor: ToolDescriptor {
                id: CapabilityId::new(),
                name: name.into(),
                version: "1".into(),
                description: description.into(),
                input_schema: schema_for!(Input).to_value(),
                output_schema: schema_for!(Output).to_value(),
                effect: EffectClass::Pure,
                risk: RiskLevel::Low,
                metadata: std::collections::BTreeMap::new(),
            },
            handler,
            types: PhantomData,
        }
    }

    /// Replaces the stable capability identity.
    #[must_use]
    pub const fn capability_id(mut self, id: CapabilityId) -> Self {
        self.descriptor.id = id;
        self
    }

    /// Sets the semantic Tool contract version.
    #[must_use]
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.descriptor.version = version.into();
        self
    }

    /// Declares external-effect behavior.
    #[must_use]
    pub const fn effect(mut self, effect: EffectClass) -> Self {
        self.descriptor.effect = effect;
        self
    }

    /// Declares policy risk.
    #[must_use]
    pub const fn risk(mut self, risk: RiskLevel) -> Self {
        self.descriptor.risk = risk;
        self
    }

    /// Adds host-only namespaced metadata.
    #[must_use]
    pub fn metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.descriptor.metadata.insert(key.into(), value);
        self
    }
}

impl<Input, Output, Handler> std::fmt::Debug for FunctionTool<Input, Output, Handler> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FunctionTool")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

impl<Input, Output, Handler, HandlerFuture> Tool for FunctionTool<Input, Output, Handler>
where
    Input: DeserializeOwned + JsonSchema + Send + 'static,
    Output: JsonSchema + Serialize + Send + 'static,
    Handler: Fn(Input, ToolContext) -> HandlerFuture + Send + Sync,
    HandlerFuture: Future<Output = Result<Output, ToolError>> + Send + 'static,
{
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        input: serde_json::Value,
        context: ToolContext,
    ) -> ToolFuture<'_, Result<ToolOutput, ToolError>> {
        let input = match serde_json::from_value(input) {
            Ok(input) => input,
            Err(error) => {
                return Box::pin(async move {
                    Err(ToolError::local(
                        ToolErrorKind::InvalidInput,
                        format!("typed Tool input is invalid: {error}"),
                    ))
                });
            }
        };
        let future = (self.handler)(input, context);
        Box::pin(async move {
            let output = future.await?;
            let value = serde_json::to_value(output).map_err(|error| {
                ToolError::local(
                    ToolErrorKind::InvalidOutput,
                    format!("typed Tool output cannot be serialized: {error}"),
                )
            })?;
            Ok(ToolOutput::model_visible(value))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use runifold_core::{Budget, BudgetTracker, CapabilitySet, RunContext};
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::FunctionTool;
    use crate::{Tool, ToolErrorKind, ToolRegistry};

    #[derive(Deserialize, JsonSchema)]
    struct AddInput {
        left: i64,
        right: i64,
    }

    #[derive(JsonSchema, Serialize)]
    struct AddOutput {
        sum: i64,
    }

    #[test]
    fn typed_function_generates_schemas_and_runs_through_registry() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let tool = Arc::new(FunctionTool::new(
            "add",
            "adds two integers",
            move |input: AddInput, _context| {
                let observed = observed.clone();
                async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Ok(AddOutput {
                        sum: input.left + input.right,
                    })
                }
            },
        ));
        let descriptor = tool.descriptor();
        assert_eq!(
            descriptor.input_schema["required"],
            json!(["left", "right"])
        );
        assert_eq!(descriptor.output_schema["required"], json!(["sum"]));

        let mut capabilities = CapabilitySet::new();
        capabilities.grant(descriptor.capability());
        let run = RunContext::root(BudgetTracker::new(Budget::default()), capabilities);
        let mut registry = ToolRegistry::new();
        registry.register(tool).unwrap();

        let output = futures_executor::block_on(registry.invoke(
            "add",
            json!({"left": 2, "right": 3}),
            &run,
        ))
        .unwrap();

        assert_eq!(output.value, json!({"sum": 5}));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn invalid_typed_input_never_calls_handler() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let tool = Arc::new(FunctionTool::new(
            "add",
            "adds two integers",
            move |_input: AddInput, _context| {
                let observed = observed.clone();
                async move {
                    observed.fetch_add(1, Ordering::SeqCst);
                    Ok(AddOutput { sum: 0 })
                }
            },
        ));
        let mut capabilities = CapabilitySet::new();
        capabilities.grant(tool.descriptor().capability());
        let run = RunContext::root(BudgetTracker::new(Budget::default()), capabilities);
        let mut registry = ToolRegistry::new();
        registry.register(tool).unwrap();

        let error = futures_executor::block_on(registry.invoke("add", json!({"left": 2}), &run))
            .unwrap_err();

        assert_eq!(error.kind, ToolErrorKind::InvalidInput);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
