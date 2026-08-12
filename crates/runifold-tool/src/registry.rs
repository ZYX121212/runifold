use std::{collections::BTreeMap, fmt, fmt::Write as _, sync::Arc};

use futures_util::future::{Either, select};
use jsonschema::{ValidationError, Validator, error::ValidationErrorKind};
use runifold_core::RunContext;
use runifold_model::{ArtifactScope, ArtifactStore, ToolSpec};
use serde_json::Value;

use crate::{
    Tool, ToolContext, ToolDescriptor, ToolError, ToolErrorKind, ToolFuture, ToolOutput,
    ToolRegistrationError,
};

/// Immutable-name registry and capability gate for tools.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ToolLimits {
    /// Maximum serialized invocation input size.
    pub max_input_bytes: usize,
    /// Maximum serialized canonical output size.
    pub max_output_bytes: usize,
}

impl Default for ToolLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: 1024 * 1024,
            max_output_bytes: 8 * 1024 * 1024,
        }
    }
}

#[derive(Clone)]
struct RegisteredTool {
    tool: Arc<dyn Tool>,
    input_validator: Validator,
    output_validator: Validator,
}

/// Immutable-name Tool registry with compiled contracts and bounded I/O.
#[derive(Clone, Default)]
pub struct ToolRegistry {
    tools: BTreeMap<String, RegisteredTool>,
    limits: ToolLimits,
    artifact_store: Option<Arc<dyn ArtifactStore>>,
    artifact_scope: Option<ArtifactScope>,
}

impl ToolRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the serialized Tool I/O limits.
    #[must_use]
    pub const fn with_limits(mut self, limits: ToolLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Makes an artifact store available to every Tool invocation context.
    #[must_use]
    pub fn with_artifact_store(
        mut self,
        scope: ArtifactScope,
        store: Arc<dyn ArtifactStore>,
    ) -> Self {
        self.artifact_scope = Some(scope);
        self.artifact_store = Some(store);
        self
    }

    /// Registers a tool without replacing an existing name.
    ///
    /// # Errors
    ///
    /// Returns [`ToolRegistrationError`] for blank or duplicate names.
    pub fn register(&mut self, tool: Arc<dyn Tool>) -> Result<(), ToolRegistrationError> {
        let name = tool.descriptor().name.trim();
        if name.is_empty() {
            return Err(ToolRegistrationError::EmptyName);
        }
        if self.tools.contains_key(name) {
            return Err(ToolRegistrationError::DuplicateName(name.into()));
        }
        let input_validator = compile_schema(name, "input", &tool.descriptor().input_schema)?;
        let output_validator = compile_schema(name, "output", &tool.descriptor().output_schema)?;
        self.tools.insert(
            name.into(),
            RegisteredTool {
                tool,
                input_validator,
                output_validator,
            },
        );
        Ok(())
    }

    /// Returns model-facing specifications in deterministic name order.
    pub fn model_specs(&self) -> Vec<ToolSpec> {
        self.tools
            .values()
            .map(|registered| registered.tool.descriptor().model_spec())
            .collect()
    }

    /// Returns the number of registered tools.
    pub fn len(&self) -> usize {
        self.tools.len()
    }

    /// Returns whether no tools are registered.
    pub fn is_empty(&self) -> bool {
        self.tools.is_empty()
    }

    /// Returns whether a tool is registered under `name`.
    pub fn contains(&self, name: &str) -> bool {
        self.tools.contains_key(name)
    }

    /// Returns the immutable descriptor registered under `name`.
    pub fn descriptor(&self, name: &str) -> Option<&ToolDescriptor> {
        self.tools
            .get(name)
            .map(|registered| registered.tool.descriptor())
    }

    /// Invokes a registered tool after checking the owning run's explicit
    /// capability grant.
    pub fn invoke<'a>(
        &'a self,
        name: &'a str,
        input: Value,
        run: &'a RunContext,
    ) -> ToolFuture<'a, Result<ToolOutput, ToolError>> {
        Box::pin(async move {
            let registered = self.tools.get(name).ok_or_else(|| {
                ToolError::local(
                    ToolErrorKind::NotFound,
                    format!("tool `{name}` is not registered"),
                )
            })?;
            let descriptor = registered.tool.descriptor();
            if !run.capabilities().contains(descriptor.id) {
                return Err(ToolError::local(
                    ToolErrorKind::CapabilityDenied,
                    format!("run is not granted tool capability `{name}`"),
                ));
            }
            let context = ToolContext::for_run(run)
                .with_artifact_store(self.artifact_scope.clone(), self.artifact_store.clone());
            validate_size(
                "Tool input",
                &input,
                self.limits.max_input_bytes,
                ToolErrorKind::InvalidInput,
            )?;
            registered
                .input_validator
                .validate(&input)
                .map_err(|error| input_validation_error(name, &error))?;
            if context
                .remaining()
                .is_some_and(|remaining| remaining.is_zero())
            {
                return Err(ToolError::local(
                    ToolErrorKind::DeadlineExceeded,
                    "tool invocation deadline already elapsed",
                ));
            }
            let cancellation = context.cancellation().clone();
            match select(
                Box::pin(cancellation.cancelled()),
                Box::pin(registered.tool.invoke(input, context)),
            )
            .await
            {
                Either::Left(_) => Err(ToolError::local(
                    ToolErrorKind::Cancelled,
                    "tool invocation was cancelled",
                )),
                Either::Right((result, _)) => {
                    let output = result?;
                    validate_output(&output, &registered.output_validator, self.limits)?;
                    Ok(output)
                }
            }
        })
    }
}

fn input_validation_error(tool: &str, error: &ValidationError<'_>) -> ToolError {
    let input_path = error.instance_path().to_string();
    let schema_path = error.schema_path().to_string();
    let keyword = error.kind().keyword().to_owned();
    let mut diagnostic = ToolError::local(
        ToolErrorKind::InvalidInput,
        format!(
            "Tool `{tool}` rejected its arguments: input at `{input_path}` violates `{keyword}` at schema `{schema_path}`; call the tool again with corrected arguments and do not guess its result"
        ),
    );
    diagnostic
        .metadata
        .insert("validation.input_path".into(), Value::String(input_path));
    diagnostic
        .metadata
        .insert("validation.schema_path".into(), Value::String(schema_path));
    diagnostic
        .metadata
        .insert("validation.keyword".into(), Value::String(keyword));
    diagnostic
        .metadata
        .insert("tool.name".into(), Value::String(tool.into()));
    if let ValidationErrorKind::Enum { options } = error.kind() {
        diagnostic
            .metadata
            .insert("validation.allowed_values".into(), options.clone());
        let _ = write!(
            diagnostic.message,
            "; allowed values: {}",
            bounded_json(options, 512)
        );
    }
    if is_safe_scalar(error.instance()) {
        diagnostic.metadata.insert(
            "validation.actual_value".into(),
            error.instance().as_ref().clone(),
        );
        let _ = write!(
            diagnostic.message,
            "; received: {}",
            bounded_json(error.instance(), 128)
        );
    }
    diagnostic
}

fn is_safe_scalar(value: &Value) -> bool {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => true,
        Value::String(value) => value.len() <= 64,
        Value::Array(_) | Value::Object(_) => false,
    }
}

fn bounded_json(value: &Value, maximum: usize) -> String {
    let encoded = value.to_string();
    if encoded.len() <= maximum {
        encoded
    } else {
        "<redacted: value exceeds diagnostic limit>".into()
    }
}

fn compile_schema(
    tool: &str,
    direction: &'static str,
    schema: &Value,
) -> Result<Validator, ToolRegistrationError> {
    jsonschema::validator_for(schema).map_err(|error| ToolRegistrationError::InvalidSchema {
        tool: tool.into(),
        direction,
        message: error.to_string(),
    })
}

fn validate_output(
    output: &ToolOutput,
    validator: &Validator,
    limits: ToolLimits,
) -> Result<(), ToolError> {
    if output.content.is_empty() {
        return Err(ToolError::local(
            ToolErrorKind::InvalidOutput,
            "Tool output content cannot be empty",
        ));
    }
    validate_size(
        "Tool output",
        output,
        limits.max_output_bytes,
        ToolErrorKind::InvalidOutput,
    )?;
    if output.is_error {
        return Ok(());
    }
    let instance = output
        .structured_content
        .clone()
        .unwrap_or_else(|| serde_json::to_value(&output.content).unwrap_or(Value::Null));
    validator.validate(&instance).map_err(|error| {
        ToolError::local(
            ToolErrorKind::InvalidOutput,
            format!(
                "Tool output violates its declared schema at `{}`",
                error.schema_path()
            ),
        )
    })
}

fn validate_size<T: serde::Serialize>(
    label: &str,
    value: &T,
    limit: usize,
    kind: ToolErrorKind,
) -> Result<(), ToolError> {
    let size = serde_json::to_vec(value)
        .map_err(|error| {
            ToolError::local(kind.clone(), format!("{label} cannot be encoded: {error}"))
        })?
        .len();
    if size > limit {
        return Err(ToolError::local(
            kind,
            format!("{label} is {size} bytes and exceeds the {limit}-byte limit"),
        ));
    }
    Ok(())
}

impl fmt::Debug for ToolRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolRegistry")
            .field("tools", &self.tools.keys().collect::<Vec<_>>())
            .field("limits", &self.limits)
            .field("artifact_store", &self.artifact_store.is_some())
            .field("artifact_scope", &self.artifact_scope)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use runifold_core::{
        Budget, BudgetTracker, CapabilityId, CapabilitySet, EffectClass, RiskLevel, RunContext,
    };
    use serde_json::{Value, json};

    use crate::{
        Tool, ToolContext, ToolDescriptor, ToolError, ToolErrorKind, ToolFuture, ToolLimits,
        ToolOutput, ToolRegistrationError,
    };

    use super::ToolRegistry;

    #[derive(Debug)]
    struct EchoTool {
        descriptor: ToolDescriptor,
    }

    impl EchoTool {
        fn new(name: &str) -> Self {
            Self {
                descriptor: ToolDescriptor {
                    id: CapabilityId::new(),
                    name: name.into(),
                    version: "1".into(),
                    description: "Echo structured input".into(),
                    input_schema: json!({"type": "object"}),
                    output_schema: json!({"type": "object"}),
                    effect: EffectClass::Pure,
                    risk: RiskLevel::Low,
                    metadata: BTreeMap::new(),
                },
            }
        }
    }

    impl Tool for EchoTool {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.descriptor
        }

        fn invoke(
            &self,
            input: Value,
            _context: ToolContext,
        ) -> ToolFuture<'_, Result<ToolOutput, ToolError>> {
            Box::pin(async move { Ok(ToolOutput::model_visible(input)) })
        }
    }

    #[test]
    fn registry_requires_explicit_capability_grants() {
        let tool = Arc::new(EchoTool::new("echo"));
        let mut registry = ToolRegistry::new();
        registry.register(tool).unwrap();
        let run = RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new());

        let error =
            futures_executor::block_on(registry.invoke("echo", json!({"x": 1}), &run)).unwrap_err();

        assert_eq!(error.kind, ToolErrorKind::CapabilityDenied);
    }

    #[test]
    fn granted_tools_execute_through_the_object_safe_boundary() {
        let tool = Arc::new(EchoTool::new("echo"));
        let mut capabilities = CapabilitySet::new();
        capabilities.grant(tool.descriptor().capability());
        let run = RunContext::root(BudgetTracker::new(Budget::default()), capabilities);
        let mut registry = ToolRegistry::new();
        registry.register(tool).unwrap();

        let output =
            futures_executor::block_on(registry.invoke("echo", json!({"x": 1}), &run)).unwrap();

        assert_eq!(output.structured_content, Some(json!({"x": 1})));
    }

    #[test]
    fn invalid_enum_reports_input_path_allowed_values_and_safe_actual_value() {
        let mut tool = EchoTool::new("market_history");
        tool.descriptor.input_schema = json!({
            "type": "object",
            "properties": {
                "lookback": {"type": "string", "enum": ["1d", "7d", "30d"]}
            },
            "required": ["lookback"]
        });
        let tool = Arc::new(tool);
        let mut capabilities = CapabilitySet::new();
        capabilities.grant(tool.descriptor().capability());
        let run = RunContext::root(BudgetTracker::new(Budget::default()), capabilities);
        let mut registry = ToolRegistry::new();
        registry.register(tool).unwrap();

        let error = futures_executor::block_on(registry.invoke(
            "market_history",
            json!({"lookback": "forever"}),
            &run,
        ))
        .unwrap_err();

        assert_eq!(error.kind, ToolErrorKind::InvalidInput);
        assert_eq!(error.metadata["tool.name"], "market_history");
        assert_eq!(error.metadata["validation.input_path"], "/lookback");
        assert_eq!(error.metadata["validation.keyword"], "enum");
        assert_eq!(error.metadata["validation.actual_value"], "forever");
        assert_eq!(
            error.metadata["validation.allowed_values"],
            json!(["1d", "7d", "30d"])
        );
        assert!(error.message.contains("call the tool again"));
    }

    #[test]
    fn duplicate_names_are_rejected_instead_of_replaced() {
        let mut registry = ToolRegistry::new();
        registry.register(Arc::new(EchoTool::new("echo"))).unwrap();

        let error = registry
            .register(Arc::new(EchoTool::new("echo")))
            .unwrap_err();

        assert_eq!(error, ToolRegistrationError::DuplicateName("echo".into()));
    }

    #[test]
    fn invalid_schemas_fail_during_registration() {
        let mut tool = EchoTool::new("invalid");
        tool.descriptor.input_schema = json!({"type":"not-a-json-schema-type"});
        let error = ToolRegistry::new().register(Arc::new(tool)).unwrap_err();
        assert!(matches!(
            error,
            ToolRegistrationError::InvalidSchema {
                direction: "input",
                ..
            }
        ));
    }

    #[test]
    fn output_contract_and_size_are_enforced_after_execution() {
        let tool = Arc::new(EchoTool::new("bounded"));
        let mut capabilities = CapabilitySet::new();
        capabilities.grant(tool.descriptor().capability());
        let run = RunContext::root(BudgetTracker::new(Budget::default()), capabilities);
        let mut registry = ToolRegistry::new().with_limits(ToolLimits {
            max_input_bytes: 1024,
            max_output_bytes: 32,
        });
        registry.register(tool).unwrap();

        let error = futures_executor::block_on(registry.invoke(
            "bounded",
            json!({"payload":"this output is intentionally larger than the limit"}),
            &run,
        ))
        .unwrap_err();
        assert_eq!(error.kind, ToolErrorKind::InvalidOutput);
    }
}
