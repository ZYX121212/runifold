use std::{collections::BTreeMap, fmt, sync::Arc};

use futures_util::future::{Either, select};
use runifold_core::RunContext;
use runifold_model::ToolSpec;
use serde_json::Value;

use crate::{
    Tool, ToolContext, ToolDescriptor, ToolError, ToolErrorKind, ToolFuture, ToolOutput,
    ToolRegistrationError,
};

/// Immutable-name registry and capability gate for tools.
#[derive(Clone, Default)]
pub struct ToolRegistry {
    tools: BTreeMap<String, Arc<dyn Tool>>,
}

impl ToolRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
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
        self.tools.insert(name.into(), tool);
        Ok(())
    }

    /// Returns model-facing specifications in deterministic name order.
    pub fn model_specs(&self) -> Vec<ToolSpec> {
        self.tools
            .values()
            .map(|tool| tool.descriptor().model_spec())
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
        self.tools.get(name).map(|tool| tool.descriptor())
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
            let tool = self.tools.get(name).ok_or_else(|| {
                ToolError::local(
                    ToolErrorKind::NotFound,
                    format!("tool `{name}` is not registered"),
                )
            })?;
            let descriptor = tool.descriptor();
            if !run.capabilities().contains(descriptor.id) {
                return Err(ToolError::local(
                    ToolErrorKind::CapabilityDenied,
                    format!("run is not granted tool capability `{name}`"),
                ));
            }
            let context = ToolContext::for_run(run);
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
                Box::pin(tool.invoke(input, context)),
            )
            .await
            {
                Either::Left(_) => Err(ToolError::local(
                    ToolErrorKind::Cancelled,
                    "tool invocation was cancelled",
                )),
                Either::Right((result, _)) => result,
            }
        })
    }
}

impl fmt::Debug for ToolRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ToolRegistry")
            .field("tools", &self.tools.keys().collect::<Vec<_>>())
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
        Tool, ToolContext, ToolDescriptor, ToolError, ToolErrorKind, ToolFuture, ToolOutput,
        ToolRegistrationError,
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

        assert_eq!(output.value, json!({"x": 1}));
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
}
