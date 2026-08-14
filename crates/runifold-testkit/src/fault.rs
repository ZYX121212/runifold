//! Deterministic fault injection shared by model, Tool, and recovery tests.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};

use futures_util::StreamExt;
use runifold_model::{
    ContentPart, Model, ModelCallContext, ModelCapabilities, ModelError, ModelEventStream,
    ModelFuture, ModelRef, ModelRequest, ModelStreamEvent,
};
use runifold_tool::{Tool, ToolContext, ToolDescriptor, ToolError, ToolFuture, ToolOutput};
use serde_json::Value;
use thiserror::Error;

/// Fault applied to one model invocation.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum ModelFault {
    /// Executes the invocation without injection.
    Pass,
    /// Fails before a canonical stream is opened.
    FailOpen(ModelError),
    /// Ends the stream after the specified number of canonical events.
    DisconnectAfterEvents(usize),
    /// Ends the stream immediately after the first completed Tool call.
    DisconnectAfterToolCall,
}

/// Fault applied to one named Tool invocation.
#[derive(Clone, Debug)]
pub struct ToolFault {
    tool: String,
    invocation: u64,
    error: ToolError,
}

impl ToolFault {
    /// Fails the selected one-based invocation of a named Tool.
    pub fn on_invocation(tool: impl Into<String>, invocation: u64, error: ToolError) -> Self {
        Self {
            tool: tool.into(),
            invocation,
            error,
        }
    }
}

#[derive(Debug, Default)]
struct FaultState {
    model_faults: VecDeque<ModelFault>,
    tool_faults: Vec<ToolFault>,
    tool_invocations: BTreeMap<String, u64>,
    tool_executions: BTreeMap<String, u64>,
    runtime_restarts: u64,
}

/// Shared deterministic fault plan and execution counters.
#[derive(Clone, Debug, Default)]
pub struct FaultController {
    state: Arc<Mutex<FaultState>>,
}

/// Fluent scenario name for [`FaultController`].
pub type FaultScenario = FaultController;

impl FaultController {
    /// Creates an empty fault plan.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queues a fault for the next model invocation.
    #[must_use]
    pub fn model_fault(self, fault: ModelFault) -> Self {
        self.lock().model_faults.push_back(fault);
        self
    }

    /// Disconnects the next model stream after a completed Tool call.
    #[must_use]
    pub fn disconnect_after_tool_call(self) -> Self {
        self.model_fault(ModelFault::DisconnectAfterToolCall)
    }

    /// Disconnects the next model stream after `events` canonical events.
    #[must_use]
    pub fn disconnect_after_model_events(self, events: usize) -> Self {
        self.model_fault(ModelFault::DisconnectAfterEvents(events))
    }

    /// Fails the next model invocation before opening its stream.
    #[must_use]
    pub fn fail_next_model(self, error: ModelError) -> Self {
        self.model_fault(ModelFault::FailOpen(error))
    }

    /// Queues a named Tool fault.
    #[must_use]
    pub fn tool_fault(self, fault: ToolFault) -> Self {
        self.lock().tool_faults.push(fault);
        self
    }

    /// Fails the selected one-based invocation of a named Tool.
    #[must_use]
    pub fn fail_tool_on_invocation(
        self,
        tool: impl Into<String>,
        invocation: u64,
        error: ToolError,
    ) -> Self {
        self.tool_fault(ToolFault::on_invocation(tool, invocation, error))
    }

    /// Wraps a model with this plan.
    pub fn model<M>(&self, model: M) -> FaultInjectingModel<M>
    where
        M: Model,
    {
        FaultInjectingModel {
            inner: model,
            controller: self.clone(),
        }
    }

    /// Wraps a Tool with this plan.
    pub fn tool(&self, tool: Arc<dyn Tool>) -> FaultInjectingTool {
        FaultInjectingTool {
            inner: tool,
            controller: self.clone(),
        }
    }

    /// Returns the number of observed invocations for one Tool.
    pub fn tool_invocations(&self, tool: &str) -> u64 {
        self.lock().tool_invocations.get(tool).copied().unwrap_or(0)
    }

    /// Returns the number of calls forwarded to the underlying Tool.
    pub fn tool_executions(&self, tool: &str) -> u64 {
        self.lock().tool_executions.get(tool).copied().unwrap_or(0)
    }

    /// Returns the number of explicit runtime reconstructions.
    pub fn runtime_restarts(&self) -> u64 {
        self.lock().runtime_restarts
    }

    /// Asserts exact Tool execution count with a typed failure.
    ///
    /// # Errors
    ///
    /// Returns [`ScenarioAssertionError`] when the observed count differs.
    pub fn assert_tool_executed_exactly(
        &self,
        tool: &str,
        expected: u64,
    ) -> Result<(), ScenarioAssertionError> {
        let actual = self.tool_executions(tool);
        if actual == expected {
            Ok(())
        } else {
            Err(ScenarioAssertionError::ToolInvocationCount {
                tool: tool.into(),
                expected,
                actual,
            })
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FaultState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn next_model_fault(&self) -> ModelFault {
        self.lock()
            .model_faults
            .pop_front()
            .unwrap_or(ModelFault::Pass)
    }

    fn tool_result(&self, tool: &str) -> Option<ToolError> {
        let mut state = self.lock();
        let invocation = {
            let count = state.tool_invocations.entry(tool.into()).or_default();
            *count = count.saturating_add(1);
            *count
        };
        let error = state
            .tool_faults
            .iter()
            .find(|fault| fault.tool == tool && fault.invocation == invocation)
            .map(|fault| fault.error.clone());
        if error.is_none() {
            let count = state.tool_executions.entry(tool.into()).or_default();
            *count = count.saturating_add(1);
        }
        error
    }

    fn record_restart(&self) {
        let mut state = self.lock();
        state.runtime_restarts = state.runtime_restarts.saturating_add(1);
    }
}

/// Model wrapper applying one queued fault per invocation.
#[derive(Clone, Debug)]
pub struct FaultInjectingModel<M> {
    inner: M,
    controller: FaultController,
}

impl<M> Model for FaultInjectingModel<M>
where
    M: Model,
{
    fn capabilities<'a>(
        &'a self,
        model: &'a ModelRef,
    ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
        self.inner.capabilities(model)
    }

    fn stream(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
        let fault = self.controller.next_model_fault();
        Box::pin(async move {
            if let ModelFault::FailOpen(error) = fault {
                return Err(error);
            }
            let stream = self.inner.stream(request, context).await?;
            match fault {
                ModelFault::Pass => Ok(stream),
                ModelFault::DisconnectAfterEvents(limit) => Ok(Box::pin(stream.take(limit))),
                ModelFault::DisconnectAfterToolCall => {
                    let truncated = futures_util::stream::unfold(
                        (stream, false),
                        |(mut stream, stopped)| async move {
                            if stopped {
                                return None;
                            }
                            let item = stream.next().await?;
                            let stop = matches!(
                                &item,
                                Ok(ModelStreamEvent::ContentPartCompleted {
                                    part: ContentPart::ToolCall(_),
                                    ..
                                })
                            );
                            Some((item, (stream, stop)))
                        },
                    );
                    Ok(Box::pin(truncated))
                }
                ModelFault::FailOpen(_) => unreachable!("handled before opening the stream"),
            }
        })
    }
}

/// Tool wrapper applying named, one-based invocation faults.
#[derive(Clone)]
pub struct FaultInjectingTool {
    inner: Arc<dyn Tool>,
    controller: FaultController,
}

impl Tool for FaultInjectingTool {
    fn descriptor(&self) -> &ToolDescriptor {
        self.inner.descriptor()
    }

    fn invoke(
        &self,
        input: Value,
        context: ToolContext,
    ) -> ToolFuture<'_, Result<ToolOutput, ToolError>> {
        let injected = self.controller.tool_result(&self.inner.descriptor().name);
        if let Some(error) = injected {
            return Box::pin(async move { Err(error) });
        }
        self.inner.invoke(input, context)
    }
}

impl std::fmt::Debug for FaultInjectingTool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FaultInjectingTool")
            .field("tool", &self.inner.descriptor().name)
            .finish_non_exhaustive()
    }
}

/// Reconstructs runtime state from one factory while preserving external
/// fixtures such as stores, fault counters, and cassette servers.
pub struct RecoveryHarness<T, F>
where
    F: FnMut() -> T,
{
    current: T,
    factory: F,
    controller: FaultController,
}

impl<T, F> RecoveryHarness<T, F>
where
    F: FnMut() -> T,
{
    /// Constructs the first runtime instance.
    pub fn new(mut factory: F, controller: FaultController) -> Self {
        let current = factory();
        Self {
            current,
            factory,
            controller,
        }
    }

    /// Returns the active runtime fixture.
    pub const fn current(&self) -> &T {
        &self.current
    }

    /// Returns the active runtime fixture mutably.
    pub fn current_mut(&mut self) -> &mut T {
        &mut self.current
    }

    /// Drops and reconstructs runtime-owned state while retaining the shared
    /// external fixture and fault controller.
    pub fn restart(&mut self) -> &mut T {
        self.current = (self.factory)();
        self.controller.record_restart();
        &mut self.current
    }
}

/// Typed failure from a scenario assertion.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ScenarioAssertionError {
    /// A Tool did not execute the expected number of times.
    #[error("tool `{tool}` executed {actual} times; expected {expected}")]
    ToolInvocationCount {
        /// Tool name.
        tool: String,
        /// Expected count.
        expected: u64,
        /// Observed count.
        actual: u64,
    },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use runifold_core::RetrySafety;
    use runifold_model::{
        ContentPart, FinishReason, Message, Model, ModelCallContext, ModelErrorKind, ModelRef,
        ModelRequest, ModelStreamEvent, ToolCall,
    };
    use runifold_tool::{ToolError, ToolErrorKind};

    use crate::ScriptedModel;

    use super::{FaultController, ModelFault, RecoveryHarness, ToolFault};

    #[test]
    fn disconnect_after_tool_call_never_reaches_terminal_success() {
        let model = ScriptedModel::new();
        let model_ref = ModelRef::new("test", "model");
        model.enqueue([
            ModelStreamEvent::ResponseStarted {
                id: Some("response".into()),
                model: model_ref.clone(),
            },
            ModelStreamEvent::ContentPartCompleted {
                index: 0,
                part: ContentPart::ToolCall(ToolCall {
                    id: "call".into(),
                    name: "charge".into(),
                    arguments: serde_json::json!({}),
                    raw_arguments: Some("{}".into()),
                    metadata: BTreeMap::new(),
                }),
            },
            ModelStreamEvent::ResponseCompleted {
                finish_reason: FinishReason::ToolCalls,
                provider_metadata: BTreeMap::new(),
            },
        ]);
        let controller = FaultController::new().model_fault(ModelFault::DisconnectAfterToolCall);
        let model = controller.model(model);
        let request = ModelRequest::new(model_ref, Message::user("run"));

        let error =
            futures_executor::block_on(model.invoke(request, ModelCallContext::new())).unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::Protocol);
    }

    #[test]
    fn tool_faults_are_one_based_and_distinguish_attempts_from_execution() {
        let mut error = ToolError::local(ToolErrorKind::Execution, "injected");
        error.retry_safety = RetrySafety::Safe;
        let controller =
            FaultController::new().tool_fault(ToolFault::on_invocation("charge", 1, error.clone()));

        assert_eq!(controller.tool_result("charge"), Some(error));
        assert_eq!(controller.tool_result("charge"), None);
        assert_eq!(controller.tool_invocations("charge"), 2);
        assert_eq!(controller.tool_executions("charge"), 1);
        controller
            .assert_tool_executed_exactly("charge", 1)
            .unwrap();
    }

    #[test]
    fn recovery_harness_reconstructs_runtime_and_counts_restarts() {
        let controller = FaultController::new();
        let mut next = 0_u64;
        let mut harness = RecoveryHarness::new(
            || {
                next += 1;
                next
            },
            controller.clone(),
        );

        assert_eq!(*harness.current(), 1);
        assert_eq!(*harness.restart(), 2);
        assert_eq!(controller.runtime_restarts(), 1);
    }
}
