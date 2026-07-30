use std::{collections::BTreeMap, fmt, sync::Arc};

use runifold_core::{
    BudgetEvent, CapabilitySet, ChildEvent, Instant, RunContext, RunEventKind, Usage,
};
use runifold_model::ToolSpec;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::{
    Agent, AgentDescriptor, AgentError, AgentOutcome, DelegationRequest, GatewayMiddleware,
    GatewayNext,
};

const DELEGATION_DEPTH_KEY: &str = "runifold.agent.delegation_depth";
const DELEGATED_AGENT_KEY: &str = "runifold.agent.delegated_agent";

/// One explicitly configured route from a caller to a child agent.
#[derive(Clone)]
pub struct AgentRoute {
    descriptor: AgentDescriptor,
    agent: Arc<Agent>,
    capabilities: CapabilitySet,
}

impl AgentRoute {
    /// Creates a route whose child starts with no capabilities.
    pub fn new(descriptor: AgentDescriptor, agent: Arc<Agent>) -> Self {
        Self {
            descriptor,
            agent,
            capabilities: CapabilitySet::new(),
        }
    }

    /// Sets the exact capabilities requested for the child run.
    ///
    /// Invocation still rejects any grant not held by the parent.
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: CapabilitySet) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Returns the model-facing and policy-facing route contract.
    pub const fn descriptor(&self) -> &AgentDescriptor {
        &self.descriptor
    }

    /// Returns the configured child agent.
    pub fn agent(&self) -> &Arc<Agent> {
        &self.agent
    }

    /// Returns the exact child capability grant.
    pub const fn capabilities(&self) -> &CapabilitySet {
        &self.capabilities
    }
}

impl fmt::Debug for AgentRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentRoute")
            .field("descriptor", &self.descriptor)
            .field("agent", &self.agent.name())
            .field("capabilities", &self.capabilities)
            .finish()
    }
}

/// Capability-gated router for parent-to-child agent delegation.
#[derive(Clone)]
pub struct AgentGateway {
    routes: BTreeMap<String, AgentRoute>,
    middleware: Vec<Arc<dyn GatewayMiddleware>>,
    max_depth: u32,
}

impl Default for AgentGateway {
    fn default() -> Self {
        Self {
            routes: BTreeMap::new(),
            middleware: Vec::new(),
            max_depth: 8,
        }
    }
}

impl AgentGateway {
    /// Creates an empty gateway.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum delegation depth accepted by this gateway.
    #[must_use]
    pub fn with_max_depth(mut self, max_depth: u32) -> Self {
        self.max_depth = max_depth;
        self
    }

    /// Appends around-middleware to the gateway chain.
    ///
    /// Middleware runs in registration order before the child boundary and in
    /// reverse order after `next` completes.
    #[must_use]
    pub fn layer(mut self, middleware: Arc<dyn GatewayMiddleware>) -> Self {
        self.middleware.push(middleware);
        self
    }

    /// Appends around-middleware without consuming the gateway.
    pub fn push_middleware(&mut self, middleware: Arc<dyn GatewayMiddleware>) {
        self.middleware.push(middleware);
    }

    /// Registers a route without replacing an existing model-facing name.
    ///
    /// # Errors
    ///
    /// Returns [`AgentRegistrationError`] when the route name is blank or
    /// already registered.
    pub fn register(&mut self, route: AgentRoute) -> Result<(), AgentRegistrationError> {
        let name = route.descriptor.name.trim();
        if name.is_empty() {
            return Err(AgentRegistrationError::EmptyName);
        }
        if self.routes.contains_key(name) {
            return Err(AgentRegistrationError::DuplicateName(name.into()));
        }
        self.routes.insert(name.into(), route);
        Ok(())
    }

    /// Returns whether a route is registered under `name`.
    pub fn contains(&self, name: &str) -> bool {
        self.routes.contains_key(name)
    }

    /// Returns the immutable route descriptor registered under `name`.
    pub fn descriptor(&self, name: &str) -> Option<&AgentDescriptor> {
        self.routes.get(name).map(|route| &route.descriptor)
    }

    /// Returns model-facing route specifications in deterministic name order.
    pub fn model_specs(&self) -> Vec<ToolSpec> {
        self.routes
            .values()
            .map(|route| route.descriptor.model_spec())
            .collect()
    }

    /// Returns the number of registered routes.
    pub fn len(&self) -> usize {
        self.routes.len()
    }

    /// Returns whether no routes are registered.
    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }

    /// Invokes a child agent through an explicitly authorized route.
    ///
    /// # Errors
    ///
    /// Returns [`GatewayError`] when lifecycle, capability, authority, depth,
    /// budget, or child execution checks fail.
    pub async fn delegate(
        &self,
        name: &str,
        input: impl Into<String>,
        parent: &RunContext,
    ) -> Result<AgentOutcome, GatewayError> {
        let route = self.routes.get(name).ok_or_else(|| {
            GatewayError::new(
                GatewayErrorKind::NotFound,
                format!("agent route `{name}` is not registered"),
            )
        })?;
        let request =
            DelegationRequest::new(route.descriptor.clone(), input.into(), parent.clone());
        GatewayNext {
            middleware: &self.middleware,
            route,
            max_depth: self.max_depth,
            index: 0,
        }
        .run(request)
        .await
    }
}

impl fmt::Debug for AgentGateway {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentGateway")
            .field("routes", &self.routes)
            .field("middleware_count", &self.middleware.len())
            .field("max_depth", &self.max_depth)
            .finish()
    }
}

pub(crate) async fn execute_route(
    route: &AgentRoute,
    max_depth: u32,
    request: DelegationRequest,
) -> Result<AgentOutcome, GatewayError> {
    let parent = request.parent();
    let name = &request.descriptor().name;
    let depth = validate_route(route, max_depth, parent, name)?;
    let usage = parent
        .budget()
        .try_consume(Usage {
            delegations: 1,
            ..Usage::default()
        })
        .map_err(|error| GatewayError::new(GatewayErrorKind::BudgetExceeded, error.to_string()))?;
    parent
        .record(RunEventKind::Budget(BudgetEvent::Updated { usage }), None)
        .map_err(observability_error)?;

    let mut child = parent.child(route.capabilities.clone()).map_err(|error| {
        GatewayError::new(GatewayErrorKind::AuthorityEscalation, error.to_string())
    })?;
    child
        .metadata_mut()
        .insert(DELEGATION_DEPTH_KEY.into(), Value::from(depth + 1));
    child
        .metadata_mut()
        .insert(DELEGATED_AGENT_KEY.into(), Value::from(name.clone()));

    let child_started = parent
        .record(
            RunEventKind::Child(ChildEvent::Started {
                child_run_id: child.run_id(),
            }),
            None,
        )
        .map_err(observability_error)?
        .map(|event| event.meta.event_id);
    if let Some(event_id) = child_started {
        child = child.with_cause(event_id);
    }

    let result = route
        .agent
        .run(request.input().to_owned(), &child)
        .await
        .map_err(|error| GatewayError::from_agent(&error));
    record_child_terminal(parent, child.run_id(), child_started, &result)?;
    result
}

fn validate_route(
    route: &AgentRoute,
    max_depth: u32,
    parent: &RunContext,
    name: &str,
) -> Result<u64, GatewayError> {
    if parent.cancellation().is_cancelled() {
        return Err(GatewayError::new(
            GatewayErrorKind::Cancelled,
            "delegation was cancelled before the child run started",
        ));
    }
    if parent
        .deadline()
        .is_some_and(|deadline| deadline <= Instant::now())
    {
        return Err(GatewayError::new(
            GatewayErrorKind::DeadlineExceeded,
            "delegation deadline elapsed before the child run started",
        ));
    }
    if max_depth == 0 {
        return Err(GatewayError::new(
            GatewayErrorKind::MaxDepth,
            "gateway max_depth must be greater than zero",
        ));
    }
    if !parent.capabilities().contains(route.descriptor.id) {
        return Err(GatewayError::new(
            GatewayErrorKind::CapabilityDenied,
            format!("run is not granted agent capability `{name}`"),
        ));
    }
    if let Some(missing) = route.capabilities.first_missing_from(parent.capabilities()) {
        return Err(GatewayError::new(
            GatewayErrorKind::AuthorityEscalation,
            format!(
                "child agent `{name}` requested capability `{}` not held by its parent",
                missing.name
            ),
        ));
    }

    let depth = delegation_depth(parent);
    if depth >= u64::from(max_depth) {
        return Err(GatewayError::new(
            GatewayErrorKind::MaxDepth,
            format!("delegation depth {depth} reached gateway maximum {max_depth}"),
        ));
    }
    Ok(depth)
}

fn record_child_terminal(
    parent: &RunContext,
    child_run_id: runifold_core::RunId,
    child_started: Option<runifold_core::EventId>,
    result: &Result<AgentOutcome, GatewayError>,
) -> Result<(), GatewayError> {
    let child_event = match result {
        Ok(_) => ChildEvent::Completed { child_run_id },
        Err(error) if error.kind == GatewayErrorKind::Cancelled => {
            ChildEvent::Cancelled { child_run_id }
        }
        Err(_) => ChildEvent::Failed { child_run_id },
    };
    parent
        .record(RunEventKind::Child(child_event), child_started)
        .map_err(observability_error)?;
    Ok(())
}

fn observability_error(error: runifold_core::JournalError) -> GatewayError {
    GatewayError::new(GatewayErrorKind::ObservabilityFailed, error.message)
}

fn delegation_depth(run: &RunContext) -> u64 {
    run.metadata()
        .get(DELEGATION_DEPTH_KEY)
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

/// Normalized gateway failure category.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum GatewayErrorKind {
    /// The requested route is not registered.
    NotFound,
    /// The delegation input did not match the canonical schema.
    InvalidInput,
    /// The parent was not granted the agent capability.
    CapabilityDenied,
    /// The configured child grant would amplify parent authority.
    AuthorityEscalation,
    /// The configured delegation-depth bound was reached.
    MaxDepth,
    /// The shared run-tree budget rejected the delegation.
    BudgetExceeded,
    /// Delegated work was cancelled.
    Cancelled,
    /// Delegated work exceeded its effective deadline.
    DeadlineExceeded,
    /// The child agent failed without violating a hard runtime invariant.
    ChildFailed,
    /// Gateway middleware or policy denied the delegation.
    PolicyDenied,
    /// The configured journal rejected a gateway event.
    ObservabilityFailed,
}

/// Structured failure from the agent delegation boundary.
#[derive(Clone, Debug, Deserialize, Error, Eq, PartialEq, Serialize)]
#[error("{kind:?}: {message}")]
pub struct GatewayError {
    /// Normalized category.
    pub kind: GatewayErrorKind,
    /// Safe human-readable explanation.
    pub message: String,
}

impl GatewayError {
    /// Creates a gateway error.
    pub fn new(kind: GatewayErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    fn from_agent(error: &AgentError) -> Self {
        let kind = match error {
            AgentError::Model(error)
                if matches!(error.kind, runifold_model::ModelErrorKind::Cancelled) =>
            {
                GatewayErrorKind::Cancelled
            }
            AgentError::Model(error)
                if matches!(error.kind, runifold_model::ModelErrorKind::DeadlineExceeded) =>
            {
                GatewayErrorKind::DeadlineExceeded
            }
            AgentError::Tool(error)
                if matches!(error.kind, runifold_tool::ToolErrorKind::CapabilityDenied) =>
            {
                GatewayErrorKind::CapabilityDenied
            }
            AgentError::Tool(error)
                if matches!(error.kind, runifold_tool::ToolErrorKind::Cancelled) =>
            {
                GatewayErrorKind::Cancelled
            }
            AgentError::Tool(error)
                if matches!(error.kind, runifold_tool::ToolErrorKind::DeadlineExceeded) =>
            {
                GatewayErrorKind::DeadlineExceeded
            }
            AgentError::Budget(_) => GatewayErrorKind::BudgetExceeded,
            AgentError::Gateway(error) => error.kind.clone(),
            AgentError::Journal(_) => GatewayErrorKind::ObservabilityFailed,
            _ => GatewayErrorKind::ChildFailed,
        };
        Self::new(kind, error.to_string())
    }
}

/// Failure to add an agent route to a gateway.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum AgentRegistrationError {
    /// Agent route names must not be blank.
    #[error("agent route name cannot be empty")]
    EmptyName,
    /// Another route already owns the model-facing name.
    #[error("agent route `{0}` is already registered")]
    DuplicateName(String),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use runifold_core::{Budget, BudgetTracker, CapabilitySet, RunContext};
    use runifold_model::ModelRef;
    use runifold_testkit::ScriptedModel;

    use crate::{
        Agent, AgentDescriptor, AgentGateway, AgentRoute, GatewayErrorKind,
        gateway::DELEGATION_DEPTH_KEY,
    };

    fn gateway_and_run() -> (AgentGateway, RunContext, ScriptedModel) {
        let model = ScriptedModel::new();
        let child = Arc::new(Agent::new(
            "child",
            Arc::new(model.clone()),
            ModelRef::new("test", "child"),
        ));
        let descriptor = AgentDescriptor::new("ask_child", "Delegate work");
        let mut gateway = AgentGateway::new();
        gateway
            .register(AgentRoute::new(descriptor.clone(), child))
            .unwrap();
        let mut capabilities = CapabilitySet::new();
        capabilities.grant(descriptor.capability());
        let run = RunContext::root(BudgetTracker::new(Budget::default()), capabilities);
        (gateway, run, model)
    }

    #[test]
    fn preexisting_cancellation_stops_before_budget_or_child_execution() {
        let (gateway, run, model) = gateway_and_run();
        run.cancellation().cancel();

        let error =
            futures_executor::block_on(gateway.delegate("ask_child", "work", &run)).unwrap_err();

        assert_eq!(error.kind, GatewayErrorKind::Cancelled);
        assert_eq!(run.budget().usage().delegations, 0);
        assert!(model.recorded_requests().is_empty());
    }

    #[test]
    fn depth_limit_stops_before_budget_or_child_execution() {
        let (gateway, mut run, model) = gateway_and_run();
        let gateway = gateway.with_max_depth(1);
        run.metadata_mut()
            .insert(DELEGATION_DEPTH_KEY.into(), serde_json::json!(1));

        let error =
            futures_executor::block_on(gateway.delegate("ask_child", "work", &run)).unwrap_err();

        assert_eq!(error.kind, GatewayErrorKind::MaxDepth);
        assert_eq!(run.budget().usage().delegations, 0);
        assert!(model.recorded_requests().is_empty());
    }
}
