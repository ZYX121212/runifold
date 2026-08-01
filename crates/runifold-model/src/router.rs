use std::sync::{Arc, Mutex};

use runifold_core::RetrySafety;
use thiserror::Error;

use crate::circuit::{BreakerPermit, BreakerState, RoutePermit, SharedBreakerState};
use crate::{
    CircuitBreakerConfig, Model, ModelCallContext, ModelCapabilities, ModelError, ModelErrorKind,
    ModelEventStream, ModelFuture, ModelRef, ModelRequest, ModelRetryPolicy, ModelRouteHealth,
    ModelStreamEvent, ProviderEvent, RouterClock, RouterSleeper, SystemRouterClock,
    SystemRouterSleeper,
};

mod capabilities;
mod execution;

use capabilities::intersect_capabilities;
use execution::{RoutingRuntime, routed_stream};

/// One named physical endpoint eligible for a logical model invocation.
#[derive(Clone)]
pub struct ModelRoute {
    name: String,
    model: Arc<dyn Model>,
    target: ModelRef,
    health: SharedBreakerState,
}

impl ModelRoute {
    /// Creates a physical route.
    pub fn new(name: impl Into<String>, model: Arc<dyn Model>, target: ModelRef) -> Self {
        Self {
            name: name.into(),
            model,
            target,
            health: Arc::new(Mutex::new(BreakerState::default())),
        }
    }

    /// Returns the stable route name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the provider-qualified physical target.
    pub const fn target(&self) -> &ModelRef {
        &self.target
    }
}

impl std::fmt::Debug for ModelRoute {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ModelRoute")
            .field("name", &self.name)
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

/// Explicit authority for selecting another physical model after a failure.
///
/// Safe errors are always eligible. Errors with unknown retry safety are
/// eligible only when their kind is explicitly added. Cancellation and errors
/// marked unsafe are never eligible.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ModelFallbackPolicy {
    unknown_safety_kinds: Vec<ModelErrorKind>,
}

impl ModelFallbackPolicy {
    /// Creates the conservative policy: only errors explicitly marked safe.
    pub const fn safe_only() -> Self {
        Self {
            unknown_safety_kinds: Vec::new(),
        }
    }

    /// Allows fallback for one error kind whose retry safety is unknown.
    ///
    /// This is explicit authority to risk duplicate provider cost. It never
    /// overrides cancellation or an error marked unsafe.
    #[must_use]
    pub fn allow_unknown(mut self, kind: ModelErrorKind) -> Self {
        if !self.unknown_safety_kinds.contains(&kind) {
            self.unknown_safety_kinds.push(kind);
        }
        self
    }

    fn permits(&self, error: &ModelError) -> bool {
        if error.kind == ModelErrorKind::Cancelled {
            return false;
        }
        match error.retry_safety {
            RetrySafety::Safe => true,
            RetrySafety::Unknown => self.unknown_safety_kinds.contains(&error.kind),
            _ => false,
        }
    }
}

/// Invalid logical router configuration.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum ModelRouterBuildError {
    /// Logical provider or model name is blank.
    #[error("logical model provider and name cannot be empty")]
    EmptyLogicalModel,
    /// A route name is blank.
    #[error("model route name cannot be empty")]
    EmptyRouteName,
    /// A physical provider or model name is blank.
    #[error("physical model provider and name cannot be empty")]
    EmptyTarget,
    /// A route name was registered more than once.
    #[error("model route `{0}` is already registered")]
    DuplicateRoute(String),
    /// No physical route was registered.
    #[error("model router requires at least one route")]
    NoRoutes,
}

/// Fluent, validation-preserving assembly of a [`ModelRouter`].
pub struct ModelRouterBuilder {
    logical: ModelRef,
    routes: Vec<ModelRoute>,
    policy: ModelFallbackPolicy,
    circuit_breaker: Option<CircuitBreakerConfig>,
    clock: Arc<dyn RouterClock>,
    retry_policy: Option<ModelRetryPolicy>,
    sleeper: Arc<dyn RouterSleeper>,
    error: Option<ModelRouterBuildError>,
}

impl ModelRouterBuilder {
    /// Adds a physical route in selection order.
    #[must_use]
    pub fn route(
        mut self,
        name: impl Into<String>,
        model: Arc<dyn Model>,
        target: ModelRef,
    ) -> Self {
        if self.error.is_some() {
            return self;
        }
        let route = ModelRoute::new(name, model, target);
        if route.name.trim().is_empty() {
            self.error = Some(ModelRouterBuildError::EmptyRouteName);
        } else if route.target.provider.trim().is_empty() || route.target.name.trim().is_empty() {
            self.error = Some(ModelRouterBuildError::EmptyTarget);
        } else if self
            .routes
            .iter()
            .any(|existing| existing.name == route.name)
        {
            self.error = Some(ModelRouterBuildError::DuplicateRoute(route.name));
        } else {
            self.routes.push(route);
        }
        self
    }

    /// Sets fallback authority.
    #[must_use]
    pub fn fallback_policy(mut self, policy: ModelFallbackPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Enables an independent circuit breaker for every physical route.
    #[must_use]
    pub fn circuit_breaker(mut self, config: CircuitBreakerConfig) -> Self {
        self.circuit_breaker = Some(config);
        self
    }

    /// Replaces the monotonic clock used by circuit-breaker policy.
    #[must_use]
    pub fn clock(mut self, clock: Arc<dyn RouterClock>) -> Self {
        self.clock = clock;
        self
    }

    /// Enables bounded same-route retries before fallback selection.
    #[must_use]
    pub fn retry_policy(mut self, policy: ModelRetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    /// Replaces the asynchronous timer used by retry backoff.
    #[must_use]
    pub fn sleeper(mut self, sleeper: Arc<dyn RouterSleeper>) -> Self {
        self.sleeper = sleeper;
        self
    }

    /// Validates and builds the router.
    ///
    /// # Errors
    ///
    /// Returns [`ModelRouterBuildError`] for blank identities, duplicate route
    /// names, or an empty route list.
    pub fn build(self) -> Result<ModelRouter, ModelRouterBuildError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        if self.logical.provider.trim().is_empty() || self.logical.name.trim().is_empty() {
            return Err(ModelRouterBuildError::EmptyLogicalModel);
        }
        if self.routes.is_empty() {
            return Err(ModelRouterBuildError::NoRoutes);
        }
        Ok(ModelRouter {
            logical: self.logical,
            routes: self.routes,
            policy: self.policy,
            circuit_breaker: self.circuit_breaker,
            clock: self.clock,
            retry_policy: self.retry_policy,
            sleeper: self.sleeper,
        })
    }
}

impl std::fmt::Debug for ModelRouterBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ModelRouterBuilder")
            .field("logical", &self.logical)
            .field("routes", &self.routes)
            .field("policy", &self.policy)
            .field("circuit_breaker", &self.circuit_breaker)
            .field("retry_policy", &self.retry_policy)
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

/// Ordered, provider-neutral fallback routing behind the canonical [`Model`]
/// boundary.
///
/// A router owns process-local retry and circuit-breaker state. Applications
/// should build it once and reuse this value or its clones for the lifetime of
/// the service. Clones share route health; rebuilding a router intentionally
/// starts with fresh health state.
#[derive(Clone)]
pub struct ModelRouter {
    logical: ModelRef,
    routes: Vec<ModelRoute>,
    policy: ModelFallbackPolicy,
    circuit_breaker: Option<CircuitBreakerConfig>,
    clock: Arc<dyn RouterClock>,
    retry_policy: Option<ModelRetryPolicy>,
    sleeper: Arc<dyn RouterSleeper>,
}

impl ModelRouter {
    /// Starts a router builder for one logical model identity.
    pub fn builder(logical: ModelRef) -> ModelRouterBuilder {
        ModelRouterBuilder {
            logical,
            routes: Vec::new(),
            policy: ModelFallbackPolicy::default(),
            circuit_breaker: None,
            clock: Arc::new(SystemRouterClock),
            retry_policy: None,
            sleeper: Arc::new(SystemRouterSleeper),
            error: None,
        }
    }

    /// Returns the identity applications use in [`ModelRequest`].
    pub const fn logical_model(&self) -> &ModelRef {
        &self.logical
    }

    /// Returns physical routes in deterministic selection order.
    pub fn routes(&self) -> &[ModelRoute] {
        &self.routes
    }

    /// Returns a point-in-time health snapshot for every physical route.
    pub fn route_health(&self) -> Vec<ModelRouteHealth> {
        let now = self.clock.now();
        self.routes
            .iter()
            .map(|route| {
                crate::circuit::snapshot(
                    &route.health,
                    route.name.clone(),
                    route.target.clone(),
                    self.circuit_breaker.as_ref(),
                    now,
                )
            })
            .collect()
    }

    fn validate_request(&self, request: &ModelRequest) -> Result<(), ModelError> {
        if request.model != self.logical {
            return Err(ModelError::local(
                ModelErrorKind::InvalidRequest,
                format!(
                    "router for `{}/{}` cannot invoke logical model `{}/{}`",
                    self.logical.provider,
                    self.logical.name,
                    request.model.provider,
                    request.model.name
                ),
            ));
        }
        Ok(())
    }
}

impl Model for ModelRouter {
    fn capabilities<'a>(
        &'a self,
        model: &'a ModelRef,
    ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
        Box::pin(async move {
            if model != &self.logical {
                return Err(ModelError::local(
                    ModelErrorKind::InvalidRequest,
                    "capabilities requested for the wrong logical model",
                ));
            }
            let mut capabilities = Vec::with_capacity(self.routes.len());
            for route in &self.routes {
                capabilities.push(route.model.capabilities(&route.target).await?);
            }
            Ok(intersect_capabilities(capabilities))
        })
    }

    fn stream(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
        let validation = self.validate_request(&request);
        let stream = routed_stream(
            self.routes.clone(),
            self.policy.clone(),
            RoutingRuntime {
                circuit_breaker: self.circuit_breaker.clone(),
                clock: self.clock.clone(),
                retry_policy: self.retry_policy.clone(),
                sleeper: self.sleeper.clone(),
            },
            request,
            context,
        );
        Box::pin(async move {
            validation?;
            Ok(stream)
        })
    }
}

#[cfg(test)]
mod tests;
