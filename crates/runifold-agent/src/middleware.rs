use std::{fmt, future::Future, pin::Pin, sync::Arc};

use runifold_core::{DomainEvent, RunContext, RunEventKind};

use crate::{
    AgentDescriptor, AgentOutcome, AgentRoute, GatewayError, GatewayErrorKind,
    gateway::execute_route,
};

/// A boxed, sendable future returned by gateway extensions.
#[cfg(not(target_arch = "wasm32"))]
pub type GatewayFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A boxed gateway future on single-threaded WASM.
#[cfg(target_arch = "wasm32")]
pub type GatewayFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Immutable-authority request flowing through gateway middleware.
#[derive(Clone, Debug)]
pub struct DelegationRequest {
    descriptor: AgentDescriptor,
    input: String,
    parent: RunContext,
}

impl DelegationRequest {
    pub(crate) fn new(
        descriptor: AgentDescriptor,
        input: impl Into<String>,
        parent: RunContext,
    ) -> Self {
        Self {
            descriptor,
            input: input.into(),
            parent,
        }
    }

    /// Returns the fixed route descriptor.
    pub const fn descriptor(&self) -> &AgentDescriptor {
        &self.descriptor
    }

    /// Returns the child task text.
    pub fn input(&self) -> &str {
        &self.input
    }

    /// Returns the parent execution context.
    ///
    /// Middleware can inspect this context but cannot replace the authority
    /// captured by the gateway.
    pub const fn parent(&self) -> &RunContext {
        &self.parent
    }

    /// Replaces model-visible child input while retaining route and authority.
    #[must_use]
    pub fn with_input(mut self, input: impl Into<String>) -> Self {
        self.input = input.into();
        self
    }
}

/// The remaining immutable gateway chain.
#[derive(Clone, Copy)]
pub struct GatewayNext<'a> {
    pub(crate) middleware: &'a [Arc<dyn GatewayMiddleware>],
    pub(crate) route: &'a AgentRoute,
    pub(crate) max_depth: u32,
    pub(crate) index: usize,
}

impl<'a> GatewayNext<'a> {
    /// Runs the remaining middleware and eventually the protected terminal
    /// delegation boundary.
    pub fn run(
        self,
        request: DelegationRequest,
    ) -> GatewayFuture<'a, Result<AgentOutcome, GatewayError>> {
        Box::pin(async move {
            if let Some(current) = self.middleware.get(self.index) {
                let next = Self {
                    index: self.index + 1,
                    ..self
                };
                current.handle(request, next).await
            } else {
                execute_route(self.route, self.max_depth, request).await
            }
        })
    }
}

/// Object-safe around-middleware for agent delegation.
///
/// Implementations may inspect or transform input, reject an invocation,
/// observe the result, or call `next` more than once for explicit retries.
/// Every call to `next` still passes through the protected terminal authority,
/// lifecycle, depth, and budget checks.
pub trait GatewayMiddleware: Send + Sync {
    /// Handles one delegation invocation.
    fn handle<'a>(
        &'a self,
        request: DelegationRequest,
        next: GatewayNext<'a>,
    ) -> GatewayFuture<'a, Result<AgentOutcome, GatewayError>>;
}

/// Decision returned by a gateway policy.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum GatewayDecision {
    /// Continue to the next middleware.
    Allow,
    /// Reject without invoking downstream middleware or child work.
    Deny {
        /// Safe denial explanation.
        reason: String,
    },
}

/// Object-safe asynchronous policy boundary.
pub trait GatewayPolicy: Send + Sync {
    /// Evaluates one immutable-authority delegation request.
    fn evaluate<'a>(
        &'a self,
        request: &'a DelegationRequest,
    ) -> GatewayFuture<'a, Result<GatewayDecision, GatewayError>>;
}

/// Middleware adapter for a reusable authorization or approval policy.
#[derive(Clone)]
pub struct PolicyMiddleware {
    policy: Arc<dyn GatewayPolicy>,
}

impl PolicyMiddleware {
    /// Wraps an object-safe gateway policy.
    pub fn new(policy: Arc<dyn GatewayPolicy>) -> Self {
        Self { policy }
    }
}

impl GatewayMiddleware for PolicyMiddleware {
    fn handle<'a>(
        &'a self,
        request: DelegationRequest,
        next: GatewayNext<'a>,
    ) -> GatewayFuture<'a, Result<AgentOutcome, GatewayError>> {
        Box::pin(async move {
            let decision = self.policy.evaluate(&request).await;
            match decision {
                Ok(GatewayDecision::Allow) => {
                    record_policy_decision(&request, "policy.allowed")?;
                    next.run(request).await
                }
                Ok(GatewayDecision::Deny { reason }) => {
                    record_policy_decision(&request, "policy.denied")?;
                    Err(GatewayError::new(GatewayErrorKind::PolicyDenied, reason))
                }
                Err(error) => {
                    record_policy_decision(&request, "policy.failed")?;
                    Err(error)
                }
            }
        })
    }
}

fn record_policy_decision(request: &DelegationRequest, name: &str) -> Result<(), GatewayError> {
    request
        .parent()
        .record(
            RunEventKind::Domain(DomainEvent {
                namespace: "runifold.gateway".into(),
                name: name.into(),
                payload: serde_json::json!({
                    "agent": request.descriptor().name,
                }),
            }),
            None,
        )
        .map_err(|error| {
            GatewayError::new(GatewayErrorKind::ObservabilityFailed, error.to_string())
        })?;
    Ok(())
}

impl fmt::Debug for PolicyMiddleware {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PolicyMiddleware(..)")
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    use runifold_core::{
        Budget, BudgetTracker, CapabilitySet, InMemoryJournal, RunContext, RunEventKind,
    };
    use runifold_model::{
        ContentPart, FinishReason, ModelError, ModelErrorKind, ModelRef, ModelStreamEvent, Role,
    };
    use runifold_testkit::ScriptedModel;

    use crate::{
        Agent, AgentDescriptor, AgentGateway, AgentOutcome, AgentRoute, DelegationRequest,
        GatewayDecision, GatewayError, GatewayErrorKind, GatewayFuture, GatewayMiddleware,
        GatewayNext, GatewayPolicy, PolicyMiddleware,
    };

    struct RecordingMiddleware {
        name: &'static str,
        events: Arc<Mutex<Vec<String>>>,
    }

    impl GatewayMiddleware for RecordingMiddleware {
        fn handle<'a>(
            &'a self,
            request: DelegationRequest,
            next: GatewayNext<'a>,
        ) -> GatewayFuture<'a, Result<AgentOutcome, GatewayError>> {
            Box::pin(async move {
                self.record("before");
                let result = next.run(request).await;
                self.record("after");
                result
            })
        }
    }

    impl RecordingMiddleware {
        fn record(&self, phase: &str) {
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(format!("{}:{phase}", self.name));
        }
    }

    struct DenyPolicy;

    impl GatewayPolicy for DenyPolicy {
        fn evaluate<'a>(
            &'a self,
            _request: &'a DelegationRequest,
        ) -> GatewayFuture<'a, Result<GatewayDecision, GatewayError>> {
            Box::pin(async {
                Ok(GatewayDecision::Deny {
                    reason: "approval required".into(),
                })
            })
        }
    }

    struct PrefixMiddleware;

    impl GatewayMiddleware for PrefixMiddleware {
        fn handle<'a>(
            &'a self,
            request: DelegationRequest,
            next: GatewayNext<'a>,
        ) -> GatewayFuture<'a, Result<AgentOutcome, GatewayError>> {
            let input = format!("policy prefix: {}", request.input());
            next.run(request.with_input(input))
        }
    }

    struct RetryChildFailureOnce;

    impl GatewayMiddleware for RetryChildFailureOnce {
        fn handle<'a>(
            &'a self,
            request: DelegationRequest,
            next: GatewayNext<'a>,
        ) -> GatewayFuture<'a, Result<AgentOutcome, GatewayError>> {
            Box::pin(async move {
                let first = next.run(request.clone()).await;
                if matches!(
                    first,
                    Err(ref error) if error.kind == GatewayErrorKind::ChildFailed
                ) {
                    next.run(request).await
                } else {
                    first
                }
            })
        }
    }

    #[test]
    fn middleware_wraps_the_terminal_boundary_in_registration_order() {
        let (mut gateway, run, model) = gateway_and_run(true);
        let events = Arc::new(Mutex::new(Vec::new()));
        gateway.push_middleware(Arc::new(RecordingMiddleware {
            name: "outer",
            events: events.clone(),
        }));
        gateway.push_middleware(Arc::new(RecordingMiddleware {
            name: "inner",
            events: events.clone(),
        }));

        futures_executor::block_on(gateway.delegate("ask_child", "work", &run)).unwrap();

        assert_eq!(
            *events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec!["outer:before", "inner:before", "inner:after", "outer:after"]
        );
        assert_eq!(model.recorded_requests().len(), 1);
    }

    #[test]
    fn policy_denial_short_circuits_before_budget_and_child_execution() {
        let (gateway, run, model) = gateway_and_run(false);
        let journal = InMemoryJournal::new();
        let run = run.with_journal(Arc::new(journal.clone()));
        let gateway = gateway.layer(Arc::new(PolicyMiddleware::new(Arc::new(DenyPolicy))));

        let error =
            futures_executor::block_on(gateway.delegate("ask_child", "work", &run)).unwrap_err();

        assert_eq!(error.kind, GatewayErrorKind::PolicyDenied);
        assert_eq!(run.budget().usage().delegations, 0);
        assert!(model.recorded_requests().is_empty());
        assert!(journal.events().iter().any(|event| {
            matches!(
                &event.kind,
                RunEventKind::Domain(event) if event.name == "policy.denied"
            )
        }));
    }

    #[test]
    fn middleware_can_transform_input_without_replacing_authority() {
        let (gateway, run, model) = gateway_and_run(true);
        let gateway = gateway.layer(Arc::new(PrefixMiddleware));

        futures_executor::block_on(gateway.delegate("ask_child", "work", &run)).unwrap();

        let request = &model.recorded_requests()[0];
        assert!(matches!(
            request.messages.first(),
            Some(message)
                if message.role == Role::User
                    && matches!(
                        message.content.first(),
                        Some(ContentPart::Text { text }) if text == "policy prefix: work"
                    )
        ));
    }

    #[test]
    fn middleware_cannot_bypass_terminal_capability_checks() {
        let (gateway, _, model) = gateway_and_run(false);
        let run = RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new());
        let gateway = gateway.layer(Arc::new(PrefixMiddleware));

        let error =
            futures_executor::block_on(gateway.delegate("ask_child", "work", &run)).unwrap_err();

        assert_eq!(error.kind, GatewayErrorKind::CapabilityDenied);
        assert_eq!(run.budget().usage().delegations, 0);
        assert!(model.recorded_requests().is_empty());
    }

    #[test]
    fn explicit_retry_rechecks_and_accounts_for_each_terminal_attempt() {
        let (gateway, run, model) = gateway_and_run(false);
        model.enqueue_error(ModelError::local(
            ModelErrorKind::Provider,
            "transient child failure",
        ));
        model.enqueue(response_events());
        let gateway = gateway.layer(Arc::new(RetryChildFailureOnce));

        let outcome =
            futures_executor::block_on(gateway.delegate("ask_child", "work", &run)).unwrap();

        assert_eq!(outcome.response.content, vec![ContentPart::text("done")]);
        assert_eq!(run.budget().usage().delegations, 2);
        assert_eq!(model.recorded_requests().len(), 2);
    }

    fn gateway_and_run(enqueue_response: bool) -> (AgentGateway, RunContext, ScriptedModel) {
        let model = ScriptedModel::new();
        if enqueue_response {
            model.enqueue(response_events());
        }
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

    fn response_events() -> Vec<ModelStreamEvent> {
        vec![
            ModelStreamEvent::ResponseStarted {
                id: Some("child".into()),
                model: ModelRef::new("test", "child"),
            },
            ModelStreamEvent::ContentPartCompleted {
                index: 0,
                part: ContentPart::text("done"),
            },
            ModelStreamEvent::ResponseCompleted {
                finish_reason: FinishReason::Stop,
                provider_metadata: BTreeMap::new(),
            },
        ]
    }
}
