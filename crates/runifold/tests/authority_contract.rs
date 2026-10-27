//! Public contracts for preserving capability definitions across delegation.
#![cfg(all(feature = "runtime", not(target_arch = "wasm32")))]

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use runifold::{
    Agent, AgentDescriptor, AgentGateway, AgentRoute, Budget, BudgetTracker, CapabilitySet,
    ChildRunError, GatewayErrorKind, ModelRef, RunContext, Usage, Workflow, WorkflowError,
    WorkflowStep, WorkflowStepError, WorkflowStepFuture,
    core::{CapabilityDescriptor, CapabilityId, CapabilityKind, EffectClass, RiskLevel},
};
use runifold_testkit::ScriptedModel;
use serde_json::{Value, json};

fn write_capability() -> CapabilityDescriptor {
    CapabilityDescriptor {
        id: CapabilityId::new(),
        name: "publish".into(),
        version: "1".into(),
        kind: CapabilityKind::Resource,
        input_schema: json!({}),
        output_schema: json!({}),
        effect: EffectClass::NonIdempotentWrite,
        risk: RiskLevel::High,
        metadata: BTreeMap::new(),
    }
}

fn grants(descriptor: CapabilityDescriptor) -> CapabilitySet {
    let mut grants = CapabilitySet::new();
    grants.grant(descriptor);
    grants
}

#[test]
fn child_and_reserved_child_cannot_relabel_a_parent_write() {
    let descriptor = write_capability();
    let parent = RunContext::root(
        BudgetTracker::new(Budget::default()),
        grants(descriptor.clone()),
    );
    let reservation = parent
        .budget()
        .try_reserve(Usage {
            turns: 1,
            ..Usage::default()
        })
        .expect("reservation");
    let mut redefined = descriptor.clone();
    redefined.effect = EffectClass::Pure;
    redefined.risk = RiskLevel::Low;

    assert_eq!(
        parent
            .child(grants(redefined.clone()))
            .expect_err("changed contract denied")
            .capability_id,
        descriptor.id
    );
    assert!(
        matches!(parent.child_reserved(grants(redefined), &reservation), Err(ChildRunError::Authority(error)) if error.capability_id == descriptor.id)
    );
    assert_eq!(reservation.remaining().turns, 1);
    assert_eq!(parent.budget().usage(), Usage::default());

    let child = parent
        .child_reserved(grants(descriptor.clone()), &reservation)
        .expect("unchanged explicitly selected contract is valid");
    assert_eq!(child.capabilities().get(descriptor.id), Some(&descriptor));
    assert_eq!(child.parent_run_id(), Some(parent.run_id()));
    parent.cancellation().cancel();
    assert!(child.cancellation().is_cancelled());
}

#[test]
fn gateway_rejects_relabelled_child_grant_before_budget_or_model_work() {
    let model = ScriptedModel::new();
    let child = Arc::new(Agent::new(
        "child",
        Arc::new(model.clone()),
        ModelRef::new("test", "child"),
    ));
    let route = AgentDescriptor::new("delegate", "Delegate bounded work");
    let descriptor = write_capability();
    let mut parent_grants = grants(descriptor.clone());
    parent_grants.grant(route.capability());
    let parent = RunContext::root(BudgetTracker::new(Budget::default()), parent_grants);
    let mut redefined = descriptor;
    redefined.effect = EffectClass::ReadOnly;
    let mut gateway = AgentGateway::new();
    gateway
        .register(AgentRoute::new(route, child).with_capabilities(grants(redefined)))
        .expect("route registration");

    let error = futures_executor::block_on(gateway.delegate("delegate", "publish", &parent))
        .expect_err("a route cannot redefine the parent grant");
    assert_eq!(error.kind, GatewayErrorKind::AuthorityEscalation);
    assert_eq!(parent.budget().usage(), Usage::default());
    assert!(model.recorded_requests().is_empty());
}

struct CountingStep(Arc<AtomicUsize>);

impl WorkflowStep for CountingStep {
    fn execute<'a>(&'a self, input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok::<_, WorkflowStepError>(input) })
    }
}

#[test]
fn workflow_rejects_relabelled_step_grant_before_execution() {
    let descriptor = write_capability();
    let parent = RunContext::root(
        BudgetTracker::new(Budget::default()),
        grants(descriptor.clone()),
    );
    let mut redefined = descriptor;
    redefined.effect = EffectClass::Pure;
    let calls = Arc::new(AtomicUsize::new(0));
    let workflow = Workflow::builder("authority")
        .step("publish", CountingStep(calls.clone()), grants(redefined))
        .build()
        .expect("workflow");

    let error = futures_executor::block_on(workflow.run(json!({}), &parent))
        .expect_err("a workflow step cannot redefine its grant");
    assert!(matches!(error, WorkflowError::AuthorityEscalation { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(parent.budget().usage(), Usage::default());
}
