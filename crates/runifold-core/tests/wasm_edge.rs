#![allow(missing_docs)]
#![cfg(target_arch = "wasm32")]

//! Executable edge-runtime smoke tests for the provider-neutral kernel.

use runifold_core::{Budget, BudgetResource, BudgetTracker, CapabilitySet, RunContext, Usage};
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen_test]
fn runtime_identity_authority_cancellation_and_budget_work_on_edge() {
    let budget = BudgetTracker::new(Budget {
        tokens: Some(8),
        ..Budget::default()
    });
    let root = RunContext::root(budget.clone(), CapabilitySet::new());
    let child = root
        .child(CapabilitySet::new())
        .expect("empty child authority is always attenuated");

    assert_ne!(root.run_id(), child.run_id());
    assert_eq!(child.parent_run_id(), Some(root.run_id()));
    assert_eq!(child.root_run_id(), root.run_id());

    root.cancellation().cancel();
    assert!(child.cancellation().is_cancelled());

    budget
        .try_consume(Usage {
            tokens: 5,
            ..Usage::default()
        })
        .expect("usage below the configured limit must be accepted");
    let exceeded = budget
        .try_consume(Usage {
            tokens: 4,
            ..Usage::default()
        })
        .expect_err("cumulative usage above the configured limit must fail");
    assert_eq!(exceeded.resource, BudgetResource::Tokens);
    assert_eq!(budget.usage().tokens, 5);
}
