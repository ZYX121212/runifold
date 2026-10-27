//! Compile and behavior contracts for the curated AI API index and diagnostics.
#![cfg(feature = "runtime")]

use runifold::{
    Agent, AgentBuilder, AgentSession, AgentStreamEvent, EffectExecutor, ProviderRuntime,
    Retriever, RunContext, StructuredAgent, TerminalRuleReviewer, ToolContext, ToolError,
    ToolErrorKind, ToolRegistry,
    core::{RetrySafety, RunErrorKind},
    effect::{EffectExecutorError, EffectExecutorErrorKind},
    model::{ModelError, ModelErrorKind},
};

#[test]
fn preferred_api_symbols_are_public() {
    // Compile public facade imports independently of private implementation tests.
    let _ = std::any::type_name::<(
        Agent,
        AgentBuilder,
        AgentSession,
        AgentStreamEvent,
        StructuredAgent<String>,
        EffectExecutor,
        ProviderRuntime,
        RunContext,
        TerminalRuleReviewer,
        ToolContext,
        ToolRegistry,
    )>();
    let _ = std::any::type_name::<dyn Retriever>();
    let _: fn(&ProviderRuntime, String) -> AgentBuilder = ProviderRuntime::agent;
}

#[test]
fn diagnostic_codes_preserve_payload_privacy_and_retry_contracts() {
    let tool = ToolError::local(ToolErrorKind::CapabilityDenied, "private detail");
    assert_eq!(tool.diagnostic_code(), "RF-TOOL-003");
    assert_eq!(tool.retry_safety, RetrySafety::Unknown);
    let before = serde_json::to_value(&tool).unwrap();
    let code = tool.diagnostic_code();
    assert!(!code.contains("private"));
    assert_eq!(before, serde_json::to_value(&tool).unwrap());
    assert!(before.get("diagnostic_code").is_none());

    let effect = EffectExecutorError::new(EffectExecutorErrorKind::Ambiguous, "private receipt");
    assert_eq!(effect.diagnostic_code(), "RF-EFFECT-003");
    let model = ModelError::local(ModelErrorKind::UnsupportedFeature, "private request");
    assert_eq!(model.diagnostic_code(), "RF-PROVIDER-002");
    assert!(model.diagnostic_metadata().is_empty());
    assert_eq!(
        RunErrorKind::CapabilityDenied.code(),
        "runifold.capability_denied"
    );
    let agent = runifold::AgentError::Tool(tool);
    assert_eq!(agent.diagnostic_code(), "RF-TOOL-003");
}

#[test]
fn shared_fault_policy_can_be_disabled_through_the_public_profile() {
    let default = runifold::ProviderRuntimeProfile::conservative();
    let profile = default.clone().without_circuit_breaker();
    assert!(!profile.selected_circuit_breaker().is_enabled());
    assert!(default.selected_circuit_breaker().is_enabled());
    assert_eq!(
        profile.selected_retry_policy(),
        default.selected_retry_policy()
    );
    assert_eq!(
        profile.selected_response_mode(),
        default.selected_response_mode()
    );
    let (_, _, retry, circuit, _) = profile.into_parts();
    assert!(!circuit.is_enabled());
    assert_eq!(retry, *default.selected_retry_policy());
}

#[test]
fn runtime_policy_snapshot_records_effective_exceptions_without_private_options() {
    let profile = runifold::ProviderRuntimeProfile::conservative()
        .without_circuit_breaker()
        .retry_policy(
            runifold::ModelRetryPolicy::default().allow_unknown(ModelErrorKind::Transport),
        )
        .provider_option(
            "private-namespace",
            serde_json::json!({"token":"private-secret"}),
        );
    let snapshot: runifold::RuntimePolicySnapshot = profile.diagnostic_snapshot();
    let encoded = serde_json::to_value(&snapshot).unwrap();
    assert_eq!(encoded["schema_version"], 1);
    assert_eq!(encoded["circuit_breaker"]["enabled"], false);
    assert_eq!(encoded["retry_policy"]["max_attempts"], 3);
    assert_eq!(
        encoded["retry_policy"]["unknown_safety_kinds"],
        serde_json::json!(["Transport"])
    );
    assert_eq!(encoded["has_provider_options"], true);
    assert!(!encoded.to_string().contains("private"));
    assert_eq!(profile.selected_retry_policy(), &snapshot.retry_policy);
}

#[test]
fn model_scheduling_symbols_are_public() {
    let _ = std::any::type_name::<(
        runifold::ModelScheduler,
        runifold::ModelSchedulerConfig,
        runifold::ModelCapacityPoolId,
        runifold::ModelCapacityLimits,
        runifold::ModelRateLimit,
        runifold::ModelCapacitySnapshot,
        runifold::ModelSchedulerSnapshot,
        runifold::ModelSchedulerConfigError,
        runifold::ModelSchedulingError,
        runifold::ModelTokenSettlement,
        runifold::ScheduledModel<std::sync::Arc<dyn runifold::Model>>,
    )>();
    let _ = std::any::type_name::<dyn runifold::ModelTokenEstimator>();
    let error: ModelError = runifold::ModelSchedulingError::QueueFull.into();
    assert_eq!(error.diagnostic_code(), "RF-PROVIDER-010");
    assert_eq!(error.retry_safety, RetrySafety::Safe);
}

#[test]
fn context_management_symbols_and_budget_contract_are_public() {
    let _ = runifold::ContextGroupKind::IncompleteHistory;
    let _ = std::any::type_name::<(
        runifold::ContextManager,
        runifold::ContextPlan,
        runifold::ContextGroup,
        runifold::RecentContext,
        runifold::SerializedBytesCounter,
    )>();
    let _ = std::any::type_name::<dyn runifold::ContextTokenCounter>();
    let _ = std::any::type_name::<dyn runifold::ContextSelectionStrategy>();
    assert_eq!(
        runifold::ContextBudget::new(100, 20, 5)
            .unwrap()
            .input_limit(),
        75
    );
    assert!(runifold::ContextBudget::new(10, 10, 0).is_err());
    let error = runifold::AgentError::Context(runifold::ContextError::Overflow {
        required: 100,
        available: 75,
    });
    assert_eq!(error.run_error_kind(), RunErrorKind::BudgetExceeded);
    assert_eq!(error.retry_safety(), RetrySafety::Unknown);
}
