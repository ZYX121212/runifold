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
    assert_eq!(
        RunErrorKind::CapabilityDenied.code(),
        "runifold.capability_denied"
    );
    let agent = runifold::AgentError::Tool(tool);
    assert_eq!(agent.diagnostic_code(), "RF-TOOL-003");
}
