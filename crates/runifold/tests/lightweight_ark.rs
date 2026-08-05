//! Compile and behavior contract for Ark without the full Agent runtime.

use runifold::{
    FeaturePolicy, Message, Model, ModelRef, ModelRequest, OutputFormat, SupportLevel,
    ark::{ArkWebSearchTool, client},
};

#[test]
fn ark_low_level_request_needs_only_core_model_and_providers() {
    let client = client("test-key").expect("a non-empty test key is valid configuration");
    let model = ModelRef::new("ark", "doubao-test");
    let capabilities = futures_executor::block_on(client.capabilities(&model))
        .expect("static Ark capabilities do not require transport access");
    let request = ModelRequest::new(model, Message::user("research"))
        .feature_policy(FeaturePolicy::Strict)
        .provider_tool(ArkWebSearchTool::new().limit(8).max_keyword(5).into())
        .output_format(OutputFormat::Json);

    assert_eq!(capabilities.tools.level, SupportLevel::Native);
    assert_eq!(capabilities.structured_output.level, SupportLevel::Native);
    assert_eq!(capabilities.reasoning.level, SupportLevel::Native);
    assert!(capabilities.validate_request(&request, true).is_ok());
}
