//! Public-surface integration tests for the typed Tool attribute.

use std::sync::Arc;

use runifold::{
    Budget, BudgetTracker, CapabilitySet, ContentPart, IntoToolError, JsonSchema, MediaSource,
    RunContext, State, Tool, ToolContext, ToolError, ToolErrorKind, ToolOutput, ToolRegistry,
    core::{EffectClass, RiskLevel},
};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Deserialize, JsonSchema)]
struct AddInput {
    left: i64,
    right: i64,
}

#[derive(JsonSchema, Serialize)]
struct AddOutput {
    sum: i64,
}

#[derive(Deserialize, JsonSchema)]
struct ScaleInput {
    value: i64,
}

#[derive(JsonSchema, Serialize)]
struct ScaleOutput {
    value: i64,
}

#[derive(Deserialize, JsonSchema)]
struct ChartInput {
    symbol: String,
}

struct Multiplier {
    factor: i64,
}

struct CalculationError {
    _internal_detail: &'static str,
}

impl IntoToolError for CalculationError {
    fn into_tool_error(self) -> ToolError {
        ToolError::local(
            ToolErrorKind::Execution,
            "the value cannot be scaled safely",
        )
    }
}

#[runifold::tool(
    description = "Add two signed integers",
    version = "2",
    effect = "pure",
    risk = "low"
)]
async fn add(input: AddInput, _context: ToolContext) -> Result<AddOutput, ToolError> {
    std::future::ready(()).await;
    Ok(AddOutput {
        sum: input.left + input.right,
    })
}

#[runifold::tool(
    description = "Scale a non-negative integer",
    effect = "read_only",
    risk = "medium"
)]
async fn scale(
    state: State<Multiplier>,
    input: ScaleInput,
    _context: ToolContext,
) -> Result<ScaleOutput, CalculationError> {
    std::future::ready(()).await;
    if input.value < 0 {
        return Err(CalculationError {
            _internal_detail: "database policy row 42 rejected a negative value",
        });
    }
    Ok(ScaleOutput {
        value: input.value * state.factor,
    })
}

#[runifold::tool(
    description = "Return an existing K-line chart as model-visible media",
    output = "rich",
    effect = "read_only",
    risk = "low"
)]
async fn kline_chart(input: ChartInput, _context: ToolContext) -> Result<ToolOutput, ToolError> {
    std::future::ready(()).await;
    Ok(ToolOutput::rich(vec![
        ContentPart::text(format!("K-line chart for {}", input.symbol)),
        ContentPart::Image {
            source: MediaSource::Url {
                url: "https://example.com/kline.png".into(),
                media_type: Some("image/png".into()),
            },
        },
    ]))
}

#[test]
fn attribute_generates_a_typed_canonical_tool() {
    let tool = Arc::new(add_tool());
    assert_eq!(tool.descriptor().name, "add");
    assert_eq!(tool.descriptor().version, "2");
    assert_eq!(tool.descriptor().effect, EffectClass::Pure);
    assert_eq!(tool.descriptor().risk, RiskLevel::Low);
    assert_eq!(
        tool.descriptor().input_schema["required"],
        json!(["left", "right"])
    );

    let mut capabilities = CapabilitySet::new();
    capabilities.grant(tool.descriptor().capability());
    let run = RunContext::root(BudgetTracker::new(Budget::default()), capabilities);
    let mut tools = ToolRegistry::new();
    tools.register(tool).unwrap();

    let output =
        futures_executor::block_on(tools.invoke("add", json!({"left": 20, "right": 22}), &run))
            .unwrap();

    assert_eq!(output.structured_content, Some(json!({"sum": 42})));
}

#[test]
fn attribute_injects_host_state_and_maps_application_errors_explicitly() {
    let tool = Arc::new(scale_tool(Arc::new(Multiplier { factor: 3 })));
    assert_eq!(tool.descriptor().effect, EffectClass::ReadOnly);
    assert_eq!(tool.descriptor().risk, RiskLevel::Medium);
    assert!(
        tool.descriptor().input_schema["properties"]
            .get("factor")
            .is_none(),
        "host state leaked into the model input schema"
    );

    let mut capabilities = CapabilitySet::new();
    capabilities.grant(tool.descriptor().capability());
    let run = RunContext::root(BudgetTracker::new(Budget::default()), capabilities);
    let mut tools = ToolRegistry::new();
    tools.register(tool).unwrap();

    let output =
        futures_executor::block_on(tools.invoke("scale", json!({"value": 14}), &run)).unwrap();
    assert_eq!(output.structured_content, Some(json!({"value": 42})));

    let error =
        futures_executor::block_on(tools.invoke("scale", json!({"value": -1}), &run)).unwrap_err();
    assert_eq!(error.kind, ToolErrorKind::Execution);
    assert_eq!(error.message, "the value cannot be scaled safely");
    assert!(!error.message.contains("database policy row"));
}

#[test]
fn attribute_preserves_rich_function_output() {
    let tool = Arc::new(kline_chart_tool());
    assert_eq!(tool.descriptor().name, "kline_chart");
    assert_eq!(tool.descriptor().effect, EffectClass::ReadOnly);

    let mut capabilities = CapabilitySet::new();
    capabilities.grant(tool.descriptor().capability());
    let run = RunContext::root(BudgetTracker::new(Budget::default()), capabilities);
    let mut tools = ToolRegistry::new();
    tools.register(tool).unwrap();

    let output =
        futures_executor::block_on(tools.invoke("kline_chart", json!({"symbol": "AAPL"}), &run))
            .unwrap();

    assert!(matches!(
        &output.content[1],
        ContentPart::Image {
            source: MediaSource::Url { url, media_type }
        } if url.ends_with("kline.png") && media_type.as_deref() == Some("image/png")
    ));
}
