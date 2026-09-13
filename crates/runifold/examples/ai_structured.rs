//! Provider schema and local decode bound to the same Rust type.
use runifold::{Budget, BudgetTracker, CapabilitySet, JsonSchema, ProviderModelExt, RunContext};
use runifold_providers::openai::OpenAiClient;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema)]
struct Answer {
    summary: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let runtime = OpenAiClient::from_api_key(std::env::var("OPENAI_API_KEY")?)?.runtime("gpt-5")?;
    let agent = runtime
        .agent("typed")
        .build_structured::<Answer>("answer")?;
    let run = RunContext::root(
        BudgetTracker::new(Budget {
            turns: Some(2),
            ..Budget::default()
        }),
        CapabilitySet::new(),
    );
    let result = agent
        .run("Summarize why idempotency matters.", &run)
        .await?;
    println!("{}", result.output.summary);
    Ok(())
}
