//! Typed Tool registration through the recommended application entry point.
use runifold::{JsonSchema, ProviderModelExt, ToolContext, ToolError};
use runifold_providers::openai::OpenAiClient;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JsonSchema)]
struct GreetingInput {
    name: String,
}

#[derive(Serialize, JsonSchema)]
struct Greeting {
    text: String,
}

#[runifold::tool(description = "Format a greeting", effect = "pure", risk = "low")]
async fn greet(input: GreetingInput, _context: ToolContext) -> Result<Greeting, ToolError> {
    std::future::ready(()).await;
    Ok(Greeting {
        text: format!("Hello, {}!", input.name),
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Store this runtime once at application startup; clone it into handlers.
    let runtime = OpenAiClient::from_api_key(std::env::var("OPENAI_API_KEY")?)?.runtime("gpt-5")?;
    let agent = runtime.agent("greeter").tool(greet_tool()).build()?;
    let run = runifold::RunContext::root(
        runifold::BudgetTracker::new(runifold::Budget {
            turns: Some(3),
            tool_calls: Some(1),
            ..runifold::Budget::default()
        }),
        agent.callable_capabilities(),
    );
    println!(
        "{}",
        agent.run("Use greet to greet Ada.", &run).await?.text()
    );
    Ok(())
}
