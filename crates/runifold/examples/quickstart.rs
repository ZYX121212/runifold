//! Compile-checked shortest path from provider configuration to an Agent
//! prompt.

use runifold::ProviderModelExt;
use runifold_providers::openai::OpenAiClient;

async fn prompt() -> anyhow::Result<String> {
    let runtime = OpenAiClient::from_api_key(std::env::var("OPENAI_API_KEY")?)?.runtime("gpt-5")?;
    let agent = runtime
        .agent("assistant")
        .system("Answer precisely and expose uncertainty.");

    Ok(agent
        .prompt_text("Why is durable execution useful?")
        .await?)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("{}", prompt().await?);
    Ok(())
}
