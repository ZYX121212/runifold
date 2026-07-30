//! Compile-checked shortest path from provider configuration to an Agent
//! prompt.

#[cfg(feature = "openai")]
use runifold::openai::{OpenAiAgentExt, OpenAiClient};

#[cfg(feature = "openai")]
async fn prompt() -> anyhow::Result<String> {
    let agent = OpenAiClient::from_api_key(std::env::var("OPENAI_API_KEY")?)?
        .agent("assistant", "gpt-5")
        .system("Answer precisely and expose uncertainty.");

    Ok(agent
        .prompt_text("Why is durable execution useful?")
        .await?)
}

fn main() {
    #[cfg(feature = "openai")]
    let _ = prompt;
}
