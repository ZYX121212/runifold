//! Application-boundary error context without erasing Runifold's typed errors.

fn main() -> anyhow::Result<()> {
    use anyhow::Context;
    use runifold::ProviderModelExt;
    use runifold_providers::openai::{OpenAiClient, OpenAiConfig};

    let api_key = std::env::var("OPENAI_API_KEY")
        .context("OPENAI_API_KEY is required to configure the example Agent")?;
    let config =
        OpenAiConfig::new(api_key).context("failed to construct the OpenAI provider config")?;
    let agent = OpenAiClient::new(config)
        .agent("example", "gpt-5")
        .system("Answer precisely.")
        .build()
        .context("failed to assemble the example Agent")?;

    println!("configured Agent `{}`", agent.name());
    Ok(())
}
