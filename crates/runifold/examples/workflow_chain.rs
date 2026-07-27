//! Assemble a durable, capability-explicit chain of Agents.

#[cfg(feature = "openai")]
fn main() -> anyhow::Result<()> {
    use std::sync::Arc;

    use anyhow::Context;
    use runifold::openai::{OpenAiAgentExt, OpenAiClient, OpenAiConfig};
    use runifold::{CapabilitySet, Workflow};

    let api_key = std::env::var("OPENAI_API_KEY")
        .context("OPENAI_API_KEY is required to configure the workflow")?;
    let client = OpenAiClient::new(
        OpenAiConfig::new(api_key).context("failed to construct the OpenAI provider config")?,
    );
    let planner = Arc::new(
        client
            .clone()
            .agent("planner", "gpt-5")
            .system("Produce a concise implementation plan.")
            .build()
            .context("failed to assemble the planner Agent")?,
    );
    let writer = Arc::new(
        client
            .agent("writer", "gpt-5")
            .system("Turn the supplied plan into a final answer.")
            .build()
            .context("failed to assemble the writer Agent")?,
    );
    let workflow = Workflow::builder("plan-and-write")
        .version(1)
        .agent("plan", planner, CapabilitySet::new())
        .agent("write", writer, CapabilitySet::new())
        .build()
        .context("failed to assemble the workflow")?;

    println!(
        "configured Workflow `{}` with {} steps",
        workflow.name(),
        workflow.step_ids().len()
    );
    Ok(())
}

#[cfg(not(feature = "openai"))]
fn main() {
    eprintln!("run this example with `--features openai`");
}
