//! Incremental consumption of the canonical Agent stream.
use anyhow::Context;
use futures_util::StreamExt;
use runifold::{
    AgentStreamEvent, Budget, BudgetTracker, CapabilitySet, ProviderModelExt, RunContext,
};
use runifold_providers::openai::OpenAiClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let runtime = OpenAiClient::from_api_key(std::env::var("OPENAI_API_KEY")?)?.runtime("gpt-5")?;
    let agent = runtime.agent("streamer").build()?;
    let run = RunContext::root(
        BudgetTracker::new(Budget {
            turns: Some(2),
            ..Budget::default()
        }),
        CapabilitySet::new(),
    );
    let mut stream = agent.stream("Explain structured cancellation.", &run);
    let mut outcome = None;
    while let Some(event) = stream.next().await {
        match event? {
            AgentStreamEvent::Model {
                event: runifold::model::ModelStreamEvent::TextDelta { text, .. },
                ..
            } => print!("{text}"),
            AgentStreamEvent::Completed { outcome: completed } => outcome = Some(completed),
            _ => {}
        }
    }
    let completed = outcome.context("Agent stream ended without successful completion")?;
    println!("\nCompleted {} turns", completed.turns);
    Ok(())
}
