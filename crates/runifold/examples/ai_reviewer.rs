//! Rule-based terminal review before a candidate is accepted.
use runifold::{
    CapabilitySet, ProviderModelExt, TerminalReviewPolicy, TerminalReviewVerdict,
    TerminalRuleReviewer,
};
use runifold_providers::openai::OpenAiClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let runtime = OpenAiClient::from_api_key(std::env::var("OPENAI_API_KEY")?)?.runtime("gpt-5")?;
    let reviewer = TerminalRuleReviewer::new("answer-length", "v1", |request| {
        if request.candidate.text().chars().count() <= 500 {
            Ok(TerminalReviewVerdict::approve())
        } else {
            TerminalReviewVerdict::reject("Answer exceeds the 500-character application limit")
        }
    })?;
    let agent = runtime
        .agent("reviewed")
        .system("Answer in fewer than 500 characters.")
        .terminal_reviewer(reviewer, TerminalReviewPolicy::new(0), CapabilitySet::new())
        .build()?;
    println!("{}", agent.prompt_text("Explain idempotency.").await?);
    Ok(())
}
