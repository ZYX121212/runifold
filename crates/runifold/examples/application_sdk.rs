//! Offline application SDK: typed Tool, budget, stream and cancellation.
use anyhow::Context;
use futures_util::StreamExt;
use runifold::{
    Agent, AgentStreamEvent, Budget, BudgetTracker, JsonSchema, RunContext, ToolContext, ToolError,
    model::{ContentPart, FinishReason, ModelRef, ModelStreamEvent, ToolCall},
};
use runifold_testkit::ScriptedModel;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

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
    let model = ScriptedModel::new();
    model.enqueue(response(
        ContentPart::ToolCall(ToolCall {
            id: "greet-1".into(),
            name: "greet".into(),
            arguments: serde_json::json!({"name":"Ada"}),
            raw_arguments: None,
            metadata: BTreeMap::new(),
        }),
        FinishReason::ToolCalls,
    ));
    model.enqueue(response(
        ContentPart::text("Hello, Ada!"),
        FinishReason::Stop,
    ));
    // Explicit model injection is useful for offline tests. Hosted applications
    // use ProviderModelExt -> one long-lived ProviderRuntime -> .agent(...).
    let agent = Agent::builder(
        "greeter",
        Arc::new(model.clone()),
        ModelRef::new("test", "script"),
    )
    .tool(greet_tool())
    .build()?;
    let run = RunContext::root(
        BudgetTracker::new(Budget {
            turns: Some(2),
            tool_calls: Some(1),
            ..Budget::default()
        }),
        agent.callable_capabilities(),
    );
    let mut stream = agent.stream("Greet Ada", &run);
    let mut completed = None;
    while let Some(event) = stream.next().await {
        if let AgentStreamEvent::Completed { outcome } = event? {
            completed = Some(outcome);
        }
    }
    let outcome = completed.context("stream ended without successful completion")?;
    assert_eq!(outcome.text(), "Hello, Ada!");
    assert_eq!(outcome.tool_calls, 1);
    assert_eq!(model.recorded_requests().len(), 2);
    println!("{}; usage: {:?}", outcome.text(), outcome.usage);
    // Cancelling before dispatch must not consume another scripted request.
    let stopped = RunContext::root(
        BudgetTracker::new(Budget::default()),
        agent.callable_capabilities(),
    );
    stopped.cancellation().cancel();
    assert!(agent.run("already cancelled", &stopped).await.is_err());
    assert_eq!(model.recorded_requests().len(), 2);
    Ok(())
}

fn response(part: ContentPart, finish_reason: FinishReason) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ResponseStarted {
            id: None,
            model: ModelRef::new("test", "script"),
        },
        ModelStreamEvent::ContentPartCompleted { index: 0, part },
        ModelStreamEvent::ResponseCompleted {
            finish_reason,
            provider_metadata: BTreeMap::new(),
        },
    ]
}
