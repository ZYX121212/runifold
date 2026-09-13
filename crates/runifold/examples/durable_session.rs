//! Run an offline durable session and replay a request without another model call.
//! ```sh
//! cargo run -p runifold --example durable_session --features sqlite-bundled -- /tmp/session.sqlite
//! ```

use std::{collections::BTreeMap, sync::Arc};

use anyhow::Context;
use runifold::{
    Agent, AgentSession, Budget, BudgetTracker, CapabilitySet, ConversationAppend,
    ConversationContextPolicy, ConversationId, ConversationStore, ConversationSummaryPassLimit,
    ConversationVersion, ConversationWindow, MemoryNamespace, RunContext,
    core::CheckpointId,
    model::{ContentPart, FinishReason, Message, ModelRef, ModelStreamEvent},
    sqlite::SqliteStore,
};
use runifold_testkit::ScriptedModel;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = std::env::args().nth(1).context("provide a SQLite path")?;
    let model = ScriptedModel::new();
    model.enqueue(response("Committed and safe to replay."));
    let summary_model = ScriptedModel::new();
    summary_model.enqueue(response("User prefers Rust and needs crash recovery."));
    let store = Arc::new(SqliteStore::open(path).context("open durable store")?);
    let conversation_id = ConversationId::new();
    let namespace = MemoryNamespace::parse("example.user")?;
    store.create(conversation_id, namespace.clone()).await?;
    store
        .append(
            namespace.clone(),
            ConversationAppend {
                conversation_id,
                expected_version: ConversationVersion::new(0),
                messages: vec![
                    Message::user("I use Rust."),
                    Message::new(
                        runifold::model::Role::Assistant,
                        vec![ContentPart::text("Understood.")],
                    )?,
                    Message::user("Tasks must survive a crash."),
                    Message::new(
                        runifold::model::Role::Assistant,
                        vec![ContentPart::text("We will persist them.")],
                    )?,
                ],
            },
        )
        .await?;
    let session = AgentSession::new(
        Agent::new(
            "assistant",
            Arc::new(model.clone()),
            ModelRef::new("test", "script"),
        ),
        store,
        conversation_id,
        namespace,
        ConversationContextPolicy::new(ConversationWindow::new(2)?),
    )
    .with_summary_agent(
        Agent::new(
            "summarizer",
            Arc::new(summary_model.clone()),
            ModelRef::new("test", "script"),
        ),
        ConversationSummaryPassLimit::new(4)?,
    );
    // Persist these IDs in the application and reuse the request ID on retries.
    let request_id = CheckpointId::new();
    let run = RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new());
    let first = session.run(request_id, "hello", &run).await?;
    let replay = session.run(request_id, "hello", &run).await?;
    assert_eq!(first.conversation_version, replay.conversation_version);
    assert_eq!(model.recorded_requests().len(), 1);
    assert_eq!(summary_model.recorded_requests().len(), 1);
    println!(
        "{} (conversation version {})",
        replay.outcome.text(),
        replay.conversation_version.get()
    );
    Ok(())
}

fn response(text: &str) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ResponseStarted {
            id: Some("offline".into()),
            model: ModelRef::new("test", "script"),
        },
        ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::text(text),
        },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::Stop,
            provider_metadata: BTreeMap::new(),
        },
    ]
}
