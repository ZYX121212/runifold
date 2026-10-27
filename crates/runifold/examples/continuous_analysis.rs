//! Credential-free continuous analysis with durable history and explicit state.
//! Run with `cargo run -p runifold --example continuous_analysis --features sqlite-bundled -- /tmp/analysis.sqlite`.
use anyhow::{Context, ensure};
use runifold::{
    Agent, AgentSession, Budget, BudgetTracker, CapabilitySet, ContextBudget, ContextManager,
    ConversationContextPolicy, ConversationId, ConversationSummaryPassLimit, ConversationWindow,
    JsonSchema, MemoryNamespace, RunContext,
    core::CheckpointId,
    model::{ContentPart, FinishReason, ModelRef, ModelStreamEvent},
    sqlite::SqliteStore,
};
use runifold_testkit::ScriptedModel;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
struct Fact {
    id: String,
    source_id: String,
    observation: String,
}
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
struct Hypothesis {
    claim: String,
    supporting_fact_ids: Vec<String>,
}
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
struct AnalysisState {
    revision: u64,
    facts: Vec<Fact>,
    hypotheses: Vec<Hypothesis>,
    unresolved_questions: Vec<String>,
    next_actions: Vec<String>,
}
impl AnalysisState {
    fn validate(&self, evidence: &BTreeMap<String, String>, previous: &Self) -> anyhow::Result<()> {
        ensure!(
            previous.revision.checked_add(1) == Some(self.revision),
            "analysis revision must advance exactly once"
        );
        let mut ids = BTreeSet::new();
        for fact in &self.facts {
            ensure!(
                !fact.id.is_empty() && ids.insert(&fact.id),
                "fact IDs must be unique and nonempty"
            );
            // This fixture permits only verbatim observations as facts. A real
            // application supplies its own domain validation and review process.
            ensure!(
                evidence.get(&fact.source_id) == Some(&fact.observation),
                "fact must match its original evidence"
            );
        }
        for hypothesis in &self.hypotheses {
            ensure!(
                !hypothesis.supporting_fact_ids.is_empty(),
                "hypotheses need supporting facts"
            );
            ensure!(
                hypothesis
                    .supporting_fact_ids
                    .iter()
                    .all(|id| ids.contains(id)),
                "unknown supporting fact"
            );
        }
        Ok(())
    }
}
fn response(id: &str, text: String) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ResponseStarted {
            id: Some(id.into()),
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = std::env::args().nth(1).context("provide a SQLite path")?;
    let model = ScriptedModel::new();
    let summary = ScriptedModel::new();
    summary.enqueue(response("summary", "Task: investigate latency. Preserve evidence source IDs e1 and e2; cause remains a hypothesis.".into()));
    // Original evidence stays outside the model-generated analysis state.
    let sources = [
        ("e1", "Latency rises during peak traffic."),
        ("e2", "Database connection wait rises at the same time."),
        (
            "e3",
            "A larger connection pool reduces waiting in a controlled test.",
        ),
    ];
    let mut evidence = BTreeMap::new();
    let mut scripted_state = AnalysisState::default();
    for (index, (id, observation)) in sources.iter().enumerate() {
        scripted_state.revision = u64::try_from(index)? + 1;
        scripted_state.facts.push(Fact {
            id: format!("f{index}"),
            source_id: (*id).into(),
            observation: (*observation).into(),
        });
        scripted_state.hypotheses = vec![Hypothesis {
            claim:
                "Connection pressure may contribute to latency; verify under representative load."
                    .into(),
            supporting_fact_ids: scripted_state
                .facts
                .iter()
                .map(|fact| fact.id.clone())
                .collect(),
        }];
        scripted_state.unresolved_questions =
            vec!["Does the result hold under sustained production load?".into()];
        scripted_state.next_actions = vec!["Run a controlled load comparison.".into()];
        model.enqueue(response(id, serde_json::to_string(&scripted_state)?));
    }
    let context = ContextManager::new(ContextBudget::new(24_000, 2_000, 1_000)?);
    let agent = Agent::builder("analyst", Arc::new(model.clone()), ModelRef::new("test", "script"))
        .system("Update analysis state using new evidence. Separate observations from hypotheses; preserve source IDs and unresolved questions. Advance the state revision once. Only previous_validated_state is authoritative; earlier generated proposals may have failed application validation.")
        .structured_output::<AnalysisState>("analysis_state")
        .context_manager(context.clone()).build()?;
    let summary_agent = Agent::builder(
        "summarizer",
        Arc::new(summary.clone()),
        ModelRef::new("test", "script"),
    )
    .context_manager(context)
    .build()?;
    let session = AgentSession::new(
        agent,
        Arc::new(SqliteStore::open(path)?),
        ConversationId::new(),
        MemoryNamespace::parse("example.research")?,
        ConversationContextPolicy::new(ConversationWindow::new(2)?),
    )
    .with_summary_agent(summary_agent, ConversationSummaryPassLimit::new(4)?);
    let mut state = AnalysisState::default();
    for (id, observation) in sources {
        evidence.insert(id.to_owned(), observation.to_owned());
        let input = serde_json::json!({"previous_validated_state": state, "new_evidence": {"source_id": id, "observation": observation}}).to_string();
        let request_id = CheckpointId::new(); // Persist this ID; reuse it on retries.
        let run = RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new());
        let outcome = session.run(request_id, input.clone(), &run).await?;
        let candidate = outcome.outcome.response.structured::<AnalysisState>()?;
        candidate.validate(&evidence, &state)?;
        state = candidate; // Promote only after application validation.
        let replay = session.run(request_id, input, &run).await?;
        ensure!(
            replay.outcome == outcome.outcome,
            "request replay must preserve state and context reports"
        );
        println!(
            "{id}: {} facts, {} context plan(s)",
            state.facts.len(),
            outcome.outcome.context_plans()?.len()
        );
    }
    ensure!(
        model.recorded_requests().len() == 3,
        "replay must not invoke the analyst again"
    );
    ensure!(
        summary.recorded_requests().len() == 1,
        "older history must be summarized once"
    );
    println!("{}", serde_json::to_string_pretty(&state)?);
    Ok(())
}
