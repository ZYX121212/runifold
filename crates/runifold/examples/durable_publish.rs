//! Offline report publication with a durable Session and reconciled external effect.
//! See `docs/RELEASE-0.10.md` for the forced-process-kill acceptance scenario.

use std::{collections::BTreeMap, fs, io::Write, path::PathBuf, sync::Arc};

use anyhow::{Context, ensure};
use runifold::{
    Agent, AgentSession, Budget, BudgetTracker, CapabilitySet, ConversationContextPolicy,
    ConversationId, ConversationWindow, MemoryNamespace, RunContext, TerminalReviewPolicy,
    TerminalReviewVerdict, TerminalRuleReviewer,
    core::{
        CapabilityDescriptor, CapabilityId, CapabilityKind, CheckpointId, EffectClass, EffectId,
        EffectKind, EffectRequest, InvocationId, RetrySafety, RiskLevel, RunError, RunErrorKind,
    },
    effect::{
        EffectExecutionContext, EffectExecutor, EffectFuture, EffectHandler, EffectReconciler,
        EffectReconciliation, EffectRecoveryPolicy,
    },
    model::{ContentPart, FinishReason, ModelRef, ModelStreamEvent},
    sqlite::SqliteStore,
};
use runifold_testkit::ScriptedModel;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const REPORT: &str = "Report: durable tasks retain their committed results. Source: local fixture.";

#[derive(Deserialize, Serialize)]
struct TaskIdentity {
    conversation: ConversationId,
    request: CheckpointId,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let action = args
        .next()
        .context("expected init, run, pause-after-publish, or revoked")?;
    let directory = PathBuf::from(args.next().context("expected task directory")?);
    ensure!(args.next().is_none(), "unexpected arguments");
    let identity_path = directory.join("task.json");
    if action == "init" {
        fs::create_dir_all(&directory)?;
        let identity = TaskIdentity {
            conversation: ConversationId::new(),
            request: CheckpointId::new(),
        };
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(identity_path)?;
        file.write_all(&serde_json::to_vec(&identity)?)?;
        file.sync_all()?;
        println!("Task initialized.");
        return Ok(());
    }
    ensure!(
        matches!(action.as_str(), "run" | "pause-after-publish" | "revoked"),
        "unknown action"
    );
    let identity: TaskIdentity = serde_json::from_slice(&fs::read(identity_path)?)?;
    let store = Arc::new(SqliteStore::open(directory.join("runtime.sqlite"))?);
    let (report, model_calls) = generate_report(&identity, store.clone()).await?;
    let capability = CapabilityDescriptor {
        id: CapabilityId::from_uuid(identity.request.as_uuid()),
        name: "publish-report".into(),
        version: "1".into(),
        kind: CapabilityKind::Resource,
        input_schema: json!({"type": "object"}),
        output_schema: json!({"type": "object"}),
        effect: EffectClass::NonIdempotentWrite,
        risk: RiskLevel::Low,
        metadata: BTreeMap::new(),
    };
    let request = EffectRequest {
        effect_id: EffectId::from_uuid(identity.request.as_uuid()),
        invocation_id: InvocationId::from_uuid(identity.request.as_uuid()),
        kind: EffectKind::Extension("example.publish-report".into()),
        capability_id: capability.id,
        input: json!({"report": report, "destination": "internal-report"}),
        effect_class: capability.effect,
        idempotency_key: Some(identity.request.to_string()),
    };
    let mut grants = CapabilitySet::new();
    if action != "revoked" {
        grants.grant(capability);
    }
    let publish_run = RunContext::root(BudgetTracker::new(Budget::default()), grants);
    let publisher = LocalPublisher {
        directory,
        pause: action == "pause-after-publish",
    };
    let outcome = EffectExecutor::new(store)
        .execute_reconciled(
            request,
            &publish_run,
            &publisher,
            &publisher,
            EffectRecoveryPolicy::RejectAmbiguous,
        )
        .await?;
    ensure!(
        outcome.output["report"] == report,
        "receipt differs from accepted report"
    );
    println!(
        "Published: {}; replayed={}; model_calls={}",
        outcome.output["destination"], outcome.replayed, model_calls
    );
    Ok(())
}

async fn generate_report(
    identity: &TaskIdentity,
    store: Arc<SqliteStore>,
) -> anyhow::Result<(String, usize)> {
    let model = ScriptedModel::new();
    model.enqueue(response());
    let reviewer = TerminalRuleReviewer::new("source-check", "1", |request| {
        if request.candidate.text() == REPORT {
            Ok(TerminalReviewVerdict::approve())
        } else {
            TerminalReviewVerdict::repair(json!({"code": "unexpected_report"}))
        }
    })?;
    let agent = Agent::new(
        "report-writer",
        Arc::new(model.clone()),
        ModelRef::new("test", "report"),
    )
    .terminal_reviewer(reviewer, TerminalReviewPolicy::new(0), CapabilitySet::new());
    let session = AgentSession::new(
        agent,
        store.clone(),
        identity.conversation,
        MemoryNamespace::parse("example.reports")?,
        ConversationContextPolicy::new(ConversationWindow::new(8)?),
    );
    let run = RunContext::root(
        BudgetTracker::new(Budget {
            turns: Some(2),
            ..Budget::default()
        }),
        CapabilitySet::new(),
    );
    let report = session
        .run(identity.request, "Write the fixture report.", &run)
        .await?;
    Ok((report.outcome.text(), model.recorded_requests().len()))
}

// An application-owned publishing boundary, persisted separately from the
// runtime database. create_new deliberately fails on a second physical write:
// recovery must query its receipt, not invoke it again. This fixture does not
// model arbitrary remote services, host fencing, or an approval service.
struct LocalPublisher {
    directory: PathBuf,
    pause: bool,
}

impl EffectHandler for LocalPublisher {
    fn execute(
        &self,
        request: &EffectRequest,
        _: EffectExecutionContext,
    ) -> EffectFuture<'_, Result<Value, RunError>> {
        let directory = self.directory.clone();
        let input = request.input.clone();
        Box::pin(async move {
            let output = tokio::task::spawn_blocking(move || -> anyhow::Result<Value> {
                let mut receipt = fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(directory.join("published.json"))?;
                receipt.write_all(&serde_json::to_vec(&input)?)?;
                receipt.sync_all()?;
                fs::write(directory.join("published.ready"), b"ready")?;
                Ok(input)
            })
            .await
            .map_err(publish_error)?
            .map_err(publish_error)?;
            if self.pause {
                // The acceptance harness kills this process after observing the
                // durable receipt and before the runtime can record Completed.
                std::future::pending::<()>().await;
            }
            Ok(output)
        })
    }
}

impl EffectReconciler for LocalPublisher {
    fn reconcile(
        &self,
        request: &EffectRequest,
        _: EffectExecutionContext,
    ) -> EffectFuture<'_, Result<EffectReconciliation, RunError>> {
        let receipt = self.directory.join("published.json");
        let expected = request.input.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || -> Result<EffectReconciliation, RunError> {
                match fs::read(receipt) {
                    Ok(bytes) => {
                        let output: Value =
                            serde_json::from_slice(&bytes).map_err(publish_error)?;
                        if output == expected {
                            Ok(EffectReconciliation::Completed(output))
                        } else {
                            Ok(EffectReconciliation::Ambiguous)
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        Ok(EffectReconciliation::NotExecuted)
                    }
                    Err(error) => Err(publish_error(error)),
                }
            })
            .await
            .map_err(publish_error)?
        })
    }
}

fn publish_error(_error: impl std::fmt::Display) -> RunError {
    RunError {
        kind: RunErrorKind::Transport,
        message: "local publication boundary failed; inspect its receipt".into(),
        retry_safety: RetrySafety::UnsafeAfterSideEffect,
        metadata: BTreeMap::new(),
    }
}

fn response() -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ResponseStarted {
            id: Some("fixture".into()),
            model: ModelRef::new("test", "report"),
        },
        ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::text(REPORT),
        },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::Stop,
            provider_metadata: BTreeMap::new(),
        },
    ]
}
