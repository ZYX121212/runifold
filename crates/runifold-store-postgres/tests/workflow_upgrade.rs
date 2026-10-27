//! Real `PostgreSQL` checkpoint upgrade and drained-worker fencing contracts.

mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use runifold_core::{
    Budget, BudgetTracker, CapabilitySet, Checkpoint, CheckpointErrorKind, RunContext, RunId, Usage,
};
use runifold_store_postgres::PostgresWorkflowStore;
use runifold_workflow::{
    LeaseDuration, WorkerId, Workflow, WorkflowCheckpoint, WorkflowDisposition, WorkflowError,
    WorkflowResumePolicy, WorkflowStep, WorkflowStepFuture, WorkflowStore, WorkflowStoreErrorKind,
    WorkflowTask, WorkflowTenantId,
};
use serde_json::{Value, json};

struct Increment(Arc<AtomicUsize>);

impl WorkflowStep for Increment {
    fn execute<'a>(&'a self, input: Value, run: &'a RunContext) -> WorkflowStepFuture<'a> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            assert_eq!(input, json!(41));
            run.budget()
                .try_consume(Usage {
                    tokens: 2,
                    ..Usage::default()
                })
                .expect("fixture budget");
            Ok(json!(42))
        })
    }
}

fn payload() -> Value {
    // Literal v0.7.2 Ready wire shape, independent of today's serializer.
    json!({
        "workflow": "upgrade-contract", "workflow_version": 7,
        "layout": ["committed", "pending"], "next_index": 1, "value": 41,
        "outputs": {"committed": 41},
        "usage": {"tokens": 11, "cost_microusd": 12, "duration_micros": 13,
                  "turns": 14, "tool_calls": 15, "delegations": 16},
        "phase": {"state": "ready"}
    })
}

fn run(usage: Usage) -> RunContext {
    RunContext::root(
        BudgetTracker::restore(Budget::default(), usage).expect("restored usage"),
        CapabilitySet::new(),
    )
}

#[tokio::test(flavor = "multi_thread")]
#[allow(
    clippy::too_many_lines,
    reason = "one real database matrix covers DDL replay, reconnect, schema promotion and stale ownership"
)]
async fn legacy_checkpoints_upgrade_after_drain_and_future_state_is_preserved() {
    let database = support::PostgresTestContext::isolated().await;
    let table = format!("upgrade_{}", uuid::Uuid::now_v7().simple());
    let store = PostgresWorkflowStore::connect(database.connection_url(), &table)
        .await
        .expect("seed connection");
    store.ensure_schema().await.expect("schema");
    let duration = LeaseDuration::new(Duration::from_secs(30)).expect("lease");
    for schema_version in [3, 4, 5, 6] {
        let task = WorkflowTask::new("upgrade-contract", 7, json!(0)).expect("task");
        let id = task.checkpoint_id;
        store.enqueue(task).await.expect("enqueue");
        let old = store
            .claim(WorkerId::parse("old-worker").expect("worker"), duration)
            .await
            .expect("claim")
            .expect("task");
        let source = Checkpoint::initial(
            id,
            RunId::new(),
            "runifold.workflow",
            schema_version,
            payload(),
        );
        store
            .compare_and_swap_checkpoint(old.lease.clone(), source.clone(), None)
            .await
            .expect("old checkpoint");
        store
            .finish(
                old.lease.clone(),
                WorkflowDisposition::RetryAfter(Duration::ZERO),
            )
            .await
            .expect("drain old worker");
        let reopened = Arc::new(
            PostgresWorkflowStore::connect(database.connection_url(), &table)
                .await
                .expect("new connection"),
        );
        if schema_version == 4 {
            let (admin, connection) =
                tokio_postgres::connect(database.connection_url(), tokio_postgres::NoTls)
                    .await
                    .expect("migration fixture connection");
            tokio::spawn(async move {
                let _ = connection.await;
            });
            // Pre-0.10 schema lacked this fresh-snapshot admission helper.
            admin
                .batch_execute(&format!("DROP FUNCTION {table}_claim_allowed(TEXT)"))
                .await
                .expect("restore the pre-upgrade helper boundary");
            let before = reopened
                .inspect(WorkflowTenantId::default(), id)
                .await
                .expect("queued old task");
            let error = reopened
                .claim(WorkerId::parse("not-migrated").expect("worker"), duration)
                .await
                .expect_err("runtime claim cannot silently install schema");
            assert_eq!(error.kind, WorkflowStoreErrorKind::Storage);
            assert_eq!(
                reopened
                    .inspect(WorkflowTenantId::default(), id)
                    .await
                    .expect("unchanged queue"),
                before
            );
        }
        reopened
            .ensure_schema()
            .await
            .expect("explicit schema setup is repeatable");
        let new = reopened
            .claim(WorkerId::parse("new-worker").expect("worker"), duration)
            .await
            .expect("claim")
            .expect("drained task");
        assert!(new.lease.fencing_token > old.lease.fencing_token);
        assert_eq!(
            reopened
                .load_checkpoint(new.lease.clone())
                .await
                .expect("unchanged legacy envelope"),
            source
        );
        let committed_calls = Arc::new(AtomicUsize::new(0));
        let pending_calls = Arc::new(AtomicUsize::new(0));
        let workflow = Workflow::builder("upgrade-contract")
            .version(7)
            .step(
                "committed",
                Increment(committed_calls.clone()),
                CapabilitySet::new(),
            )
            .step(
                "pending",
                Increment(pending_calls.clone()),
                CapabilitySet::new(),
            )
            .build()
            .expect("workflow");
        let checkpoint = WorkflowCheckpoint::distributed(reopened.clone(), new.lease.clone());
        let usage: Usage =
            serde_json::from_value(source.payload["usage"].clone()).expect("legacy usage");
        let result = workflow
            .resume(
                &checkpoint,
                &run(usage),
                WorkflowResumePolicy::RejectAmbiguous,
            )
            .await;
        if schema_version > 5 {
            assert!(
                matches!(result, Err(WorkflowError::Checkpoint(error)) if error.kind == CheckpointErrorKind::InvalidPayload)
            );
            assert_eq!(pending_calls.load(Ordering::SeqCst), 0);
            assert_eq!(
                reopened
                    .load_checkpoint(new.lease.clone())
                    .await
                    .expect("future state"),
                source
            );
            reopened
                .finish(
                    new.lease,
                    WorkflowDisposition::Failed("unsupported checkpoint schema".into()),
                )
                .await
                .expect("stop unsupported work");
        } else {
            let outcome = result.expect("supported schema resumes");
            assert_eq!(outcome.output, json!(42));
            assert_eq!(outcome.usage.tokens, usage.tokens + 2);
            let saved = reopened
                .load_checkpoint(new.lease.clone())
                .await
                .expect("upgraded envelope");
            assert_eq!(saved.schema_version, 5);
            assert_eq!(saved.run_id, source.run_id);
            assert_eq!(saved.payload["outputs"]["committed"], json!(41));
            let replay = workflow
                .resume(
                    &checkpoint,
                    &run(outcome.usage),
                    WorkflowResumePolicy::RejectAmbiguous,
                )
                .await
                .expect("completed replay");
            assert_eq!(replay.output, outcome.output);
            assert_eq!(pending_calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                reopened
                    .load_checkpoint(new.lease.clone())
                    .await
                    .expect("replay is read-only"),
                saved
            );
            let stale = source.next(source.payload.clone()).expect("old write");
            assert_eq!(
                store
                    .compare_and_swap_checkpoint(old.lease.clone(), stale, Some(source.revision))
                    .await
                    .expect_err("old worker is fenced")
                    .kind,
                CheckpointErrorKind::Conflict
            );
            reopened
                .finish(new.lease, WorkflowDisposition::Completed)
                .await
                .expect("complete task");
        }
        assert_eq!(committed_calls.load(Ordering::SeqCst), 0);
    }
}
