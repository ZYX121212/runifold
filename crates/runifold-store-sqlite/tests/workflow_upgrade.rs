//! Checkpoint upgrade contracts through independent durable `SQLite` connections.

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
use runifold_store_sqlite::SqliteWorkflowStore;
use runifold_workflow::{
    LeaseDuration, WorkerId, Workflow, WorkflowCheckpoint, WorkflowCheckpointHistoryLimit,
    WorkflowDisposition, WorkflowError, WorkflowResumePolicy, WorkflowStep, WorkflowStepFuture,
    WorkflowStore, WorkflowTask, WorkflowTenantId,
};
use serde_json::{Value, json};

struct Increment(Arc<AtomicUsize>);

impl WorkflowStep for Increment {
    fn execute<'a>(&'a self, input: Value, run: &'a RunContext) -> WorkflowStepFuture<'a> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            run.budget()
                .try_consume(Usage {
                    tokens: 2,
                    ..Usage::default()
                })
                .expect("fixture budget");
            assert_eq!(input, json!(41));
            Ok(json!(42))
        })
    }
}

// A literal old wire payload, independent of the current state's serializer.
// The Ready shape and all fields were checked against the v0.7.2 source.
fn payload() -> Value {
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
    reason = "one matrix checks durable upgrade, replay, history and stale ownership together"
)]
async fn legacy_checkpoints_upgrade_on_advance_and_future_state_is_preserved() {
    for schema_version in [3, 4, 5, 6] {
        let path = std::env::temp_dir().join(format!("runifold-upgrade-{}.sqlite", RunId::new()));
        let task = WorkflowTask::new("upgrade-contract", 7, json!(0)).expect("task");
        let id = task.checkpoint_id;
        let store = SqliteWorkflowStore::open(&path).expect("seed connection");
        store.enqueue(task).await.expect("enqueue");
        let duration = LeaseDuration::new(Duration::from_secs(30)).expect("lease");
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
            .expect("seed old checkpoint");
        store
            .finish(
                old.lease.clone(),
                WorkflowDisposition::RetryAfter(Duration::ZERO),
            )
            .await
            .expect("drain old worker");
        drop(store);

        let store = Arc::new(SqliteWorkflowStore::open(&path).expect("new worker connection"));
        let claimed = store
            .claim(WorkerId::parse("new-worker").expect("worker"), duration)
            .await
            .expect("claim")
            .expect("released task");
        assert!(claimed.lease.fencing_token > old.lease.fencing_token);
        assert_eq!(
            store
                .load_checkpoint(claimed.lease.clone())
                .await
                .expect("read-only load"),
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
        let checkpoint = WorkflowCheckpoint::distributed(store.clone(), claimed.lease.clone());
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
            assert_eq!(
                store
                    .load_checkpoint(claimed.lease.clone())
                    .await
                    .expect("future envelope"),
                source
            );
            assert_eq!(pending_calls.load(Ordering::SeqCst), 0);
        } else {
            let outcome = result.expect("resume supported old checkpoint");
            assert_eq!(outcome.output, json!(42));
            assert_eq!(
                outcome
                    .steps
                    .get(workflow.step_ids().next().expect("committed step")),
                Some(&json!(41))
            );
            assert_eq!(outcome.usage.tokens, usage.tokens + 2);
            let saved = store
                .load_checkpoint(claimed.lease.clone())
                .await
                .expect("current checkpoint");
            assert_eq!(saved.schema_version, 5);
            assert_eq!(saved.run_id, source.run_id);
            assert_eq!(saved.id, source.id);
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
                store
                    .load_checkpoint(claimed.lease.clone())
                    .await
                    .expect("replay is read-only"),
                saved
            );
            let history = store
                .list_checkpoint_history(
                    WorkflowTenantId::default(),
                    id,
                    None,
                    WorkflowCheckpointHistoryLimit::new(16).expect("limit"),
                )
                .await
                .expect("immutable history");
            assert_eq!(history.first().expect("source revision").revision, 0);
            assert_eq!(
                history.first().expect("source revision").state.value,
                json!(41)
            );
            assert_eq!(
                history.len(),
                usize::try_from(saved.revision + 1).expect("bounded revisions")
            );
            let stale = source
                .next(source.payload.clone())
                .expect("old writer candidate");
            let error = store
                .compare_and_swap_checkpoint(old.lease.clone(), stale, Some(source.revision))
                .await
                .expect_err("old worker is fenced");
            assert_eq!(error.kind, CheckpointErrorKind::Conflict);
        }
        assert_eq!(committed_calls.load(Ordering::SeqCst), 0);
        drop(checkpoint);
        drop(store);
        std::fs::remove_file(path).expect("remove fixture database");
    }
}
