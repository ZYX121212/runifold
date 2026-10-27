//! Bounded checkpoint-reader probe for the historical-worker upgrade gate.

use std::{error::Error, path::Path, sync::Arc};

use runifold_core::{
    Budget, BudgetTracker, CapabilitySet, Checkpoint, CheckpointError, CheckpointErrorKind,
    CheckpointId, CheckpointStore, InMemoryCheckpointStore, RunContext, RunId,
};
use runifold_workflow::{
    Workflow, WorkflowCheckpoint, WorkflowResumePolicy, WorkflowStep, WorkflowStepFuture,
};
use serde_json::{Value, json};

struct ReadOnlyCheckpoint(Checkpoint);

impl CheckpointStore for ReadOnlyCheckpoint {
    fn load(&self, id: CheckpointId) -> Result<Checkpoint, CheckpointError> {
        if id != self.0.id {
            return Err(CheckpointError::new(
                CheckpointErrorKind::NotFound,
                "probe checkpoint not found",
            ));
        }
        Ok(self.0.clone())
    }

    fn compare_and_swap(
        &self,
        _checkpoint: &Checkpoint,
        _expected_revision: Option<u64>,
    ) -> Result<(), CheckpointError> {
        Err(CheckpointError::new(
            CheckpointErrorKind::Storage,
            "reader probe does not write",
        ))
    }
}

struct Increment;

impl WorkflowStep for Increment {
    fn execute<'a>(&'a self, input: Value, _run: &'a RunContext) -> WorkflowStepFuture<'a> {
        Box::pin(async move {
            assert_eq!(
                input,
                json!(41),
                "only the synthetic probe fixture may execute"
            );
            Ok(json!(42))
        })
    }
}

fn produce(schema: u32, directory: &Path) -> Result<(), Box<dyn Error>> {
    let source = Checkpoint::initial(
        CheckpointId::new(),
        RunId::new(),
        "runifold.workflow",
        schema,
        json!({
            "workflow": "schema-probe", "workflow_version": 1, "layout": ["pending"],
            "next_index": 0, "value": 41, "outputs": {},
            "usage": {"tokens": 0, "cost_microusd": 0, "duration_micros": 0,
                      "turns": 0, "tool_calls": 0, "delegations": 0},
            "phase": {"state": "ready"}
        }),
    );
    let store = Arc::new(InMemoryCheckpointStore::new());
    store.compare_and_swap(&source, None)?;
    let handle = WorkflowCheckpoint::existing(source.id, store.clone());
    let workflow = Workflow::builder("schema-probe")
        .step("pending", Increment, CapabilitySet::new())
        .build()?;
    let run = RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new());
    let outcome = futures_executor::block_on(workflow.resume(
        &handle,
        &run,
        WorkflowResumePolicy::RejectAmbiguous,
    ))?;
    if outcome.output != json!(42) {
        return Err("probe did not complete the expected synthetic step".into());
    }
    let advanced = store.load(source.id)?;
    std::fs::create_dir_all(directory)?;
    std::fs::write(
        directory.join(format!("v{schema}-before.json")),
        serde_json::to_vec(&source)?,
    )?;
    std::fs::write(
        directory.join(format!("v{schema}-after.json")),
        serde_json::to_vec(&advanced)?,
    )?;
    println!(
        "{}",
        json!({"source_schema": schema, "advanced_schema": advanced.schema_version})
    );
    Ok(())
}

fn inspect(path: &Path) -> Result<(), Box<dyn Error>> {
    let checkpoint: Checkpoint = serde_json::from_slice(&std::fs::read(path)?)
        .map_err(|_| "probe input must be serialized Checkpoint JSON")?;
    let schema = checkpoint.schema_version;
    let handle =
        WorkflowCheckpoint::existing(checkpoint.id, Arc::new(ReadOnlyCheckpoint(checkpoint)));
    let result = match handle.load() {
        Ok(_) => json!({"readable": true, "schema_version": schema}),
        Err(error) if error.kind == CheckpointErrorKind::InvalidPayload => {
            json!({"readable": false, "schema_version": schema, "error_kind": "invalid_payload"})
        }
        Err(_) => return Err("probe failed outside the schema compatibility boundary".into()),
    };
    println!("{result}");
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = std::env::args_os().skip(1);
    match arguments
        .next()
        .as_deref()
        .and_then(std::ffi::OsStr::to_str)
    {
        Some("produce") => {
            let schema = arguments
                .next()
                .ok_or("produce requires a schema version")?
                .to_str()
                .ok_or("schema must be UTF-8")?
                .parse::<u32>()?;
            if ![3, 4].contains(&schema) {
                return Err("only legacy schemas 3 and 4 are probe inputs".into());
            }
            let directory = arguments
                .next()
                .ok_or("produce requires an output directory")?;
            produce(schema, Path::new(&directory))
        }
        Some("inspect") => {
            let path = arguments
                .next()
                .ok_or("inspect requires a checkpoint file")?;
            inspect(Path::new(&path))
        }
        _ => {
            Err("usage: checkpoint_schema_probe produce <3|4> <directory> | inspect <file>".into())
        }
    }
}
