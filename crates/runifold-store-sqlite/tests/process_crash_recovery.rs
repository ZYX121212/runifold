//! Cross-process crash recovery at the Effect/Checkpoint boundary.

use std::{
    collections::BTreeMap,
    env, fs,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
    sync::Arc,
};

use runifold_agent::{Agent, AgentCheckpoint, ResumePolicy};
use runifold_core::{
    Budget, BudgetTracker, CapabilityId, CapabilitySet, Checkpoint, CheckpointError, CheckpointId,
    CheckpointStore, EffectClass, RiskLevel, RunContext,
};
use runifold_effect::{EffectExecutor, EffectRecoveryPolicy};
use runifold_model::{ContentPart, FinishReason, ModelRef, ModelStreamEvent, ToolCall};
use runifold_store_sqlite::SqliteStore;
use runifold_testkit::ScriptedModel;
use runifold_tool::{
    Tool, ToolContext, ToolDescriptor, ToolError, ToolFuture, ToolOutput, ToolRegistry,
};
use serde_json::json;
use uuid::Uuid;

const CHILD_ENV: &str = "RUNIFOLD_SQLITE_CRASH_CHILD";
const DATABASE_ENV: &str = "RUNIFOLD_SQLITE_CRASH_DATABASE";
const SIDE_EFFECT_ENV: &str = "RUNIFOLD_SQLITE_CRASH_SIDE_EFFECT";
const CHECKPOINT_ENV: &str = "RUNIFOLD_SQLITE_CRASH_CHECKPOINT";
const CAPABILITY_ENV: &str = "RUNIFOLD_SQLITE_CRASH_CAPABILITY";
const CRASH_EXIT_CODE: i32 = 86;
const TEST_NAME: &str = "completed_tool_is_replayed_after_process_crash";

#[test]
fn completed_tool_is_replayed_after_process_crash() {
    if env::var_os(CHILD_ENV).is_some() {
        run_child();
        panic!("child should terminate at the simulated crash boundary");
    }

    let fixture = CrashFixture::new();
    let status = fixture.spawn_child();
    assert_eq!(
        status.code(),
        Some(CRASH_EXIT_CODE),
        "child did not terminate at the intended crash boundary"
    );
    assert_eq!(fixture.side_effect_count(), 1);

    let store = Arc::new(SqliteStore::open(&fixture.database).unwrap());
    let model = retry_model();
    let tool = Arc::new(FileAppendTool::new(
        fixture.capability_id,
        fixture.side_effect.clone(),
    ));
    let agent = configured_agent(model, tool, store.clone());
    let checkpoint = AgentCheckpoint::existing(fixture.checkpoint_id, store.clone());
    let run = run_context(fixture.capability_id, store);

    let outcome = futures_executor::block_on(agent.resume(
        &checkpoint,
        &run,
        ResumePolicy::RetryInterruptedTurn,
    ))
    .unwrap();

    assert_eq!(
        outcome.response.content,
        vec![ContentPart::text("recovered")]
    );
    assert_eq!(
        fixture.side_effect_count(),
        1,
        "completed Tool effect was physically executed more than once"
    );
    fixture.cleanup();
}

fn run_child() {
    let database = required_path(DATABASE_ENV);
    let side_effect = required_path(SIDE_EFFECT_ENV);
    let checkpoint_id = checkpoint_id(&required(CHECKPOINT_ENV));
    let capability_id = capability_id(&required(CAPABILITY_ENV));
    let store = Arc::new(SqliteStore::open(database).unwrap());
    let crashing_checkpoints = Arc::new(CrashOnStableCheckpoint {
        inner: store.clone(),
    });
    let checkpoint = AgentCheckpoint::existing(checkpoint_id, crashing_checkpoints);
    let model = tool_call_model();
    let tool = Arc::new(FileAppendTool::new(capability_id, side_effect));
    let agent = configured_agent(model, tool, store.clone());
    let run = run_context(capability_id, store);

    let _ = futures_executor::block_on(agent.run_checkpointed("start", &run, &checkpoint));
}

fn configured_agent(
    model: ScriptedModel,
    tool: Arc<FileAppendTool>,
    store: Arc<SqliteStore>,
) -> Agent {
    let mut tools = ToolRegistry::new();
    tools.register(tool).unwrap();
    Agent::new(
        "crash-worker",
        Arc::new(model),
        ModelRef::new("test", "crash-script"),
    )
    .tools(tools)
    .effect_executor(EffectExecutor::new(store))
    .effect_recovery_policy(EffectRecoveryPolicy::RejectAmbiguous)
}

fn run_context(capability_id: CapabilityId, journal: Arc<SqliteStore>) -> RunContext {
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(FileAppendTool::descriptor_for(capability_id).capability());
    RunContext::root(BudgetTracker::new(Budget::default()), capabilities).with_journal(journal)
}

fn tool_call_model() -> ScriptedModel {
    let model = ScriptedModel::new();
    model.enqueue(response_events(
        "tool-call",
        vec![ContentPart::ToolCall(ToolCall {
            id: "append-1".into(),
            name: "append_once".into(),
            arguments: json!({"value": "committed"}),
            raw_arguments: None,
            metadata: BTreeMap::new(),
        })],
        FinishReason::ToolCalls,
    ));
    model
}

fn retry_model() -> ScriptedModel {
    let model = tool_call_model();
    model.enqueue(response_events(
        "terminal",
        vec![ContentPart::text("recovered")],
        FinishReason::Stop,
    ));
    model
}

fn response_events(
    id: &str,
    content: Vec<ContentPart>,
    finish_reason: FinishReason,
) -> Vec<ModelStreamEvent> {
    let mut events = vec![ModelStreamEvent::ResponseStarted {
        id: Some(id.into()),
        model: ModelRef::new("test", "crash-script"),
    }];
    events.extend(content.into_iter().enumerate().map(|(index, part)| {
        ModelStreamEvent::ContentPartCompleted {
            index: u32::try_from(index).unwrap(),
            part,
        }
    }));
    events.push(ModelStreamEvent::ResponseCompleted {
        finish_reason,
        provider_metadata: BTreeMap::new(),
    });
    events
}

struct FileAppendTool {
    descriptor: ToolDescriptor,
    path: PathBuf,
}

impl FileAppendTool {
    fn new(capability_id: CapabilityId, path: PathBuf) -> Self {
        Self {
            descriptor: Self::descriptor_for(capability_id),
            path,
        }
    }

    fn descriptor_for(id: CapabilityId) -> ToolDescriptor {
        ToolDescriptor {
            id,
            name: "append_once".into(),
            version: "1".into(),
            description: "append one durable marker".into(),
            input_schema: json!({
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
                "additionalProperties": false
            }),
            output_schema: json!({"type": "object"}),
            effect: EffectClass::IdempotentWrite,
            risk: RiskLevel::Low,
            metadata: BTreeMap::new(),
        }
    }
}

impl Tool for FileAppendTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        input: serde_json::Value,
        _context: ToolContext,
    ) -> ToolFuture<'_, Result<ToolOutput, ToolError>> {
        let path = self.path.clone();
        Box::pin(async move {
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap();
            writeln!(file, "effect").unwrap();
            file.sync_all().unwrap();
            Ok(ToolOutput::model_visible(input))
        })
    }
}

struct CrashOnStableCheckpoint {
    inner: Arc<SqliteStore>,
}

impl CheckpointStore for CrashOnStableCheckpoint {
    fn load(&self, id: CheckpointId) -> Result<Checkpoint, CheckpointError> {
        CheckpointStore::load(self.inner.as_ref(), id)
    }

    fn compare_and_swap(
        &self,
        checkpoint: &Checkpoint,
        expected_revision: Option<u64>,
    ) -> Result<(), CheckpointError> {
        if checkpoint.revision == 2 {
            std::process::exit(CRASH_EXIT_CODE);
        }
        CheckpointStore::compare_and_swap(self.inner.as_ref(), checkpoint, expected_revision)
    }
}

struct CrashFixture {
    directory: PathBuf,
    database: PathBuf,
    side_effect: PathBuf,
    checkpoint_id: CheckpointId,
    capability_id: CapabilityId,
}

impl CrashFixture {
    fn new() -> Self {
        let directory = env::temp_dir().join(format!("runifold-crash-{}", Uuid::now_v7()));
        fs::create_dir(&directory).unwrap();
        Self {
            database: directory.join("runifold.sqlite3"),
            side_effect: directory.join("effect.log"),
            directory,
            checkpoint_id: CheckpointId::new(),
            capability_id: CapabilityId::new(),
        }
    }

    fn spawn_child(&self) -> ExitStatus {
        Command::new(env::current_exe().unwrap())
            .arg("--exact")
            .arg(TEST_NAME)
            .arg("--nocapture")
            .env(CHILD_ENV, "1")
            .env(DATABASE_ENV, &self.database)
            .env(SIDE_EFFECT_ENV, &self.side_effect)
            .env(CHECKPOINT_ENV, self.checkpoint_id.to_string())
            .env(CAPABILITY_ENV, self.capability_id.to_string())
            .status()
            .unwrap()
    }

    fn side_effect_count(&self) -> usize {
        fs::read_to_string(&self.side_effect)
            .unwrap_or_default()
            .lines()
            .count()
    }

    fn cleanup(&self) {
        fs::remove_dir_all(&self.directory).unwrap();
    }
}

fn required(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("missing child environment variable `{name}`"))
}

fn required_path(name: &str) -> PathBuf {
    Path::new(&required(name)).to_path_buf()
}

fn checkpoint_id(value: &str) -> CheckpointId {
    CheckpointId::from_uuid(Uuid::parse_str(value).unwrap())
}

fn capability_id(value: &str) -> CapabilityId {
    CapabilityId::from_uuid(Uuid::parse_str(value).unwrap())
}
