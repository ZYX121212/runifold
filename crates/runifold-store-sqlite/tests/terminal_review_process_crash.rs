//! Cross-process recovery at the Agent terminal-review boundary.

use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use runifold_agent::{
    Agent, AgentCheckpoint, AgentCheckpointPhase, AgentCheckpointState, ResumePolicy,
    TerminalReviewPolicy, TerminalReviewVerdict, TerminalRuleReviewer, TurnReviewPolicy,
    TurnRuleReviewer,
};
use runifold_core::{
    Budget, BudgetTracker, CapabilityId, CapabilitySet, Checkpoint, CheckpointError, CheckpointId,
    CheckpointStore, EffectClass, RiskLevel, RunContext,
};
use runifold_model::{ContentPart, FinishReason, ModelRef, ModelStreamEvent, ToolCall};
use runifold_store_sqlite::SqliteStore;
use runifold_testkit::ScriptedModel;
use runifold_tool::{
    Tool, ToolContext, ToolDescriptor, ToolError, ToolFuture, ToolOutput, ToolRegistry,
};
use serde_json::{Value, json};
use uuid::Uuid;

const CHILD_ENV: &str = "RUNIFOLD_TERMINAL_REVIEW_CRASH_CHILD";
const DATABASE_ENV: &str = "RUNIFOLD_TERMINAL_REVIEW_CRASH_DATABASE";
const CHECKPOINT_ENV: &str = "RUNIFOLD_TERMINAL_REVIEW_CRASH_CHECKPOINT";
const READY_ENV: &str = "RUNIFOLD_TERMINAL_REVIEW_CRASH_READY";
const TEST_NAME: &str = "terminal_review_in_flight_recovers_without_regenerating_candidate";
const TURN_CHILD_ENV: &str = "RUNIFOLD_TURN_REVIEW_CRASH_CHILD";
const TURN_TEST_NAME: &str = "approved_turn_plan_survives_kill_without_regeneration_or_rereview";

#[test]
fn terminal_review_in_flight_recovers_without_regenerating_candidate() {
    if env::var_os(CHILD_ENV).is_some() {
        run_child();
        panic!("child should wait for the parent to kill it");
    }

    let fixture = CrashFixture::new();
    let mut child = fixture.spawn_child();
    fixture.wait_until_ready(&mut child);
    child.kill().expect("review child can be forcibly killed");
    let status = child.wait().expect("killed review child can be reaped");
    assert!(!status.success());

    let store = Arc::new(SqliteStore::open(&fixture.database).expect("SQLite store reopens"));
    let checkpoint = AgentCheckpoint::existing(fixture.checkpoint_id, store.clone());
    let (_, state) = checkpoint.load().expect("review checkpoint survives kill");
    assert!(matches!(
        state.phase,
        AgentCheckpointPhase::TerminalReviewInFlight { attempt: 1, .. }
    ));
    let model = ScriptedModel::new();
    let agent = configured_agent(model.clone());
    let run = restored_run(&state, store);

    let outcome = futures_executor::block_on(agent.resume(
        &checkpoint,
        &run,
        ResumePolicy::RetryInterruptedTurn,
    ))
    .expect("durable candidate is reviewed after restart");

    assert_eq!(outcome.text(), "durable candidate");
    assert!(
        model.recorded_requests().is_empty(),
        "generator model was invoked again instead of reusing the durable candidate"
    );
    fixture.cleanup();
}

#[test]
fn approved_turn_plan_survives_kill_without_regeneration_or_rereview() {
    if env::var_os(TURN_CHILD_ENV).is_some() {
        run_turn_child();
        panic!("child should wait for the parent to kill it");
    }

    let fixture = CrashFixture::new();
    let mut child = fixture.spawn_turn_child();
    fixture.wait_until_ready(&mut child);
    child
        .kill()
        .expect("turn-review child can be forcibly killed");
    let status = child
        .wait()
        .expect("killed turn-review child can be reaped");
    assert!(!status.success());

    let store = Arc::new(SqliteStore::open(&fixture.database).expect("SQLite store reopens"));
    let checkpoint = AgentCheckpoint::existing(fixture.checkpoint_id, store.clone());
    let (_, state) = checkpoint
        .load()
        .expect("turn-review checkpoint survives kill");
    assert!(matches!(
        state.phase,
        AgentCheckpointPhase::TurnReviewApproved { turn: 1, .. }
    ));
    let model = ScriptedModel::new();
    model.enqueue(response_events("terminal", "recovered"));
    let tool_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let reviewer_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let tool = Arc::new(CountingTool::new(Arc::clone(&tool_calls)));
    let agent = configured_turn_agent(model.clone(), tool.clone(), Arc::clone(&reviewer_calls));
    let run = restored_turn_run(&state, store, tool.descriptor().capability());

    let outcome = futures_executor::block_on(agent.resume(
        &checkpoint,
        &run,
        ResumePolicy::RetryInterruptedTurn,
    ))
    .expect("approved durable tool plan resumes after restart");

    assert_eq!(outcome.text(), "recovered");
    assert_eq!(tool_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(reviewer_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        model.recorded_requests().len(),
        1,
        "only the post-tool terminal turn should invoke the generator"
    );
    fixture.cleanup();
}

fn run_child() {
    let database = required_path(DATABASE_ENV);
    let checkpoint_id = checkpoint_id(&required(CHECKPOINT_ENV));
    let ready = required_path(READY_ENV);
    let store = Arc::new(SqliteStore::open(database).expect("child SQLite store opens"));
    let crashing_store = Arc::new(CrashAfterReviewInFlight {
        inner: store.clone(),
        ready,
    });
    let checkpoint = AgentCheckpoint::existing(checkpoint_id, crashing_store);
    let model = ScriptedModel::new();
    model.enqueue(response_events("candidate", "durable candidate"));
    let agent = configured_agent(model);
    let run = RunContext::root(BudgetTracker::new(Budget::default()), CapabilitySet::new())
        .with_journal(store);

    let _ = futures_executor::block_on(agent.run_checkpointed("start", &run, &checkpoint));
}

fn run_turn_child() {
    let database = required_path(DATABASE_ENV);
    let checkpoint_id = checkpoint_id(&required(CHECKPOINT_ENV));
    let ready = required_path(READY_ENV);
    let store = Arc::new(SqliteStore::open(database).expect("child SQLite store opens"));
    let crashing_store = Arc::new(CrashAfterTurnReviewApproved {
        inner: store.clone(),
        ready,
    });
    let checkpoint = AgentCheckpoint::existing(checkpoint_id, crashing_store);
    let model = ScriptedModel::new();
    model.enqueue(tool_plan_events());
    let tool_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let reviewer_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let tool = Arc::new(CountingTool::new(tool_calls));
    let agent = configured_turn_agent(model, tool.clone(), reviewer_calls);
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(tool.descriptor().capability());
    let run =
        RunContext::root(BudgetTracker::new(Budget::default()), capabilities).with_journal(store);

    let _ = futures_executor::block_on(agent.run_checkpointed("start", &run, &checkpoint));
}

fn configured_agent(model: ScriptedModel) -> Agent {
    let reviewer = TerminalRuleReviewer::new("process-review", "v1", |_| {
        Ok(TerminalReviewVerdict::approve())
    })
    .expect("reviewer identity is valid");
    Agent::new(
        "review-crash-worker",
        Arc::new(model),
        ModelRef::new("test", "review-crash-script"),
    )
    .terminal_reviewer(reviewer, TerminalReviewPolicy::new(1), CapabilitySet::new())
}

fn configured_turn_agent(
    model: ScriptedModel,
    tool: Arc<CountingTool>,
    reviewer_calls: Arc<std::sync::atomic::AtomicUsize>,
) -> Agent {
    let reviewer = TurnRuleReviewer::new("process-turn-review", "v1", move |_| {
        reviewer_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(TerminalReviewVerdict::approve())
    })
    .expect("turn reviewer identity is valid");
    let mut tools = ToolRegistry::new();
    tools.register(tool).expect("test tool registers");
    Agent::new(
        "turn-review-crash-worker",
        Arc::new(model),
        ModelRef::new("test", "review-crash-script"),
    )
    .tools(tools)
    .turn_reviewer(reviewer, TurnReviewPolicy::new(0), CapabilitySet::new())
}

fn restored_run(state: &AgentCheckpointState, journal: Arc<SqliteStore>) -> RunContext {
    RunContext::root(
        BudgetTracker::restore(Budget::default(), state.usage)
            .expect("checkpoint usage fits the unbounded test budget"),
        CapabilitySet::new(),
    )
    .with_journal(journal)
}

fn restored_turn_run(
    state: &AgentCheckpointState,
    journal: Arc<SqliteStore>,
    capability: runifold_core::CapabilityDescriptor,
) -> RunContext {
    let mut capabilities = CapabilitySet::new();
    capabilities.grant(capability);
    RunContext::root(
        BudgetTracker::restore(Budget::default(), state.usage)
            .expect("checkpoint usage fits the unbounded test budget"),
        capabilities,
    )
    .with_journal(journal)
}

fn response_events(id: &str, text: &str) -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ResponseStarted {
            id: Some(id.into()),
            model: ModelRef::new("test", "review-crash-script"),
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

fn tool_plan_events() -> Vec<ModelStreamEvent> {
    vec![
        ModelStreamEvent::ResponseStarted {
            id: Some("tool-plan".into()),
            model: ModelRef::new("test", "review-crash-script"),
        },
        ModelStreamEvent::ContentPartCompleted {
            index: 0,
            part: ContentPart::ToolCall(ToolCall {
                id: "call-1".into(),
                name: "count_once".into(),
                arguments: json!({"value": 7}),
                raw_arguments: None,
                metadata: BTreeMap::new(),
            }),
        },
        ModelStreamEvent::ResponseCompleted {
            finish_reason: FinishReason::ToolCalls,
            provider_metadata: BTreeMap::new(),
        },
    ]
}

struct CrashAfterReviewInFlight {
    inner: Arc<SqliteStore>,
    ready: PathBuf,
}

struct CrashAfterTurnReviewApproved {
    inner: Arc<SqliteStore>,
    ready: PathBuf,
}

impl CheckpointStore for CrashAfterTurnReviewApproved {
    fn load(&self, id: CheckpointId) -> Result<Checkpoint, CheckpointError> {
        CheckpointStore::load(self.inner.as_ref(), id)
    }

    fn compare_and_swap(
        &self,
        checkpoint: &Checkpoint,
        expected_revision: Option<u64>,
    ) -> Result<(), CheckpointError> {
        CheckpointStore::compare_and_swap(self.inner.as_ref(), checkpoint, expected_revision)?;
        let state = serde_json::from_value::<AgentCheckpointState>(checkpoint.payload.clone());
        if state.is_ok_and(|state| {
            matches!(state.phase, AgentCheckpointPhase::TurnReviewApproved { .. })
        }) {
            fs::write(&self.ready, b"ready")
                .expect("child publishes the approved-turn kill boundary");
            thread::sleep(Duration::from_secs(60));
        }
        Ok(())
    }
}

struct CountingTool {
    descriptor: ToolDescriptor,
    calls: Arc<std::sync::atomic::AtomicUsize>,
}

impl CountingTool {
    fn new(calls: Arc<std::sync::atomic::AtomicUsize>) -> Self {
        Self {
            descriptor: ToolDescriptor {
                id: CapabilityId::new(),
                name: "count_once".into(),
                version: "1".into(),
                description: "count one test invocation".into(),
                input_schema: json!({"type": "object"}),
                output_schema: json!({"type": "object"}),
                effect: EffectClass::Pure,
                risk: RiskLevel::Low,
                metadata: BTreeMap::new(),
            },
            calls,
        }
    }
}

impl Tool for CountingTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        input: Value,
        _context: ToolContext,
    ) -> ToolFuture<'_, Result<ToolOutput, ToolError>> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move { Ok(ToolOutput::model_visible(input)) })
    }
}

impl CheckpointStore for CrashAfterReviewInFlight {
    fn load(&self, id: CheckpointId) -> Result<Checkpoint, CheckpointError> {
        CheckpointStore::load(self.inner.as_ref(), id)
    }

    fn compare_and_swap(
        &self,
        checkpoint: &Checkpoint,
        expected_revision: Option<u64>,
    ) -> Result<(), CheckpointError> {
        CheckpointStore::compare_and_swap(self.inner.as_ref(), checkpoint, expected_revision)?;
        let state = serde_json::from_value::<AgentCheckpointState>(checkpoint.payload.clone());
        if state.is_ok_and(|state| {
            matches!(
                state.phase,
                AgentCheckpointPhase::TerminalReviewInFlight { .. }
            )
        }) {
            fs::write(&self.ready, b"ready")
                .expect("child publishes the terminal-review kill boundary");
            thread::sleep(Duration::from_secs(60));
        }
        Ok(())
    }
}

struct CrashFixture {
    directory: PathBuf,
    database: PathBuf,
    ready: PathBuf,
    checkpoint_id: CheckpointId,
}

impl CrashFixture {
    fn new() -> Self {
        let directory = env::temp_dir().join(format!("runifold-review-crash-{}", Uuid::now_v7()));
        fs::create_dir(&directory).expect("temporary review-crash directory is created");
        Self {
            database: directory.join("runifold.sqlite3"),
            ready: directory.join("ready"),
            directory,
            checkpoint_id: CheckpointId::new(),
        }
    }

    fn spawn_child(&self) -> Child {
        Command::new(env::current_exe().expect("test executable path is available"))
            .arg("--exact")
            .arg(TEST_NAME)
            .arg("--nocapture")
            .env(CHILD_ENV, "1")
            .env(DATABASE_ENV, &self.database)
            .env(CHECKPOINT_ENV, self.checkpoint_id.to_string())
            .env(READY_ENV, &self.ready)
            .spawn()
            .expect("terminal-review crash child starts")
    }

    fn spawn_turn_child(&self) -> Child {
        Command::new(env::current_exe().expect("test executable path is available"))
            .arg("--exact")
            .arg(TURN_TEST_NAME)
            .arg("--nocapture")
            .env(TURN_CHILD_ENV, "1")
            .env(DATABASE_ENV, &self.database)
            .env(CHECKPOINT_ENV, self.checkpoint_id.to_string())
            .env(READY_ENV, &self.ready)
            .spawn()
            .expect("turn-review crash child starts")
    }

    fn wait_until_ready(&self, child: &mut Child) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !self.ready.exists() {
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("review child did not reach the forced-kill boundary");
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn cleanup(&self) {
        fs::remove_dir_all(&self.directory).expect("temporary review-crash directory is removable");
    }
}

fn required(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("missing child environment variable `{name}`"))
}

fn required_path(name: &str) -> PathBuf {
    Path::new(&required(name)).to_path_buf()
}

fn checkpoint_id(value: &str) -> CheckpointId {
    CheckpointId::from_uuid(Uuid::parse_str(value).expect("checkpoint id is a UUID"))
}
