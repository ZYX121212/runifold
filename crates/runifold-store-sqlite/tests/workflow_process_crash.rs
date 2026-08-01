//! Cross-process `SQLite` workflow lease and budget recovery.

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};

use runifold_core::{Budget, CheckpointId, Usage};
use runifold_store_sqlite::SqliteWorkflowStore;
use runifold_workflow::{
    LeaseDuration, WorkerId, WorkflowDisposition, WorkflowStore, WorkflowTask, WorkflowTaskStatus,
    WorkflowTenantBudgetPolicy, WorkflowTenantId,
};
use serde_json::json;
use uuid::Uuid;

const CHILD_ENV: &str = "RUNIFOLD_SQLITE_WORKFLOW_CRASH_CHILD";
const DATABASE_ENV: &str = "RUNIFOLD_SQLITE_WORKFLOW_CRASH_DATABASE";
const CHECKPOINT_ENV: &str = "RUNIFOLD_SQLITE_WORKFLOW_CRASH_CHECKPOINT";
const READY_ENV: &str = "RUNIFOLD_SQLITE_WORKFLOW_CRASH_READY";
const TEST_NAME: &str = "leased_workflow_and_budget_are_recovered_after_forced_process_kill";

#[test]
fn leased_workflow_and_budget_are_recovered_after_forced_process_kill() {
    if env::var_os(CHILD_ENV).is_some() {
        run_child();
        panic!("child should wait for the parent to kill it");
    }

    let fixture = CrashFixture::new();
    let mut child = fixture.spawn_child();
    fixture.wait_until_ready(&mut child);
    child.kill().expect("workflow child can be forcibly killed");
    let status = child.wait().expect("killed workflow child can be reaped");
    assert!(!status.success());
    thread::sleep(Duration::from_millis(150));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("test Tokio runtime builds");
    runtime.block_on(async {
        let store = SqliteWorkflowStore::open(&fixture.database).expect("workflow store reopens");
        let budget = store
            .inspect_tenant_budget(WorkflowTenantId::default())
            .await
            .expect("crashed reservation remains durable");
        assert_eq!(budget.reserved.tokens, 100);

        let recovered = store
            .claim(worker("parent-worker"), lease(Duration::from_secs(30)))
            .await
            .expect("recovery claim succeeds")
            .expect("expired crashed lease is reclaimable");
        assert_eq!(recovered.lease.attempt, 2);
        assert_eq!(recovered.lease.fencing_token, 2);
        store
            .reserve_budget(recovered.lease.clone(), workflow_budget(), Usage::default())
            .await
            .expect("successor adopts crashed reservation");
        store
            .settle_budget(
                recovered.lease.clone(),
                Usage {
                    tokens: 25,
                    ..Usage::default()
                },
            )
            .await
            .expect("successor settles adopted reservation");
        store
            .finish(recovered.lease, WorkflowDisposition::Completed)
            .await
            .expect("recovered workflow completes");
        assert_eq!(
            store
                .inspect(WorkflowTenantId::default(), fixture.checkpoint_id)
                .await
                .expect("recovered workflow remains inspectable")
                .status,
            WorkflowTaskStatus::Completed
        );
    });
    fixture.cleanup();
}

fn run_child() {
    let database = required_path(DATABASE_ENV);
    let checkpoint_id = checkpoint_id(&required(CHECKPOINT_ENV));
    let ready = required_path(READY_ENV);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("child Tokio runtime builds");
    runtime.block_on(async {
        let store = SqliteWorkflowStore::open(database).expect("child workflow store opens");
        store
            .set_tenant_budget_policy(
                WorkflowTenantId::default(),
                WorkflowTenantBudgetPolicy::new(
                    Budget {
                        tokens: Some(10_000),
                        ..Budget::default()
                    },
                    Duration::from_secs(60),
                    Duration::from_secs(30),
                )
                .expect("child budget policy is valid"),
            )
            .await
            .expect("child budget policy persists");
        store
            .enqueue(
                WorkflowTask::new("crash-recovery", 1, json!({}))
                    .expect("child task is valid")
                    .with_checkpoint_id(checkpoint_id),
            )
            .await
            .expect("child task enqueues");
        let claimed = store
            .claim(worker("child-worker"), lease(Duration::from_millis(100)))
            .await
            .expect("child claim succeeds")
            .expect("child task is claimable");
        store
            .reserve_budget(claimed.lease, workflow_budget(), Usage::default())
            .await
            .expect("child reservation persists");
        fs::write(ready, b"ready").expect("child publishes the forced-kill boundary");
        thread::sleep(Duration::from_secs(60));
    });
}

fn workflow_budget() -> Budget {
    Budget {
        tokens: Some(100),
        ..Budget::default()
    }
}

fn worker(value: &str) -> WorkerId {
    WorkerId::parse(value).expect("test worker identity is valid")
}

fn lease(duration: Duration) -> LeaseDuration {
    LeaseDuration::new(duration).expect("test lease is valid")
}

struct CrashFixture {
    directory: PathBuf,
    database: PathBuf,
    ready: PathBuf,
    checkpoint_id: CheckpointId,
}

impl CrashFixture {
    fn new() -> Self {
        let directory = env::temp_dir().join(format!("runifold-workflow-{}", Uuid::now_v7()));
        fs::create_dir(&directory).expect("test crash directory is created");
        Self {
            database: directory.join("runifold.sqlite3"),
            ready: directory.join("ready"),
            directory,
            checkpoint_id: CheckpointId::new(),
        }
    }

    fn spawn_child(&self) -> Child {
        Command::new(env::current_exe().expect("test executable is available"))
            .arg("--exact")
            .arg(TEST_NAME)
            .arg("--nocapture")
            .env(CHILD_ENV, "1")
            .env(DATABASE_ENV, &self.database)
            .env(CHECKPOINT_ENV, self.checkpoint_id.to_string())
            .env(READY_ENV, &self.ready)
            .spawn()
            .expect("workflow crash child starts")
    }

    fn wait_until_ready(&self, child: &mut Child) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !self.ready.exists() {
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("workflow child did not reach the forced-kill boundary");
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn cleanup(&self) {
        fs::remove_dir_all(&self.directory).expect("workflow crash fixture is removable");
    }
}

fn required(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("missing child environment variable `{name}`"))
}

fn required_path(name: &str) -> PathBuf {
    Path::new(&required(name)).to_path_buf()
}

fn checkpoint_id(value: &str) -> CheckpointId {
    CheckpointId::from_uuid(Uuid::parse_str(value).expect("checkpoint UUID is valid"))
}
