//! Own and project canonical Agent executions across the Python boundary.
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use futures_util::StreamExt;
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use runifold::{
    AgentCheckpoint, AgentStreamEvent, BudgetTracker, CapabilitySet, CheckpointId, ProviderRuntime,
    ResumePolicy, RunContext, Tool,
    core::Instant,
    effect::{EffectExecutor, InMemoryEffectStore},
    model::OutputFormat,
};
use serde_json::{Value, json};
use tokio::sync::{Notify, OwnedSemaphorePermit, mpsc};

use crate::{
    callbacks::PythonTool,
    config::{AgentConfig, RunOptions, invalid},
    errors::{agent_error, failure},
};

pub struct Registry {
    pub runtime: Option<ProviderRuntime>,
    pub runs: BTreeMap<String, Weak<RunState>>,
}

pub struct RunState {
    pub context: RunContext,
    pub finished: AtomicBool,
    pub terminal: Mutex<Option<Value>>,
    pub events: tokio::sync::Mutex<mpsc::Receiver<String>>,
    pub notify: Notify,
    pub polling: AtomicBool,
    pub delivered: AtomicBool,
    pub checkpoint_id: Option<CheckpointId>,
}

pub fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl RunState {
    pub async fn wait_finished(&self) {
        loop {
            let notification = self.notify.notified();
            if self.finished.load(Ordering::Acquire) {
                return;
            }
            notification.await;
        }
    }
}

struct DriverGuard {
    state: Arc<RunState>,
    permit: Option<OwnedSemaphorePermit>,
}
impl Drop for DriverGuard {
    fn drop(&mut self) {
        let mut terminal = lock(&self.state.terminal);
        if terminal.is_none() {
            self.state.context.cancellation().cancel();
            *terminal = Some(failure(
                "runifold.invocation",
                "Internal",
                "Unknown",
                "Native execution ended without a terminal result",
                &self.state.context,
            ));
        }
        drop(terminal);
        drop(self.permit.take());
        self.state.finished.store(true, Ordering::Release);
        self.state.notify.notify_waiters();
    }
}

struct PollGuard {
    state: Arc<RunState>,
    completed: bool,
}
impl PollGuard {
    fn complete(&mut self) {
        self.completed = true;
    }
}
impl Drop for PollGuard {
    fn drop(&mut self) {
        self.state.polling.store(false, Ordering::Release);
        if !self.completed && !self.state.finished.load(Ordering::Acquire) {
            self.state.context.cancellation().cancel();
        }
    }
}

#[pyclass(name = "Run", module = "runifold._native", frozen)]
pub struct NativeRun {
    pub state: Arc<RunState>,
}

impl Drop for NativeRun {
    fn drop(&mut self) {
        self.state.context.cancellation().cancel();
    }
}

#[pymethods]
impl NativeRun {
    #[getter]
    fn run_id(&self) -> String {
        self.state.context.run_id().to_string()
    }
    #[getter]
    fn checkpoint_id(&self) -> Option<String> {
        self.state.checkpoint_id.map(|id| id.to_string())
    }
    #[getter]
    fn finished(&self) -> bool {
        self.state.finished.load(Ordering::Acquire)
    }
    fn cancel(&self) -> bool {
        if self.state.finished.load(Ordering::Acquire) {
            return false;
        }
        self.state.context.cancellation().cancel();
        true
    }
    fn usage(&self) -> String {
        json!(self.state.context.budget().usage()).to_string()
    }
    fn acknowledge_terminal(&self) {
        self.state.delivered.store(true, Ordering::Release);
    }

    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if self.state.polling.swap(true, Ordering::AcqRel) {
            return Err(PyRuntimeError::new_err(
                "Only one consumer may poll a Run at a time",
            ));
        }
        let mut guard = PollGuard {
            state: self.state.clone(),
            completed: false,
        };
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let state = guard.state.clone();
            let mut receiver = state.events.lock().await;
            let event = if state.context.cancellation().is_cancelled() {
                None
            } else {
                tokio::select! {
                    biased;
                    _ = state.context.cancellation().cancelled() => None,
                    event = receiver.recv() => event,
                }
            };
            let result = if let Some(event) = event {
                Some(event)
            } else {
                state.wait_finished().await;
                if state.delivered.load(Ordering::Acquire) {
                    None
                } else {
                    lock(&state.terminal).as_ref().map(Value::to_string)
                }
            };
            guard.complete();
            Ok(result)
        })
    }

    fn wait_closed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            state.wait_finished().await;
            Ok(())
        })
    }
}

pub struct StartExecution {
    pub runtime: ProviderRuntime,
    pub config: AgentConfig,
    pub options: RunOptions,
    pub tools: Vec<Arc<PythonTool>>,
    pub memory_effects: Arc<InMemoryEffectStore>,
    pub input: String,
    pub state: Arc<RunState>,
    pub events: mpsc::Sender<String>,
}

pub fn root_context(config: &AgentConfig, options: &RunOptions) -> PyResult<RunContext> {
    let mut capabilities = CapabilitySet::new();
    if let Some(names) = &options.allowed_tools
        && names
            .iter()
            .any(|name| !config.tools.iter().any(|tool| &tool.name == name))
    {
        return Err(invalid("allowed_tools contains an unregistered Tool"));
    }
    for descriptor in &config.tools {
        if options
            .allowed_tools
            .as_ref()
            .is_none_or(|names| names.contains(&descriptor.name))
        {
            capabilities.grant(descriptor.capability());
        }
    }
    let mut run = RunContext::root(BudgetTracker::new(options.budget), capabilities);
    if let Some(timeout) = options.timeout_duration()? {
        run = run.with_deadline(
            Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| invalid("Timeout exceeds the monotonic clock range"))?,
        );
    }
    Ok(run)
}

fn outcome(value: runifold::AgentOutcome, state: &RunState, structured: bool) -> Value {
    let parsed = if structured {
        match value.response.structured::<Value>() {
            Ok(parsed) => Some(parsed),
            Err(_) => {
                return failure(
                    "runifold.structured_output",
                    "InvalidOutput",
                    "Unsafe",
                    "Structured response is missing, refused, incomplete or invalid JSON",
                    &state.context,
                );
            }
        }
    } else {
        None
    };
    json!({"type":"outcome", "run_id":state.context.run_id().to_string(),
        "checkpoint_id":state.checkpoint_id.map(|id| id.to_string()),
        "text":value.text(), "parsed":parsed, "outcome":value})
}

async fn startup<F: std::future::Future>(future: F, run: &RunContext) -> Result<F::Output, Value> {
    tokio::select! {
        biased;
        _ = run.cancellation().cancelled() => Err(failure("runifold.cancelled", "Cancelled", "Safe", "Local startup cancelled before model dispatch", run)),
        value = future => Ok(value),
    }
}

async fn execute(job: &StartExecution) -> Value {
    if job.state.context.cancellation().is_cancelled() {
        return failure(
            "runifold.cancelled",
            "Cancelled",
            "Safe",
            "Run cancelled before model dispatch",
            &job.state.context,
        );
    }
    let checkpoint = if let Some(path) = &job.options.checkpoint_path {
        let path = path.clone();
        let store = match startup(
            tokio::task::spawn_blocking(move || runifold::sqlite::SqliteStore::open(path)),
            &job.state.context,
        )
        .await
        {
            Ok(Ok(Ok(store))) => Arc::new(store),
            Err(error) => return error,
            _ => {
                return failure(
                    "runifold.checkpoint",
                    "Checkpoint",
                    "Unknown",
                    "Could not open the SQLite checkpoint store",
                    &job.state.context,
                );
            }
        };
        Some((
            AgentCheckpoint::existing(
                job.state
                    .checkpoint_id
                    .expect("validated checkpoint identity"),
                store.clone(),
            ),
            store,
        ))
    } else {
        None
    };
    let mut builder = job
        .runtime
        .agent(job.config.name.clone())
        .system(job.config.system.clone());
    for tool in &job.tools {
        builder = builder.shared_tool(tool.clone() as Arc<dyn Tool>);
    }
    if let Some(schema) = &job.config.output_schema {
        builder = builder.output_format(OutputFormat::JsonSchema {
            name: job
                .config
                .output_name
                .clone()
                .unwrap_or_else(|| "output".into()),
            schema: schema.clone(),
            strict: true,
        });
    }
    if let Some(tokens) = job.config.max_output_tokens {
        builder = builder.max_output_tokens(tokens);
    }
    builder = builder.effect_executor(match &checkpoint {
        Some((_, store)) => EffectExecutor::new(store.clone()),
        None => EffectExecutor::new(job.memory_effects.clone()),
    });
    let agent = match builder.build() {
        Ok(agent) => agent,
        Err(_) => {
            return failure(
                "RF-AGENT-001",
                "InvalidInput",
                "Safe",
                "Invalid Agent or Tool configuration; inspect declared schemas and names",
                &job.state.context,
            );
        }
    };
    if job.options.resume {
        let handle = checkpoint
            .as_ref()
            .expect("validated resume store")
            .0
            .clone();
        let loaded = startup(
            tokio::task::spawn_blocking(move || handle.load()),
            &job.state.context,
        )
        .await;
        let saved = match loaded {
            Ok(Ok(Ok((_, saved)))) => saved,
            Err(error) => return error,
            _ => {
                return failure(
                    "runifold.checkpoint",
                    "Checkpoint",
                    "Unknown",
                    "Checkpoint is missing, incompatible or invalid",
                    &job.state.context,
                );
            }
        };
        if let Err(error) = job.state.context.budget().try_consume(saved.usage) {
            return agent_error(&runifold::AgentError::Budget(error), &job.state.context);
        }
        let policy = if job.options.retry_interrupted {
            ResumePolicy::RetryInterruptedTurn
        } else {
            ResumePolicy::RejectAmbiguous
        };
        return match agent
            .resume(
                &checkpoint.as_ref().expect("validated resume store").0,
                &job.state.context,
                policy,
            )
            .await
        {
            Ok(value) => outcome(value, &job.state, job.config.output_schema.is_some()),
            Err(error) => agent_error(&error, &job.state.context),
        };
    }
    if let Some((handle, _)) = &checkpoint {
        return match agent
            .run_checkpointed(job.input.clone(), &job.state.context, handle)
            .await
        {
            Ok(value) => outcome(value, &job.state, job.config.output_schema.is_some()),
            Err(error) => agent_error(&error, &job.state.context),
        };
    }
    if job.options.stream {
        let mut stream = agent.stream(job.input.clone(), &job.state.context);
        while let Some(event) = stream.next().await {
            match event {
                Ok(AgentStreamEvent::Completed { outcome: value }) => {
                    return outcome(value, &job.state, job.config.output_schema.is_some());
                }
                Ok(event) => {
                    let value = json!({"type":"event", "event":event}).to_string();
                    tokio::select! {
                        _ = job.state.context.cancellation().cancelled() => {},
                        result = job.events.send(value) => if result.is_err() { job.state.context.cancellation().cancel(); },
                    }
                }
                Err(error) => return agent_error(&error, &job.state.context),
            }
        }
        failure(
            "runifold.cancelled",
            "Cancelled",
            "Unknown",
            "Local stream closed; remote work or callback effects may still be unknown",
            &job.state.context,
        )
    } else {
        match agent.run(job.input.clone(), &job.state.context).await {
            Ok(value) => outcome(value, &job.state, job.config.output_schema.is_some()),
            Err(error) => agent_error(&error, &job.state.context),
        }
    }
}

pub fn start(job: StartExecution, permit: OwnedSemaphorePermit) {
    pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
        // Keep ownership of the permit and terminal notification even on panic.
        let state = job.state.clone();
        let _guard = DriverGuard { state: state.clone(), permit: Some(permit) };
        // Let native cancellation produce its own classification/diagnostics.
        // The host timer signals cancellation for startup IO and callbacks,
        // which may otherwise have no deadline wakeup of their own.
        let deadline = state.context.deadline();
        let value = {
            let work = execute(&job);
            tokio::pin!(work);
            tokio::select! {
                biased;
                value = &mut work => value,
                _ = async { match deadline { Some(at) => tokio::time::sleep(at.saturating_duration_since(Instant::now())).await, None => std::future::pending().await } } => {
                    state.context.cancellation().cancel();
                    let mut value = work.await;
                    if value["type"] == "error" && value["error"]["kind"] == "Cancelled" {
                        value["error"]["kind"] = json!("DeadlineExceeded");
                        value["error"]["code"] = json!("runifold.deadline_exceeded");
                        value["error"]["diagnostics"]["sdk.deadline_triggered"] = json!(true);
                    }
                    value
                },
                _ = state.context.cancellation().cancelled() => work.await,
            }
        };
        *lock(&state.terminal) = Some(value);
    });
}
