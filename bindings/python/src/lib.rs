//! Native Python adapter over the canonical Runifold runtime.
mod callbacks;
mod config;
mod errors;
mod execution;

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::Duration,
};

use pyo3::{
    exceptions::{PyRuntimeError, PyTimeoutError},
    prelude::*,
};
use runifold::{CheckpointId, ModelScheduler, ProviderRuntime, effect::InMemoryEffectStore};
use runifold_testkit::ScriptedModel;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::{Notify, Semaphore, mpsc};

use callbacks::PythonTool;
use config::{AgentConfig, ClientConfig, RunOptions, ScriptedProvider, invalid};
use execution::{NativeRun, Registry, RunState, StartExecution, lock};

#[pyclass(name = "Runtime", module = "runifold._native", frozen)]
struct NativeRuntime {
    registry: Mutex<Registry>,
    scheduler: ModelScheduler,
    runs: Arc<Semaphore>,
    callbacks: Arc<Semaphore>,
    max_runs: u32,
    max_callbacks: u32,
    scripted: Option<ScriptedModel>,
    policy: serde_json::Value,
    limits: serde_json::Value,
}

impl NativeRuntime {
    fn create(
        config: &ClientConfig,
        runtime: ProviderRuntime,
        scheduler: ModelScheduler,
        scripted: Option<ScriptedModel>,
    ) -> Self {
        let policy = json!(runtime.profile().diagnostic_snapshot());
        let limits = json!({"model_concurrency": config.model_concurrency, "request_queue": config.request_queue,
            "run_concurrency": config.run_concurrency, "tool_concurrency": config.tool_concurrency});
        Self {
            registry: Mutex::new(Registry {
                runtime: Some(runtime),
                runs: BTreeMap::new(),
            }),
            scheduler,
            runs: Arc::new(Semaphore::new(config.run_concurrency)),
            callbacks: Arc::new(Semaphore::new(config.tool_concurrency)),
            max_runs: u32::try_from(config.run_concurrency)
                .expect("validated concurrency fits u32"),
            max_callbacks: u32::try_from(config.tool_concurrency)
                .expect("validated concurrency fits u32"),
            scripted,
            policy,
            limits,
        }
    }
}

#[pymethods]
impl NativeRuntime {
    #[new]
    fn new(config: &str) -> PyResult<Self> {
        let config: ClientConfig =
            serde_json::from_str(config).map_err(|_| invalid("Invalid Client configuration"))?;
        config.validate()?;
        let _entered = pyo3_async_runtimes::tokio::get_runtime().enter();
        let (runtime, scheduler) = config.provider_runtime()?;
        Ok(Self::create(&config, runtime, scheduler, None))
    }

    #[staticmethod]
    fn scripted(config: &str, scripts: &str, delay_ms: u64) -> PyResult<Self> {
        let config: ClientConfig =
            serde_json::from_str(config).map_err(|_| invalid("Invalid testing configuration"))?;
        let scripts = serde_json::from_str(scripts)
            .map_err(|_| invalid("Invalid canonical Model scripts"))?;
        if delay_ms > 60_000 {
            return Err(invalid("Testing delay must be at most 60000 milliseconds"));
        }
        let model = config::scripted_model(scripts);
        let (runtime, scheduler) = config.runtime(ScriptedProvider {
            model: model.clone(),
            delay: Duration::from_millis(delay_ms),
        })?;
        Ok(Self::create(&config, runtime, scheduler, Some(model)))
    }

    fn start(
        &self,
        py: Python<'_>,
        config: &str,
        input: String,
        options: &str,
        dispatcher: Py<PyAny>,
    ) -> PyResult<NativeRun> {
        let config: AgentConfig =
            serde_json::from_str(config).map_err(|_| invalid("Invalid Agent configuration"))?;
        let options: RunOptions =
            serde_json::from_str(options).map_err(|_| invalid("Invalid Run options or budget"))?;
        if options.stream && options.checkpoint_path.is_some() {
            return Err(invalid(
                "Checkpointed calls use run_checkpointed/resume; native checkpoint streaming is not exposed",
            ));
        }
        if options.resume && (options.checkpoint_path.is_none() || options.checkpoint_id.is_none())
        {
            return Err(invalid("Resume requires checkpoint_path and checkpoint_id"));
        }
        if options.checkpoint_path.is_none() && options.checkpoint_id.is_some() {
            return Err(invalid("checkpoint_id requires checkpoint_path"));
        }
        let checkpoint_id = if options.checkpoint_path.is_some() {
            Some(match &options.checkpoint_id {
                Some(id) => id
                    .parse::<CheckpointId>()
                    .map_err(|_| invalid("Invalid checkpoint identity"))?,
                None => CheckpointId::new(),
            })
        } else {
            None
        };
        let context = execution::root_context(&config, &options)?;
        let locals = pyo3_async_runtimes::tokio::get_current_locals(py)?;
        let dispatcher = Arc::new(dispatcher);
        let tools = config
            .tools
            .iter()
            .map(|descriptor| {
                Arc::new(PythonTool {
                    descriptor: descriptor.clone(),
                    dispatcher: dispatcher.clone(),
                    locals: locals.clone(),
                    slots: self.callbacks.clone(),
                })
            })
            .collect();
        let permit = self.runs.clone().try_acquire_owned().map_err(|_| {
            errors::NativeError::new_err(
                errors::failure(
                    "RF-PROVIDER-010",
                    "AdmissionDenied",
                    "Safe",
                    "Client active-run limit reached; wait for an existing Run to close",
                    &context,
                )
                .to_string(),
            )
        })?;
        let (events, receiver) = mpsc::channel(32);
        let state = Arc::new(RunState {
            context,
            finished: AtomicBool::new(false),
            terminal: Mutex::new(None),
            events: tokio::sync::Mutex::new(receiver),
            notify: Notify::new(),
            polling: AtomicBool::new(false),
            delivered: AtomicBool::new(false),
            checkpoint_id,
        });
        let runtime = {
            let mut registry = lock(&self.registry);
            let runtime = registry
                .runtime
                .as_ref()
                .ok_or_else(|| {
                    errors::NativeError::new_err(
                        errors::failure(
                            "runifold.client_closed",
                            "InvalidInput",
                            "Safe",
                            "Client is closed",
                            &state.context,
                        )
                        .to_string(),
                    )
                })?
                .clone();
            registry.runs.retain(|_, run| run.strong_count() > 0);
            registry
                .runs
                .insert(state.context.run_id().to_string(), Arc::downgrade(&state));
            runtime
        };
        execution::start(
            StartExecution {
                runtime,
                config,
                options,
                tools,
                memory_effects: Arc::new(InMemoryEffectStore::new()),
                input,
                state: state.clone(),
                events,
            },
            permit,
        );
        Ok(NativeRun { state })
    }

    fn close(&self) {
        let (runtime, runs) = {
            let mut registry = lock(&self.registry);
            let runtime = registry.runtime.take();
            let runs = registry
                .runs
                .values()
                .filter_map(std::sync::Weak::upgrade)
                .collect::<Vec<_>>();
            (runtime, runs)
        };
        for run in runs {
            if !run.finished.load(std::sync::atomic::Ordering::Acquire) {
                run.context.cancellation().cancel();
            }
        }
        drop(runtime);
    }

    fn wait_closed<'py>(
        &self,
        py: Python<'py>,
        timeout: f64,
        callbacks: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        if !timeout.is_finite() || timeout <= 0.0 || timeout > 3600.0 {
            return Err(invalid(
                "Close timeout must be finite and between zero and one hour",
            ));
        }
        if lock(&self.registry).runtime.is_some() {
            return Err(PyRuntimeError::new_err(
                "Close the Client before waiting for shutdown",
            ));
        }
        let runs = self.runs.clone();
        let tools = self.callbacks.clone();
        let max_runs = self.max_runs;
        let max_tools = self.max_callbacks;
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            tokio::time::timeout(Duration::from_secs_f64(timeout), async move {
                let _runs = runs.acquire_many_owned(max_runs).await.map_err(|_| PyRuntimeError::new_err("Run capacity closed unexpectedly"))?;
                let _tools = if callbacks { Some(tools.acquire_many_owned(max_tools).await.map_err(|_| PyRuntimeError::new_err("Callback capacity closed unexpectedly"))?) } else { None };
                Ok(())
            }).await.map_err(|_| PyTimeoutError::new_err("Client shutdown timed out; running synchronous callbacks may still complete effects"))?
        })
    }

    fn snapshot(&self) -> String {
        let state = self.scheduler.snapshot();
        json!({"active_runs": self.max_runs as usize - self.runs.available_permits(),
            "active_callbacks":self.max_callbacks as usize - self.callbacks.available_permits(),
            "closed":lock(&self.registry).runtime.is_none(), "policy": self.policy, "limits": self.limits,
            "model": {"queued":state.queued,"admitted":state.admitted,"rejected":state.rejected,"cancelled":state.cancelled,"expired":state.expired,"unknown_usage":state.unknown_usage,
                "pools":state.pools.iter().map(|pool| json!({"pool":pool.pool.as_str(),"active":pool.active,"requests":pool.requests,"reserved_tokens":pool.reserved_tokens.to_string(),"settled_tokens":pool.settled_tokens.to_string()})).collect::<Vec<_>>()}}).to_string()
    }

    fn recorded_requests(&self) -> PyResult<String> {
        self.scripted
            .as_ref()
            .map(|model| json!(model.recorded_requests()).to_string())
            .ok_or_else(|| {
                invalid(
                    "Request recording is only available for an explicit scripted testing Client",
                )
            })
    }
}

impl Drop for NativeRuntime {
    fn drop(&mut self) {
        self.close();
    }
}

#[pymodule]
fn _native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__version__", env!("CARGO_PKG_VERSION"))?;
    let lock_hash = Sha256::digest(include_bytes!("../Cargo.lock"))
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    module.add("__lock_sha256__", lock_hash)?;
    module.add("NativeError", module.py().get_type::<errors::NativeError>())?;
    module.add_class::<NativeRuntime>()?;
    module.add_class::<NativeRun>()?;
    module.add_class::<callbacks::PythonToolContext>()?;
    module.add_class::<callbacks::CallbackLease>()?;
    Ok(())
}
