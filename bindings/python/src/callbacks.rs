//! Callback adaptation only; dispatch and authorization remain in ToolRegistry.
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

use pyo3::prelude::*;
use pyo3_async_runtimes::TaskLocals;
use runifold::{Tool, ToolContext, ToolDescriptor, ToolError, ToolErrorKind, ToolOutput};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[pyclass(name = "ToolContext", module = "runifold._native", frozen)]
pub struct PythonToolContext {
    pub inner: ToolContext,
}
#[pymethods]
impl PythonToolContext {
    #[getter]
    fn run_id(&self) -> String {
        self.inner.run_id().to_string()
    }
    #[getter]
    fn invocation_id(&self) -> String {
        self.inner.invocation_id().to_string()
    }
    #[getter]
    fn cancelled(&self) -> bool {
        self.inner.cancellation().is_cancelled()
    }
    #[getter]
    fn remaining_seconds(&self) -> Option<f64> {
        self.inner.remaining().map(|d| d.as_secs_f64())
    }
}

// A synchronous Python worker retains this lease until it actually returns,
// including when its caller has stopped waiting. It is not exposed to user code.
#[pyclass(name = "_CallbackLease", module = "runifold._native", frozen)]
pub struct CallbackLease {
    permit: Mutex<Option<OwnedSemaphorePermit>>,
}
#[pymethods]
impl CallbackLease {
    fn release(&self) {
        let permit = {
            self.permit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
        };
        drop(permit);
    }
}

struct CancelPythonFuture(Option<Py<PyAny>>);
impl Drop for CancelPythonFuture {
    fn drop(&mut self) {
        if let Some(future) = self.0.take() {
            // concurrent.futures.Future.cancel is thread-safe. Its chain cancels
            // the Python Task on its owning loop; do not call Task.cancel here.
            let _ = Python::try_attach(|py| {
                let _ = future.bind(py).call_method0("cancel");
            });
        }
    }
}

pub struct PythonTool {
    pub descriptor: ToolDescriptor,
    pub dispatcher: Arc<Py<PyAny>>,
    pub locals: TaskLocals,
    pub slots: Arc<Semaphore>,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum CallbackReply {
    Output { output: Value },
    Error { error: ToolError },
}

fn callback_error() -> ToolError {
    ToolError::local(
        ToolErrorKind::Execution,
        "Python Tool callback failed; inspect application callback handling",
    )
}

impl Tool for PythonTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        input: Value,
        context: ToolContext,
    ) -> Pin<Box<dyn Future<Output = Result<ToolOutput, ToolError>> + Send + '_>> {
        Box::pin(async move {
            let permit = self
                .slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| callback_error())?;
            let input = serde_json::to_string(&input).map_err(|_| callback_error())?;
            let (future, _cancel) = Python::attach(|py| -> PyResult<_> {
                let lease = Py::new(
                    py,
                    CallbackLease {
                        permit: Mutex::new(Some(permit)),
                    },
                )?;
                let context = Py::new(py, PythonToolContext { inner: context })?;
                let coroutine = self.dispatcher.bind(py).call1((
                    &self.descriptor.name,
                    input,
                    context,
                    lease,
                ))?;
                let asyncio = py.import("asyncio")?;
                let copied = self.locals.context(py).call_method0("copy")?;
                let scheduled = match copied.call_method1(
                    "run",
                    (
                        asyncio.getattr("run_coroutine_threadsafe")?,
                        &coroutine,
                        self.locals.event_loop(py),
                    ),
                ) {
                    Ok(future) => future,
                    Err(error) => {
                        let _ = coroutine.call_method0("close");
                        return Err(error);
                    }
                };
                let cancel = CancelPythonFuture(Some(scheduled.clone().unbind()));
                let waiter = py
                    .import("runifold._tools")?
                    .getattr("_await_concurrent")?
                    .call1((scheduled,))?;
                Ok((
                    pyo3_async_runtimes::into_future_with_locals(&self.locals, waiter)?,
                    cancel,
                ))
            })
            .map_err(|_| callback_error())?;
            let reply = future.await.map_err(|_| callback_error())?;
            let reply = Python::attach(|py| reply.extract::<String>(py)).map_err(|_| {
                ToolError::local(
                    ToolErrorKind::InvalidOutput,
                    "Python callback must return a serialized Tool result",
                )
            })?;
            match serde_json::from_str::<CallbackReply>(&reply).map_err(|_| {
                ToolError::local(
                    ToolErrorKind::InvalidOutput,
                    "Python callback returned an invalid canonical Tool result",
                )
            })? {
                CallbackReply::Output { output } => {
                    let structured = output.get("structured_content").cloned();
                    let mut result: ToolOutput = serde_json::from_value(output).map_err(|_| {
                        ToolError::local(
                            ToolErrorKind::InvalidOutput,
                            "Python callback returned invalid canonical content",
                        )
                    })?;
                    // Preserve explicit JSON null separately from an absent
                    // structured channel; Option<Value> alone cannot do this.
                    result.structured_content = structured;
                    Ok(result)
                }
                CallbackReply::Error { error } => Err(error),
            }
        })
    }
}
