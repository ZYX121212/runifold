use std::{future::Future, pin::Pin, time::Duration};

use futures_core::Stream;
use futures_util::{
    StreamExt,
    future::{Either, select},
};
use runifold_core::{CancellationToken, Instant, InvocationId, RunContext, RunId};

use crate::{
    ModelCapabilities, ModelError, ModelErrorKind, ModelRef, ModelRequest, ModelResponse,
    ModelStreamAccumulator, ModelStreamEvent,
};

/// A boxed, sendable future returned by a model implementation.
#[cfg(not(target_arch = "wasm32"))]
pub type ModelFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A boxed future returned by a model implementation on single-threaded WASM.
#[cfg(target_arch = "wasm32")]
pub type ModelFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// A provider-neutral stream of canonical model events.
#[cfg(not(target_arch = "wasm32"))]
pub type ModelEventStream =
    Pin<Box<dyn Stream<Item = Result<ModelStreamEvent, ModelError>> + Send + 'static>>;

/// A provider-neutral model stream on single-threaded WASM.
#[cfg(target_arch = "wasm32")]
pub type ModelEventStream =
    Pin<Box<dyn Stream<Item = Result<ModelStreamEvent, ModelError>> + 'static>>;

/// Execution scope for one model invocation.
///
/// It deliberately carries lifecycle data rather than provider configuration.
/// Adapters must observe cancellation and should translate the deadline into
/// their transport's timeout mechanism.
#[derive(Clone, Debug)]
pub struct ModelCallContext {
    invocation_id: InvocationId,
    run_id: Option<RunId>,
    deadline: Option<Instant>,
    cancellation: CancellationToken,
}

impl ModelCallContext {
    /// Creates a standalone invocation context.
    pub fn new() -> Self {
        Self {
            invocation_id: InvocationId::new(),
            run_id: None,
            deadline: None,
            cancellation: CancellationToken::new(),
        }
    }

    /// Creates an invocation scoped beneath a run.
    pub fn for_run(run: &RunContext) -> Self {
        Self {
            invocation_id: InvocationId::new(),
            run_id: Some(run.run_id()),
            deadline: run.deadline(),
            cancellation: run.cancellation().child_token(),
        }
    }

    /// Returns this model invocation's identity.
    pub const fn invocation_id(&self) -> InvocationId {
        self.invocation_id
    }

    /// Returns the owning run identity, when invoked inside a run.
    pub const fn run_id(&self) -> Option<RunId> {
        self.run_id
    }

    /// Returns the effective deadline.
    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Returns the remaining time before the deadline.
    pub fn remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    /// Returns the invocation's hierarchical cancellation token.
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Sets a deadline, retaining an existing earlier deadline.
    #[must_use]
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(
            self.deadline
                .map_or(deadline, |current| current.min(deadline)),
        );
        self
    }

    /// Replaces the cancellation root for an externally scoped invocation.
    ///
    /// The invocation receives a child token so provider attempts cannot
    /// cancel their caller's broader operation.
    #[must_use]
    pub fn with_cancellation(mut self, cancellation: &CancellationToken) -> Self {
        self.cancellation = cancellation.child_token();
        self
    }

    /// Creates a distinct provider-attempt context under the same logical
    /// invocation scope.
    ///
    /// The attempt receives a new invocation identity while inheriting the
    /// run, effective deadline, and hierarchical cancellation.
    #[must_use]
    pub fn child_attempt(&self) -> Self {
        Self {
            invocation_id: InvocationId::new(),
            run_id: self.run_id,
            deadline: self.deadline,
            cancellation: self.cancellation.child_token(),
        }
    }
}

impl Default for ModelCallContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Object-safe boundary implemented by model provider adapters.
///
/// Streaming is the source of truth. [`Model::invoke`] is a canonical
/// collector over that stream, so streamed and non-streamed calls cannot
/// silently develop different normalization behavior.
pub trait Model: Send + Sync {
    /// Resolves capabilities for a provider-qualified model.
    fn capabilities<'a>(
        &'a self,
        model: &'a ModelRef,
    ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>>;

    /// Opens a canonical event stream for a request.
    fn stream(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>>;

    /// Invokes the model and reconstructs its terminal response.
    fn invoke(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelResponse, ModelError>> {
        Box::pin(async move {
            let cancellation = context.cancellation().clone();
            let stream_future = self.stream(request, context);
            let mut stream =
                match select(Box::pin(cancellation.cancelled()), Box::pin(stream_future)).await {
                    Either::Left(_) => return Err(cancelled_error()),
                    Either::Right((result, _)) => result?,
                };

            let mut accumulator = ModelStreamAccumulator::new();
            loop {
                let next = stream.next();
                match select(Box::pin(cancellation.cancelled()), Box::pin(next)).await {
                    Either::Left(_) => return Err(cancelled_error()),
                    Either::Right((Some(event), _)) => {
                        if let Some(response) = accumulator.push(event?)? {
                            return Ok(response);
                        }
                    }
                    Either::Right((None, _)) => {
                        return Err(ModelError::local(
                            ModelErrorKind::Protocol,
                            "model stream ended before a terminal response event",
                        ));
                    }
                }
            }
        })
    }
}

/// Stable provider identity carried by a concrete model adapter.
///
/// Implementing this trait in addition to [`Model`] lets higher runtime layers
/// construct provider-qualified model references and attach Agent, routing,
/// retry, circuit-breaker, observability, budget, and workflow behavior
/// without provider-specific orchestration code.
pub trait ProviderModel: Model {
    /// Returns the canonical provider namespace used by this adapter.
    fn provider(&self) -> &str;

    /// Qualifies one model name with this adapter's provider identity.
    fn model_ref(&self, model: impl Into<String>) -> ModelRef
    where
        Self: Sized,
    {
        ModelRef::new(self.provider(), model)
    }
}

fn cancelled_error() -> ModelError {
    ModelError::local(ModelErrorKind::Cancelled, "model invocation was cancelled")
}
