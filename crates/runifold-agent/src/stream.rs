use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll},
};

use futures_core::Stream;
use runifold_core::Usage;
use runifold_model::{ModelStreamEvent, ToolCall};
use serde::{Deserialize, Serialize};

use crate::{AgentError, AgentFuture, AgentOutcome, TerminalRequirementFailure};

/// The callable boundary represented by an Agent stream event.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum CallableKind {
    /// A locally registered Tool.
    Tool,
    /// A child Agent route.
    Agent,
}

/// One real-time event from the canonical Agent execution loop.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[non_exhaustive]
pub enum AgentStreamEvent {
    /// The Agent execution started.
    Started {
        /// Stable local Agent name.
        agent: String,
    },
    /// A model turn started.
    TurnStarted {
        /// One-based turn number.
        turn: u32,
    },
    /// One provider-neutral model streaming event.
    Model {
        /// One-based owning turn.
        turn: u32,
        /// Canonical event accepted by the model stream accumulator.
        event: ModelStreamEvent,
    },
    /// One dynamic context source completed retrieval.
    ContextRetrieved {
        /// Operator-visible source name.
        source: String,
        /// Number of documents injected into the transcript.
        documents: usize,
    },
    /// A Tool or child Agent call started.
    CallableStarted {
        /// One-based owning turn.
        turn: u32,
        /// Callable boundary.
        kind: CallableKind,
        /// Canonical model-emitted call.
        call: ToolCall,
    },
    /// A Tool or child Agent call reached a recoverable terminal result.
    CallableCompleted {
        /// One-based owning turn.
        turn: u32,
        /// Callable boundary.
        kind: CallableKind,
        /// Model-emitted call identity.
        call_id: String,
        /// Model-facing callable name.
        name: String,
        /// Whether execution produced a successful output.
        success: bool,
    },
    /// Shared run-tree resource usage changed.
    UsageUpdated {
        /// Latest cumulative usage snapshot.
        usage: Usage,
    },
    /// The Agent reached a terminal model response.
    Completed {
        /// Complete canonical outcome.
        outcome: AgentOutcome,
    },
    /// An invalid terminal candidate scheduled a bounded repair turn.
    TerminalRepairScheduled {
        /// One-based repair attempt number.
        attempt: u32,
        /// Safe reason the candidate was rejected.
        failure: TerminalRequirementFailure,
    },
}

pub(crate) trait AgentObserver: Send + Sync {
    fn emit(&self, event: AgentStreamEvent);

    fn backpressured(&self) -> bool {
        false
    }
}

#[derive(Debug)]
pub(crate) struct NoopObserver;

impl AgentObserver for NoopObserver {
    fn emit(&self, _event: AgentStreamEvent) {}
}

#[derive(Clone, Debug, Default)]
pub(crate) struct BufferedObserver {
    events: Arc<Mutex<VecDeque<AgentStreamEvent>>>,
}

impl BufferedObserver {
    pub(crate) fn events(&self) -> Arc<Mutex<VecDeque<AgentStreamEvent>>> {
        self.events.clone()
    }
}

impl AgentObserver for BufferedObserver {
    fn emit(&self, event: AgentStreamEvent) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_back(event);
    }

    fn backpressured(&self) -> bool {
        true
    }
}

pub(crate) async fn emit_agent_event(observer: &dyn AgentObserver, event: AgentStreamEvent) {
    observer.emit(event);
    if observer.backpressured() {
        YieldOnce::new().await;
    }
}

struct YieldOnce {
    yielded: bool,
}

impl YieldOnce {
    const fn new() -> Self {
        Self { yielded: false }
    }
}

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// A borrow-scoped stream that drives the canonical Agent loop when polled.
#[must_use = "streams do nothing unless polled"]
pub struct AgentEventStream<'a> {
    execution: Option<AgentFuture<'a, Result<AgentOutcome, AgentError>>>,
    events: Arc<Mutex<VecDeque<AgentStreamEvent>>>,
    failure: Option<AgentError>,
    finished: bool,
}

impl<'a> AgentEventStream<'a> {
    pub(crate) fn new(
        execution: AgentFuture<'a, Result<AgentOutcome, AgentError>>,
        events: Arc<Mutex<VecDeque<AgentStreamEvent>>>,
    ) -> Self {
        Self {
            execution: Some(execution),
            events,
            failure: None,
            finished: false,
        }
    }

    fn events(&self) -> MutexGuard<'_, VecDeque<AgentStreamEvent>> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn pop_event(&self) -> Option<AgentStreamEvent> {
        self.events().pop_front()
    }
}

impl Stream for AgentEventStream<'_> {
    type Item = Result<AgentStreamEvent, AgentError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(event) = this.pop_event() {
            return Poll::Ready(Some(Ok(event)));
        }
        if let Some(execution) = this.execution.as_mut() {
            match execution.as_mut().poll(context) {
                Poll::Pending => {
                    return this
                        .pop_event()
                        .map_or(Poll::Pending, |event| Poll::Ready(Some(Ok(event))));
                }
                Poll::Ready(Ok(_outcome)) => {
                    this.execution = None;
                }
                Poll::Ready(Err(error)) => {
                    this.execution = None;
                    this.failure = Some(error);
                }
            }
        }
        if let Some(event) = this.pop_event() {
            return Poll::Ready(Some(Ok(event)));
        }
        if let Some(error) = this.failure.take() {
            return Poll::Ready(Some(Err(error)));
        }
        if this.execution.is_none() {
            this.finished = true;
        }
        if this.finished {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

impl std::fmt::Debug for AgentEventStream<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentEventStream")
            .field("queued_events", &self.events().len())
            .field("has_execution", &self.execution.is_some())
            .field("has_failure", &self.failure.is_some())
            .field("finished", &self.finished)
            .finish()
    }
}
