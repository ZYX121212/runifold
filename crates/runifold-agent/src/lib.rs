//! Structured model-tool agent runtime for Runifold.

mod agent;
mod builder;
mod checkpoint;
mod descriptor;
mod error;
mod gateway;
mod middleware;
mod outcome;
mod stream;
mod structured;

pub use agent::{Agent, AgentConfig, AgentFuture, ToolErrorPolicy};
pub use builder::{AgentBuildError, AgentBuilder};
pub use checkpoint::{AgentCheckpoint, AgentCheckpointPhase, AgentCheckpointState, ResumePolicy};
pub use descriptor::AgentDescriptor;
pub use error::AgentError;
pub use gateway::{
    AgentGateway, AgentRegistrationError, AgentRoute, GatewayError, GatewayErrorKind,
};
pub use middleware::{
    DelegationRequest, GatewayDecision, GatewayFuture, GatewayMiddleware, GatewayNext,
    GatewayPolicy, PolicyMiddleware,
};
pub use outcome::{AgentOutcome, StructuredAgentOutcome};
pub use stream::{AgentEventStream, AgentStreamEvent, CallableKind};
pub use structured::{StructuredAgent, StructuredAgentError};
