//! Typed and capability-gated tool execution for Runifold.

mod context;
mod descriptor;
mod error;
mod function;
mod registry;
mod state;
mod tool;

pub use context::ToolContext;
pub use descriptor::ToolDescriptor;
pub use error::{IntoToolError, ToolError, ToolErrorKind, ToolRegistrationError};
pub use function::FunctionTool;
pub use registry::{ToolLimits, ToolRegistry};
pub use state::State;
pub use tool::{Tool, ToolFuture, ToolOutput};
