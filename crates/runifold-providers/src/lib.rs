//! Feature-gated first-party model providers for Runifold.
//!
//! Portable HTTP adapters and fully optional SDK-backed integrations share
//! this publication boundary. Every provider retains an independent feature,
//! protocol module, and dependency boundary. A companion crate is reserved
//! for integrations that require an incompatible runtime, toolchain, license,
//! or release lifecycle.

pub mod content_projection;

#[cfg(any(
    feature = "anthropic",
    feature = "gemini",
    feature = "ollama",
    feature = "openai"
))]
mod reliability;

#[cfg(feature = "anthropic")]
pub mod anthropic;
#[cfg(feature = "bedrock")]
pub mod bedrock;
#[cfg(feature = "gemini")]
pub mod gemini;
#[cfg(feature = "ollama")]
pub mod ollama;
#[cfg(feature = "openai")]
pub mod openai;
