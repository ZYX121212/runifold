//! Feature-gated first-party model providers for Runifold.
//!
//! Portable HTTP adapters and fully optional SDK-backed integrations share
//! this publication boundary. Every provider retains an independent feature,
//! protocol module, and dependency boundary. A companion crate is reserved
//! for integrations that require an incompatible runtime, toolchain, license,
//! or release lifecycle.

pub mod content_projection;

#[cfg(feature = "openai")]
mod compatible;

#[cfg(feature = "openai")]
pub use compatible::{
    ark, azure, deepseek, groq, huggingface, llama_cpp, llamafile, minimax, mistral, openrouter,
    perplexity, qwen, siliconflow, together, vllm, xai, zhipu,
};

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
#[cfg(feature = "cohere")]
pub mod cohere;
#[cfg(feature = "gemini")]
pub mod gemini;
#[cfg(feature = "ollama")]
pub mod ollama;
#[cfg(feature = "openai")]
pub mod openai;
