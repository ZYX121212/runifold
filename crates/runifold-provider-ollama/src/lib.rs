//! Native Ollama chat API adapter for Runifold.

mod client;
mod config;
mod decode;
mod encode;

pub use client::OllamaClient;
pub use config::{OllamaConfig, OllamaConfigError};
pub use decode::OllamaChunkDecoder;
pub use encode::encode_request;
