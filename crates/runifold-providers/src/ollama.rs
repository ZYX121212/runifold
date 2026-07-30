//! Native Ollama chat and embeddings provider.

mod client;
mod config;
mod decode;
mod embedding;
mod encode;

pub use client::OllamaClient;
pub use config::{OllamaConfig, OllamaConfigError};
pub use decode::OllamaChunkDecoder;
pub use embedding::OllamaEmbeddingModel;
pub use encode::encode_request;
