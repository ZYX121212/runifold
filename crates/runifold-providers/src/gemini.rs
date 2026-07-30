//! Native Gemini generation and embeddings provider.

mod client;
mod config;
mod decode;
mod embedding;
mod encode;

pub use client::GeminiClient;
pub use config::{GeminiConfig, GeminiConfigError};
pub use decode::GeminiEventDecoder;
pub use embedding::GeminiEmbeddingModel;
pub use encode::encode_request;
