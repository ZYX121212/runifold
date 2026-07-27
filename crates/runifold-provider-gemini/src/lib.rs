//! Native Gemini `GenerateContent` API adapter for Runifold.

mod client;
mod config;
mod decode;
mod encode;

pub use client::GeminiClient;
pub use config::{GeminiConfig, GeminiConfigError};
pub use decode::GeminiEventDecoder;
pub use encode::encode_request;
