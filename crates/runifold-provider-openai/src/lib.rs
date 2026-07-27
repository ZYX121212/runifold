//! `OpenAI` Responses API adapter for Runifold.

mod chat;
mod client;
mod config;
mod decode;
mod encode;

pub use chat::{ChatCompletionsDecoder, encode_chat_request};
pub use client::OpenAiClient;
pub use config::{OpenAiConfig, OpenAiConfigError, OpenAiWireProtocol};
pub use decode::OpenAiEventDecoder;
pub use encode::encode_request;
