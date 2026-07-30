//! Native Amazon Bedrock Converse Stream provider.

mod client;
mod decode;
mod encode;

pub use aws_sdk_bedrockruntime::{
    Config as BedrockSdkConfig,
    config::{Credentials as AwsCredentials, Region as AwsRegion},
};
pub use client::{BedrockClient, BedrockConfigError};
pub use decode::BedrockEventDecoder;
