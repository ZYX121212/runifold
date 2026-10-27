//! Python configuration adapted to existing native runtime profiles.
use std::{num::NonZeroUsize, time::Duration};

use futures_util::StreamExt;
use pyo3::{exceptions::PyValueError, prelude::*};
use runifold::{
    Model, ModelCallContext, ModelCapacityLimits, ModelCapacityPoolId, ModelEventStream, ModelRef,
    ModelScheduler, ModelSchedulerConfig, ProviderModel, ProviderModelExt, ProviderRuntime,
    ToolDescriptor,
    model::{
        FeatureSupport, ModelCapabilities, ModelError, ModelFuture, ModelStreamEvent, SupportLevel,
    },
};
use runifold_providers::{
    anthropic::{AnthropicClient, AnthropicConfig},
    gemini::{GeminiClient, GeminiConfig},
    ollama::{OllamaClient, OllamaConfig},
    openai::{OpenAiClient, OpenAiConfig, OpenAiWireProtocol},
};
use runifold_testkit::ScriptedModel;
use serde::Deserialize;
use serde_json::Value;

pub fn invalid(message: &'static str) -> PyErr {
    PyValueError::new_err(message)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    pub provider: String,
    pub provider_name: Option<String>,
    pub model: String,
    pub api_key: Option<String>,
    pub base_url: Option<String>,
    pub protocol: String,
    pub model_concurrency: usize,
    pub request_queue: usize,
    pub run_concurrency: usize,
    pub tool_concurrency: usize,
    pub circuit_breaker: bool,
}

impl ClientConfig {
    pub fn validate(&self) -> PyResult<()> {
        for value in [
            self.model_concurrency,
            self.request_queue,
            self.run_concurrency,
            self.tool_concurrency,
        ] {
            if !(1..=4096).contains(&value) {
                return Err(invalid(
                    "Concurrency and queue bounds must be between 1 and 4096",
                ));
            }
        }
        if self.model.trim().is_empty() {
            return Err(invalid("Model must not be empty"));
        }
        Ok(())
    }

    pub fn runtime<M: ProviderModel + 'static>(
        &self,
        model: M,
    ) -> PyResult<(ProviderRuntime, ModelScheduler)> {
        self.validate()?;
        let pool = ModelCapacityPoolId::new("python-client")
            .map_err(|_| invalid("Invalid capacity pool"))?;
        let scheduler = ModelScheduler::new(
            ModelSchedulerConfig::new(
                NonZeroUsize::new(self.request_queue).expect("validated queue"),
            )
            .pool(
                pool.clone(),
                ModelCapacityLimits::new(
                    NonZeroUsize::new(self.model_concurrency).expect("validated concurrency"),
                ),
            )
            .map_err(|_| invalid("Invalid capacity pool configuration"))?,
        )
        .map_err(|_| invalid("Invalid request scheduler configuration"))?;
        let mut profile = model
            .runtime_profile()
            .response_mode(runifold::model::ResponseMode::Streaming);
        if !self.circuit_breaker {
            profile = profile.without_circuit_breaker();
        }
        let runtime = model
            .scheduled(&scheduler, [pool])
            .map_err(|_| invalid("Could not bind request scheduling"))?
            .runtime_with_profile(self.model.clone(), profile)
            .map_err(|_| invalid("Invalid model runtime configuration"))?;
        Ok((runtime, scheduler))
    }

    pub fn provider_runtime(&self) -> PyResult<(ProviderRuntime, ModelScheduler)> {
        if !matches!(self.provider.as_str(), "openai" | "compatible") && self.protocol != "native" {
            return Err(invalid(
                "Protocol selection only applies to OpenAI-compatible endpoints",
            ));
        }
        let key = || {
            self.api_key
                .clone()
                .ok_or_else(|| invalid("Provider requires an API key"))
        };
        match self.provider.as_str() {
            "openai" | "compatible" => {
                let wire = match self.protocol.as_str() {
                    "responses" => OpenAiWireProtocol::Responses,
                    "chat_completions" => OpenAiWireProtocol::ChatCompletions,
                    _ => return Err(invalid("Protocol must be responses or chat_completions")),
                };
                let config = if self.provider == "compatible" {
                    OpenAiConfig::compatible(
                        self.provider_name.as_deref().unwrap_or("compatible"),
                        key()?,
                        self.base_url
                            .as_deref()
                            .ok_or_else(|| invalid("Compatible endpoints require base_url"))?,
                        wire,
                    )
                } else {
                    let config = OpenAiConfig::new(key()?)
                        .map_err(|_| invalid("Invalid OpenAI credential configuration"))?
                        .with_wire_protocol(wire);
                    match &self.base_url {
                        Some(url) => config.with_base_url(url),
                        None => Ok(config),
                    }
                }
                .map_err(|_| invalid("Invalid OpenAI endpoint configuration"))?;
                self.runtime(OpenAiClient::new(config))
            }
            "anthropic" => {
                let config = AnthropicConfig::new(key()?)
                    .map_err(|_| invalid("Invalid Anthropic configuration"))?;
                let config = match &self.base_url {
                    Some(url) => config
                        .with_base_url(url)
                        .map_err(|_| invalid("Invalid Anthropic endpoint"))?,
                    None => config,
                };
                self.runtime(AnthropicClient::new(config))
            }
            "gemini" => {
                let config = GeminiConfig::new(key()?)
                    .map_err(|_| invalid("Invalid Gemini configuration"))?;
                let config = match &self.base_url {
                    Some(url) => config
                        .with_base_url(url)
                        .map_err(|_| invalid("Invalid Gemini endpoint"))?,
                    None => config,
                };
                self.runtime(GeminiClient::new(config))
            }
            "ollama" => {
                let config =
                    OllamaConfig::new(self.base_url.as_deref().unwrap_or("http://localhost:11434"))
                        .map_err(|_| invalid("Invalid Ollama endpoint"))?;
                let config = match &self.api_key {
                    Some(key) => config
                        .with_bearer_token(key.clone())
                        .map_err(|_| invalid("Invalid Ollama credential"))?,
                    None => config,
                };
                self.runtime(OllamaClient::new(config))
            }
            _ => Err(invalid(
                "Unsupported provider; use openai, compatible, anthropic, gemini or ollama",
            )),
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    pub name: String,
    pub system: String,
    pub tools: Vec<ToolDescriptor>,
    pub output_schema: Option<Value>,
    pub output_name: Option<String>,
    pub max_output_tokens: Option<u64>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunOptions {
    pub budget: runifold::Budget,
    pub timeout: Option<f64>,
    pub stream: bool,
    pub checkpoint_path: Option<String>,
    pub checkpoint_id: Option<String>,
    pub resume: bool,
    pub retry_interrupted: bool,
    pub allowed_tools: Option<Vec<String>>,
}

impl RunOptions {
    pub fn timeout_duration(&self) -> PyResult<Option<Duration>> {
        self.timeout
            .map(|seconds| {
                if !seconds.is_finite() || seconds <= 0.0 || seconds > 31_536_000.0 {
                    return Err(invalid(
                        "Timeout must be finite, positive and at most one year",
                    ));
                }
                Duration::try_from_secs_f64(seconds).map_err(|_| invalid("Invalid timeout"))
            })
            .transpose()
    }
}

pub struct ScriptedProvider {
    pub model: ScriptedModel,
    pub delay: Duration,
}
impl ProviderModel for ScriptedProvider {
    fn provider(&self) -> &str {
        "scripted"
    }
}
impl Model for ScriptedProvider {
    fn capabilities<'a>(
        &'a self,
        model: &'a ModelRef,
    ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
        self.model.capabilities(model)
    }
    fn stream(
        &self,
        request: runifold::ModelRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
        Box::pin(async move {
            let stream = self.model.stream(request, context).await?;
            let delay = self.delay;
            Ok(Box::pin(stream.then(move |event| async move {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                event
            })) as ModelEventStream)
        })
    }
}

pub fn scripted_model(scripts: Vec<Vec<ModelStreamEvent>>) -> ScriptedModel {
    let native = FeatureSupport::new(SupportLevel::Native);
    let model = ScriptedModel::new().with_capabilities(ModelCapabilities {
        streaming: native.clone(),
        tools: native.clone(),
        parallel_tools: native.clone(),
        structured_output: native,
        ..ModelCapabilities::default()
    });
    for script in scripts {
        model.enqueue(script);
    }
    model
}
