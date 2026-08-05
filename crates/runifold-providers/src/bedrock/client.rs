//! Amazon Bedrock Runtime client boundary.

use std::future::Future;

use aws_sdk_bedrockruntime::{
    Client, Config,
    config::{BehaviorVersion, Credentials, Region},
    error::SdkError,
    operation::converse_stream::ConverseStreamError,
    types::error::ConverseStreamOutputError,
};
use aws_smithy_types::{error::metadata::ProvideErrorMetadata, retry::RetryConfig};
use futures_timer::Delay;
use futures_util::future::{Either, select};
use runifold_core::RetrySafety;
use runifold_model::{
    FeatureSupport, Model, ModelCallContext, ModelCapabilities, ModelError, ModelErrorKind,
    ModelEventStream, ModelFuture, ModelRef, ModelRequest, ProviderModel, SupportLevel,
};
use serde_json::Value;
use thiserror::Error;

use super::{BedrockEventDecoder, encode::encode_request};

/// Invalid configuration detected before constructing an Amazon Bedrock client.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum BedrockConfigError {
    /// The AWS region was blank.
    #[error("AWS region cannot be empty")]
    EmptyRegion,
    /// The access-key ID was blank.
    #[error("AWS access-key ID cannot be empty")]
    EmptyAccessKey,
    /// The secret access key was blank.
    #[error("AWS secret access key cannot be empty")]
    EmptySecretKey,
}

/// Native Amazon Bedrock Converse Stream implementation of [`Model`].
#[derive(Clone)]
pub struct BedrockClient {
    sdk: Client,
    capabilities: ModelCapabilities,
}

impl BedrockClient {
    /// Creates a client from a Bedrock service configuration.
    ///
    /// SDK-level retries are disabled so Runifold's canonical retry policy is
    /// the only retry authority and duplicate model charges remain visible.
    pub fn new(config: &Config) -> Self {
        let config = config
            .to_builder()
            .retry_config(RetryConfig::disabled())
            .build();
        Self::from_sdk_client(Client::from_conf(config))
    }

    /// Wraps an application-owned SDK client without changing its policies.
    ///
    /// Applications using this escape hatch must ensure the SDK retry policy
    /// does not conflict with Runifold's retry and budget accounting.
    pub fn from_sdk_client(sdk: Client) -> Self {
        Self {
            sdk,
            capabilities: adapter_capabilities(),
        }
    }

    /// Creates a `SigV4` client from explicit, possibly temporary credentials.
    ///
    /// Prefer short-lived credentials supplied by a secure application-owned
    /// credential provider. The optional session token supports STS, ECS, and
    /// other temporary credential sources.
    ///
    /// # Errors
    ///
    /// Returns [`BedrockConfigError`] when a required value is blank.
    pub fn from_credentials(
        region: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Result<Self, BedrockConfigError> {
        let region = region.into();
        if region.trim().is_empty() {
            return Err(BedrockConfigError::EmptyRegion);
        }
        let access_key_id = access_key_id.into();
        if access_key_id.trim().is_empty() {
            return Err(BedrockConfigError::EmptyAccessKey);
        }
        let secret_access_key = secret_access_key.into();
        if secret_access_key.trim().is_empty() {
            return Err(BedrockConfigError::EmptySecretKey);
        }
        let credentials = Credentials::new(
            access_key_id,
            secret_access_key,
            session_token,
            None,
            "runifold-explicit",
        );
        let config = Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region))
            .credentials_provider(credentials)
            .retry_config(RetryConfig::disabled())
            .build();
        Ok(Self::new(&config))
    }

    /// Declares application-verified capabilities for a specific model family.
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: ModelCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    fn prepare(
        &self,
        request: &ModelRequest,
    ) -> Result<
        (
            super::encode::EncodedRequest,
            Vec<runifold_model::ModelWarning>,
        ),
        ModelError,
    > {
        if !matches!(
            request.selected_response_mode(),
            runifold_model::ResponseMode::Streaming
        ) {
            return Err(ModelError::local(
                ModelErrorKind::UnsupportedFeature,
                "Bedrock adapter currently requires streaming response mode",
            ));
        }
        if request.model.provider != "bedrock" {
            return Err(invalid(format!(
                "Bedrock client cannot invoke provider `{}`",
                request.model.provider
            )));
        }
        if request.model.name.trim().is_empty() {
            return Err(invalid("Bedrock model or inference-profile ID is empty"));
        }
        let warnings = self.capabilities.validate_request(request, true)?;
        encode_request(request).map(|encoded| (encoded, warnings))
    }
}

impl std::fmt::Debug for BedrockClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BedrockClient")
            .field("capabilities", &self.capabilities)
            .finish_non_exhaustive()
    }
}

impl Model for BedrockClient {
    fn capabilities<'a>(
        &'a self,
        _model: &'a ModelRef,
    ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
        let capabilities = self.capabilities.clone();
        Box::pin(async move { Ok(capabilities) })
    }

    fn stream(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
        Box::pin(async move {
            let (encoded, warnings) = self.prepare(&request)?;
            if context
                .remaining()
                .is_some_and(|remaining| remaining.is_zero())
            {
                return Err(deadline());
            }

            let operation = self
                .sdk
                .converse_stream()
                .model_id(&request.model.name)
                .set_messages(Some(encoded.messages))
                .set_system((!encoded.system.is_empty()).then_some(encoded.system))
                .inference_config(encoded.inference)
                .set_tool_config(encoded.tools)
                .set_additional_model_request_fields(encoded.additional_fields);
            let output = Box::pin(scoped(operation.send(), &context))
                .await?
                .map_err(|error| map_open_error(&error))?;
            let mut stream = output.stream;
            let model = request.model.name;
            Ok(Box::pin(async_stream::try_stream! {
                for warning in warnings {
                    yield runifold_model::ModelStreamEvent::Warning { warning };
                }
                let mut decoder = BedrockEventDecoder::new(model);
                loop {
                    let next = scoped(stream.recv(), &context).await?;
                    match next {
                        Ok(Some(output)) => {
                            for event in decoder.decode(output)? {
                                yield event;
                            }
                        }
                        Ok(None) => {
                            for event in decoder.finish()? {
                                yield event;
                            }
                            break;
                        }
                        Err(error) => Err(map_stream_sdk_error(&error))?,
                    }
                }
            }) as ModelEventStream)
        })
    }
}

impl ProviderModel for BedrockClient {
    fn provider(&self) -> &'static str {
        "bedrock"
    }
}

async fn scoped<F, T, E>(future: F, context: &ModelCallContext) -> Result<Result<T, E>, ModelError>
where
    F: Future<Output = Result<T, E>>,
{
    let cancellation = context.cancellation().clone();
    let cancellable = async move {
        match select(Box::pin(cancellation.cancelled()), Box::pin(future)).await {
            Either::Left(_) => Err(cancelled()),
            Either::Right((result, _)) => Ok(result),
        }
    };
    if let Some(remaining) = context.remaining() {
        match select(Box::pin(Delay::new(remaining)), Box::pin(cancellable)).await {
            Either::Left(_) => Err(deadline()),
            Either::Right((result, _)) => result,
        }
    } else {
        cancellable.await
    }
}

fn map_open_error(
    error: &SdkError<ConverseStreamError, aws_sdk_bedrockruntime::config::http::HttpResponse>,
) -> ModelError {
    if let Some(service) = error.as_service_error() {
        let mut model_error = provider_error(service.code(), service.message());
        if service.is_throttling_exception()
            || service.is_service_unavailable_exception()
            || service.is_model_not_ready_exception()
        {
            model_error.retry_safety = RetrySafety::Safe;
        }
        return model_error;
    }
    let kind = match error {
        SdkError::TimeoutError(_) => ModelErrorKind::DeadlineExceeded,
        SdkError::ConstructionFailure(_) => ModelErrorKind::InvalidRequest,
        SdkError::ResponseError(_) => ModelErrorKind::Protocol,
        SdkError::ServiceError(_) => unreachable!("service errors are handled above"),
        _ => ModelErrorKind::Transport,
    };
    with_provider(ModelError::local(
        kind,
        "Amazon Bedrock transport failed before a stream was opened",
    ))
}

fn map_stream_error(error: &ConverseStreamOutputError) -> ModelError {
    let mut model_error = provider_error(error.code(), error.message());
    if error.is_throttling_exception() || error.is_service_unavailable_exception() {
        model_error.retry_safety = RetrySafety::Safe;
    }
    model_error
}

fn map_stream_sdk_error<R>(error: &SdkError<ConverseStreamOutputError, R>) -> ModelError {
    if let Some(service) = error.as_service_error() {
        return map_stream_error(service);
    }
    let kind = match error {
        SdkError::TimeoutError(_) => ModelErrorKind::DeadlineExceeded,
        SdkError::DispatchFailure(_) => ModelErrorKind::Transport,
        SdkError::ServiceError(_) => unreachable!("service errors are handled above"),
        _ => ModelErrorKind::Protocol,
    };
    with_provider(ModelError::local(
        kind,
        "Amazon Bedrock response stream transport failed",
    ))
}

fn provider_error(code: Option<&str>, message: Option<&str>) -> ModelError {
    let mut error = with_provider(ModelError::local(
        ModelErrorKind::Provider,
        message.unwrap_or("Amazon Bedrock rejected the model request"),
    ));
    if let Some(code) = code {
        error
            .metadata
            .insert("bedrock.error.code".into(), Value::String(code.into()));
    }
    error
}

fn adapter_capabilities() -> ModelCapabilities {
    ModelCapabilities {
        streaming: FeatureSupport::new(SupportLevel::Native),
        audio_input: FeatureSupport::new(SupportLevel::Unsupported),
        ..ModelCapabilities::default()
    }
}

fn invalid(message: impl Into<String>) -> ModelError {
    with_provider(ModelError::local(ModelErrorKind::InvalidRequest, message))
}

fn cancelled() -> ModelError {
    with_provider(ModelError::local(
        ModelErrorKind::Cancelled,
        "Amazon Bedrock invocation was cancelled",
    ))
}

fn deadline() -> ModelError {
    with_provider(ModelError::local(
        ModelErrorKind::DeadlineExceeded,
        "Amazon Bedrock invocation exceeded its deadline",
    ))
}

fn with_provider(mut error: ModelError) -> ModelError {
    error.provider = Some("bedrock".into());
    error
}

#[cfg(test)]
mod tests {
    use super::{BedrockClient, BedrockConfigError};

    #[test]
    fn explicit_credentials_validate_before_sdk_construction() {
        assert_eq!(
            BedrockClient::from_credentials("", "key", "secret", None).unwrap_err(),
            BedrockConfigError::EmptyRegion
        );
        assert_eq!(
            BedrockClient::from_credentials("us-east-1", "", "secret", None).unwrap_err(),
            BedrockConfigError::EmptyAccessKey
        );
        assert_eq!(
            BedrockClient::from_credentials("us-east-1", "key", "", None).unwrap_err(),
            BedrockConfigError::EmptySecretKey
        );
    }
}
