use std::{collections::BTreeMap, sync::Arc};

use runifold_model::{
    ContentPart, FeaturePolicy, FinishReason, MediaSource, Message, Model, ModelCallContext,
    ModelRef, ModelRequest, Role,
};
use serde_json::Value;

use crate::{
    ContentBlock, CreateMessageParams, CreateMessageResult, ModelPreferences, SamplingCallContext,
    SamplingContent, SamplingError, SamplingErrorKind, SamplingFuture, SamplingProvider,
    SamplingRole,
};

/// Host-owned model selection boundary for MCP Sampling.
pub trait SamplingModelSelector: Send + Sync {
    /// Selects one configured model. Server hints are advisory.
    ///
    /// # Errors
    ///
    /// Returns [`SamplingError`] when no host-approved model can satisfy the request.
    fn select(&self, preferences: Option<&ModelPreferences>) -> Result<ModelRef, SamplingError>;
}

/// Model selector that always uses one host-configured model.
#[derive(Clone, Debug)]
pub struct FixedSamplingModel {
    model: ModelRef,
}

impl FixedSamplingModel {
    /// Creates a fixed host-side selection.
    pub fn new(model: ModelRef) -> Self {
        Self { model }
    }
}

impl SamplingModelSelector for FixedSamplingModel {
    fn select(&self, _preferences: Option<&ModelPreferences>) -> Result<ModelRef, SamplingError> {
        Ok(self.model.clone())
    }
}

/// Adapter from approved MCP Sampling requests to Runifold's canonical Model.
pub struct ModelSamplingProvider {
    model: Arc<dyn Model>,
    selector: Arc<dyn SamplingModelSelector>,
}

impl ModelSamplingProvider {
    /// Creates a provider using a host-controlled model selector.
    pub fn new(model: Arc<dyn Model>, selector: Arc<dyn SamplingModelSelector>) -> Self {
        Self { model, selector }
    }
}

impl SamplingProvider for ModelSamplingProvider {
    fn sample(
        &self,
        request: CreateMessageParams,
        context: SamplingCallContext,
    ) -> SamplingFuture<'_, CreateMessageResult> {
        Box::pin(async move {
            let model_ref = self.selector.select(request.model_preferences.as_ref())?;
            let model_request = to_model_request(&request, model_ref)?;
            let call_context = ModelCallContext::new()
                .with_deadline(context.deadline())
                .with_cancellation(context.cancellation());
            let response = self
                .model
                .invoke(model_request, call_context)
                .await
                .map_err(|error| {
                    SamplingError::new(
                        match error.kind {
                            runifold_model::ModelErrorKind::Cancelled => {
                                SamplingErrorKind::Cancelled
                            }
                            runifold_model::ModelErrorKind::DeadlineExceeded => {
                                SamplingErrorKind::DeadlineExceeded
                            }
                            _ => SamplingErrorKind::Execution,
                        },
                        error.message,
                    )
                })?;
            let visible_content = response
                .content
                .iter()
                .filter_map(to_mcp_content)
                .collect::<Result<Vec<_>, _>>()?;
            if visible_content.is_empty() {
                return Err(SamplingError::new(
                    SamplingErrorKind::InvalidOutput,
                    "model produced no MCP-visible Sampling content",
                ));
            }
            Ok(CreateMessageResult {
                model: format!("{}:{}", response.model.provider, response.model.name),
                stop_reason: Some(finish_reason(response.finish_reason)),
                role: SamplingRole::Assistant,
                content: if visible_content.len() == 1 {
                    SamplingContent::One(
                        visible_content
                            .into_iter()
                            .next()
                            .expect("length was proven to equal one"),
                    )
                } else {
                    SamplingContent::Many(visible_content)
                },
                meta: BTreeMap::new(),
            })
        })
    }
}

impl std::fmt::Debug for ModelSamplingProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ModelSamplingProvider")
            .field("model", &"<model>")
            .field("selector", &"<selector>")
            .finish()
    }
}

fn to_model_request(
    request: &CreateMessageParams,
    model: ModelRef,
) -> Result<ModelRequest, SamplingError> {
    let mut messages = Vec::with_capacity(
        request
            .messages
            .len()
            .saturating_add(usize::from(request.system_prompt.is_some())),
    );
    if let Some(system) = &request.system_prompt {
        messages.push(Message::system(system));
    }
    for message in &request.messages {
        let role = match message.role {
            SamplingRole::User => Role::User,
            SamplingRole::Assistant => Role::Assistant,
        };
        let content = message
            .content
            .as_slice()
            .iter()
            .map(to_model_content)
            .collect::<Result<Vec<_>, _>>()?;
        messages.push(Message::new(role, content).map_err(|error| {
            SamplingError::new(SamplingErrorKind::InvalidRequest, error.message)
        })?);
    }
    let mut model_request = ModelRequest::new(
        model,
        messages.first().cloned().ok_or_else(|| {
            SamplingError::new(
                SamplingErrorKind::InvalidRequest,
                "Sampling request has no messages",
            )
        })?,
    );
    model_request.messages = messages;
    model_request.generation.temperature = request.temperature;
    model_request.generation.max_output_tokens = Some(request.max_tokens);
    model_request
        .generation
        .stop
        .clone_from(&request.stop_sequences);
    model_request.feature_policy = FeaturePolicy::Strict;
    if let Some(metadata) = &request.metadata {
        model_request
            .metadata
            .insert("mcp.sampling.metadata".into(), metadata.clone());
    }
    Ok(model_request)
}

fn to_model_content(block: &ContentBlock) -> Result<ContentPart, SamplingError> {
    match block.kind.as_str() {
        "text" => Ok(ContentPart::text(required_string(block, "text")?)),
        "image" => Ok(ContentPart::Image {
            source: inline_media(block)?,
        }),
        "audio" => Ok(ContentPart::Audio {
            source: inline_media(block)?,
        }),
        _ => Err(SamplingError::new(
            SamplingErrorKind::InvalidRequest,
            "unsupported Sampling content block",
        )),
    }
}

fn inline_media(block: &ContentBlock) -> Result<MediaSource, SamplingError> {
    Ok(MediaSource::Base64 {
        media_type: required_string(block, "mimeType")?,
        data: required_string(block, "data")?,
    })
}

fn required_string(block: &ContentBlock, field: &str) -> Result<String, SamplingError> {
    block
        .fields
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            SamplingError::new(
                SamplingErrorKind::InvalidRequest,
                format!("Sampling content is missing `{field}`"),
            )
        })
}

fn to_mcp_content(content: &ContentPart) -> Option<Result<ContentBlock, SamplingError>> {
    match content {
        ContentPart::Text { text } | ContentPart::Refusal { text } => {
            Some(Ok(ContentBlock::text(text)))
        }
        ContentPart::Image { source } => Some(media_block("image", source)),
        ContentPart::Audio { source } => Some(media_block("audio", source)),
        ContentPart::Reasoning(_) | ContentPart::Citation(_) => None,
        ContentPart::Document { .. }
        | ContentPart::ToolCall(_)
        | ContentPart::ToolResult(_)
        | ContentPart::ProviderOpaque(_) => Some(Err(SamplingError::new(
            SamplingErrorKind::InvalidOutput,
            "model output cannot be represented by basic MCP Sampling",
        ))),
        _ => Some(Err(SamplingError::new(
            SamplingErrorKind::InvalidOutput,
            "unknown model output cannot be represented by MCP Sampling",
        ))),
    }
}

fn media_block(kind: &str, source: &MediaSource) -> Result<ContentBlock, SamplingError> {
    let MediaSource::Base64 { media_type, data } = source else {
        return Err(SamplingError::new(
            SamplingErrorKind::InvalidOutput,
            "MCP Sampling media must use inline base64",
        ));
    };
    Ok(ContentBlock {
        kind: kind.into(),
        fields: BTreeMap::from([
            ("data".into(), Value::String(data.clone())),
            ("mimeType".into(), Value::String(media_type.clone())),
        ]),
    })
}

fn finish_reason(reason: FinishReason) -> String {
    match reason {
        FinishReason::Stop => "endTurn".into(),
        FinishReason::Length => "maxTokens".into(),
        FinishReason::ToolCalls => "toolUse".into(),
        FinishReason::ContentFilter => "contentFilter".into(),
        FinishReason::Cancelled => "cancelled".into(),
        FinishReason::Error => "error".into(),
        FinishReason::Other(reason) => reason,
        _ => "unknown".into(),
    }
}
