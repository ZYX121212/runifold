use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use runifold_model::{
    ContentPart, FeaturePolicy, FinishReason, MediaSource, Message, Model, ModelCallContext,
    ModelCapabilities, ModelRef, ModelRequest, Role, SupportLevel, ToolCall, ToolChoice,
    ToolResult, ToolSpec,
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

    /// Selects a model while considering canonical request requirements.
    ///
    /// Existing selectors remain source compatible and may rely on the
    /// default implementation. Capability-aware selectors should override
    /// this method.
    ///
    /// # Errors
    ///
    /// Returns [`SamplingError`] when no host-approved model can satisfy the
    /// request.
    fn select_with_requirements(
        &self,
        preferences: Option<&ModelPreferences>,
        _requirements: &SamplingModelRequirements,
    ) -> Result<ModelRef, SamplingError> {
        self.select(preferences)
    }
}

/// Capabilities required by one approved MCP Sampling request.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum SamplingModelFeature {
    /// Tool declarations and Tool-use history.
    Tools,
    /// Inline image input.
    ImageInput,
    /// Inline audio input.
    AudioInput,
    /// Document or resource input.
    DocumentInput,
}

/// Capabilities required by one approved MCP Sampling request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SamplingModelRequirements {
    features: BTreeSet<SamplingModelFeature>,
}

impl SamplingModelRequirements {
    /// Creates requirements from canonical feature identities.
    pub fn from_features(features: impl IntoIterator<Item = SamplingModelFeature>) -> Self {
        Self {
            features: features.into_iter().collect(),
        }
    }

    /// Returns whether one feature is required.
    pub fn requires(&self, feature: SamplingModelFeature) -> bool {
        self.features.contains(&feature)
    }

    /// Iterates over required features in stable order.
    pub fn features(&self) -> impl Iterator<Item = SamplingModelFeature> + '_ {
        self.features.iter().copied()
    }

    fn insert(&mut self, feature: SamplingModelFeature) {
        self.features.insert(feature);
    }
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
    fn supports_tools(&self) -> bool {
        true
    }

    fn sample(
        &self,
        request: CreateMessageParams,
        context: SamplingCallContext,
    ) -> SamplingFuture<'_, CreateMessageResult> {
        Box::pin(async move {
            let requirements = sampling_requirements(&request);
            let model_ref = self
                .selector
                .select_with_requirements(request.model_preferences.as_ref(), &requirements)?;
            let capabilities =
                self.model.capabilities(&model_ref).await.map_err(|error| {
                    SamplingError::new(SamplingErrorKind::Execution, error.message)
                })?;
            validate_capabilities(&capabilities, &requirements)?;
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
                .map(to_mcp_content)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
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

fn sampling_requirements(request: &CreateMessageParams) -> SamplingModelRequirements {
    let mut requirements = SamplingModelRequirements::default();
    if !request.tools.is_empty() || request.tool_choice.is_some() {
        requirements.insert(SamplingModelFeature::Tools);
    }
    for block in request
        .messages
        .iter()
        .flat_map(|message| message.content.as_slice())
    {
        update_requirements(block, &mut requirements);
    }
    requirements
}

fn update_requirements(block: &ContentBlock, requirements: &mut SamplingModelRequirements) {
    match block.kind.as_str() {
        "image" => requirements.insert(SamplingModelFeature::ImageInput),
        "audio" => requirements.insert(SamplingModelFeature::AudioInput),
        "resource_link" => requirements.insert(SamplingModelFeature::DocumentInput),
        "resource" => {
            if block
                .fields
                .get("resource")
                .and_then(Value::as_object)
                .is_some_and(|resource| resource.get("blob").is_some())
            {
                requirements.insert(SamplingModelFeature::DocumentInput);
            }
        }
        "tool_use" | "tool_result" => requirements.insert(SamplingModelFeature::Tools),
        "runifold/content" => {
            if let Some(content) = block.fields.get("content")
                && let Ok(content) = serde_json::from_value::<ContentPart>(content.clone())
            {
                match content {
                    ContentPart::Image { .. } => {
                        requirements.insert(SamplingModelFeature::ImageInput);
                    }
                    ContentPart::Audio { .. } => {
                        requirements.insert(SamplingModelFeature::AudioInput);
                    }
                    ContentPart::Document { .. } | ContentPart::ResourceLink { .. } => {
                        requirements.insert(SamplingModelFeature::DocumentInput);
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
    if block.kind == "tool_result"
        && let Some(content) = block.fields.get("content")
        && let Ok(blocks) = decode_content_blocks(content)
    {
        for nested in &blocks {
            update_requirements(nested, requirements);
        }
    }
}

fn validate_capabilities(
    capabilities: &ModelCapabilities,
    requirements: &SamplingModelRequirements,
) -> Result<(), SamplingError> {
    let unsupported = (requirements.requires(SamplingModelFeature::Tools)
        && capabilities.tools.level == SupportLevel::Unsupported)
        .then_some("Tools")
        .or_else(|| {
            (requirements.requires(SamplingModelFeature::ImageInput)
                && capabilities.image_input.level == SupportLevel::Unsupported)
                .then_some("image input")
        })
        .or_else(|| {
            (requirements.requires(SamplingModelFeature::AudioInput)
                && capabilities.audio_input.level == SupportLevel::Unsupported)
                .then_some("audio input")
        })
        .or_else(|| {
            (requirements.requires(SamplingModelFeature::DocumentInput)
                && capabilities.document_input.level == SupportLevel::Unsupported)
                .then_some("document input")
        });
    if let Some(feature) = unsupported {
        return Err(SamplingError::new(
            SamplingErrorKind::Execution,
            format!("selected Sampling model does not support {feature}"),
        ));
    }
    Ok(())
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
        let mut canonical = Message::new(role, content).map_err(|error| {
            SamplingError::new(SamplingErrorKind::InvalidRequest, error.message)
        })?;
        if !message.meta.is_empty() {
            canonical.metadata.insert(
                "mcp.sampling.message_meta".into(),
                serde_json::to_value(&message.meta)
                    .map_err(|_| invalid_request("Sampling message metadata is malformed"))?,
            );
        }
        messages.push(canonical);
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
    model_request.tools = request
        .tools
        .iter()
        .map(|tool| {
            let mut metadata = BTreeMap::new();
            if let Some(annotations) = &tool.annotations {
                metadata.insert("mcp.tool.annotations".into(), annotations.clone());
            }
            if let Some(title) = &tool.title {
                metadata.insert("mcp.tool.title".into(), Value::String(title.clone()));
            }
            ToolSpec {
                name: tool.name.clone(),
                description: tool.description.clone().unwrap_or_default(),
                input_schema: tool.input_schema.clone(),
                output_schema: tool.output_schema.clone(),
                metadata,
            }
        })
        .collect();
    model_request.tool_choice = match request
        .tool_choice
        .as_ref()
        .map(|choice| choice.mode)
        .unwrap_or_default()
    {
        crate::SamplingToolChoiceMode::Auto => ToolChoice::Auto,
        crate::SamplingToolChoiceMode::Required => ToolChoice::Required,
        crate::SamplingToolChoiceMode::None => ToolChoice::None,
    };
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
        "resource_link" => decode_resource_link(block),
        "resource" => decode_embedded_resource(block),
        "tool_use" => {
            let extension = block
                .fields
                .get("_meta")
                .and_then(Value::as_object)
                .and_then(|meta| meta.get("runifold.tool_use.v1"))
                .and_then(Value::as_object);
            let mut metadata = block_metadata(block)?;
            if let Some(extension_metadata) = extension
                .and_then(|value| value.get("metadata"))
                .and_then(Value::as_object)
            {
                merge_metadata(&mut metadata, extension_metadata);
            }
            Ok(ContentPart::ToolCall(ToolCall {
                id: required_string(block, "id")?,
                name: required_string(block, "name")?,
                arguments: required_object(block, "input")?,
                raw_arguments: extension
                    .and_then(|value| value.get("rawArguments"))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                metadata,
            }))
        }
        "tool_result" => {
            let content = block
                .fields
                .get("content")
                .ok_or_else(|| invalid_request("Sampling Tool result content is missing"))?;
            let content = decode_content_blocks(content)?
                .iter()
                .map(to_model_content)
                .collect::<Result<Vec<_>, _>>()?;
            let extension = block
                .fields
                .get("_meta")
                .and_then(Value::as_object)
                .and_then(|meta| meta.get("runifold.tool_result.v1"))
                .and_then(Value::as_object);
            let mut metadata = block_metadata(block)?;
            if let Some(extension_metadata) = extension
                .and_then(|value| value.get("metadata"))
                .and_then(Value::as_object)
            {
                merge_metadata(&mut metadata, extension_metadata);
            }
            Ok(ContentPart::ToolResult(ToolResult {
                call_id: required_string(block, "toolUseId")?,
                name: extension
                    .and_then(|value| value.get("name"))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                content,
                structured_content: block.fields.get("structuredContent").cloned().or_else(|| {
                    extension
                        .and_then(|value| value.get("structuredContent"))
                        .cloned()
                }),
                is_error: block
                    .fields
                    .get("isError")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                metadata,
            }))
        }
        "runifold/content" => {
            let content = serde_json::from_value(
                block
                    .fields
                    .get("content")
                    .cloned()
                    .ok_or_else(|| invalid_request("Runifold Sampling content is missing"))?,
            )
            .map_err(|_| invalid_request("Runifold Sampling content is malformed"))?;
            validate_extension_content(&content)?;
            Ok(content)
        }
        _ => Ok(ContentPart::text(
            serde_json::json!({
                "type": "runifold.mcp.content.v1",
                "content": block,
            })
            .to_string(),
        )),
    }
}

fn required_object(block: &ContentBlock, field: &str) -> Result<Value, SamplingError> {
    block
        .fields
        .get(field)
        .filter(|value| value.is_object())
        .cloned()
        .ok_or_else(|| invalid_request(format!("Sampling content is missing object `{field}`")))
}

fn decode_resource_link(block: &ContentBlock) -> Result<ContentPart, SamplingError> {
    Ok(ContentPart::ResourceLink {
        uri: required_string(block, "uri")?,
        name: required_string(block, "name")?,
        title: optional_string(block, "title")?,
        description: optional_string(block, "description")?,
        media_type: optional_string(block, "mimeType")?,
        size: optional_u64(block, "size")?,
    })
}

fn decode_embedded_resource(block: &ContentBlock) -> Result<ContentPart, SamplingError> {
    let resource = block
        .fields
        .get("resource")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            invalid_request("Sampling embedded resource is missing object `resource`")
        })?;
    let uri = object_string(resource, "uri")?;
    if let Some(text) = resource.get("text").and_then(Value::as_str) {
        return Ok(ContentPart::text(text));
    }
    let data = object_string(resource, "blob")?;
    let media_type = resource
        .get("mimeType")
        .and_then(Value::as_str)
        .unwrap_or("application/octet-stream");
    Ok(ContentPart::Document {
        source: MediaSource::Base64 {
            media_type: media_type.into(),
            data: data.into(),
        },
        name: Some(uri.into()),
    })
}

fn block_metadata(block: &ContentBlock) -> Result<BTreeMap<String, Value>, SamplingError> {
    block
        .fields
        .get("_meta")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| invalid_request("Sampling content _meta must be an object"))
        .map(Option::unwrap_or_default)
}

fn merge_metadata(
    metadata: &mut BTreeMap<String, Value>,
    extension: &serde_json::Map<String, Value>,
) {
    for (key, value) in extension {
        metadata.entry(key.clone()).or_insert_with(|| value.clone());
    }
}

fn object_string<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &str,
) -> Result<&'a str, SamplingError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid_request(format!("Sampling resource is missing `{field}`")))
}

fn optional_string(block: &ContentBlock, field: &str) -> Result<Option<String>, SamplingError> {
    match block.fields.get(field) {
        None => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(invalid_request(format!(
            "Sampling content field `{field}` must be a string"
        ))),
    }
}

fn optional_u64(block: &ContentBlock, field: &str) -> Result<Option<u64>, SamplingError> {
    match block.fields.get(field) {
        None => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or_else(|| {
            invalid_request(format!(
                "Sampling content field `{field}` must be an unsigned integer"
            ))
        }),
    }
}

fn decode_content_blocks(value: &Value) -> Result<Vec<ContentBlock>, SamplingError> {
    if let Some(text) = value.as_str() {
        return Ok(vec![ContentBlock::text(text)]);
    }
    let values = value
        .as_array()
        .cloned()
        .unwrap_or_else(|| vec![value.clone()]);
    values
        .into_iter()
        .map(|value| {
            serde_json::from_value(value)
                .map_err(|_| invalid_request("Sampling content block is malformed"))
        })
        .collect()
}

fn invalid_request(message: impl Into<String>) -> SamplingError {
    SamplingError::new(SamplingErrorKind::InvalidRequest, message)
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
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            SamplingError::new(
                SamplingErrorKind::InvalidRequest,
                format!("Sampling content is missing `{field}`"),
            )
        })
}

fn to_mcp_content(content: &ContentPart) -> Result<Option<ContentBlock>, SamplingError> {
    match content {
        ContentPart::Text { text } => Ok(Some(ContentBlock::text(text))),
        ContentPart::Image {
            source: source @ MediaSource::Base64 { .. },
        } => media_block("image", source).map(Some),
        ContentPart::Audio {
            source: source @ MediaSource::Base64 { .. },
        } => media_block("audio", source).map(Some),
        ContentPart::ToolCall(call) => {
            let fields = BTreeMap::from([
                ("id".into(), Value::String(call.id.clone())),
                ("name".into(), Value::String(call.name.clone())),
                ("input".into(), call.arguments.clone()),
            ]);
            Ok(Some(ContentBlock {
                kind: "tool_use".into(),
                fields,
            }))
        }
        ContentPart::ToolResult(_) => Err(SamplingError::new(
            SamplingErrorKind::InvalidOutput,
            "Sampling model output cannot contain Tool results",
        )),
        ContentPart::Reasoning(_) => Ok(None),
        ContentPart::ProviderOpaque(data)
            if data.provider == "mcp" && data.kind == "sampling_content" =>
        {
            serde_json::from_value(data.value.clone())
                .map_err(|_| {
                    SamplingError::new(
                        SamplingErrorKind::InvalidOutput,
                        "preserved MCP Sampling content is malformed",
                    )
                })
                .map(Some)
        }
        ContentPart::ProviderOpaque(_) => Err(SamplingError::new(
            SamplingErrorKind::InvalidOutput,
            "Provider-private model output cannot cross MCP Sampling",
        )),
        _ => extension_block(content).map(Some),
    }
}

fn extension_block(content: &ContentPart) -> Result<ContentBlock, SamplingError> {
    validate_extension_content(content)
        .map_err(|error| SamplingError::new(SamplingErrorKind::InvalidOutput, error.message))?;
    Ok(ContentBlock {
        kind: "runifold/content".into(),
        fields: BTreeMap::from([(
            "content".into(),
            serde_json::to_value(content).map_err(|_| {
                SamplingError::new(
                    SamplingErrorKind::InvalidOutput,
                    "Runifold Sampling extension content cannot be encoded",
                )
            })?,
        )]),
    })
}

fn validate_extension_content(content: &ContentPart) -> Result<(), SamplingError> {
    let source = match content {
        ContentPart::Image { source }
        | ContentPart::Audio { source }
        | ContentPart::Document { source, .. } => Some(source),
        ContentPart::Text { .. }
        | ContentPart::ResourceLink { .. }
        | ContentPart::Refusal { .. }
        | ContentPart::Citation(_) => None,
        _ => {
            return Err(invalid_request(
                "Runifold Sampling extension contains private or recursive content",
            ));
        }
    };
    if matches!(
        source,
        Some(MediaSource::Artifact { .. } | MediaSource::ProviderFile { .. })
    ) {
        return Err(invalid_request(
            "Runifold Sampling extensions cannot expose host or Provider file references",
        ));
    }
    Ok(())
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
