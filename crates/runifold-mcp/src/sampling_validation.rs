use std::collections::{BTreeMap, BTreeSet};

use base64::Engine;
use runifold_model::{ContentPart, MediaSource};
use serde::Serialize;
use serde_json::Value;

use crate::{
    ContentBlock, CreateMessageParams, CreateMessageResult, IncludeContext, McpTool, SamplingError,
    SamplingErrorKind, SamplingMessage, SamplingPolicy, SamplingRole, SamplingToolChoice,
    SamplingToolChoiceMode,
};

pub(crate) fn validate_request(
    request: &CreateMessageParams,
    policy: &SamplingPolicy,
    context_supported: bool,
    tools_supported: bool,
) -> Result<(), SamplingError> {
    if request.messages.is_empty() || request.messages.len() > policy.max_messages {
        return Err(invalid("Sampling message count is outside policy"));
    }
    if request.max_tokens == 0 || request.max_tokens > policy.max_tokens_per_request {
        return Err(limit("Sampling maxTokens is outside policy"));
    }
    if let Some(ttl) = request.task.as_ref().and_then(|task| task.ttl) {
        let maximum = u64::try_from(policy.max_task_ttl.as_millis()).unwrap_or(u64::MAX);
        if ttl == 0 || ttl > maximum {
            return Err(limit("Sampling task ttl is outside policy"));
        }
    }
    validate_encoded_size(request, policy.max_serialized_bytes, "Sampling request")?;
    if !matches!(request.include_context, IncludeContext::None) && !context_supported {
        return Err(invalid("Sampling context inclusion was not negotiated"));
    }
    if (!request.tools.is_empty() || request.tool_choice.is_some()) && !tools_supported {
        return Err(invalid("Tool-enabled Sampling was not negotiated"));
    }
    if request
        .metadata
        .as_ref()
        .is_some_and(|metadata| !metadata.is_object())
    {
        return Err(invalid("Sampling metadata must be an object"));
    }
    if request
        .temperature
        .is_some_and(|temperature| !temperature.is_finite())
    {
        return Err(invalid("Sampling temperature must be finite"));
    }
    if request
        .model_preferences
        .as_ref()
        .is_some_and(|preferences| {
            [
                preferences.cost_priority,
                preferences.speed_priority,
                preferences.intelligence_priority,
            ]
            .into_iter()
            .flatten()
            .any(|priority| !priority.is_finite() || !(0.0..=1.0).contains(&priority))
        })
    {
        return Err(invalid("Sampling model priorities must be between 0 and 1"));
    }
    validate_tools(request)?;
    validate_messages(&request.messages, policy, &request.tools)
}

pub(crate) fn validate_response(
    response: &CreateMessageResult,
    policy: &SamplingPolicy,
    tools: &[McpTool],
    tool_choice: Option<&SamplingToolChoice>,
) -> Result<(), SamplingError> {
    if response.model.trim().is_empty() || response.role != SamplingRole::Assistant {
        return Err(output("Sampling response model or role is invalid"));
    }
    validate_encoded_size(response, policy.max_serialized_bytes, "Sampling response")
        .map_err(|error| SamplingError::new(SamplingErrorKind::InvalidOutput, error.message))?;
    validate_blocks(response.content.as_slice(), policy, &mut 0)
        .map_err(|error| SamplingError::new(SamplingErrorKind::InvalidOutput, error.message))?;
    let mut tool_ids = BTreeSet::new();
    let tool_names = tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<BTreeSet<_>>();
    let mut has_tool_use = false;
    for block in response.content.as_slice() {
        if block.kind == "tool_result" {
            return Err(output("Sampling model output cannot contain Tool results"));
        }
        if block.kind == "tool_use" {
            has_tool_use = true;
            let id = output_block_string(block, "id")?;
            let name = output_block_string(block, "name")?;
            if !tool_ids.insert(id) || !tool_names.contains(name) {
                return Err(output(
                    "Sampling response Tool use is duplicate or references an undeclared Tool",
                ));
            }
        }
    }
    if has_tool_use != (response.stop_reason.as_deref() == Some("toolUse")) {
        return Err(output(
            "Sampling Tool-use content and stopReason must agree",
        ));
    }
    match tool_choice.map(|choice| choice.mode).unwrap_or_default() {
        SamplingToolChoiceMode::Required if !has_tool_use => {
            return Err(output("Sampling required at least one Tool use"));
        }
        SamplingToolChoiceMode::None if has_tool_use => {
            return Err(output("Sampling disabled Tool use"));
        }
        _ => {}
    }
    Ok(())
}

fn validate_messages(
    messages: &[SamplingMessage],
    policy: &SamplingPolicy,
    tools: &[McpTool],
) -> Result<(), SamplingError> {
    let mut blocks = 0;
    let tool_names = tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<BTreeSet<_>>();
    let mut pending = BTreeMap::<String, String>::new();
    for message in messages {
        validate_blocks(message.content.as_slice(), policy, &mut blocks)?;
        let contents = message.content.as_slice();
        let has_results = contents.iter().any(|block| block.kind == "tool_result");
        if has_results {
            if message.role != SamplingRole::User
                || contents.iter().any(|block| block.kind != "tool_result")
            {
                return Err(invalid(
                    "Sampling Tool results must be the only blocks in a user message",
                ));
            }
            let mut resolved = BTreeSet::new();
            for block in contents {
                let id = required_block_string(block, "toolUseId")?;
                if !pending.contains_key(id) || !resolved.insert(id) {
                    return Err(invalid(
                        "Sampling Tool result does not match one pending use",
                    ));
                }
            }
            if resolved.len() != pending.len() {
                return Err(invalid(
                    "Sampling Tool result message is missing a pending use",
                ));
            }
            pending.clear();
            continue;
        }
        if !pending.is_empty() {
            return Err(invalid(
                "Sampling Tool uses must be followed immediately by matching results",
            ));
        }
        for block in contents.iter().filter(|block| block.kind == "tool_use") {
            if message.role != SamplingRole::Assistant {
                return Err(invalid("Sampling Tool uses require the assistant role"));
            }
            let id = required_block_string(block, "id")?.to_owned();
            let name = required_block_string(block, "name")?.to_owned();
            if !tool_names.contains(name.as_str()) || pending.insert(id, name).is_some() {
                return Err(invalid(
                    "Sampling Tool use is duplicate or references an undeclared Tool",
                ));
            }
        }
    }
    if !pending.is_empty() {
        return Err(invalid("Sampling request contains unresolved Tool uses"));
    }
    Ok(())
}

fn validate_tools(request: &CreateMessageParams) -> Result<(), SamplingError> {
    if request.tool_choice.is_some() && request.tools.is_empty() {
        return Err(invalid("Sampling toolChoice requires at least one Tool"));
    }
    let mut names = BTreeSet::new();
    for tool in &request.tools {
        if tool.name.trim().is_empty()
            || !names.insert(tool.name.as_str())
            || !tool.input_schema.is_object()
            || tool
                .output_schema
                .as_ref()
                .is_some_and(|schema| !schema.is_object())
            || tool
                .annotations
                .as_ref()
                .is_some_and(|annotations| !annotations.is_object())
        {
            return Err(invalid(
                "Sampling Tools require unique names, object schemas, and object annotations",
            ));
        }
    }
    Ok(())
}

fn validate_blocks(
    content: &[ContentBlock],
    policy: &SamplingPolicy,
    total: &mut usize,
) -> Result<(), SamplingError> {
    if content.is_empty() {
        return Err(invalid("Sampling message content must not be empty"));
    }
    add_blocks(total, content.len(), policy.max_content_blocks)?;
    for block in content {
        match block.kind.as_str() {
            "text" if block.fields.get("text").and_then(Value::as_str).is_some() => {}
            "image" | "audio" => validate_media(block, policy.max_media_bytes)?,
            "tool_use" => validate_tool_use(block)?,
            "tool_result" => validate_tool_result(block, policy, total)?,
            "resource" | "resource_link" => {
                return Err(invalid(
                    "Sampling resources are allowed only inside Tool results",
                ));
            }
            "runifold/content" => validate_extension(block, policy.max_media_bytes)?,
            kind if !kind.trim().is_empty() => {}
            _ => return Err(invalid("Sampling content type must not be blank")),
        }
    }
    Ok(())
}

fn validate_tool_use(block: &ContentBlock) -> Result<(), SamplingError> {
    required_block_string(block, "id")?;
    required_block_string(block, "name")?;
    if !block.fields.get("input").is_some_and(Value::is_object) {
        return Err(invalid("Sampling Tool use input must be an object"));
    }
    if block
        .fields
        .get("_meta")
        .is_some_and(|value| !value.is_object())
    {
        return Err(invalid("Sampling Tool use _meta must be an object"));
    }
    Ok(())
}

fn validate_tool_result(
    block: &ContentBlock,
    policy: &SamplingPolicy,
    total: &mut usize,
) -> Result<(), SamplingError> {
    required_block_string(block, "toolUseId")?;
    let content = block
        .fields
        .get("content")
        .ok_or_else(|| invalid("Sampling Tool result content is missing"))?;
    let blocks = decode_tool_result_content(content)?;
    if blocks.is_empty() {
        return Err(invalid("Sampling Tool result content must not be empty"));
    }
    if block
        .fields
        .get("structuredContent")
        .is_some_and(|value| !value.is_object())
    {
        return Err(invalid(
            "Sampling Tool result structuredContent must be an object",
        ));
    }
    if block
        .fields
        .get("isError")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err(invalid("Sampling Tool result isError must be boolean"));
    }
    if block
        .fields
        .get("_meta")
        .is_some_and(|value| !value.is_object())
    {
        return Err(invalid("Sampling Tool result _meta must be an object"));
    }
    add_blocks(total, blocks.len(), policy.max_content_blocks)?;
    for nested in &blocks {
        match nested.kind.as_str() {
            "text" if nested.fields.get("text").and_then(Value::as_str).is_some() => {}
            "image" | "audio" => validate_media(nested, policy.max_media_bytes)?,
            "resource_link" => validate_resource_link(nested)?,
            "resource" => validate_embedded_resource(nested, policy.max_media_bytes)?,
            "runifold/content" => validate_extension(nested, policy.max_media_bytes)?,
            kind if !kind.trim().is_empty() && kind != "tool_use" && kind != "tool_result" => {}
            _ => return Err(invalid("Sampling Tool result contains an invalid block")),
        }
    }
    Ok(())
}

fn add_blocks(total: &mut usize, amount: usize, maximum: usize) -> Result<(), SamplingError> {
    *total = total.saturating_add(amount);
    if *total > maximum {
        return Err(limit("Sampling content-block limit exceeded"));
    }
    Ok(())
}

fn decode_tool_result_content(value: &Value) -> Result<Vec<ContentBlock>, SamplingError> {
    let values = value
        .as_array()
        .cloned()
        .ok_or_else(|| invalid("Sampling Tool result content must be an array"))?;
    values
        .into_iter()
        .map(|value| {
            serde_json::from_value(value)
                .map_err(|_| invalid("Sampling Tool result contains a malformed content block"))
        })
        .collect()
}

fn required_block_string<'a>(
    block: &'a ContentBlock,
    field: &str,
) -> Result<&'a str, SamplingError> {
    block
        .fields
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid(format!("Sampling content is missing `{field}`")))
}

fn output_block_string<'a>(block: &'a ContentBlock, field: &str) -> Result<&'a str, SamplingError> {
    required_block_string(block, field)
        .map_err(|error| SamplingError::new(SamplingErrorKind::InvalidOutput, error.message))
}

fn validate_media(block: &ContentBlock, max_media_bytes: usize) -> Result<(), SamplingError> {
    let data = block
        .fields
        .get("data")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("Sampling media data is missing"))?;
    let mime = block
        .fields
        .get("mimeType")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("Sampling media MIME type is missing"))?;
    if mime.trim().is_empty() {
        return Err(invalid("Sampling media MIME type is blank"));
    }
    validate_base64(data, max_media_bytes)
}

fn validate_resource_link(block: &ContentBlock) -> Result<(), SamplingError> {
    required_block_string(block, "uri")?;
    required_block_string(block, "name")?;
    for field in ["title", "description", "mimeType"] {
        if block
            .fields
            .get(field)
            .is_some_and(|value| !value.is_string())
        {
            return Err(invalid(format!(
                "Sampling resource link `{field}` must be a string"
            )));
        }
    }
    if block
        .fields
        .get("size")
        .is_some_and(|value| value.as_u64().is_none())
    {
        return Err(invalid(
            "Sampling resource link size must be an unsigned integer",
        ));
    }
    Ok(())
}

fn validate_embedded_resource(
    block: &ContentBlock,
    max_media_bytes: usize,
) -> Result<(), SamplingError> {
    let resource = block
        .fields
        .get("resource")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("Sampling embedded resource is missing object `resource`"))?;
    object_string(resource, "uri")?;
    if resource
        .get("mimeType")
        .is_some_and(|value| !value.is_string())
    {
        return Err(invalid(
            "Sampling embedded resource MIME type must be a string",
        ));
    }
    match (resource.get("text"), resource.get("blob")) {
        (Some(Value::String(_)), None) => Ok(()),
        (None, Some(Value::String(data))) => validate_base64(data, max_media_bytes),
        _ => Err(invalid(
            "Sampling embedded resource requires exactly one text or blob payload",
        )),
    }
}

fn validate_extension(block: &ContentBlock, max_media_bytes: usize) -> Result<(), SamplingError> {
    let content: ContentPart = serde_json::from_value(
        block
            .fields
            .get("content")
            .cloned()
            .ok_or_else(|| invalid("Runifold Sampling content is missing"))?,
    )
    .map_err(|_| invalid("Runifold Sampling content is malformed"))?;
    match content {
        ContentPart::Text { .. } | ContentPart::Refusal { .. } | ContentPart::Citation(_) => Ok(()),
        ContentPart::ResourceLink { uri, name, .. } => {
            if uri.trim().is_empty() || name.trim().is_empty() {
                return Err(invalid(
                    "Runifold Sampling resource links require a URI and name",
                ));
            }
            Ok(())
        }
        ContentPart::Image { source }
        | ContentPart::Audio { source }
        | ContentPart::Document { source, .. } => {
            validate_extension_media(&source, max_media_bytes)
        }
        _ => Err(invalid(
            "Runifold Sampling extension contains private or recursive content",
        )),
    }
}

fn validate_extension_media(
    source: &MediaSource,
    max_media_bytes: usize,
) -> Result<(), SamplingError> {
    match source {
        MediaSource::Base64 { media_type, data } => {
            if media_type.trim().is_empty() {
                return Err(invalid("Runifold Sampling media MIME type is blank"));
            }
            validate_base64(data, max_media_bytes)
        }
        MediaSource::Url { url, media_type } => {
            if url.trim().is_empty()
                || media_type
                    .as_ref()
                    .is_some_and(|value| value.trim().is_empty())
            {
                return Err(invalid("Runifold Sampling media URL or MIME type is blank"));
            }
            Ok(())
        }
        MediaSource::Artifact { .. } | MediaSource::ProviderFile { .. } => Err(invalid(
            "Runifold Sampling extensions cannot expose host or Provider file references",
        )),
        _ => Err(invalid("Runifold Sampling media source is unsupported")),
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
        .ok_or_else(|| invalid(format!("Sampling resource is missing `{field}`")))
}

fn validate_base64(data: &str, max_media_bytes: usize) -> Result<(), SamplingError> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|_| invalid("Sampling media is not valid base64"))?;
    if bytes.len() > max_media_bytes {
        return Err(limit("Sampling decoded-media limit exceeded"));
    }
    Ok(())
}

fn validate_encoded_size<T: Serialize>(
    value: &T,
    max_bytes: usize,
    label: &str,
) -> Result<(), SamplingError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| invalid(format!("{label} cannot be encoded")))?
        .len();
    if bytes > max_bytes {
        return Err(limit(format!("{label} exceeds the serialized-size limit")));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> SamplingError {
    SamplingError::new(SamplingErrorKind::InvalidRequest, message)
}

fn limit(message: impl Into<String>) -> SamplingError {
    SamplingError::new(SamplingErrorKind::LimitExceeded, message)
}

fn output(message: impl Into<String>) -> SamplingError {
    SamplingError::new(SamplingErrorKind::InvalidOutput, message)
}
