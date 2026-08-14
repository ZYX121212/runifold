//! `OpenAI` Responses request encoding.

use serde_json::{Map, Value, json};

use crate::content_projection::{
    encode_content_envelope, validate_image_media_type, validate_inline_image,
    validate_inline_media, validate_media_url, validate_optional_media_type,
};

use runifold_model::{
    ContentPart, MediaSource, ModelError, ModelErrorKind, ModelRequest, OutputFormat, ResponseMode,
    Role, ToolChoice, ToolResult,
};

/// Encodes a canonical request as an `OpenAI` Responses API request.
///
/// # Errors
///
/// Returns [`ModelError`] when a canonical feature has no lossless Responses
/// API representation, or when provider options try to replace fields owned by
/// the adapter.
pub fn encode_request(request: &ModelRequest) -> Result<Value, ModelError> {
    encode_request_for(request, "openai")
}

pub(crate) fn encode_request_for(
    request: &ModelRequest,
    provider: &str,
) -> Result<Value, ModelError> {
    if request.messages.is_empty() {
        return Err(invalid("a model request must contain at least one message"));
    }
    if request.generation.seed.is_some() {
        return Err(unsupported(
            "Responses API adapter does not support deterministic seed",
        ));
    }
    if !request.generation.stop.is_empty() {
        return Err(unsupported(
            "Responses API adapter does not support stop sequences",
        ));
    }

    let mut body = Map::new();
    body.insert("model".into(), Value::String(request.model.name.clone()));
    body.insert("input".into(), Value::Array(encode_input(request)?));
    body.insert(
        "stream".into(),
        Value::Bool(matches!(
            request.selected_response_mode(),
            ResponseMode::Streaming
        )),
    );

    insert_optional(
        &mut body,
        "temperature",
        request.generation.temperature.map(Value::from),
    );
    insert_optional(
        &mut body,
        "top_p",
        request.generation.top_p.map(Value::from),
    );
    insert_optional(
        &mut body,
        "max_output_tokens",
        request.generation.max_output_tokens.map(Value::from),
    );

    let provider_tools = encode_provider_tools(request, provider)?;
    if !request.tools.is_empty() || !provider_tools.is_empty() {
        let mut tools = request
            .tools
            .iter()
            .map(|tool| -> Result<Value, ModelError> {
                let strict = function_tool_strict(tool, provider)?;
                Ok(json!({
                    "type": "function",
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.input_schema,
                    "strict": strict
                }))
            })
            .collect::<Result<Vec<_>, _>>()?;
        tools.extend(provider_tools);
        body.insert("tools".into(), Value::Array(tools));
    }
    body.insert(
        "tool_choice".into(),
        encode_tool_choice(&request.tool_choice)?,
    );
    body.insert("text".into(), encode_output_format(&request.output_format)?);
    merge_provider_options(&mut body, request, provider)?;

    Ok(Value::Object(body))
}

fn encode_provider_tools(request: &ModelRequest, provider: &str) -> Result<Vec<Value>, ModelError> {
    request
        .provider_tools()
        .into_iter()
        .map(|tool| {
            if tool.provider != provider && tool.provider != "openai-compatible" {
                return Err(invalid(format!(
                    "provider-native tool for `{}` cannot be sent to `{provider}`",
                    tool.provider
                )));
            }
            if tool.options.contains_key("type") {
                return Err(invalid(
                    "provider-native tool option `type` conflicts with its declared tool type",
                ));
            }
            let mut encoded =
                Map::from_iter([("type".into(), Value::String(tool.tool_type.clone()))]);
            encoded.extend(tool.options.clone());
            Ok(Value::Object(encoded))
        })
        .collect()
}

fn encode_input(request: &ModelRequest) -> Result<Vec<Value>, ModelError> {
    let mut input = Vec::new();
    for message in &request.messages {
        let mut message_content = Vec::new();
        for part in &message.content {
            match part {
                ContentPart::Text { text } => {
                    let part_type = if message.role == Role::Assistant {
                        "output_text"
                    } else {
                        "input_text"
                    };
                    message_content.push(json!({"type": part_type, "text": text}));
                }
                ContentPart::Image { source } => {
                    require_input_role(message.role, "image")?;
                    message_content.push(encode_image(source, &request.model.provider)?);
                }
                ContentPart::Document { source, name } => {
                    require_input_role(message.role, "document")?;
                    message_content.push(encode_document(
                        source,
                        name.as_deref(),
                        &request.model.provider,
                    )?);
                }
                ContentPart::ToolCall(call) => {
                    flush_message(&mut input, message.role, &mut message_content)?;
                    let arguments = call
                        .raw_arguments
                        .clone()
                        .unwrap_or_else(|| call.arguments.to_string());
                    input.push(encode_function_call(
                        call,
                        arguments,
                        &request.model.provider,
                    )?);
                }
                ContentPart::ToolResult(result) => {
                    flush_message(&mut input, message.role, &mut message_content)?;
                    input.push(encode_tool_result(result, &request.model.provider)?);
                }
                ContentPart::ProviderOpaque(data)
                    if (data.provider == request.model.provider
                        || data.provider == "openai-compatible")
                        && data.kind == "input_item" =>
                {
                    flush_message(&mut input, message.role, &mut message_content)?;
                    input.push(data.value.clone());
                }
                ContentPart::Audio { .. } | ContentPart::ResourceLink { .. } => {
                    require_input_role(message.role, "projected rich content")?;
                    message_content.push(json!({
                        "type": "input_text",
                        "text": encode_content_envelope(part)?
                    }));
                }
                ContentPart::Reasoning(_) => {
                    return Err(unsupported(
                        "reasoning round trips require an OpenAI input_item escape hatch",
                    ));
                }
                ContentPart::Refusal { .. } => {
                    return Err(unsupported(
                        "refusal round trips are not accepted as generic message input",
                    ));
                }
                ContentPart::Citation(_) => {
                    return Err(unsupported("citations cannot be sent as message input"));
                }
                ContentPart::ProviderOpaque(_) => {
                    return Err(unsupported(
                        "opaque content belongs to another provider or unsupported OpenAI kind",
                    ));
                }
                _ => {
                    return Err(unsupported(
                        "content variant is newer than this OpenAI adapter",
                    ));
                }
            }
        }
        flush_message(&mut input, message.role, &mut message_content)?;
    }
    Ok(input)
}

fn encode_function_call(
    call: &runifold_model::ToolCall,
    arguments: String,
    provider: &str,
) -> Result<Value, ModelError> {
    let mut item = Map::from_iter([
        ("type".into(), Value::String("function_call".into())),
        ("call_id".into(), Value::String(call.id.clone())),
        ("name".into(), Value::String(call.name.clone())),
        ("arguments".into(), Value::String(arguments)),
        ("status".into(), Value::String("completed".into())),
    ]);
    if let Some(id) = provider_metadata_string(&call.metadata, provider, "id")? {
        item.insert("id".into(), Value::String(id));
    }
    if let Some(status) = provider_metadata_string(&call.metadata, provider, "status")?
        && status != "completed"
    {
        return Err(invalid(format!(
            "completed tool call `{}` cannot replay provider status `{status}`",
            call.id
        )));
    }
    if let Some(caller) = provider_caller(&call.metadata, provider)? {
        item.insert("caller".into(), caller);
    }
    Ok(Value::Object(item))
}

fn provider_caller(
    metadata: &runifold_model::ExtensionMap,
    provider: &str,
) -> Result<Option<Value>, ModelError> {
    let compatible_key = "openai-compatible.caller";
    let provider_key = format!("{provider}.caller");
    let Some(caller) = metadata
        .get(&provider_key)
        .or_else(|| metadata.get(compatible_key))
    else {
        return Ok(None);
    };
    let caller_object = caller.as_object().ok_or_else(|| {
        invalid(format!(
            "tool correlation metadata `{provider_key}` must be an object"
        ))
    })?;
    if caller_object.get("type").and_then(Value::as_str) != Some("program")
        || caller_object
            .get("caller_id")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
    {
        return Err(invalid(format!(
            "tool correlation metadata `{provider_key}` must identify a program caller"
        )));
    }
    Ok(Some(caller.clone()))
}

fn provider_metadata_string(
    metadata: &runifold_model::ExtensionMap,
    provider: &str,
    field: &str,
) -> Result<Option<String>, ModelError> {
    let compatible_key = format!("openai-compatible.{field}");
    let provider_key = format!("{provider}.{field}");
    let value = metadata
        .get(&provider_key)
        .or_else(|| metadata.get(&compatible_key));
    value
        .map(|value| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                invalid(format!(
                    "tool-call metadata `{provider_key}` must be a string"
                ))
            })
        })
        .transpose()
}

fn flush_message(
    input: &mut Vec<Value>,
    role: Role,
    content: &mut Vec<Value>,
) -> Result<(), ModelError> {
    if content.is_empty() {
        return Ok(());
    }
    let mut message = json!({
        "type": "message",
        "role": role_name(role)?,
        "content": std::mem::take(content)
    });
    if role == Role::Assistant {
        message["status"] = Value::String("completed".into());
    }
    input.push(message);
    Ok(())
}

fn encode_image(source: &MediaSource, provider: &str) -> Result<Value, ModelError> {
    if let MediaSource::ProviderFile {
        provider: owner,
        file_id,
    } = source
    {
        require_provider_file_owner(owner, provider)?;
        return Ok(json!({"type": "input_image", "file_id": file_id}));
    }
    let image_url = match source {
        MediaSource::Url { url, media_type } => {
            validate_media_url(url, &["http", "https"])?;
            validate_image_media_type(media_type.as_deref())?;
            url.clone()
        }
        MediaSource::Base64 { media_type, data } => {
            validate_inline_image(media_type, data)?;
            format!("data:{media_type};base64,{data}")
        }
        MediaSource::Artifact { .. } => {
            return Err(unsupported(
                "artifact images must be resolved before provider invocation",
            ));
        }
        _ => {
            return Err(unsupported(
                "image source is newer than this OpenAI adapter",
            ));
        }
    };
    Ok(json!({"type": "input_image", "image_url": image_url}))
}

fn encode_document(
    source: &MediaSource,
    name: Option<&str>,
    provider: &str,
) -> Result<Value, ModelError> {
    match source {
        MediaSource::Url { url, media_type } => {
            validate_media_url(url, &["http", "https"])?;
            validate_optional_media_type(media_type.as_deref())?;
            Ok(json!({
                "type": "input_file",
                "file_url": url
            }))
        }
        MediaSource::Base64 { media_type, data } => {
            validate_inline_media(media_type, data)?;
            Ok(json!({
                "type": "input_file",
                "filename": name.unwrap_or("document"),
                "file_data": format!("data:{media_type};base64,{data}")
            }))
        }
        MediaSource::Artifact { .. } => Err(unsupported(
            "artifact documents must be resolved before provider invocation",
        )),
        MediaSource::ProviderFile {
            provider: owner,
            file_id,
        } => {
            require_provider_file_owner(owner, provider)?;
            Ok(json!({"type": "input_file", "file_id": file_id}))
        }
        _ => Err(unsupported(
            "document source is newer than this OpenAI adapter",
        )),
    }
}

fn require_provider_file_owner(owner: &str, provider: &str) -> Result<(), ModelError> {
    if owner == provider || owner == "openai-compatible" {
        Ok(())
    } else {
        Err(invalid(format!(
            "provider file owned by `{owner}` cannot be sent to `{provider}`"
        )))
    }
}

fn encode_tool_result(result: &ToolResult, provider: &str) -> Result<Value, ModelError> {
    let mut text = Vec::new();
    let mut rich = Vec::new();
    for part in &result.content {
        match part {
            ContentPart::Text { text: value } => text.push(value.clone()),
            ContentPart::Image { source } => match encode_image(source, provider) {
                Ok(image) => rich.push(image),
                Err(error) if error.kind == ModelErrorKind::UnsupportedFeature => {
                    text.push(encode_content_envelope(part)?);
                }
                Err(error) => return Err(error),
            },
            ContentPart::Document { source, name } => {
                match encode_document(source, name.as_deref(), provider) {
                    Ok(document) => rich.push(document),
                    Err(error) if error.kind == ModelErrorKind::UnsupportedFeature => {
                        text.push(encode_content_envelope(part)?);
                    }
                    Err(error) => return Err(error),
                }
            }
            ContentPart::ResourceLink {
                uri,
                name,
                media_type,
                ..
            } => {
                text.push(encode_content_envelope(part)?);
                let source = MediaSource::Url {
                    url: uri.clone(),
                    media_type: media_type.clone(),
                };
                if media_type
                    .as_deref()
                    .is_some_and(|kind| kind.starts_with("image/"))
                {
                    if let Ok(image) = encode_image(&source, provider) {
                        rich.push(image);
                    }
                } else if let Ok(document) = encode_document(&source, Some(name), provider) {
                    rich.push(document);
                }
            }
            _ => text.push(encode_content_envelope(part)?),
        }
    }
    if let Some(structured) = &result.structured_content {
        let encoded = structured.to_string();
        if !text.iter().any(|item| item == &encoded) {
            text.push(encoded);
        }
    }
    if result.is_error {
        text.insert(0, "Tool execution reported an application error.".into());
    }
    let output = if rich.is_empty() {
        Value::String(text.join("\n"))
    } else {
        let mut content = text
            .into_iter()
            .map(|text| json!({"type":"input_text","text":text}))
            .collect::<Vec<_>>();
        content.extend(rich);
        Value::Array(content)
    };
    let mut item = json!({
        "type": "function_call_output",
        "call_id": result.call_id,
        "output": output
    });
    if let Some(caller) = provider_caller(&result.metadata, provider)? {
        item["caller"] = caller;
    }
    Ok(item)
}

fn encode_tool_choice(choice: &ToolChoice) -> Result<Value, ModelError> {
    Ok(match choice {
        ToolChoice::Auto => Value::String("auto".into()),
        ToolChoice::None => Value::String("none".into()),
        ToolChoice::Required => Value::String("required".into()),
        ToolChoice::Named { name } => json!({"type": "function", "name": name}),
        _ => {
            return Err(unsupported("tool choice is newer than this OpenAI adapter"));
        }
    })
}

fn encode_output_format(format: &OutputFormat) -> Result<Value, ModelError> {
    let format = match format {
        OutputFormat::Text => json!({"type": "text"}),
        OutputFormat::Json => json!({"type": "json_object"}),
        OutputFormat::JsonSchema {
            name,
            schema,
            strict,
        } => {
            if *strict {
                validate_strict_schema(schema)?;
            }
            json!({
                "type": "json_schema",
                "name": name,
                "schema": schema,
                "strict": strict
            })
        }
        _ => {
            return Err(unsupported(
                "output format is newer than this OpenAI adapter",
            ));
        }
    };
    Ok(json!({"format": format}))
}

pub(crate) fn function_tool_strict(
    tool: &runifold_model::ToolSpec,
    provider: &str,
) -> Result<bool, ModelError> {
    let provider_key = format!("{provider}.strict");
    let strict = tool
        .metadata
        .get(&provider_key)
        .or_else(|| tool.metadata.get("openai-compatible.strict"));
    let Some(strict) = strict else {
        return Ok(false);
    };
    let strict = strict
        .as_bool()
        .ok_or_else(|| invalid(format!("tool metadata `{provider_key}` must be a boolean")))?;
    if strict {
        validate_strict_schema(&tool.input_schema).map_err(|error| {
            invalid(format!(
                "tool `{}` requested OpenAI strict mode with an incompatible JSON Schema: {}",
                tool.name, error.message
            ))
        })?;
    }
    Ok(strict)
}

#[derive(Default)]
struct StrictSchemaStats {
    properties: usize,
    enum_values: usize,
    string_chars: usize,
}

pub(crate) fn validate_strict_schema(schema: &Value) -> Result<(), ModelError> {
    if schema.get("type").and_then(Value::as_str) != Some("object") || schema.get("anyOf").is_some()
    {
        return Err(invalid(
            "strict schema root must be an object and cannot use anyOf",
        ));
    }
    let mut stats = StrictSchemaStats::default();
    validate_strict_schema_node(schema, 1, &mut stats)?;
    if stats.properties > 5_000 {
        return Err(invalid("strict schema exceeds 5000 object properties"));
    }
    if stats.enum_values > 1_000 {
        return Err(invalid("strict schema exceeds 1000 enum values"));
    }
    if stats.string_chars > 120_000 {
        return Err(invalid("strict schema exceeds the 120000 character limit"));
    }
    Ok(())
}

const STRICT_SCHEMA_KEYWORDS: &[&str] = &[
    "$defs",
    "$ref",
    "additionalProperties",
    "anyOf",
    "const",
    "description",
    "enum",
    "exclusiveMaximum",
    "exclusiveMinimum",
    "format",
    "items",
    "maxItems",
    "maximum",
    "minItems",
    "minimum",
    "multipleOf",
    "pattern",
    "properties",
    "required",
    "title",
    "type",
];

fn validate_strict_schema_node(
    schema: &Value,
    object_depth: usize,
    stats: &mut StrictSchemaStats,
) -> Result<(), ModelError> {
    let Some(object) = schema.as_object() else {
        return Err(invalid("strict schema nodes must be objects"));
    };
    validate_schema_header(object)?;
    validate_schema_constraints(object)?;
    validate_schema_subschemas(object, object_depth, stats)?;
    let is_object = schema_has_type(schema, "object");
    let is_array = schema_has_type(schema, "array");
    validate_keyword_placements(schema, object)?;
    if is_object {
        validate_object_schema(object, object_depth, stats)?;
    }
    if is_array {
        validate_array_schema(object, object_depth, stats)?;
    }
    accumulate_schema_values(object, stats)
}

fn validate_schema_header(object: &Map<String, Value>) -> Result<(), ModelError> {
    if let Some(keyword) = object
        .keys()
        .find(|keyword| !STRICT_SCHEMA_KEYWORDS.contains(&keyword.as_str()))
    {
        return Err(invalid(format!(
            "strict schema keyword `{keyword}` is not supported"
        )));
    }
    let Some(types) = object.get("type") else {
        if ["$ref", "anyOf", "enum", "const"]
            .iter()
            .any(|keyword| object.contains_key(*keyword))
        {
            return Ok(());
        }
        return Err(invalid(
            "strict schema node must declare type, $ref, anyOf, enum, or const",
        ));
    };
    let valid = match types {
        Value::String(kind) => strict_schema_type(kind),
        Value::Array(kinds) => {
            !kinds.is_empty()
                && kinds
                    .iter()
                    .all(|kind| kind.as_str().is_some_and(strict_schema_type))
                && kinds
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    == kinds.len()
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(invalid("strict schema contains an unsupported `type`"))
    }
}

fn validate_schema_constraints(object: &Map<String, Value>) -> Result<(), ModelError> {
    for keyword in ["description", "title", "pattern"] {
        if object.get(keyword).is_some_and(|value| !value.is_string()) {
            return Err(invalid(format!(
                "strict schema `{keyword}` must be a string"
            )));
        }
    }
    if let Some(reference) = object.get("$ref") {
        let reference = reference
            .as_str()
            .ok_or_else(|| invalid("strict schema `$ref` must be a string"))?;
        if reference != "#" && !reference.starts_with("#/") {
            return Err(invalid("strict schema `$ref` must be a local reference"));
        }
    }
    validate_schema_format(object)?;
    for keyword in [
        "exclusiveMaximum",
        "exclusiveMinimum",
        "maximum",
        "minimum",
        "multipleOf",
    ] {
        if object.get(keyword).is_some_and(|value| !value.is_number()) {
            return Err(invalid(format!(
                "strict schema `{keyword}` must be a number"
            )));
        }
    }
    for keyword in ["maxItems", "minItems"] {
        if object
            .get(keyword)
            .is_some_and(|value| value.as_u64().is_none())
        {
            return Err(invalid(format!(
                "strict schema `{keyword}` must be a non-negative integer"
            )));
        }
    }
    if object
        .get("multipleOf")
        .and_then(Value::as_f64)
        .is_some_and(|value| value <= 0.0)
    {
        return Err(invalid("strict schema `multipleOf` must be positive"));
    }
    if let (Some(minimum), Some(maximum)) = (
        object.get("minItems").and_then(Value::as_u64),
        object.get("maxItems").and_then(Value::as_u64),
    ) && minimum > maximum
    {
        return Err(invalid("strict schema `minItems` cannot exceed `maxItems`"));
    }
    Ok(())
}

fn validate_schema_format(object: &Map<String, Value>) -> Result<(), ModelError> {
    let Some(format) = object.get("format") else {
        return Ok(());
    };
    let supported = format.as_str().is_some_and(|format| {
        matches!(
            format,
            "date-time"
                | "time"
                | "date"
                | "duration"
                | "email"
                | "hostname"
                | "ipv4"
                | "ipv6"
                | "uuid"
        )
    });
    if supported {
        Ok(())
    } else {
        Err(invalid("strict schema contains an unsupported `format`"))
    }
}

fn validate_schema_subschemas(
    object: &Map<String, Value>,
    object_depth: usize,
    stats: &mut StrictSchemaStats,
) -> Result<(), ModelError> {
    if let Some(definitions) = object.get("$defs") {
        let definitions = definitions
            .as_object()
            .ok_or_else(|| invalid("strict schema `$defs` must be an object"))?;
        for (name, value) in definitions {
            stats.string_chars = stats.string_chars.saturating_add(name.chars().count());
            validate_strict_schema_node(value, object_depth, stats)?;
        }
    }
    if let Some(branches) = object.get("anyOf") {
        let branches = branches
            .as_array()
            .filter(|branches| !branches.is_empty())
            .ok_or_else(|| invalid("strict schema `anyOf` must be a non-empty array"))?;
        for branch in branches {
            validate_strict_schema_node(branch, object_depth, stats)?;
        }
    }
    Ok(())
}

fn validate_keyword_placements(
    schema: &Value,
    object: &Map<String, Value>,
) -> Result<(), ModelError> {
    let placements = [
        (
            schema_has_type(schema, "object"),
            &["additionalProperties", "properties", "required"][..],
            "object",
        ),
        (
            schema_has_type(schema, "array"),
            &["items", "maxItems", "minItems"][..],
            "array",
        ),
        (
            schema_has_type(schema, "string"),
            &["format", "pattern"][..],
            "string",
        ),
        (
            schema_has_type(schema, "number") || schema_has_type(schema, "integer"),
            &[
                "exclusiveMaximum",
                "exclusiveMinimum",
                "maximum",
                "minimum",
                "multipleOf",
            ][..],
            "numeric",
        ),
    ];
    for (valid_type, keywords, kind) in placements {
        if !valid_type && keywords.iter().any(|keyword| object.contains_key(*keyword)) {
            return Err(invalid(format!(
                "strict schema {kind} keywords require a compatible type"
            )));
        }
    }
    Ok(())
}

fn validate_object_schema(
    object: &Map<String, Value>,
    object_depth: usize,
    stats: &mut StrictSchemaStats,
) -> Result<(), ModelError> {
    if object_depth > 10 {
        return Err(invalid("strict schema exceeds 10 object nesting levels"));
    }
    if object.get("additionalProperties") != Some(&Value::Bool(false)) {
        return Err(invalid(
            "strict schema objects require `additionalProperties: false`",
        ));
    }
    let properties = object
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("strict schema objects require `properties`"))?;
    let required = object
        .get("required")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("strict schema objects require `required`"))?;
    let required_count = required.len();
    let required = required
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| invalid("strict schema required names must be strings"))
        })
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    if required.len() != required_count
        || required.len() != properties.len()
        || properties
            .keys()
            .any(|name| !required.contains(name.as_str()))
    {
        return Err(invalid("strict schema requires every object property"));
    }
    stats.properties = stats.properties.saturating_add(properties.len());
    for (name, value) in properties {
        stats.string_chars = stats.string_chars.saturating_add(name.chars().count());
        validate_strict_schema_node(value, object_depth + 1, stats)?;
    }
    Ok(())
}

fn validate_array_schema(
    object: &Map<String, Value>,
    object_depth: usize,
    stats: &mut StrictSchemaStats,
) -> Result<(), ModelError> {
    let items = object
        .get("items")
        .ok_or_else(|| invalid("strict schema arrays require `items`"))?;
    validate_strict_schema_node(items, object_depth, stats)
}

fn accumulate_schema_values(
    object: &Map<String, Value>,
    stats: &mut StrictSchemaStats,
) -> Result<(), ModelError> {
    if let Some(values) = object.get("enum") {
        let values = values
            .as_array()
            .filter(|values| !values.is_empty())
            .ok_or_else(|| invalid("strict schema `enum` must be a non-empty array"))?;
        stats.enum_values = stats.enum_values.saturating_add(values.len());
        let enum_string_chars = values
            .iter()
            .filter_map(Value::as_str)
            .map(|value| value.chars().count())
            .sum::<usize>();
        if values.len() > 250 && enum_string_chars > 15_000 {
            return Err(invalid(
                "strict schema enum exceeds the 15000 character limit",
            ));
        }
        stats.string_chars = stats.string_chars.saturating_add(enum_string_chars);
    }
    if let Some(value) = object.get("const").and_then(Value::as_str) {
        stats.string_chars = stats.string_chars.saturating_add(value.chars().count());
    }
    Ok(())
}

fn strict_schema_type(kind: &str) -> bool {
    matches!(
        kind,
        "string" | "number" | "integer" | "boolean" | "object" | "array" | "null"
    )
}

fn schema_has_type(schema: &Value, expected: &str) -> bool {
    schema.get("type").is_some_and(|kind| {
        kind.as_str() == Some(expected)
            || kind
                .as_array()
                .is_some_and(|kinds| kinds.iter().any(|kind| kind.as_str() == Some(expected)))
    })
}

fn merge_provider_options(
    body: &mut Map<String, Value>,
    request: &ModelRequest,
    provider: &str,
) -> Result<(), ModelError> {
    for namespace in std::iter::once("openai-compatible")
        .chain((provider != "openai-compatible").then_some(provider))
    {
        let Some(options) = request.provider_options.get(namespace) else {
            continue;
        };
        let options = options
            .as_object()
            .ok_or_else(|| invalid(format!("provider_options.{namespace} must be an object")))?;
        for (key, value) in options {
            if body.contains_key(key) {
                return Err(invalid(format!(
                    "provider option `{key}` conflicts with an adapter-owned field"
                )));
            }
            body.insert(key.clone(), value.clone());
        }
    }
    Ok(())
}

fn require_input_role(role: Role, content: &str) -> Result<(), ModelError> {
    if matches!(role, Role::User | Role::System) {
        Ok(())
    } else {
        Err(unsupported(format!(
            "{content} content is only supported on input roles"
        )))
    }
}

fn role_name(role: Role) -> Result<&'static str, ModelError> {
    Ok(match role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => {
            return Err(unsupported(
                "tool messages must contain canonical tool-result parts",
            ));
        }
        _ => return Err(unsupported("role is newer than this OpenAI adapter")),
    })
}

fn insert_optional(body: &mut Map<String, Value>, name: &str, value: Option<Value>) {
    if let Some(value) = value {
        body.insert(name.into(), value);
    }
}

fn invalid(message: impl Into<String>) -> ModelError {
    ModelError::local(ModelErrorKind::InvalidRequest, message)
}

fn unsupported(message: impl Into<String>) -> ModelError {
    ModelError::local(ModelErrorKind::UnsupportedFeature, message)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use runifold_model::{
        ContentPart, MediaSource, Message, ModelErrorKind, ModelRef, ModelRequest, OutputFormat,
        ProviderToolSpec, ResponseMode, Role, ToolCall, ToolResult, ToolSpec,
    };

    use super::encode_request;

    use crate::content_projection::decode_content_envelope;

    #[test]
    fn encodes_text_tools_and_structured_output() {
        let request = ModelRequest::new(
            ModelRef::new("openai", "test-model"),
            Message::user("weather?"),
        )
        .tool(ToolSpec {
            name: "weather".into(),
            description: "Get weather".into(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {"city": {"type": "string"}},
                "required": ["city"],
                "additionalProperties": false
            }),
            output_schema: None,
            metadata: BTreeMap::new(),
        })
        .output_format(OutputFormat::JsonSchema {
            name: "answer".into(),
            schema: serde_json::json!({
                "type": "object",
                "properties": {},
                "required": [],
                "additionalProperties": false
            }),
            strict: true,
        });

        let encoded = encode_request(&request).unwrap();

        assert_eq!(encoded["model"], "test-model");
        assert_eq!(encoded["stream"], true);
        assert_eq!(encoded["tools"][0]["name"], "weather");
        assert_eq!(encoded["tools"][0]["strict"], false);
        assert_eq!(encoded["text"]["format"]["type"], "json_schema");
        assert_eq!(encoded["input"][0]["content"][0]["type"], "input_text");
    }

    #[test]
    fn function_strictness_is_explicit_and_validated_locally() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
            "additionalProperties": false
        });
        let strict_tool = ToolSpec {
            name: "weather".into(),
            description: "Get weather".into(),
            input_schema: schema.clone(),
            output_schema: None,
            metadata: BTreeMap::from([("openai.strict".into(), serde_json::json!(true))]),
        };
        let request =
            ModelRequest::new(ModelRef::new("openai", "model"), Message::user("weather?"))
                .tool(strict_tool);
        assert_eq!(
            encode_request(&request).unwrap()["tools"][0]["strict"],
            true
        );

        let invalid_tool = ToolSpec {
            name: "weather".into(),
            description: "Get weather".into(),
            input_schema: serde_json::json!({
                "type":"object",
                "properties":{"city":{"type":"string"}}
            }),
            output_schema: None,
            metadata: BTreeMap::from([("openai.strict".into(), serde_json::json!(true))]),
        };
        let invalid =
            ModelRequest::new(ModelRef::new("openai", "model"), Message::user("weather?"))
                .tool(invalid_tool);
        assert_eq!(
            encode_request(&invalid).unwrap_err().kind,
            ModelErrorKind::InvalidRequest
        );
    }

    #[test]
    fn strict_schema_rejects_unsupported_keywords_and_nested_optional_fields() {
        for schema in [
            serde_json::json!({
                "type":"object",
                "properties":{"value":{"type":"string"}},
                "required":["value"],
                "additionalProperties":false,
                "allOf":[]
            }),
            serde_json::json!({
                "type":"object",
                "properties":{
                    "nested":{
                        "type":"object",
                        "properties":{"value":{"type":"string"}},
                        "required":[],
                        "additionalProperties":false
                    }
                },
                "required":["nested"],
                "additionalProperties":false
            }),
        ] {
            let tool = ToolSpec {
                name: "strict_tool".into(),
                description: "strict".into(),
                input_schema: schema,
                output_schema: None,
                metadata: BTreeMap::from([("openai.strict".into(), serde_json::json!(true))]),
            };
            let request = ModelRequest::new(ModelRef::new("openai", "model"), Message::user("run"))
                .tool(tool);
            assert_eq!(
                encode_request(&request).unwrap_err().kind,
                ModelErrorKind::InvalidRequest
            );
        }
    }

    #[test]
    fn strict_schema_rejects_invalid_keyword_values_and_placements() {
        for property_schema in [
            serde_json::json!({"type":"string","format":"uri"}),
            serde_json::json!({"type":"string","minimum":0}),
            serde_json::json!({"type":"array","items":{"type":"string"},"minItems":2,"maxItems":1}),
            serde_json::json!({"type":"number","multipleOf":0}),
            serde_json::json!({"$ref":"https://example.com/schema.json"}),
        ] {
            let request =
                ModelRequest::new(ModelRef::new("openai", "model"), Message::user("answer"))
                    .output_format(OutputFormat::JsonSchema {
                        name: "answer".into(),
                        schema: serde_json::json!({
                            "type":"object",
                            "properties":{"value":property_schema},
                            "required":["value"],
                            "additionalProperties":false
                        }),
                        strict: true,
                    });

            assert_eq!(
                encode_request(&request).unwrap_err().kind,
                ModelErrorKind::InvalidRequest
            );
        }
    }

    #[test]
    fn strict_schema_rejects_duplicate_required_names() {
        let request = ModelRequest::new(ModelRef::new("openai", "model"), Message::user("answer"))
            .output_format(OutputFormat::JsonSchema {
                name: "answer".into(),
                schema: serde_json::json!({
                    "type":"object",
                    "properties":{"value":{"type":"string"}},
                    "required":["value", "value"],
                    "additionalProperties":false
                }),
                strict: true,
            });

        assert_eq!(
            encode_request(&request).unwrap_err().kind,
            ModelErrorKind::InvalidRequest
        );
    }

    #[test]
    fn strict_schema_accepts_root_recursion() {
        let request = ModelRequest::new(ModelRef::new("openai", "model"), Message::user("answer"))
            .output_format(OutputFormat::JsonSchema {
                name: "answer".into(),
                schema: serde_json::json!({
                    "type":"object",
                    "properties":{
                        "value":{"type":"string"},
                        "next":{"anyOf":[{"$ref":"#"},{"type":"null"}]}
                    },
                    "required":["value", "next"],
                    "additionalProperties":false
                }),
                strict: true,
            });

        assert!(encode_request(&request).is_ok());
    }

    #[test]
    fn strict_output_schema_is_validated_before_transport() {
        let request = ModelRequest::new(ModelRef::new("openai", "model"), Message::user("answer"))
            .output_format(OutputFormat::JsonSchema {
                name: "answer".into(),
                schema: serde_json::json!({"anyOf":[{"type":"string"}]}),
                strict: true,
            });

        assert_eq!(
            encode_request(&request).unwrap_err().kind,
            ModelErrorKind::InvalidRequest
        );
    }

    #[test]
    fn preserves_tool_call_identity_and_raw_arguments() {
        let message = Message::new(
            Role::Assistant,
            vec![ContentPart::ToolCall(ToolCall {
                id: "call-7".into(),
                name: "lookup".into(),
                arguments: serde_json::json!({"value": 7}),
                raw_arguments: Some("{ \"value\": 7 }".into()),
                metadata: BTreeMap::from([
                    ("openai.id".into(), serde_json::json!("fc_7")),
                    ("openai.status".into(), serde_json::json!("completed")),
                    (
                        "openai.caller".into(),
                        serde_json::json!({
                            "type":"program",
                            "caller_id":"program-call"
                        }),
                    ),
                ]),
            })],
        )
        .unwrap();
        let request = ModelRequest::new(ModelRef::new("openai", "model"), message);

        let encoded = encode_request(&request).unwrap();

        assert_eq!(encoded["input"][0]["call_id"], "call-7");
        assert_eq!(encoded["input"][0]["id"], "fc_7");
        assert_eq!(encoded["input"][0]["status"], "completed");
        assert_eq!(encoded["input"][0]["arguments"], "{ \"value\": 7 }");
        assert_eq!(
            encoded["input"][0]["caller"],
            serde_json::json!({"type":"program","caller_id":"program-call"})
        );
    }

    #[test]
    fn legacy_tool_calls_replay_as_completed() {
        let message = Message::new(
            Role::Assistant,
            vec![ContentPart::ToolCall(ToolCall {
                id: "call-legacy".into(),
                name: "lookup".into(),
                arguments: serde_json::json!({}),
                raw_arguments: Some("{}".into()),
                metadata: BTreeMap::new(),
            })],
        )
        .unwrap();

        let encoded =
            encode_request(&ModelRequest::new(ModelRef::new("ark", "doubao"), message)).unwrap();

        assert_eq!(encoded["input"][0]["status"], "completed");
        assert!(encoded["input"][0].get("id").is_none());
    }

    #[test]
    fn assistant_messages_replay_with_required_completion_status() {
        let request = ModelRequest::new(
            ModelRef::new("openai", "model"),
            Message::new(Role::Assistant, vec![ContentPart::text("previous answer")]).unwrap(),
        );

        let encoded = encode_request(&request).unwrap();

        assert_eq!(encoded["input"][0]["type"], "message");
        assert_eq!(encoded["input"][0]["role"], "assistant");
        assert_eq!(encoded["input"][0]["status"], "completed");
    }

    #[test]
    fn rejects_replaying_an_incomplete_canonical_tool_call() {
        let message = Message::new(
            Role::Assistant,
            vec![ContentPart::ToolCall(ToolCall {
                id: "call-incomplete".into(),
                name: "lookup".into(),
                arguments: serde_json::json!({}),
                raw_arguments: Some("{}".into()),
                metadata: BTreeMap::from([("ark.status".into(), serde_json::json!("incomplete"))]),
            })],
        )
        .unwrap();

        let error = encode_request(&ModelRequest::new(ModelRef::new("ark", "doubao"), message))
            .unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::InvalidRequest);
    }

    #[test]
    fn function_outputs_preserve_images_files_and_structured_content() {
        let message = Message::new(
            Role::Tool,
            vec![ContentPart::ToolResult(ToolResult {
                call_id: "call-rich".into(),
                name: Some("render".into()),
                content: vec![
                    ContentPart::Image {
                        source: MediaSource::Base64 {
                            media_type: "image/png".into(),
                            data: "iVBORw0KGgo=".into(),
                        },
                    },
                    ContentPart::Document {
                        source: MediaSource::Url {
                            url: "https://example.com/report.pdf".into(),
                            media_type: Some("application/pdf".into()),
                        },
                        name: Some("report.pdf".into()),
                    },
                ],
                structured_content: Some(serde_json::json!({"count":2})),
                is_error: false,
                metadata: BTreeMap::from([(
                    "openai.caller".into(),
                    serde_json::json!({
                        "type":"program",
                        "caller_id":"program-call"
                    }),
                )]),
            })],
        )
        .unwrap();
        let body = encode_request(&ModelRequest::new(
            ModelRef::new("openai", "vision"),
            message,
        ))
        .unwrap();

        assert_eq!(body["input"][0]["type"], "function_call_output");
        assert_eq!(body["input"][0]["output"][0]["type"], "input_text");
        assert_eq!(body["input"][0]["output"][1]["type"], "input_image");
        assert_eq!(body["input"][0]["output"][2]["type"], "input_file");
        assert_eq!(
            body["input"][0]["caller"],
            serde_json::json!({"type":"program","caller_id":"program-call"})
        );
    }

    #[test]
    fn function_outputs_bridge_audio_without_rejecting_the_request() {
        let message = Message::new(
            Role::Tool,
            vec![ContentPart::ToolResult(ToolResult {
                call_id: "call-audio".into(),
                name: Some("listen".into()),
                content: vec![ContentPart::Audio {
                    source: MediaSource::Base64 {
                        media_type: "audio/wav".into(),
                        data: "UklGRg==".into(),
                    },
                }],
                structured_content: None,
                is_error: false,
                metadata: BTreeMap::new(),
            })],
        )
        .unwrap();

        let body = encode_request(&ModelRequest::new(
            ModelRef::new("openai", "model"),
            message,
        ))
        .unwrap();
        let envelope = body["input"][0]["output"].as_str().unwrap();

        assert!(envelope.contains("runifold.content.v1"));
        assert!(envelope.contains("audio/wav"));
        assert!(envelope.contains("UklGRg=="));
    }

    #[test]
    fn ordinary_audio_input_uses_the_bounded_projection() {
        let message = Message::new(
            Role::User,
            vec![ContentPart::Audio {
                source: MediaSource::Base64 {
                    media_type: "audio/wav".into(),
                    data: "YXVkaW8=".into(),
                },
            }],
        )
        .unwrap();

        let body = encode_request(&ModelRequest::new(
            ModelRef::new("openai", "model"),
            message,
        ))
        .unwrap();
        let envelope = body["input"][0]["content"][0]["text"].as_str().unwrap();

        assert!(decode_content_envelope(envelope).unwrap().is_some());
    }

    #[test]
    fn native_image_input_rejects_invalid_base64() {
        let message = Message::new(
            Role::User,
            vec![ContentPart::Image {
                source: MediaSource::Base64 {
                    media_type: "image/png".into(),
                    data: "not base64".into(),
                },
            }],
        )
        .unwrap();

        let error = encode_request(&ModelRequest::new(
            ModelRef::new("openai", "vision"),
            message,
        ))
        .unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::InvalidRequest);
    }

    #[test]
    fn native_image_input_rejects_mime_and_signature_mismatches() {
        for source in [
            MediaSource::Base64 {
                media_type: "text/plain".into(),
                data: "iVBORw0KGgo=".into(),
            },
            MediaSource::Base64 {
                media_type: "image/png".into(),
                data: "bm90IGEgcG5n".into(),
            },
        ] {
            let message = Message::new(Role::User, vec![ContentPart::Image { source }]).unwrap();
            let error = encode_request(&ModelRequest::new(
                ModelRef::new("openai", "vision"),
                message,
            ))
            .unwrap_err();
            assert_eq!(error.kind, ModelErrorKind::InvalidRequest);
        }
    }

    #[test]
    fn native_document_input_rejects_credentialed_url() {
        let message = Message::new(
            Role::User,
            vec![ContentPart::Document {
                source: MediaSource::Url {
                    url: "https://user:secret@example.com/report.pdf".into(),
                    media_type: Some("application/pdf".into()),
                },
                name: Some("report.pdf".into()),
            }],
        )
        .unwrap();

        let error = encode_request(&ModelRequest::new(
            ModelRef::new("openai", "model"),
            message,
        ))
        .unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::InvalidRequest);
    }

    #[test]
    fn invalid_provider_file_ownership_is_not_downgraded_to_text() {
        let message = Message::new(
            Role::Tool,
            vec![ContentPart::ToolResult(ToolResult {
                call_id: "call-file".into(),
                name: Some("inspect".into()),
                content: vec![ContentPart::Image {
                    source: MediaSource::provider_file("anthropic", "file-secret"),
                }],
                structured_content: None,
                is_error: false,
                metadata: BTreeMap::new(),
            })],
        )
        .unwrap();

        let error = encode_request(&ModelRequest::new(
            ModelRef::new("openai", "model"),
            message,
        ))
        .unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::InvalidRequest);
    }

    #[test]
    fn never_silently_ignores_unsupported_options() {
        let mut request =
            ModelRequest::new(ModelRef::new("openai", "model"), Message::user("hello"));
        request.generation.stop.push("STOP".into());

        let error = encode_request(&request).unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::UnsupportedFeature);
    }

    #[test]
    fn adapter_owned_fields_cannot_be_overridden() {
        let mut request =
            ModelRequest::new(ModelRef::new("openai", "model"), Message::user("hello"));
        request
            .provider_options
            .insert("openai".into(), serde_json::json!({"model": "other"}));

        let error = encode_request(&request).unwrap_err();

        assert_eq!(error.kind, ModelErrorKind::InvalidRequest);
    }

    #[test]
    fn compatible_parallel_tool_policy_is_forwarded_explicitly() {
        let request = ModelRequest::new(ModelRef::new("openai", "model"), Message::user("hello"))
            .provider_option(
                "openai-compatible",
                serde_json::json!({"parallel_tool_calls": false}),
            );

        let encoded = encode_request(&request).unwrap();

        assert_eq!(encoded["parallel_tool_calls"], false);
    }

    #[test]
    fn canonical_compatible_namespace_is_merged_once() {
        let request = ModelRequest::new(
            ModelRef::new("openai-compatible", "model"),
            Message::user("hello"),
        )
        .provider_option(
            "openai-compatible",
            serde_json::json!({"parallel_tool_calls":false}),
        );

        let encoded = super::encode_request_for(&request, "openai-compatible").unwrap();

        assert_eq!(encoded["parallel_tool_calls"], false);
    }

    #[test]
    fn ark_native_and_function_tools_share_one_wire_array() {
        let request = ModelRequest::new(ModelRef::new("ark", "doubao"), Message::user("research"))
            .tool(ToolSpec {
                name: "lookup".into(),
                description: "lookup".into(),
                input_schema: serde_json::json!({"type": "object"}),
                output_schema: None,
                metadata: BTreeMap::new(),
            })
            .provider_tool(
                ProviderToolSpec::new("ark", "web_search")
                    .unwrap()
                    .option("limit", 8_u32)
                    .option("max_keyword", 5_u32),
            );

        let encoded = super::encode_request_for(&request, "ark").unwrap();

        assert_eq!(encoded["tools"][0]["type"], "function");
        assert_eq!(encoded["tools"][1]["type"], "web_search");
        assert_eq!(encoded["tools"][1]["limit"], 8);
    }

    #[test]
    fn complete_mode_and_provider_file_ids_are_encoded() {
        let message = Message::new(
            Role::User,
            vec![
                ContentPart::Image {
                    source: MediaSource::provider_file("ark", "file_image"),
                },
                ContentPart::Document {
                    source: MediaSource::provider_file("ark", "file_document"),
                    name: Some("report.pdf".into()),
                },
            ],
        )
        .unwrap();
        let request = ModelRequest::new(ModelRef::new("ark", "doubao"), message)
            .response_mode(ResponseMode::Complete);

        let encoded = super::encode_request_for(&request, "ark").unwrap();

        assert_eq!(encoded["stream"], false);
        assert_eq!(encoded["input"][0]["content"][0]["file_id"], "file_image");
        assert_eq!(
            encoded["input"][0]["content"][1]["file_id"],
            "file_document"
        );
    }
}
