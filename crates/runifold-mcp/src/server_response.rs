use std::collections::BTreeMap;

use runifold_model::{ContentPart, MediaSource};
use runifold_tool::{ToolError, ToolErrorKind, ToolOutput};
use serde_json::Map;
use serde_json::{Value, json};

use crate::{
    CallToolResult, CompletionError, CompletionErrorKind, ContentBlock, JsonRpcResponse,
    PromptError, PromptErrorKind, RequestId, ResourceError, ResourceErrorKind,
};

const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
const INTERNAL_ERROR: i64 = -32603;

pub(crate) fn serialize_result<T: serde::Serialize>(id: RequestId, result: &T) -> JsonRpcResponse {
    match serde_json::to_value(result) {
        Ok(result) => JsonRpcResponse::success(id, result),
        Err(error) => JsonRpcResponse::error(
            id,
            INTERNAL_ERROR,
            "failed to encode result",
            Some(json!({"error": error.to_string()})),
        ),
    }
}

pub(crate) fn tool_invocation_response(
    id: RequestId,
    invocation: Result<ToolOutput, ToolError>,
) -> JsonRpcResponse {
    match invocation {
        Ok(output) if output.model_visible => {
            match encode_tool_content(&output.content, output.structured_content.as_ref()) {
                Ok(content) => {
                    let mut metadata = output.metadata;
                    let structured_content = match output.structured_content {
                        Some(value) if value.is_object() => Some(value),
                        Some(value) => {
                            metadata.insert("runifold.structured_content".into(), value);
                            None
                        }
                        None => None,
                    };
                    serialize_result(
                        id,
                        &CallToolResult {
                            content,
                            structured_content,
                            is_error: output.is_error,
                            metadata,
                        },
                    )
                }
                Err(message) => serialize_result(
                    id,
                    &CallToolResult {
                        content: vec![ContentBlock::text(message)],
                        structured_content: None,
                        is_error: true,
                        metadata: BTreeMap::new(),
                    },
                ),
            }
        }
        Ok(_) => serialize_result(
            id,
            &CallToolResult {
                content: vec![ContentBlock::text(
                    "tool output is not permitted for model visibility",
                )],
                structured_content: None,
                is_error: true,
                metadata: BTreeMap::new(),
            },
        ),
        Err(error)
            if matches!(
                error.kind,
                ToolErrorKind::NotFound | ToolErrorKind::CapabilityDenied
            ) =>
        {
            JsonRpcResponse::error(id, METHOD_NOT_FOUND, "tool not found", None)
        }
        Err(error) => serialize_result(
            id,
            &CallToolResult {
                content: vec![ContentBlock::text(error.message)],
                structured_content: None,
                is_error: true,
                metadata: BTreeMap::new(),
            },
        ),
    }
}

fn encode_tool_content(
    content: &[ContentPart],
    structured_content: Option<&Value>,
) -> Result<Vec<ContentBlock>, String> {
    if content.is_empty() {
        return Err("tool output content cannot be empty".into());
    }
    let mut encoded = content
        .iter()
        .map(encode_content_block)
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(value) = structured_content.filter(|value| !value.is_object()) {
        encoded.push(ContentBlock::text(value.to_string()));
    }
    Ok(encoded)
}

fn encode_content_block(part: &ContentPart) -> Result<ContentBlock, String> {
    match part {
        ContentPart::Text { text } => Ok(ContentBlock::text(text)),
        ContentPart::Image { source } => encode_inline_media("image", source),
        ContentPart::Audio { source } => encode_inline_media("audio", source),
        ContentPart::Document { source, name } => encode_document(source, name.as_deref()),
        ContentPart::ResourceLink {
            uri,
            name,
            title,
            description,
            media_type,
            size,
        } => {
            let mut fields = BTreeMap::from([
                ("uri".into(), Value::String(uri.clone())),
                ("name".into(), Value::String(name.clone())),
            ]);
            insert_optional(&mut fields, "title", title.clone().map(Value::String));
            insert_optional(
                &mut fields,
                "description",
                description.clone().map(Value::String),
            );
            insert_optional(
                &mut fields,
                "mimeType",
                media_type.clone().map(Value::String),
            );
            insert_optional(&mut fields, "size", size.map(Value::from));
            Ok(ContentBlock {
                kind: "resource_link".into(),
                fields,
            })
        }
        _ => Err("tool output contains content that MCP cannot represent".into()),
    }
}

fn encode_inline_media(kind: &str, source: &MediaSource) -> Result<ContentBlock, String> {
    match source {
        MediaSource::Base64 { media_type, data } => Ok(ContentBlock {
            kind: kind.into(),
            fields: BTreeMap::from([
                ("data".into(), Value::String(data.clone())),
                ("mimeType".into(), Value::String(media_type.clone())),
            ]),
        }),
        MediaSource::Url { url, media_type } => {
            Ok(encode_media_link(url, kind, media_type.as_deref()))
        }
        MediaSource::Artifact { reference } => Ok(encode_media_link(
            &format!(
                "artifact:{}:{}",
                reference.scope.as_str(),
                reference.artifact_id
            ),
            kind,
            Some(&reference.media_type),
        )),
        MediaSource::ProviderFile { provider, file_id } => Ok(encode_media_link(
            &format!("provider-file:{provider}:{file_id}"),
            kind,
            None,
        )),
        _ => Err(format!("{kind} source is newer than this MCP adapter")),
    }
}

fn encode_media_link(uri: &str, name: &str, media_type: Option<&str>) -> ContentBlock {
    let mut fields = BTreeMap::from([
        ("uri".into(), Value::String(uri.into())),
        ("name".into(), Value::String(name.into())),
    ]);
    insert_optional(
        &mut fields,
        "mimeType",
        media_type.map(|value| Value::String(value.into())),
    );
    ContentBlock {
        kind: "resource_link".into(),
        fields,
    }
}

fn encode_document(source: &MediaSource, name: Option<&str>) -> Result<ContentBlock, String> {
    match source {
        MediaSource::Url { url, media_type } => {
            let mut fields = BTreeMap::from([
                ("uri".into(), Value::String(url.clone())),
                (
                    "name".into(),
                    Value::String(name.unwrap_or("document").to_owned()),
                ),
            ]);
            insert_optional(
                &mut fields,
                "mimeType",
                media_type.clone().map(Value::String),
            );
            Ok(ContentBlock {
                kind: "resource_link".into(),
                fields,
            })
        }
        MediaSource::Base64 { media_type, data } => {
            let uri = format!("urn:runifold:embedded:{}", name.unwrap_or("document"));
            let resource = Value::Object(Map::from_iter([
                ("uri".into(), Value::String(uri)),
                ("mimeType".into(), Value::String(media_type.clone())),
                ("blob".into(), Value::String(data.clone())),
            ]));
            Ok(ContentBlock {
                kind: "resource".into(),
                fields: BTreeMap::from([("resource".into(), resource)]),
            })
        }
        MediaSource::Artifact { reference } => {
            let mut fields = BTreeMap::from([
                (
                    "uri".into(),
                    Value::String(format!(
                        "artifact:{}:{}",
                        reference.scope.as_str(),
                        reference.artifact_id
                    )),
                ),
                (
                    "name".into(),
                    Value::String(name.unwrap_or(&reference.artifact_id).to_owned()),
                ),
            ]);
            insert_optional(
                &mut fields,
                "mimeType",
                Some(Value::String(reference.media_type.clone())),
            );
            Ok(ContentBlock {
                kind: "resource_link".into(),
                fields,
            })
        }
        MediaSource::ProviderFile { provider, file_id } => Ok(ContentBlock {
            kind: "resource_link".into(),
            fields: BTreeMap::from([
                (
                    "uri".into(),
                    Value::String(format!("provider-file:{provider}:{file_id}")),
                ),
                (
                    "name".into(),
                    Value::String(name.unwrap_or(file_id).to_owned()),
                ),
            ]),
        }),
        _ => Err("document source is newer than this MCP adapter".into()),
    }
}

fn insert_optional(fields: &mut BTreeMap<String, Value>, key: &str, value: Option<Value>) {
    if let Some(value) = value {
        fields.insert(key.into(), value);
    }
}

pub(crate) fn resource_error_response(id: RequestId, error: &ResourceError) -> JsonRpcResponse {
    match error.kind {
        ResourceErrorKind::NotFound | ResourceErrorKind::CapabilityDenied => {
            JsonRpcResponse::error(id, -32002, "resource not found", None)
        }
        ResourceErrorKind::Cancelled => {
            JsonRpcResponse::error(id, INTERNAL_ERROR, "resource read was cancelled", None)
        }
        ResourceErrorKind::DeadlineExceeded => {
            JsonRpcResponse::error(id, INTERNAL_ERROR, "resource read deadline elapsed", None)
        }
        ResourceErrorKind::InvalidOutput | ResourceErrorKind::Execution => {
            JsonRpcResponse::error(id, INTERNAL_ERROR, "resource read failed", None)
        }
    }
}

pub(crate) fn prompt_error_response(id: RequestId, error: PromptError) -> JsonRpcResponse {
    match error.kind {
        PromptErrorKind::NotFound | PromptErrorKind::CapabilityDenied => {
            JsonRpcResponse::error(id, INVALID_PARAMS, "invalid prompt name", None)
        }
        PromptErrorKind::InvalidArguments => {
            JsonRpcResponse::error(id, INVALID_PARAMS, error.message, None)
        }
        PromptErrorKind::Cancelled => {
            JsonRpcResponse::error(id, INTERNAL_ERROR, "prompt rendering was cancelled", None)
        }
        PromptErrorKind::DeadlineExceeded => JsonRpcResponse::error(
            id,
            INTERNAL_ERROR,
            "prompt rendering deadline elapsed",
            None,
        ),
        PromptErrorKind::InvalidOutput | PromptErrorKind::Execution => {
            JsonRpcResponse::error(id, INTERNAL_ERROR, "prompt rendering failed", None)
        }
    }
}

pub(crate) fn completion_error_response(id: RequestId, error: CompletionError) -> JsonRpcResponse {
    match error.kind {
        CompletionErrorKind::NotFound | CompletionErrorKind::CapabilityDenied => {
            JsonRpcResponse::error(id, INVALID_PARAMS, "completion not found", None)
        }
        CompletionErrorKind::InvalidInput => {
            JsonRpcResponse::error(id, INVALID_PARAMS, error.message, None)
        }
        CompletionErrorKind::Cancelled => {
            JsonRpcResponse::error(id, INTERNAL_ERROR, "completion was cancelled", None)
        }
        CompletionErrorKind::DeadlineExceeded => {
            JsonRpcResponse::error(id, INTERNAL_ERROR, "completion deadline elapsed", None)
        }
        CompletionErrorKind::InvalidOutput | CompletionErrorKind::Execution => {
            JsonRpcResponse::error(id, INTERNAL_ERROR, "completion failed", None)
        }
    }
}
