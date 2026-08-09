//! Safe, versioned projection for rich content on text-only Provider wires.

use runifold_model::{ContentPart, MediaSource, ModelError, ModelErrorKind, ToolResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Stable envelope type for one or more canonical content parts.
pub const CONTENT_ENVELOPE_KIND: &str = "runifold.content.v1";
/// Stable envelope type for a complete canonical Tool result.
pub const TOOL_RESULT_ENVELOPE_KIND: &str = "runifold.tool_result.v1";
/// Maximum encoded envelope size accepted by the projection boundary.
pub const MAX_CONTENT_ENVELOPE_BYTES: usize = 256 * 1024;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ContentEnvelope {
    #[serde(rename = "type")]
    kind: String,
    content: Vec<ContentPart>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ToolResultEnvelope {
    #[serde(rename = "type")]
    kind: String,
    call_id: String,
    name: Option<String>,
    content: Vec<ContentPart>,
    structured_content: Option<Value>,
    is_error: bool,
}

/// Encodes one safe canonical content part into a versioned envelope.
///
/// # Errors
///
/// Returns [`ModelErrorKind::UnsupportedFeature`] for host-only, recursive, or
/// Provider-private content and [`ModelErrorKind::InvalidRequest`] for invalid
/// media or an envelope larger than [`MAX_CONTENT_ENVELOPE_BYTES`].
pub fn encode_content_envelope(part: &ContentPart) -> Result<String, ModelError> {
    encode_content_envelope_many(std::slice::from_ref(part))
}

/// Encodes ordered safe canonical content into one unambiguous envelope.
///
/// # Errors
///
/// Returns a typed model error when any part is unsafe or the encoded envelope
/// exceeds the projection limit.
pub fn encode_content_envelope_many(parts: &[ContentPart]) -> Result<String, ModelError> {
    validate_projectable(parts)?;
    encode_bounded(&ContentEnvelope {
        kind: CONTENT_ENVELOPE_KIND.into(),
        content: parts.to_vec(),
    })
}

/// Decodes a content envelope without interpreting ordinary model text.
///
/// `Ok(None)` means that `input` is not a Runifold content envelope. Callers
/// must opt in to decoding because model-generated JSON remains untrusted.
///
/// # Errors
///
/// Returns a typed model error for malformed, oversized, or unsafe Runifold
/// envelopes.
pub fn decode_content_envelope(input: &str) -> Result<Option<Vec<ContentPart>>, ModelError> {
    ensure_encoded_limit(input)?;
    let Some(kind) = envelope_kind(input) else {
        return Ok(None);
    };
    if kind != CONTENT_ENVELOPE_KIND {
        return Ok(None);
    }
    let envelope: ContentEnvelope = serde_json::from_str(input)
        .map_err(|_| invalid("Runifold content envelope is malformed"))?;
    validate_projectable(&envelope.content)?;
    Ok(Some(envelope.content))
}

/// Encodes a complete Tool result for Provider protocols that expose only one
/// text field. Host metadata is intentionally excluded from model-visible data.
///
/// # Errors
///
/// Returns a typed model error for unsafe content or an oversized envelope.
pub fn encode_tool_result_envelope(result: &ToolResult) -> Result<String, ModelError> {
    validate_projectable(&result.content)?;
    encode_bounded(&ToolResultEnvelope {
        kind: TOOL_RESULT_ENVELOPE_KIND.into(),
        call_id: result.call_id.clone(),
        name: result.name.clone(),
        content: result.content.clone(),
        structured_content: result.structured_content.clone(),
        is_error: result.is_error,
    })
}

/// Decodes an explicitly recognized Tool-result envelope.
///
/// Decoded host metadata is empty because metadata never crosses this
/// model-visible projection boundary.
///
/// # Errors
///
/// Returns a typed model error for malformed, oversized, or unsafe envelopes.
pub fn decode_tool_result_envelope(input: &str) -> Result<Option<ToolResult>, ModelError> {
    ensure_encoded_limit(input)?;
    let Some(kind) = envelope_kind(input) else {
        return Ok(None);
    };
    if kind != TOOL_RESULT_ENVELOPE_KIND {
        return Ok(None);
    }
    let envelope: ToolResultEnvelope = serde_json::from_str(input)
        .map_err(|_| invalid("Runifold Tool-result envelope is malformed"))?;
    validate_projectable(&envelope.content)?;
    Ok(Some(ToolResult {
        call_id: envelope.call_id,
        name: envelope.name,
        content: envelope.content,
        structured_content: envelope.structured_content,
        is_error: envelope.is_error,
        metadata: std::collections::BTreeMap::new(),
    }))
}

pub(crate) fn native_or_projected<T>(
    native: Result<T, ModelError>,
    part: &ContentPart,
    projected: impl FnOnce(String) -> T,
) -> Result<T, ModelError> {
    match native {
        Ok(value) => Ok(value),
        Err(error) if error.kind == ModelErrorKind::UnsupportedFeature => {
            encode_content_envelope(part).map(projected)
        }
        Err(error) => Err(error),
    }
}

fn envelope_kind(input: &str) -> Option<String> {
    let Ok(value) = serde_json::from_str::<Value>(input) else {
        return None;
    };
    value.get("type").and_then(Value::as_str).map(str::to_owned)
}

fn encode_bounded(value: &impl Serialize) -> Result<String, ModelError> {
    let encoded = serde_json::to_string(value)
        .map_err(|error| invalid(format!("failed to encode rich content envelope: {error}")))?;
    ensure_encoded_limit(&encoded)?;
    Ok(encoded)
}

fn ensure_encoded_limit(encoded: &str) -> Result<(), ModelError> {
    if encoded.len() > MAX_CONTENT_ENVELOPE_BYTES {
        return Err(invalid(format!(
            "rich content envelope exceeds {MAX_CONTENT_ENVELOPE_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_projectable(parts: &[ContentPart]) -> Result<(), ModelError> {
    if parts.is_empty() {
        return Err(invalid("rich content envelope must not be empty"));
    }
    for part in parts {
        match part {
            ContentPart::Text { .. } | ContentPart::Refusal { .. } | ContentPart::Citation(_) => {}
            ContentPart::Image { source }
            | ContentPart::Audio { source }
            | ContentPart::Document { source, .. } => validate_media(source)?,
            ContentPart::ResourceLink { uri, name, .. } => {
                if uri.trim().is_empty() || name.trim().is_empty() {
                    return Err(invalid("projected resource links require a URI and name"));
                }
            }
            ContentPart::Reasoning(_)
            | ContentPart::ProviderOpaque(_)
            | ContentPart::ToolCall(_)
            | ContentPart::ToolResult(_) => {
                return Err(unsupported(
                    "Provider-private, reasoning, or recursive Tool content cannot use a text envelope",
                ));
            }
            _ => {
                return Err(unsupported(
                    "content is newer than the safe projection contract",
                ));
            }
        }
    }
    Ok(())
}

fn validate_media(source: &MediaSource) -> Result<(), ModelError> {
    match source {
        MediaSource::Url { url, media_type } => {
            if url.trim().is_empty()
                || media_type
                    .as_ref()
                    .is_some_and(|value| value.trim().is_empty())
            {
                return Err(invalid("projected media URL or MIME type is blank"));
            }
        }
        MediaSource::Base64 { media_type, data } => {
            if media_type.trim().is_empty() || data.trim().is_empty() {
                return Err(invalid(
                    "projected inline media requires MIME type and data",
                ));
            }
        }
        MediaSource::Artifact { .. } => {
            return Err(unsupported(
                "artifact media must be resolved before model-visible projection",
            ));
        }
        MediaSource::ProviderFile { .. } => {
            return Err(unsupported(
                "provider-owned file identities cannot cross a text projection boundary",
            ));
        }
        _ => {
            return Err(unsupported(
                "media source is newer than the projection contract",
            ));
        }
    }
    Ok(())
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

    use runifold_model::{ContentPart, MediaSource, ProviderData, ToolResult};

    use super::{
        MAX_CONTENT_ENVELOPE_BYTES, decode_content_envelope, decode_tool_result_envelope,
        encode_content_envelope, encode_tool_result_envelope,
    };

    #[test]
    fn safe_media_envelope_round_trips_exactly() {
        let part = ContentPart::Audio {
            source: MediaSource::Base64 {
                media_type: "audio/wav".into(),
                data: "AQID".into(),
            },
        };

        let encoded = encode_content_envelope(&part).unwrap();
        let decoded = decode_content_envelope(&encoded).unwrap().unwrap();

        assert_eq!(decoded, vec![part]);
    }

    #[test]
    fn tool_result_round_trip_excludes_host_metadata() {
        let result = ToolResult {
            call_id: "call-1".into(),
            name: Some("listen".into()),
            content: vec![ContentPart::text("done")],
            structured_content: Some(serde_json::json!({"ok":true})),
            is_error: false,
            metadata: BTreeMap::from([("host.secret".into(), serde_json::json!("hidden"))]),
        };

        let encoded = encode_tool_result_envelope(&result).unwrap();
        let decoded = decode_tool_result_envelope(&encoded).unwrap().unwrap();

        assert_eq!(decoded.call_id, result.call_id);
        assert_eq!(decoded.content, result.content);
        assert!(decoded.metadata.is_empty());
        assert!(!encoded.contains("host.secret"));
    }

    #[test]
    fn provider_private_content_fails_closed() {
        let part = ContentPart::ProviderOpaque(ProviderData {
            provider: "private".into(),
            kind: "continuation".into(),
            value: serde_json::json!({"token":"secret"}),
        });

        let error = encode_content_envelope(&part).unwrap_err();

        assert_eq!(
            error.kind,
            runifold_model::ModelErrorKind::UnsupportedFeature
        );
    }

    #[test]
    fn oversized_envelopes_are_rejected() {
        let part = ContentPart::text("x".repeat(MAX_CONTENT_ENVELOPE_BYTES));

        let error = encode_content_envelope(&part).unwrap_err();

        assert_eq!(error.kind, runifold_model::ModelErrorKind::InvalidRequest);
    }

    #[test]
    fn ordinary_text_is_not_interpreted_as_an_envelope() {
        assert_eq!(decode_content_envelope("not json").unwrap(), None);
        assert_eq!(
            decode_content_envelope(r#"{"type":"other"}"#).unwrap(),
            None
        );
    }
}
