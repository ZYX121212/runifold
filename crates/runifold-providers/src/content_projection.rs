//! Safe, versioned projection for rich content on text-only Provider wires.

use base64::Engine;
use runifold_model::{ContentPart, MediaSource, ModelError, ModelErrorKind, ToolResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Stable envelope type for one or more canonical content parts.
pub const CONTENT_ENVELOPE_KIND: &str = "runifold.content.v1";
/// Stable envelope type for a complete canonical Tool result.
pub const TOOL_RESULT_ENVELOPE_KIND: &str = "runifold.tool_result.v1";
/// Maximum encoded envelope size accepted by the projection boundary.
pub const MAX_CONTENT_ENVELOPE_BYTES: usize = 256 * 1024;
const MAX_MEDIA_TYPE_BYTES: usize = 255;
const MAX_MEDIA_URL_BYTES: usize = 8 * 1024;
const MAX_INLINE_MEDIA_BYTES: usize = 32 * 1024 * 1024;

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

#[cfg(any(feature = "bedrock", feature = "gemini"))]
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

pub(crate) fn validate_inline_media(media_type: &str, data: &str) -> Result<(), ModelError> {
    validate_media_type(media_type)?;
    if data.trim().is_empty() {
        return Err(invalid("inline media requires base64 data"));
    }
    if data.len() > MAX_INLINE_MEDIA_BYTES.saturating_mul(4).div_ceil(3) + 4 {
        return Err(invalid("inline media exceeds the 32 MiB decoded limit"));
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|_| invalid("inline media is not valid base64"))?;
    if decoded.is_empty() {
        return Err(invalid("inline media decoded to an empty payload"));
    }
    if decoded.len() > MAX_INLINE_MEDIA_BYTES {
        return Err(invalid("inline media exceeds the 32 MiB decoded limit"));
    }
    Ok(())
}

#[cfg(feature = "openai")]
pub(crate) fn validate_inline_image(media_type: &str, data: &str) -> Result<(), ModelError> {
    validate_image_media_type(Some(media_type))?;
    validate_inline_media(media_type, data)?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|_| invalid("inline image is not valid base64"))?;
    let signature_matches = match media_type {
        "image/png" => decoded.starts_with(b"\x89PNG\r\n\x1a\n"),
        "image/jpeg" => decoded.starts_with(&[0xff, 0xd8, 0xff]),
        "image/gif" => decoded.starts_with(b"GIF87a") || decoded.starts_with(b"GIF89a"),
        "image/webp" => {
            decoded.starts_with(b"RIFF") && decoded.get(8..12) == Some(b"WEBP".as_slice())
        }
        _ => false,
    };
    if !signature_matches {
        return Err(invalid(
            "inline image bytes do not match the declared supported MIME type",
        ));
    }
    Ok(())
}

#[cfg(feature = "openai")]
pub(crate) fn validate_image_media_type(media_type: Option<&str>) -> Result<(), ModelError> {
    let Some(media_type) = media_type else {
        return Ok(());
    };
    validate_media_type(media_type)?;
    if !matches!(
        media_type,
        "image/png" | "image/jpeg" | "image/gif" | "image/webp"
    ) {
        return Err(invalid(
            "image MIME type must be image/png, image/jpeg, image/gif, or image/webp",
        ));
    }
    Ok(())
}

pub(crate) fn validate_optional_media_type(media_type: Option<&str>) -> Result<(), ModelError> {
    media_type.map_or(Ok(()), validate_media_type)
}

pub(crate) fn validate_media_url(url: &str, schemes: &[&str]) -> Result<(), ModelError> {
    if url.len() > MAX_MEDIA_URL_BYTES || url.chars().any(char::is_control) {
        return Err(invalid(
            "media URL is too long or contains control characters",
        ));
    }
    let parsed = url::Url::parse(url).map_err(|_| invalid("media URL is not absolute"))?;
    if !schemes.contains(&parsed.scheme())
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        return Err(invalid(
            "media URL uses an unsupported scheme, authority, credentials, or fragment",
        ));
    }
    Ok(())
}

fn validate_media_type(media_type: &str) -> Result<(), ModelError> {
    let Some((kind, subtype)) = media_type.split_once('/') else {
        return Err(invalid(
            "media MIME type must contain one type/subtype pair",
        ));
    };
    if media_type.len() > MAX_MEDIA_TYPE_BYTES
        || kind.is_empty()
        || subtype.is_empty()
        || subtype.contains('/')
        || !kind.bytes().all(is_mime_token_byte)
        || !subtype.bytes().all(is_mime_token_byte)
    {
        return Err(invalid("media MIME type is malformed or too long"));
    }
    Ok(())
}

fn is_mime_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
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
            validate_media_url(url, &["http", "https", "gs"])?;
            validate_optional_media_type(media_type.as_deref())?;
        }
        MediaSource::Base64 { media_type, data } => {
            validate_inline_media(media_type, data)?;
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
    fn invalid_inline_media_is_rejected_on_encode_and_decode() {
        let part = ContentPart::Audio {
            source: MediaSource::Base64 {
                media_type: "audio/wav".into(),
                data: "not base64".into(),
            },
        };

        let error = encode_content_envelope(&part).unwrap_err();
        assert_eq!(error.kind, runifold_model::ModelErrorKind::InvalidRequest);

        let encoded = serde_json::json!({
            "type": super::CONTENT_ENVELOPE_KIND,
            "content": [part],
        })
        .to_string();
        let error = decode_content_envelope(&encoded).unwrap_err();
        assert_eq!(error.kind, runifold_model::ModelErrorKind::InvalidRequest);
    }

    #[test]
    fn unsafe_remote_media_and_mime_types_are_rejected() {
        for url in [
            "relative.png",
            "file:///etc/passwd",
            "https://user:secret@example.com/image.png",
            "https://example.com/image.png#fragment",
        ] {
            let part = ContentPart::Image {
                source: MediaSource::Url {
                    url: url.into(),
                    media_type: Some("image/png".into()),
                },
            };
            assert_eq!(
                encode_content_envelope(&part).unwrap_err().kind,
                runifold_model::ModelErrorKind::InvalidRequest
            );
        }

        let invalid_mime = ContentPart::Image {
            source: MediaSource::Url {
                url: "https://example.com/image.png".into(),
                media_type: Some("image/png\r\nunsafe".into()),
            },
        };
        assert_eq!(
            encode_content_envelope(&invalid_mime).unwrap_err().kind,
            runifold_model::ModelErrorKind::InvalidRequest
        );

        let gcs = ContentPart::Document {
            source: MediaSource::Url {
                url: "gs://bucket/report.pdf".into(),
                media_type: Some("application/pdf".into()),
            },
            name: Some("report.pdf".into()),
        };
        assert!(encode_content_envelope(&gcs).is_ok());
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
