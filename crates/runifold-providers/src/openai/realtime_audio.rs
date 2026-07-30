//! Bounded audio values for the `OpenAI` Realtime protocol.

use std::fmt;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _, ser::SerializeMap};

use super::realtime::OpenAiRealtimeError;

/// Runifold's per-event raw audio bound.
///
/// The Provider protocol permits larger appends, but this limit leaves safe
/// headroom for Base64 and JSON inside Runifold's bounded 1 MiB WebSocket
/// frame.
pub const OPENAI_REALTIME_MAX_AUDIO_CHUNK_BYTES: usize = 512 * 1024;

/// Audio encoding negotiated in a GA Realtime session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiRealtimeAudioFormat {
    /// Signed 16-bit little-endian PCM at 24 kHz, mono.
    Pcm24Khz,
    /// G.711 μ-law.
    Pcmu,
    /// G.711 A-law.
    Pcma,
}

impl Serialize for OpenAiRealtimeAudioFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(match self {
            Self::Pcm24Khz => 2,
            Self::Pcmu | Self::Pcma => 1,
        }))?;
        match self {
            Self::Pcm24Khz => {
                map.serialize_entry("type", "audio/pcm")?;
                map.serialize_entry("rate", &24_000_u32)?;
            }
            Self::Pcmu => map.serialize_entry("type", "audio/pcmu")?,
            Self::Pcma => map.serialize_entry("type", "audio/pcma")?,
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for OpenAiRealtimeAudioFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            #[serde(rename = "type")]
            kind: String,
            #[serde(default)]
            rate: Option<u32>,
        }

        let wire = Wire::deserialize(deserializer)?;
        match (wire.kind.as_str(), wire.rate) {
            ("audio/pcm", Some(24_000)) => Ok(Self::Pcm24Khz),
            ("audio/pcmu", None) => Ok(Self::Pcmu),
            ("audio/pcma", None) => Ok(Self::Pcma),
            ("audio/pcm", _) => Err(D::Error::custom(
                "Realtime PCM audio must use the 24000 Hz GA format",
            )),
            _ => Err(D::Error::custom("unsupported Realtime audio format")),
        }
    }
}

/// Validated raw audio carried by one Realtime event.
#[derive(Clone, Eq, PartialEq)]
pub struct OpenAiRealtimeAudioChunk(Vec<u8>);

impl OpenAiRealtimeAudioChunk {
    /// Creates one bounded, non-empty audio chunk.
    ///
    /// # Errors
    ///
    /// Rejects empty chunks and chunks larger than 512 KiB.
    pub fn new(bytes: impl Into<Vec<u8>>) -> Result<Self, OpenAiRealtimeError> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(OpenAiRealtimeError::InvalidRequest(
                "Realtime audio chunk cannot be empty".into(),
            ));
        }
        if bytes.len() > OPENAI_REALTIME_MAX_AUDIO_CHUNK_BYTES {
            return Err(OpenAiRealtimeError::InvalidRequest(format!(
                "Realtime audio chunk exceeds the {OPENAI_REALTIME_MAX_AUDIO_CHUNK_BYTES} byte limit"
            )));
        }
        Ok(Self(bytes))
    }

    /// Borrows the decoded raw bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the chunk and returns its raw bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub(crate) fn encode_base64(&self) -> String {
        STANDARD.encode(&self.0)
    }

    pub(crate) fn decode_base64(value: &str) -> Result<Self, OpenAiRealtimeError> {
        let max_encoded = OPENAI_REALTIME_MAX_AUDIO_CHUNK_BYTES.div_ceil(3) * 4;
        if value.len() > max_encoded {
            return Err(OpenAiRealtimeError::Protocol(format!(
                "Realtime output audio exceeds the {OPENAI_REALTIME_MAX_AUDIO_CHUNK_BYTES} byte limit"
            )));
        }
        let bytes = STANDARD.decode(value).map_err(|_| {
            OpenAiRealtimeError::Protocol("Realtime output audio is not valid Base64".into())
        })?;
        Self::new(bytes).map_err(|_| {
            OpenAiRealtimeError::Protocol(format!(
                "Realtime output audio exceeds the {OPENAI_REALTIME_MAX_AUDIO_CHUNK_BYTES} byte limit"
            ))
        })
    }
}

impl fmt::Debug for OpenAiRealtimeAudioChunk {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenAiRealtimeAudioChunk")
            .field("bytes", &self.0.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_use_the_ga_nested_wire_shape() {
        assert_eq!(
            serde_json::to_value(OpenAiRealtimeAudioFormat::Pcm24Khz).unwrap(),
            serde_json::json!({"type": "audio/pcm", "rate": 24000})
        );
        assert_eq!(
            serde_json::to_value(OpenAiRealtimeAudioFormat::Pcmu).unwrap(),
            serde_json::json!({"type": "audio/pcmu"})
        );
    }

    #[test]
    fn chunks_are_bounded_and_base64_round_trip() {
        let chunk = OpenAiRealtimeAudioChunk::new(vec![0, 1, 2, 255]).unwrap();
        assert_eq!(
            OpenAiRealtimeAudioChunk::decode_base64(&chunk.encode_base64()).unwrap(),
            chunk
        );
        assert!(OpenAiRealtimeAudioChunk::new(Vec::new()).is_err());
        assert!(
            OpenAiRealtimeAudioChunk::new(vec![0; OPENAI_REALTIME_MAX_AUDIO_CHUNK_BYTES + 1])
                .is_err()
        );
        assert!(OpenAiRealtimeAudioChunk::decode_base64("%%%").is_err());
    }
}
