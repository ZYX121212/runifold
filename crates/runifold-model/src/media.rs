//! Provider-neutral image, speech, and transcription task boundaries.

use serde::{Deserialize, Serialize};

use crate::{MediaSource, ModelCallContext, ModelError, ModelFuture, ModelRef};

/// Image output encoding requested from a generation model.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ImageFormat {
    /// Portable Network Graphics.
    #[default]
    Png,
    /// WebP image.
    Webp,
    /// JPEG image.
    Jpeg,
}

impl ImageFormat {
    /// Returns the canonical media type.
    #[must_use]
    pub const fn media_type(self) -> &'static str {
        match self {
            Self::Png => "image/png",
            Self::Webp => "image/webp",
            Self::Jpeg => "image/jpeg",
        }
    }
}

/// Provider-neutral image-generation request.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ImageGenerationRequest {
    /// Provider and model identity.
    pub model: ModelRef,
    /// Natural-language image description.
    pub prompt: String,
    /// Number of requested images.
    pub count: u8,
    /// Provider-supported size such as `1024x1024` or `auto`.
    pub size: Option<String>,
    /// Provider-supported quality such as `low`, `high`, or `auto`.
    pub quality: Option<String>,
    /// Requested output encoding.
    pub format: ImageFormat,
    /// Whether a transparent background is required.
    pub transparent: bool,
}

/// One generated image.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GeneratedImage {
    /// Inline or remotely hosted image source.
    pub source: MediaSource,
    /// Provider-revised prompt, when supplied.
    pub revised_prompt: Option<String>,
}

/// Complete image-generation result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ImageGenerationResponse {
    /// Generated images in provider order.
    pub images: Vec<GeneratedImage>,
}

/// Independent image-generation model boundary.
pub trait ImageGenerationModel: Send + Sync {
    /// Generates complete image outputs.
    fn generate_image(
        &self,
        request: ImageGenerationRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ImageGenerationResponse, ModelError>>;
}

/// Audio encoding requested from a speech model.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SpeechFormat {
    /// MPEG Layer III.
    #[default]
    Mp3,
    /// Opus audio.
    Opus,
    /// Advanced Audio Coding.
    Aac,
    /// Free Lossless Audio Codec.
    Flac,
    /// Waveform Audio File Format.
    Wav,
    /// Headerless PCM audio.
    Pcm,
}

impl SpeechFormat {
    /// Returns the canonical media type.
    #[must_use]
    pub const fn media_type(self) -> &'static str {
        match self {
            Self::Mp3 => "audio/mpeg",
            Self::Opus => "audio/opus",
            Self::Aac => "audio/aac",
            Self::Flac => "audio/flac",
            Self::Wav => "audio/wav",
            Self::Pcm => "audio/L16",
        }
    }
}

/// Provider-neutral text-to-speech request.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct SpeechRequest {
    /// Provider and model identity.
    pub model: ModelRef,
    /// Text to synthesize.
    pub input: String,
    /// Built-in or provider-specific voice identity.
    pub voice: String,
    /// Optional performance instructions.
    pub instructions: Option<String>,
    /// Output encoding.
    pub format: SpeechFormat,
    /// Playback speed multiplier.
    pub speed: Option<f32>,
}

/// Complete synthesized speech bytes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SpeechResponse {
    /// Canonical media type matching the requested format.
    pub media_type: String,
    /// Encoded audio bytes.
    #[serde(with = "byte_serde")]
    pub bytes: Vec<u8>,
}

/// Independent text-to-speech model boundary.
pub trait SpeechModel: Send + Sync {
    /// Synthesizes one complete audio output.
    fn synthesize_speech(
        &self,
        request: SpeechRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<SpeechResponse, ModelError>>;
}

/// Provider-neutral audio-transcription request.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TranscriptionRequest {
    /// Provider and model identity.
    pub model: ModelRef,
    /// Input file name used by multipart providers.
    pub file_name: String,
    /// Input audio media type.
    pub media_type: String,
    /// Encoded audio bytes.
    #[serde(with = "byte_serde")]
    pub bytes: Vec<u8>,
    /// Optional ISO-639-1 input language.
    pub language: Option<String>,
    /// Optional vocabulary or style hint.
    pub prompt: Option<String>,
}

/// Complete transcription result.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TranscriptionResponse {
    /// Transcribed text.
    pub text: String,
    /// Provider-reported language, when present.
    pub language: Option<String>,
    /// Provider-reported duration in seconds, when present.
    pub duration_seconds: Option<f64>,
}

/// Independent speech-to-text model boundary.
pub trait TranscriptionModel: Send + Sync {
    /// Transcribes one complete audio input.
    fn transcribe(
        &self,
        request: TranscriptionRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<TranscriptionResponse, ModelError>>;
}

mod byte_serde {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(value)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<u8>::deserialize(deserializer)
    }
}
