//! `OpenAI` image generation, speech synthesis, and transcription adapters.

use std::collections::BTreeMap;

use futures_util::{StreamExt, future::Either, pin_mut};
use reqwest::{Response, multipart};
use runifold_model::{
    GeneratedImage, ImageFormat, ImageGenerationModel, ImageGenerationRequest,
    ImageGenerationResponse, MediaSource, ModelCallContext, ModelError, ModelErrorKind,
    ModelFuture, ModelRef, SpeechFormat, SpeechModel, SpeechRequest, SpeechResponse,
    TranscriptionModel, TranscriptionRequest, TranscriptionResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{
    OpenAiClient,
    client::{
        cancelled, http_error, protocol_error, read_error_body, request_id, retry_after,
        send_request, transport_error,
    },
};

const MAX_MEDIA_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
const MAX_TRANSCRIPTION_INPUT_BYTES: usize = 25 * 1024 * 1024;

/// Image-generation request dialect used by one exact OpenAI-compatible model.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OpenAiImageWireProfile {
    /// Fail-closed portable subset: PNG without format or background fields.
    #[default]
    Conservative,
    /// GPT Image request fields and inline base64 response defaults.
    GptImage,
    /// DALL-E 2 request fields and URL response defaults.
    DallE2,
    /// DALL-E 3 request fields, single-output limit, and URL response defaults.
    DallE3,
}

/// Speech request dialect used by one exact OpenAI-compatible model.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OpenAiSpeechWireProfile {
    /// Base speech fields only; optional performance instructions are rejected.
    #[default]
    Conservative,
    /// GPT-4o TTS fields including performance instructions.
    Instructional,
}

/// Transcription request dialect used by one exact OpenAI-compatible model.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OpenAiTranscriptionWireProfile {
    /// Base JSON transcription fields; optional prompts are rejected.
    #[default]
    Conservative,
    /// Standard transcription fields including vocabulary/style prompts.
    Prompted,
    /// Diarized model subset, which does not accept prompts.
    Diarized,
}

/// Exact per-model media declarations for one OpenAI-compatible client.
#[derive(Clone, Debug, Default)]
pub struct OpenAiMediaCapabilityCatalog {
    images: BTreeMap<ModelRef, OpenAiImageWireProfile>,
    speech: BTreeMap<ModelRef, OpenAiSpeechWireProfile>,
    transcriptions: BTreeMap<ModelRef, OpenAiTranscriptionWireProfile>,
}

impl OpenAiMediaCapabilityCatalog {
    /// Creates an empty catalog with conservative unknown-model behavior.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            images: BTreeMap::new(),
            speech: BTreeMap::new(),
            transcriptions: BTreeMap::new(),
        }
    }

    /// Inserts or replaces one exact image-generation wire declaration.
    pub fn insert_image_profile(
        &mut self,
        model: ModelRef,
        profile: OpenAiImageWireProfile,
    ) -> Option<OpenAiImageWireProfile> {
        self.images.insert(model, profile)
    }

    /// Returns the exact image-generation declaration, when present.
    #[must_use]
    pub fn image_profile(&self, model: &ModelRef) -> Option<OpenAiImageWireProfile> {
        self.images.get(model).copied()
    }

    /// Inserts or replaces one exact speech wire declaration.
    pub fn insert_speech_profile(
        &mut self,
        model: ModelRef,
        profile: OpenAiSpeechWireProfile,
    ) -> Option<OpenAiSpeechWireProfile> {
        self.speech.insert(model, profile)
    }

    /// Returns the exact speech declaration, when present.
    #[must_use]
    pub fn speech_profile(&self, model: &ModelRef) -> Option<OpenAiSpeechWireProfile> {
        self.speech.get(model).copied()
    }

    /// Inserts or replaces one exact transcription wire declaration.
    pub fn insert_transcription_profile(
        &mut self,
        model: ModelRef,
        profile: OpenAiTranscriptionWireProfile,
    ) -> Option<OpenAiTranscriptionWireProfile> {
        self.transcriptions.insert(model, profile)
    }

    /// Returns the exact transcription declaration, when present.
    #[must_use]
    pub fn transcription_profile(
        &self,
        model: &ModelRef,
    ) -> Option<OpenAiTranscriptionWireProfile> {
        self.transcriptions.get(model).copied()
    }

    pub(super) fn public_openai_defaults(provider: &str) -> Self {
        let mut catalog = Self::new();
        if provider == "openai" {
            for model in ["gpt-image-1", "gpt-image-1-mini", "gpt-image-1.5"] {
                catalog.insert_image_profile(
                    ModelRef::new(provider, model),
                    OpenAiImageWireProfile::GptImage,
                );
            }
            catalog.insert_image_profile(
                ModelRef::new(provider, "dall-e-2"),
                OpenAiImageWireProfile::DallE2,
            );
            catalog.insert_image_profile(
                ModelRef::new(provider, "dall-e-3"),
                OpenAiImageWireProfile::DallE3,
            );
            for model in ["gpt-4o-mini-tts", "gpt-4o-mini-tts-2025-12-15"] {
                catalog.insert_speech_profile(
                    ModelRef::new(provider, model),
                    OpenAiSpeechWireProfile::Instructional,
                );
            }
            for model in ["tts-1", "tts-1-hd"] {
                catalog.insert_speech_profile(
                    ModelRef::new(provider, model),
                    OpenAiSpeechWireProfile::Conservative,
                );
            }
            for model in [
                "whisper-1",
                "gpt-4o-transcribe",
                "gpt-4o-mini-transcribe",
                "gpt-4o-mini-transcribe-2025-12-15",
            ] {
                catalog.insert_transcription_profile(
                    ModelRef::new(provider, model),
                    OpenAiTranscriptionWireProfile::Prompted,
                );
            }
            catalog.insert_transcription_profile(
                ModelRef::new(provider, "gpt-4o-transcribe-diarize"),
                OpenAiTranscriptionWireProfile::Diarized,
            );
        }
        catalog
    }
}

impl ImageGenerationModel for OpenAiClient {
    fn generate_image(
        &self,
        request: ImageGenerationRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ImageGenerationResponse, ModelError>> {
        Box::pin(async move {
            validate_provider(self, &request.model.provider)?;
            if request.prompt.trim().is_empty() || request.count == 0 || request.count > 10 {
                return Err(invalid(
                    "image prompt must be non-empty and count must be 1..=10",
                ));
            }
            let profile = self.image_wire_profile(&request.model);
            if profile != OpenAiImageWireProfile::GptImage && request.format != ImageFormat::Png {
                return Err(invalid(
                    "the selected image wire profile only supports PNG output",
                ));
            }
            if profile != OpenAiImageWireProfile::GptImage && request.transparent {
                return Err(invalid(
                    "the selected image wire profile does not support transparent backgrounds",
                ));
            }
            if profile == OpenAiImageWireProfile::DallE3 && request.count != 1 {
                return Err(invalid("dall-e-3 requires an image count of 1"));
            }
            let mut body = json!({
                "model": request.model.name,
                "prompt": request.prompt,
                "n": request.count,
            });
            if profile == OpenAiImageWireProfile::GptImage {
                body["output_format"] = Value::String(image_format(request.format).into());
            }
            if let Some(size) = request.size {
                body["size"] = Value::String(size);
            }
            if let Some(quality) = request.quality {
                body["quality"] = Value::String(quality);
            }
            if request.transparent {
                body["background"] = Value::String("transparent".into());
            }
            let response = send_media_request(self, "images/generations", &context, |builder| {
                builder.json(&body)
            })
            .await?;
            let bytes = read_bounded(response, &context, self.provider()).await?;
            let payload: ImagePayload = serde_json::from_slice(&bytes)
                .map_err(|_| protocol_error(self.provider(), "image response is invalid JSON"))?;
            let images = payload
                .data
                .into_iter()
                .map(|image| {
                    let source = match (image.b64_json, image.url) {
                        (Some(data), _) if !data.is_empty() => MediaSource::Base64 {
                            media_type: request.format.media_type().into(),
                            data,
                        },
                        (None, Some(url)) if !url.is_empty() => MediaSource::Url {
                            url,
                            media_type: Some(request.format.media_type().into()),
                        },
                        _ => {
                            return Err(protocol_error(
                                self.provider(),
                                "image response item has no image payload",
                            ));
                        }
                    };
                    Ok(GeneratedImage {
                        source,
                        revised_prompt: image.revised_prompt,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            if images.is_empty() {
                return Err(protocol_error(
                    self.provider(),
                    "image response contains no outputs",
                ));
            }
            Ok(ImageGenerationResponse { images })
        })
    }
}

impl SpeechModel for OpenAiClient {
    fn synthesize_speech(
        &self,
        request: SpeechRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<SpeechResponse, ModelError>> {
        Box::pin(async move {
            validate_provider(self, &request.model.provider)?;
            if request.input.trim().is_empty() || request.voice.trim().is_empty() {
                return Err(invalid("speech input and voice must be non-empty"));
            }
            if request.input.chars().count() > 4_096 {
                return Err(invalid("speech input exceeds 4096 characters"));
            }
            if request
                .speed
                .is_some_and(|speed| !(0.25..=4.0).contains(&speed))
            {
                return Err(invalid("speech speed must be between 0.25 and 4.0"));
            }
            if request.instructions.is_some()
                && self.speech_wire_profile(&request.model)
                    != OpenAiSpeechWireProfile::Instructional
            {
                return Err(invalid(
                    "the selected speech wire profile does not support instructions",
                ));
            }
            let mut body = json!({
                "model": request.model.name,
                "input": request.input,
                "voice": request.voice,
                "response_format": speech_format(request.format),
            });
            if let Some(instructions) = request.instructions {
                body["instructions"] = Value::String(instructions);
            }
            if let Some(speed) = request.speed {
                body["speed"] = Value::from(speed);
            }
            let response = send_media_request(self, "audio/speech", &context, |builder| {
                builder
                    .header("Accept", request.format.media_type())
                    .json(&body)
            })
            .await?;
            let bytes = read_bounded(response, &context, self.provider()).await?;
            if bytes.is_empty() {
                return Err(protocol_error(self.provider(), "speech response is empty"));
            }
            Ok(SpeechResponse {
                media_type: request.format.media_type().into(),
                bytes,
            })
        })
    }
}

impl TranscriptionModel for OpenAiClient {
    fn transcribe(
        &self,
        request: TranscriptionRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<TranscriptionResponse, ModelError>> {
        Box::pin(async move {
            validate_provider(self, &request.model.provider)?;
            if request.file_name.trim().is_empty()
                || request.media_type.trim().is_empty()
                || request.bytes.is_empty()
            {
                return Err(invalid(
                    "transcription file name, media type, and bytes must be non-empty",
                ));
            }
            if request.bytes.len() > MAX_TRANSCRIPTION_INPUT_BYTES {
                return Err(invalid("transcription input exceeds 25 MiB"));
            }
            if request.prompt.is_some()
                && self.transcription_wire_profile(&request.model)
                    != OpenAiTranscriptionWireProfile::Prompted
            {
                return Err(invalid(
                    "the selected transcription wire profile does not support prompts",
                ));
            }
            let file = multipart::Part::bytes(request.bytes)
                .file_name(request.file_name)
                .mime_str(&request.media_type)
                .map_err(|_| invalid("transcription media type is invalid"))?;
            let mut form = multipart::Form::new()
                .part("file", file)
                .text("model", request.model.name)
                .text("response_format", "json");
            if let Some(language) = request.language {
                form = form.text("language", language);
            }
            if let Some(prompt) = request.prompt {
                form = form.text("prompt", prompt);
            }
            let response = send_media_request(self, "audio/transcriptions", &context, |builder| {
                builder.multipart(form)
            })
            .await?;
            let bytes = read_bounded(response, &context, self.provider()).await?;
            let payload: TranscriptionPayload = serde_json::from_slice(&bytes).map_err(|_| {
                protocol_error(self.provider(), "transcription response is invalid JSON")
            })?;
            if payload.text.trim().is_empty() {
                return Err(protocol_error(
                    self.provider(),
                    "transcription response text is empty",
                ));
            }
            Ok(TranscriptionResponse {
                text: payload.text,
                language: payload.language,
                duration_seconds: payload.duration,
            })
        })
    }
}

async fn send_media_request(
    client: &OpenAiClient,
    path: &str,
    context: &ModelCallContext,
    prepare: impl FnOnce(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
) -> Result<Response, ModelError> {
    let builder = prepare(client.authenticated_post(client.endpoint_for(path), context));
    let response = send_request(
        builder,
        context.cancellation(),
        client.provider(),
        context.deadline(),
    )
    .await?;
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let id = request_id(&response);
    let after = retry_after(&response);
    let body = read_error_body(
        response,
        context.cancellation(),
        client.provider(),
        context.deadline(),
    )
    .await?;
    Err(http_error(status, id, &body, client.provider(), after))
}

async fn read_bounded(
    response: Response,
    context: &ModelCallContext,
    provider: &str,
) -> Result<Vec<u8>, ModelError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_MEDIA_RESPONSE_BYTES as u64)
    {
        return Err(protocol_error(provider, "media response exceeds 32 MiB"));
    }
    let capacity = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or_default()
        .min(MAX_MEDIA_RESPONSE_BYTES);
    let mut bytes = Vec::with_capacity(capacity);
    let mut body = response.bytes_stream();
    while let Some(chunk) = {
        let cancellation_wait = context.cancellation().cancelled();
        let next = body.next();
        pin_mut!(cancellation_wait, next);
        match futures_util::future::select(cancellation_wait, next).await {
            Either::Left(_) => return Err(cancelled()),
            Either::Right((result, _)) => result,
        }
    } {
        let chunk = chunk.map_err(|error| transport_error(&error, provider, context.deadline()))?;
        append_bounded(&mut bytes, &chunk, provider, MAX_MEDIA_RESPONSE_BYTES)?;
    }
    Ok(bytes)
}

fn append_bounded(
    output: &mut Vec<u8>,
    chunk: &[u8],
    provider: &str,
    max_bytes: usize,
) -> Result<(), ModelError> {
    if chunk.len() > max_bytes.saturating_sub(output.len()) {
        return Err(protocol_error(provider, "media response exceeds 32 MiB"));
    }
    output.extend_from_slice(chunk);
    Ok(())
}

fn validate_provider(client: &OpenAiClient, provider: &str) -> Result<(), ModelError> {
    if provider == client.provider() {
        Ok(())
    } else {
        Err(invalid("media request provider does not match the client"))
    }
}

fn invalid(message: &str) -> ModelError {
    ModelError::local(ModelErrorKind::InvalidRequest, message)
}

const fn image_format(format: ImageFormat) -> &'static str {
    match format {
        ImageFormat::Webp => "webp",
        ImageFormat::Jpeg => "jpeg",
        _ => "png",
    }
}

const fn speech_format(format: SpeechFormat) -> &'static str {
    match format {
        SpeechFormat::Opus => "opus",
        SpeechFormat::Aac => "aac",
        SpeechFormat::Flac => "flac",
        SpeechFormat::Wav => "wav",
        SpeechFormat::Pcm => "pcm",
        _ => "mp3",
    }
}

#[derive(Deserialize)]
struct ImagePayload {
    data: Vec<ImagePayloadItem>,
}

#[derive(Deserialize)]
struct ImagePayloadItem {
    b64_json: Option<String>,
    url: Option<String>,
    revised_prompt: Option<String>,
}

#[derive(Deserialize)]
struct TranscriptionPayload {
    text: String,
    language: Option<String>,
    duration: Option<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_append_rejects_before_extending_the_buffer() {
        let mut output = vec![0; 4];
        let error = append_bounded(&mut output, &[1], "test", 4).unwrap_err();
        assert_eq!(error.kind, ModelErrorKind::Protocol);
        assert_eq!(output.len(), 4);
    }

    #[test]
    fn public_defaults_are_exact_and_unknown_models_are_conservative() {
        let catalog = OpenAiMediaCapabilityCatalog::public_openai_defaults("openai");
        assert_eq!(
            catalog.image_profile(&ModelRef::new("openai", "dall-e-3")),
            Some(OpenAiImageWireProfile::DallE3)
        );
        assert_eq!(
            catalog.image_profile(&ModelRef::new("openai", "future-image-model")),
            None
        );
        assert_eq!(
            catalog.speech_profile(&ModelRef::new("openai", "tts-1")),
            Some(OpenAiSpeechWireProfile::Conservative)
        );
        assert_eq!(
            catalog.transcription_profile(&ModelRef::new("openai", "gpt-4o-transcribe-diarize")),
            Some(OpenAiTranscriptionWireProfile::Diarized)
        );
    }
}
