//! Strict decoding for `OpenAI` Realtime server events.

use serde_json::Value;

use super::{
    realtime::{OpenAiRealtimeError, OpenAiRealtimeEvent},
    realtime_audio::OpenAiRealtimeAudioChunk,
};

pub(crate) fn parse_event(payload: Value) -> Result<OpenAiRealtimeEvent, OpenAiRealtimeError> {
    let event_type = string_at(&payload, &["type"])?;
    let event = match event_type {
        "session.created" => OpenAiRealtimeEvent::SessionCreated {
            session_id: string_at(&payload, &["session", "id"])?.into(),
            payload,
        },
        "session.updated" => OpenAiRealtimeEvent::SessionUpdated {
            session_id: optional_string_at(&payload, &["session", "id"]),
            payload,
        },
        "response.created" => OpenAiRealtimeEvent::ResponseCreated {
            response_id: string_at(&payload, &["response", "id"])?.into(),
            payload,
        },
        "response.output_text.delta" => OpenAiRealtimeEvent::OutputTextDelta {
            response_id: string_at(&payload, &["response_id"])?.into(),
            delta: string_at(&payload, &["delta"])?.into(),
            payload,
        },
        "response.function_call_arguments.delta" => OpenAiRealtimeEvent::FunctionArgumentsDelta {
            response_id: string_at(&payload, &["response_id"])?.into(),
            call_id: string_at(&payload, &["call_id"])?.into(),
            delta: string_at(&payload, &["delta"])?.into(),
            payload,
        },
        "input_audio_buffer.committed" => OpenAiRealtimeEvent::InputAudioCommitted {
            item_id: string_at(&payload, &["item_id"])?.into(),
            previous_item_id: optional_string_at(&payload, &["previous_item_id"]),
            payload,
        },
        "input_audio_buffer.speech_started" => OpenAiRealtimeEvent::InputAudioSpeechStarted {
            audio_start_ms: u64_at(&payload, &["audio_start_ms"])?,
            item_id: string_at(&payload, &["item_id"])?.into(),
            payload,
        },
        "input_audio_buffer.speech_stopped" => OpenAiRealtimeEvent::InputAudioSpeechStopped {
            audio_end_ms: u64_at(&payload, &["audio_end_ms"])?,
            item_id: string_at(&payload, &["item_id"])?.into(),
            payload,
        },
        "response.output_audio.delta" => OpenAiRealtimeEvent::OutputAudioDelta {
            response_id: string_at(&payload, &["response_id"])?.into(),
            item_id: string_at(&payload, &["item_id"])?.into(),
            output_index: u64_at(&payload, &["output_index"])?,
            content_index: u64_at(&payload, &["content_index"])?,
            audio: OpenAiRealtimeAudioChunk::decode_base64(string_at(&payload, &["delta"])?)?,
            payload,
        },
        "response.output_audio.done" => OpenAiRealtimeEvent::OutputAudioDone {
            response_id: string_at(&payload, &["response_id"])?.into(),
            payload,
        },
        "response.output_audio_transcript.delta" => {
            OpenAiRealtimeEvent::OutputAudioTranscriptDelta {
                response_id: string_at(&payload, &["response_id"])?.into(),
                delta: string_at(&payload, &["delta"])?.into(),
                payload,
            }
        }
        "response.output_audio_transcript.done" => OpenAiRealtimeEvent::OutputAudioTranscriptDone {
            response_id: string_at(&payload, &["response_id"])?.into(),
            transcript: string_at(&payload, &["transcript"])?.into(),
            payload,
        },
        "response.done" => OpenAiRealtimeEvent::ResponseDone {
            response_id: string_at(&payload, &["response", "id"])?.into(),
            status: string_at(&payload, &["response", "status"])?.into(),
            payload,
        },
        "error" => OpenAiRealtimeEvent::Error {
            code: optional_string_at(&payload, &["error", "code"]),
            message: string_at(&payload, &["error", "message"])?.into(),
            event_id: optional_string_at(&payload, &["error", "event_id"]),
            payload,
        },
        _ => OpenAiRealtimeEvent::Unknown {
            event_type: event_type.into(),
            payload,
        },
    };
    Ok(event)
}

fn string_at<'a>(value: &'a Value, path: &[&str]) -> Result<&'a str, OpenAiRealtimeError> {
    let mut current = value;
    for segment in path {
        current = current.get(segment).ok_or_else(|| {
            OpenAiRealtimeError::Protocol(format!(
                "event is missing string field `{}`",
                path.join(".")
            ))
        })?;
    }
    current.as_str().ok_or_else(|| {
        OpenAiRealtimeError::Protocol(format!("event field `{}` must be a string", path.join(".")))
    })
}

fn optional_string_at(value: &Value, path: &[&str]) -> Option<String> {
    let mut current = value;
    for segment in path {
        current = current.get(segment)?;
    }
    current.as_str().map(str::to_owned)
}

fn u64_at(value: &Value, path: &[&str]) -> Result<u64, OpenAiRealtimeError> {
    let mut current = value;
    for segment in path {
        current = current.get(segment).ok_or_else(|| {
            OpenAiRealtimeError::Protocol(format!(
                "event is missing integer field `{}`",
                path.join(".")
            ))
        })?;
    }
    current.as_u64().ok_or_else(|| {
        OpenAiRealtimeError::Protocol(format!(
            "event field `{}` must be a non-negative integer",
            path.join(".")
        ))
    })
}
