//! OpenAI-compatible Responses, Chat Completions, and embeddings.

mod chat;
mod client;
mod config;
mod control;
mod decode;
mod embedding;
mod encode;
mod realtime;
mod realtime_audio;
mod realtime_call;
#[cfg(target_arch = "wasm32")]
mod realtime_data_channel;
mod realtime_event;
mod realtime_reconnect;
mod realtime_session_transport;
mod realtime_transport;
#[cfg(target_arch = "wasm32")]
mod realtime_webrtc;

pub use chat::{ChatCompletionsDecoder, encode_chat_request};
pub use client::OpenAiClient;
pub use config::{
    AzureOpenAiApiVersion, OpenAiCompatibleProfile, OpenAiConfig, OpenAiConfigError,
    OpenAiWireProtocol,
};
pub use control::{
    OpenAiBatch, OpenAiBatchEndpoint, OpenAiBatchRequest, OpenAiBatchStatus, OpenAiControlError,
    OpenAiControlFuture, OpenAiControlPlane, OpenAiFile, OpenAiFilePurpose, OpenAiFileUpload,
    OpenAiModelInfo, OpenAiRealtimeClientSecret, OpenAiRealtimeClientSecretRequest,
};
pub use decode::OpenAiEventDecoder;
pub use embedding::OpenAiEmbeddingModel;
pub use encode::encode_request;
pub use realtime::{
    OpenAiRealtimeClient, OpenAiRealtimeCommand, OpenAiRealtimeConnection, OpenAiRealtimeError,
    OpenAiRealtimeEvent, OpenAiRealtimeModality, OpenAiRealtimeSessionUpdate, OpenAiRealtimeState,
    RealtimeReconnectDisposition,
};
pub use realtime_audio::{
    OPENAI_REALTIME_MAX_AUDIO_CHUNK_BYTES, OpenAiRealtimeAudioChunk, OpenAiRealtimeAudioFormat,
};
pub use realtime_call::{OpenAiRealtimeCall, OpenAiRealtimeCallRequest, OpenAiRealtimeSdpOffer};
pub use realtime_reconnect::{
    OpenAiRealtimeReconnectAttempt, OpenAiRealtimeReconnectController,
    OpenAiRealtimeReconnectError, OpenAiRealtimeReconnectEvent, OpenAiRealtimeReconnectFailureKind,
    OpenAiRealtimeReconnectPolicy, OpenAiRealtimeReconnectPolicyError,
    OpenAiRealtimeReconnectStopReason,
};
#[cfg(target_arch = "wasm32")]
pub use realtime_webrtc::{
    OpenAiRealtimeIceServer, OpenAiRealtimeIceTransportPolicy, OpenAiRealtimePendingWebRtc,
    OpenAiRealtimeWebRtcConnectionState, OpenAiRealtimeWebRtcIceState, OpenAiRealtimeWebRtcOptions,
    OpenAiRealtimeWebRtcSession,
};
