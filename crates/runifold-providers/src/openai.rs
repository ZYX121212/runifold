//! OpenAI-compatible Responses, Chat Completions, and embeddings.

mod chat;
mod client;
mod config;
mod control;
mod decode;
mod embedding;
mod encode;
mod hosted_tool;
mod media;
#[cfg(feature = "openai-realtime")]
mod realtime;
#[cfg(feature = "openai-realtime")]
mod realtime_audio;
#[cfg(feature = "openai-realtime")]
mod realtime_call;
#[cfg(all(feature = "openai-realtime", target_arch = "wasm32"))]
mod realtime_data_channel;
#[cfg(feature = "openai-realtime")]
mod realtime_event;
#[cfg(feature = "openai-realtime")]
mod realtime_reconnect;
#[cfg(feature = "openai-realtime")]
mod realtime_session_transport;
#[cfg(feature = "openai-realtime")]
mod realtime_transport;
#[cfg(all(feature = "openai-realtime", target_arch = "wasm32"))]
mod realtime_webrtc;

pub use chat::{ChatCompletionsDecoder, encode_chat_request};
pub use client::OpenAiClient;
pub use config::{
    AzureOpenAiApiVersion, OpenAiCompatibleProfile, OpenAiConfig, OpenAiConfigError,
    OpenAiWireProtocol,
};
pub use control::{
    OpenAiBatch, OpenAiBatchEndpoint, OpenAiBatchRequest, OpenAiBatchStatus, OpenAiControlError,
    OpenAiControlFuture, OpenAiControlPlane, OpenAiFile, OpenAiFileDeletion, OpenAiFilePurpose,
    OpenAiFileStatus, OpenAiFileUpload, OpenAiFileWaitPolicy, OpenAiModelInfo,
};
#[cfg(feature = "openai-realtime")]
pub use control::{OpenAiRealtimeClientSecret, OpenAiRealtimeClientSecretRequest};
pub use decode::OpenAiEventDecoder;
pub use embedding::OpenAiEmbeddingModel;
pub use encode::encode_request;
pub use hosted_tool::{OpenAiHostedTool, OpenAiHostedToolError};
pub use media::{
    OpenAiImageWireProfile, OpenAiMediaCapabilityCatalog, OpenAiSpeechWireProfile,
    OpenAiTranscriptionWireProfile,
};
#[cfg(feature = "openai-realtime")]
pub use realtime::{
    OpenAiRealtimeClient, OpenAiRealtimeCommand, OpenAiRealtimeConnection, OpenAiRealtimeError,
    OpenAiRealtimeEvent, OpenAiRealtimeModality, OpenAiRealtimeSessionUpdate, OpenAiRealtimeState,
    RealtimeReconnectDisposition,
};
#[cfg(feature = "openai-realtime")]
pub use realtime_audio::{
    OPENAI_REALTIME_MAX_AUDIO_CHUNK_BYTES, OpenAiRealtimeAudioChunk, OpenAiRealtimeAudioFormat,
};
#[cfg(feature = "openai-realtime")]
pub use realtime_call::{OpenAiRealtimeCall, OpenAiRealtimeCallRequest, OpenAiRealtimeSdpOffer};
#[cfg(feature = "openai-realtime")]
pub use realtime_reconnect::{
    OpenAiRealtimeReconnectAttempt, OpenAiRealtimeReconnectController,
    OpenAiRealtimeReconnectError, OpenAiRealtimeReconnectEvent, OpenAiRealtimeReconnectFailureKind,
    OpenAiRealtimeReconnectPolicy, OpenAiRealtimeReconnectPolicyError,
    OpenAiRealtimeReconnectStopReason,
};

/// Ark-hosted web-search configuration for the Responses API.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArkWebSearchTool {
    limit: Option<u32>,
    max_keyword: Option<u32>,
}

impl ArkWebSearchTool {
    /// Creates web search with Ark defaults.
    pub const fn new() -> Self {
        Self {
            limit: None,
            max_keyword: None,
        }
    }

    /// Limits the number of returned search results.
    #[must_use]
    pub const fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Limits the number of generated search keywords.
    #[must_use]
    pub const fn max_keyword(mut self, max_keyword: u32) -> Self {
        self.max_keyword = Some(max_keyword);
        self
    }
}

impl From<ArkWebSearchTool> for runifold_model::ProviderToolSpec {
    fn from(tool: ArkWebSearchTool) -> Self {
        let mut options = std::collections::BTreeMap::new();
        if let Some(limit) = tool.limit {
            options.insert("limit".into(), serde_json::Value::from(limit));
        }
        if let Some(max_keyword) = tool.max_keyword {
            options.insert("max_keyword".into(), serde_json::Value::from(max_keyword));
        }
        Self {
            provider: "ark".into(),
            tool_type: "web_search".into(),
            options,
        }
    }
}
#[cfg(all(feature = "openai-realtime", target_arch = "wasm32"))]
pub use realtime_webrtc::{
    OpenAiRealtimeIceServer, OpenAiRealtimeIceTransportPolicy, OpenAiRealtimePendingWebRtc,
    OpenAiRealtimeWebRtcConnectionState, OpenAiRealtimeWebRtcIceState, OpenAiRealtimeWebRtcOptions,
    OpenAiRealtimeWebRtcSession,
};
