//! Typed `OpenAI` Realtime commands, events, and session state.

use std::future::Future;

use futures_timer::Delay;
use futures_util::future::{Either, select};
use runifold_model::ModelCallContext;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

use super::{
    OpenAiConfig,
    realtime_audio::{OpenAiRealtimeAudioChunk, OpenAiRealtimeAudioFormat},
    realtime_event::parse_event,
    realtime_session_transport::{OpenAiRealtimeTransport, connect_headers},
    realtime_transport::{RealtimeConnectOptions, RealtimeTransport, RealtimeTransportError},
};

const MAX_TEXT_BYTES: usize = 256 * 1024;
const MAX_EVENT_ID_BYTES: usize = 512;

/// Failure at the typed Realtime session boundary.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OpenAiRealtimeError {
    /// A command or configuration violated a local invariant.
    #[error("invalid Realtime request: {0}")]
    InvalidRequest(String),
    /// The operation was cancelled by its owning run.
    #[error("Realtime operation was cancelled")]
    Cancelled,
    /// The operation crossed its explicit deadline.
    #[error("Realtime operation exceeded its deadline")]
    DeadlineExceeded,
    /// The WebSocket handshake or frame transport failed.
    #[error("Realtime WebSocket transport failed")]
    Transport,
    /// A browser WebRTC API rejected negotiation or media setup.
    #[error("Realtime browser WebRTC failed: {0}")]
    BrowserWebRtc(String),
    /// The application Gateway rejected SDP exchange.
    #[error("Realtime SDP Gateway exchange failed with HTTP {status}")]
    SdpExchange {
        /// HTTP status returned by the Gateway.
        status: u16,
        /// Whether a fresh negotiation may retry this status.
        retryable: bool,
    },
    /// The peer closed the WebSocket.
    #[error("Realtime WebSocket closed with code {code}: {reason}")]
    Closed {
        /// WebSocket close code.
        code: u16,
        /// Whether the browser reported a clean close.
        clean: bool,
        /// Bounded close reason.
        reason: String,
        /// Whether reconnecting can safely repeat the interrupted operation.
        disposition: RealtimeReconnectDisposition,
    },
    /// The peer violated the expected JSON event protocol.
    #[error("invalid Realtime protocol event: {0}")]
    Protocol(String),
}

/// Replay safety after a Realtime connection loss.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RealtimeReconnectDisposition {
    /// No server session was observed, so reconnecting is safe.
    SafeBeforeSession,
    /// The session was idle; reconnecting starts a new, empty server session.
    SafeWhenIdle,
    /// A response may have committed output before disconnecting.
    AmbiguousResponseInFlight,
}

/// Output modality requested for a Realtime session.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OpenAiRealtimeModality {
    /// Text-only output.
    Text,
    /// Audio output with its transcript.
    Audio,
}

/// Validated partial configuration for a `session.update` command.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct OpenAiRealtimeSessionUpdate {
    #[serde(rename = "type")]
    session_type: RealtimeSessionType,
    #[serde(skip_serializing_if = "Option::is_none")]
    instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_modalities: Option<Vec<OpenAiRealtimeModality>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_output_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    audio: Option<RealtimeAudioConfig>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct RealtimeAudioConfig {
    input: RealtimeAudioInput,
    output: RealtimeAudioOutput,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct RealtimeAudioInput {
    format: OpenAiRealtimeAudioFormat,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct RealtimeAudioOutput {
    format: OpenAiRealtimeAudioFormat,
    #[serde(skip_serializing_if = "Option::is_none")]
    voice: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RealtimeSessionType {
    #[default]
    Realtime,
}

impl OpenAiRealtimeSessionUpdate {
    /// Creates an empty partial update with the required GA session type.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets system instructions.
    ///
    /// # Errors
    ///
    /// Rejects instructions larger than 256 KiB.
    pub fn with_instructions(
        mut self,
        instructions: impl Into<String>,
    ) -> Result<Self, OpenAiRealtimeError> {
        let instructions = instructions.into();
        validate_text("instructions", &instructions)?;
        self.instructions = Some(instructions);
        Ok(self)
    }

    /// Selects exactly one GA output modality.
    #[must_use]
    pub fn with_modality(mut self, modality: OpenAiRealtimeModality) -> Self {
        self.output_modalities = Some(vec![modality]);
        self
    }

    /// Bounds each generated response.
    ///
    /// # Errors
    ///
    /// GA accepts integer limits from 1 through 4096.
    pub fn with_max_output_tokens(mut self, tokens: u32) -> Result<Self, OpenAiRealtimeError> {
        if !(1..=4096).contains(&tokens) {
            return Err(OpenAiRealtimeError::InvalidRequest(
                "Realtime max_output_tokens must be between 1 and 4096".into(),
            ));
        }
        self.max_output_tokens = Some(tokens);
        Ok(self)
    }

    /// Configures the input and output audio encodings.
    #[must_use]
    pub fn with_audio_formats(
        mut self,
        input: OpenAiRealtimeAudioFormat,
        output: OpenAiRealtimeAudioFormat,
    ) -> Self {
        self.audio = Some(RealtimeAudioConfig {
            input: RealtimeAudioInput { format: input },
            output: RealtimeAudioOutput {
                format: output,
                voice: None,
            },
        });
        self
    }

    /// Selects a forward-compatible output voice.
    ///
    /// # Errors
    ///
    /// Rejects a voice before audio is configured and invalid identifiers.
    pub fn with_voice(mut self, voice: impl Into<String>) -> Result<Self, OpenAiRealtimeError> {
        let voice = voice.into();
        validate_id("Realtime voice", &voice)?;
        let audio = self.audio.as_mut().ok_or_else(|| {
            OpenAiRealtimeError::InvalidRequest(
                "configure Realtime audio formats before selecting a voice".into(),
            )
        })?;
        audio.output.voice = Some(voice);
        Ok(self)
    }
}

/// A validated client command sent over the Realtime WebSocket.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum OpenAiRealtimeCommand {
    /// Apply a partial session configuration update.
    UpdateSession {
        /// Optional client correlation identity.
        event_id: Option<String>,
        /// Partial GA session configuration.
        session: OpenAiRealtimeSessionUpdate,
    },
    /// Append one user text message to the default conversation.
    UserText {
        /// Optional client correlation identity.
        event_id: Option<String>,
        /// User-visible text.
        text: String,
    },
    /// Appends one bounded raw audio chunk to the session input buffer.
    AppendInputAudio {
        /// Optional client correlation identity.
        event_id: Option<String>,
        /// Raw bytes in the configured input format.
        audio: OpenAiRealtimeAudioChunk,
    },
    /// Commits buffered input audio when automatic VAD is disabled.
    CommitInputAudio {
        /// Optional client correlation identity.
        event_id: Option<String>,
    },
    /// Clears buffered input audio without creating a conversation item.
    ClearInputAudio {
        /// Optional client correlation identity.
        event_id: Option<String>,
    },
    /// Start a response in the default conversation.
    CreateResponse {
        /// Optional client correlation identity.
        event_id: Option<String>,
    },
    /// Cancel an active response.
    CancelResponse {
        /// Optional client correlation identity.
        event_id: Option<String>,
        /// Specific response identity, or the active default response.
        response_id: Option<String>,
    },
}

impl OpenAiRealtimeCommand {
    /// Creates a session update command.
    pub fn update_session(session: OpenAiRealtimeSessionUpdate) -> Self {
        Self::UpdateSession {
            event_id: None,
            session,
        }
    }

    /// Creates a validated user text command.
    ///
    /// # Errors
    ///
    /// Rejects empty or oversized text.
    pub fn user_text(text: impl Into<String>) -> Result<Self, OpenAiRealtimeError> {
        let text = text.into();
        validate_text("user text", &text)?;
        if text.trim().is_empty() {
            return Err(OpenAiRealtimeError::InvalidRequest(
                "Realtime user text cannot be empty".into(),
            ));
        }
        Ok(Self::UserText {
            event_id: None,
            text,
        })
    }

    /// Creates an input-audio append command.
    pub fn append_input_audio(audio: OpenAiRealtimeAudioChunk) -> Self {
        Self::AppendInputAudio {
            event_id: None,
            audio,
        }
    }

    /// Creates a manual input-audio commit command.
    pub fn commit_input_audio() -> Self {
        Self::CommitInputAudio { event_id: None }
    }

    /// Creates an input-audio clear command.
    pub fn clear_input_audio() -> Self {
        Self::ClearInputAudio { event_id: None }
    }

    /// Creates a response command.
    pub fn create_response() -> Self {
        Self::CreateResponse { event_id: None }
    }

    /// Creates a response cancellation command.
    pub fn cancel_response(response_id: Option<String>) -> Self {
        Self::CancelResponse {
            event_id: None,
            response_id,
        }
    }

    /// Adds a client-generated correlation identity.
    ///
    /// # Errors
    ///
    /// Rejects empty, control-containing, or oversized identities.
    pub fn with_event_id(
        mut self,
        event_id: impl Into<String>,
    ) -> Result<Self, OpenAiRealtimeError> {
        let event_id = event_id.into();
        validate_id("event_id", &event_id)?;
        match &mut self {
            Self::UpdateSession {
                event_id: target, ..
            }
            | Self::UserText {
                event_id: target, ..
            }
            | Self::AppendInputAudio {
                event_id: target, ..
            }
            | Self::CommitInputAudio { event_id: target }
            | Self::ClearInputAudio { event_id: target }
            | Self::CreateResponse { event_id: target }
            | Self::CancelResponse {
                event_id: target, ..
            } => *target = Some(event_id),
        }
        Ok(self)
    }

    fn as_value(&self) -> Value {
        let (mut value, event_id) = match self {
            Self::UpdateSession { event_id, session } => (
                json!({"type": "session.update", "session": session}),
                event_id,
            ),
            Self::UserText { event_id, text } => (
                json!({
                    "type": "conversation.item.create",
                    "item": {
                        "type": "message",
                        "role": "user",
                        "content": [{"type": "input_text", "text": text}],
                    },
                }),
                event_id,
            ),
            Self::AppendInputAudio { event_id, audio } => (
                json!({
                    "type": "input_audio_buffer.append",
                    "audio": audio.encode_base64(),
                }),
                event_id,
            ),
            Self::CommitInputAudio { event_id } => {
                (json!({"type": "input_audio_buffer.commit"}), event_id)
            }
            Self::ClearInputAudio { event_id } => {
                (json!({"type": "input_audio_buffer.clear"}), event_id)
            }
            Self::CreateResponse { event_id } => (json!({"type": "response.create"}), event_id),
            Self::CancelResponse {
                event_id,
                response_id,
            } => {
                let mut value = json!({"type": "response.cancel"});
                if let Some(response_id) = response_id {
                    value["response_id"] = Value::String(response_id.clone());
                }
                (value, event_id)
            }
        };
        if let Some(event_id) = event_id {
            value["event_id"] = Value::String(event_id.clone());
        }
        value
    }
}

/// A typed event received from the Realtime server.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum OpenAiRealtimeEvent {
    /// The initial server session was created.
    SessionCreated {
        /// Server session identity.
        session_id: String,
        /// Original forward-compatible payload.
        payload: Value,
    },
    /// A session update was acknowledged.
    SessionUpdated {
        /// Server session identity when exposed.
        session_id: Option<String>,
        /// Original forward-compatible payload.
        payload: Value,
    },
    /// A response started.
    ResponseCreated {
        /// Server response identity.
        response_id: String,
        /// Original forward-compatible payload.
        payload: Value,
    },
    /// Incremental output text.
    OutputTextDelta {
        /// Owning response identity.
        response_id: String,
        /// Text fragment.
        delta: String,
        /// Original forward-compatible payload.
        payload: Value,
    },
    /// Incremental function-call arguments.
    FunctionArgumentsDelta {
        /// Owning response identity.
        response_id: String,
        /// Tool-call identity.
        call_id: String,
        /// Raw JSON fragment.
        delta: String,
        /// Original forward-compatible payload.
        payload: Value,
    },
    /// Buffered input audio was committed to a user conversation item.
    InputAudioCommitted {
        /// Created user-item identity.
        item_id: String,
        /// Preceding item, or no predecessor.
        previous_item_id: Option<String>,
        /// Original forward-compatible payload.
        payload: Value,
    },
    /// Server VAD detected the start of speech.
    InputAudioSpeechStarted {
        /// Millisecond offset into the input buffer.
        audio_start_ms: u64,
        /// Tentative conversation item identity.
        item_id: String,
        /// Original forward-compatible payload.
        payload: Value,
    },
    /// Server VAD detected the end of speech.
    InputAudioSpeechStopped {
        /// Millisecond offset into the input buffer.
        audio_end_ms: u64,
        /// Tentative conversation item identity.
        item_id: String,
        /// Original forward-compatible payload.
        payload: Value,
    },
    /// Incremental decoded model audio.
    OutputAudioDelta {
        /// Owning response identity.
        response_id: String,
        /// Owning output-item identity.
        item_id: String,
        /// Output item position.
        output_index: u64,
        /// Content-part position.
        content_index: u64,
        /// Decoded bounded raw audio.
        audio: OpenAiRealtimeAudioChunk,
        /// Original forward-compatible payload.
        payload: Value,
    },
    /// Model audio generation completed for one content part.
    OutputAudioDone {
        /// Owning response identity.
        response_id: String,
        /// Original forward-compatible payload.
        payload: Value,
    },
    /// Incremental transcript of model audio.
    OutputAudioTranscriptDelta {
        /// Owning response identity.
        response_id: String,
        /// Transcript fragment.
        delta: String,
        /// Original forward-compatible payload.
        payload: Value,
    },
    /// Model audio transcript completed.
    OutputAudioTranscriptDone {
        /// Owning response identity.
        response_id: String,
        /// Complete transcript.
        transcript: String,
        /// Original forward-compatible payload.
        payload: Value,
    },
    /// A response reached a terminal state.
    ResponseDone {
        /// Server response identity.
        response_id: String,
        /// Provider terminal status.
        status: String,
        /// Original payload, including usage.
        payload: Value,
    },
    /// A provider error correlated to an optional client event.
    Error {
        /// Stable provider error code when exposed.
        code: Option<String>,
        /// Safe provider message.
        message: String,
        /// Client event that caused the error.
        event_id: Option<String>,
        /// Original forward-compatible payload.
        payload: Value,
    },
    /// An event not yet normalized by this version.
    Unknown {
        /// Provider event discriminator.
        event_type: String,
        /// Losslessly retained payload.
        payload: Value,
    },
}

/// Observable local lifecycle of a Realtime connection.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiRealtimeState {
    /// Connected, awaiting the mandatory `session.created` event.
    AwaitingSession,
    /// Session exists and no response is active.
    Ready {
        /// Server session identity.
        session_id: String,
    },
    /// The server is generating one response.
    Responding {
        /// Server session identity.
        session_id: String,
        /// Active response identity.
        response_id: String,
    },
    /// The client closed the connection.
    Closed,
}

/// Model-bound Realtime connector created by [`super::OpenAiClient`].
#[derive(Clone, Debug)]
pub struct OpenAiRealtimeClient {
    config: OpenAiConfig,
    model: String,
}

impl OpenAiRealtimeClient {
    pub(crate) fn new(
        config: OpenAiConfig,
        model: impl Into<String>,
    ) -> Result<Self, OpenAiRealtimeError> {
        let model = model.into();
        validate_id("Realtime model", &model)?;
        Ok(Self { config, model })
    }

    /// Opens a GA Realtime WebSocket session.
    ///
    /// # Errors
    ///
    /// Returns a typed cancellation, deadline, configuration, or transport
    /// failure. Browser builds reject long-lived credentials and require a
    /// credential-free application Gateway.
    pub async fn connect(
        &self,
        context: ModelCallContext,
    ) -> Result<OpenAiRealtimeConnection, OpenAiRealtimeError> {
        let options = self.connect_options()?;
        let transport = run_operation(RealtimeTransport::connect(options), &context).await?;
        Ok(OpenAiRealtimeConnection {
            transport: OpenAiRealtimeTransport::WebSocket(transport),
            state: OpenAiRealtimeState::AwaitingSession,
        })
    }

    fn connect_options(&self) -> Result<RealtimeConnectOptions, OpenAiRealtimeError> {
        if self.config.azure_api_version.is_some() {
            return Err(OpenAiRealtimeError::InvalidRequest(
                "Azure Realtime endpoints require an explicit compatible Gateway URL".into(),
            ));
        }
        let mut url = self
            .config
            .base_url
            .join("realtime")
            .map_err(|_| OpenAiRealtimeError::InvalidRequest("invalid Realtime URL".into()))?;
        let scheme = match url.scheme() {
            "http" => "ws",
            "https" => "wss",
            _ => {
                return Err(OpenAiRealtimeError::InvalidRequest(
                    "Realtime base URL must use HTTP or HTTPS".into(),
                ));
            }
        };
        url.set_scheme(scheme).map_err(|()| {
            OpenAiRealtimeError::InvalidRequest("invalid Realtime WebSocket scheme".into())
        })?;
        url.query_pairs_mut().append_pair("model", &self.model);

        let headers = connect_headers(&self.config);
        #[cfg(target_arch = "wasm32")]
        if self.config.api_key.is_some()
            || self.config.azure_api_key.is_some()
            || self.config.organization.is_some()
            || self.config.project.is_some()
        {
            return Err(OpenAiRealtimeError::InvalidRequest(
                "browser Realtime requires a credential-free Gateway configuration".into(),
            ));
        }
        Ok(RealtimeConnectOptions {
            url: url.into(),
            headers,
        })
    }
}

/// One bounded, explicitly driven Realtime connection.
#[derive(Debug)]
pub struct OpenAiRealtimeConnection {
    transport: OpenAiRealtimeTransport,
    state: OpenAiRealtimeState,
}

impl OpenAiRealtimeConnection {
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn from_data_channel(
        transport: super::realtime_data_channel::RealtimeDataChannelTransport,
    ) -> Self {
        Self {
            transport: OpenAiRealtimeTransport::WebRtc(transport),
            state: OpenAiRealtimeState::AwaitingSession,
        }
    }

    /// Returns the current local lifecycle.
    pub const fn state(&self) -> &OpenAiRealtimeState {
        &self.state
    }

    /// Classifies whether opening a new Realtime session can safely recover
    /// from a connection failure at the current protocol phase.
    pub const fn reconnect_disposition(&self) -> RealtimeReconnectDisposition {
        reconnect_disposition(&self.state)
    }

    /// Sends one typed command after validating it against local state.
    ///
    /// # Errors
    ///
    /// Fails before transport when the command is invalid for the current
    /// session phase.
    pub async fn send(
        &mut self,
        command: &OpenAiRealtimeCommand,
        context: ModelCallContext,
    ) -> Result<(), OpenAiRealtimeError> {
        self.validate_command(command)?;
        let text = serde_json::to_string(&command.as_value())
            .map_err(|error| OpenAiRealtimeError::Protocol(error.to_string()))?;
        let result = run_operation(self.transport.send_text(&text), &context).await;
        self.with_connection_disposition(result)
    }

    /// Receives and applies exactly one server event, providing natural
    /// consumer-driven backpressure.
    ///
    /// # Errors
    ///
    /// Returns typed cancellation, deadline, close, transport, and strict
    /// lifecycle failures.
    pub async fn next_event(
        &mut self,
        context: ModelCallContext,
    ) -> Result<OpenAiRealtimeEvent, OpenAiRealtimeError> {
        let text = match run_operation(self.transport.next_text(), &context).await {
            Ok(Some(text)) => text,
            Ok(None) => return Err(self.closed_error(1005, true, "peer ended stream")),
            Err(OpenAiRealtimeError::Closed {
                code,
                clean,
                reason,
                ..
            }) => return Err(self.closed_error(code, clean, &reason)),
            Err(error) => return Err(error),
        };
        let payload: Value = serde_json::from_str(&text)
            .map_err(|error| OpenAiRealtimeError::Protocol(error.to_string()))?;
        let event = parse_event(payload)?;
        self.apply_event(&event)?;
        Ok(event)
    }

    /// Closes the socket and marks the local session terminal.
    ///
    /// # Errors
    ///
    /// Returns a typed cancellation, deadline, or transport failure.
    pub async fn close(&mut self, context: ModelCallContext) -> Result<(), OpenAiRealtimeError> {
        let result = run_operation(self.transport.close(), &context).await;
        self.with_connection_disposition(result)?;
        self.state = OpenAiRealtimeState::Closed;
        Ok(())
    }

    fn validate_command(&self, command: &OpenAiRealtimeCommand) -> Result<(), OpenAiRealtimeError> {
        match (&self.state, command) {
            (OpenAiRealtimeState::Ready { .. }, _)
            | (
                OpenAiRealtimeState::Responding { .. },
                OpenAiRealtimeCommand::CancelResponse { .. },
            )
            | (
                OpenAiRealtimeState::Responding { .. },
                OpenAiRealtimeCommand::UpdateSession { .. },
            )
            | (
                OpenAiRealtimeState::Responding { .. },
                OpenAiRealtimeCommand::AppendInputAudio { .. }
                | OpenAiRealtimeCommand::ClearInputAudio { .. },
            ) => Ok(()),
            (OpenAiRealtimeState::AwaitingSession, _) => Err(OpenAiRealtimeError::InvalidRequest(
                "wait for session.created before sending commands".into(),
            )),
            (OpenAiRealtimeState::Responding { .. }, _) => Err(
                OpenAiRealtimeError::InvalidRequest("a response is already in flight".into()),
            ),
            (OpenAiRealtimeState::Closed, _) => Err(OpenAiRealtimeError::InvalidRequest(
                "Realtime connection is closed".into(),
            )),
        }
    }

    fn apply_event(&mut self, event: &OpenAiRealtimeEvent) -> Result<(), OpenAiRealtimeError> {
        if let Some(event_response_id) = response_event_id(event) {
            let OpenAiRealtimeState::Responding { response_id, .. } = &self.state else {
                return Err(OpenAiRealtimeError::Protocol(
                    "received response output without an active response".into(),
                ));
            };
            if response_id != event_response_id {
                return Err(OpenAiRealtimeError::Protocol(
                    "response event identity does not match the active response".into(),
                ));
            }
        }

        match (&self.state, event) {
            (
                OpenAiRealtimeState::AwaitingSession,
                OpenAiRealtimeEvent::SessionCreated { session_id, .. },
            ) => {
                self.state = OpenAiRealtimeState::Ready {
                    session_id: session_id.clone(),
                };
            }
            (
                OpenAiRealtimeState::Ready { session_id },
                OpenAiRealtimeEvent::ResponseCreated { response_id, .. },
            ) => {
                self.state = OpenAiRealtimeState::Responding {
                    session_id: session_id.clone(),
                    response_id: response_id.clone(),
                };
            }
            (
                OpenAiRealtimeState::Responding {
                    session_id,
                    response_id,
                },
                OpenAiRealtimeEvent::ResponseDone {
                    response_id: done_id,
                    ..
                },
            ) if response_id == done_id => {
                self.state = OpenAiRealtimeState::Ready {
                    session_id: session_id.clone(),
                };
            }
            (
                _,
                OpenAiRealtimeEvent::Error { .. }
                | OpenAiRealtimeEvent::Unknown { .. }
                | OpenAiRealtimeEvent::SessionUpdated { .. }
                | OpenAiRealtimeEvent::InputAudioCommitted { .. }
                | OpenAiRealtimeEvent::InputAudioSpeechStarted { .. }
                | OpenAiRealtimeEvent::InputAudioSpeechStopped { .. },
            )
            | (
                OpenAiRealtimeState::Responding { .. },
                OpenAiRealtimeEvent::OutputTextDelta { .. }
                | OpenAiRealtimeEvent::FunctionArgumentsDelta { .. }
                | OpenAiRealtimeEvent::OutputAudioDelta { .. }
                | OpenAiRealtimeEvent::OutputAudioDone { .. }
                | OpenAiRealtimeEvent::OutputAudioTranscriptDelta { .. }
                | OpenAiRealtimeEvent::OutputAudioTranscriptDone { .. },
            ) => {}
            _ => {
                return Err(OpenAiRealtimeError::Protocol(
                    "Realtime server event is invalid for the local session state".into(),
                ));
            }
        }
        Ok(())
    }

    fn closed_error(&self, code: u16, clean: bool, reason: &str) -> OpenAiRealtimeError {
        OpenAiRealtimeError::Closed {
            code,
            clean,
            reason: reason.chars().take(256).collect(),
            disposition: self.reconnect_disposition(),
        }
    }

    fn with_connection_disposition<T>(
        &self,
        result: Result<T, OpenAiRealtimeError>,
    ) -> Result<T, OpenAiRealtimeError> {
        match result {
            Err(OpenAiRealtimeError::Closed {
                code,
                clean,
                reason,
                ..
            }) => Err(self.closed_error(code, clean, &reason)),
            other => other,
        }
    }
}

const fn reconnect_disposition(state: &OpenAiRealtimeState) -> RealtimeReconnectDisposition {
    match state {
        OpenAiRealtimeState::AwaitingSession => RealtimeReconnectDisposition::SafeBeforeSession,
        OpenAiRealtimeState::Ready { .. } | OpenAiRealtimeState::Closed => {
            RealtimeReconnectDisposition::SafeWhenIdle
        }
        OpenAiRealtimeState::Responding { .. } => {
            RealtimeReconnectDisposition::AmbiguousResponseInFlight
        }
    }
}

fn response_event_id(event: &OpenAiRealtimeEvent) -> Option<&str> {
    match event {
        OpenAiRealtimeEvent::OutputTextDelta { response_id, .. }
        | OpenAiRealtimeEvent::FunctionArgumentsDelta { response_id, .. }
        | OpenAiRealtimeEvent::OutputAudioDelta { response_id, .. }
        | OpenAiRealtimeEvent::OutputAudioDone { response_id, .. }
        | OpenAiRealtimeEvent::OutputAudioTranscriptDelta { response_id, .. }
        | OpenAiRealtimeEvent::OutputAudioTranscriptDone { response_id, .. } => Some(response_id),
        _ => None,
    }
}

fn validate_text(name: &str, value: &str) -> Result<(), OpenAiRealtimeError> {
    if value.len() > MAX_TEXT_BYTES {
        return Err(OpenAiRealtimeError::InvalidRequest(format!(
            "{name} exceeds the {MAX_TEXT_BYTES} byte limit"
        )));
    }
    Ok(())
}

fn validate_id(name: &str, value: &str) -> Result<(), OpenAiRealtimeError> {
    if value.trim().is_empty()
        || value.len() > MAX_EVENT_ID_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(OpenAiRealtimeError::InvalidRequest(format!(
            "{name} must be non-empty, control-free, and at most {MAX_EVENT_ID_BYTES} bytes"
        )));
    }
    Ok(())
}

async fn run_operation<T>(
    operation: impl Future<Output = Result<T, RealtimeTransportError>>,
    context: &ModelCallContext,
) -> Result<T, OpenAiRealtimeError> {
    if context.cancellation().is_cancelled() {
        return Err(OpenAiRealtimeError::Cancelled);
    }
    if context
        .remaining()
        .is_some_and(|remaining| remaining.is_zero())
    {
        return Err(OpenAiRealtimeError::DeadlineExceeded);
    }

    let cancellation = context.cancellation().cancelled();
    let operation = select(Box::pin(cancellation), Box::pin(operation));
    let cancellable = async {
        match operation.await {
            Either::Left(((), _)) => Err(OpenAiRealtimeError::Cancelled),
            Either::Right((result, _)) => result.map_err(map_transport_error),
        }
    };
    if let Some(remaining) = context.remaining() {
        match select(Box::pin(Delay::new(remaining)), Box::pin(cancellable)).await {
            Either::Left(((), _)) => Err(OpenAiRealtimeError::DeadlineExceeded),
            Either::Right((result, _)) => result,
        }
    } else {
        cancellable.await
    }
}

fn map_transport_error(error: RealtimeTransportError) -> OpenAiRealtimeError {
    match error {
        RealtimeTransportError::Closed {
            code,
            clean,
            reason,
        } => OpenAiRealtimeError::Closed {
            code,
            clean,
            reason,
            disposition: RealtimeReconnectDisposition::SafeBeforeSession,
        },
        RealtimeTransportError::Connect
        | RealtimeTransportError::Transport
        | RealtimeTransportError::BinaryFrame
        | RealtimeTransportError::FrameTooLarge => OpenAiRealtimeError::Transport,
        #[cfg(target_arch = "wasm32")]
        RealtimeTransportError::ReceiveOverflow => OpenAiRealtimeError::Closed {
            code: 1009,
            clean: false,
            reason: "bounded Realtime receive queue overflowed".into(),
            disposition: RealtimeReconnectDisposition::SafeBeforeSession,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_use_the_ga_event_shape_and_validate_bounds() {
        let update = OpenAiRealtimeSessionUpdate::new()
            .with_instructions("be concise")
            .unwrap()
            .with_modality(OpenAiRealtimeModality::Text)
            .with_max_output_tokens(128)
            .unwrap();
        let value = OpenAiRealtimeCommand::update_session(update).as_value();
        assert_eq!(value["type"], "session.update");
        assert_eq!(value["session"]["type"], "realtime");
        assert_eq!(value["session"]["output_modalities"][0], "text");

        assert!(OpenAiRealtimeCommand::user_text("").is_err());
        assert!(
            OpenAiRealtimeSessionUpdate::new()
                .with_max_output_tokens(0)
                .is_err()
        );
    }

    #[test]
    fn unknown_server_events_are_lossless() {
        let payload = json!({"type": "future.event", "opaque": {"answer": 42}});
        let event = parse_event(payload.clone()).unwrap();
        assert_eq!(
            event,
            OpenAiRealtimeEvent::Unknown {
                event_type: "future.event".into(),
                payload,
            }
        );
    }
}
