//! Browser WebRTC negotiation and media lifecycle for `OpenAI` Realtime.

use std::{future::Future, pin::Pin};

use futures_util::{
    StreamExt,
    future::{Either, select},
};
use reqwest::{Client, Response, Url};
use runifold_model::ModelCallContext;
use secrecy::{ExposeSecret, SecretString};
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    HtmlAudioElement, MediaStream, MediaStreamConstraints, MediaStreamTrack, RtcConfiguration,
    RtcIceConnectionState, RtcIceGatheringState, RtcIceServer, RtcIceTransportPolicy,
    RtcPeerConnection, RtcPeerConnectionState, RtcSdpType, RtcSessionDescriptionInit,
    RtcTrackEvent,
};

use super::{
    OpenAiRealtimeClientSecret, OpenAiRealtimeSdpOffer,
    realtime::{
        OpenAiRealtimeClient, OpenAiRealtimeCommand, OpenAiRealtimeConnection, OpenAiRealtimeError,
        OpenAiRealtimeEvent, OpenAiRealtimeState, RealtimeReconnectDisposition,
    },
    realtime_data_channel::RealtimeDataChannelTransport,
    realtime_reconnect::{
        OpenAiRealtimeReconnectController, OpenAiRealtimeReconnectError,
        OpenAiRealtimeReconnectEvent,
    },
    realtime_transport::RealtimeTransportError,
};

const DIRECT_CALL_ENDPOINT: &str = "https://api.openai.com/v1/realtime/calls";
const MAX_SDP_BYTES: usize = 256 * 1024;
const MAX_ICE_SERVERS: usize = 8;
const MAX_ICE_URL_BYTES: usize = 2 * 1024;
const MAX_TURN_USERNAME_BYTES: usize = 512;
const MAX_TURN_CREDENTIAL_BYTES: usize = 4 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IceServerKind {
    Stun,
    Turn,
}

/// Validated browser STUN or credential-bearing TURN server.
#[derive(Clone)]
pub struct OpenAiRealtimeIceServer {
    kind: IceServerKind,
    url: String,
    username: Option<String>,
    credential: Option<SecretString>,
}

impl OpenAiRealtimeIceServer {
    /// Creates a STUN server from a `stun:` or `stuns:` URL.
    ///
    /// # Errors
    ///
    /// Rejects malformed, credential-bearing, control-containing, or
    /// oversized URLs.
    pub fn stun(url: impl Into<String>) -> Result<Self, OpenAiRealtimeError> {
        Ok(Self {
            kind: IceServerKind::Stun,
            url: validate_ice_url(url.into(), IceServerKind::Stun)?,
            username: None,
            credential: None,
        })
    }

    /// Creates a TURN server with credentials kept redacted from `Debug`.
    ///
    /// # Errors
    ///
    /// Rejects malformed URLs or blank, control-containing, or oversized
    /// credentials.
    pub fn turn(
        url: impl Into<String>,
        username: impl Into<String>,
        credential: impl Into<String>,
    ) -> Result<Self, OpenAiRealtimeError> {
        let username =
            validate_ice_value("TURN username", username.into(), MAX_TURN_USERNAME_BYTES)?;
        let credential = validate_ice_value(
            "TURN credential",
            credential.into(),
            MAX_TURN_CREDENTIAL_BYTES,
        )?;
        Ok(Self {
            kind: IceServerKind::Turn,
            url: validate_ice_url(url.into(), IceServerKind::Turn)?,
            username: Some(username),
            credential: Some(SecretString::from(credential)),
        })
    }

    /// Returns the validated ICE server URL.
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl std::fmt::Debug for OpenAiRealtimeIceServer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAiRealtimeIceServer")
            .field("kind", &self.kind)
            .field("url", &self.url)
            .field("username", &self.username.as_ref().map(|_| "[REDACTED]"))
            .field(
                "credential",
                &self.credential.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// Browser ICE candidate policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiRealtimeIceTransportPolicy {
    /// Allows host, server-reflexive, and relay candidates.
    #[default]
    All,
    /// Requires relay candidates, preventing direct peer connectivity.
    Relay,
}

/// Observable aggregate browser peer state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiRealtimeWebRtcConnectionState {
    /// No connectivity checks have started.
    New,
    /// Connectivity checks are in progress.
    Connecting,
    /// The peer is usable.
    Connected,
    /// Connectivity was interrupted and may recover.
    Disconnected,
    /// Connectivity failed and requires application recovery.
    Failed,
    /// The peer was closed.
    Closed,
    /// A newer browser exposed a state unknown to this SDK version.
    Unknown,
}

/// Observable browser ICE connectivity state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiRealtimeWebRtcIceState {
    /// ICE has not started.
    New,
    /// Candidate pairs are being checked.
    Checking,
    /// A usable candidate pair exists.
    Connected,
    /// All required candidate checks completed.
    Completed,
    /// ICE connectivity failed.
    Failed,
    /// ICE connectivity was interrupted and may recover.
    Disconnected,
    /// ICE was closed.
    Closed,
    /// A newer browser exposed a state unknown to this SDK version.
    Unknown,
}

/// Browser media behavior used while preparing a Realtime WebRTC peer.
#[derive(Clone, Debug)]
pub struct OpenAiRealtimeWebRtcOptions {
    microphone: bool,
    playback: bool,
    ice_servers: Vec<OpenAiRealtimeIceServer>,
    ice_transport_policy: OpenAiRealtimeIceTransportPolicy,
}

impl Default for OpenAiRealtimeWebRtcOptions {
    fn default() -> Self {
        Self {
            microphone: true,
            playback: true,
            ice_servers: Vec::new(),
            ice_transport_policy: OpenAiRealtimeIceTransportPolicy::All,
        }
    }
}

impl OpenAiRealtimeWebRtcOptions {
    /// Creates the voice-oriented default with microphone and playback enabled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables or disables browser microphone capture.
    #[must_use]
    pub fn with_microphone(mut self, enabled: bool) -> Self {
        self.microphone = enabled;
        self
    }

    /// Enables or disables automatic remote-audio attachment.
    #[must_use]
    pub fn with_playback(mut self, enabled: bool) -> Self {
        self.playback = enabled;
        self
    }

    /// Adds one validated STUN or TURN server.
    ///
    /// # Errors
    ///
    /// Rejects more than eight configured ICE servers.
    pub fn with_ice_server(
        mut self,
        server: OpenAiRealtimeIceServer,
    ) -> Result<Self, OpenAiRealtimeError> {
        if self.ice_servers.len() >= MAX_ICE_SERVERS {
            return Err(OpenAiRealtimeError::InvalidRequest(
                "Realtime WebRTC accepts at most 8 ICE servers".into(),
            ));
        }
        self.ice_servers.push(server);
        Ok(self)
    }

    /// Restricts or expands the candidate types used by the browser.
    #[must_use]
    pub fn with_ice_transport_policy(mut self, policy: OpenAiRealtimeIceTransportPolicy) -> Self {
        self.ice_transport_policy = policy;
        self
    }
}

/// Prepared browser peer containing an SDP offer but no remote answer yet.
pub struct OpenAiRealtimePendingWebRtc {
    peer: Option<RtcPeerConnection>,
    transport: Option<RealtimeDataChannelTransport>,
    offer: OpenAiRealtimeSdpOffer,
    microphone: Option<MediaStream>,
    audio: Option<HtmlAudioElement>,
    _on_track: Option<Closure<dyn FnMut(RtcTrackEvent)>>,
}

impl std::fmt::Debug for OpenAiRealtimePendingWebRtc {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAiRealtimePendingWebRtc")
            .field("offer", &self.offer)
            .field("microphone", &self.microphone.is_some())
            .field("playback", &self.audio.is_some())
            .finish_non_exhaustive()
    }
}

impl OpenAiRealtimeClient {
    /// Creates a browser peer, optional microphone/playback media, the
    /// `oai-events` data channel, and a local SDP offer.
    ///
    /// # Errors
    ///
    /// Returns typed cancellation, deadline, permission, or browser WebRTC
    /// failures. This method is only available on `wasm32`.
    pub async fn prepare_webrtc(
        &self,
        options: OpenAiRealtimeWebRtcOptions,
        context: ModelCallContext,
    ) -> Result<OpenAiRealtimePendingWebRtc, OpenAiRealtimeError> {
        let peer = create_peer(&options)?;
        let (microphone, audio, on_track) = prepare_media(&peer, options, &context).await?;
        let channel = peer.create_data_channel("oai-events");
        let transport = RealtimeDataChannelTransport::attach(channel);
        let offer_value = run_js(peer.create_offer(), "create offer", &context).await?;
        let offer = offer_value.unchecked_into::<RtcSessionDescriptionInit>();
        run_js(
            peer.set_local_description(&offer),
            "set local description",
            &context,
        )
        .await?;
        wait_for_ice_gathering(&peer, &context).await?;
        let local = peer.local_description().ok_or_else(|| {
            OpenAiRealtimeError::Protocol("browser WebRTC offer omitted local description".into())
        })?;
        let offer = OpenAiRealtimeSdpOffer::new(local.sdp())
            .map_err(|error| OpenAiRealtimeError::Protocol(error.to_string()))?;
        Ok(OpenAiRealtimePendingWebRtc {
            peer: Some(peer),
            transport: Some(transport),
            offer,
            microphone,
            audio,
            _on_track: on_track,
        })
    }

    /// Rebuilds a browser peer through a credential-free application Gateway.
    ///
    /// Each controller attempt creates a new peer, offer, data channel, and
    /// Gateway request. The Gateway must acquire a fresh ephemeral Provider
    /// credential for every request and must not cache SDP answers.
    ///
    /// # Errors
    ///
    /// Returns the reconnect controller's fail-closed ambiguity, exhaustion,
    /// permanent-failure, cancellation, or deadline result.
    pub async fn reconnect_webrtc_with_gateway<Observe>(
        &self,
        controller: &mut OpenAiRealtimeReconnectController,
        disposition: RealtimeReconnectDisposition,
        options: OpenAiRealtimeWebRtcOptions,
        endpoint: impl AsRef<str>,
        context: ModelCallContext,
        observe: Observe,
    ) -> Result<OpenAiRealtimeWebRtcSession, OpenAiRealtimeReconnectError>
    where
        Observe: FnMut(OpenAiRealtimeReconnectEvent),
    {
        let endpoint = endpoint.as_ref().to_owned();
        controller
            .reconnect(
                disposition,
                context,
                |_, attempt_context| {
                    let options = options.clone();
                    let endpoint = endpoint.clone();
                    async move {
                        let pending = self
                            .prepare_webrtc(options, attempt_context.clone())
                            .await?;
                        pending
                            .connect_with_gateway(endpoint, attempt_context)
                            .await
                    }
                },
                observe,
            )
            .await
    }
}

impl OpenAiRealtimePendingWebRtc {
    /// Borrows the local offer to send through an application Gateway.
    pub const fn offer(&self) -> &OpenAiRealtimeSdpOffer {
        &self.offer
    }

    /// Returns whether this peer owns a captured microphone stream.
    pub const fn has_microphone(&self) -> bool {
        self.microphone.is_some()
    }

    /// Returns the autoplay element prepared for remote model audio.
    pub const fn audio_element(&self) -> Option<&HtmlAudioElement> {
        self.audio.as_ref()
    }

    /// Abandons negotiation and releases browser media immediately.
    pub fn abort(mut self) {
        self.release_pending_resources();
    }

    /// Exchanges the offer directly with OpenAI using a short-lived secret.
    ///
    /// The long-lived API key must never be passed to this browser method.
    pub async fn connect_with_ephemeral_secret(
        self,
        secret: &OpenAiRealtimeClientSecret,
        context: ModelCallContext,
    ) -> Result<OpenAiRealtimeWebRtcSession, OpenAiRealtimeError> {
        let endpoint = Url::parse(DIRECT_CALL_ENDPOINT).map_err(|_| {
            OpenAiRealtimeError::InvalidRequest("invalid direct Realtime call endpoint".into())
        })?;
        let answer = match exchange_sdp(
            endpoint,
            self.offer.as_str(),
            Some(secret.secret().expose_secret()),
            &context,
        )
        .await
        {
            Ok(answer) => answer,
            Err(error) => {
                self.abort();
                return Err(error);
            }
        };
        self.complete(answer, context).await
    }

    /// Exchanges the offer through a credential-free application Gateway.
    pub async fn connect_with_gateway(
        self,
        endpoint: impl AsRef<str>,
        context: ModelCallContext,
    ) -> Result<OpenAiRealtimeWebRtcSession, OpenAiRealtimeError> {
        let endpoint = Url::parse(endpoint.as_ref()).map_err(|_| {
            OpenAiRealtimeError::InvalidRequest("invalid Realtime Gateway endpoint".into())
        })?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err(OpenAiRealtimeError::InvalidRequest(
                "Realtime Gateway endpoint must use HTTP or HTTPS".into(),
            ));
        }
        let answer = match exchange_sdp(endpoint, self.offer.as_str(), None, &context).await {
            Ok(answer) => answer,
            Err(error) => {
                self.abort();
                return Err(error);
            }
        };
        self.complete(answer, context).await
    }

    /// Applies an SDP answer obtained by application-controlled negotiation.
    pub async fn complete(
        mut self,
        answer: impl AsRef<str>,
        context: ModelCallContext,
    ) -> Result<OpenAiRealtimeWebRtcSession, OpenAiRealtimeError> {
        if let Err(error) = validate_sdp_answer(answer.as_ref()) {
            self.abort();
            return Err(error);
        }
        let description = RtcSessionDescriptionInit::new(RtcSdpType::Answer);
        description.set_sdp(answer.as_ref());
        let peer = self
            .peer
            .as_ref()
            .expect("pending WebRTC peer exists until successful completion");
        if let Err(error) = run_js(
            peer.set_remote_description(&description),
            "set remote description",
            &context,
        )
        .await
        {
            self.abort();
            return Err(error);
        }
        let transport = self
            .transport
            .as_mut()
            .expect("pending data channel exists until successful completion");
        if let Err(error) = run_transport_open(transport.wait_open(), &context).await {
            self.abort();
            return Err(error);
        }
        Ok(OpenAiRealtimeWebRtcSession {
            peer: self
                .peer
                .take()
                .expect("validated pending WebRTC peer is transferred once"),
            connection: OpenAiRealtimeConnection::from_data_channel(
                self.transport
                    .take()
                    .expect("validated pending data channel is transferred once"),
            ),
            microphone: self.microphone.take(),
            audio: self.audio.take(),
            _on_track: self._on_track.take(),
        })
    }

    fn release_pending_resources(&mut self) {
        stop_media(self.microphone.as_ref());
        self.microphone = None;
        if let Some(peer) = self.peer.take() {
            peer.set_ontrack(None);
            peer.close();
        }
        self.transport = None;
        self.audio = None;
        self._on_track = None;
    }
}

impl Drop for OpenAiRealtimePendingWebRtc {
    fn drop(&mut self) {
        self.release_pending_resources();
    }
}

/// Connected WebRTC media peer backed by the canonical typed event state.
pub struct OpenAiRealtimeWebRtcSession {
    peer: RtcPeerConnection,
    connection: OpenAiRealtimeConnection,
    microphone: Option<MediaStream>,
    audio: Option<HtmlAudioElement>,
    _on_track: Option<Closure<dyn FnMut(RtcTrackEvent)>>,
}

impl std::fmt::Debug for OpenAiRealtimeWebRtcSession {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAiRealtimeWebRtcSession")
            .field("state", self.connection.state())
            .field("microphone", &self.microphone.is_some())
            .field("playback", &self.audio.is_some())
            .finish_non_exhaustive()
    }
}

impl OpenAiRealtimeWebRtcSession {
    /// Returns the canonical typed Realtime lifecycle.
    pub const fn state(&self) -> &OpenAiRealtimeState {
        self.connection.state()
    }

    /// Returns the browser peer's aggregate connectivity state.
    pub fn connection_state(&self) -> OpenAiRealtimeWebRtcConnectionState {
        match self.peer.connection_state() {
            RtcPeerConnectionState::New => OpenAiRealtimeWebRtcConnectionState::New,
            RtcPeerConnectionState::Connecting => OpenAiRealtimeWebRtcConnectionState::Connecting,
            RtcPeerConnectionState::Connected => OpenAiRealtimeWebRtcConnectionState::Connected,
            RtcPeerConnectionState::Disconnected => {
                OpenAiRealtimeWebRtcConnectionState::Disconnected
            }
            RtcPeerConnectionState::Failed => OpenAiRealtimeWebRtcConnectionState::Failed,
            RtcPeerConnectionState::Closed => OpenAiRealtimeWebRtcConnectionState::Closed,
            _ => OpenAiRealtimeWebRtcConnectionState::Unknown,
        }
    }

    /// Returns the browser peer's detailed ICE connectivity state.
    pub fn ice_connection_state(&self) -> OpenAiRealtimeWebRtcIceState {
        match self.peer.ice_connection_state() {
            RtcIceConnectionState::New => OpenAiRealtimeWebRtcIceState::New,
            RtcIceConnectionState::Checking => OpenAiRealtimeWebRtcIceState::Checking,
            RtcIceConnectionState::Connected => OpenAiRealtimeWebRtcIceState::Connected,
            RtcIceConnectionState::Completed => OpenAiRealtimeWebRtcIceState::Completed,
            RtcIceConnectionState::Failed => OpenAiRealtimeWebRtcIceState::Failed,
            RtcIceConnectionState::Disconnected => OpenAiRealtimeWebRtcIceState::Disconnected,
            RtcIceConnectionState::Closed => OpenAiRealtimeWebRtcIceState::Closed,
            _ => OpenAiRealtimeWebRtcIceState::Unknown,
        }
    }

    /// Classifies whether a new session can safely replace this connection.
    pub const fn reconnect_disposition(&self) -> super::RealtimeReconnectDisposition {
        self.connection.reconnect_disposition()
    }

    /// Sends one typed command over `oai-events`.
    pub async fn send(
        &mut self,
        command: &OpenAiRealtimeCommand,
        context: ModelCallContext,
    ) -> Result<(), OpenAiRealtimeError> {
        self.connection.send(command, context).await
    }

    /// Receives one bounded typed server event from `oai-events`.
    pub async fn next_event(
        &mut self,
        context: ModelCallContext,
    ) -> Result<OpenAiRealtimeEvent, OpenAiRealtimeError> {
        self.connection.next_event(context).await
    }

    /// Mutes or unmutes every captured microphone audio track.
    pub fn set_microphone_muted(&self, muted: bool) {
        if let Some(stream) = &self.microphone {
            for value in stream.get_audio_tracks().iter() {
                if let Ok(track) = value.dyn_into::<MediaStreamTrack>() {
                    track.set_enabled(!muted);
                }
            }
        }
    }

    /// Returns the autoplay element receiving the model's remote media.
    pub const fn audio_element(&self) -> Option<&HtmlAudioElement> {
        self.audio.as_ref()
    }

    /// Stops local media, closes the data channel, and closes the peer.
    pub async fn close(&mut self, context: ModelCallContext) -> Result<(), OpenAiRealtimeError> {
        let result = self.connection.close(context).await;
        stop_media(self.microphone.as_ref());
        self.peer.close();
        result
    }
}

fn create_peer(
    options: &OpenAiRealtimeWebRtcOptions,
) -> Result<RtcPeerConnection, OpenAiRealtimeError> {
    if options.ice_transport_policy == OpenAiRealtimeIceTransportPolicy::Relay
        && !options
            .ice_servers
            .iter()
            .any(|server| server.kind == IceServerKind::Turn)
    {
        return Err(OpenAiRealtimeError::InvalidRequest(
            "relay-only WebRTC requires at least one TURN server".into(),
        ));
    }
    let configuration = RtcConfiguration::new();
    let servers = js_sys::Array::new();
    for configured in &options.ice_servers {
        let server = RtcIceServer::new();
        server.set_urls_str(&configured.url);
        if let Some(username) = &configured.username {
            server.set_username(username);
        }
        if let Some(credential) = &configured.credential {
            server.set_credential(credential.expose_secret());
        }
        servers.push(&server);
    }
    configuration.set_ice_servers(&servers);
    configuration.set_ice_transport_policy(match options.ice_transport_policy {
        OpenAiRealtimeIceTransportPolicy::All => RtcIceTransportPolicy::All,
        OpenAiRealtimeIceTransportPolicy::Relay => RtcIceTransportPolicy::Relay,
    });
    RtcPeerConnection::new_with_configuration(&configuration)
        .map_err(|error| js_transport_at("create peer", error))
}

fn validate_ice_url(value: String, expected: IceServerKind) -> Result<String, OpenAiRealtimeError> {
    if value.trim() != value
        || value.is_empty()
        || value.len() > MAX_ICE_URL_BYTES
        || value.chars().any(char::is_control)
        || value.contains('@')
    {
        return Err(OpenAiRealtimeError::InvalidRequest(
            "ICE URL must be bounded, control-free, and must not embed credentials".into(),
        ));
    }
    let parsed = Url::parse(&value)
        .map_err(|_| OpenAiRealtimeError::InvalidRequest("ICE URL is malformed".into()))?;
    let scheme_matches = match expected {
        IceServerKind::Stun => matches!(parsed.scheme(), "stun" | "stuns"),
        IceServerKind::Turn => matches!(parsed.scheme(), "turn" | "turns"),
    };
    if !scheme_matches || parsed.path().trim_matches('/').is_empty() {
        return Err(OpenAiRealtimeError::InvalidRequest(
            "ICE URL scheme does not match its STUN or TURN server type".into(),
        ));
    }
    Ok(value)
}

fn validate_ice_value(
    name: &str,
    value: String,
    max_bytes: usize,
) -> Result<String, OpenAiRealtimeError> {
    if value.trim().is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(OpenAiRealtimeError::InvalidRequest(format!(
            "{name} must be non-empty, control-free, and at most {max_bytes} bytes"
        )));
    }
    Ok(value)
}

fn stop_media(stream: Option<&MediaStream>) {
    if let Some(stream) = stream {
        for value in stream.get_tracks().iter() {
            if let Ok(track) = value.dyn_into::<MediaStreamTrack>() {
                track.stop();
            }
        }
    }
}

type PreparedMedia = (
    Option<MediaStream>,
    Option<HtmlAudioElement>,
    Option<Closure<dyn FnMut(RtcTrackEvent)>>,
);

async fn prepare_media(
    peer: &RtcPeerConnection,
    options: OpenAiRealtimeWebRtcOptions,
    context: &ModelCallContext,
) -> Result<PreparedMedia, OpenAiRealtimeError> {
    let microphone = if options.microphone {
        let window = web_sys::window().ok_or(OpenAiRealtimeError::Transport)?;
        let devices = window
            .navigator()
            .media_devices()
            .map_err(|error| js_transport_at("access media devices", error))?;
        let constraints = MediaStreamConstraints::new();
        constraints.set_audio(&JsValue::TRUE);
        let stream = run_js(
            devices
                .get_user_media_with_constraints(&constraints)
                .map_err(|error| js_transport_at("request microphone", error))?,
            "request microphone",
            context,
        )
        .await?
        .dyn_into::<MediaStream>()
        .map_err(|error| js_transport_at("decode microphone stream", error))?;
        let track = stream
            .get_audio_tracks()
            .get(0)
            .dyn_into::<MediaStreamTrack>()
            .map_err(|error| js_transport_at("decode microphone track", error))?;
        peer.add_track_0(&track, &stream);
        Some(stream)
    } else {
        None
    };

    let (audio, on_track) = if options.playback {
        let audio = HtmlAudioElement::new()
            .map_err(|error| js_transport_at("create playback element", error))?;
        audio.set_autoplay(true);
        let target = audio.clone();
        let on_track = Closure::wrap(Box::new(move |event: RtcTrackEvent| {
            if let Ok(stream) = event.streams().get(0).dyn_into::<MediaStream>() {
                target.set_src_object(Some(&stream));
            }
        }) as Box<dyn FnMut(RtcTrackEvent)>);
        peer.set_ontrack(Some(on_track.as_ref().unchecked_ref()));
        (Some(audio), Some(on_track))
    } else {
        (None, None)
    };
    Ok((microphone, audio, on_track))
}

async fn exchange_sdp(
    endpoint: Url,
    offer: &str,
    bearer: Option<&str>,
    context: &ModelCallContext,
) -> Result<String, OpenAiRealtimeError> {
    let mut request = Client::new()
        .post(endpoint)
        .header("content-type", "application/sdp")
        .body(offer.to_owned());
    if let Some(bearer) = bearer {
        request = request.bearer_auth(bearer);
    }
    if let Some(remaining) = context.remaining() {
        request = request.timeout(remaining);
    }
    let response = run_http(request.send(), context)
        .await?
        .map_err(|_| OpenAiRealtimeError::Transport)?;
    read_sdp_response(response, context).await
}

async fn read_sdp_response(
    response: Response,
    context: &ModelCallContext,
) -> Result<String, OpenAiRealtimeError> {
    if !response.status().is_success() {
        let status = response.status().as_u16();
        return Err(OpenAiRealtimeError::SdpExchange {
            status,
            retryable: status == 408 || status == 429 || status >= 500,
        });
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_SDP_BYTES as u64)
    {
        return Err(OpenAiRealtimeError::Protocol(
            "Realtime SDP answer exceeds the bounded response limit".into(),
        ));
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = run_http(stream.next(), context).await? {
        let chunk = chunk.map_err(|_| OpenAiRealtimeError::Transport)?;
        if bytes.len().saturating_add(chunk.len()) > MAX_SDP_BYTES {
            return Err(OpenAiRealtimeError::Protocol(
                "Realtime SDP answer exceeds the bounded response limit".into(),
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes)
        .map_err(|_| OpenAiRealtimeError::Protocol("Realtime SDP answer is not UTF-8".into()))
}

fn validate_sdp_answer(answer: &str) -> Result<(), OpenAiRealtimeError> {
    if answer.len() > MAX_SDP_BYTES
        || !answer.starts_with("v=0")
        || !answer.lines().any(|line| line.starts_with("m="))
    {
        return Err(OpenAiRealtimeError::Protocol(
            "Realtime SDP answer is malformed or oversized".into(),
        ));
    }
    Ok(())
}

async fn wait_for_ice_gathering(
    peer: &RtcPeerConnection,
    context: &ModelCallContext,
) -> Result<(), OpenAiRealtimeError> {
    for _ in 0..500 {
        if peer.ice_gathering_state() == RtcIceGatheringState::Complete {
            return Ok(());
        }
        run_http(
            futures_timer::Delay::new(std::time::Duration::from_millis(10)),
            context,
        )
        .await?;
    }
    Err(OpenAiRealtimeError::BrowserWebRtc(
        "ICE gathering did not complete within 5 seconds".into(),
    ))
}

async fn run_js(
    promise: js_sys::Promise,
    stage: &'static str,
    context: &ModelCallContext,
) -> Result<JsValue, OpenAiRealtimeError> {
    run_context(Box::pin(JsFuture::from(promise)), context)
        .await?
        .map_err(|error| js_transport_at(stage, error))
}

async fn run_http<T>(
    future: impl Future<Output = T>,
    context: &ModelCallContext,
) -> Result<T, OpenAiRealtimeError> {
    run_context(Box::pin(async move { future.await }), context).await
}

async fn run_transport_open(
    future: impl Future<Output = Result<(), RealtimeTransportError>>,
    context: &ModelCallContext,
) -> Result<(), OpenAiRealtimeError> {
    run_context(Box::pin(future), context)
        .await?
        .map_err(|_| OpenAiRealtimeError::Transport)
}

async fn run_context<T>(
    future: Pin<Box<dyn Future<Output = T> + '_>>,
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
    let operation = select(Box::pin(context.cancellation().cancelled()), future);
    let cancellable = async {
        match operation.await {
            Either::Left(((), _)) => Err(OpenAiRealtimeError::Cancelled),
            Either::Right((value, _)) => Ok(value),
        }
    };
    if let Some(remaining) = context.remaining() {
        match select(
            Box::pin(futures_timer::Delay::new(remaining)),
            Box::pin(cancellable),
        )
        .await
        {
            Either::Left(((), _)) => Err(OpenAiRealtimeError::DeadlineExceeded),
            Either::Right((result, _)) => result,
        }
    } else {
        cancellable.await
    }
}

fn js_transport_at(stage: &str, error: JsValue) -> OpenAiRealtimeError {
    let message = error
        .as_string()
        .or_else(|| {
            js_sys::Reflect::get(&error, &JsValue::from_str("message"))
                .ok()
                .and_then(|value| value.as_string())
        })
        .or_else(|| {
            error
                .dyn_ref::<js_sys::Error>()
                .map(js_sys::Error::message)
                .map(String::from)
        })
        .unwrap_or_else(|| "browser rejected the WebRTC operation".into());
    OpenAiRealtimeError::BrowserWebRtc(format!("{stage}: {message}").chars().take(256).collect())
}
