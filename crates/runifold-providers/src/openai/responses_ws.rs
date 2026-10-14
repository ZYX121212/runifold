//! Native Responses WebSocket transport, separate from Realtime sessions.

use std::{collections::BTreeMap, future::Future, sync::Arc};

use futures_util::{
    future::{Either, select},
    lock::Mutex,
};
use runifold_core::RetrySafety;
use runifold_model::{
    Model, ModelCallContext, ModelCapabilities, ModelError, ModelErrorKind, ModelEventStream,
    ModelFuture, ModelRef, ModelRequest, ModelStreamEvent, ProviderModel, ProviderRuntimeProfile,
};
use serde_json::{Value, json};

use super::{
    OpenAiClient, OpenAiConfig, OpenAiEventDecoder, OpenAiWireProtocol,
    encode::{apply_tool_dispatch_aliases, encode_request_for, tool_dispatch_aliases},
    realtime_session_transport::connect_headers,
    realtime_transport::{RealtimeConnectOptions, RealtimeTransport, RealtimeTransportError},
};

const MAX_PENDING: usize = 64;

/// One Responses connection event; canonical events belong only to its lane.
///
/// Raw payloads may contain private content and are omitted from Debug output.
pub struct OpenAiResponsesEvent {
    /// Connection-local lane, independent of response lineage.
    pub stream_id: Option<String>,
    /// Original provider event, including steering acknowledgements and errors.
    pub payload: Value,
    /// Canonical model events for supported response content.
    pub canonical: Vec<ModelStreamEvent>,
}

impl std::fmt::Debug for OpenAiResponsesEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAiResponsesEvent")
            .field("stream_id", &self.stream_id)
            .field("type", &self.payload.get("type"))
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct Lane {
    pending: usize,
    decoder: Option<OpenAiEventDecoder>,
    response_id: Option<String>,
    sequence: Option<u64>,
}

impl Lane {
    fn observe_sequence(
        &mut self,
        kind: &str,
        payload: &Value,
        dialect: super::OpenAiResponsesDialect,
    ) -> Result<(), ModelError> {
        if kind == "response.created" {
            self.sequence = None;
        }
        match payload.get("sequence_number") {
            Some(value) => {
                let sequence = value.as_u64().ok_or_else(|| {
                    error(
                        ModelErrorKind::Protocol,
                        "Responses sequence must be unsigned",
                    )
                })?;
                if self.sequence.is_some_and(|previous| sequence <= previous) {
                    return Err(error(
                        ModelErrorKind::Protocol,
                        "Responses lane sequence did not advance",
                    ));
                }
                self.sequence = Some(sequence);
            }
            None if dialect == super::OpenAiResponsesDialect::OpenAi => {
                return Err(error(
                    ModelErrorKind::Protocol,
                    "Responses event has no sequence number",
                ));
            }
            None => {}
        }
        Ok(())
    }
}

/// An explicitly owned Responses socket with bounded lane and request state.
///
/// Dropping a connection does not prove remote work stopped. Reconnect only
/// after reconciling response identities and effects; no command is auto-replayed.
pub struct OpenAiResponsesConnection {
    config: OpenAiConfig,
    transport: RealtimeTransport,
    lanes: BTreeMap<String, Lane>,
    poisoned: bool,
}

impl std::fmt::Debug for OpenAiResponsesConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAiResponsesConnection")
            .field("lanes", &self.lanes.len())
            .field("poisoned", &self.poisoned)
            .finish_non_exhaustive()
    }
}

impl OpenAiResponsesConnection {
    pub(super) async fn connect(
        config: OpenAiConfig,
        context: &ModelCallContext,
    ) -> Result<Self, ModelError> {
        if config.wire_protocol != OpenAiWireProtocol::Responses {
            return Err(error(
                ModelErrorKind::InvalidRequest,
                "Responses WebSocket requires the Responses protocol",
            ));
        }
        let mut url = config.endpoint_url();
        let scheme = match url.scheme() {
            "https" => "wss",
            "http" => "ws",
            _ => {
                return Err(error(
                    ModelErrorKind::InvalidRequest,
                    "Responses WebSocket endpoint must use HTTP(S)",
                ));
            }
        };
        url.set_scheme(scheme)
            .map_err(|()| error(ModelErrorKind::InvalidRequest, "invalid WebSocket URL"))?;
        let mut headers = connect_headers(&config);
        headers.insert(
            "x-client-request-id".into(),
            context.invocation_id().to_string(),
        );
        if let Some(api_key) = &config.azure_api_key {
            use secrecy::ExposeSecret;
            headers.insert("api-key".into(), api_key.expose_secret().to_owned());
        }
        let options = RealtimeConnectOptions {
            url: url.to_string(),
            headers,
        };
        let transport = Box::pin(operation(RealtimeTransport::connect(options), context)).await?;
        Ok(Self {
            config,
            transport,
            lanes: BTreeMap::new(),
            poisoned: false,
        })
    }

    /// Submits one response on a default or named lane using the existing encoder.
    ///
    /// `previous_response_id` selects lineage, not execution ownership. Same-lane
    /// commands are FIFO; one reader must consume all lanes. No send is retried.
    ///
    /// # Errors
    /// Returns validation, protocol, cancellation, deadline or transport failures.
    pub async fn create_response(
        &mut self,
        request: ModelRequest,
        stream_id: Option<&str>,
        previous_response_id: Option<&str>,
        context: ModelCallContext,
    ) -> Result<(), ModelError> {
        self.ensure_usable()?;
        let lane = lane_key(stream_id)?;
        if request.model.provider != self.config.provider {
            return Err(error(
                ModelErrorKind::InvalidRequest,
                "request belongs to a different provider",
            ));
        }
        if !self.lanes.contains_key(&lane)
            && !lane.is_empty()
            && self.lanes.keys().filter(|key| !key.is_empty()).count() >= 32
        {
            return Err(error(
                ModelErrorKind::InvalidRequest,
                "Responses connection has reached 32 named lanes; reuse a lane or open another connection",
            ));
        }
        if self.lanes.values().map(|lane| lane.pending).sum::<usize>() >= MAX_PENDING {
            return Err(error(
                ModelErrorKind::InvalidRequest,
                "consume queued response events before submitting more work",
            ));
        }
        let mut body = encode_request_for(&request, &self.config.provider)?;
        if body.get("background").and_then(Value::as_bool) == Some(true) {
            return Err(error(
                ModelErrorKind::UnsupportedFeature,
                "use HTTP background response lifecycle instead of WebSocket background=true",
            ));
        }
        let object = body.as_object_mut().ok_or_else(|| {
            error(
                ModelErrorKind::Protocol,
                "encoded response must be an object",
            )
        })?;
        object.remove("stream");
        object.insert("type".into(), json!("response.create"));
        if let Some(stream_id) = stream_id {
            object.insert("stream_id".into(), json!(stream_id));
        }
        if let Some(previous) = previous_response_id {
            if previous.is_empty() || previous.len() > 255 {
                return Err(error(
                    ModelErrorKind::InvalidRequest,
                    "previous response identity is invalid",
                ));
            }
            object.insert("previous_response_id".into(), json!(previous));
        }
        self.lanes.entry(lane).or_default().pending += 1;
        if let Err(error) = operation(self.transport.send_text(&body.to_string()), &context).await {
            self.poisoned = true;
            return Err(error);
        }
        Ok(())
    }

    /// Queues additional user instructions against a response on this connection.
    ///
    /// An accepted event only acknowledges queuing. Continue reading through
    /// any pending tool input and automatic continuation. Pending steering is
    /// not durable and must not be blindly repeated after disconnection.
    ///
    /// # Errors
    /// Returns validation, protocol, cancellation, deadline or transport failures.
    pub async fn steer(
        &mut self,
        response_id: &str,
        input: &str,
        context: ModelCallContext,
    ) -> Result<(), ModelError> {
        self.ensure_usable()?;
        if input.trim().is_empty()
            || input.len() > 256 * 1024
            || !self
                .lanes
                .values()
                .any(|lane| lane.response_id.as_deref() == Some(response_id))
        {
            return Err(error(
                ModelErrorKind::InvalidRequest,
                "steering requires bounded nonempty input and a response from this connection",
            ));
        }
        let command =
            json!({"type":"response.steer","previous_response_id":response_id,"input":input});
        if let Err(error) =
            operation(self.transport.send_text(&command.to_string()), &context).await
        {
            self.poisoned = true;
            return Err(error);
        }
        Ok(())
    }

    /// Reads one routed event, preserving unknown and steering events verbatim.
    ///
    /// Errors after dispatch have unknown retry safety. Request-scoped provider
    /// errors remain visible as raw events so the caller can reconcile that lane.
    ///
    /// # Errors
    /// Returns validation, protocol, cancellation, deadline or transport failures.
    pub async fn next_event(
        &mut self,
        context: ModelCallContext,
    ) -> Result<OpenAiResponsesEvent, ModelError> {
        self.ensure_usable()?;
        let result = self.read_event(&context).await.map_err(|mut failure| {
            // Decoder errors are local classifications of already-dispatched
            // work, not evidence that the provider did not accept the request.
            failure.retry_safety = RetrySafety::Unknown;
            failure
        });
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    async fn receive_payload(&mut self, context: &ModelCallContext) -> Result<Value, ModelError> {
        let text = operation(self.transport.next_text(), context)
            .await?
            .ok_or_else(|| {
                error(
                    ModelErrorKind::Transport,
                    "Responses socket closed; reconcile accepted work before reconnecting",
                )
            })?;
        serde_json::from_str(&text).map_err(|_| {
            error(
                ModelErrorKind::Protocol,
                "Responses WebSocket event is not JSON",
            )
        })
    }

    async fn read_event(
        &mut self,
        context: &ModelCallContext,
    ) -> Result<OpenAiResponsesEvent, ModelError> {
        let payload = self.receive_payload(context).await?;
        let kind = payload.get("type").and_then(Value::as_str).ok_or_else(|| {
            error(
                ModelErrorKind::Protocol,
                "Responses WebSocket event has no type",
            )
        })?;
        let stream_id = payload
            .get("stream_id")
            .map(|value| {
                value.as_str().map(str::to_owned).ok_or_else(|| {
                    error(
                        ModelErrorKind::Protocol,
                        "Responses event lane must be a string",
                    )
                })
            })
            .transpose()?;
        let lane_key = lane_key(stream_id.as_deref())?;
        if kind == "error" {
            self.observe_request_error(&lane_key, &payload)?;
            return Ok(OpenAiResponsesEvent {
                stream_id,
                payload,
                canonical: Vec::new(),
            });
        }
        let lane = self.lanes.get_mut(&lane_key).ok_or_else(|| {
            error(
                ModelErrorKind::Protocol,
                "Responses event targeted an unregistered lane",
            )
        })?;
        lane.observe_sequence(kind, &payload, self.config.responses_dialect)?;
        if kind.starts_with("response.steer.") {
            return Ok(OpenAiResponsesEvent {
                stream_id,
                payload,
                canonical: Vec::new(),
            });
        }
        if kind == "response.created" {
            if lane.decoder.is_some() {
                return Err(error(
                    ModelErrorKind::Protocol,
                    "Responses lane started overlapping responses",
                ));
            }
            lane.pending = lane.pending.saturating_sub(1);
            lane.response_id = payload
                .get("response")
                .and_then(|response| response.get("id"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            lane.decoder = Some(
                OpenAiEventDecoder::for_provider(&self.config.provider)
                    .with_dialect(self.config.responses_dialect),
            );
        }
        if kind == "response.failed" {
            if lane.decoder.is_none()
                || payload.pointer("/response/id").and_then(Value::as_str)
                    != lane.response_id.as_deref()
            {
                return Err(error(
                    ModelErrorKind::Protocol,
                    "Responses failure does not match the active response",
                ));
            }
            lane.decoder = None;
            lane.response_id = None;
            return Ok(OpenAiResponsesEvent {
                stream_id,
                payload,
                canonical: Vec::new(),
            });
        }
        let decoder = lane.decoder.as_mut().ok_or_else(|| {
            error(
                ModelErrorKind::Protocol,
                "Responses event arrived before a response started",
            )
        })?;
        let canonical = decoder.decode(payload.clone())?;
        if matches!(kind, "response.completed" | "response.incomplete") {
            lane.decoder = None;
        }
        Ok(OpenAiResponsesEvent {
            stream_id,
            payload,
            canonical,
        })
    }

    fn observe_request_error(&mut self, key: &str, payload: &Value) -> Result<(), ModelError> {
        if let Some(lane) = self.lanes.get_mut(key) {
            if payload.get("sequence_number").is_some() {
                lane.observe_sequence("error", payload, self.config.responses_dialect)?;
                lane.decoder = None;
                lane.response_id = None;
            } else if lane.pending == 0 {
                lane.decoder = None;
                lane.response_id = None;
            } else {
                lane.pending = lane.pending.saturating_sub(1);
            }
        }
        if payload.pointer("/error/code").and_then(Value::as_str)
            == Some("websocket_connection_limit_reached")
        {
            self.poisoned = true;
        }
        Ok(())
    }

    /// Closes this transport without claiming accepted remote work was cancelled.
    ///
    /// # Errors
    /// Returns validation, protocol, cancellation, deadline or transport failures.
    pub async fn close(mut self, context: ModelCallContext) -> Result<(), ModelError> {
        operation(self.transport.close(), &context).await
    }

    fn ensure_usable(&self) -> Result<(), ModelError> {
        if self.poisoned {
            return Err(error(
                ModelErrorKind::Transport,
                "Responses socket state is ambiguous; reconcile work and open a new connection",
            ));
        }
        Ok(())
    }
}

fn lane_key(stream_id: Option<&str>) -> Result<String, ModelError> {
    match stream_id {
        None => Ok(String::new()),
        Some(value)
            if !value.is_empty()
                && value.len() <= 256
                && value.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')
                }) =>
        {
            Ok(value.into())
        }
        Some(_) => Err(error(
            ModelErrorKind::InvalidRequest,
            "stream_id must be a 1..=256 byte ASCII lane token",
        )),
    }
}

async fn operation<T>(
    future: impl Future<Output = Result<T, RealtimeTransportError>>,
    context: &ModelCallContext,
) -> Result<T, ModelError> {
    run_context(
        async {
            future.await.map_err(|_| {
                error(
                    ModelErrorKind::Transport,
                    "Responses WebSocket transport failed; accepted work may still be running",
                )
            })
        },
        context,
    )
    .await
}

async fn run_context<T>(
    future: impl Future<Output = Result<T, ModelError>>,
    context: &ModelCallContext,
) -> Result<T, ModelError> {
    if context.cancellation().is_cancelled() {
        return Err(error(
            ModelErrorKind::Cancelled,
            "Responses operation cancelled",
        ));
    }
    if context
        .remaining()
        .is_some_and(|remaining| remaining.is_zero())
    {
        return Err(error(
            ModelErrorKind::DeadlineExceeded,
            "Responses operation deadline elapsed",
        ));
    }
    let cancellable = async {
        match select(
            Box::pin(context.cancellation().cancelled()),
            Box::pin(future),
        )
        .await
        {
            Either::Left(_) => Err(error(
                ModelErrorKind::Cancelled,
                "Responses operation cancelled",
            )),
            Either::Right((result, _)) => result,
        }
    };
    if let Some(remaining) = context.remaining() {
        match select(
            Box::pin(futures_timer::Delay::new(remaining)),
            Box::pin(cancellable),
        )
        .await
        {
            Either::Left(_) => Err(error(
                ModelErrorKind::DeadlineExceeded,
                "Responses operation exceeded its deadline",
            )),
            Either::Right((result, _)) => result,
        }
    } else {
        cancellable.await
    }
}

fn error(kind: ModelErrorKind, message: &str) -> ModelError {
    let mut error = ModelError::local(kind, message);
    error.retry_safety = RetrySafety::Unknown;
    error
}

/// A cloneable model adapter that reuses a socket through the canonical Agent loop.
///
/// Calls on this adapter are serialized. Use explicit connections for multiplexing
/// and steering. An interrupted stream drops its socket instead of replaying work.
#[derive(Clone)]
pub struct OpenAiResponsesWebSocketModel {
    client: OpenAiClient,
    connection: Arc<Mutex<Option<OpenAiResponsesConnection>>>,
}

impl OpenAiResponsesWebSocketModel {
    pub(super) fn new(client: OpenAiClient) -> Self {
        Self {
            client,
            connection: Arc::new(Mutex::new(None)),
        }
    }
}

impl Model for OpenAiResponsesWebSocketModel {
    fn capabilities<'a>(
        &'a self,
        model: &'a ModelRef,
    ) -> ModelFuture<'a, Result<ModelCapabilities, ModelError>> {
        self.client.capabilities(model)
    }
    fn stream(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> ModelFuture<'_, Result<ModelEventStream, ModelError>> {
        Box::pin(async move {
            let (_, warnings) = self.client.prepare(&request)?;
            let atomic = request.selected_response_mode() == runifold_model::ResponseMode::Complete;
            let aliases = tool_dispatch_aliases(&request, self.client.provider())?;
            let lock = Arc::clone(&self.connection).lock_owned();
            let mut guard = run_context(async { Ok(lock.await) }, &context).await?;
            let mut connection = match guard.take() {
                Some(connection) => connection,
                None => Box::pin(self.client.connect_responses_websocket(context.clone())).await?,
            };
            connection
                .create_response(request, None, None, context.clone())
                .await?;
            let provider = self.client.provider().to_owned();
            Ok(Box::pin(async_stream::try_stream! {
                let mut connection = Some(connection);
                let mut buffered = Vec::new();
                let mut canonical_check = runifold_model::ModelStreamAccumulator::new();
                let mut buffered_bytes = 0usize;
                loop {
                    let mut event = connection.as_mut().ok_or_else(|| error(ModelErrorKind::Protocol, "Responses connection was already returned"))?.next_event(context.clone()).await?;
                    apply_tool_dispatch_aliases(&mut event.canonical, &aliases);
                    if matches!(event.payload["type"].as_str(), Some("error" | "response.failed")) {
                        let mut failure = super::decode::provider_failure(&provider, &event.payload);
                        failure.retry_safety = RetrySafety::Unknown;
                        Err(failure)?;
                    }
                    let completed = event.canonical.iter().any(|event| matches!(event, ModelStreamEvent::ResponseCompleted { .. }));
                    // Return the socket before yielding completion: invoke() stops
                    // polling immediately when it receives the terminal event.
                    if completed && !atomic { *guard = connection.take(); }
                    for event in event.canonical {
                        let started = matches!(event, ModelStreamEvent::ResponseStarted { .. });
                        if atomic {
                            let _ = canonical_check.push(event.clone())?;
                            buffered_bytes = buffered_bytes.saturating_add(serde_json::to_vec(&event).map_err(|_| error(ModelErrorKind::Protocol, "canonical event serialization failed"))?.len());
                            if buffered_bytes > 32 * 1024 * 1024 { Err(error(ModelErrorKind::Protocol, "atomic WebSocket response exceeds 32 MiB"))?; }
                            buffered.push(event);
                            if started { buffered.extend(warnings.iter().cloned().map(|warning| ModelStreamEvent::Warning { warning })); }
                        } else {
                            yield event;
                            if started { for warning in &warnings { yield ModelStreamEvent::Warning { warning: warning.clone() }; } }
                        }
                    }
                    if completed {
                        // Atomic validation and size checks can still fail on
                        // terminal metadata. Cache only after all of them pass.
                        if atomic { *guard = connection.take(); }
                        for event in buffered { yield event; }
                        break;
                    }
                }
            }) as ModelEventStream)
        })
    }
}

impl ProviderModel for OpenAiResponsesWebSocketModel {
    fn provider(&self) -> &str {
        self.client.provider()
    }
    fn runtime_profile(&self) -> ProviderRuntimeProfile {
        self.client.runtime_profile()
    }
}
