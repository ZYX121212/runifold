//! Explicit background-response lifecycle and native context compaction.

use std::fmt;

use runifold_model::{
    ContentPart, Message, ModelCallContext, ModelRef, ModelRequest, ModelResponse, ProviderData,
    Role,
};
use serde_json::{Value, json};

use super::{OpenAiControlError, OpenAiControlFuture, OpenAiControlPlane, validate_id};
use crate::openai::{decode::decode_complete_response_for, encode::encode_request_for};

/// Provider-owned response lifecycle; it does not represent a committed Agent turn.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OpenAiResponseStatus {
    /// The provider has queued generation.
    Queued,
    /// Generation is executing.
    InProgress,
    /// Generation completed normally.
    Completed,
    /// Generation ended without a complete result.
    Incomplete,
    /// The provider reported failure.
    Failed,
    /// Cancellation completed on the provider.
    Cancelled,
    /// A future provider status retained without claiming completion.
    Other(String),
}

/// A retrieved response bound to its provider; output is private in Debug.
#[derive(Clone)]
pub struct OpenAiStoredResponse {
    provider: String,
    id: String,
    status: OpenAiResponseStatus,
    payload: Value,
}

impl fmt::Debug for OpenAiStoredResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiStoredResponse")
            .field("id", &self.id)
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}

impl OpenAiStoredResponse {
    /// Returns the provider response identity used for retrieval and cancellation.
    pub fn id(&self) -> &str {
        &self.id
    }
    /// Returns generation state, independently of conversation commit state.
    pub fn status(&self) -> &OpenAiResponseStatus {
        &self.status
    }
    /// Inspects the original provider response, including unsupported fields.
    pub fn payload(&self) -> &Value {
        &self.payload
    }
    /// Reconstructs a terminal canonical response without another model invocation.
    ///
    /// # Errors
    /// Rejects pending/cancelled responses, failed responses, or invalid protocol content.
    pub fn model_response(&self) -> Result<ModelResponse, runifold_model::ModelError> {
        let events = decode_complete_response_for(
            &self.provider,
            &self.payload,
            None,
            crate::openai::OpenAiResponsesDialect::OpenAi,
        )?;
        let mut accumulator = runifold_model::ModelStreamAccumulator::new();
        for event in events {
            if let Some(response) = accumulator.push(event)? {
                return Ok(response);
            }
        }
        Err(runifold_model::ModelError::local(
            runifold_model::ModelErrorKind::Protocol,
            "stored response did not produce a terminal result",
        ))
    }
}

/// A compacted input window; its encrypted content is private in Debug.
#[derive(Clone)]
pub struct OpenAiCompactedResponse {
    provider: String,
    id: String,
    output: Vec<Value>,
    payload: Value,
}

impl fmt::Debug for OpenAiCompactedResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiCompactedResponse")
            .field("id", &self.id)
            .field("items", &self.output.len())
            .finish_non_exhaustive()
    }
}

impl OpenAiCompactedResponse {
    /// Returns the compaction identity, which is not a response continuation ID.
    pub fn id(&self) -> &str {
        &self.id
    }
    /// Returns every item in the canonical next input window in provider order.
    pub fn output(&self) -> &[Value] {
        &self.output
    }
    /// Inspects original usage and future provider fields.
    pub fn payload(&self) -> &Value {
        &self.payload
    }
    /// Wraps the entire window for the existing model request encoder.
    ///
    /// Replace the previous model input window with this message. Do not append
    /// it to the old window or prune its items. This does not rewrite durable
    /// conversation history or grant capabilities.
    ///
    /// # Errors
    /// Rejects an empty compacted window.
    pub fn input_message(&self) -> Result<Message, runifold_model::ModelError> {
        Message::new(
            Role::User,
            self.output
                .iter()
                .map(|value| {
                    ContentPart::ProviderOpaque(ProviderData {
                        provider: self.provider.clone(),
                        kind: "input_item".into(),
                        value: value.clone(),
                    })
                })
                .collect(),
        )
    }
}

impl OpenAiControlPlane {
    /// Starts provider background generation with explicit cancellation and deadline.
    ///
    /// The host must persist the returned identity, reconcile usage and retrieve
    /// the result. Dropping this future does not cancel accepted remote work.
    /// This operation never retries an ambiguous submission.
    pub fn create_background_response(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<OpenAiStoredResponse, OpenAiControlError>> {
        Box::pin(async move {
            self.require_response_provider(&request.model)?;
            let mut body = encode_request_for(&request, &self.config.provider).map_err(|_| {
                OpenAiControlError::InvalidRequest(
                    "background request failed model preflight".into(),
                )
            })?;
            body["background"] = Value::Bool(true);
            body["stream"] = Value::Bool(false);
            let response = self
                .send(
                    self.http.post(self.endpoint("responses")).json(&body),
                    &context,
                )
                .await?;
            self.decode_stored_response(response, &context, None).await
        })
    }

    /// Starts a background response with resumable provider streaming enabled.
    ///
    /// The first response identity is returned before exposing its event stream.
    /// Persist it and each consumed cursor before reconnecting. This never
    /// retries an ambiguous submission or changes the caller's storage policy.
    pub fn create_background_stream(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<OpenAiBackgroundStream, OpenAiControlError>> {
        Box::pin(async move {
            use futures_util::StreamExt;
            self.require_response_provider(&request.model)?;
            let mut body = encode_request_for(&request, &self.config.provider).map_err(|_| {
                OpenAiControlError::InvalidRequest(
                    "background request failed model preflight".into(),
                )
            })?;
            body["background"] = Value::Bool(true);
            body["stream"] = Value::Bool(true);
            let response = self
                .send(
                    self.http
                        .post(self.endpoint("responses"))
                        .header("accept", "text/event-stream")
                        .json(&body),
                    &context,
                )
                .await?;
            if !response.status().is_success() {
                let _: Value = self.decode(response, &context).await?;
                return Err(OpenAiControlError::Protocol(
                    "background streaming request was not successful".into(),
                ));
            }
            let mut events = response_events(response, context, None, None);
            let first = events.next().await.ok_or_else(|| {
                OpenAiControlError::Protocol(
                    "background stream ended before response identity".into(),
                )
            })??;
            if first.get("type").and_then(Value::as_str) != Some("response.created") {
                return Err(OpenAiControlError::Protocol(
                    "background stream did not begin with response.created".into(),
                ));
            }
            let id = first
                .get("response")
                .and_then(|response| response.get("id"))
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    OpenAiControlError::Protocol(
                        "background stream did not acknowledge a response identity".into(),
                    )
                })?;
            let id = validate_id("response", id.into())?;
            let events =
                Box::pin(futures_util::stream::once(async move { Ok(first) }).chain(events));
            Ok(OpenAiBackgroundStream { id, events })
        })
    }

    /// Retrieves generation state without submitting another model request.
    pub fn get_response(
        &self,
        id: &str,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<OpenAiStoredResponse, OpenAiControlError>> {
        let id = validate_id("response", id.into());
        Box::pin(async move {
            let id = id?;
            let response = self
                .send(
                    self.http.get(self.endpoint(&format!("responses/{id}"))),
                    &context,
                )
                .await?;
            self.decode_stored_response(response, &context, Some(&id))
                .await
        })
    }

    /// Requests remote cancellation without undoing already accepted external effects.
    pub fn cancel_response(
        &self,
        id: &str,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<OpenAiStoredResponse, OpenAiControlError>> {
        let id = validate_id("response", id.into());
        Box::pin(async move {
            let id = id?;
            let response = self
                .send(
                    self.http
                        .post(self.endpoint(&format!("responses/{id}/cancel"))),
                    &context,
                )
                .await?;
            self.decode_stored_response(response, &context, Some(&id))
                .await
        })
    }

    /// Deletes a stored provider response after the host has retained needed evidence.
    pub fn delete_response(
        &self,
        id: &str,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<(), OpenAiControlError>> {
        let id = validate_id("response", id.into());
        Box::pin(async move {
            let id = id?;
            let response = self
                .send(
                    self.http.delete(self.endpoint(&format!("responses/{id}"))),
                    &context,
                )
                .await?;
            let payload: Value = self.decode(response, &context).await?;
            if payload.get("id").and_then(Value::as_str) != Some(&id)
                || payload.get("deleted").and_then(Value::as_bool) != Some(true)
            {
                return Err(OpenAiControlError::Protocol(
                    "response deletion was not acknowledged for the requested identity".into(),
                ));
            }
            Ok(())
        })
    }

    /// Compacts an explicit model input window without altering conversation storage.
    ///
    /// All returned items must be used as the next window. The request's tools,
    /// generation settings and output mode are not compaction parameters.
    pub fn compact_response(
        &self,
        request: ModelRequest,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<OpenAiCompactedResponse, OpenAiControlError>> {
        Box::pin(async move {
            self.require_response_provider(&request.model)?;
            let input = crate::openai::encode::encode_input(&request).map_err(|_| {
                OpenAiControlError::InvalidRequest(
                    "compaction input cannot be encoded losslessly".into(),
                )
            })?;
            if input.is_empty() {
                return Err(OpenAiControlError::InvalidRequest(
                    "compaction requires an input window".into(),
                ));
            }
            let mut body = json!({"model":request.model.name,"input":input});
            merge_compaction_options(&mut body, &request, &self.config.provider)?;
            let response = self
                .send(
                    self.http
                        .post(self.endpoint("responses/compact"))
                        .json(&body),
                    &context,
                )
                .await?;
            let payload: Value = self.decode(response, &context).await?;
            let id = payload
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .ok_or_else(|| {
                    OpenAiControlError::Protocol("compaction identity is missing".into())
                })?
                .to_owned();
            if payload.get("object").and_then(Value::as_str) != Some("response.compaction") {
                return Err(OpenAiControlError::Protocol(
                    "compaction returned an unexpected object".into(),
                ));
            }
            let output = payload
                .get("output")
                .and_then(Value::as_array)
                .filter(|items| {
                    !items.is_empty()
                        && items
                            .iter()
                            .all(|item| item.get("type").and_then(Value::as_str).is_some())
                })
                .ok_or_else(|| {
                    OpenAiControlError::Protocol(
                        "compaction output is not a nonempty item window".into(),
                    )
                })?
                .clone();
            Ok(OpenAiCompactedResponse {
                provider: self.config.provider.clone(),
                id,
                output,
                payload,
            })
        })
    }

    fn require_response_provider(&self, model: &ModelRef) -> Result<(), OpenAiControlError> {
        if model.provider != self.config.provider
            || model.name.trim().is_empty()
            || self.config.wire_protocol != crate::openai::OpenAiWireProtocol::Responses
        {
            return Err(OpenAiControlError::InvalidRequest("response operations require the configured Responses provider and a nonempty model identity".into()));
        }
        Ok(())
    }

    async fn decode_stored_response(
        &self,
        response: reqwest::Response,
        context: &ModelCallContext,
        expected: Option<&str>,
    ) -> Result<OpenAiStoredResponse, OpenAiControlError> {
        let payload: Value = self.decode(response, context).await?;
        let id = payload
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| OpenAiControlError::Protocol("response identity is missing".into()))?
            .to_owned();
        if expected.is_some_and(|expected| id != expected) {
            return Err(OpenAiControlError::Protocol(
                "retrieved response identity disagrees with the request".into(),
            ));
        }
        let status = match payload
            .get("status")
            .and_then(Value::as_str)
            .ok_or_else(|| OpenAiControlError::Protocol("response status is missing".into()))?
        {
            "queued" => OpenAiResponseStatus::Queued,
            "in_progress" => OpenAiResponseStatus::InProgress,
            "completed" => OpenAiResponseStatus::Completed,
            "incomplete" => OpenAiResponseStatus::Incomplete,
            "failed" => OpenAiResponseStatus::Failed,
            "cancelled" => OpenAiResponseStatus::Cancelled,
            other => OpenAiResponseStatus::Other(other.into()),
        };
        Ok(OpenAiStoredResponse {
            provider: self.config.provider.clone(),
            id,
            status,
            payload,
        })
    }
}

fn merge_compaction_options(
    body: &mut Value,
    request: &ModelRequest,
    provider: &str,
) -> Result<(), OpenAiControlError> {
    for namespace in std::iter::once("openai-compatible")
        .chain((provider != "openai-compatible").then_some(provider))
    {
        let Some(options) = request.provider_options.get(namespace) else {
            continue;
        };
        let options = options.as_object().ok_or_else(|| {
            OpenAiControlError::InvalidRequest(
                "compaction provider options must be an object".into(),
            )
        })?;
        for (key, value) in options {
            let supported = match key.as_str() {
                "instructions"
                | "previous_response_id"
                | "prompt_cache_key"
                | "prompt_cache_retention"
                | "service_tier" => value.is_string() || value.is_null(),
                "prompt_cache_options" => value.is_object() || value.is_null(),
                _ => false,
            };
            if !supported || body.get(key).is_some() {
                return Err(OpenAiControlError::InvalidRequest(format!(
                    "unsupported, malformed or conflicting compaction option `{key}`; use only options documented for /responses/compact"
                )));
            }
            body[key] = value.clone();
        }
    }
    Ok(())
}

/// An acknowledged background response and its resumable raw event subscription.
pub struct OpenAiBackgroundStream {
    id: String,
    events: OpenAiResponseEventStream,
}

impl fmt::Debug for OpenAiBackgroundStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiBackgroundStream")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl OpenAiBackgroundStream {
    /// Returns the accepted response identity; persist it before consuming further events.
    pub fn id(&self) -> &str {
        &self.id
    }
    /// Transfers ownership of this event subscription without changing remote task ownership.
    pub fn into_events(self) -> OpenAiResponseEventStream {
        self.events
    }
}

/// Provider events from a stored response, without fabricating missing prefix deltas.
#[cfg(not(target_arch = "wasm32"))]
pub type OpenAiResponseEventStream =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<Value, OpenAiControlError>> + Send>>;
/// Provider events from a stored response in a single-threaded browser host.
#[cfg(target_arch = "wasm32")]
pub type OpenAiResponseEventStream =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<Value, OpenAiControlError>>>>;

impl OpenAiControlPlane {
    /// Reattaches to a stored streaming response after an optional event cursor.
    ///
    /// The response must originally have been created with stream=true.
    /// Events before the cursor are not reconstructed. Fetch the final stored
    /// response to obtain canonical content. Disconnect never submits new model work.
    pub fn stream_response(
        &self,
        id: &str,
        starting_after: Option<u64>,
        context: ModelCallContext,
    ) -> OpenAiControlFuture<'_, Result<OpenAiResponseEventStream, OpenAiControlError>> {
        let id = validate_id("response", id.into());
        Box::pin(async move {
            let id = id?;
            let mut endpoint = self.endpoint(&format!("responses/{id}"));
            endpoint.query_pairs_mut().append_pair("stream", "true");
            if let Some(cursor) = starting_after {
                endpoint
                    .query_pairs_mut()
                    .append_pair("starting_after", &cursor.to_string());
            }
            let builder = self
                .http
                .get(endpoint)
                .header("accept", "text/event-stream");
            let response = self.send(builder, &context).await?;
            if !response.status().is_success() {
                let _: Value = self.decode(response, &context).await?;
                return Err(OpenAiControlError::Protocol(
                    "response stream was not successful".into(),
                ));
            }
            Ok(response_events(response, context, starting_after, Some(id)))
        })
    }
}

fn response_events(
    response: reqwest::Response,
    context: ModelCallContext,
    mut cursor: Option<u64>,
    mut id: Option<String>,
) -> OpenAiResponseEventStream {
    use eventsource_stream::{EventStreamError, Eventsource};
    use futures_util::{
        StreamExt,
        future::{Either, select},
        pin_mut,
    };
    Box::pin(async_stream::try_stream! {
        let deadline = context.deadline();
        let bounded = async_stream::try_stream! {
            let mut chunks = response.bytes_stream();
            let mut guard = crate::openai::client::SseSizeGuard::default();
            while let Some(chunk) = chunks.next().await {
                let chunk = chunk.map_err(|error| super::transport_error(error, deadline))?;
                guard.observe(&chunk).map_err(|()| OpenAiControlError::Protocol("stored response event exceeds the SSE size limit".into()))?;
                yield chunk;
            }
        };
        let source = bounded.eventsource();
        pin_mut!(source);
        let mut terminal = false;
        loop {
            let event = match select(Box::pin(context.cancellation().cancelled()), Box::pin(source.next())).await {
                Either::Left(_) => Err(OpenAiControlError::Cancelled)?,
                Either::Right((event, _)) => event,
            };
            let Some(event) = event else { break; };
            let event = event.map_err(|error| match error {
                EventStreamError::Transport(error) => error,
                _ => OpenAiControlError::Protocol("invalid stored response SSE frame".into()),
            })?;
            let payload: Value = serde_json::from_str(&event.data).map_err(|_| OpenAiControlError::Protocol("stored response SSE payload is not JSON".into()))?;
            let sequence = payload.get("sequence_number").and_then(Value::as_u64).ok_or_else(|| OpenAiControlError::Protocol("stored response event has no sequence number".into()))?;
            if cursor.is_some_and(|previous| sequence <= previous) { Err(OpenAiControlError::Protocol("stored response cursor did not advance".into()))?; }
            cursor = Some(sequence);
            let kind = payload.get("type").and_then(Value::as_str).ok_or_else(|| OpenAiControlError::Protocol("stored response event has no type".into()))?;
            if let Some(event_id) = payload.get("response_id").filter(|id| !id.is_null()) {
                let event_id = event_id.as_str().ok_or_else(|| OpenAiControlError::Protocol("stored response event identity is not a string".into()))?;
                if id.as_deref().is_some_and(|id| event_id != id) { Err(OpenAiControlError::Protocol("stored response event changed response identity".into()))?; }
                if id.is_none() { id = Some(event_id.into()); }
            }
            if let Some(response_id) = payload.get("response").and_then(|response| response.get("id")).and_then(Value::as_str)
            {
                if id.as_deref().is_some_and(|id| response_id != id) { Err(OpenAiControlError::Protocol("stored response event changed response identity".into()))?; }
                if id.is_none() { id = Some(response_id.into()); }
            }
            terminal = matches!(kind, "response.completed" | "response.incomplete" | "response.failed" | "error");
            yield payload;
            if terminal { break; }
        }
        if !terminal { Err(OpenAiControlError::Protocol("stored response stream ended before a terminal event; reconnect with the last cursor".into()))?; }
    })
}
