use super::LIFECYCLE_ERROR;
use super::{
    Arc, CacheHint, CacheOperation, CacheScope, CancellationToken, CapabilityDescriptor,
    CapabilitySet, ClientCapabilities, ClientPeerTransport, CreateMessageParams, Duration, HashMap,
    HashSet, INTERNAL_ERROR, INVALID_PARAMS, IncludeContext, InflightGuard, JsonRpcNotification,
    JsonRpcRequest, JsonRpcResponse, McpError, McpSamplingClient, McpSession, MutexGuard,
    RequestEra, RequestId, RunContext, STATELESS_PROTOCOL_VERSION, SUPPORTED_PROTOCOL_VERSIONS,
    SessionState, StatelessRequestMetadata, UNSUPPORTED_PROTOCOL_VERSION, Value, json,
};

impl McpSession {
    pub(super) fn require_active(
        &self,
        id: &RequestId,
        era: RequestEra,
    ) -> Option<JsonRpcResponse> {
        (era == RequestEra::Legacy && !matches!(*self.lock_state(), SessionState::Active)).then(
            || {
                JsonRpcResponse::error(
                    id.clone(),
                    LIFECYCLE_ERROR,
                    "session is not initialized",
                    None,
                )
            },
        )
    }

    pub(super) fn request_era(
        id: &RequestId,
        params: Option<&Value>,
    ) -> Result<RequestEra, JsonRpcResponse> {
        let Some(metadata) = params
            .and_then(Value::as_object)
            .and_then(|params| params.get("_meta"))
        else {
            return Ok(RequestEra::Legacy);
        };
        let metadata: StatelessRequestMetadata =
            serde_json::from_value(metadata.clone()).map_err(|error| {
                JsonRpcResponse::error(id.clone(), INVALID_PARAMS, error.to_string(), None)
            })?;
        if metadata.protocol_version != STATELESS_PROTOCOL_VERSION {
            return Err(JsonRpcResponse::error(
                id.clone(),
                UNSUPPORTED_PROTOCOL_VERSION,
                "unsupported protocol version",
                Some(json!({
                    "requested": metadata.protocol_version,
                    "supported": SUPPORTED_PROTOCOL_VERSIONS,
                })),
            ));
        }
        Ok(RequestEra::Stateless)
    }

    pub(super) fn cursor_namespace(&self, era: RequestEra) -> &str {
        match era {
            RequestEra::Stateless => &self.inner.server.stateless_cursor_namespace,
            RequestEra::Legacy => &self.inner.cursor_namespace,
        }
    }

    pub(super) fn cache_fields(
        &self,
        era: RequestEra,
        operation: CacheOperation,
    ) -> (Option<u64>, Option<CacheScope>) {
        if era == RequestEra::Legacy {
            return (None, None);
        }
        let hint = self
            .inner
            .server
            .cache_hints
            .get(&operation)
            .copied()
            .unwrap_or_else(CacheHint::no_store);
        (Some(hint.ttl_ms), Some(hint.cache_scope))
    }

    pub(super) fn stateless_response(&self, response: JsonRpcResponse) -> JsonRpcResponse {
        match response {
            JsonRpcResponse::Success {
                jsonrpc,
                id,
                mut result,
            } => {
                let Some(result) = result.as_object_mut() else {
                    return JsonRpcResponse::error(
                        id,
                        INTERNAL_ERROR,
                        "stateless MCP results must be objects",
                        None,
                    );
                };
                result
                    .entry("resultType")
                    .or_insert_with(|| Value::String("complete".into()));
                result.entry("_meta").or_insert_with(|| {
                    json!({
                        "io.modelcontextprotocol/serverInfo":
                            self.inner.server.implementation
                    })
                });
                JsonRpcResponse::Success {
                    jsonrpc,
                    id,
                    result: Value::Object(result.clone()),
                }
            }
            JsonRpcResponse::Error {
                jsonrpc,
                id,
                mut error,
            } => {
                if error.code == LIFECYCLE_ERROR {
                    error.code = INVALID_PARAMS;
                }
                JsonRpcResponse::Error { jsonrpc, id, error }
            }
        }
    }

    pub(super) fn scoped_request(
        &self,
        id: &RequestId,
        capability: &CapabilityDescriptor,
    ) -> Option<(RunContext, InflightGuard)> {
        if !self
            .inner
            .server
            .authority
            .capabilities()
            .contains(capability.id)
        {
            return None;
        }
        let mut capabilities = CapabilitySet::new();
        capabilities.grant(capability.clone());
        let child = self.inner.server.authority.child(capabilities).ok()?;
        self.lock_inflight()
            .insert(id.clone(), child.cancellation().clone());
        let guard = InflightGuard::new(id.clone(), Arc::clone(&self.inner.inflight));
        Some((child, guard))
    }

    pub(super) fn lock_state(&self) -> MutexGuard<'_, SessionState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn lock_inflight(&self) -> MutexGuard<'_, HashMap<RequestId, CancellationToken>> {
        self.inner
            .inflight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn lock_subscriptions(&self) -> MutexGuard<'_, HashSet<String>> {
        self.inner
            .subscriptions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(super) fn lock_client_capabilities(&self) -> MutexGuard<'_, Option<ClientCapabilities>> {
        self.inner
            .client_capabilities
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_client_peer(&self) -> MutexGuard<'_, Option<Arc<dyn ClientPeerTransport>>> {
        self.inner
            .client_peer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn install_client_peer(&self, peer: Arc<dyn ClientPeerTransport>) {
        *self.lock_client_peer() = Some(peer);
    }

    pub(crate) async fn request_peer(
        &self,
        request: JsonRpcRequest,
    ) -> Result<JsonRpcResponse, McpError> {
        let peer = self
            .lock_client_peer()
            .clone()
            .ok_or_else(|| McpError::protocol("MCP client peer is not connected"))?;
        peer.request_client(request).await
    }

    pub(crate) async fn notify_peer(
        &self,
        notification: JsonRpcNotification,
    ) -> Result<(), McpError> {
        let peer = self
            .lock_client_peer()
            .clone()
            .ok_or_else(|| McpError::protocol("MCP client peer is not connected"))?;
        peer.notify_client(notification).await
    }

    pub(crate) fn ensure_sampling_supported(
        &self,
        params: &CreateMessageParams,
    ) -> Result<(), McpError> {
        let capabilities = self.lock_client_capabilities();
        let sampling = capabilities
            .as_ref()
            .and_then(|capabilities| capabilities.sampling.as_ref())
            .ok_or_else(|| McpError::protocol("client did not negotiate Sampling"))?;
        if (!params.tools.is_empty() || params.tool_choice.is_some()) && sampling.tools.is_none() {
            return Err(McpError::protocol(
                "client did not negotiate Tool-enabled Sampling",
            ));
        }
        if !matches!(params.include_context, IncludeContext::None) && sampling.context.is_none() {
            return Err(McpError::protocol(
                "client did not negotiate Sampling context inclusion",
            ));
        }
        if params.task.is_some()
            && !capabilities.as_ref().is_some_and(|capabilities| {
                capabilities
                    .tasks
                    .as_ref()
                    .and_then(|tasks| tasks.requests.as_ref())
                    .and_then(|requests| requests.sampling.as_ref())
                    .and_then(|sampling| sampling.create_message.as_ref())
                    .is_some()
            })
        {
            return Err(McpError::protocol(
                "client did not negotiate task-augmented Sampling",
            ));
        }
        Ok(())
    }

    pub(crate) fn ensure_sampling_task_cancel_supported(&self) -> Result<(), McpError> {
        let capabilities = self.lock_client_capabilities();
        if capabilities
            .as_ref()
            .and_then(|capabilities| capabilities.tasks.as_ref())
            .and_then(|tasks| tasks.cancel.as_ref())
            .is_none()
        {
            return Err(McpError::protocol(
                "client did not negotiate Sampling Task cancellation",
            ));
        }
        Ok(())
    }

    pub(crate) async fn await_active(&self, timeout: Duration) -> Result<(), McpError> {
        let notified = self.inner.active.notified();
        match *self.lock_state() {
            SessionState::Active => return Ok(()),
            SessionState::AwaitingInitialized => {}
            SessionState::Created | SessionState::Initializing => {
                return Err(McpError::lifecycle("session is not initialized"));
            }
        }
        tokio::time::timeout(timeout, notified)
            .await
            .map_err(|_| McpError::DeadlineExceeded)?;
        if matches!(*self.lock_state(), SessionState::Active) {
            Ok(())
        } else {
            Err(McpError::lifecycle("session is not initialized"))
        }
    }

    pub(crate) fn cancel_all_inflight(&self) {
        for cancellation in self.lock_inflight().values() {
            cancellation.cancel();
        }
    }

    /// Creates a server-to-client Sampling requester bound to this session.
    pub fn sampling_client(&self) -> McpSamplingClient {
        McpSamplingClient::new(self.clone())
    }
}
