use super::{
    Arc, AtomicU64, ClientInner, ClientPeerHandler, ClientResponseCache, ClientState,
    DiscoverParams, DiscoverResult, InitializeParams, InitializeResult, JsonRpcNotification,
    LATEST_PROTOCOL_VERSION, McpClient, McpClientConfig, McpError, McpProtocolMode, McpTransport,
    Mutex, STATELESS_PROTOCOL_VERSION, StatelessRequestMetadata,
};

impl McpClient {
    /// Creates a client over a pluggable transport.
    pub fn new(transport: Arc<dyn McpTransport>, config: McpClientConfig) -> Self {
        let cache = ClientResponseCache::new(
            Arc::clone(&config.response_cache),
            config.cache_namespace.clone(),
            config.private_cache_partition.clone(),
            config.max_cache_ttl,
        );
        Self {
            inner: Arc::new(ClientInner {
                transport,
                config,
                cache,
                next_id: AtomicU64::new(1),
                state: Mutex::new(ClientState::Created),
            }),
        }
    }

    /// Discovers server identity, capabilities, and supported protocol versions
    /// without creating a protocol session.
    ///
    /// The returned identity is self-reported and must not be used for
    /// authorization decisions. This compatibility probe leaves the client in
    /// the created state, so callers can subsequently use [`Self::initialize`]
    /// when the server advertises the legacy revision.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for lifecycle, transport, timeout, or malformed
    /// discovery responses.
    pub async fn discover(&self) -> Result<DiscoverResult, McpError> {
        {
            let mut state = self.inner.state.lock().await;
            if !matches!(*state, ClientState::Created) {
                return Err(McpError::lifecycle(
                    "server discovery must run before initialization",
                ));
            }
            *state = ClientState::Discovering;
        }
        let params = DiscoverParams {
            metadata: StatelessRequestMetadata {
                protocol_version: STATELESS_PROTOCOL_VERSION.into(),
                client_info: Some(self.inner.config.implementation.clone()),
                client_capabilities: self.client_capabilities(),
            },
        };
        let result = self
            .request_typed(
                "server/discover",
                &params,
                self.inner.config.request_timeout,
            )
            .await;
        *self.inner.state.lock().await = ClientState::Created;
        result
    }

    /// Selects the newest mutually supported protocol era.
    ///
    /// Modern servers are activated without an initialization handshake.
    /// Servers that advertise only `2025-11-25` use the legacy initialization
    /// flow. The selected mode is available through [`Self::protocol_mode`].
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when discovery, version selection, or legacy
    /// initialization fails.
    pub async fn connect(&self) -> Result<McpProtocolMode, McpError> {
        let discovered = match self.discover().await {
            Ok(discovered) => discovered,
            Err(error) if Self::should_fallback_to_legacy(&error) => {
                self.initialize().await?;
                return Ok(McpProtocolMode::Legacy);
            }
            Err(error) => return Err(error),
        };
        if discovered
            .supported_versions
            .iter()
            .any(|version| version == STATELESS_PROTOCOL_VERSION)
        {
            let server = InitializeResult {
                protocol_version: STATELESS_PROTOCOL_VERSION.into(),
                capabilities: discovered.capabilities,
                server_info: discovered.metadata.server_info,
                instructions: discovered.instructions,
            };
            *self.inner.state.lock().await = ClientState::Active {
                server: Box::new(server),
                mode: McpProtocolMode::Stateless,
            };
            return Ok(McpProtocolMode::Stateless);
        }
        if discovered
            .supported_versions
            .iter()
            .any(|version| version == LATEST_PROTOCOL_VERSION)
        {
            self.initialize().await?;
            return Ok(McpProtocolMode::Legacy);
        }
        Err(McpError::UnsupportedVersion {
            selected: discovered.supported_versions.join(","),
        })
    }

    /// Negotiates the finalized MCP protocol and Tool capability.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for transport, protocol, version, timeout, or
    /// lifecycle failures.
    pub async fn initialize(&self) -> Result<InitializeResult, McpError> {
        if self.inner.config.sampling_tasks.is_some() && self.inner.config.sampling.is_none() {
            return Err(McpError::lifecycle(
                "durable Sampling Tasks require a SamplingService",
            ));
        }
        {
            let mut state = self.inner.state.lock().await;
            if !matches!(*state, ClientState::Created) {
                return Err(McpError::lifecycle(
                    "client initialization may only run once",
                ));
            }
            *state = ClientState::Initializing;
        }
        let params = InitializeParams {
            protocol_version: LATEST_PROTOCOL_VERSION.into(),
            capabilities: self.client_capabilities(),
            client_info: self.inner.config.implementation.clone(),
        };
        let initialized = self
            .request_typed::<_, InitializeResult>(
                "initialize",
                &params,
                self.inner.config.request_timeout,
            )
            .await;
        let initialized = match initialized {
            Ok(initialized) => initialized,
            Err(error) => {
                *self.inner.state.lock().await = ClientState::Created;
                return Err(error);
            }
        };
        if initialized.protocol_version != LATEST_PROTOCOL_VERSION {
            *self.inner.state.lock().await = ClientState::Created;
            return Err(McpError::UnsupportedVersion {
                selected: initialized.protocol_version,
            });
        }
        if let Some(sampling) = &self.inner.config.sampling {
            let handler = Arc::new(ClientPeerHandler::new(
                Arc::clone(sampling),
                self.inner.config.sampling_tasks.clone(),
            ));
            if let Err(error) = self.inner.transport.install_peer_handler(handler) {
                *self.inner.state.lock().await = ClientState::Created;
                return Err(error);
            }
            if let Err(error) = self.inner.transport.start_peer().await {
                *self.inner.state.lock().await = ClientState::Created;
                return Err(error);
            }
        }
        if let Err(error) = self
            .inner
            .transport
            .notify(JsonRpcNotification::new("notifications/initialized", None))
            .await
        {
            *self.inner.state.lock().await = ClientState::Created;
            return Err(error);
        }
        *self.inner.state.lock().await = ClientState::Active {
            server: Box::new(initialized.clone()),
            mode: McpProtocolMode::Legacy,
        };
        Ok(initialized)
    }

    /// Returns the negotiated server information.
    pub async fn server_info(&self) -> Option<InitializeResult> {
        match &*self.inner.state.lock().await {
            ClientState::Active { server, .. } => Some((**server).clone()),
            ClientState::Created | ClientState::Discovering | ClientState::Initializing => None,
        }
    }

    /// Returns the protocol mode selected for ordinary requests.
    pub async fn protocol_mode(&self) -> Option<McpProtocolMode> {
        match &*self.inner.state.lock().await {
            ClientState::Active { mode, .. } => Some(*mode),
            ClientState::Created | ClientState::Discovering | ClientState::Initializing => None,
        }
    }
    fn should_fallback_to_legacy(error: &McpError) -> bool {
        matches!(
            error,
            McpError::Remote { code: -32601, .. }
                | McpError::HttpStatus {
                    status: 400 | 404 | 405,
                    ..
                }
        )
    }
}
