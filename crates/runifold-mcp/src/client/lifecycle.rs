use super::{
    BTreeMap, ClientCapabilities, ClientState, ClientTaskRequestsCapability,
    ClientTaskSamplingRequestsCapability, ClientTasksCapability, Duration, JsonRpcNotification,
    JsonRpcRequest, McpClient, McpError, McpProtocolMode, Ordering, RequestId,
    STATELESS_PROTOCOL_VERSION, Serialize, StatelessCancellation, StatelessRequestMetadata, json,
};

impl McpClient {
    pub(super) async fn require_active(&self) -> Result<(), McpError> {
        if matches!(*self.inner.state.lock().await, ClientState::Active { .. }) {
            Ok(())
        } else {
            Err(McpError::lifecycle("client is not initialized"))
        }
    }

    pub(super) async fn require_resources(&self) -> Result<(), McpError> {
        match &*self.inner.state.lock().await {
            ClientState::Active { server, .. } if server.capabilities.resources.is_some() => Ok(()),
            ClientState::Active { .. } => Err(McpError::protocol(
                "server did not negotiate the resources capability",
            )),
            ClientState::Created | ClientState::Discovering | ClientState::Initializing => {
                Err(McpError::lifecycle("client is not initialized"))
            }
        }
    }

    pub(super) async fn require_prompts(&self) -> Result<(), McpError> {
        match &*self.inner.state.lock().await {
            ClientState::Active { server, .. } if server.capabilities.prompts.is_some() => Ok(()),
            ClientState::Active { .. } => Err(McpError::protocol(
                "server did not negotiate the prompts capability",
            )),
            ClientState::Created | ClientState::Discovering | ClientState::Initializing => {
                Err(McpError::lifecycle("client is not initialized"))
            }
        }
    }

    pub(super) async fn require_resource_subscriptions(&self) -> Result<(), McpError> {
        match &*self.inner.state.lock().await {
            ClientState::Active { server, .. }
                if server
                    .capabilities
                    .resources
                    .as_ref()
                    .is_some_and(|capability| capability.subscribe) =>
            {
                Ok(())
            }
            ClientState::Active { .. } => Err(McpError::protocol(
                "server did not negotiate resource subscriptions",
            )),
            ClientState::Created | ClientState::Discovering | ClientState::Initializing => {
                Err(McpError::lifecycle("client is not initialized"))
            }
        }
    }

    pub(super) async fn require_completions(&self) -> Result<(), McpError> {
        match &*self.inner.state.lock().await {
            ClientState::Active { server, .. } if server.capabilities.completions.is_some() => {
                Ok(())
            }
            ClientState::Active { .. } => Err(McpError::protocol(
                "server did not negotiate the completions capability",
            )),
            ClientState::Created | ClientState::Discovering | ClientState::Initializing => {
                Err(McpError::lifecycle("client is not initialized"))
            }
        }
    }

    pub(crate) async fn require_tasks(&self) -> Result<(), McpError> {
        if !self.inner.config.tasks_enabled {
            return Err(McpError::protocol(
                "MCP Tasks are disabled in the client policy",
            ));
        }
        match &*self.inner.state.lock().await {
            ClientState::Active { server, mode }
                if *mode == McpProtocolMode::Stateless
                    && server
                        .capabilities
                        .extensions
                        .contains_key(crate::TASKS_EXTENSION_ID) =>
            {
                Ok(())
            }
            ClientState::Active { .. } => Err(McpError::protocol(
                "server did not negotiate the MCP Tasks extension",
            )),
            ClientState::Created | ClientState::Discovering | ClientState::Initializing => {
                Err(McpError::lifecycle("client is not initialized"))
            }
        }
    }

    pub(super) async fn tasks_negotiated(&self) -> bool {
        self.inner.config.tasks_enabled
            && matches!(
                &*self.inner.state.lock().await,
                ClientState::Active { server, mode }
                    if *mode == McpProtocolMode::Stateless
                        && server.capabilities.extensions.contains_key(crate::TASKS_EXTENSION_ID)
            )
    }

    pub(super) async fn cancel_request(&self, id: &RequestId, reason: &str) {
        let is_stateless = matches!(
            &*self.inner.state.lock().await,
            ClientState::Active {
                mode: McpProtocolMode::Stateless,
                ..
            }
        );
        if is_stateless
            && self.inner.transport.stateless_cancellation() == StatelessCancellation::DropRequest
        {
            return;
        }
        let _ = self
            .inner
            .transport
            .notify(JsonRpcNotification::new(
                "notifications/cancelled",
                Some(json!({
                    "requestId": id,
                    "reason": reason,
                })),
            ))
            .await;
    }

    pub(super) fn next_id(&self) -> RequestId {
        RequestId::String(format!(
            "runifold-{}",
            self.inner.next_id.fetch_add(1, Ordering::Relaxed)
        ))
    }

    pub(super) fn client_capabilities(&self) -> ClientCapabilities {
        let mut capabilities = self
            .inner
            .config
            .mrtr_input_handler
            .as_ref()
            .map_or_else(ClientCapabilities::default, |handler| {
                handler.capabilities()
            });
        if let Some(sampling) = &self.inner.config.sampling {
            capabilities.sampling = Some(sampling.capability());
            if self.inner.config.sampling_tasks.is_some() {
                capabilities.tasks = Some(ClientTasksCapability {
                    list: None,
                    cancel: Some(BTreeMap::new()),
                    requests: Some(ClientTaskRequestsCapability {
                        sampling: Some(ClientTaskSamplingRequestsCapability {
                            create_message: Some(BTreeMap::new()),
                        }),
                    }),
                });
            }
        }
        if self.inner.config.tasks_enabled {
            capabilities
                .extensions
                .insert(crate::TASKS_EXTENSION_ID.into(), json!({}));
        }
        capabilities
    }

    pub(crate) fn request_timeout(&self) -> Duration {
        self.inner.config.request_timeout
    }

    pub(crate) fn max_task_poll_interval(&self) -> Duration {
        self.inner.config.max_task_poll_interval
    }

    pub(crate) fn min_task_poll_interval(&self) -> Duration {
        self.inner.config.min_task_poll_interval
    }

    pub(crate) fn max_task_inputs(&self) -> usize {
        self.inner.config.max_mrtr_inputs_per_round
    }

    pub(super) async fn request_with_current_metadata<P>(
        &self,
        id: RequestId,
        method: &str,
        params: &P,
    ) -> Result<JsonRpcRequest, McpError>
    where
        P: Serialize + ?Sized,
    {
        let mut params = serde_json::to_value(params)?;
        let is_stateless = matches!(
            &*self.inner.state.lock().await,
            ClientState::Active {
                mode: McpProtocolMode::Stateless,
                ..
            }
        );
        if is_stateless {
            let object = params
                .as_object_mut()
                .ok_or_else(|| McpError::protocol("MCP request parameters must be an object"))?;
            object.insert(
                "_meta".into(),
                serde_json::to_value(StatelessRequestMetadata {
                    protocol_version: STATELESS_PROTOCOL_VERSION.into(),
                    client_info: Some(self.inner.config.implementation.clone()),
                    client_capabilities: self.client_capabilities(),
                })?,
            );
        }
        Ok(JsonRpcRequest::new(id, method, Some(params)))
    }
}
