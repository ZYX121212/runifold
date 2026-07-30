use super::{
    Arc, BTreeMap, CancellationToken, CancelledParams, INVALID_PARAMS, INVALID_REQUEST,
    InflightGuard, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, METHOD_NOT_FOUND,
    McpError, McpSession, McpTaskBackendErrorKind, RequestEra, RequestId, ServerNotificationStream,
    SessionState, SubscriptionFilter, SubscriptionsListenParams, acknowledgement,
    attach_subscription_id, broadcast, decode_params, json, missing_tasks_capability,
    task_capability_declared,
};

impl McpSession {
    pub(crate) fn open_subscription(
        &self,
        request: JsonRpcRequest,
    ) -> Result<ServerNotificationStream, JsonRpcResponse> {
        let id = request.id.clone();
        if request.jsonrpc != "2.0" {
            return Err(JsonRpcResponse::error(
                id,
                INVALID_REQUEST,
                "jsonrpc must be `2.0`",
                None,
            ));
        }
        if request.method != "subscriptions/listen" {
            return Err(JsonRpcResponse::error(
                id,
                METHOD_NOT_FOUND,
                "method not found",
                None,
            ));
        }
        let era = Self::request_era(&id, request.params.as_ref())?;
        if era != RequestEra::Stateless {
            return Err(JsonRpcResponse::error(
                id,
                METHOD_NOT_FOUND,
                "subscriptions/listen requires the stateless protocol",
                None,
            ));
        }
        let params: SubscriptionsListenParams = decode_params(request.params).map_err(|error| {
            JsonRpcResponse::error(id.clone(), INVALID_PARAMS, error.to_string(), None)
        })?;
        if !params.notifications.task_ids.is_empty()
            && !task_capability_declared(params.metadata.as_ref())
        {
            return Err(missing_tasks_capability(id));
        }
        let accepted = self.accepted_subscription_filter(params.notifications);
        Ok(self.accepted_subscription_stream(id, accepted))
    }

    fn accepted_subscription_stream(
        &self,
        id: RequestId,
        accepted: SubscriptionFilter,
    ) -> ServerNotificationStream {
        let acknowledgment = acknowledgement(&id, &accepted);
        let mut receiver = self.inner.server.subscription_events.subscribe();
        let task_backend = self.inner.server.task_backend.clone();
        let mut task_ids = accepted.task_ids.clone();
        let mut observed_tasks = BTreeMap::new();
        let mut task_tick = tokio::time::interval(self.inner.server.task_notification_interval);
        task_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let cancellation = CancellationToken::new();
        self.lock_inflight()
            .insert(id.clone(), cancellation.clone());
        let guard = InflightGuard::new(id.clone(), Arc::clone(&self.inner.inflight));
        Box::pin(async_stream::stream! {
            let _guard = guard;
            yield Ok(acknowledgment);
            loop {
                tokio::select! {
                    () = cancellation.cancelled() => break,
                    event = receiver.recv() => {
                        match event {
                            Ok(notification) if accepted.accepts(&notification) => {
                                yield Ok(attach_subscription_id(notification, &id));
                            }
                            Ok(_) => {}
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                yield Err(McpError::protocol(format!(
                                    "MCP subscription receiver lagged by {skipped} messages"
                                )));
                                break;
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    _ = task_tick.tick(), if !task_ids.is_empty() => {
                        let Some(backend) = task_backend.as_ref() else {
                            task_ids.clear();
                            continue;
                        };
                        for task_id in task_ids.clone() {
                            match backend.get(task_id.clone()).await {
                                Ok(task)
                                    if task.validate().is_ok()
                                        && observed_tasks.get(&task_id) != Some(&task) =>
                                {
                                    let terminal = task.status.is_terminal();
                                    observed_tasks.insert(task_id.clone(), task.clone());
                                    let params = match serde_json::to_value(task) {
                                        Ok(params) => params,
                                        Err(error) => {
                                            yield Err(error.into());
                                            break;
                                        }
                                    };
                                    yield Ok(attach_subscription_id(
                                        JsonRpcNotification::new(
                                            "notifications/tasks",
                                            Some(params),
                                        ),
                                        &id,
                                    ));
                                    if terminal {
                                        task_ids.retain(|candidate| candidate != &task_id);
                                    }
                                }
                                Err(error)
                                    if error.kind == McpTaskBackendErrorKind::NotFound =>
                                {
                                    task_ids.retain(|candidate| candidate != &task_id);
                                }
                                Ok(_) | Err(_) => {}
                            }
                        }
                    }
                }
            }
        })
    }

    fn accepted_subscription_filter(&self, requested: SubscriptionFilter) -> SubscriptionFilter {
        let capabilities = self.server_capabilities(RequestEra::Stateless);
        let requested = requested.normalized();
        let accepted_task_ids = self
            .inner
            .server
            .task_backend
            .as_ref()
            .map(|_| {
                requested
                    .task_ids
                    .iter()
                    .take(self.inner.server.max_task_subscription_ids)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        SubscriptionFilter {
            tools_list_changed: requested.tools_list_changed
                && capabilities
                    .tools
                    .as_ref()
                    .is_some_and(|capability| capability.list_changed),
            prompts_list_changed: requested.prompts_list_changed
                && capabilities
                    .prompts
                    .as_ref()
                    .is_some_and(|capability| capability.list_changed),
            resources_list_changed: requested.resources_list_changed
                && capabilities
                    .resources
                    .as_ref()
                    .is_some_and(|capability| capability.list_changed),
            resource_subscriptions: self
                .inner
                .server
                .resources
                .as_ref()
                .map(|resources| {
                    requested
                        .resource_subscriptions
                        .into_iter()
                        .filter(|uri| {
                            resources.contains_authorized_uri(uri, &self.inner.server.authority)
                        })
                        .collect()
                })
                .unwrap_or_default(),
            task_ids: accepted_task_ids,
        }
    }

    /// Applies one client-to-server JSON-RPC notification to this session.
    ///
    /// # Errors
    ///
    /// Returns an error when the notification is malformed or violates the
    /// current session lifecycle.
    pub fn handle_notification(&self, notification: JsonRpcNotification) -> Result<(), McpError> {
        if notification.jsonrpc != "2.0" {
            return Err(McpError::protocol("jsonrpc must be `2.0`"));
        }
        match notification.method.as_str() {
            "notifications/initialized" => {
                let mut state = self.lock_state();
                if !matches!(*state, SessionState::AwaitingInitialized) {
                    return Err(McpError::lifecycle(
                        "initialized notification arrived outside initialization",
                    ));
                }
                *state = SessionState::Active;
                self.inner.active.notify_waiters();
                Ok(())
            }
            "notifications/cancelled" => {
                let params: CancelledParams = decode_params(notification.params)?;
                if let Some(token) = self.lock_inflight().get(&params.request_id) {
                    token.cancel();
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Opens this session's server-to-client notification stream.
    pub fn subscribe_notifications(&self) -> ServerNotificationStream {
        let mut receiver = self.inner.notifications.subscribe();
        Box::pin(async_stream::stream! {
            loop {
                match receiver.recv().await {
                    Ok(notification) => yield Ok(notification),
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        yield Err(McpError::protocol(format!(
                            "MCP notification receiver lagged by {skipped} messages"
                        )));
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }

    /// Emits a resource update only when this session subscribed to `uri`.
    pub fn notify_resource_updated(&self, uri: &str) -> bool {
        if !self.lock_subscriptions().contains(uri) {
            return false;
        }
        self.inner
            .notifications
            .send(JsonRpcNotification::new(
                "notifications/resources/updated",
                Some(json!({"uri": uri})),
            ))
            .is_ok()
    }

    pub(crate) fn is_resource_subscribed(&self, uri: &str) -> bool {
        self.lock_subscriptions().contains(uri)
    }

    /// Emits a resource-list change notification.
    pub fn notify_resource_list_changed(&self) -> bool {
        self.inner
            .notifications
            .send(JsonRpcNotification::new(
                "notifications/resources/list_changed",
                None,
            ))
            .is_ok()
    }
}
