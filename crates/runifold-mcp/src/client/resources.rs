use super::{
    CacheMode, HashSet, ListResourceTemplatesParams, ListResourceTemplatesResult,
    ListResourcesParams, ListResourcesResult, McpClient, McpError, McpProtocolMode, McpResource,
    McpResourceTemplate, McpSubscription, ReadResourceParams, ReadResourceResult,
    ResourceSubscriptionParams, ServerNotificationStream, StreamExt,
    SubscriptionAcknowledgedParams, SubscriptionFilter, SubscriptionsListenParams,
    notification_subscription_id,
};

impl McpClient {
    /// Lists authorized remote resources.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when resources were not negotiated, pagination is
    /// returned, or the request fails.
    pub async fn list_resources(&self) -> Result<Vec<McpResource>, McpError> {
        self.require_resources().await?;
        let mut resources = Vec::new();
        let mut cursor = None;
        let mut seen = HashSet::new();
        for _ in 0..self.inner.config.max_pagination_pages {
            let page = self.list_resources_page(cursor).await?;
            resources.extend(page.resources);
            let Some(next) = page.next_cursor else {
                return Ok(resources);
            };
            if !seen.insert(next.clone()) {
                return Err(McpError::protocol("server repeated a pagination cursor"));
            }
            cursor = Some(next);
        }
        Err(McpError::protocol(
            "resource list exceeded the configured pagination limit",
        ))
    }

    /// Fetches one resource-list page without following its cursor.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for lifecycle, transport, cursor, or peer failures.
    pub async fn list_resources_page(
        &self,
        cursor: Option<String>,
    ) -> Result<ListResourcesResult, McpError> {
        self.list_resources_page_with_cache(cursor, CacheMode::Use)
            .await
    }

    /// Fetches one resource-list page with explicit cache behavior.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for lifecycle, transport, cursor, or peer failures.
    pub async fn list_resources_page_with_cache(
        &self,
        cursor: Option<String>,
        cache_mode: CacheMode,
    ) -> Result<ListResourcesResult, McpError> {
        self.require_resources().await?;
        self.request_typed_with_cache(
            "resources/list",
            &ListResourcesParams { cursor },
            self.inner.config.request_timeout,
            cache_mode,
        )
        .await
    }

    /// Lists all authorized resource templates across pagination.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for capability, transport, pagination, or peer failures.
    pub async fn list_resource_templates(&self) -> Result<Vec<McpResourceTemplate>, McpError> {
        self.require_resources().await?;
        let mut templates = Vec::new();
        let mut cursor = None;
        let mut seen = HashSet::new();
        for _ in 0..self.inner.config.max_pagination_pages {
            let page = self.list_resource_templates_page(cursor).await?;
            templates.extend(page.resource_templates);
            let Some(next) = page.next_cursor else {
                return Ok(templates);
            };
            if !seen.insert(next.clone()) {
                return Err(McpError::protocol("server repeated a pagination cursor"));
            }
            cursor = Some(next);
        }
        Err(McpError::protocol(
            "resource-template list exceeded the configured pagination limit",
        ))
    }

    /// Fetches one resource-template-list page without following its cursor.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for capability, transport, cursor, or peer failures.
    pub async fn list_resource_templates_page(
        &self,
        cursor: Option<String>,
    ) -> Result<ListResourceTemplatesResult, McpError> {
        self.list_resource_templates_page_with_cache(cursor, CacheMode::Use)
            .await
    }

    /// Fetches one resource-template page with explicit cache behavior.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for lifecycle, transport, cursor, or peer failures.
    pub async fn list_resource_templates_page_with_cache(
        &self,
        cursor: Option<String>,
        cache_mode: CacheMode,
    ) -> Result<ListResourceTemplatesResult, McpError> {
        self.require_resources().await?;
        self.request_typed_with_cache(
            "resources/templates/list",
            &ListResourceTemplatesParams { cursor },
            self.inner.config.request_timeout,
            cache_mode,
        )
        .await
    }

    /// Reads one exact remote resource URI.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when resources were not negotiated or the peer
    /// rejects the URI.
    pub async fn read_resource(
        &self,
        uri: impl Into<String>,
    ) -> Result<ReadResourceResult, McpError> {
        self.read_resource_with_cache(uri, CacheMode::Use).await
    }

    /// Reads one exact resource URI with explicit cache behavior.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when resources were not negotiated or the peer
    /// rejects the URI.
    pub async fn read_resource_with_cache(
        &self,
        uri: impl Into<String>,
        cache_mode: CacheMode,
    ) -> Result<ReadResourceResult, McpError> {
        self.require_resources().await?;
        self.request_typed_with_cache(
            "resources/read",
            &ReadResourceParams { uri: uri.into() },
            self.inner.config.request_timeout,
            cache_mode,
        )
        .await
    }

    /// Subscribes to updates for one exact authorized resource URI.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when subscriptions were not negotiated or the peer rejects the URI.
    pub async fn subscribe_resource(&self, uri: impl Into<String>) -> Result<(), McpError> {
        self.require_resource_subscriptions().await?;
        let _: serde_json::Value = self
            .request_typed(
                "resources/subscribe",
                &ResourceSubscriptionParams { uri: uri.into() },
                self.inner.config.request_timeout,
            )
            .await?;
        Ok(())
    }

    /// Removes one resource update subscription.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when subscriptions were not negotiated or the request fails.
    pub async fn unsubscribe_resource(&self, uri: impl Into<String>) -> Result<(), McpError> {
        self.require_resource_subscriptions().await?;
        let _: serde_json::Value = self
            .request_typed(
                "resources/unsubscribe",
                &ResourceSubscriptionParams { uri: uri.into() },
                self.inner.config.request_timeout,
            )
            .await?;
        Ok(())
    }

    /// Opens the transport's server-to-client notification stream.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when the client is inactive or the transport cannot subscribe.
    pub async fn notifications(&self) -> Result<ServerNotificationStream, McpError> {
        self.require_active().await?;
        let stream = self.inner.transport.subscribe().await?;
        let cache = self.inner.cache.clone();
        Ok(Box::pin(stream.map(move |notification| {
            if let Ok(notification) = &notification {
                cache.invalidate_notification(notification);
            }
            notification
        })))
    }

    /// Opens one explicitly filtered modern notification subscription.
    ///
    /// The returned stream has already consumed and validated the server's
    /// acknowledgment. Dropping it cancels the transport-scoped subscription.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for lifecycle, transport, malformed
    /// acknowledgment, correlation, or timeout failures.
    pub async fn listen(
        &self,
        notifications: SubscriptionFilter,
    ) -> Result<McpSubscription, McpError> {
        self.require_active().await?;
        if self.protocol_mode().await != Some(McpProtocolMode::Stateless) {
            return Err(McpError::protocol(
                "subscriptions/listen requires the stateless protocol",
            ));
        }
        let requested = notifications.normalized();
        let id = self.next_id();
        let request = self
            .request_with_current_metadata(
                id.clone(),
                "subscriptions/listen",
                &SubscriptionsListenParams::new(requested.clone()),
            )
            .await?;
        let mut stream = self.inner.transport.listen(request).await?;
        let first = tokio::time::timeout(self.inner.config.request_timeout, stream.next())
            .await
            .map_err(|_| McpError::DeadlineExceeded)?
            .ok_or_else(|| McpError::protocol("subscription closed before acknowledgment"))??;
        if first.method != "notifications/subscriptions/acknowledged" {
            return Err(McpError::protocol(
                "subscription stream did not begin with an acknowledgment",
            ));
        }
        let acknowledgment: SubscriptionAcknowledgedParams = first
            .params
            .ok_or_else(|| McpError::protocol("subscription acknowledgment omitted parameters"))
            .and_then(|params| serde_json::from_value(params).map_err(Into::into))?;
        if acknowledgment.subscription_id()? != id {
            return Err(McpError::protocol(
                "subscription acknowledgment id does not match its request",
            ));
        }
        let accepted = acknowledgment.notifications.normalized();
        if !accepted.is_subset_of(&requested) {
            return Err(McpError::protocol(
                "server acknowledged notifications the client did not request",
            ));
        }
        let subscription_id = id.clone();
        let cache = self.inner.cache.clone();
        let correlated = Box::pin(async_stream::stream! {
            while let Some(notification) = stream.next().await {
                match notification {
                    Ok(notification)
                        if notification_subscription_id(&notification)
                            == Some(subscription_id.clone()) =>
                    {
                        cache.invalidate_notification(&notification);
                        yield Ok(notification);
                    }
                    Ok(_) => {
                        yield Err(McpError::protocol(
                            "subscription notification omitted or mismatched its subscription id",
                        ));
                        break;
                    }
                    Err(error) => {
                        yield Err(error);
                        break;
                    }
                }
            }
        });
        Ok(McpSubscription::new(id, accepted, correlated))
    }
}
