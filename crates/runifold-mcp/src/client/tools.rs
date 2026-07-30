use super::{
    CacheMode, CallToolParams, CallToolResult, CancellationToken, HashSet, ListToolsParams,
    ListToolsResult, McpClient, McpError, McpTool, ToolContext,
};

impl McpClient {
    /// Lists available remote tools.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when the client is not initialized or the peer
    /// rejects the request.
    pub async fn list_tools(&self) -> Result<Vec<McpTool>, McpError> {
        self.require_active().await?;
        let mut tools = Vec::new();
        let mut cursor = None;
        let mut seen = HashSet::new();
        for _ in 0..self.inner.config.max_pagination_pages {
            let page = self.list_tools_page(cursor).await?;
            tools.extend(page.tools);
            let Some(next) = page.next_cursor else {
                return Ok(tools);
            };
            if !seen.insert(next.clone()) {
                return Err(McpError::protocol("server repeated a pagination cursor"));
            }
            cursor = Some(next);
        }
        Err(McpError::protocol(
            "tool list exceeded the configured pagination limit",
        ))
    }

    /// Fetches one tool-list page without following its cursor.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for lifecycle, transport, cursor, or peer failures.
    pub async fn list_tools_page(
        &self,
        cursor: Option<String>,
    ) -> Result<ListToolsResult, McpError> {
        self.list_tools_page_with_cache(cursor, CacheMode::Use)
            .await
    }

    /// Fetches one tool-list page with explicit cache behavior.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for lifecycle, transport, cursor, or peer failures.
    pub async fn list_tools_page_with_cache(
        &self,
        cursor: Option<String>,
        cache_mode: CacheMode,
    ) -> Result<ListToolsResult, McpError> {
        self.require_active().await?;
        let mut page: ListToolsResult = self
            .request_typed_with_cache(
                "tools/list",
                &ListToolsParams { cursor },
                self.inner.config.request_timeout,
                cache_mode,
            )
            .await?;
        page.tools = self.inner.transport.prepare_tools(page.tools)?;
        Ok(page)
    }

    /// Calls one remote tool with the configured timeout.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for lifecycle, transport, protocol, timeout, or
    /// peer failures.
    pub async fn call_tool(&self, params: CallToolParams) -> Result<CallToolResult, McpError> {
        self.require_active().await?;
        if self.tasks_negotiated().await {
            let deadline = tokio::time::Instant::now()
                .checked_add(self.inner.config.request_timeout)
                .ok_or_else(|| McpError::protocol("Task timeout is outside platform limits"))?;
            return match self
                .call_tool_outcome_scoped(
                    params,
                    self.inner.config.request_timeout,
                    CancellationToken::new(),
                )
                .await?
            {
                crate::CallToolOutcome::Complete(result) => Ok(result),
                crate::CallToolOutcome::Task(task) => {
                    self.wait_task_scoped(task, deadline, CancellationToken::new())
                        .await
                }
            };
        }
        self.request_typed("tools/call", &params, self.inner.config.request_timeout)
            .await
    }

    pub(crate) async fn call_tool_scoped(
        &self,
        params: CallToolParams,
        context: &ToolContext,
    ) -> Result<CallToolResult, McpError> {
        self.require_active().await?;
        let timeout = context
            .remaining()
            .map_or(self.inner.config.request_timeout, |remaining| {
                remaining.min(self.inner.config.request_timeout)
            });
        if self.tasks_negotiated().await {
            let deadline = tokio::time::Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| McpError::protocol("Task timeout is outside platform limits"))?;
            return match self
                .call_tool_outcome_scoped(params, timeout, context.cancellation().clone())
                .await?
            {
                crate::CallToolOutcome::Complete(result) => Ok(result),
                crate::CallToolOutcome::Task(task) => {
                    self.wait_task_scoped(task, deadline, context.cancellation().clone())
                        .await
                }
            };
        }
        self.request_typed_mrtr(
            "tools/call",
            &params,
            timeout,
            context.cancellation().clone(),
            CacheMode::Bypass,
        )
        .await
    }
}
