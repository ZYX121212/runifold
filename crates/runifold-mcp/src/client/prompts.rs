use super::{
    BTreeMap, CacheMode, CompleteParams, CompleteResult, GetPromptParams, GetPromptResult, HashSet,
    ListPromptsParams, ListPromptsResult, McpClient, McpError, McpPrompt,
};

impl McpClient {
    /// Lists authorized remote prompts.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when prompts were not negotiated, pagination is
    /// returned, or the request fails.
    pub async fn list_prompts(&self) -> Result<Vec<McpPrompt>, McpError> {
        self.require_prompts().await?;
        let mut prompts = Vec::new();
        let mut cursor = None;
        let mut seen = HashSet::new();
        for _ in 0..self.inner.config.max_pagination_pages {
            let page = self.list_prompts_page(cursor).await?;
            prompts.extend(page.prompts);
            let Some(next) = page.next_cursor else {
                return Ok(prompts);
            };
            if !seen.insert(next.clone()) {
                return Err(McpError::protocol("server repeated a pagination cursor"));
            }
            cursor = Some(next);
        }
        Err(McpError::protocol(
            "prompt list exceeded the configured pagination limit",
        ))
    }

    /// Fetches one prompt-list page without following its cursor.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for capability, transport, cursor, or peer failures.
    pub async fn list_prompts_page(
        &self,
        cursor: Option<String>,
    ) -> Result<ListPromptsResult, McpError> {
        self.list_prompts_page_with_cache(cursor, CacheMode::Use)
            .await
    }

    /// Fetches one prompt-list page with explicit cache behavior.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] for capability, transport, cursor, or peer failures.
    pub async fn list_prompts_page_with_cache(
        &self,
        cursor: Option<String>,
        cache_mode: CacheMode,
    ) -> Result<ListPromptsResult, McpError> {
        self.require_prompts().await?;
        self.request_typed_with_cache(
            "prompts/list",
            &ListPromptsParams { cursor },
            self.inner.config.request_timeout,
            cache_mode,
        )
        .await
    }

    /// Renders one user-selected remote prompt with string arguments.
    ///
    /// This method returns protocol content only; it never injects messages
    /// into a model request automatically.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when prompts were not negotiated, arguments are
    /// invalid, or the peer rejects the request.
    pub async fn get_prompt(
        &self,
        name: impl Into<String>,
        arguments: BTreeMap<String, String>,
    ) -> Result<GetPromptResult, McpError> {
        self.require_prompts().await?;
        self.request_typed(
            "prompts/get",
            &GetPromptParams {
                name: name.into(),
                arguments: (!arguments.is_empty()).then_some(arguments),
            },
            self.inner.config.request_timeout,
        )
        .await
    }

    /// Completes one prompt or resource-template argument.
    ///
    /// # Errors
    ///
    /// Returns [`McpError`] when completion was not negotiated or the peer rejects the request.
    pub async fn complete(&self, params: CompleteParams) -> Result<CompleteResult, McpError> {
        self.require_completions().await?;
        self.request_typed(
            "completion/complete",
            &params,
            self.inner.config.request_timeout,
        )
        .await
    }
}
