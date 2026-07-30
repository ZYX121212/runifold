use super::{
    Arc, BTreeMap, CacheMode, CancellationToken, DeserializeOwned, Duration, InputRequiredResult,
    McpClient, McpError, McpProtocolMode, MrtrInputHandler, Serialize,
};

impl McpClient {
    pub(super) async fn request_typed<P, R>(
        &self,
        method: &str,
        params: &P,
        timeout: Duration,
    ) -> Result<R, McpError>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.request_typed_with_cache(method, params, timeout, CacheMode::Use)
            .await
    }

    pub(super) async fn request_typed_with_cache<P, R>(
        &self,
        method: &str,
        params: &P,
        timeout: Duration,
        cache_mode: CacheMode,
    ) -> Result<R, McpError>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.request_typed_mrtr(
            method,
            params,
            timeout,
            CancellationToken::new(),
            cache_mode,
        )
        .await
    }

    pub(crate) async fn request_typed_mrtr<P, R>(
        &self,
        method: &str,
        params: &P,
        timeout: Duration,
        cancellation: CancellationToken,
        cache_mode: CacheMode,
    ) -> Result<R, McpError>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let base_params = serde_json::to_value(params)?;
        if !base_params.is_object() {
            return Err(McpError::protocol(
                "MCP request parameters must be an object",
            ));
        }
        let cacheable = self.cacheable_request(method).await;
        if let Some(result) = self.cached_result(method, &base_params, cache_mode, cacheable)? {
            return Ok(result);
        }
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| McpError::protocol("MCP request timeout is outside platform limits"))?;
        let mut attempt_params = base_params.clone();
        for round in 0..=self.inner.config.max_mrtr_rounds {
            if cancellation.is_cancelled() {
                return Err(McpError::Cancelled);
            }
            let id = self.next_id();
            let request = self
                .request_with_current_metadata(id.clone(), method, &attempt_params)
                .await?;
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .ok_or(McpError::DeadlineExceeded)?;
            let response = tokio::select! {
                response = self.inner.transport.request(request) => response?,
                () = cancellation.cancelled() => {
                    self.cancel_request(&id, "Runifold MCP request cancelled").await;
                    return Err(McpError::Cancelled);
                }
                () = tokio::time::sleep(remaining) => {
                    self.cancel_request(&id, "Runifold MCP request timed out").await;
                    return Err(McpError::DeadlineExceeded);
                }
            };
            if response.id() != &id {
                return Err(McpError::protocol("response id does not match request id"));
            }
            let result = match response.into_result() {
                Ok(result) => result,
                Err(error) => {
                    if cacheable {
                        self.inner.cache.invalidate_method(method);
                    }
                    return Err(error);
                }
            };
            if !Self::is_input_required(&result, method)? {
                return self.decode_result(
                    method,
                    &base_params,
                    result,
                    cache_mode,
                    cacheable && round == 0,
                );
            }
            if !matches!(method, "tools/call" | "prompts/get" | "resources/read") {
                return Err(McpError::protocol(format!(
                    "MRTR input_required is not valid for `{method}`"
                )));
            }
            if round == self.inner.config.max_mrtr_rounds {
                return Err(McpError::protocol(
                    "MRTR exceeded the configured round limit",
                ));
            }
            let incomplete: InputRequiredResult = serde_json::from_value(result)?;
            incomplete.validate(self.inner.config.max_mrtr_inputs_per_round)?;
            let input_responses = self
                .resolve_mrtr_inputs(&incomplete, deadline, &cancellation)
                .await?;
            attempt_params = base_params.clone();
            let attempt = attempt_params
                .as_object_mut()
                .expect("base request parameters were verified as an object");
            if !input_responses.is_empty() {
                attempt.insert(
                    "inputResponses".into(),
                    serde_json::to_value(input_responses)?,
                );
            }
            if let Some(request_state) = incomplete.request_state {
                attempt.insert(
                    "requestState".into(),
                    serde_json::Value::String(request_state),
                );
            }
        }
        Err(McpError::protocol("MRTR driver exhausted unexpectedly"))
    }

    fn is_input_required(result: &serde_json::Value, method: &str) -> Result<bool, McpError> {
        let result_type = result
            .as_object()
            .and_then(|result| result.get("resultType"))
            .and_then(serde_json::Value::as_str);
        match result_type {
            None | Some("complete") => Ok(false),
            Some("input_required") => Ok(true),
            Some("task") if method == "tools/call" => Ok(false),
            Some(other) => Err(McpError::protocol(format!(
                "unsupported MCP resultType `{other}`"
            ))),
        }
    }

    async fn cacheable_request(&self, method: &str) -> bool {
        let recognized = crate::CacheOperation::from_method(method).is_some();
        recognized
            && (method == "server/discover"
                || self.protocol_mode().await == Some(McpProtocolMode::Stateless))
    }

    fn cached_result<R>(
        &self,
        method: &str,
        params: &serde_json::Value,
        mode: CacheMode,
        cacheable: bool,
    ) -> Result<Option<R>, McpError>
    where
        R: DeserializeOwned,
    {
        if !cacheable || mode != CacheMode::Use {
            return Ok(None);
        }
        self.inner
            .cache
            .get(method, params)
            .map(serde_json::from_value)
            .transpose()
            .map_err(Into::into)
    }

    fn decode_result<R>(
        &self,
        method: &str,
        params: &serde_json::Value,
        result: serde_json::Value,
        mode: CacheMode,
        cacheable: bool,
    ) -> Result<R, McpError>
    where
        R: DeserializeOwned,
    {
        if cacheable && mode != CacheMode::Bypass {
            self.inner.cache.put(method, params, result.clone());
        }
        serde_json::from_value(result).map_err(Into::into)
    }

    pub(crate) async fn resolve_mrtr_inputs(
        &self,
        incomplete: &InputRequiredResult,
        deadline: tokio::time::Instant,
        cancellation: &CancellationToken,
    ) -> Result<BTreeMap<String, serde_json::Value>, McpError> {
        let handler: Arc<dyn MrtrInputHandler> =
            if let Some(handler) = &self.inner.config.mrtr_input_handler {
                Arc::clone(handler)
            } else if let Some(sampling) = &self.inner.config.sampling {
                Arc::clone(sampling) as Arc<dyn MrtrInputHandler>
            } else if incomplete.input_requests.is_empty() {
                return Ok(BTreeMap::new());
            } else {
                return Err(McpError::protocol(
                    "server requested MRTR input but no input handler is configured",
                ));
            };
        let mut responses = BTreeMap::new();
        for (key, request) in &incomplete.input_requests {
            if cancellation.is_cancelled() {
                return Err(McpError::Cancelled);
            }
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .ok_or(McpError::DeadlineExceeded)?;
            let input_cancellation = cancellation.child_token();
            let response = tokio::select! {
                response = handler.handle(
                    key.clone(),
                    request.clone(),
                    input_cancellation.clone(),
                ) => response?,
                () = cancellation.cancelled() => return Err(McpError::Cancelled),
                () = tokio::time::sleep(remaining) => {
                    input_cancellation.cancel();
                    return Err(McpError::DeadlineExceeded);
                },
            };
            responses.insert(key.clone(), response);
        }
        Ok(responses)
    }
}
